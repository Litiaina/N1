use crate::{
    Error, Options, Result, format,
    transaction::{Snapshot, Transaction},
    tree, worker,
};
use format::Generation;
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::JoinHandle,
};

pub(crate) struct State {
    pub generation: Arc<Generation>,
    pub sequence: u64,
    pub root: u64,
    pub end: u64,
    pub commit: u64,
    pub anchor_end: u64,
}

pub(crate) struct Core {
    pub(crate) path: PathBuf,
    pub(crate) identity: [u8; 16],
    pub(crate) options: Options,
    pub(crate) state: RwLock<Arc<State>>,
    pub(crate) cache: Arc<tree::NodeCache>,
    pub(crate) poisoned: Arc<AtomicBool>,
    pub(crate) stopping: AtomicBool,
}

pub(crate) struct Owner {
    pub(crate) core: Arc<Core>,
    pub(crate) sender: mpsc::SyncSender<worker::Command>,
    worker: Mutex<Option<JoinHandle<()>>>,
    // Retained until every database, transaction and snapshot handle is gone.
    _lease: File,
}

#[cfg(test)]
impl Owner {
    pub(crate) fn duplicate_lease(&self) -> File {
        self._lease.try_clone().unwrap()
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.core.stopping.store(true, Ordering::Release);
        let _ = self.sender.send(worker::Command::Shutdown);
        if let Some(worker) = self
            .worker
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = worker.join();
        }
        // A concurrent fork may briefly inherit this open-file description.
        // Explicit unlock releases our lease after workers drain, rather than
        // letting an unrelated child keep it until its next exec/exit.
        let _ = self._lease.unlock();
    }
}

#[derive(Clone)]
pub struct Database(pub(crate) Arc<Owner>);

impl Database {
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        if options.cache_bytes < 64 * 1024
            || options.write_buffer_bytes < 64 * 1024
            || options.journal_bytes < 64 * 1024
            || !(1..=64).contains(&options.background_workers)
        {
            return Err(Error::Invalid("invalid M1 storage resource limits".into()));
        }
        let path = path.as_ref().to_owned();
        let opened = format::open(&path)?;
        let cache = Arc::new(tree::NodeCache::new(options.cache_bytes as usize));
        let poisoned = Arc::new(AtomicBool::new(false));
        let generation = Arc::new(Generation::new(
            opened.file,
            opened.manifest.generation,
            format::generation_path(&path, opened.manifest.generation),
            cache.clone(),
            poisoned.clone(),
            false,
        ));
        let state = format::recover(generation, &opened.manifest)?;
        format::cleanup(&path, opened.manifest.generation, &opened.identity)?;
        let core = Arc::new(Core {
            path,
            identity: opened.identity,
            options,
            state: RwLock::new(Arc::new(state)),
            cache,
            poisoned,
            stopping: AtomicBool::new(false),
        });
        let (sender, receiver) = mpsc::sync_channel(64);
        let worker_core = core.clone();
        let worker = std::thread::Builder::new()
            .name("m1-commit".into())
            .spawn(move || worker::run(worker_core, receiver))?;
        Ok(Self(Arc::new(Owner {
            core,
            sender,
            worker: Mutex::new(Some(worker)),
            _lease: opened.lease,
        })))
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            owner: self.0.clone(),
            state: self.0.core.current(),
        }
    }

    pub fn write(&self) -> Result<Transaction> {
        self.0.core.healthy()?;
        Ok(Transaction::new(self.snapshot()))
    }

    /// Explicitly complete a compact checkpoint, without invalidating readers.
    pub fn checkpoint(&self) -> Result<()> {
        self.0.core.healthy()?;
        let (sender, receiver) = mpsc::sync_channel(1);
        self.0
            .sender
            .send(worker::Command::Checkpoint(sender))
            .map_err(|_| Error::Closed)?;
        receiver.recv().map_err(|_| Error::Closed)?
    }

    pub fn disk_bytes(&self) -> Result<u64> {
        let mut bytes = 0u64;
        for entry in std::fs::read_dir(&self.0.core.path)? {
            let metadata = match entry?.metadata() {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if metadata.is_file() {
                bytes = bytes
                    .checked_add(metadata.len())
                    .ok_or_else(|| Error::Invalid("M1 disk byte accounting overflow".into()))?;
            }
        }
        Ok(bytes)
    }

    pub fn cached_bytes(&self) -> usize {
        self.0.core.cache.bytes()
    }

    /// Full, explicitly requested data/index verification; never a polling path.
    pub fn verify(&self) -> Result<crate::Integrity> {
        let state = self.0.core.current();
        tree::verify(&state.generation, state.root, state.sequence)
    }
}

impl Core {
    pub(crate) fn healthy(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(Error::Poisoned)
        } else if self.stopping.load(Ordering::Acquire) {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }

    pub(crate) fn current(&self) -> Arc<State> {
        self.state.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub(crate) fn publish(&self, state: State) {
        *self.state.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(state);
    }
}
