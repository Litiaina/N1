use super::*;

struct CacheShard {
    entries: HashMap<(u64, u64), Arc<Node>>,
    order: VecDeque<(u64, u64)>,
    bytes: usize,
}

pub(crate) struct NodeCache {
    shards: Vec<Mutex<CacheShard>>,
    budget: usize,
}

impl NodeCache {
    pub fn new(budget: usize) -> Self {
        Self {
            budget: budget / 8,
            shards: (0..8)
                .map(|_| {
                    Mutex::new(CacheShard {
                        entries: HashMap::new(),
                        order: VecDeque::new(),
                        bytes: 0,
                    })
                })
                .collect(),
        }
    }

    fn shard(&self, generation: u64, offset: u64) -> &Mutex<CacheShard> {
        &self.shards[((offset >> 4) ^ generation) as usize % self.shards.len()]
    }

    pub(super) fn get(&self, generation: u64, offset: u64) -> Option<Arc<Node>> {
        self.shard(generation, offset)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .get(&(generation, offset))
            .cloned()
    }

    pub(super) fn insert(&self, generation: u64, offset: u64, node: Arc<Node>) {
        let size = node.bytes();
        if size > self.budget {
            return;
        }
        let mut shard = self
            .shard(generation, offset)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if shard.entries.contains_key(&(generation, offset)) {
            return;
        }
        while shard.bytes + size > self.budget {
            let Some(old) = shard.order.pop_front() else {
                break;
            };
            if let Some(node) = shard.entries.remove(&old) {
                shard.bytes -= node.bytes();
            }
        }
        shard.bytes += size;
        shard.order.push_back((generation, offset));
        shard.entries.insert((generation, offset), node);
    }

    pub fn bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap_or_else(|e| e.into_inner()).bytes)
            .sum()
    }

    pub(super) fn remove(&self, generation: u64, offset: u64) {
        let key = (generation, offset);
        let mut shard = self
            .shard(generation, offset)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(node) = shard.entries.remove(&key) {
            shard.bytes -= node.bytes();
            shard.order.retain(|entry| *entry != key);
        }
    }

    pub fn remove_generation(&self, generation: u64) {
        for shard in &self.shards {
            let mut shard = shard.lock().unwrap_or_else(|e| e.into_inner());
            shard.entries.retain(|(id, _), _| *id != generation);
            shard.order.retain(|(id, _)| *id != generation);
            shard.bytes = shard.entries.values().map(|node| node.bytes()).sum();
        }
    }
}
