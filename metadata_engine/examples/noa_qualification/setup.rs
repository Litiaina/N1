use super::{Args, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};
#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    pub root: PathBuf,
    pub first: PathBuf,
    pub second: PathBuf,
    pub url: String,
    pub metadata_native: u16,
    pub object_native: u16,
    pub object_binary: PathBuf,
    pub metadata_binary: PathBuf,
    pub object_sha256: String,
    pub metadata_sha256: String,
    pub harness_sha256: String,
    pub fragment_secret: String,
    pub master: String,
    pub identity: String,
}
pub struct Process(pub Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
pub struct Stack {
    pub config: Config,
    pub object: Option<Process>,
    pub metadata: Option<Process>,
    startup_timeout: Duration,
}
fn port() -> Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}
fn directory(path: &Path) -> Result<()> {
    match std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
    {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(e) => return Err(e.into()),
    }
    let m = std::fs::symlink_metadata(path)?;
    if !m.is_dir() || m.file_type().is_symlink() || m.permissions().mode() & 0o077 != 0 {
        return Err("qualification paths must be real owner-only directories".into());
    }
    Ok(())
}
fn hash(path: &Path) -> Result<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut h = Sha256::new();
    let mut b = [0; 65536];
    loop {
        let n = file.read(&mut b)?;
        if n == 0 {
            break;
        }
        h.update(&b[..n]);
    }
    Ok(hex::encode(h.finalize()))
}
impl Stack {
    pub async fn open(args: &Args) -> Result<Self> {
        if !args.root.is_absolute()
            || !args.second_root.is_absolute()
            || args
                .first_root
                .as_ref()
                .is_some_and(|path| !path.is_absolute())
        {
            return Err("qualification roots must be absolute".into());
        }
        directory(&args.root)?;
        if let Some(first) = &args.first_root {
            directory(first)?;
        }
        directory(&args.second_root)?;
        if let Some(first) = &args.first_root
            && std::fs::metadata(first)?.dev() == std::fs::metadata(&args.second_root)?.dev()
        {
            return Err("object members require two independent filesystems".into());
        }
        if std::fs::metadata(&args.root)?.dev() == std::fs::metadata(&args.second_root)?.dev() {
            return Err("two independent filesystems are required".into());
        }
        let state = args.root.join("stack.json");
        let config: Config = if state.exists() {
            let mut c: Config = serde_json::from_slice(&std::fs::read(&state)?)?;
            if hash(&args.object_binary)? != c.object_sha256
                || hash(&args.metadata_binary)? != c.metadata_sha256
                || hash(&std::env::current_exe()?)? != c.harness_sha256
            {
                return Err(
                    "resume requires the exact frozen engine and qualification binaries".into(),
                );
            }
            if c.root.canonicalize()? != args.root.canonicalize()?
                || c.second
                    .parent()
                    .ok_or("invalid secondary root")?
                    .canonicalize()?
                    != args.second_root.canonicalize()?
            {
                return Err("resume roots do not match this campaign".into());
            }
            let expected = match &args.first_root {
                Some(parent) => parent
                    .join(format!("n1-noa-{}", c.identity))
                    .join("member0"),
                None => args.root.join("member0"),
            };
            if c.first.canonicalize()? != expected.canonicalize()? {
                return Err("resume first member does not match this campaign".into());
            }
            c.object_binary = args.object_binary.canonicalize()?;
            c.metadata_binary = args.metadata_binary.canonicalize()?;
            c
        } else {
            let identity = ulid::Ulid::new().to_string();
            let second = args.second_root.join(format!("n1-noa-{identity}"));
            directory(&second)?;
            directory(&args.root.join("object"))?;
            directory(&args.root.join("metadata"))?;
            let member0 = args
                .first_root
                .as_ref()
                .map(|parent| parent.join(format!("n1-noa-{identity}")).join("member0"))
                .unwrap_or_else(|| args.root.join("member0"));
            let member1 = second.join("member1");
            directory(&member0)?;
            directory(&member1)?;
            let object_native = port()?;
            let metadata_native = port()?;
            let object_http = port()?;
            let metadata_http = port()?;
            let secret = format!("{}{}", ulid::Ulid::new(), ulid::Ulid::new());
            let master = format!("{}{}", ulid::Ulid::new(), ulid::Ulid::new());
            let jwt = format!("{}{}", ulid::Ulid::new(), ulid::Ulid::new());
            for (component, http, native) in [
                ("object", object_http, object_native),
                ("metadata", metadata_http, metadata_native),
            ] {
                let config_file = if component == "metadata" {
                    "m1_config.ini"
                } else {
                    "n1_config.ini"
                };
                let path = args.root.join(component).join(config_file);
                cli_core::settings::create_default_config_ini(&path)?;
                let mut ini = ini::Ini::load_from_file(&path)?;
                ini.with_section(Some("server"))
                    .set("host", "127.0.0.1")
                    .set("https_port", http.to_string())
                    .set("native_port", native.to_string())
                    .set("native_max_connections", "64")
                    .set("cert_path", "")
                    .set("key_path", "");
                ini.with_section(Some("disk_config")).set(
                    "fs_storage_path",
                    args.root
                        .join(component)
                        .join("control")
                        .display()
                        .to_string(),
                );
                ini.with_section(Some("storage_pool.A"))
                    .set(
                        "roots",
                        format!("{},{}", member0.display(), member1.display()),
                    )
                    .set("data_shards", "1")
                    .set("parity_shards", "1")
                    .set("generation", "1")
                    .set("sealed", "false");
                ini.with_section(Some("upload_config"))
                    .set("stripe_block_size", "65536")
                    .set("max_concurrent_io", "16")
                    .set("upload_memory_budget_bytes", "268435456")
                    .set("adaptive_concurrency_enabled", "true")
                    .set("upload_part_strict_durability_default", "true");
                ini.with_section(Some("auto_healing"))
                    .set("enabled", "false")
                    .set("scrub_schedule_utc", "");
                ini.with_section(Some("rate_limiter_config"))
                    .set("enable_rate_limiting", "false");
                if component == "metadata" {
                    if let Some(limit) = args.metadata_max_concurrent_io {
                        ini.with_section(Some("upload_config"))
                            .set("max_concurrent_io", limit.to_string());
                    }
                    ini.with_section(Some("metadata_engine"))
                        .set(
                            "object_native_url",
                            format!("n1://127.0.0.1:{object_native}"),
                        )
                        .set("object_fragment", "root")
                        .set("object_connections", "32");
                }
                ini.write_to_file(&path)?;
                let secrets_file = if component == "metadata" {
                    "m1.env"
                } else {
                    "n1.env"
                };
                let env = args.root.join(component).join(secrets_file);
                std::fs::write(
                    &env,
                    format!(
                        "JWT_SECRET={jwt}\nAUTH_KEY={secret}\nMASTER_KEY={master}\nADMIN_KEY={master}\nACCESS_TOKEN_DEFAULT_TTL_SECS=3600\nACCESS_TOKEN_MAX_TTL_SECS=86400\n"
                    ),
                )?;
                std::fs::set_permissions(env, std::fs::Permissions::from_mode(0o600))?;
            }
            let c = Config {
                root: args.root.clone(),
                first: member0,
                second,
                url: format!("http://127.0.0.1:{metadata_http}"),
                metadata_native,
                object_native,
                object_binary: args.object_binary.canonicalize()?,
                metadata_binary: args.metadata_binary.canonicalize()?,
                object_sha256: hash(&args.object_binary)?,
                metadata_sha256: hash(&args.metadata_binary)?,
                harness_sha256: hash(&std::env::current_exe()?)?,
                fragment_secret: format!("{}{}", ulid::Ulid::new(), ulid::Ulid::new()),
                master,
                identity,
            };
            std::fs::write(&state, serde_json::to_vec_pretty(&c)?)?;
            std::fs::set_permissions(state, std::fs::Permissions::from_mode(0o600))?;
            c
        };
        let mut stack = Self {
            config,
            object: None,
            metadata: None,
            startup_timeout: Duration::from_secs(args.startup_timeout_seconds),
        };
        stack.start().await?;
        Ok(stack)
    }
    fn spawn(&self, component: &str, binary: &Path) -> Result<Process> {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.config.root.join(format!("{component}.log")))?;
        Ok(Process(
            Command::new(binary)
                .arg("--config")
                .arg(
                    self.config
                        .root
                        .join(component)
                        .join(if component == "metadata" {
                            "m1_config.ini"
                        } else {
                            "n1_config.ini"
                        }),
                )
                .current_dir(self.config.root.join(component))
                .stdout(Stdio::from(log.try_clone()?))
                .stderr(Stdio::from(log))
                .spawn()?,
        ))
    }
    async fn start(&mut self) -> Result<()> {
        self.object = Some(self.spawn("object", &self.config.object_binary)?);
        let mut ready = false;
        let deadline = tokio::time::Instant::now() + self.startup_timeout;
        while tokio::time::Instant::now() < deadline {
            if tokio::net::TcpStream::connect(("127.0.0.1", self.config.object_native))
                .await
                .is_ok()
            {
                ready = true;
                break;
            }
            if self.object.as_mut().unwrap().0.try_wait()?.is_some() {
                return Err("object server exited; inspect object.log".into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if !ready {
            return Err(format!(
                "object server did not start within {} seconds; inspect object.log",
                self.startup_timeout.as_secs()
            )
            .into());
        }
        self.metadata = Some(self.spawn("metadata", &self.config.metadata_binary)?);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()?;
        let deadline = tokio::time::Instant::now() + self.startup_timeout;
        while tokio::time::Instant::now() < deadline {
            if client
                .get(format!("{}/noa/v1/ping", self.config.url))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                std::fs::write(
                    self.config.root.join("stack-ready.json"),
                    serde_json::to_vec(&serde_json::json!({
                        "url": self.config.url,
                        "object_pid": self.object.as_ref().unwrap().0.id(),
                        "metadata_pid": self.metadata.as_ref().unwrap().0.id(),
                        "native_port": self.config.metadata_native
                    }))?,
                )?;
                return Ok(());
            }
            if self.metadata.as_mut().unwrap().0.try_wait()?.is_some() {
                return Err("metadata server exited; inspect metadata.log".into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(format!(
            "metadata server did not start within {} seconds; inspect metadata.log",
            self.startup_timeout.as_secs()
        )
        .into())
    }
    pub async fn restart(&mut self) -> Result<()> {
        drop(self.metadata.take());
        drop(self.object.take());
        self.start().await
    }
}
