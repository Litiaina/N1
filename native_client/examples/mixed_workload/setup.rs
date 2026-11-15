use super::{
    Result,
    api::{self, Api},
};
use n1_native_client::{Command, N1NativeClient};
use serde_json::{Value, json};
use std::{
    fs::{self, OpenOptions},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    process::{Child, Command as Process, Stdio},
    time::{Duration, Instant},
};

pub struct Stack {
    child: Option<Child>,
    binary: PathBuf,
    config: PathBuf,
    output: PathBuf,
    roots: [PathBuf; 2],
    secret: String,
    pub api: Api,
}
impl Drop for Stack {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
fn openssl(output: &Path, args: &[&str]) -> Result<()> {
    let status = Process::new("openssl")
        .args(args)
        .current_dir(output)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if !status.success() {
        return Err("private test certificate creation failed".into());
    }
    Ok(())
}
impl Stack {
    pub async fn start(binary: &str, first: &str, second: &str, output: &Path) -> Result<Self> {
        let binary = PathBuf::from(binary).canonicalize()?;
        let first = PathBuf::from(first).canonicalize()?;
        let second = PathBuf::from(second).canonicalize()?;
        if first.metadata()?.dev() == second.metadata()?.dev() {
            return Err("mixed workload requires two independent filesystems".into());
        }
        fs::DirBuilder::new().mode(0o700).create(output)?;
        let output = output.canonicalize()?;
        let roots = [first.join("member-0"), second.join("member-1")];
        for root in &roots {
            fs::DirBuilder::new().mode(0o700).create(root)?;
        }
        openssl(
            &output,
            &[
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-sha256",
                "-nodes",
                "-days",
                "2",
                "-keyout",
                "ca.key",
                "-out",
                "ca.pem",
                "-subj",
                "/CN=Workload Test CA",
            ],
        )?;
        openssl(
            &output,
            &[
                "req",
                "-newkey",
                "rsa:2048",
                "-sha256",
                "-nodes",
                "-keyout",
                "server.key",
                "-out",
                "server.csr",
                "-subj",
                "/CN=localhost",
            ],
        )?;
        fs::write(
            output.join("extensions.cnf"),
            "subjectAltName=IP:127.0.0.1\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n",
        )?;
        openssl(
            &output,
            &[
                "x509",
                "-req",
                "-in",
                "server.csr",
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-out",
                "server.pem",
                "-days",
                "2",
                "-sha256",
                "-extfile",
                "extensions.cnf",
            ],
        )?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        drop(listener);
        let config = output.join("service.json");
        let log = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(output.join("enrollment.log"))?;
        let status = Process::new(&binary)
            .args(["init", "--config"])
            .arg(&config)
            .arg("--member")
            .arg(&roots[0])
            .arg("--member")
            .arg(&roots[1])
            .args([
                "--data-shards",
                "1",
                "--parity-shards",
                "1",
                "--native-address",
            ])
            .arg(address.to_string())
            .args(["--tls-certificate"])
            .arg(output.join("server.pem"))
            .arg("--tls-key")
            .arg(output.join("server.key"))
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .status()?;
        if !status.success() {
            return Err("private N1 enrollment failed".into());
        }
        let mut configuration: Value = serde_json::from_slice(&fs::read(&config)?)?;
        configuration["rate_limits"]["enabled"] = json!(false);
        configuration["max_connections"] = json!(64);
        configuration["max_operations"] = json!(32);
        fs::write(&config, serde_json::to_vec_pretty(&configuration)?)?;
        let credentials: Value =
            serde_json::from_slice(&fs::read(config.with_extension("credentials.json"))?)?;
        let secret = credentials["secret"]
            .as_str()
            .ok_or("missing private root credential")?
            .to_owned();
        let api = Api {
            url: format!("n1s://{address}"),
            tls: api::tls(&output.join("ca.pem"))?,
            token: String::new(),
        };
        api::save(
            &output.join("provenance.json"),
            &json!({
                "n1_sha256":api::file_sha(&binary)?,"client_sha256":api::file_sha(&std::env::current_exe()?)?,
                "topology":"1+1 on distinct filesystems","pool":configuration["pools"],
                "max_connections":64,"max_operations":32,"operation_memory_bytes":configuration["operation_memory_bytes"],
                "concurrency":configuration["concurrency"],"runtime":configuration["runtime"],
                "rate_limits_enabled":false,"io_backend":"required io_uring",
                "transport":"loopback native TLS with verified private CA","http":false,"m1":false,
                "cache_policy":"no cache drop, no cold-read claim, library may be cached",
                "client_model":"closed-loop paced user sessions; request latency includes payload verification but excludes think time",
                "measurement_limit":"this measures local storage service, not a remote network or browser"
            }),
        )?;
        let mut stack = Self {
            child: None,
            binary,
            config,
            output,
            roots,
            secret,
            api,
        };
        stack.launch().await?;
        let mut client = stack.api.connect().await?;
        let pools = api::value(client.control(Command::Pools).await?)?;
        let pool = pools["pools"]
            .as_array()
            .and_then(|p| p.first())
            .ok_or("pool missing")?;
        if pool["data_shards"] != 1 || pool["parity_shards"] != 1 || pool["available_members"] != 2
        {
            return Err("private pool topology/availability mismatch".into());
        }
        Ok(stack)
    }
    async fn launch(&mut self) -> Result<()> {
        let log = OpenOptions::new()
            .append(true)
            .create(true)
            .mode(0o600)
            .open(self.output.join("engine.log"))?;
        self.child = Some(
            Process::new(&self.binary)
                .args(["serve", "--config"])
                .arg(&self.config)
                .env("N1_IO_BACKEND", "uring")
                .env("RUST_LOG", "info")
                .current_dir(&self.output)
                .stdin(Stdio::null())
                .stdout(log.try_clone()?)
                .stderr(log)
                .spawn()?,
        );
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if self.child.as_mut().unwrap().try_wait()?.is_some() {
                return Err("private N1 exited during startup".into());
            }
            if let Ok(mut client) =
                N1NativeClient::connect_with_tls(&self.api.url, Some(self.api.tls.clone())).await
            {
                let login = api::value(client.auth_fragment("root", &self.secret).await?)?;
                self.api.token = login["token"].as_str().ok_or("missing login token")?.into();
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("private N1 startup deadline".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    pub fn pid(&self) -> u32 {
        self.child.as_ref().expect("owned running N1").id()
    }
    pub async fn stop(&mut self) -> Result<()> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        if child.try_wait()?.is_some() {
            return Err("N1 exited unexpectedly".into());
        }
        if unsafe { libc::kill(child.id() as i32, libc::SIGTERM) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            if let Some(status) = child.try_wait()? {
                if !status.success() {
                    return Err(format!("N1 shutdown failure: {status}").into());
                }
                self.child.take();
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err("N1 shutdown exceeded deadline".into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    pub async fn restart(&mut self) -> Result<()> {
        self.stop().await?;
        self.launch().await
    }
    pub fn census(&self) -> Result<Value> {
        fn visit(path: &Path, files: &mut Vec<PathBuf>, symlinks: &mut usize) -> Result<()> {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let kind = entry.file_type()?;
                if kind.is_symlink() {
                    *symlinks += 1;
                } else if kind.is_dir() {
                    visit(&entry.path(), files, symlinks)?;
                } else {
                    files.push(entry.path());
                }
            }
            Ok(())
        }
        let mut files = Vec::new();
        let mut symlinks = 0;
        for root in &self.roots {
            visit(root, &mut files, &mut symlinks)?;
        }
        if files.len() != 2
            || symlinks != 0
            || files
                .iter()
                .any(|p| p.file_name().and_then(|s| s.to_str()) != Some(".n1-arc-pool.json"))
        {
            return Err(format!(
                "identity residue after workload cleanup: {files:?}, symlinks={symlinks}"
            )
            .into());
        }
        Ok(json!({"remaining_files":files.len(),"pool_markers_only":true,"symlinks":symlinks}))
    }
}
