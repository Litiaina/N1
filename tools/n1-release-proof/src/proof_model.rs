type ProofResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DEFAULT_MAX_PART_SIZE: u64 = 52_428_800;

#[derive(Parser, Debug)]
#[command(about = "Rust-only N1 release proof matrix runner")]
struct Args {
    #[arg(long)]
    binary: PathBuf,
    #[arg(long)]
    source_file: PathBuf,
    #[arg(long)]
    disk_roots: String,
    #[arg(long)]
    out_dir: PathBuf,
    #[arg(long, default_value_t = 50_671)]
    port: u16,
    #[arg(long, default_value_t = 0)]
    native_port: u16,
    #[arg(long, default_value = "release-proof/")]
    prefix: String,
    #[arg(long, default_value_t = 2_147_483_648)]
    quota_bytes: u64,
    #[arg(long, default_value = "disabled")]
    quota_backend: String,
    #[arg(long, default_value_t = DEFAULT_MAX_PART_SIZE)]
    max_part_size: u64,
    #[arg(long, default_value = "n1-release-proof-master")]
    master_key: String,
    #[arg(long, default_value = "n1-release-proof-fragment")]
    fragment_key: String,
    #[arg(long, default_value = "")]
    fragment_name: String,
    #[arg(long, default_value_t = false)]
    cleanup_roots: bool,
    #[arg(long, default_value_t = 10_800)]
    job_timeout_seconds: u64,
    #[arg(long, default_value_t = 2)]
    large_copies: usize,
    #[arg(long, default_value_t = 10_000)]
    storm_objects: usize,
    #[arg(long, default_value_t = 24)]
    storm_concurrency: usize,
    #[arg(long, default_value_t = 16_384)]
    storm_object_size: usize,
    #[arg(long, default_value_t = 4)]
    mixed_users: usize,
    #[arg(long, default_value_t = 1_000)]
    mixed_files_per_user: usize,
    #[arg(long, default_value_t = 1_000)]
    mixed_background_uploads: usize,
    #[arg(long, default_value_t = 4)]
    multi_fragment_users: usize,
    #[arg(long, default_value_t = 250)]
    multi_fragment_files_per_user: usize,
    #[arg(long, default_value_t = false)]
    chaos: bool,
    #[arg(long, default_value_t = 0)]
    chaos_seconds: u64,
    #[arg(long, default_value_t = 0)]
    chaos_cycles: usize,
    #[arg(long, default_value_t = 1_000)]
    chaos_storm_objects: usize,
    #[arg(long, default_value_t = false)]
    maintenance_interrupt_proof: bool,
    #[arg(long, default_value_t = false)]
    object_maintenance_gate_proof: bool,
    #[arg(long, default_value_t = false)]
    native_protocol_proof: bool,
    #[arg(long, default_value_t = false)]
    native_transport_benchmark: bool,
    #[arg(long, default_value_t = 1_000)]
    benchmark_iterations: usize,
    #[arg(long, default_value_t = 100)]
    benchmark_warmup: usize,
}

#[derive(Debug)]
struct Proof {
    args: Args,
    stamp: String,
    client: Client,
    base_url: String,
    index_roots: Vec<PathBuf>,
    blob_roots: Vec<PathBuf>,
    server: Option<Child>,
    fragment: String,
    token: String,
    summary: Value,
    active_disk_count: usize,
    parity_shards: u16,
}

struct ActorReport {
    final_keys: Vec<String>,
    report: Value,
}

#[derive(Deserialize)]
struct FragmentCreate {
    fragment_hash: String,
}

#[derive(Deserialize)]
struct AuthResponse {
    access_token: String,
}

#[derive(Deserialize)]
struct MultipartInit {
    version_id: String,
    session_id: String,
}

impl Proof {
    fn new(args: Args) -> ProofResult<Self> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs()
            .to_string();
        let request_timeout = Duration::from_secs(args.job_timeout_seconds.max(900) + 600);
        let client = Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(request_timeout)
            .build()?;
        let base_url = format!("https://127.0.0.1:{}", args.port);
        Ok(Self {
            args,
            stamp,
            client,
            base_url,
            index_roots: Vec::new(),
            blob_roots: Vec::new(),
            server: None,
            fragment: String::new(),
            token: String::new(),
            summary: json!({}),
            active_disk_count: 0,
            parity_shards: 1,
        })
    }

    fn quota_backend_enforced(&self) -> bool {
        !matches!(
            self.args.quota_backend.trim().to_ascii_lowercase().as_str(),
            "" | "disabled" | "disable" | "none" | "off" | "false" | "0"
        )
    }

    fn native_port(&self) -> u16 {
        if self.args.native_port == 0 {
            self.args.port.saturating_add(1)
        } else {
            self.args.native_port
        }
    }

}

impl Drop for Proof {
    fn drop(&mut self) {
        let _ = self.stop_server();
    }
}
