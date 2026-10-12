type ProofResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DEFAULT_MAX_PART_SIZE: u64 = 52_428_800;
const PROOF_EXPORT_CHUNK_SIZE: usize = 16 * 1024 * 1024;

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
    #[arg(long, default_value = "127.0.0.1")]
    listen_host: String,
    #[arg(long, default_value_t = 0)]
    native_port: u16,
    #[arg(long, default_value = "release-proof/")]
    prefix: String,
    #[arg(long, default_value_t = false)]
    rate_limiter_enabled: bool,
    #[arg(long, default_value_t = DEFAULT_MAX_PART_SIZE)]
    max_part_size: u64,
    #[arg(long, default_value_t = 1)]
    data_shards: u16,
    #[arg(long, default_value_t = 1)]
    parity_shards: u16,
    #[arg(long, default_value = "n1-release-proof-master")]
    master_key: String,
    #[arg(long, default_value = "n1-release-proof-fragment")]
    fragment_key: String,
    #[arg(long, default_value = "")]
    fragment_name: String,
    #[arg(long, default_value_t = false)]
    cleanup_roots: bool,
    #[arg(long, default_value_t = false)]
    colocated_roots: bool,
    #[arg(long, default_value_t = 10_800)]
    job_timeout_seconds: u64,
    #[arg(long, default_value_t = 60)]
    trash_retention_seconds: u64,
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
    noa_api_proof: bool,
    #[arg(long, default_value_t = false)]
    backup_proof: bool,
    /// Offline byte-exact clone, same-path device cutover and missing-member reconstruction.
    #[arg(long, default_value_t = false)]
    pool_maintenance_proof: bool,
    /// Offline location changes without formatting, mounts or production services.
    #[arg(long, default_value_t = false)]
    pool_relocation_proof: bool,
    /// Optional previous engine used to seed absolute-address v4 objects.
    #[arg(long)]
    baseline_binary: Option<PathBuf>,
    /// Independent spare filesystem for member cutover or a 1+0 portable restore.
    #[arg(long)]
    spare_root: Option<PathBuf>,
    #[arg(long, default_value_t = 0)]
    pool_member: usize,
    #[arg(long, default_value_t = false)]
    flat_namespace_proof: bool,
    #[arg(long, default_value_t = false)]
    pool_expansion_proof: bool,
    #[arg(long, default_value_t = false)]
    native_transport_benchmark: bool,
    #[arg(long, default_value_t = false)]
    upload_benchmark: bool,
    #[arg(long, default_value_t = false)]
    concurrent_upload_proof: bool,
    #[arg(long, default_value_t = false)]
    export_proof: bool,
    #[arg(long, default_value_t = 64)]
    export_objects: usize,
    #[arg(long, default_value_t = 512 * 1024)]
    export_object_bytes: usize,
    #[arg(long, default_value_t = 8)]
    export_upload_concurrency: usize,
    #[arg(long, default_value_t = 60)]
    export_retention_seconds: u64,
    #[arg(long, default_value_t = false)]
    multipart_resume_proof: bool,
    #[arg(long, default_value_t = false)]
    auto_healing_proof: bool,
    #[arg(long, default_value_t = false)]
    trash_maintenance_proof: bool,
    #[arg(long, default_value_t = false)]
    maintenance_coverage_proof: bool,
    #[arg(long, default_value_t = false)]
    gc_safety_proof: bool,
    /// Aggregate mixed-workload deadline; per-request admission stays unchanged.
    #[arg(long, default_value_t = 300)]
    gc_workload_timeout_seconds: u64,
    #[arg(long, default_value_t = 1_000)]
    benchmark_iterations: usize,
    #[arg(long, default_value_t = 100)]
    benchmark_warmup: usize,
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    download_benchmark_bytes: usize,
    #[arg(long, default_value_t = 32 * 1024 * 1024)]
    download_benchmark_part_bytes: usize,
    #[arg(long, default_value_t = 5)]
    download_benchmark_iterations: usize,
    #[arg(long, default_value_t = 1)]
    download_benchmark_warmup: usize,
    #[arg(long, default_value_t = 3)]
    upload_benchmark_repeats: usize,
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    one_shot_benchmark_bytes: usize,
    #[arg(long, default_value_t = 128 * 1024 * 1024)]
    multipart_benchmark_bytes: usize,
    #[arg(long, default_value_t = 32 * 1024 * 1024)]
    multipart_benchmark_part_bytes: usize,
    #[arg(long, default_value_t = 0)]
    multipart_benchmark_concurrency: usize,
    #[arg(long, default_value_t = 2 * 1024 * 1024 * 1024)]
    multipart_resume_bytes: usize,
    #[arg(long, default_value_t = 32 * 1024 * 1024)]
    multipart_resume_part_bytes: usize,
    #[arg(long, default_value_t = 4)]
    multipart_resume_concurrency: usize,
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
    stable_fragment_id: String,
    token: String,
    operator_token: String,
    summary: Value,
    active_disk_count: usize,
    data_shards: u16,
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
        let data_shards = args.data_shards;
        let parity_shards = args.parity_shards;
        Ok(Self {
            args,
            stamp,
            client,
            base_url,
            index_roots: Vec::new(),
            blob_roots: Vec::new(),
            server: None,
            fragment: String::new(),
            stable_fragment_id: String::new(),
            token: String::new(),
            operator_token: String::new(),
            summary: json!({}),
            active_disk_count: 0,
            data_shards,
            parity_shards,
        })
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
