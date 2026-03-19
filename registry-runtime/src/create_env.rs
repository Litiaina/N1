use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

pub fn create_registry_env_file<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref();

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp_path = path.with_extension("tmp");
    let mut file = File::create(&tmp_path)?;

    writeln!(
        file,
        "# ───────────────── N2 REGISTRY ENV CONFIGURATION ─────────────────"
    )?;
    writeln!(file, "# Edit these values before starting the Registry.")?;
    writeln!(file)?;

    writeln!(file, "# Bind host")?;
    writeln!(file, "REGISTRY_HOST=0.0.0.0")?;
    writeln!(file)?;

    writeln!(file, "# HTTPS port")?;
    writeln!(file, "REGISTRY_HTTPS_PORT=51001")?;
    writeln!(file)?;

    writeln!(file, "# TLS certificate path")?;
    writeln!(file, "REGISTRY_CERT_PATH=certificates/cert.pem")?;
    writeln!(file)?;

    writeln!(file, "# TLS private key path")?;
    writeln!(file, "REGISTRY_KEY_PATH=certificates/key.pem")?;
    writeln!(file)?;

    writeln!(file, "# SQLite database path")?;
    writeln!(file, "REGISTRY_SQLITE_PATH=registry.db")?;
    writeln!(file)?;

    writeln!(file, "# JWT signing secret for Registry user sessions")?;
    writeln!(file, "REGISTRY_JWT_SECRET=")?;
    writeln!(file)?;

    writeln!(file, "# Registry master key for privileged admin operations")?;
    writeln!(file, "REGISTRY_MASTER_KEY=")?;
    writeln!(file)?;

    writeln!(file, "# Access-token TTL in seconds")?;
    writeln!(file, "REGISTRY_ACCESS_TOKEN_TTL_SECS=900")?;
    writeln!(file)?;

    writeln!(file, "# Default quota in bytes for new users")?;
    writeln!(file, "REGISTRY_DEFAULT_QUOTA_BYTES=26843545600")?;
    writeln!(file)?;

    writeln!(file, "# Registry-issued storage-session TTL in seconds")?;
    writeln!(file, "REGISTRY_STORAGE_SESSION_TTL_SECS=900")?;
    writeln!(file)?;

    writeln!(file, "# Default single-node storage endpoint for Drive brokering")?;
    writeln!(file, "REGISTRY_SINGLE_NODE_URL=")?;
    writeln!(file)?;

    writeln!(file, "# Master key used by Registry to mint fragment sessions against single-node N1")?;
    writeln!(file, "REGISTRY_SINGLE_NODE_MASTER_KEY=")?;
    writeln!(file)?;

    writeln!(file, "# Set true when the single-node backend uses a self-signed or locally-issued TLS cert")?;
    writeln!(file, "REGISTRY_SINGLE_NODE_INSECURE_TLS=false")?;
    writeln!(file)?;

    writeln!(file, "# Default multi-node storage endpoint for Drive brokering")?;
    writeln!(file, "REGISTRY_MULTI_NODE_URL=")?;
    writeln!(file)?;

    writeln!(file, "# Master key used by Registry to mint fragment sessions against multi-node N2")?;
    writeln!(file, "REGISTRY_MULTI_NODE_MASTER_KEY=")?;
    writeln!(file)?;

    writeln!(file, "# Set true when the multi-node backend uses a self-signed or locally-issued TLS cert")?;
    writeln!(file, "REGISTRY_MULTI_NODE_INSECURE_TLS=false")?;
    writeln!(file)?;

    writeln!(
        file,
        "# ──────────────────────────────────────────────────────────────────"
    )?;

    file.flush()?;
    drop(file);
    fs::rename(&tmp_path, path)?;
    Ok(())
}
