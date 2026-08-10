use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

use tracing::info;

pub fn create_env_file<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref();

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp_path = path.with_extension("tmp");
    let mut file = File::create(&tmp_path)?;

    writeln!(
        file,
        "# ───────────────── N1 ENV CONFIGURATION ─────────────────"
    )?;
    writeln!(file, "# Edit these values before starting the server.")?;
    writeln!(file)?;

    writeln!(file, "# Secret key used for signing JWT tokens")?;
    writeln!(file, "JWT_SECRET=")?;
    writeln!(file)?;

    writeln!(
        file,
        "# Default access-token TTL in seconds (default runtime: 900 / 15 minutes)"
    )?;
    writeln!(file, "ACCESS_TOKEN_DEFAULT_TTL_SECS=900")?;
    writeln!(file)?;

    writeln!(
        file,
        "# Maximum access-token TTL in seconds even if clients request more (default runtime: 3600 / 1 hour)"
    )?;
    writeln!(file, "ACCESS_TOKEN_MAX_TTL_SECS=3600")?;
    writeln!(file)?;

    writeln!(
        file,
        "# Refresh-token TTL in seconds (default runtime: 604800 / 7 days)"
    )?;
    writeln!(file, "REFRESH_TOKEN_TTL_SECS=604800")?;
    writeln!(file)?;

    writeln!(file, "# Storage authentication key")?;
    writeln!(file, "AUTH_KEY=")?;
    writeln!(file)?;

    writeln!(file, "# Server authentication key")?;
    writeln!(file, "MASTER_KEY=")?;
    writeln!(file)?;

    writeln!(file, "# Admin access key")?;
    writeln!(file, "ADMIN_KEY=")?;
    writeln!(file)?;

    writeln!(
        file,
        "# ─────────────────────────────────────────────────────────"
    )?;

    file.flush()?;
    drop(file);

    fs::rename(&tmp_path, &path)?;

    info!(
        "Created new .env file at '{}'. Please configure it before running the server.",
        path.display()
    );

    Ok(())
}
