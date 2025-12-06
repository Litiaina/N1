use crate::settings::constants::{CONFIG_FILE, ENV_FILE};
use crate::settings::create_config::create_default_config_ini;
use crate::settings::create_env::create_env_file;
use std::path::Path;
use std::{fs, io};
use tracing::info;

pub fn ensure_env_file() -> io::Result<()> {
    let env_path = Path::new(ENV_FILE);

    if env_path.exists() {
        info!(".env file already exists — skipping creation.");
        return Ok(());
    }

    if let Some(parent) = env_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let temp_path = env_path.with_extension("tmp");
    create_env_file(&temp_path)?;

    if env_path.exists() {
        let _ = fs::remove_file(env_path);
    }

    fs::rename(&temp_path, &env_path)?;

    info!(
        "Created .env file at '{}'. Please configure it with your environment variables.",
        env_path.display()
    );

    Ok(())
}

pub fn ensure_config_file() -> io::Result<()> {
    let config_path = Path::new(CONFIG_FILE);

    if config_path.exists() {
        info!(
            "Configuration file '{}' already exists — skipping creation.",
            CONFIG_FILE
        );
        return Ok(());
    }

    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let temp_path = config_path.with_extension("tmp");
    create_default_config_ini(&temp_path)?;

    if config_path.exists() {
        let _ = fs::remove_file(config_path);
    }

    fs::rename(&temp_path, &config_path)?;
    info!(
        "Created default '{}' file with standard settings.",
        CONFIG_FILE
    );
    
    Ok(())
}
