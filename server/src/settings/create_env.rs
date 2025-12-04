use std::fs::File;
use std::io::{self, Write};

pub fn create_env_file(path: &str) -> io::Result<()> {
    let mut file = File::create(path)?;

    // DATABASE
    writeln!(file, "# ==========================")?;
    writeln!(file, "# DATABASE")?;
    writeln!(file, "# ==========================")?;
    writeln!(file, "DATABASE_URL=mongodb://localhost:27017")?;
    writeln!(file)?;

    // SERVER AUTH
    writeln!(file, "# ==========================")?;
    writeln!(file, "# SERVER AUTH")?;
    writeln!(file, "# ==========================")?;
    writeln!(file, "SERVER_AUTH_KEY=enter_server_auth_key_here")?;
    writeln!(file, "SERVER_AUTH_PATH=userdata/server/admin/key")?;
    writeln!(file)?;

    // SECURITY
    writeln!(file, "# ==========================")?;
    writeln!(file, "# SECURITY")?;
    writeln!(file, "# ==========================")?;
    writeln!(file, "JWT_SECRET=enter_jwt_secret_here")?;
    writeln!(file, "URL_SIGNING_SECRET=enter_url_signing_secret_here")?;
    writeln!(file)?;

    Ok(())
}
