use std::fs::File;
use std::io::{self, Write};

pub fn create_env_file(path: &str) -> io::Result<()> {
    let mut file = File::create(path)?;
    writeln!(file, "JWT_SECRET=enter_jwt_secret_here")?;
    writeln!(file, "URL_SIGNING_SECRET=enter_url_signing_secret_here")?;
    writeln!(file, "DATABASE_URL=mongodb://localhost:27017")?;
    Ok(())
}
