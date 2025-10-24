use aes_gcm::{
    Aes256Gcm,
    aead::{Aead, KeyInit, Nonce},
};
use bson::{Document, deserialize_from_slice, doc, serialize_to_vec};
use pbkdf2::pbkdf2_hmac;
use rand::{TryRngCore, rngs::OsRng};
use rpassword::read_password;
use sha2::Sha256;
use std::convert::TryInto;
use std::io::{self, ErrorKind, Write};
use std::path::Path;
use std::{env, fs};
use tokio::io::{AsyncBufReadExt, BufReader, stdin};
use tracing::{error, info};

use crate::constants::{NONCE_LEN, PBKDF2_ITERATIONS, SALT_LEN};
use crate::parser;

fn derive_key(password: &str, salt: &[u8]) -> [u8; 32] {
    let mut key = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, PBKDF2_ITERATIONS, &mut key);
    key
}

async fn get_user_input(prompt: &str) -> Result<String, io::Error> {
    print!("{}", prompt);
    io::stdout().flush()?;
    let mut input = String::new();
    BufReader::new(stdin()).read_line(&mut input).await?;
    Ok(input.trim().to_string())
}

pub async fn server_register() -> Result<(), io::Error> {
    let server_auth_file_path =
        env::var("SERVER_AUTH_PATH").unwrap_or_else(|_| "server_auth.bin".to_string());
    let server_registry_key =
        env::var("SERVER_AUTH_KEY").unwrap_or_else(|_| "default_key".to_string());

    match validate_server_key(&server_registry_key) {
        Ok(()) => {
            info!("╔═════════════════════════════════╗");
            info!("║         R E G I S T E R         ║");
            info!("╚═════════════════════════════════╝");

            let access_id = get_user_input("Access ID: ").await?;
            let password = get_user_input("Password: ").await?;

            let mut rng = OsRng;
            let mut salt = [0u8; SALT_LEN];
            rng.try_fill_bytes(&mut salt)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let key_bytes = derive_key(&password, &salt);
            let cipher = Aes256Gcm::new(&key_bytes.into());

            let user_doc = doc! { "access_id": &access_id };
            let bson_bytes =
                serialize_to_vec(&user_doc).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let mut nonce_bytes = [0u8; NONCE_LEN];
            rng.try_fill_bytes(&mut nonce_bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let nonce = Nonce::<Aes256Gcm>::from(nonce_bytes);
            let ciphertext = cipher
                .encrypt(&nonce, bson_bytes.as_ref())
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Encryption failed"))?;

            let mut file_bytes = Vec::with_capacity(SALT_LEN + NONCE_LEN + ciphertext.len());
            file_bytes.extend_from_slice(&salt);
            file_bytes.extend_from_slice(&nonce_bytes);
            file_bytes.extend_from_slice(&ciphertext);

            if let Some(parent) = Path::new(&server_auth_file_path).parent() {
                fs::create_dir_all(parent)?;
            }

            fs::write(&server_auth_file_path, &file_bytes)?;
            parser::clear_screen();
            info!("Registration complete. Please log in.");
            Ok(())
        }
        Err(e) => {
            error!("{}", e);
            panic!("{}", e);
        }
    }
}

pub async fn server_login() -> Result<String, io::Error> {
    let server_auth_file_path =
        env::var("SERVER_AUTH_PATH").unwrap_or_else(|_| "server_auth.bin".to_string());
    let auth_path = Path::new(&server_auth_file_path);

    if !auth_path.exists() {
        server_register().await?;
    }

    let file_bytes = fs::read(auth_path)?;
    if file_bytes.len() < SALT_LEN + NONCE_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Corrupted auth file",
        ));
    }

    let (salt, rest) = file_bytes.split_at(SALT_LEN);
    let (nonce_bytes, ciphertext) = rest.split_at(NONCE_LEN);

    info!("╔═══════════════════════════════╗");
    info!("║           L O G I N           ║");
    info!("╚═══════════════════════════════╝");

    let access_id = get_user_input("Access ID: ").await?;
    print!("Password: ");
    io::stdout().flush()?;
    let password = read_password()?;

    let key_bytes = derive_key(&password, salt);
    let cipher = Aes256Gcm::new(&key_bytes.into());

    let nonce_array: [u8; NONCE_LEN] = nonce_bytes
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid nonce length"))?;
    let nonce = Nonce::<Aes256Gcm>::from(nonce_array);

    let decrypted_bytes = cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid credentials"))?;

    let doc: Document = deserialize_from_slice(&decrypted_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Failed to parse auth file"))?;

    let stored_id = doc.get_str("access_id").unwrap_or_default();

    if access_id == stored_id {
        parser::clear_screen();
        Ok(access_id)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Invalid credentials",
        ))
    }
}

fn validate_server_key(server_registry_key: &str) -> Result<(), io::Error> {
    print!("Enter server registry key: ");
    io::stdout().flush()?;
    let server_key = read_password()?;
    if server_registry_key == server_key {
        Ok(())
    } else {
        Err(io::Error::new(
            ErrorKind::Other,
            "Invalid server registry key",
        ))
    }
}
