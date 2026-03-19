use std::time::{SystemTime, UNIX_EPOCH};

use base32::Alphabet;
use base64::Engine;
use hmac::{Hmac, Mac};
use image::Luma;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use qrcode::QrCode;
use rand::RngCore;
use sha1::Sha1;

type HmacSha1 = Hmac<Sha1>;

const TOTP_STEP_SECS: i64 = 30;
const TOTP_DIGITS: u32 = 6;

pub fn generate_secret() -> String {
    let mut bytes = [0u8; 20];
    rand::thread_rng().fill_bytes(&mut bytes);
    base32::encode(Alphabet::Rfc4648 { padding: false }, &bytes)
}

pub fn verify_totp(secret: &str, user_otp: &str, window: i64) -> bool {
    let trimmed = user_otp.trim();
    if trimmed.len() != TOTP_DIGITS as usize || !trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return false;
    }

    let secret = secret.trim().replace('=', "");
    let key = match base32::decode(Alphabet::Rfc4648 { padding: false }, &secret) {
        Some(bytes) if !bytes.is_empty() => bytes,
        _ => return false,
    };

    let counter = current_counter();
    (-window..=window)
        .map(|offset| generate_totp_from_counter(&key, counter + offset))
        .any(|candidate| candidate == trimmed)
}

pub fn otpauth_uri(email: &str, secret: &str) -> String {
    let account = utf8_percent_encode(email.trim(), NON_ALPHANUMERIC).to_string();
    let issuer = utf8_percent_encode("Litiaina Registry", NON_ALPHANUMERIC).to_string();
    format!(
        "otpauth://totp/{}:{}?secret={}&issuer={}&algorithm=SHA1&digits={}&period={}",
        issuer,
        account,
        secret.trim(),
        issuer,
        TOTP_DIGITS,
        TOTP_STEP_SECS
    )
}

pub fn qr_data_url(payload: &str) -> Result<String, String> {
    let code = QrCode::new(payload.as_bytes()).map_err(|err| err.to_string())?;
    let image = code.render::<Luma<u8>>().min_dimensions(256, 256).build();
    let mut bytes = Vec::new();
    let dynamic = image::DynamicImage::ImageLuma8(image);
    dynamic
        .write_to(
            &mut std::io::Cursor::new(&mut bytes),
            image::ImageFormat::Png,
        )
        .map_err(|err| err.to_string())?;
    Ok(format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(bytes)
    ))
}

fn generate_totp_from_counter(key: &[u8], counter: i64) -> String {
    let mut msg = [0u8; 8];
    msg.copy_from_slice(&(counter as u64).to_be_bytes());

    let mut mac = match HmacSha1::new_from_slice(key) {
        Ok(value) => value,
        Err(_) => return "000000".to_string(),
    };
    mac.update(&msg);
    let hash = mac.finalize().into_bytes();

    let offset = (hash[hash.len() - 1] & 0x0f) as usize;
    let part = &hash[offset..offset + 4];
    let truncated = ((u32::from(part[0]) & 0x7f) << 24)
        | (u32::from(part[1]) << 16)
        | (u32::from(part[2]) << 8)
        | u32::from(part[3]);
    let code = truncated % 10u32.pow(TOTP_DIGITS);
    format!("{:0width$}", code, width = TOTP_DIGITS as usize)
}

fn current_counter() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs() as i64 / TOTP_STEP_SECS)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{generate_secret, otpauth_uri, qr_data_url, verify_totp};

    #[test]
    fn generated_secret_is_base32ish() {
        let secret = generate_secret();
        assert!(secret.len() >= 16);
        assert!(secret.chars().all(|ch| matches!(ch, 'A'..='Z' | '2'..='7')));
    }

    #[test]
    fn qr_data_url_is_png() {
        let payload = otpauth_uri("user@example.com", "JBSWY3DPEHPK3PXP");
        let data_url = qr_data_url(&payload).expect("qr data url");
        assert!(data_url.starts_with("data:image/png;base64,"));
    }

    #[test]
    fn invalid_code_does_not_verify() {
        assert!(!verify_totp("JBSWY3DPEHPK3PXP", "abcdef", 1));
    }
}
