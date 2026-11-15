use super::Result;
use n1_native_client::{Command, N1NativeClient, NativeClientError, Response};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub const MIB: u64 = 1024 * 1024;
pub const PART: u64 = 8 * MIB;

#[derive(Clone)]
pub struct Api {
    pub url: String,
    pub tls: Arc<rustls::ClientConfig>,
    pub token: String,
}
impl Api {
    pub async fn connect(&self) -> Result<N1NativeClient> {
        let mut client =
            N1NativeClient::connect_with_tls(&self.url, Some(self.tls.clone())).await?;
        client.auth_bearer(self.token.clone()).await?;
        Ok(client)
    }
}
pub fn tls(path: &Path) -> Result<Arc<rustls::ClientConfig>> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_file_iter(path)? {
        roots.add(certificate?)?;
    }
    Ok(Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth(),
    ))
}
pub fn value(response: Response) -> Result<Value> {
    response
        .result
        .ok_or_else(|| "missing native result".into())
}
pub async fn begin(client: &mut N1NativeClient, size: u64, durable: bool) -> Result<String> {
    let reply = value(
        client
            .control(Command::UploadInit {
                pool: None,
                total_bytes: size,
                part_bytes: PART,
                durable,
                ttl_seconds: 3600,
            })
            .await?,
    )?;
    Ok(reply["object_key"]
        .as_str()
        .ok_or("missing LINK identity")?
        .into())
}
pub async fn absent(client: &mut N1NativeClient, key: &str) -> Result<()> {
    match client
        .control(Command::ObjectStat {
            object_key: key.into(),
        })
        .await
    {
        Err(NativeClientError::Native { code, .. }) if code == "404" => Ok(()),
        other => Err(format!("purged identity is not absent: {other:?}").into()),
    }
}
pub fn save(path: &Path, value: &Value) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}
pub fn file_sha(path: &Path) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}

#[derive(Clone)]
pub struct Payload {
    pub block: Arc<Vec<u8>>,
    pub bytes: u64,
}
impl Payload {
    pub fn new(seed: u64, bytes: u64) -> Self {
        let mut state = seed;
        let mut block = Vec::with_capacity(MIB as usize);
        while block.len() < MIB as usize {
            state = state.wrapping_add(0x9e3779b97f4a7c15);
            let mut word = state;
            word = (word ^ (word >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            word = (word ^ (word >> 27)).wrapping_mul(0x94d049bb133111eb);
            block.extend_from_slice(&(word ^ (word >> 31)).to_le_bytes());
        }
        Self {
            block: Arc::new(block),
            bytes,
        }
    }
    pub fn reader(&self, offset: u64, length: u64) -> Source {
        Source {
            block: self.block.clone(),
            offset: offset as usize % self.block.len(),
            remaining: length,
        }
    }
    pub fn sink(&self, offset: u64, length: u64) -> Verify {
        Verify {
            block: self.block.clone(),
            offset,
            remaining: length,
            received: 0,
            hash: Sha256::new(),
        }
    }
}
pub struct Source {
    block: Arc<Vec<u8>>,
    offset: usize,
    remaining: u64,
}
impl AsyncRead for Source {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let count = buffer
            .remaining()
            .min(self.remaining as usize)
            .min(self.block.len() - self.offset);
        buffer.put_slice(&self.block[self.offset..self.offset + count]);
        self.remaining -= count as u64;
        self.offset = (self.offset + count) % self.block.len();
        Poll::Ready(Ok(()))
    }
}
pub struct Verify {
    block: Arc<Vec<u8>>,
    offset: u64,
    remaining: u64,
    received: u64,
    hash: Sha256,
}
impl Verify {
    pub fn finish(self) -> Result<Value> {
        if self.remaining != 0 {
            return Err("truncated workload readback".into());
        }
        Ok(json!({"bytes":self.received,"sha256":format!("{:x}",self.hash.finalize())}))
    }
}
impl AsyncWrite for Verify {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if bytes.len() as u64 > self.remaining {
            return Poll::Ready(Err(std::io::Error::other(
                "readback exceeds requested range",
            )));
        }
        let mut used = 0;
        while used < bytes.len() {
            let at = self.offset as usize % self.block.len();
            let count = (bytes.len() - used).min(self.block.len() - at);
            if bytes[used..used + count] != self.block[at..at + count] {
                return Poll::Ready(Err(std::io::Error::other("readback content mismatch")));
            }
            self.offset += count as u64;
            used += count;
        }
        self.hash.update(bytes);
        self.received += bytes.len() as u64;
        self.remaining -= bytes.len() as u64;
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
pub async fn get(
    client: &mut N1NativeClient,
    key: &str,
    payload: &Payload,
    offset: u64,
    length: u64,
) -> Result<Value> {
    if offset
        .checked_add(length)
        .is_none_or(|end| end > payload.bytes)
    {
        return Err("requested workload range exceeds its source object".into());
    }
    let mut sink = payload.sink(offset, length);
    let (_, received) = client
        .download(key.into(), offset, Some(length), &mut sink)
        .await?;
    if received != length {
        return Err("native response length mismatch".into());
    }
    sink.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn verifier_rejects_corruption_truncation_and_excess_bytes() {
        let payload = Payload::new(17, 2 * MIB);
        let mut correct = payload.sink(MIB - 3, 7);
        let mut source = payload.reader(MIB - 3, 7);
        tokio::io::copy(&mut source, &mut correct).await.unwrap();
        assert_eq!(correct.finish().unwrap()["bytes"], 7);
        let mut corrupt = payload.sink(0, 1);
        assert!(corrupt.write_all(&[payload.block[0] ^ 1]).await.is_err());
        assert!(payload.sink(0, 1).finish().is_err());
        let mut extra = payload.sink(0, 0);
        assert!(extra.write_all(&[0]).await.is_err());
    }
}
