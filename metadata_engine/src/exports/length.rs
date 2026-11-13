//! Account for the retained stored ZIP64 and GNU TAR encodings while capturing
//! immutable metadata. No payload reads, archive copy or inventory-sized index.
use super::models::ExportManifestEntry;
use crate::{Error, Result};

#[derive(Debug, Clone, Copy)]
pub(super) struct ManifestSummary {
    pub objects: u64,
    pub source_bytes: u64,
    tar_bytes: u64,
    zip_bytes: u64,
}

impl Default for ManifestSummary {
    fn default() -> Self {
        Self {
            objects: 0,
            source_bytes: 0,
            tar_bytes: 1024, // Two end-of-archive blocks.
            zip_bytes: 98,   // Forced ZIP64 end record, locator and ordinary end record.
        }
    }
}

fn overflow() -> Error {
    Error::Invalid("archive size exceeds the supported byte range".into())
}

fn padded_tar(bytes: u64) -> Result<u64> {
    bytes
        .checked_add(511)
        .map(|v| v / 512 * 512)
        .ok_or_else(overflow)
}

impl ManifestSummary {
    pub fn record(&mut self, entry: &ExportManifestEntry) -> Result<()> {
        let name = entry.archive_path.len() as u64;
        let mut tar = 512_u64
            .checked_add(padded_tar(entry.size_bytes)?)
            .ok_or_else(overflow)?;
        if name > 100 {
            // The retained GNU builder emits a long-name header and NUL-ended path.
            let long_name = padded_tar(name + 1)?;
            tar = tar
                .checked_add(512)
                .and_then(|v| v.checked_add(long_name))
                .ok_or_else(overflow)?;
        }
        // async_zip's stored stream: local header + ZIP64 size extra + 16-byte
        // descriptor + central header and ZIP64 sizes/offset extra. Names occur twice.
        let zip = entry
            .size_bytes
            .checked_add(140)
            .and_then(|v| v.checked_add(name.checked_mul(2)?))
            .ok_or_else(overflow)?;
        self.objects = self.objects.checked_add(1).ok_or_else(overflow)?;
        self.source_bytes = self
            .source_bytes
            .checked_add(entry.size_bytes)
            .ok_or_else(overflow)?;
        self.tar_bytes = self.tar_bytes.checked_add(tar).ok_or_else(overflow)?;
        self.zip_bytes = self.zip_bytes.checked_add(zip).ok_or_else(overflow)?;
        Ok(())
    }

    pub fn archive_bytes(&self, format: &str) -> u64 {
        if format == "tar" {
            self.tar_bytes
        } else {
            self.zip_bytes
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_zip::{Compression, ZipEntryBuilder, tokio::write::ZipFileWriter};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::compat::FuturesAsyncWriteCompatExt;

    async fn encoded_bytes(format: &'static str, entries: Vec<(String, Vec<u8>)>) -> u64 {
        let (mut reader, writer) = tokio::io::duplex(1024);
        let producer = tokio::spawn(async move {
            if format == "tar" {
                let mut tar = tokio_tar::Builder::new(writer);
                for (name, data) in entries {
                    let mut header = tokio_tar::Header::new_gnu();
                    header.set_size(data.len() as u64);
                    header.set_mode(0o644);
                    header.set_cksum();
                    tar.append_data(&mut header, name, data.as_slice())
                        .await
                        .unwrap();
                }
                tar.finish().await.unwrap();
            } else {
                let mut zip = ZipFileWriter::with_tokio(writer).force_zip64();
                for (name, data) in entries {
                    let mut entry = zip
                        .write_entry_stream(ZipEntryBuilder::new(name.into(), Compression::Stored))
                        .await
                        .unwrap();
                    (&mut entry).compat_write().write_all(&data).await.unwrap();
                    entry.close().await.unwrap();
                }
                zip.close().await.unwrap();
            }
        });
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        producer.await.unwrap();
        bytes.len() as u64
    }

    #[tokio::test]
    async fn lengths_match_the_retained_library_encodings() {
        for format in ["tar", "zip"] {
            let empty = ManifestSummary::default();
            assert_eq!(
                encoded_bytes(format, vec![]).await,
                empty.archive_bytes(format)
            );
            let mut summary = ManifestSummary::default();
            let mut entries = Vec::new();
            for (name, size) in [
                ("empty".to_owned(), 0),
                ("a".repeat(100), 1),
                ("b".repeat(101), 511),
                (format!("nested/{}/video.bin", "c".repeat(255)), 512),
                (format!("unicode/{}/file", "星".repeat(70)), 513),
                ("large.bin".to_owned(), 65537),
            ] {
                summary
                    .record(&ExportManifestEntry {
                        archive_path: name.clone(),
                        object_id: "object".into(),
                        version_id: "version".into(),
                        source_offset_bytes: 0,
                        size_bytes: size,
                    })
                    .unwrap();
                entries.push((name, vec![42; size as usize]));
            }
            assert_eq!(
                encoded_bytes(format, entries).await,
                summary.archive_bytes(format),
                "{format}"
            );
            assert_eq!(summary.objects, 6);
            assert_eq!(summary.source_bytes, 67074);
        }
    }

    #[test]
    fn length_accounting_supports_large_sources_and_rejects_overflow() {
        let mut summary = ManifestSummary::default();
        let mut entry = ExportManifestEntry {
            archive_path: "large".into(),
            object_id: "object".into(),
            version_id: "version".into(),
            source_offset_bytes: 0,
            size_bytes: 1_u64 << 40,
        };
        summary.record(&entry).unwrap();
        assert!(summary.archive_bytes("tar") > entry.size_bytes);
        assert!(summary.archive_bytes("zip") > entry.size_bytes);
        entry.size_bytes = u64::MAX;
        assert!(summary.record(&entry).is_err());
    }
}
