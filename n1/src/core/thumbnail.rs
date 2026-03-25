use crate::core::disk_manager::{ObjectMetadata, ObjectPathManager};
use crate::core::download::read_object_bytes_direct;
use crate::core::metadata_parser::{get_metadata, get_version_metadata};
use crate::enums::app_error_enum::AppError;
use crate::structs::auth_structs::Claims;
use axum::extract::{Path, Query};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use image::ColorType;
use image::ImageEncoder;
use image::codecs::jpeg::JpegEncoder;
use image::codecs::png::PngEncoder;
use image::imageops::FilterType;
use mime_guess::from_path;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::path::{Path as FsPath, PathBuf};
use tokio::fs;
use tracing::warn;
use ulid::Ulid;

const DEFAULT_THUMB_DIMENSION: u32 = 256;
const MAX_THUMB_DIMENSION: u32 = 2048;
const MAX_THUMB_SOURCE_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug, Deserialize, Clone)]
pub struct ThumbnailQuery {
    pub w: Option<u32>,
    pub h: Option<u32>,
    pub fit: Option<String>,
    pub format: Option<String>,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThumbnailFit {
    Contain,
    Cover,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThumbnailFormat {
    Jpeg,
    Png,
}

#[derive(Debug, Clone)]
struct ThumbnailProfile {
    width: u32,
    height: u32,
    fit: ThumbnailFit,
    format: ThumbnailFormat,
}

impl ThumbnailProfile {
    fn parse(query: &ThumbnailQuery) -> Result<Self, AppError> {
        let width = query.w.unwrap_or(DEFAULT_THUMB_DIMENSION);
        let height = query
            .h
            .unwrap_or(query.w.unwrap_or(DEFAULT_THUMB_DIMENSION));
        if width == 0 || height == 0 {
            return Err(AppError::BadRequest(
                "Thumbnail width and height must be greater than zero".into(),
            ));
        }
        if width > MAX_THUMB_DIMENSION || height > MAX_THUMB_DIMENSION {
            return Err(AppError::BadRequest(format!(
                "Thumbnail dimensions must be <= {}",
                MAX_THUMB_DIMENSION
            )));
        }

        let fit = match query.fit.as_deref().unwrap_or("contain") {
            "contain" => ThumbnailFit::Contain,
            "cover" => ThumbnailFit::Cover,
            other => {
                return Err(AppError::BadRequest(format!(
                    "Unsupported thumbnail fit '{}'",
                    other
                )));
            }
        };

        let format = match query.format.as_deref().unwrap_or("jpeg") {
            "jpeg" | "jpg" => ThumbnailFormat::Jpeg,
            "png" => ThumbnailFormat::Png,
            other => {
                return Err(AppError::BadRequest(format!(
                    "Unsupported thumbnail format '{}'",
                    other
                )));
            }
        };

        Ok(Self {
            width,
            height,
            fit,
            format,
        })
    }

    fn cache_stem(&self) -> String {
        let fit = match self.fit {
            ThumbnailFit::Contain => "contain",
            ThumbnailFit::Cover => "cover",
        };
        let format = match self.format {
            ThumbnailFormat::Jpeg => "jpeg",
            ThumbnailFormat::Png => "png",
        };
        format!("thumb-{}x{}-{}-{}", self.width, self.height, fit, format)
    }

    fn content_type(&self) -> &'static str {
        match self.format {
            ThumbnailFormat::Jpeg => "image/jpeg",
            ThumbnailFormat::Png => "image/png",
        }
    }
}

pub async fn get_object_thumbnail(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(query): Query<ThumbnailQuery>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let profile = ThumbnailProfile::parse(&query)?;
    let metadata =
        resolve_thumbnail_metadata(&fragment, &object_key, query.version_id.as_deref()).await?;

    if !looks_like_image(&metadata) {
        return Err(AppError::BadRequest(
            "Thumbnail generation only supports image objects".into(),
        ));
    }

    let cache_path = thumbnail_cache_file_for_metadata(&metadata, &profile)?;
    if let Ok(bytes) = fs::read(&cache_path).await {
        return Ok(thumbnail_response(
            bytes,
            profile.content_type(),
            &metadata.version_id,
            true,
        ));
    }

    let (_, source_bytes) = read_object_bytes_direct(
        &fragment,
        &object_key,
        Some(metadata.version_id.as_str()),
        MAX_THUMB_SOURCE_BYTES,
    )
    .await?;

    let thumb_bytes = render_thumbnail(&source_bytes, &profile)?;
    persist_thumbnail_cache(&cache_path, &thumb_bytes).await?;

    Ok(thumbnail_response(
        thumb_bytes,
        profile.content_type(),
        &metadata.version_id,
        false,
    ))
}

async fn resolve_thumbnail_metadata(
    fragment: &str,
    object_key: &str,
    version_id: Option<&str>,
) -> Result<ObjectMetadata, AppError> {
    let metadata = if let Some(version_id) = version_id {
        get_version_metadata(fragment, object_key, version_id, fragment).await?
    } else {
        get_metadata(fragment, object_key, fragment).await?
    };

    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }

    Ok(metadata)
}

fn looks_like_image(metadata: &ObjectMetadata) -> bool {
    if metadata.content_type.starts_with("image/") {
        return true;
    }

    from_path(&metadata.object_key)
        .first_raw()
        .map(|mime| mime.starts_with("image/"))
        .unwrap_or(false)
}

fn thumbnail_cache_file_for_metadata(
    metadata: &ObjectMetadata,
    profile: &ThumbnailProfile,
) -> Result<PathBuf, AppError> {
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    if blob_roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No blob storage configured".into(),
        ));
    }

    let mut hasher = Sha256::new();
    hasher.update(metadata.fragment.as_bytes());
    hasher.update([0]);
    hasher.update(metadata.version_id.as_bytes());
    hasher.update([0]);
    hasher.update(profile.cache_stem().as_bytes());
    let digest = hasher.finalize();
    let idx = ((usize::from(digest[0]) << 8) | usize::from(digest[1])) % blob_roots.len();
    let ext = match profile.format {
        ThumbnailFormat::Jpeg => "jpg",
        ThumbnailFormat::Png => "png",
    };

    Ok(blob_roots[idx]
        .join(&metadata.fragment)
        .join(".thumbs")
        .join(&metadata.version_id)
        .join(format!("{}.{}", profile.cache_stem(), ext)))
}

async fn persist_thumbnail_cache(path: &FsPath, bytes: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError("Thumbnail cache path has no parent".into())
    })?;
    fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, "create thumbnail cache directory".into()))?;

    let tmp_path = path.with_extension(format!(
        "{}.tmp-{}",
        path.extension().and_then(|v| v.to_str()).unwrap_or("bin"),
        Ulid::new()
    ));
    fs::write(&tmp_path, bytes)
        .await
        .map_err(|e| AppError::Io(e, "write thumbnail cache file".into()))?;

    if let Err(e) = fs::rename(&tmp_path, path).await {
        if fs::try_exists(path).await.unwrap_or(false) {
            let _ = fs::remove_file(&tmp_path).await;
            return Ok(());
        }
        let _ = fs::remove_file(&tmp_path).await;
        return Err(AppError::Io(e, "finalize thumbnail cache file".into()));
    }

    Ok(())
}

fn render_thumbnail(source_bytes: &[u8], profile: &ThumbnailProfile) -> Result<Vec<u8>, AppError> {
    let image = image::load_from_memory(source_bytes).map_err(|e| {
        warn!("Thumbnail decode failed: {}", e);
        AppError::BadRequest("Object is not a supported raster image".into())
    })?;

    let resized = match profile.fit {
        ThumbnailFit::Contain => image.resize(profile.width, profile.height, FilterType::Lanczos3),
        ThumbnailFit::Cover => {
            image.resize_to_fill(profile.width, profile.height, FilterType::Lanczos3)
        }
    };

    let mut encoded = Vec::new();
    match profile.format {
        ThumbnailFormat::Jpeg => {
            let rgb = resized.to_rgb8();
            let mut encoder = JpegEncoder::new_with_quality(&mut encoded, 82);
            encoder
                .encode(&rgb, rgb.width(), rgb.height(), ColorType::Rgb8.into())
                .map_err(|e| {
                    AppError::InternalServerError(format!("encode jpeg thumbnail: {}", e))
                })?;
        }
        ThumbnailFormat::Png => {
            let rgba = resized.to_rgba8();
            let encoder = PngEncoder::new(&mut encoded);
            encoder
                .write_image(&rgba, rgba.width(), rgba.height(), ColorType::Rgba8.into())
                .map_err(|e| {
                    AppError::InternalServerError(format!("encode png thumbnail: {}", e))
                })?;
        }
    }

    Ok(encoded)
}

fn thumbnail_response(
    bytes: Vec<u8>,
    content_type: &'static str,
    version_id: &str,
    cache_hit: bool,
) -> Response {
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header("x-n1-thumbnail-version", version_id)
        .header(
            "x-n1-thumbnail-cache",
            if cache_hit { "hit" } else { "miss" },
        )
        .body(axum::body::Body::from(bytes))
        .expect("thumbnail response");

    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=60"),
    );
    response
}
