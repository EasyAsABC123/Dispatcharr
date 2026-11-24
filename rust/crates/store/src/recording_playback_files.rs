//! Original RecordingViewSet disk/HLS response behavior; full author credit is
//! retained by Dispatcharr contributors. No playback converter is spawned.
use super::recording_playback::Owner;
use crate::{AuthService, StoreError};
use axum::{
    body::{Body, Bytes},
    http::{HeaderValue, StatusCode},
    response::Response,
};
use dispatcharr_contracts::PlaybackRequest;
use num_bigint::BigInt;
use serde_json::{json, Value};
use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};
struct Frame {
    bytes: Vec<u8>,
    _owner: Arc<Owner>,
}
impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
fn fault() -> StoreError {
    StoreError::UnhandledRestFault
}
fn missing(message: impl Into<String>) -> StoreError {
    StoreError::StatusBody(404, json!({"detail":message.into()}))
}
async fn safe(raw: Option<&Value>) -> Option<PathBuf> {
    let raw = raw?.as_str()?;
    if !raw.starts_with("/data") {
        return None;
    }
    let root = tokio::fs::canonicalize("/data/recordings").await.ok()?;
    let path = tokio::fs::canonicalize(raw).await.ok()?;
    path.starts_with(root).then_some(path)
}
fn header(response: &mut Response, key: &'static str, value: &str) -> Result<(), StoreError> {
    response
        .headers_mut()
        .insert(key, HeaderValue::from_str(value).map_err(|_| fault())?);
    Ok(())
}
fn redirect(location: String) -> Result<Response, StoreError> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::FOUND;
    header(&mut response, "location", &location)?;
    header(&mut response, "content-type", "text/html; charset=utf-8")?;
    Ok(response)
}
/// Match Python int(), including signed/unicode/underscore literals and its
/// decimal digit cap. The Source suffix interpretation intentionally stays 0..N.
fn integer(raw: &str) -> Option<BigInt> {
    let text = dispatcharr_ingest_core::source_integer_literal(raw)?;
    BigInt::parse_bytes(text.as_bytes(), 10)
}
fn range(raw: &str, size: u64) -> Option<(BigInt, BigInt, BigInt)> {
    let raw = raw.trim().strip_prefix("bytes=")?;
    let (start, end) = raw.split_once('-')?;
    let start = if start.is_empty() {
        BigInt::from(0)
    } else {
        integer(start)?
    }
    .max(BigInt::from(0));
    let last: BigInt = BigInt::from(size) - 1;
    let end = if end.is_empty() {
        last.clone()
    } else {
        integer(end)?
    }
    .min(last);
    let length = &end - &start + 1;
    Some((start, end, length))
}
async fn stream(
    mut file: File,
    start: u64,
    length: u64,
    head: bool,
    owner: Arc<Owner>,
) -> Result<Body, StoreError> {
    file.seek(io::SeekFrom::Start(start))
        .await
        .map_err(|_| fault())?;
    if head {
        return Ok(Body::empty());
    }
    let stream = async_stream::try_stream! {
        let retained=owner;
        let mut left=length;
        while left>0 {
            let mut bytes=vec![0u8;(left.min(8192)) as usize];
            let count=file.read(&mut bytes).await?;
            if count==0 { break; }
            left-=count as u64; bytes.truncate(count);
            yield Bytes::from_owner(Frame { bytes, _owner:retained.clone() });
        }
    };
    let stream: std::pin::Pin<
        Box<dyn futures_util::Stream<Item = Result<Bytes, io::Error>> + Send>,
    > = Box::pin(stream);
    Ok(Body::from_stream(stream))
}
pub(super) async fn file(
    properties: &Value,
    identity: &PlaybackRequest,
    base: &str,
    suffix: &str,
    head: bool,
    owner: Arc<Owner>,
) -> Result<Response, StoreError> {
    let path = safe(properties.get("file_path")).await;
    let size = match path.as_ref() {
        Some(path) => tokio::fs::metadata(path).await.ok().map(|v| v.len()),
        None => None,
    };
    if size.is_none() || size == Some(0) {
        if let Some(hls) = safe(properties.get("_hls_dir")).await {
            if tokio::fs::metadata(hls).await.is_ok_and(|m| m.is_dir()) {
                return redirect(format!("{base}/hls/index.m3u8{suffix}"));
            }
        }
        if size.is_none() {
            return Err(missing("Recording file not found"));
        }
    }
    let path = path.ok_or_else(|| missing("Recording file not found"))?;
    let input = File::open(&path).await.map_err(|_| fault())?;
    // Use this owned descriptor for metadata and bytes; subsequent unlink cannot
    // switch the reader to an unrelated file and never reacquires a pathname.
    let size = input.metadata().await.map_err(|_| fault())?.len();
    let name = properties
        .get("file_name")
        .filter(|v| crate::channels::truthy(Some(v)))
        .map(|v| match v.as_str() {
            Some(v) => Ok(v.to_owned()),
            None => crate::python_action::collection_text(v, 65536).ok_or_else(fault),
        })
        .transpose()?
        .unwrap_or_else(|| "recording".into());
    let mime = match path
        .extension()
        .and_then(|v| v.to_str())
        .unwrap_or("")
        .to_ascii_lowercase()
        .as_str()
    {
        "mp4" => "video/mp4".to_owned(),
        "mkv" => "video/x-matroska".to_owned(),
        _ => mime_guess::from_path(&path)
            .first_or_octet_stream()
            .to_string(),
    };
    let requested = identity.header("range").and_then(|v| range(v, size));
    let (status, start, length, content_range) = match requested {
        Some((start, end, length)) => (
            StatusCode::PARTIAL_CONTENT,
            start.to_string().parse::<u64>().unwrap_or(size),
            length.to_string(),
            Some(format!("bytes {start}-{end}/{size}")),
        ),
        None => (StatusCode::OK, 0, size.to_string(), None),
    };
    let remaining = length.parse::<u64>().unwrap_or(0);
    let mut response = Response::new(stream(input, start, remaining, head, owner).await?);
    *response.status_mut() = status;
    header(&mut response, "content-type", &mime)?;
    header(&mut response, "content-length", &length)?;
    header(&mut response, "accept-ranges", "bytes")?;
    response
        .headers_mut()
        .insert("content-disposition", inline_file_disposition(&name)?);
    if let Some(range) = content_range {
        header(&mut response, "content-range", &range)?;
    }
    Ok(response)
}
pub(super) async fn hls(
    auth: &AuthService,
    id: &str,
    properties: &Value,
    segment: &str,
    urls: (&str, &str),
    head: bool,
    owner: Arc<Owner>,
) -> Result<Response, StoreError> {
    let (base, suffix) = urls;
    let directory = safe(properties.get("_hls_dir")).await;
    let directory = match directory {
        Some(path) if tokio::fs::metadata(&path).await.is_ok_and(|m| m.is_dir()) => Some(path),
        _ => None,
    };
    let Some(directory) = directory else {
        if segment.ends_with(".m3u8") {
            if let Some(file) = safe(properties.get("file_path")).await {
                if tokio::fs::metadata(file).await.is_ok_and(|m| m.len() > 0) {
                    return redirect(format!("{base}/file/{suffix}"));
                }
            }
        }
        return Err(missing("HLS content not available for this recording"));
    };
    // Resolve the real existing prefix too, so traversal is forbidden even when
    // its target does not exist (Source checks containment before isfile).
    let target = resolve_prefix(&directory.join(segment))
        .await
        .map_err(|_| fault())?;
    if !target.starts_with(&directory) {
        return Err(StoreError::Forbidden);
    }
    if !tokio::fs::metadata(&target)
        .await
        .is_ok_and(|m| m.is_file())
    {
        return Err(missing(format!("HLS file not found: {segment}")));
    }
    if segment.ends_with(".m3u8") {
        let mut input = File::open(target)
            .await
            .map_err(|_| fault())?
            .take(1024 * 1024 + 1);
        let mut bytes = Vec::new();
        input.read_to_end(&mut bytes).await.map_err(|_| fault())?;
        if bytes.len() > 1024 * 1024 {
            return Err(StoreError::StatusBody(
                503,
                json!({"detail":"Recording playlist capacity unavailable."}),
            ));
        }
        let text = String::from_utf8(bytes)
            .map_err(|_| fault())?
            .replace("\r\n", "\n")
            .replace('\r', "\n");
        let mut rewritten = String::new();
        for line in text.split_inclusive('\n') {
            let stripped = line.trim();
            let additional = if !stripped.is_empty() && !stripped.starts_with('#') {
                base.len() + 5 + stripped.len() + suffix.len() + 1
            } else {
                line.len()
            };
            if rewritten.len().saturating_add(additional) > 4 * 1024 * 1024 {
                return Err(StoreError::StatusBody(
                    503,
                    json!({"detail":"Recording playlist response capacity unavailable."}),
                ));
            }
            if !stripped.is_empty() && !stripped.starts_with('#') {
                rewritten.push_str(&format!("{base}/hls/{stripped}{suffix}\n"));
            } else {
                rewritten.push_str(line);
            }
        }
        let length = rewritten.len();
        let mut response = Response::new(if head {
            Body::empty()
        } else {
            Body::from(Bytes::from_owner(Frame {
                bytes: rewritten.into_bytes(),
                _owner: owner,
            }))
        });
        header(&mut response, "content-type", "application/x-mpegURL")?;
        header(&mut response, "cache-control", "no-cache")?;
        header(&mut response, "content-length", &length.to_string())?;
        return Ok(response);
    }
    if segment.ends_with(".ts") {
        // Original best-effort viewer heartbeat, shared production cache and
        // retained physical reply scope. Cache failure never denies the segment.
        if let Some(cache) = auth.notification_cache.as_ref() {
            let mut cmd = redis::cmd("SET");
            cmd.arg(format!("dvr:hls_viewer:{id}"))
                .arg("1")
                .arg("EX")
                .arg(20);
            let _ = cache.command::<()>(cmd).await;
        }
        let name = target
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("")
            .to_owned();
        let input = File::open(target).await.map_err(|_| fault())?;
        let size = input.metadata().await.map_err(|_| fault())?.len();
        let mut response = Response::new(stream(input, 0, size, head, owner).await?);
        header(&mut response, "content-type", "video/mp2t")?;
        if !name.is_empty() {
            header(
                &mut response,
                "content-disposition",
                &file_disposition(&name),
            )?;
        }
        header(&mut response, "content-length", &size.to_string())?;
        Ok(response)
    } else {
        Err(missing("Unsupported HLS file type"))
    }
}
async fn resolve_prefix(path: &Path) -> io::Result<PathBuf> {
    use std::{collections::VecDeque, ffi::OsString};
    let mut pending: VecDeque<OsString> = path
        .components()
        .map(|v| v.as_os_str().to_os_string())
        .collect();
    let mut current = PathBuf::new();
    let mut links = 0;
    while let Some(part) = pending.pop_front() {
        if part == "/" {
            current = PathBuf::from("/");
            continue;
        }
        if part == "." {
            continue;
        }
        if part == ".." {
            current.pop();
            continue;
        }
        current.push(&part);
        match tokio::fs::symlink_metadata(&current).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                links += 1;
                if links > 40 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "recording symlink loop",
                    ));
                }
                let target = tokio::fs::read_link(&current).await?;
                current.pop();
                for next in target.components().rev() {
                    pending.push_front(next.as_os_str().to_os_string());
                }
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(current)
}

fn inline_file_disposition(name: &str) -> Result<HeaderValue, StoreError> {
    crate::backup_files::django_file_header(&format!("inline; filename=\"{name}\""))
        .ok_or_else(fault)
}

fn file_disposition(name: &str) -> String {
    if name.is_ascii() {
        format!(
            "inline; filename=\"{}\"",
            name.replace('\\', "\\\\").replace('\"', "\\\"")
        )
    } else {
        let mut quoted = String::new();
        for byte in name.as_bytes() {
            if byte.is_ascii_alphanumeric() || b"_.-~".contains(byte) {
                quoted.push(*byte as char);
            } else {
                quoted.push_str(&format!("%{byte:02X}"));
            }
        }
        format!("inline; filename*=utf-8''{quoted}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn completed_files_keep_django_inline_filename_wire_encoding() {
        for (name, expected) in [
            ("News.mkv", b"inline; filename=\"News.mkv\"".as_slice()),
            ("Café.mkv", b"inline; filename=\"Caf\xe9.mkv\"".as_slice()),
            (
                "東京.mkv",
                b"=?utf-8?b?aW5saW5lOyBmaWxlbmFtZT0i5p2x5LqsLm1rdiI=?=".as_slice(),
            ),
        ] {
            assert_eq!(inline_file_disposition(name).unwrap().as_bytes(), expected);
        }
        assert_eq!(
            file_disposition("東京.ts"),
            "inline; filename*=utf-8''%E6%9D%B1%E4%BA%AC.ts"
        );
    }

    #[test]
    fn original_ranges_keep_suffix_and_malformed_fallback_semantics() {
        let parsed = range("bytes=-5", 20).unwrap();
        assert_eq!(
            (
                parsed.0.to_string(),
                parsed.1.to_string(),
                parsed.2.to_string()
            ),
            ("0".into(), "5".into(), "6".into())
        );
        let parsed = range("bytes=18-100", 20).unwrap();
        assert_eq!(
            (
                parsed.0.to_string(),
                parsed.1.to_string(),
                parsed.2.to_string()
            ),
            ("18".into(), "19".into(), "2".into())
        );
        assert!(range("bytes=0-1,3-4", 20).is_none());
        assert!(range("Bytes=0-1", 20).is_none());
        let parsed = range("bytes=+1_0-", 20).unwrap();
        assert_eq!(parsed.0.to_string(), "10");
        let parsed = range("bytes=100-", 20).unwrap();
        assert_eq!(parsed.2.to_string(), "-80");
    }
}
