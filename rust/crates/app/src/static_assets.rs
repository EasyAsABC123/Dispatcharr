//! Captured pinned nginx/DRF static responses. Original authors retain credit.
//! File bodies use ServeDir's bounded streaming, with no second payload cache.
use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::Response,
};
use serde_json::Value;
use std::{collections::BTreeMap, path::Path, sync::Arc};

#[derive(Clone)]
struct Asset {
    mime: HeaderValue,
    modified: HeaderValue,
    etag: HeaderValue,
}
pub struct Assets {
    files: BTreeMap<String, Asset>,
    errors: BTreeMap<u16, (String, HeaderValue)>,
}
use crate::response_headers::SourceStaticResponse;

pub async fn load(root: &Path) -> Result<Option<Arc<Assets>>, Box<dyn std::error::Error>> {
    if !root.join("rest_framework").is_dir() {
        return Ok(None);
    }
    // Immutable bundled assets only. Custom static trees retain their existing
    // ServeDir behavior when the captured distribution directory is absent.
    let source: Value =
        serde_json::from_str(include_str!("../../../runtime/drf-static-source-1.json"))?;
    let mut files = BTreeMap::new();
    for file in source["files"]
        .as_array()
        .ok_or("Static inventory missing")?
    {
        let path = file["path"].as_str().ok_or("Static path missing")?;
        let metadata = tokio::fs::metadata(root.join("rest_framework").join(path)).await?;
        if !metadata.is_file()
            || Some(metadata.len()) != file["bytes"].as_u64()
            || metadata
                .modified()?
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
                != file["mtime_ns"].as_u64().ok_or("Static mtime missing")? as u128
        {
            return Err("Pinned static metadata changed".into());
        }
        files.insert(
            format!("/rest_framework/{path}"),
            Asset {
                mime: HeaderValue::from_str(file["mime"].as_str().ok_or("Static MIME missing")?)?,
                modified: HeaderValue::from_str(
                    file["last_modified"]
                        .as_str()
                        .ok_or("Static date missing")?,
                )?,
                etag: HeaderValue::from_str(file["etag"].as_str().ok_or("Static etag missing")?)?,
            },
        );
    }
    let mut errors = BTreeMap::new();
    for (status, page) in source["errors"]
        .as_object()
        .ok_or("Static errors missing")?
    {
        errors.insert(
            status.parse()?,
            (
                page["body"]
                    .as_str()
                    .ok_or("Static error body missing")?
                    .to_owned(),
                HeaderValue::from_str(
                    page["content_type"]
                        .as_str()
                        .ok_or("Static error MIME missing")?,
                )?,
            ),
        );
    }
    Ok(Some(Arc::new(Assets { files, errors })))
}

fn tagged(mut response: Response) -> Response {
    response.extensions_mut().insert(SourceStaticResponse);
    response
}
fn not_modified(asset: &Asset) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NOT_MODIFIED;
    response
        .headers_mut()
        .insert(header::ETAG, asset.etag.clone());
    response
        .headers_mut()
        .insert(header::LAST_MODIFIED, asset.modified.clone());
    tagged(response)
}
fn error_page(
    state: &Assets,
    status: StatusCode,
    head: bool,
    range: Option<HeaderValue>,
) -> Response {
    let Some((body, mime)) = state.errors.get(&status.as_u16()) else {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = status;
        return tagged(response);
    };
    let mut response = Response::new(if head {
        Body::empty()
    } else {
        Body::from(body.clone())
    });
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, mime.clone());
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&body.len().to_string()).unwrap(),
    );
    if let Some(range) = range {
        response.headers_mut().insert(header::CONTENT_RANGE, range);
    }
    tagged(response)
}

pub async fn apply(State(state): State<Arc<Assets>>, mut request: Request, next: Next) -> Response {
    let path = request.uri().path();
    let head = request.method() == Method::HEAD;
    if path.to_ascii_lowercase().contains("%00") {
        return error_page(&state, StatusCode::BAD_REQUEST, head, None);
    }
    if request.method() != Method::GET && !head {
        return error_page(&state, StatusCode::METHOD_NOT_ALLOWED, head, None);
    }
    if path == "/rest_framework/" {
        return error_page(&state, StatusCode::FORBIDDEN, head, None);
    }
    let asset = state.files.get(path).cloned();
    if let Some(asset) = &asset {
        if request
            .headers()
            .get(header::IF_NONE_MATCH)
            .and_then(|h| h.to_str().ok())
            .is_some_and(|h| {
                h.split(',').any(|tag| {
                    tag.trim() == "*"
                        || tag.trim().trim_start_matches("W/") == asset.etag.to_str().unwrap()
                })
            })
        {
            return not_modified(asset);
        }
        if let Some(value) = request
            .headers()
            .get(header::IF_RANGE)
            .and_then(|h| h.to_str().ok())
        {
            // ServeDir has no entity tag. Translate only its captured tag branch;
            // date validators continue through its existing filesystem logic.
            if value.starts_with('"') || value.starts_with("W/") {
                if value != asset.etag.to_str().unwrap() {
                    request.headers_mut().remove(header::RANGE);
                }
                request.headers_mut().remove(header::IF_RANGE);
            }
        }
    }
    let mut response = next.run(request).await;
    let status = response.status();
    if status.is_client_error() {
        let range = response.headers().get(header::CONTENT_RANGE).cloned();
        return error_page(&state, status, head, range);
    }
    if let Some(asset) = asset {
        if matches!(
            status,
            StatusCode::OK | StatusCode::PARTIAL_CONTENT | StatusCode::NOT_MODIFIED
        ) {
            response.headers_mut().insert(header::ETAG, asset.etag);
            response
                .headers_mut()
                .insert(header::LAST_MODIFIED, asset.modified);
            if status != StatusCode::NOT_MODIFIED {
                response
                    .headers_mut()
                    .insert(header::CONTENT_TYPE, asset.mime);
            } else {
                response.headers_mut().remove(header::CONTENT_TYPE);
                response.headers_mut().remove(header::CONTENT_LENGTH);
            }
            if status != StatusCode::OK {
                response.headers_mut().remove(header::ACCEPT_RANGES);
            }
        }
    }
    tagged(response)
}
