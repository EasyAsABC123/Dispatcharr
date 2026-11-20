//! Original Dispatcharr LogoViewSet/serializer URL and upload mutations.
//! Original contributors retain full credit; see ATTRIBUTION.md.
use crate::{admin_writes::character, AuthService, StoreError};
use axum::{
    extract::{ConnectInfo, FromRequest, Multipart, OriginalUri, Path, Request, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Map, Value};
use std::{
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
    time::UNIX_EPOCH,
};
const FILE_LIMIT: usize = 5 * 1024 * 1024;
const REQUEST_LIMIT: usize = 8 * 1024 * 1024;
struct Upload {
    filename: String,
    mime: String,
    bytes: Vec<u8>,
    size: usize,
}
struct Input {
    fields: Map<String, Value>,
    file: Option<Upload>,
}
fn invalid(field: &str, message: &str) -> StoreError {
    StoreError::InvalidInput(json!({field:[message]}))
}
fn error(message: &str) -> StoreError {
    StoreError::InvalidInput(json!({"error":message}))
}
fn parser(message: &str) -> StoreError {
    StoreError::InvalidInput(json!({"detail":message}))
}
async fn input(request: Request, upload_only: bool) -> Result<Input, StoreError> {
    let mime = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_owned();
    let mut fields = Map::new();
    let mut file = None;
    if mime == "multipart/form-data" {
        let mut multipart = Multipart::from_request(request, &()).await.map_err(|_| {
            parser("Multipart form parse error - Invalid boundary in multipart: None")
        })?;
        let mut total = 0usize;
        let mut parts = 0usize;
        while let Some(mut part) = multipart
            .next_field()
            .await
            .map_err(|_| parser("Multipart form parse error"))?
        {
            parts += 1;
            if parts > 1100 {
                return Err(parser("Too many upload fields."));
            }
            let name = part.name().unwrap_or("").to_owned();
            let filename = part.file_name().map(str::to_owned);
            let mime = part
                .content_type()
                .unwrap_or("")
                .split(';')
                .next()
                .unwrap_or("")
                .to_owned();
            let mut bytes = Vec::new();
            let mut size = 0usize;
            while let Some(chunk) = part
                .chunk()
                .await
                .map_err(|_| parser("Multipart form parse error"))?
            {
                total = total.saturating_add(chunk.len());
                size = size.saturating_add(chunk.len());
                if total > REQUEST_LIMIT {
                    return Err(parser("Upload request exceeds bounded service limit."));
                }
                if size <= FILE_LIMIT || filename.is_none() {
                    crate::image_proxy::append_bounded(
                        &mut bytes,
                        &chunk,
                        if filename.is_some() {
                            FILE_LIMIT
                        } else {
                            REQUEST_LIMIT
                        },
                    );
                }
            }
            if let Some(filename) = filename.filter(|v| !v.is_empty()) {
                if name == "file" {
                    if let Some(filename) = sanitize_filename(&filename) {
                        file = Some(Upload {
                            filename,
                            mime,
                            bytes,
                            size,
                        });
                    }
                }
            } else {
                fields.insert(
                    name,
                    Value::String(String::from_utf8_lossy(&bytes).into_owned()),
                );
            }
        }
    } else {
        let raw = axum::body::to_bytes(request.into_body(), REQUEST_LIMIT)
            .await
            .map_err(|_| parser("Request exceeds bounded service limit."))?;
        if raw.is_empty() {
            return Ok(Input { fields, file });
        }
        if mime == "application/json" {
            let value: Value = serde_json::from_slice(&raw).map_err(|e| {
                parser(&format!(
                    "JSON parse error - {}",
                    crate::startup_bootstrap::json_error(&raw, e)
                ))
            })?;
            if !upload_only && value.is_null() {
                return Err(StoreError::InvalidInput(
                    json!({"non_field_errors":["No data provided"]}),
                ));
            }
            fields = if upload_only {
                value.as_object().cloned().unwrap_or_default()
            } else {
                value.as_object().cloned().ok_or_else(||StoreError::InvalidInput(json!({"non_field_errors":[format!("Invalid data. Expected a dictionary, but got {}.",crate::channel_memberships::datatype(&value))]})))?
            };
        } else if mime == "application/x-www-form-urlencoded" {
            for (key, value) in url::form_urlencoded::parse(&raw) {
                fields.insert(key.into_owned(), json!(value));
            }
        } else {
            return Err(StoreError::StatusBody(
                415,
                json!({"detail":format!("Unsupported media type \"{mime}\" in request.")}),
            ));
        }
    }
    Ok(Input { fields, file })
}
pub(crate) async fn mutation_fields(
    request: Request,
) -> Result<(Map<String, Value>, bool), StoreError> {
    let html = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.starts_with("multipart/form-data")
                || v.starts_with("application/x-www-form-urlencoded")
        });
    Ok((input(request, false).await?.fields, html))
}
fn sanitize_filename(filename: &str) -> Option<String> {
    // Django's multipart filename sanitization runs before safe_upload_path.
    let filename = crate::html_filename::unescape(filename);
    let filename = filename
        .rsplit(['/', '\\'])
        .next()?
        .chars()
        .filter(|c| crate::python_action::printable(*c))
        .collect::<String>();
    let filename = filename.as_str();
    if ["", ".", ".."].contains(&filename) {
        return None;
    }
    let mut filename = filename.to_owned();
    if filename.chars().count() > 255 {
        let (stem, extension) = filename
            .rsplit_once('.')
            .map_or((filename.as_str(), String::new()), |(s, e)| {
                (s, format!(".{e}"))
            });
        let extension = extension.chars().take(255).collect::<String>();
        let stem = stem
            .chars()
            .take(255 - extension.chars().count())
            .collect::<String>();
        filename = stem + &extension;
    }
    Some(filename)
}
async fn validate(
    auth: &AuthService,
    body: Map<String, Value>,
    existing: Option<i64>,
    partial: bool,
) -> Result<Map<String, Value>, StoreError> {
    let mut result = Map::new();
    let mut errors = Map::new();
    for key in ["name", "url"] {
        let value = match body.get(key) {
            Some(v) => character(key, v, None, false),
            None if !partial => Err(invalid(key, "This field is required.")),
            None => continue,
        };
        let value = match value {
            Ok(v) => v,
            Err(StoreError::InvalidInput(Value::Object(map))) => {
                errors.extend(map);
                continue;
            }
            Err(e) => return Err(e),
        };
        if key == "url" {
            let duplicate:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_logo WHERE url=$1 AND ($2::bigint IS NULL OR id<>$2))").bind(&value).bind(existing).fetch_one(auth.store.pool()).await?;
            if duplicate {
                errors.insert("url".into(), json!(["logo with this url already exists."]));
                continue;
            }
        }
        result.insert(
            key.into(),
            json!(if key == "name" {
                value.chars().take(255).collect::<String>()
            } else {
                value
            }),
        );
    }
    if errors.is_empty() {
        Ok(result)
    } else {
        Err(StoreError::InvalidInput(Value::Object(errors)))
    }
}
pub(crate) async fn create(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    let headers = request.headers().clone();
    auth.account_user(peer, headers.clone(), true).await?;
    let _permit = auth.images.reserve().await?;
    let body = validate(&auth, input(request, false).await?.fields, None, false).await?;
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO dispatcharr_channels_logo(name,url) VALUES($1,$2) RETURNING id",
    )
    .bind(body["name"].as_str().unwrap())
    .bind(body["url"].as_str().unwrap())
    .fetch_one(auth.store.pool())
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(crate::logos::serialized(&auth, id, peer.ip(), &headers).await?),
    )
        .into_response())
}
async fn update(
    auth: AuthService,
    peer: SocketAddr,
    raw: String,
    uri: Uri,
    request: Request,
    partial: bool,
) -> Result<Json<Value>, StoreError> {
    let headers = request.headers().clone();
    auth.account_user(peer, headers.clone(), true).await?;
    let _permit = auth.images.reserve().await?;
    let original = crate::logos::lookup(&auth, &raw, &uri).await?;
    let id = original["id"].as_i64().unwrap();
    let values = validate(
        &auth,
        input(request, false).await?.fields,
        Some(id),
        partial,
    )
    .await?;
    let name = values
        .get("name")
        .unwrap_or(&original["name"])
        .as_str()
        .unwrap();
    let url = values
        .get("url")
        .unwrap_or(&original["url"])
        .as_str()
        .unwrap();
    sqlx::query("UPDATE dispatcharr_channels_logo SET name=$2,url=$3 WHERE id=$1")
        .bind(id)
        .bind(name)
        .bind(url)
        .execute(auth.store.pool())
        .await?;
    Ok(Json(
        crate::logos::serialized(&auth, id, peer.ip(), &headers).await?,
    ))
}
pub(crate) async fn patch(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
    OriginalUri(uri): OriginalUri,
    request: Request,
) -> Result<Json<Value>, StoreError> {
    update(auth, peer, id, uri, request, true).await
}
pub(crate) async fn put(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
    OriginalUri(uri): OriginalUri,
    request: Request,
) -> Result<Json<Value>, StoreError> {
    update(auth, peer, id, uri, request, false).await
}
pub(crate) async fn delete(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(raw): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let _permit = auth.images.reserve().await?;
    let row = crate::logos::lookup(&auth, &raw, &uri).await?;
    let id = row["id"].as_i64().unwrap();
    let url = row["url"].as_str().unwrap_or("").to_owned();
    let delete_file = crate::logos::params(&uri)
        .iter()
        .rev()
        .find(|(key, _)| key == "delete_file")
        .is_some_and(|(_, v)| v.to_lowercase() == "true");
    if delete_file && url.starts_with("/data/logos") {
        // Only file effects within the canonical logo root are implemented. Never
        // pretend an escaped legacy path was removed before deleting its DB row.
        let safe = tokio::task::spawn_blocking(move || safe_file_delete(&url))
            .await
            .map_err(|_| {
                StoreError::StatusBody(500, json!({"error":"Failed to delete logo file"}))
            })?;
        safe?;
    }
    let mut tx = auth.store.pool().begin().await?;
    sqlx::query("UPDATE dispatcharr_channels_channel SET logo_id=NULL WHERE logo_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE dispatcharr_channels_channeloverride SET logo_id=NULL WHERE logo_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM dispatcharr_channels_logo WHERE id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((
        StatusCode::NO_CONTENT,
        [(axum::http::header::CONTENT_LENGTH, "0")],
        // StreamBody keeps framing explicit: Hyper otherwise omits the
        // original uWSGI Content-Length:0 on a fully-ended 204 body.
        axum::body::Body::from_stream(futures_util::stream::empty::<
            Result<bytes::Bytes, std::convert::Infallible>,
        >()),
    )
        .into_response())
}
fn bulk_delete_json_error(raw: &[u8], error: serde_json::Error) -> StoreError {
    // serde_json reports the last byte for an absent value. DRF reports the
    // next character, including whitespace after the separator.
    if error.is_eof() {
        if let Ok(text) = std::str::from_utf8(raw) {
            let last = text
                .trim_end_matches([' ', '\t', '\n', '\r'])
                .chars()
                .next_back();
            let detail = error.to_string();
            let missing_value = match last {
                Some(':') => detail.starts_with("EOF while parsing a value"),
                Some('[') => detail.starts_with("EOF while parsing a list"),
                _ => false,
            };
            if missing_value {
                let position = text.chars().count();
                let line = text.bytes().filter(|byte| *byte == b'\n').count() + 1;
                let column = text.rsplit('\n').next().unwrap_or_default().chars().count() + 1;
                return parser(&format!(
                    "JSON parse error - Expecting value: line {line} column {column} (char {position})"
                ));
            }
        }
    }
    parser(&format!(
        "JSON parse error - {}",
        crate::source_json_error(raw, error)
    ))
}

async fn bulk_delete_body(request: Request) -> Result<Map<String, Value>, StoreError> {
    let media = request
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let raw = axum::body::to_bytes(request.into_body(), REQUEST_LIMIT)
        .await
        .map_err(|_| StoreError::StatusBody(413, json!({"detail":"Request body too large."})))?;
    if raw.is_empty() {
        return Ok(Map::new());
    }
    if media.split(';').next().map(str::trim) != Some("application/json") {
        return Err(StoreError::StatusBody(
            415,
            json!({"detail":format!("Unsupported media type \"{media}\" in request.")}),
        ));
    }
    match serde_json::from_slice::<Value>(&raw)
        .map_err(|error| bulk_delete_json_error(&raw, error))?
    {
        Value::Object(fields) => Ok(fields),
        _ => Err(StoreError::UnhandledRestFault),
    }
}

fn bulk_logo_ids(values: Option<&Value>) -> Result<Vec<i64>, StoreError> {
    let mut ids = Vec::new();
    let mut add = |value: &Value| {
        let id = match value {
            Value::Null => None,
            Value::Bool(value) => Some(i64::from(*value)),
            _ => crate::channel_memberships::primary_key(value)
                .map_err(|_| StoreError::UnhandledRestFault)?,
        };
        ids.extend(id);
        Ok::<_, StoreError>(())
    };
    match values {
        None => {}
        Some(Value::Array(values)) => {
            for value in values {
                add(value)?;
            }
        }
        Some(Value::String(value)) => {
            for item in value.chars() {
                add(&Value::String(item.to_string()))?;
            }
        }
        Some(Value::Object(value)) => {
            for key in value.keys() {
                add(&Value::String(key.clone()))?;
            }
        }
        Some(_) => return Err(StoreError::UnhandledRestFault),
    }
    Ok(ids)
}

pub(crate) async fn bulk_delete(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    auth.account_user(peer, request.headers().clone(), true)
        .await?;
    let body = bulk_delete_body(request).await?;
    let ids = bulk_logo_ids(body.get("logo_ids"))?;
    let delete_files = crate::channels::truthy(body.get("delete_files"));
    let permit = auth.images.reserve().await?;
    let work = auth.images.begin()?;
    tokio::spawn(async move {
        let (_permit, _work) = (permit, work);
        bulk_delete_owned(&auth, &ids, delete_files).await
    })
    .await
    .map_err(|_| StoreError::UnhandledRestFault)??;
    Ok((
        StatusCode::NO_CONTENT,
        [
            (axum::http::header::CONTENT_LENGTH, "0"),
            (axum::http::header::CONTENT_TYPE, "application/json"),
        ],
        axum::body::Body::from_stream(futures_util::stream::empty::<
            Result<bytes::Bytes, std::convert::Infallible>,
        >()),
    )
        .into_response())
}

async fn bulk_delete_owned(
    auth: &AuthService,
    ids: &[i64],
    delete_files: bool,
) -> Result<(), StoreError> {
    if ids.is_empty() {
        return Ok(());
    }
    let pool = auth.store.pool();
    let logos: Vec<(i64, Option<String>)> = sqlx::query_as(
        "SELECT id::bigint, CASE WHEN $2 THEN url END          FROM dispatcharr_channels_logo WHERE id=ANY($1)",
    )
    .bind(ids)
    .bind(delete_files)
    .fetch_all(pool)
    .await?;
    let mut selected = Vec::with_capacity(logos.len());
    for (id, url) in logos {
        if let Some(url) = url.filter(|url| url.starts_with("/data/logos")) {
            tokio::task::spawn_blocking(move || delete_local_logo(&url, true))
                .await
                .map_err(|_| StoreError::UnhandledRestFault)??;
        }
        // Original clears the direct links for this logo before proceeding to
        // the next file. Only the final Collector delete is transactional.
        sqlx::query("UPDATE dispatcharr_channels_channel SET logo_id=NULL WHERE logo_id=$1")
            .bind(id)
            .execute(pool)
            .await?;
        selected.push(id);
    }
    let mut tx = pool.begin().await?;
    // Recheck direct links like Django's Collector does before deleting rows.
    for query in [
        "UPDATE dispatcharr_channels_channel SET logo_id=NULL WHERE logo_id=ANY($1)",
        "UPDATE dispatcharr_channels_channeloverride SET logo_id=NULL WHERE logo_id=ANY($1)",
        "DELETE FROM dispatcharr_channels_logo WHERE id=ANY($1)",
    ] {
        sqlx::query(query).bind(&selected).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

fn safe_file_delete(url: &str) -> Result<(), StoreError> {
    delete_local_logo(url, false)
}

fn delete_local_logo(url: &str, bulk: bool) -> Result<(), StoreError> {
    if !FsPath::new(url).exists() {
        return Ok(());
    }
    let root = std::fs::canonicalize("/data/logos").map_err(|_| {
        StoreError::Unsupported("Logo file deletion requires a canonical logo root")
    })?;
    let path = std::fs::canonicalize(url)
        .map_err(|_| StoreError::Unsupported("Unsafe legacy logo path"))?;
    if !path.starts_with(root) {
        return Err(StoreError::Unsupported(
            "Escaped legacy logo file deletion is not implemented",
        ));
    }
    std::fs::remove_file(url).map_err(|error| {
        let message = match error.raw_os_error() {
            Some(code @ (1 | 2 | 13 | 20 | 21 | 30)) => {
                let label = match code {
                    1 => "Operation not permitted",
                    2 => "No such file or directory",
                    13 => "Permission denied",
                    20 => "Not a directory",
                    21 => "Is a directory",
                    _ => "Read-only file system",
                };
                format!("[Errno {code}] {label}: '{url}'")
            }
            _ => error.to_string(),
        };
        let path = if bulk {
            format!(" {url}")
        } else {
            String::new()
        };
        StoreError::StatusBody(
            500,
            json!({"error":format!("Failed to delete logo file{path}: {message}")}),
        )
    })
}
fn upload_path(filename: &str) -> Result<PathBuf, StoreError> {
    if filename.contains('\0') {
        return Err(error("Invalid filename."));
    }
    let base = FsPath::new("/data/logos");
    let root = if base.exists() {
        std::fs::canonicalize(base).map_err(|_| error("Invalid filename."))?
    } else {
        std::fs::canonicalize("/data")
            .map_err(|_| error("Invalid filename."))?
            .join("logos")
    };
    let path = root.join(filename);
    let resolved = if path.exists() || path.is_symlink() {
        std::fs::canonicalize(&path).map_err(|_| error("Invalid filename."))?
    } else {
        path
    };
    if !resolved.starts_with(&root) || resolved == root {
        return Err(error("Invalid filename."));
    }
    Ok(resolved)
}
pub(crate) async fn upload(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    let headers = request.headers().clone();
    auth.account_user(peer, headers.clone(), true).await?;
    let _permit = auth.images.reserve().await?;
    let Input { fields, file } = input(request, true).await?;
    let Some(file) = file else {
        return Err(error("No file uploaded"));
    };
    if ![
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
        "image/svg+xml",
    ]
    .contains(&file.mime.as_str())
    {
        return Err(error(
            "['Unsupported file type. Allowed types: JPEG, PNG, GIF, WebP, SVG.']",
        ));
    }
    if file.size > FILE_LIMIT {
        return Err(error("['File too large. Max 5MB.']"));
    }
    let filename = file.filename.clone();
    let path = tokio::task::spawn_blocking(move || upload_path(&filename))
        .await
        .map_err(|_| error("Invalid filename."))??;
    let overwrite = fields.get("overwrite").is_some_and(|v| {
        v.as_str()
            .is_some_and(|v| ["1", "true", "yes", "on"].contains(&v.trim().to_lowercase().as_str()))
    });
    if path.exists() && !overwrite {
        return Err(StoreError::StatusBody(
            409,
            json!({"error":format!("A logo named '{}' already exists.",path.file_name().and_then(|v|v.to_str()).unwrap_or("")),"already_exists":true}),
        ));
    }
    tokio::fs::create_dir_all("/data/logos")
        .await
        .map_err(|_| {
            StoreError::StatusBody(500, json!({"detail":"Logo directory unavailable."}))
        })?;
    tokio::fs::write(&path, &file.bytes)
        .await
        .map_err(|_| StoreError::StatusBody(500, json!({"detail":"Logo upload failed."})))?;
    let url = path.to_str().ok_or_else(|| error("Invalid filename."))?;
    if let (Some(cache), Ok(modified)) = (
        auth.notification_cache.as_ref(),
        tokio::fs::metadata(&path).await.and_then(|m| m.modified()),
    ) {
        if let Ok(modified) = modified.duration_since(UNIX_EPOCH) {
            let mut cmd = redis::cmd("SETEX");
            cmd.arg(format!("processed_file:{url}"))
                .arg(3 * 24 * 3600)
                .arg(crate::json_compat::float_string(modified.as_secs_f64()));
            let _ = cache.command::<()>(cmd).await;
        }
    }
    let name = fields
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or(&file.filename)
        .chars()
        .take(255)
        .collect::<String>();
    let id:i64=sqlx::query_scalar("INSERT INTO dispatcharr_channels_logo(name,url) VALUES($1,$2) ON CONFLICT(url) DO UPDATE SET name=EXCLUDED.name RETURNING id").bind(name).bind(url).fetch_one(auth.store.pool()).await?;
    Ok((
        StatusCode::CREATED,
        Json(crate::logos::serialized(&auth, id, peer.ip(), &headers).await?),
    )
        .into_response())
}

#[cfg(test)]
mod bulk_tests {
    use super::*;

    #[tokio::test]
    async fn missing_json_values_follow_original_character_offsets() {
        for (body, line, column, position) in [
            (r#"{"logo_ids":"#, 1, 13, 12),
            ("{\"logo_ids\": \t", 1, 15, 14),
            ("{\"x\":\n ", 2, 2, 7),
            (r#"{"é":"#, 1, 6, 5),
            (r#"{"x":["#, 1, 7, 6),
        ] {
            let request = Request::builder()
                .header("Content-Type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap();
            let result = bulk_delete_body(request).await;
            let detail = format!(
                "JSON parse error - Expecting value: line {line} column {column} (char {position})"
            );
            assert!(
                matches!(result, Err(StoreError::InvalidInput(value)) if value == json!({"detail": detail}))
            );
        }
    }

    #[test]
    fn unrelated_json_errors_keep_the_shared_formatter() {
        for body in [
            b"{".as_slice(),
            br#"{"logo_ids":"incomplete:"#,
            br#"{"logo_ids":1,"#,
        ] {
            let error = serde_json::from_slice::<Value>(body).unwrap_err();
            let expected = format!(
                "JSON parse error - {}",
                crate::source_json_error(body, error)
            );
            let error = serde_json::from_slice::<Value>(body).unwrap_err();
            let actual = bulk_delete_json_error(body, error);
            assert!(
                matches!(actual, StoreError::InvalidInput(value) if value == json!({"detail": expected}))
            );
        }
    }

    #[test]
    fn prepares_integer_ids_and_ignores_null_members() {
        for body in [None, Some(&json!([]))] {
            assert!(bulk_logo_ids(body).unwrap().is_empty());
        }
        assert_eq!(
            bulk_logo_ids(Some(&json!([41, "42", true, null, 41]))).unwrap(),
            [41, 42, 1, 41]
        );
        assert!(matches!(
            bulk_logo_ids(Some(&Value::Null)),
            Err(StoreError::UnhandledRestFault)
        ));
        assert!(matches!(
            bulk_logo_ids(Some(&json!([1, "invalid"]))),
            Err(StoreError::UnhandledRestFault)
        ));
    }
}
