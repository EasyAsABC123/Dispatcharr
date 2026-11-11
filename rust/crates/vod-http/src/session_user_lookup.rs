//! Source views.py optional session identity restoration. Original Dispatcharr
//! authors retain application/algorithm credit. No authentication policy change.
use super::{key, redis_error, unavailable, Inner};
use dispatcharr_store::StoreError;
use redis::{AsyncCommands, Script};
use std::sync::Arc;

// Existing writers store a decimal user PK (at most20 bytes). Accept additional
// pinned Source integer spelling within4KiB, without transferring oversized data.
// This private read shares the application's existing Lua capability requirement.
pub(super) const READ_MAPPING: &str = r#"
local size=redis.call('STRLEN',KEYS[1])
if size>4096 then return {'CAPACITY'} end
local value=redis.call('GET',KEYS[1])
if not value then return {'NONE'} end
return {'VALUE',value}
"#;

pub(super) async fn optional_user(inner: Arc<Inner>, id: &str) -> Result<Option<i64>, StoreError> {
    if id.is_empty() || id.len() > 128 || id.contains(':') {
        return Err(StoreError::NotFound);
    }
    if *inner.stopped.borrow() {
        return Err(unavailable());
    }
    let permit = inner
        .requests
        .clone()
        .try_acquire_owned()
        .map_err(|_| unavailable())?;
    let id = id.to_owned();
    // JoinHandle drop detaches rather than cancels: request admission and the
    // service survive until the real optional-read reply, including observer abort.
    let task = tokio::spawn(async move {
        let _permit = permit;
        let _service = inner;
        let mut connection = _service.connection.clone();
        // Original skips the mapping for an already present public connection.
        // Its exact optional block catches Redis errors and parser exceptions.
        match connection.exists::<_, bool>(key(&id)).await {
            Ok(false) => {}
            Ok(true) | Err(_) => return Ok(None),
        }
        let result: redis::RedisResult<Vec<String>> = Script::new(READ_MAPPING)
            .key(format!("vod_session_user:{id}"))
            .invoke_async(&mut connection)
            .await;
        match result {
            Ok(values) if values.first().is_some_and(|v| v == "CAPACITY") => {
                Err(StoreError::StatusBody(
                    503,
                    serde_json::json!({"error":"VOD optional identity mapping exceeds bounded input"}),
                ))
            }
            Ok(values) if values.len() == 2 && values[0] == "VALUE" => {
                Ok(dispatcharr_ingest_core::source_integer_literal(&values[1])
                    .and_then(|v| v.parse::<i64>().ok()))
            }
            Ok(_) | Err(_) => Ok(None),
        }
    });
    task.await.map_err(|_| redis_error())?
}
