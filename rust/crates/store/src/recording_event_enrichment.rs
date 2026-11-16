//! Native port of core.utils.dispatch_event_system enrichment only.
//! Original Dispatcharr authors retain application and algorithm credit.
//! All SQL uses the caller's actual Auth-owned TaskSqlSession; no dispatch here.
use crate::{AuthService, StoreError, TaskSqlSession};
use serde_json::{json, Map, Value};
use sqlx::Row;
use std::sync::Arc;
use uuid::Uuid;
type Owner = Arc<dyn Send + Sync>;
fn unsupported() -> StoreError {
    StoreError::Unsupported("Recording Connect enrichment domain/capacity unsupported")
}
fn id(value: &Value) -> Result<Option<i64>, StoreError> {
    match value {
        Value::Null => Ok(None),
        Value::Bool(v) => Ok(Some(i64::from(*v))),
        Value::Number(v) if v.is_i64() => Ok(v.as_i64()),
        Value::Number(v) if v.is_u64() => Ok(v.as_u64().and_then(|v| i64::try_from(v).ok())),
        Value::Number(v) => match v.as_f64().filter(|v| v.is_finite()) {
            Some(v) if v > i64::MIN as f64 && v < i64::MAX as f64 => Ok(Some(v.trunc() as i64)),
            _ => Ok(None),
        },
        Value::String(v) if v.len() <= 256 => {
            Ok(dispatcharr_ingest_core::source_integer_literal(v).and_then(|v| v.parse().ok()))
        }
        Value::String(_) => Err(unsupported()),
        _ => Err(unsupported()),
    }
}
fn suffix(value: &Value) -> Result<String, StoreError> {
    match value {
        Value::Bool(v) => Ok(if *v { "True" } else { "False" }.into()),
        Value::String(v) if v.len() <= 256 => Ok(v.clone()),
        Value::Number(v) => Ok(v.to_string()),
        _ => Err(unsupported()),
    }
}
fn clean(payload: &mut Map<String, Value>) {
    // Source removes only None and empty strings; false/zero/empty containers stay.
    payload.retain(|_, v| !v.is_null() && v.as_str() != Some(""));
}
async fn cache_id(
    auth: &AuthService,
    key: String,
    owner: Owner,
) -> Result<Option<Value>, StoreError> {
    let Some(cache) = auth.notification_cache.clone() else {
        return Ok(None);
    };
    match cache.with_retained_scope(owner).get(&key).await {
        Ok(Some(raw)) if !raw.is_empty() => {
            if raw.len() > 256 {
                return Err(unsupported());
            }
            let parsed = std::str::from_utf8(&raw)
                .ok()
                .and_then(dispatcharr_ingest_core::source_integer_literal)
                .and_then(|v| v.parse::<i64>().ok());
            Ok(Some(parsed.map_or(Value::Null, |v| json!(v))))
        }
        // A missing/empty Redis value leaves the caller's existing falsy value.
        Ok(_) => Ok(None),
        // Source catches Redis lookup errors and clears stream/profile selection.
        Err(_) => Ok(Some(Value::Null)),
    }
}
pub(crate) async fn enrich(
    auth: &AuthService,
    session: &TaskSqlSession,
    channel: Option<Uuid>,
    channel_name: Option<String>,
    details: Value,
    owner: Owner,
) -> Result<Value, StoreError> {
    let mut payload = details.as_object().cloned().ok_or_else(unsupported)?;
    let mut channel_pk = None;
    let mut actual_name = channel_name
        .filter(|v| !v.is_empty())
        .map_or(Value::Null, Value::String);
    if let Some(channel) = channel {
        payload.insert("channel_id".into(), json!(channel.to_string()));
        let selected = session.run(move |c| Box::pin(async move {
            sqlx::query("SELECT id::bigint,CASE WHEN octet_length(name)<=1024 THEN name ELSE NULL END AS name FROM dispatcharr_channels_channel WHERE uuid=$1")
                .bind(channel).fetch_optional(c).await
        })).await?;
        match selected {
            Ok(Some(row)) => {
                channel_pk = Some(row.try_get::<i64, _>("id")?);
                actual_name = Value::String(
                    row.try_get::<Option<String>, _>("name")?
                        .ok_or_else(unsupported)?,
                );
            }
            Ok(None) | Err(sqlx::Error::Database(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    payload.insert("channel_name".into(), actual_name);
    let mut stream_id = payload.get("stream_id").cloned().unwrap_or(Value::Null);
    if !crate::channels::truthy(Some(&stream_id)) {
        if let Some(pk) = channel_pk {
            if let Some(selected) =
                cache_id(auth, format!("channel_stream:{pk}"), owner.clone()).await?
            {
                stream_id = selected;
            }
        }
    }
    let mut stream_name = Value::Null;
    let mut stream_url = Value::Null;
    let mut provider_name = Value::Null;
    let mut profile_name = Value::Null;
    if crate::channels::truthy(Some(&stream_id)) {
        if let Some(ident) = id(&stream_id)? {
            let selected = session.run(move |c| Box::pin(async move {
                sqlx::query("SELECT CASE WHEN octet_length(name)<=4096 THEN name ELSE NULL END AS name,CASE WHEN octet_length(url)<=16384 THEN url ELSE NULL END AS url,m3u_account_id::bigint AS account,octet_length(name)<=4096 AND octet_length(url)<=16384 AS bounded FROM dispatcharr_channels_stream WHERE id=$1")
                    .bind(ident).fetch_optional(c).await
            })).await?;
            match selected {
                Ok(Some(row)) => {
                    if !row.try_get::<bool, _>("bounded")? {
                        return Err(unsupported());
                    }
                    stream_name = row
                        .try_get::<Option<String>, _>("name")?
                        .map_or(Value::Null, Value::String);
                    stream_url = row
                        .try_get::<Option<String>, _>("url")?
                        .map_or(Value::Null, Value::String);
                    // Source resolves the FK separately and catches its failure
                    // without discarding the already loaded Stream name/URL.
                    if let Some(account) = row.try_get::<Option<i64>, _>("account")? {
                        let provider=session.run(move|c|Box::pin(async move{
                            sqlx::query("SELECT CASE WHEN octet_length(name)<=4096 THEN name ELSE NULL END AS name FROM m3u_m3uaccount WHERE id=$1").bind(account).fetch_optional(c).await
                        })).await?;
                        match provider {
                            Ok(Some(row)) => {
                                provider_name = Value::String(
                                    row.try_get::<Option<String>, _>("name")?
                                        .ok_or_else(unsupported)?,
                                )
                            }
                            Ok(None) | Err(sqlx::Error::Database(_)) => {}
                            Err(error) => return Err(error.into()),
                        }
                    }
                }
                Ok(None) | Err(sqlx::Error::Database(_)) => {}
                Err(error) => return Err(error.into()),
            }
        }
        // Source resolves profile even when the selected Stream row disappeared.
        if let Some(profile) = cache_id(
            auth,
            format!("stream_profile:{}", suffix(&stream_id)?),
            owner,
        )
        .await?
        {
            if let Some(ident) = id(&profile)? {
                let selected = session.run(move |c| Box::pin(async move {
                    sqlx::query("SELECT CASE WHEN octet_length(name)<=4096 THEN name ELSE NULL END AS name FROM core_streamprofile WHERE id=$1")
                        .bind(ident).fetch_optional(c).await
                })).await?;
                match selected {
                    Ok(Some(row)) => {
                        profile_name = Value::String(
                            row.try_get::<Option<String>, _>("name")?
                                .ok_or_else(unsupported)?,
                        )
                    }
                    Ok(None) | Err(sqlx::Error::Database(_)) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
    }
    for (key, value) in [
        ("stream_id", stream_id),
        ("stream_name", stream_name),
        ("stream_url", stream_url.clone()),
        ("channel_url", stream_url),
        ("provider_name", provider_name),
        ("profile_used", profile_name),
    ] {
        payload.insert(key.into(), value);
    }
    clean(&mut payload);
    let result = Value::Object(payload);
    if serde_json::to_vec(&result)
        .map_err(|_| unsupported())?
        .len()
        > 65536
    {
        return Err(unsupported());
    }
    Ok(result)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_null_cleanup_preserves_falsy_json_values() {
        let mut payload=json!({"none":null,"empty":"","zero":0,"false":false,"array":[],"object":{},"channel_name":"Owned"}).as_object().unwrap().clone();
        clean(&mut payload);
        assert_eq!(
            Value::Object(payload),
            json!({"zero":0,"false":false,"array":[],"object":{},"channel_name":"Owned"})
        );
    }
    #[test]
    fn source_numeric_ids_and_cache_suffix_keep_type() {
        assert_eq!(id(&json!("  +1_2  ")).unwrap(), Some(12));
        assert_eq!(id(&json!(true)).unwrap(), Some(1));
        assert_eq!(suffix(&json!(true)).unwrap(), "True");
        assert_eq!(suffix(&json!("001")).unwrap(), "001");
        assert_eq!(id(&json!("not-an-id")).unwrap(), None);
        assert!(id(&json!([1])).is_err());
    }
}
