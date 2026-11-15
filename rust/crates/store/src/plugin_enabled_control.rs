//! Rust ordering for Source plugin enabled mutation. Actual Django atomic/thread
//! context and manager/user hooks stay in the owned compatibility interpreter;
//! every ORM operation uses the SAME admitted native TaskSqlSession.
//! Original Dispatcharr/Django/plugin authors retain full algorithm credit.
use super::plugin_execution_host::{Error, Invocation, Reentry};
use serde_json::{json, Value};
use std::sync::Arc;
pub(crate) async fn change(
    params: Value,
    scope: Invocation,
    reentry: Arc<dyn Reentry>,
) -> Result<Value, Error> {
    let _owner = scope.owner;
    let session = scope.sql.ok_or(Error::Unavailable)?;
    let key = params["key"]
        .as_str()
        .filter(|v| !v.is_empty() && v.len() <= 512)
        .ok_or(Error::Protocol)?;
    let enabled = params["enabled"].as_bool().ok_or(Error::Protocol)?;
    // Pure physical fence only; transaction BEGIN/COMMIT/ROLLBACK are emitted by
    // the actual Django atomic context, never synthesized by this orchestrator.
    session.fence().await.map_err(|_| Error::Callback)?;
    let config = reentry
        .invoke("select_enabled_config".into(), json!({"key":key}))
        .await?;
    if config["found"] == false {
        return Ok(json!({"found":false}));
    }
    let was_enabled = config["enabled"].as_bool().ok_or(Error::Protocol)?;
    if was_enabled && !enabled {
        reentry
            .invoke("suspend_enabled_schedules".into(), json!({}))
            .await?;
        // This helper catches exactly Exception as Source does, retaining actual
        // stop side effects. Failure of the RPC itself is never a caught fake stop.
        reentry
            .invoke("stop_disabled_plugin_caught".into(), json!({"key":key}))
            .await?;
        reentry
            .invoke("suspend_enabled_schedules".into(), json!({}))
            .await?;
    }
    let saved = reentry
        .invoke("save_enabled_config".into(), json!({"enabled":enabled}))
        .await?;
    if enabled {
        reentry
            .invoke("restore_enabled_schedules".into(), json!({}))
            .await?;
    }
    session.fence().await.map_err(|_| Error::Callback)?;
    Ok(json!({"found":true,"enabled":enabled,"ever_enabled":saved["ever_enabled"]}))
}
