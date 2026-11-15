//! Rust event-handler ordering; actual pinned run_action handles params-or-{}.
//! One Python thread owns the shared mutable outer params throughout the plan.
use super::plugin_execution_host::{Error, Invocation, Reentry};
use serde_json::{json, Value};
use std::sync::Arc;
pub(crate) async fn dispatch(
    _scope: Invocation,
    reentry: Arc<dyn Reentry>,
) -> Result<Value, Error> {
    let plan = reentry.invoke("event_plan".into(), Value::Null).await?;
    let actions = plan
        .as_array()
        .filter(|a| a.len() <= 1024)
        .ok_or(Error::Capacity)?;
    let mut attempted = 0u64;
    let mut completed = 0u64;
    let mut caught = 0u64;
    for action in actions {
        if !action["enabled"].as_bool().ok_or(Error::Protocol)? {
            continue;
        }
        let result = reentry
            .invoke("event_invoke_caught".into(), action.clone())
            .await?;
        attempted += 1;
        if result["actual_completed"] == true {
            completed += 1;
        } else if result["actual_exception_caught"] == true {
            caught += 1;
        } else {
            return Err(Error::Protocol);
        }
    }
    Ok(json!({"attempted":attempted,"completed":completed,"caught":caught}))
}
