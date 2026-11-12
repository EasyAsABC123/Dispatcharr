//! Explicit scanner-assembly authority, separate from legacy refresh intake.
//! Original Dispatcharr/Celery/Kombu authors retain protocol/behavior credit.
use super::*;
use crate::{refresh_protocol::RefreshMessage, scanner_refresh::Scope};
use serde_json::Value;
const PARENT: &str = r#"
local t=redis.call('TYPE',KEYS[1]).ok
if t=='none' then return {0,''} end
if t~='string' then return {-1,''} end
if redis.call('STRLEN',KEYS[1])>65536 then return {-2,''} end
return {1,redis.call('GET',KEYS[1])}
"#;
fn parent_lists(bytes: &[u8], scope: &Scope, child: Uuid) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
        return false;
    };
    if value["task_id"] != scope.parent().to_string()
        || value["status"] != "SUCCESS"
        || value.get("result") != Some(&Value::Null)
        || value.get("traceback") != Some(&Value::Null)
        || value.get("parent_id").is_some()
    {
        return false;
    }
    let Some(children) = value["children"].as_array().filter(|v| v.len() <= 32) else {
        return false;
    };
    children
        .iter()
        .any(|v| *v == serde_json::json!([[child.to_string(), null], null]))
}
impl Jobs {
    pub(crate) async fn claim_scanner_refresh(
        &self,
        scope: &Scope,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<RefreshMessage>, Error> {
        if !self.allows_refresh() {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller.clone())?;
        let keys = vec!["celery".to_owned()];
        let (reply, head_held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &keys, &[], lease)
            .await
            .map_err(map)?;
        if reply.0 == 0 {
            return Ok(None);
        }
        if reply.0 != 1 {
            return Err(Error::Corrupt);
        }
        let message =
            crate::refresh_protocol::parse(&reply.1).map_err(|_| Error::UnsupportedTask)?;
        if !scope.accepts(&message) {
            return Err(Error::UnsupportedTask);
        }
        let parent_keys = vec![format!("celery-tasks:celery-task-meta-{}", scope.parent())];
        let lease = self.lease(caller.clone())?;
        let (parent, parent_held) = self
            .inner
            .result
            .command_classified::<(i64, Vec<u8>), _>(PARENT, &parent_keys, &[], lease)
            .await
            .map_err(map)?;
        if parent.0 != 1 || !parent_lists(&parent.1, scope, message.task_id) {
            return Err(Error::UnsupportedTask);
        }
        drop(parent_held);
        let lease = self.lease(caller)?;
        let claim = self
            .inner
            .broker
            .command_classified::<i64, _>(CLAIM_CREATE, &keys, &[&reply.1], lease)
            .await;
        drop(head_held);
        match claim {
            Ok((1, _)) => Ok(Some(message)),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(message.task_id)),
            Err(error) => Err(map(error)),
        }
    }
    pub(crate) async fn write_scanner_refresh(
        &self,
        scope: &Scope,
        message: &RefreshMessage,
        result: crate::refresh_worker::NormalReturn,
        children: Vec<Uuid>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_refresh() || !scope.accepts(message) || children.len() > 64 {
            return Err(Error::UnsupportedTask);
        }
        let value = match result {
            crate::refresh_worker::NormalReturn::Null => Value::Null,
            crate::refresh_worker::NormalReturn::MissingSource => serde_json::json!(format!(
                "EPG source {} does not exist, task cleaned up",
                message.source_id
            )),
        };
        self.write_result(message.task_id, value, message.parent, children, caller)
            .await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_successful_parent_receipt_only() {
        let parent = Uuid::new_v4();
        let child = Uuid::new_v4();
        let scope = Scope::new(parent, vec![1]).unwrap();
        let good = serde_json::json!({"task_id":parent,"status":"SUCCESS","result":null,"traceback":null,"children":[[[child,null],null]]});
        assert!(parent_lists(
            &serde_json::to_vec(&good).unwrap(),
            &scope,
            child
        ));
        for field in ["result", "traceback", "children"] {
            let mut bad = good.clone();
            bad.as_object_mut().unwrap().remove(field);
            assert!(!parent_lists(
                &serde_json::to_vec(&bad).unwrap(),
                &scope,
                child
            ));
        }
        let mut bad = good.clone();
        bad["status"] = serde_json::json!("FAILURE");
        assert!(!parent_lists(
            &serde_json::to_vec(&bad).unwrap(),
            &scope,
            child
        ));
        assert!(!parent_lists(
            &serde_json::to_vec(&good).unwrap(),
            &scope,
            Uuid::new_v4()
        ));
    }
}
