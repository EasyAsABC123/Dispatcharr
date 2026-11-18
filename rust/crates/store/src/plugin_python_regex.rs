//! Sealed language/regex dependency process. No Django/plugin/DB/cache/Jobs ports.
//! The admitted native caller owns ordering/effects and its SAME resource scope.
use super::plugin_python_host::{Config, Error, Host, Identity, Ports, Receipt};
use dispatcharr_contracts::BoxFuture;
use serde_json::Value;
use std::{path::PathBuf, sync::Arc, time::Duration};

type Owner = Arc<dyn Send + Sync>;
struct RegexPorts;
impl Ports for RegexPorts {
    fn ready_for_execution(&self) -> bool {
        true
    }
    fn child_started(&self, identity: Identity, owner: Owner) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let _owner = owner;
            // The actor created this exact child as its own process group.
            // Regex-only code exposes no subprocess/import/eval operation.
            let pgid = unsafe { libc::getpgid(identity.pid as libc::pid_t) };
            if pgid != identity.pid as libc::pid_t {
                return Err(Error::Child);
            }
            Ok(())
        })
    }
    fn callback(
        &self,
        _: String,
        _: String,
        _: Value,
        scope: Owner,
    ) -> BoxFuture<'_, Result<Value, Error>> {
        Box::pin(async move {
            let _scope = scope;
            Err(Error::Unavailable)
        })
    }
    fn settle_callbacks(&self, _: Identity, owner: Owner) -> BoxFuture<'_, Result<bool, Error>> {
        // This sealed mode has zero accepted callbacks. Every attempted callback
        // fails above, and the shared actor joins its physical reply before EOF.
        Box::pin(async move {
            let _owner = owner;
            Ok(true)
        })
    }
    fn settle_descendants(
        &self,
        identity: Identity,
        _: bool,
        owner: Owner,
    ) -> BoxFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            let _owner = owner;
            // Actual group absence AFTER actual child wait, not a timer/claim.
            let result = unsafe { libc::kill(-(identity.pid as libc::pid_t), 0) };
            Ok(result == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH))
        })
    }
}
/// Exact group order/null/empty distinctions from the pinned regex engine.
pub enum RegexNamedGroupRow {
    NotMatched,
    Matched(Vec<(String, Option<String>)>),
    SearchError(Value),
}
pub struct RegexNamedGroups {
    pub compile_error: Option<Value>,
    pub rows: Vec<RegexNamedGroupRow>,
}
fn named_groups(value: Value, count: usize) -> Result<RegexNamedGroups, Error> {
    let compiled = value
        .get("compiled")
        .and_then(Value::as_bool)
        .ok_or(Error::Protocol)?;
    if !compiled {
        let error = value
            .get("caught")
            .filter(|v| v.is_object())
            .ok_or(Error::Protocol)?;
        if !value.get("rows").is_some_and(Value::is_null) {
            return Err(Error::Protocol);
        }
        return Ok(RegexNamedGroups {
            compile_error: Some(error.clone()),
            rows: Vec::new(),
        });
    }
    if !value.get("caught").is_some_and(Value::is_null) {
        return Err(Error::Protocol);
    }
    let rows = value
        .get("rows")
        .and_then(Value::as_array)
        .filter(|v| v.len() == count)
        .ok_or(Error::Protocol)?;
    let mut result = Vec::with_capacity(count);
    for row in rows {
        let groups = row.get("groups").ok_or(Error::Protocol)?;
        let caught = row.get("caught").ok_or(Error::Protocol)?;
        match row.get("matched") {
            Some(Value::Null) if groups.is_null() && caught.is_object() => {
                result.push(RegexNamedGroupRow::SearchError(caught.clone()))
            }
            Some(Value::Bool(false)) if groups.is_null() && caught.is_null() => {
                result.push(RegexNamedGroupRow::NotMatched)
            }
            Some(Value::Bool(true)) if caught.is_null() => {
                let mut fields = Vec::new();
                let mut keys = std::collections::HashSet::new();
                for pair in groups.as_array().ok_or(Error::Protocol)? {
                    let pair = pair
                        .as_array()
                        .filter(|v| v.len() == 2)
                        .ok_or(Error::Protocol)?;
                    let key = pair[0].as_str().ok_or(Error::Protocol)?.to_owned();
                    if !keys.insert(key.clone()) {
                        return Err(Error::Protocol);
                    }
                    let value = match &pair[1] {
                        Value::Null => None,
                        Value::String(value) if value.chars().count() <= 512 => Some(value.clone()),
                        _ => return Err(Error::Protocol),
                    };
                    fields.push((key, value));
                }
                result.push(RegexNamedGroupRow::Matched(fields));
            }
            _ => return Err(Error::Protocol),
        }
    }
    Ok(RegexNamedGroups {
        compile_error: None,
        rows: result,
    })
}
#[derive(Clone)]
pub struct RegexExecutor {
    host: Host,
}
impl RegexExecutor {
    pub fn prepare(executable: PathBuf, lifetime: Owner) -> Self {
        let config = Config { executable };
        Self {
            host: Host::prepare(config, Arc::new(RegexPorts), lifetime),
        }
    }
    /// Caller is already admitted by the shared normal worker; no second pool,
    /// admission semaphore or fallback implementation is constructed here.
    /// Search has NO per-pattern deadline. Forced service retirement returns
    /// Err/unknown and keeps its owner through wait/EOF, never match=false.
    pub async fn execute(&self, args: Value, owner: Owner) -> Result<Value, Error> {
        self.host.call("regex".into(), args, owner).await
    }
    pub async fn search_named_groups(
        &self,
        pattern: String,
        names: &[String],
        owner: Owner,
    ) -> Result<RegexNamedGroups, Error> {
        if pattern.len() > 65536
            || names.len() > 100
            || names.iter().any(|name| name.chars().count() > 512)
        {
            return Err(Error::Capacity);
        }
        let value=self.execute(serde_json::json!({"kind":"named_groups","names":names,"config":{"pattern":pattern}}),owner).await?;
        named_groups(value, names.len())
    }
    pub async fn normalize_capture_values(
        &self,
        names: &[String],
        owner: Owner,
    ) -> Result<Vec<String>, Error> {
        if names.len() > 100 || names.iter().any(|name| name.chars().count() > 512) {
            return Err(Error::Capacity);
        }
        let value = self
            .execute(
                serde_json::json!({"kind":"normalize_captures","names":names,"config":{}}),
                owner,
            )
            .await?;
        let values = value
            .get("values")
            .and_then(Value::as_array)
            .filter(|v| v.len() == names.len())
            .ok_or(Error::Protocol)?;
        values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|value| {
                        value.len() <= 512 && value.bytes().all(|c| c.is_ascii_alphanumeric())
                    })
                    .map(str::to_owned)
                    .ok_or(Error::Protocol)
            })
            .collect()
    }
    /// Pinned CPython Unicode primitives; no Source application or effect port.
    /// Input is already validated by the Source serializer-compatible caller.
    pub async fn dummy_epg_name(&self, name: &str, owner: Owner) -> Result<String, Error> {
        if name.chars().count() > 255 {
            return Err(Error::Capacity);
        }
        let value = self
            .execute(
                serde_json::json!({"kind":"dummy_epg_name","name":name}),
                owner,
            )
            .await?;
        let object = value
            .as_object()
            .filter(|value| value.len() == 1)
            .ok_or(Error::Protocol)?;
        object
            .get("tvg_id")
            .and_then(Value::as_str)
            .filter(|value| value.len() <= 4096 && value.starts_with("dummy_"))
            .map(str::to_owned)
            .ok_or(Error::Protocol)
    }
    pub fn no_spawn_retirement(&self) -> Option<super::plugin_python_host::NoSpawnRetirement> {
        self.host.no_spawn_retirement()
    }
    pub fn stop_admission(&self) {
        self.host.stop_admission();
    }
    /// Signal an accepted task's actual drain deadline before lane join.
    /// Calls remain allowed until then; async shutdown still proves receipts.
    pub fn interrupt_at(&self, deadline: tokio::time::Instant) {
        self.host.interrupt_at(deadline);
    }
    pub async fn shutdown(&self, budget: Duration) -> Result<Option<Receipt>, Error> {
        self.host.shutdown(budget).await
    }
    pub async fn last_receipt(&self) -> Option<Receipt> {
        self.host.last_receipt().await
    }
}

#[cfg(test)]
mod group_tests {
    use super::*;
    use serde_json::json;
    #[tokio::test]
    async fn actual_no_spawn_marker_is_not_a_process_receipt_or_no_match() {
        let executor =
            RegexExecutor::prepare("/owned-not-called-regex-executable".into(), Arc::new(()));
        assert!(executor.no_spawn_retirement().is_none());
        executor.interrupt_at(tokio::time::Instant::now());
        tokio::time::timeout(Duration::from_secs(2), async {
            while executor.no_spawn_retirement().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(executor.last_receipt().await.is_none());
        assert!(executor
            .execute(
                json!({"kind":"filter","names":["Owned"],"config":{"pattern":"Owned"}}),
                Arc::new(())
            )
            .await
            .is_err());
    }
    #[test]
    fn production_capture_decoder_preserves_order_empty_null_and_no_group_match() {
        let result=named_groups(json!({"compiled":true,"caught":null,"rows":[{"matched":true,"caught":null,"groups":[["z",null],["a",""],["value","Owned"]]},{"matched":true,"caught":null,"groups":[]},{"matched":false,"caught":null,"groups":null}]}),3).unwrap();
        match &result.rows[0] {
            RegexNamedGroupRow::Matched(fields) => assert_eq!(
                fields,
                &vec![
                    ("z".into(), None),
                    ("a".into(), Some("".into())),
                    ("value".into(), Some("Owned".into()))
                ]
            ),
            _ => panic!("actual match lost"),
        }
        assert!(matches!(&result.rows[1],RegexNamedGroupRow::Matched(fields)if fields.is_empty()));
        assert!(matches!(&result.rows[2], RegexNamedGroupRow::NotMatched));
    }
    #[test]
    fn malformed_or_unknown_capture_results_never_become_normal_no_match() {
        for value in [
            json!({"compiled":true,"caught":null,"rows":[]}),
            json!({"compiled":true,"caught":null,"rows":[{"matched":true,"caught":null,"groups":[["x","a"],["x","b"]]}]}),
            json!({"compiled":true,"caught":null,"rows":[{"matched":false,"caught":{"actual_class":"Interrupted"},"groups":null}]}),
        ] {
            assert!(matches!(named_groups(value, 1), Err(Error::Protocol)));
        }
    }
}
