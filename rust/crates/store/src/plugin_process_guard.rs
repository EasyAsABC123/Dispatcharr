//! Concrete guarded no-child slice. This does NOT claim arbitrary plugin
//! subprocess compatibility. Normal stdlib spawning is rejected before effect;
//! actual host wait/EOF and kernel group absence remain required at retirement.
//! Original Dispatcharr/CPython authors retain all algorithm credit.
use super::{
    plugin_execution_host::{Error, Identity, Invocation},
    plugin_execution_ports::NativePluginProcesses,
};
use dispatcharr_contracts::BoxFuture;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
type Owner = Arc<dyn Send + Sync>;
#[derive(Default)]
pub struct GuardedPluginProcesses {
    identity: Mutex<Option<Identity>>,
}
fn actual(identity: &Identity) -> bool {
    let Ok(raw) = std::fs::read_to_string(format!("/proc/{}/stat", identity.pid)) else {
        return false;
    };
    let Some((_, tail)) = raw.rsplit_once(')') else {
        return false;
    };
    let fields = tail.split_whitespace().collect::<Vec<_>>();
    fields.get(1).and_then(|s| s.parse::<u32>().ok()) == Some(std::process::id())
        && fields.get(2).and_then(|s| s.parse::<u32>().ok()) == Some(identity.pid)
        && fields
            .get(19)
            .is_some_and(|s| *s == identity.start_generation)
        && std::fs::read_link(format!("/proc/{}/ns/pid", identity.pid))
            .ok()
            .as_ref()
            == Some(&identity.pid_namespace)
}
impl NativePluginProcesses for GuardedPluginProcesses {
    fn ready(&self) -> bool {
        true
    }
    fn child_started(&self, identity: Identity, _owner: Owner) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            if !actual(&identity) {
                return Err(Error::Protocol);
            }
            let mut slot = self.identity.lock().map_err(|_| Error::Protocol)?;
            if slot.is_some() {
                return Err(Error::Protocol);
            }
            *slot = Some(identity);
            Ok(())
        })
    }
    fn callback(
        &self,
        identity: Identity,
        method: String,
        _params: Value,
        _scope: Invocation,
    ) -> BoxFuture<'_, Result<Value, Error>> {
        Box::pin(async move {
            let same = self
                .identity
                .lock()
                .map_err(|_| Error::Protocol)?
                .as_ref()
                .is_some_and(|known| {
                    known.pid == identity.pid
                        && known.start_generation == identity.start_generation
                        && known.pid_namespace == identity.pid_namespace
                });
            if !same || !actual(&identity) {
                return Err(Error::Protocol);
            }
            if method != "policy" {
                return Err(Error::Unavailable);
            }
            Ok(json!({"mode":"guarded-no-spawn","actual_host_group_owned":true}))
        })
    }
    fn settle(
        &self,
        identity: Identity,
        _forced: bool,
        owner: Owner,
    ) -> BoxFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            let _owner = owner;
            let same = self
                .identity
                .lock()
                .map_err(|_| Error::Protocol)?
                .as_ref()
                .is_some_and(|known| {
                    known.pid == identity.pid
                        && known.start_generation == identity.start_generation
                        && known.pid_namespace == identity.pid_namespace
                });
            if !same {
                return Err(Error::Protocol);
            }
            let result = unsafe { libc::kill(-(identity.pid as libc::pid_t), 0) };
            // Never kill a guessed/reused foreign group or turn an unexpected descendant
            // into a normal receipt. Native caller already performed the real child wait.
            Ok(result == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH))
        })
    }
}
