//! Sealed regex-only dependency process. No Django/plugin/DB/cache/Jobs ports.
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
