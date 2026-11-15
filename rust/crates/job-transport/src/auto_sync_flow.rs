//! Real per-parent auto-sync EPG/metadata owner for normal M3U and Auto tasks.
//! Original Dispatcharr/Celery/Kombu authors retain application/protocol credit.
//! Draft factory; actual normal-worker/Main assembly is still being joined.
use crate::{
    auto_sync_epg_dispatch::Dispatcher,
    full_metadata::{ChildrenSpool, ResultSpool},
    transport::{Error, Jobs},
};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::{
    auto_sync::EpgDispatch,
    auto_sync_epg::Plan,
    recording_http::{RecordingEffectError, RecordingJobs},
    ProducerIdentitySource, RegexExecutor,
};
use serde_json::Value;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex as StdMutex, Weak,
    },
    time::Duration,
};
use tokio::sync::Mutex;
use uuid::Uuid;
type Owner = Arc<dyn Send + Sync>;
/// Weak capabilities avoid Inner->Flow->owner->Inner cycles. Entries are
/// pruned on each real registration; live count is the existing worker budget.
#[derive(Default)]
pub(crate) struct ActiveFlows(StdMutex<FlowState>);
#[derive(Default)]
struct FlowState {
    deadline: Option<tokio::time::Instant>,
    flows: Vec<Weak<Flow>>,
}
impl ActiveFlows {
    pub(crate) fn track(&self, flow: Arc<Flow>) -> Arc<Flow> {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        state.flows.retain(|f| f.strong_count() > 0);
        state.flows.push(Arc::downgrade(&flow));
        if let Some(deadline) = state.deadline {
            flow.regex.interrupt_at(deadline);
        }
        flow
    }
    pub(crate) fn begin_drain(&self, budget: Duration) {
        // Reserve the existing5s physical child wait/EOF budget inside, not in
        // addition to, the caller's actual remaining application deadline.
        let deadline = tokio::time::Instant::now() + budget.saturating_sub(Duration::from_secs(5));
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let deadline = state
            .deadline
            .map(|old| old.min(deadline))
            .unwrap_or(deadline);
        state.deadline = Some(deadline);
        for flow in state.flows.iter().filter_map(Weak::upgrade) {
            flow.regex.interrupt_at(deadline);
        }
    }
}
pub(crate) struct Flow {
    jobs: Jobs,
    dispatcher: Dispatcher,
    directory: PathBuf,
    id: Uuid,
    parent: Option<Uuid>,
    root: Uuid,
    children: Mutex<Option<ChildrenSpool>>,
    regex: RegexExecutor,
    retired: Mutex<Option<bool>>,
    stopped: Arc<dyn Fn() -> bool + Send + Sync>,
    owner: Owner,
    diagnostics: AtomicUsize,
}
fn effect(error: Error) -> RecordingEffectError {
    match error {
        Error::UnsupportedTask | Error::Protocol => RecordingEffectError::Unsupported,
        _ => RecordingEffectError::Unsettled,
    }
}
impl Flow {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        jobs: Jobs,
        identity: ProducerIdentitySource,
        directory: PathBuf,
        id: Uuid,
        parent: Option<Uuid>,
        root: Uuid,
        owner: Owner,
        stopped: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Arc<Self> {
        {
            let regex = RegexExecutor::prepare(
                PathBuf::from("/usr/local/bin/dispatcharr-regex-python"),
                owner.clone(),
            );
            Arc::new(Self {
                dispatcher: Dispatcher::new(jobs.clone(), identity),
                jobs,
                directory,
                id,
                parent,
                root,
                children: Mutex::new(None),
                regex,
                retired: Mutex::new(None),
                stopped,
                owner,
                diagnostics: AtomicUsize::new(0),
            })
        }
    }
    /// Opt-in fixed-category records, bounded to64 per actual accepted parent.
    /// No provider/configuration/body strings are formatted.
    pub(crate) fn observe(&self, stage: &'static str) {
        if std::env::var_os("RUST_AUTO_STAGE_DIAGNOSTICS").as_deref()
            != Some(std::ffi::OsStr::new("1"))
        {
            return;
        }
        if self.diagnostics.fetch_add(1, Ordering::Relaxed) < 64 {
            eprintln!("rust_auto_stage task={} stage={stage}", self.id);
        }
    }
    pub(crate) fn services(
        self: &Arc<Self>,
        jobs: Arc<dyn RecordingJobs>,
    ) -> dispatcharr_store::auto_sync::Services {
        dispatcharr_store::auto_sync::Services {
            jobs,
            epg: self.clone(),
            regex: self.regex.clone(),
        }
    }
    pub(crate) async fn retire(&self, budget: std::time::Duration) -> Result<(), Error> {
        self.observe("regex_retire_begin");
        let mut retired = self.retired.lock().await;
        if let Some(clean) = *retired {
            return if clean {
                Ok(())
            } else {
                Err(Error::DrainTimeout)
            };
        }
        *retired = Some(false);
        self.regex
            .shutdown(budget)
            .await
            .map_err(|_| Error::DrainTimeout)?;
        self.observe("regex_retire_complete");
        *retired = Some(true);
        Ok(())
    }
    pub(crate) async fn append_child(
        &self,
        actual: &impl crate::full_metadata::ActualPublishedChildren,
    ) -> Result<(), Error> {
        let mut slot = self.children.lock().await;
        if slot.is_none() {
            *slot = Some(self.create_children().await?)
        }
        slot.as_mut()
            .ok_or(Error::Backend)?
            .append_actual(actual)
            .await
    }
    async fn create_children(&self) -> Result<ChildrenSpool, Error> {
        self.observe("children_spool_begin");
        let directory = self.directory.clone();
        let id = self.id;
        let root = self.root;
        let owner = self.owner.clone();
        let result =
            tokio::task::spawn_blocking(move || ChildrenSpool::create(&directory, id, root, owner))
                .await
                .map_err(|_| Error::Backend)?;
        self.observe(if result.is_ok() {
            "children_spool_complete"
        } else {
            "children_spool_error"
        });
        result
    }
    /// All dispatch calls must finish before this terminal write. Actual child
    /// pages were consumed while their task SQL session still existed; the final
    /// metadata no longer depends on that connection or a provider-sized Vec.
    pub(crate) async fn complete(&self, result: Value) -> Result<bool, Error> {
        self.retire(std::time::Duration::from_secs(5)).await?;
        let children = match self.children.lock().await.take() {
            Some(children) => children,
            None => self.create_children().await?,
        };
        let directory = self.directory.clone();
        let id = self.id;
        let parent = self.parent;
        let owner = self.owner.clone();
        self.observe("result_spool_begin");
        let file = tokio::task::spawn_blocking(move || {
            ResultSpool::success(&directory, id, parent, &result, children, owner)
        })
        .await
        .map_err(|_| Error::Backend)??;
        self.observe("result_spool_complete");
        self.observe("result_publish_begin");
        let result = self
            .jobs
            .write_full_metadata(file, self.owner.clone())
            .await;
        self.observe(if result.is_ok() {
            "result_publish_complete"
        } else {
            "result_publish_error"
        });
        result
    }
}
impl EpgDispatch for Flow {
    fn observe(&self, stage: &'static str) {
        Flow::observe(self, stage);
    }
    fn dispatch(
        &self,
        plan: Plan,
        owner: Owner,
    ) -> BoxFuture<'_, Result<(), RecordingEffectError>> {
        Box::pin(async move {
            self.observe("epg_dispatch_begin");
            let actual = self
                .dispatcher
                .dispatch(plan, self.id, self.root, owner, || self.stopped.as_ref()())
                .await
                .map_err(effect)?;
            self.observe("epg_dispatch_complete");
            let mut slot = self.children.lock().await;
            if slot.is_none() {
                *slot = Some(self.create_children().await.map_err(effect)?)
            }
            self.observe("children_append_begin");
            let append = slot
                .as_mut()
                .ok_or(RecordingEffectError::Unsettled)?
                .append_actual(&actual)
                .await;
            // Join actual SQL cleanup even after a poisoned/failed append. Its
            // uncertainty dominates and can never become a clean task result.
            self.observe(if append.is_ok() {
                "children_append_complete"
            } else {
                "children_append_error"
            });
            self.observe("epg_plan_close_begin");
            let closed = actual.close().await;
            self.observe(if closed.is_ok() {
                "epg_plan_close_complete"
            } else {
                "epg_plan_close_error"
            });
            closed.map_err(effect)?;
            append.map_err(effect)?;
            Ok(())
        })
    }
}
