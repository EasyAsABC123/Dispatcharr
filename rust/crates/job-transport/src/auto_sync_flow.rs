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
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Mutex;
use uuid::Uuid;
type Owner = Arc<dyn Send + Sync>;
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
            })
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
        let directory = self.directory.clone();
        let id = self.id;
        let root = self.root;
        let owner = self.owner.clone();
        tokio::task::spawn_blocking(move || ChildrenSpool::create(&directory, id, root, owner))
            .await
            .map_err(|_| Error::Backend)?
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
        let file = tokio::task::spawn_blocking(move || {
            ResultSpool::success(&directory, id, parent, &result, children, owner)
        })
        .await
        .map_err(|_| Error::Backend)??;
        self.jobs
            .write_full_metadata(file, self.owner.clone())
            .await
    }
}
impl EpgDispatch for Flow {
    fn dispatch(
        &self,
        plan: Plan,
        owner: Owner,
    ) -> BoxFuture<'_, Result<(), RecordingEffectError>> {
        Box::pin(async move {
            let actual = self
                .dispatcher
                .dispatch(plan, self.id, self.root, owner, || self.stopped.as_ref()())
                .await
                .map_err(effect)?;
            let mut slot = self.children.lock().await;
            if slot.is_none() {
                *slot = Some(self.create_children().await.map_err(effect)?)
            }
            let append = slot
                .as_mut()
                .ok_or(RecordingEffectError::Unsettled)?
                .append_actual(&actual)
                .await;
            // Join actual SQL cleanup even after a poisoned/failed append. Its
            // uncertainty dominates and can never become a clean task result.
            let closed = actual.close().await;
            closed.map_err(effect)?;
            append.map_err(effect)?;
            Ok(())
        })
    }
}
