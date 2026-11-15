//! Actual standalone sync_auto_channels worker; same global6/Auth4/pool8/Jobs.
//! Original Dispatcharr/Celery authors retain all application/protocol credit.
//! Draft assembly, not registered or qualified as packaged Main acceptance yet.
use crate::{
    auto_sync_flow::Flow,
    milestone_dispatch::{Disposition as D, Family, Handler, Lifecycle},
    normal_revocation::Revoked,
    transport::{Error as WireError, Jobs},
};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::{recording_http::RecordingJobs, AuthService, Store, StoreError};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;
type Owner = Arc<dyn Send + Sync>;
struct Inner {
    jobs: Jobs,
    auth: AuthService,
    recording: Arc<dyn RecordingJobs>,
    revoked: Revoked,
    directory: PathBuf,
    closed: AtomicBool,
    active: AtomicUsize,
    changed: Notify,
    heads: Arc<Notify>,
    uncertain: Mutex<Vec<Owner>>,
}
#[derive(Clone)]
pub struct Prepared {
    inner: Arc<Inner>,
}
struct Active(Arc<Inner>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
struct Claim {
    inner: Arc<Inner>,
    owner: Owner,
    armed: bool,
}
impl Drop for Claim {
    fn drop(&mut self) {
        if self.armed {
            self.inner
                .uncertain
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(self.owner.clone())
        }
    }
}
impl Prepared {
    pub fn new(
        jobs: Jobs,
        store: Store,
        auth: AuthService,
        recording: Arc<dyn RecordingJobs>,
        revoked: Revoked,
        directory: PathBuf,
    ) -> Result<Self, &'static str> {
        if !auth.uses_store_pool(&store) || !recording.supports_task_session() {
            return Err("Auto-sync requires actual shared task SQL/recording ports");
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                auth,
                recording,
                revoked,
                directory,
                closed: AtomicBool::new(false),
                active: AtomicUsize::new(0),
                changed: Notify::new(),
                heads: Arc::new(Notify::new()),
                uncertain: Mutex::new(Vec::new()),
            }),
        })
    }
    pub fn entry(&self) -> (Family, Arc<dyn Handler>) {
        (Family::AutoChannels, Arc::new(self.clone()))
    }
    async fn execute(&self, expected: Option<Arc<[u8]>>, caller: Owner) -> Result<D, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(D::Operational("Auto-sync intake stopped".into()));
        }
        let inner = self.inner.clone();
        inner.active.fetch_add(1, Ordering::AcqRel);
        tokio::spawn(async move {
            let _active = Active(inner.clone());
            let owner: Owner = Arc::new((caller, inner.clone()));
            let mut claim = Claim {
                inner: inner.clone(),
                owner: owner.clone(),
                armed: false,
            };
            let result = async {
                let candidate = match inner
                    .jobs
                    .peek_auto_sync(expected.as_deref(), owner.clone())
                    .await
                {
                    Ok(Some(candidate)) => candidate,
                    Ok(None) => return Ok(D::Empty),
                    Err(WireError::ClaimRace) => return Ok(D::HeadChanged),
                    Err(WireError::UnsupportedTask) => return Ok(D::UnsupportedHead),
                    Err(_) => return Err(D::Operational("Auto-sync raw head read failed".into())),
                };
                // The public session factory acquires and physically retains the
                // SAME Auth4 writer; no second permit or request binding is needed.
                // Admission precedes CAS, so capacity leaves the actual raw head.
                let auth = inner.auth.clone().with_retained_scope(owner.clone());
                let session = match auth.borrow_task_sql_session(owner.clone()).await {
                    Ok(session) => session,
                    Err(StoreError::StatusBody(503, _)) => return Ok(D::Capacity),
                    Err(_) => {
                        return Err(D::Operational("Auto-sync actual SQL session failed".into()))
                    }
                };
                if inner.closed.load(Ordering::Acquire) {
                    session
                        .close()
                        .await
                        .map_err(|_| D::Operational("Auto-sync SQL drain failed".into()))?;
                    return Err(D::Operational("Auto-sync admission stopped".into()));
                }
                if let Err(error) = dispatcharr_store::auto_sync::preflight(
                    &session,
                    candidate.message.common.account,
                )
                .await
                {
                    session.close().await.map_err(|_| {
                        D::Operational("Auto-sync preflight SQL drain failed".into())
                    })?;
                    return match error {
                        StoreError::Unsupported(_) => Ok(D::UnsupportedDomain),
                        _ => Err(D::Operational("Auto-sync preflight failed".into())),
                    };
                }
                claim.armed = true;
                let message = match inner.jobs.claim_auto_sync(candidate, owner.clone()).await {
                    Ok(message) => message,
                    Err(WireError::ClaimRace) => {
                        claim.armed = false;
                        session
                            .close()
                            .await
                            .map_err(|_| D::Operational("Auto-sync SQL close failed".into()))?;
                        return Ok(D::HeadChanged);
                    }
                    Err(WireError::Capacity) => {
                        claim.armed = false;
                        session
                            .close()
                            .await
                            .map_err(|_| D::Operational("Auto-sync SQL close failed".into()))?;
                        return Ok(D::Capacity);
                    }
                    Err(_) => {
                        return Err(D::Operational("Auto-sync claim outcome unsettled".into()))
                    }
                };
                inner.heads.notify_one();
                let context = &message.common;
                if inner
                    .revoked
                    .contains(&context.id.to_string())
                    .map_err(|_| D::Operational("Auto-sync revoked set failed".into()))?
                {
                    session
                        .close()
                        .await
                        .map_err(|_| D::Operational("Auto-sync revoked SQL drain failed".into()))?;
                    inner
                        .jobs
                        .write_xc_live_revoked(context, owner.clone())
                        .await
                        .map_err(|_| {
                            D::Operational("Auto-sync REVOKED backend unsettled".into())
                        })?;
                    return Ok(D::Revoked(context.id));
                }
                let stopped = inner.clone();
                let flow = Flow::new(
                    inner.jobs.clone(),
                    auth.producer_identity_source(),
                    inner.directory.clone(),
                    context.id,
                    context.parent,
                    context.root,
                    owner.clone(),
                    Arc::new(move || stopped.closed.load(Ordering::Acquire)),
                );
                let body = dispatcharr_store::auto_sync::sync(
                    session.clone(),
                    auth,
                    flow.services(inner.recording.clone()),
                    context.account,
                    message.start.unwrap_or_else(chrono::Utc::now),
                    owner,
                )
                .await;
                let closed = session.close().await;
                if closed.is_err() {
                    let _ = flow.retire(Duration::from_secs(5)).await;
                    return Err(D::Operational("Auto-sync physical SQL drain failed".into()));
                }
                let report = match body {
                    Ok(report) => report,
                    Err(error) => {
                        let _ = flow.retire(Duration::from_secs(5)).await;
                        return Err(match error {
                            dispatcharr_store::auto_sync::Error::Unsupported => {
                                D::UnsupportedDomain
                            }
                            _ => D::Operational("Auto-sync accepted effects unsettled".into()),
                        });
                    }
                };
                flow.complete(report.value()).await.map_err(|_| {
                    D::Operational("Auto-sync full backend/process outcome unsettled".into())
                })?;
                Ok(D::Completed(context.id))
            }
            .await;
            if result.is_ok() {
                claim.armed = false
            }
            result
        })
        .await
        .map_err(|_| D::Operational("Auto-sync supervisor lost".into()))?
    }
}
impl Lifecycle for Prepared {
    fn stop_admission(&self) {
        self.inner.closed.store(true, Ordering::Release)
    }
    fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            self.stop_admission();
            let inner = self.inner.clone();
            let waited = tokio::time::timeout(timeout, async {
                loop {
                    let changed = inner.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if inner.active.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    changed.await;
                }
            })
            .await;
            if waited.is_err()
                || !inner
                    .uncertain
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_empty()
            {
                Err(D::Operational(
                    "Auto-sync physical/accepted scopes did not drain".into(),
                ))
            } else {
                Ok(())
            }
        })
    }
}
impl Handler for Prepared {
    fn maximum_concurrency(&self) -> usize {
        6
    }
    fn head_advance_notifier(&self) -> Option<Arc<Notify>> {
        Some(self.inner.heads.clone())
    }
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_once(&self, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.execute(None, owner))
    }
    fn run_selected(&self, raw: Arc<[u8]>, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.execute(Some(raw), owner))
    }
}
