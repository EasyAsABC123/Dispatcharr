//! Aggregate all-active refresh on existing M3U worker slots and shared Jobs.
//! Original Dispatcharr/Celery authors retain application/algorithm credit.
use super::*;
use crate::m3u_refresh_all_protocol::Terminal;
use crate::milestone_dispatch::{Handler, Lifecycle};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::m3u_refresh_all::{self as service, RefreshChildPublisher};

struct All(Worker, crate::scanner_m3u::Service);
struct Admission(crate::scanner_m3u::Service, Arc<Inner>);
struct Children {
    inner: Arc<Inner>,
    parent: Uuid,
    _claimed_raw: Vec<u8>,
    admission: Admission,
    acknowledged: Mutex<Vec<Uuid>>,
    attempted: Mutex<Vec<(Uuid, Vec<u8>)>>,
}
impl service::ChildAdmission for Admission {
    fn admit_single(
        &self,
        account: i64,
        owner: Owner,
    ) -> BoxFuture<'_, Result<(), service::AdmissionError>> {
        Box::pin(async move {
            if self.1.closed.load(Ordering::Acquire) {
                return Err(service::AdmissionError::Draining);
            }
            let observed = dispatcharr_store::xc_live_tasks::admit_refresh_single(
                self.1.store.clone(),
                self.1.auth.clone(),
                account,
                owner.clone(),
                self.1.events.clone(),
                self.1.auto_jobs.is_some(),
            )
            .await
            .map_err(|error| match error {
                dispatcharr_store::xc_live_tasks::ChildAdmissionError::Unsupported => {
                    service::AdmissionError::Unsupported
                }
                dispatcharr_store::xc_live_tasks::ChildAdmissionError::Operational(error) => {
                    service::AdmissionError::Operational(error)
                }
            })?;
            match observed {
                dispatcharr_store::xc_live_tasks::SingleAdmission::Xc => {
                    let path = self.1.cache_root.join(format!("{account}.json"));
                    let retained = owner.clone();
                    let absent = tokio::task::spawn_blocking(move || {
                        let _owner = retained;
                        matches!(std::fs::symlink_metadata(path), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
                    })
                    .await
                    .map_err(|_| {
                        service::AdmissionError::Operational(StoreError::Unsupported(
                            "Aggregate XC cache observation unsettled",
                        ))
                    })?;
                    if !absent {
                        return Err(service::AdmissionError::Unsupported);
                    }
                }
                dispatcharr_store::xc_live_tasks::SingleAdmission::MissingStdInput => {
                    crate::normal_m3u_remote::admit_missing_input_cache(
                        self.1.cache_root.clone(),
                        account,
                        owner.clone(),
                    )
                    .await
                    .map_err(|error| match error {
                        D::UnsupportedDomain => service::AdmissionError::Unsupported,
                        _ => service::AdmissionError::Operational(StoreError::Unsupported(
                            "Aggregate missing-input cache admission unsettled",
                        )),
                    })?;
                }
                dispatcharr_store::xc_live_tasks::SingleAdmission::RemoteStd => {}
                dispatcharr_store::xc_live_tasks::SingleAdmission::LocalStd => {
                    self.0
                        .admit_single(account, owner)
                        .await
                        .map_err(|error| match error {
                            crate::scanner_m3u::Error::Unsupported => {
                                service::AdmissionError::Unsupported
                            }
                            crate::scanner_m3u::Error::Capacity => {
                                service::AdmissionError::Operational(StoreError::StatusBody(
                                    503,
                                    json!({"error":"Aggregate M3U child admission capacity"}),
                                ))
                            }
                            _ => service::AdmissionError::Operational(StoreError::Unsupported(
                                "Aggregate M3U child physical admission unsettled",
                            )),
                        })?;
                }
            }
            if self.1.closed.load(Ordering::Acquire) {
                return Err(service::AdmissionError::Draining);
            }
            Ok(())
        })
    }
}
impl RefreshChildPublisher for Children {
    fn publish_single(
        &self,
        account: i64,
        owner: Owner,
    ) -> BoxFuture<'_, Result<Uuid, StoreError>> {
        Box::pin(async move {
            if self.inner.closed.load(Ordering::Acquire) {
                return Err(StoreError::Unsupported(
                    "Aggregate M3U claimed publication interrupted by drain",
                ));
            }
            // Recheck THIS child's actual domain immediately before publication.
            service::ChildAdmission::admit_single(&self.admission, account, owner.clone())
                .await
                .map_err(|_| {
                    StoreError::Unsupported("Aggregate M3U child domain changed during publication")
                })?;
            if self.inner.closed.load(Ordering::Acquire) {
                return Err(StoreError::Unsupported(
                    "Aggregate M3U claimed publication interrupted after admission",
                ));
            }
            let identity = self.inner.identity.current()?;
            let (id, raw) =
                crate::m3u_refresh_all_protocol::encode_child(self.parent, account, &identity)
                    .map_err(|_| StoreError::Unsupported("Aggregate M3U child envelope"))?;
            self.attempted
                .lock()
                .map_err(|_| StoreError::Unsupported("Aggregate M3U attempted trail poisoned"))?
                .push((id, raw.clone()));
            let receipt = self
                .inner
                .jobs
                .publish_m3u_all_frame(&raw, Some(self.parent), owner)
                .await
                .map_err(|_| {
                    StoreError::Unsupported("Aggregate M3U child publication unsettled")
                })?;
            if receipt.task_id != id {
                return Err(StoreError::Unsupported(
                    "Aggregate M3U child receipt identity",
                ));
            }
            self.acknowledged
                .lock()
                .map_err(|_| StoreError::Unsupported("Aggregate M3U trail poisoned"))?
                .push(id);
            Ok(id)
        })
    }
}
impl Worker {
    pub(crate) fn all_handler(&self, service: crate::scanner_m3u::Service) -> Arc<dyn Handler> {
        Arc::new(All(self.clone(), service))
    }
    async fn run_all(
        &self,
        expected: Option<Arc<[u8]>>,
        caller: Owner,
        service: crate::scanner_m3u::Service,
    ) -> Result<D, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Ok(D::Empty);
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| D::Capacity)?;
        let owner: Owner = Arc::new((slot, caller, self.inner.clone()));
        let inner = self.inner.clone();
        let failure_inner = inner.clone();
        let failure_owner = owner.clone();
        let joined = tokio::spawn(async move {
            let mut guard = FaultGuard {
                inner: inner.clone(),
                owner: owner.clone(),
                armed: true,
            };
            let admission = Admission(service.clone(), inner.clone());
            let result = async {
                let candidate = match inner
                    .jobs
                    .peek_m3u_all(expected.as_deref(), owner.clone())
                    .await
                {
                    Ok(Some(value)) => value,
                    Ok(None) => return Ok(D::Empty),
                    Err(crate::transport::Error::ClaimRace) => return Ok(D::HeadChanged),
                    Err(crate::transport::Error::UnsupportedTask) => {
                        return Err(D::UnsupportedHead)
                    }
                    Err(_) => return Err(D::Operational("Aggregate M3U head observation".into())),
                };
                let revoked = inner
                    .revoked
                    .contains(&candidate.id.to_string())
                    .map_err(|_| D::Operational("Aggregate M3U revoked membership".into()))?;
                let prepared = if revoked {
                    None
                } else {
                    match service::preflight(&inner.auth, &admission, owner.clone()).await {
                        Ok(prepared) => Some(prepared),
                        Err(service::AdmissionError::Draining) => return Ok(D::Empty),
                        Err(service::AdmissionError::Unsupported) => {
                            return Err(D::UnsupportedDomain)
                        }
                        Err(service::AdmissionError::Operational(StoreError::StatusBody(
                            503,
                            _,
                        ))) => return Ok(D::Capacity),
                        Err(_) => return Err(D::Operational("Aggregate M3U SQL admission".into())),
                    }
                };
                // A stopped preclaim admission must leave the exact FIFO head.
                if inner.closed.load(Ordering::Acquire) {
                    return Ok(D::Empty);
                }
                let claimed_raw = candidate.raw.clone();
                guard.owner = Arc::new((owner.clone(), candidate.id, claimed_raw.clone()));
                let id = match inner.jobs.claim_m3u_all(candidate, owner.clone()).await {
                    Ok(id) => id,
                    Err(crate::transport::Error::ClaimRace) => return Ok(D::HeadChanged),
                    Err(_) => return Err(D::Operational("Aggregate M3U claim outcome".into())),
                };
                if revoked {
                    if !inner
                        .jobs
                        .write_m3u_all_terminal(id, Terminal::Revoked, Vec::new(), owner.clone())
                        .await
                        .map_err(|_| D::Operational("Aggregate M3U revoked backend".into()))?
                    {
                        return Err(D::Operational(
                            "Aggregate M3U revoked backend rejected".into(),
                        ));
                    }
                    return Ok(D::Revoked(id));
                }
                let children = Arc::new(Children {
                    inner: inner.clone(),
                    parent: id,
                    _claimed_raw: claimed_raw,
                    admission: Admission(service.clone(), inner.clone()),
                    acknowledged: Mutex::new(Vec::new()),
                    attempted: Mutex::new(Vec::new()),
                });
                guard.owner = Arc::new((owner.clone(), children.clone()));
                let executed = service::execute(
                    &inner.auth,
                    children.as_ref(),
                    &admission,
                    prepared.ok_or_else(|| {
                        D::Operational("Aggregate M3U actual preparation absent".into())
                    })?,
                    owner.clone(),
                )
                .await;
                let (result, failure) = match executed {
                    Ok(value) => (value, None),
                    Err(service::ExecutionError::RaisedSql(error)) => {
                        let pg = error
                            .as_database_error()
                            .and_then(|e| e.try_downcast_ref::<sqlx::postgres::PgDatabaseError>())
                            .ok_or_else(|| {
                                D::Operational("Aggregate M3U unclassified SQL failure".into())
                            })?;
                        if pg.severity() != sqlx::postgres::PgSeverity::Error {
                            return Err(D::Operational(
                                "Aggregate M3U SQL severity unsettled".into(),
                            ));
                        }
                        let message = crate::pg_failure_format::render_pg(pg).map_err(|_| {
                            D::Operational("Aggregate M3U SQL diagnostic unsupported".into())
                        })?;
                        (Value::Null, Some(message))
                    }
                    Err(_) => {
                        return Err(D::Operational("Aggregate M3U execution unsettled".into()))
                    }
                };
                let trail = children
                    .acknowledged
                    .lock()
                    .map_err(|_| D::Operational("Aggregate M3U trail poisoned".into()))?
                    .clone();
                let failed = failure.is_some();
                if !inner
                    .jobs
                    .write_m3u_all_terminal(
                        id,
                        if let Some(message) = failure {
                            Terminal::Failure(message)
                        } else {
                            Terminal::Success(result)
                        },
                        trail,
                        owner.clone(),
                    )
                    .await
                    .map_err(|_| D::Operational("Aggregate M3U terminal backend".into()))?
                {
                    return Err(D::Operational(
                        "Aggregate M3U terminal backend rejected".into(),
                    ));
                }
                Ok(if failed {
                    D::TaskFailed(id)
                } else {
                    D::Completed(id)
                })
            }
            .await;
            if !matches!(&result, Err(D::Operational(_))) {
                guard.armed = false;
            }
            result
        })
        .await;
        match joined {
            Ok(result) => result,
            Err(_) => {
                let mut uncertain = failure_inner
                    .uncertain
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if !uncertain
                    .iter()
                    .any(|value| Arc::ptr_eq(value, &failure_owner))
                {
                    uncertain.push(failure_owner);
                }
                Err(D::Operational("Aggregate M3U owner join unsettled".into()))
            }
        }
    }
}
impl Lifecycle for All {
    fn stop_admission(&self) {
        self.0.stop();
    }
    fn begin_drain(&self, budget: Duration) {
        self.0.begin_drain(budget);
    }
    fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            if self.0.shutdown(timeout).await {
                Ok(())
            } else {
                Err(D::Operational("Aggregate M3U drain unsettled".into()))
            }
        })
    }
}
impl Handler for All {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_selected(&self, expected: Arc<[u8]>, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.0.run_all(Some(expected), owner, self.1.clone()))
    }
    fn run_once(&self, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.0.run_all(None, owner, self.1.clone()))
    }
}
