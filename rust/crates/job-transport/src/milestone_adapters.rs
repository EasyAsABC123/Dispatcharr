//! Existing normal workers retain their actual owners and typed preflight.
//! These adapters neither retry nor fabricate task metadata after an error.
use crate::milestone_dispatch::{Disposition, Handler, Lifecycle};
use dispatcharr_contracts::BoxFuture;
use std::sync::Arc;
impl Handler for crate::configured_recording_worker::Worker {
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            use crate::configured_recording_worker::Run;
            match crate::configured_recording_worker::Worker::run_once(self, owner).await {
                Ok(Run::Empty) => Ok(Disposition::Empty),
                Ok(Run::UnsupportedHead) => Ok(Disposition::UnsupportedHead),
                Ok(Run::Completed(id) | Run::AlreadySucceeded(id)) => {
                    Ok(Disposition::Completed(id))
                }
                Err(e) => Err(Disposition::Operational(format!("{e:?}"))),
            }
        })
    }
}
impl Handler for crate::refresh_worker::RefreshWorker {
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            use crate::refresh_worker::Run;
            match crate::refresh_worker::RefreshWorker::run_once(self, owner).await {
                Ok(Run::Empty) => Ok(Disposition::Empty),
                Ok(Run::Completed(id) | Run::ResultAlreadySucceeded(id)) => {
                    Ok(Disposition::Completed(id))
                }
                Err(e) => Err(Disposition::Operational(format!("{e:?}"))),
            }
        })
    }
}
impl Handler for crate::index_worker::IndexWorker {
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            use crate::index_worker::Run;
            match crate::index_worker::IndexWorker::run_once(self, owner).await {
                Ok(Run::Empty) => Ok(Disposition::Empty),
                Ok(Run::Completed(id) | Run::ResultAlreadySucceeded(id)) => {
                    Ok(Disposition::Completed(id))
                }
                Err(e) => Err(Disposition::Operational(format!("{e:?}"))),
            }
        })
    }
}
impl Handler for crate::recording_artwork_worker::Worker {
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            use crate::recording_artwork_worker::Run;
            match crate::recording_artwork_worker::Worker::run_once(self, owner).await {
                Ok(Run::Empty) => Ok(Disposition::Empty),
                Ok(Run::UnsupportedHead) => Ok(Disposition::UnsupportedHead),
                Ok(Run::NotDue) => Ok(Disposition::NotDue),
                Ok(
                    Run::Completed { task_id: id, .. } | Run::AlreadySucceeded { task_id: id, .. },
                ) => Ok(Disposition::Completed(id)),
                Err(e) => Err(Disposition::Operational(format!("{e:?}"))),
            }
        })
    }
}
impl Handler for crate::tvg_programme_worker::Worker {
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            use crate::tvg_programme_worker::Run;
            match crate::tvg_programme_worker::Worker::run_once(self, owner).await {
                Ok(Run::Empty) => Ok(Disposition::Empty),
                Ok(Run::UnsupportedHead) => Ok(Disposition::UnsupportedHead),
                Ok(Run::Completed(id)) => Ok(Disposition::Completed(id)),
                Err(e) => Err(Disposition::Operational(format!("{e:?}"))),
            }
        })
    }
}
impl Handler for crate::stats_worker::StatsWorker {
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            use crate::stats_worker::Run;
            match crate::stats_worker::StatsWorker::run_once(self, owner).await {
                Ok(Run::Empty) => Ok(Disposition::Empty),
                Ok(Run::Completed(id) | Run::AlreadySucceeded(id)) => {
                    Ok(Disposition::Completed(id))
                }
                Err(e) => Err(Disposition::Operational(format!("{e:?}"))),
            }
        })
    }
}
impl Handler for crate::default_worker::DefaultWorker {
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            use crate::default_worker::Run;
            match crate::default_worker::DefaultWorker::run_once(self, owner).await {
                Ok(Run::Empty) => Ok(Disposition::Empty),
                Ok(Run::UnsupportedHead) => Ok(Disposition::UnsupportedHead),
                Ok(Run::Unsupported(_)) => Ok(Disposition::UnsupportedDomain),
                Ok(Run::Completed(id) | Run::AlreadySucceeded(id)) => {
                    Ok(Disposition::Completed(id))
                }
                Ok(Run::Failed(id)) => Ok(Disposition::TaskFailed(id)),
                Ok(Run::Revoked(id, _)) => Ok(Disposition::Revoked(id)),
                Err(e) => Err(Disposition::Operational(format!("{e:?}"))),
            }
        })
    }
}
impl Handler for crate::recording_dvr::Dvr {
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            use crate::recording_dvr::Run;
            match crate::recording_dvr::Dvr::run_once(self, owner).await {
                Ok(Run::Empty) => Ok(Disposition::Empty),
                Ok(Run::UnsupportedHead) => Ok(Disposition::UnsupportedHead),
                Ok(Run::Completed { task, .. }) => Ok(Disposition::Completed(task)),
                Err(e) => Err(Disposition::Operational(format!("{e:?}"))),
            }
        })
    }
}
impl Handler for crate::scanner_m3u_worker::Worker {
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            use crate::scanner_m3u_worker::Run;
            match crate::scanner_m3u_worker::Worker::run_once(self, owner).await {
                Ok(Run::Empty) => Ok(Disposition::Empty),
                Ok(Run::Completed(id) | Run::ResultAlreadySucceeded(id)) => {
                    Ok(Disposition::Completed(id))
                }
                Err(e) => Err(Disposition::Operational(format!("{e:?}"))),
            }
        })
    }
}
impl Handler for crate::scanner_files_worker::Worker {
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            use crate::scanner_files_worker::Run;
            match crate::scanner_files_worker::Worker::run_once(self, owner).await {
                Ok(Run::Empty) => Ok(Disposition::Empty),
                Ok(Run::UnsupportedHead) => Ok(Disposition::UnsupportedHead),
                // The legacy scanner API returns no ID. Do not attribute a
                // possibly raced initial peek's ID to the consumed task.
                Ok(Run::Succeeded | Run::AlreadySucceeded) => Ok(Disposition::CompletedWithoutId),
                Ok(Run::Failed) => Ok(Disposition::TaskFailedWithoutId),
                Err(e) => Err(Disposition::Operational(format!("{e:?}"))),
            }
        })
    }
}

macro_rules! result_lifecycle {
    ($worker:ty,$method:ident) => {
        impl Lifecycle for $worker {
            fn shutdown(
                &self,
                timeout: std::time::Duration,
            ) -> BoxFuture<'_, Result<(), Disposition>> {
                Box::pin(async move {
                    <$worker>::$method(self, timeout)
                        .await
                        .map_err(|e| Disposition::Operational(format!("{e:?}")))
                })
            }
        }
    };
}
macro_rules! boolean_lifecycle {
    ($worker:ty) => {
        impl Lifecycle for $worker {
            fn shutdown(
                &self,
                timeout: std::time::Duration,
            ) -> BoxFuture<'_, Result<(), Disposition>> {
                Box::pin(async move {
                    if <$worker>::shutdown(self, timeout).await {
                        Ok(())
                    } else {
                        Err(Disposition::Operational(
                            "actual family remains unsettled".into(),
                        ))
                    }
                })
            }
        }
    };
}
result_lifecycle!(crate::configured_recording_worker::Worker, shutdown);
result_lifecycle!(crate::refresh_worker::RefreshWorker, shutdown);
result_lifecycle!(crate::index_worker::IndexWorker, shutdown);
result_lifecycle!(crate::recording_artwork_worker::Worker, shutdown);
result_lifecycle!(crate::tvg_programme_worker::Worker, shutdown);
result_lifecycle!(crate::scanner_m3u_worker::Worker, shutdown);
result_lifecycle!(crate::recording_dvr::Dvr, drain);
boolean_lifecycle!(crate::default_worker::DefaultWorker);
boolean_lifecycle!(crate::stats_worker::StatsWorker);
boolean_lifecycle!(crate::scanner_files_worker::Worker);
