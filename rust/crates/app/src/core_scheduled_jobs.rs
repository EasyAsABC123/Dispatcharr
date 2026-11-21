//! One Core Celery receiver and periodic scheduler for admitted ordinary families.
//! The original Dispatcharr and Celery authors retain full credit.
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_job_transport::{
    normal_backup::Application,
    normal_beat::{Scheduler, Status},
    normal_queue::CoreScheduledWorker,
    normal_revocation::Revoked,
    transport::{Config, Jobs},
};
use dispatcharr_store::{AuthService, Store};
use std::{env, ffi::OsStr, sync::Arc, time::Duration};

pub fn enabled() -> Result<bool, String> {
    match env::var_os("RUST_CORE_SCHEDULE_OWNER") {
        None => Ok(false),
        Some(value) if value == OsStr::new("rust") => Ok(true),
        Some(_) => Err("RUST_CORE_SCHEDULE_OWNER must be rust or unset".into()),
    }
}

pub fn restore_grace(core_enabled: bool, shutdown: Duration) -> Result<Option<Duration>, String> {
    match env::var_os("RUST_CORE_BACKUP_RESTORE_OWNER") {
        None => Ok(None),
        Some(value) => restore_grace_for(&value, core_enabled, shutdown).map(Some),
    }
}

fn restore_grace_for(
    value: &OsStr,
    core_enabled: bool,
    shutdown: Duration,
) -> Result<Duration, String> {
    if value != OsStr::new("rust") || !core_enabled {
        return Err(
            "RUST_CORE_BACKUP_RESTORE_OWNER requires rust and the Core schedule owner".into(),
        );
    }
    // Retain the existing restore and downstream drain bounds. Core must be
    // able to settle even a restore stopped during its full SQL admission wait.
    crate::manual_backup_config::restore_drain_grace(shutdown)
        .ok()
        .filter(|grace| *grace >= Duration::from_secs(30))
        .ok_or_else(|| "Core backup restore requires RUST_SHUTDOWN_SECONDS of at least 46".into())
}

#[derive(Clone)]
pub struct CoreScheduledJobs {
    worker: CoreScheduledWorker,
    scheduler: Scheduler,
}
impl CoreScheduledJobs {
    pub async fn prepare(
        store: Store,
        auth: AuthService,
        cache: Option<&str>,
        events: Arc<dyn UpdateEventSink>,
        version: dispatcharr_job_transport::version_service::Options,
        competing_owner: bool,
    ) -> Result<Self, String> {
        if competing_owner {
            return Err("Core scheduled jobs require exclusive application queue ownership".into());
        }
        let broker = env::var("CELERY_BROKER_URL")
            .map_err(|_| "Core scheduled jobs require an explicit CELERY_BROKER_URL")?;
        let result = env::var("CELERY_RESULT_BACKEND")
            .map_err(|_| "Core scheduled jobs require an explicit CELERY_RESULT_BACKEND")?;
        let cache = cache.ok_or("Core scheduled jobs require the actual cache")?;
        let (database, runtime, root, scratch) = crate::backup_config::configured()?;
        let jobs = Jobs::connect_defaults(&broker, &result, Config::default())
            .await
            .map_err(|_| "Core scheduled jobs transport initialization failed")?;
        let outcome = Self::assemble(
            jobs.clone(),
            store,
            auth,
            cache,
            events,
            version,
            database,
            runtime,
            root,
            scratch,
        )
        .await;
        if outcome.is_err() {
            jobs.shutdown(Duration::from_secs(4))
                .await
                .map_err(|_| "Core scheduled jobs preparation did not settle")?;
        }
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    async fn assemble(
        jobs: Jobs,
        store: Store,
        auth: AuthService,
        cache: &str,
        events: Arc<dyn UpdateEventSink>,
        version: dispatcharr_job_transport::version_service::Options,
        database: dispatcharr_job_transport::normal_backup::PgConfig,
        runtime: dispatcharr_job_transport::normal_backup::Runtime,
        root: std::path::PathBuf,
        scratch: std::path::PathBuf,
    ) -> Result<Self, String> {
        let identity = auth.producer_identity_source();
        let application = Application::prepare(
            jobs,
            Revoked::new(),
            store.clone(),
            auth,
            cache.to_owned(),
            database,
            runtime,
            root,
            scratch,
            identity.clone(),
        )
        .map_err(|_| "Core scheduled backup preparation failed")?;
        let application = Arc::new(application);
        let worker = CoreScheduledWorker::prepare(
            application.clone(),
            &store,
            events,
            version,
            Duration::from_millis(100),
        )
        .map_err(|_| "Core scheduled worker configuration failed")?;
        application
            .initialize_core_schedule(Arc::new(()))
            .await
            .map_err(|_| "Core backup schedule initialization failed")?;
        let scheduler = Scheduler::prepare_core_schedules(
            store.pool().clone(),
            identity,
            Duration::from_secs(5),
            worker.clone(),
        )
        .map_err(|_| "Core scheduler configuration failed")?;
        scheduler
            .initialize(Arc::new(()))
            .await
            .map_err(|_| "Core scheduler initialization failed")?;
        Ok(Self { worker, scheduler })
    }

    pub fn scheduler(&self) -> Scheduler {
        self.scheduler.clone()
    }

    pub fn backup_create_api(&self) -> Arc<dyn dispatcharr_store::BackupJobApi> {
        self.worker.backup_create_api()
    }

    pub fn bind_restore_plugins(
        &self,
        plugins: dispatcharr_store::PluginActions,
    ) -> Result<(), String> {
        self.worker
            .bind_restore_plugins(plugins)
            .map_err(|_| "Core backup restore plugin reconciliation binding failed".into())
    }

    pub fn ready(&self) -> bool {
        self.scheduler.core_schedule_ready()
    }

    pub async fn start(&self) -> Result<(), String> {
        self.worker
            .start(Arc::new(()))
            .map_err(|_| "Core scheduled worker did not start")?;
        let started = async {
            self.worker
                .await_ready(Duration::from_secs(8))
                .await
                .map_err(|_| "Core scheduled worker did not reach readiness")?;
            self.scheduler
                .start(Arc::new(()))
                .map_err(|_| "Core scheduler did not start")
        }
        .await;
        if let Err(reason) = started {
            self.shutdown(Duration::from_secs(8)).await?;
            return Err(reason.into());
        }
        Ok(())
    }

    pub fn stop_admission(&self) {
        self.scheduler.stop_admission();
        self.worker.stop_backup_admission();
    }

    pub async fn shutdown(&self, grace: Duration) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + grace;
        self.stop_admission();
        let publisher = self.scheduler.shutdown(grace).await;
        let publisher_faulted = matches!(self.scheduler.status(), Status::Fault(_));
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let worker = if publisher.is_ok() {
            self.worker.shutdown(remaining).await
        } else {
            self.worker.drain(remaining).await
        };
        publisher.map_err(|_| "Core periodic publisher did not settle")?;
        worker.map_err(|_| "Core scheduled worker did not settle")?;
        if publisher_faulted {
            return Err("Core periodic publisher faulted after startup".into());
        }
        Ok(())
    }
}

#[cfg(test)]
#[test]
fn core_restore_requires_its_owner_and_a_full_admission_budget() {
    let rust = OsStr::new("rust");
    assert!(restore_grace_for(rust, false, Duration::from_secs(136)).is_err());
    assert!(restore_grace_for(OsStr::new("yes"), true, Duration::from_secs(136)).is_err());
    assert!(restore_grace_for(rust, true, Duration::from_secs(45)).is_err());
    assert_eq!(
        restore_grace_for(rust, true, Duration::from_secs(46)),
        Ok(Duration::from_secs(30))
    );
    assert_eq!(
        restore_grace_for(rust, true, Duration::from_secs(240)),
        Ok(Duration::from_secs(120))
    );
}
