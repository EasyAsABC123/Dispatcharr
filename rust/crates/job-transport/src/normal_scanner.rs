//! Normal bounded scanner/local-M3U assembly; Original Dispatcharr authors retain credit.
use crate::{
    logo_task::Clock,
    milestone_dispatch::{Disposition, Family, Handler, Lifecycle},
    scanner_parents::Registry,
    transport::{Config, Jobs, M3uPorts, ScannerFilesPorts},
};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use dispatcharr_coordination::TaskLocks;
use dispatcharr_store::{AuthService, Store};
use std::{sync::Arc, time::Duration};
pub struct Prepared {
    scanner: crate::scanner_files_worker::Worker,
    m3u: crate::scanner_m3u_worker::Worker,
    service: crate::scanner_m3u::Service,
    _ports: ScannerFilesPorts,
    registry: Registry,
    xc: crate::normal_xc_live::Worker,
    remote: crate::normal_m3u_remote::Worker,
}
impl Prepared {
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        jobs: Jobs,
        store: Store,
        auth: AuthService,
        sink: Arc<dyn UpdateEventSink>,
        locks: TaskLocks,
        broker: &str,
        cache: &str,
        config: Config,
        roots: crate::logo_scanner::Roots,
        m3u_roots: crate::scanner_m3u::Roots,
        date: crate::scanner_m3u::DateClock,
        clock: Arc<dyn Clock>,
        revoked: crate::normal_revocation::Revoked,
    ) -> Result<Self, Disposition> {
        if !auth.uses_store_pool(&store) {
            return Err(Disposition::UnsupportedDomain);
        }
        let registry = Registry::new(jobs.clone());
        let ports = ScannerFilesPorts::connect_shared(
            jobs.clone(),
            broker,
            cache,
            config,
            auth.producer_identity_source(),
        )
        .await
        .map_err(|e| Disposition::Operational(format!("Scanner transport: {e:?}")))?;
        let scan = crate::scanner_files::Service::new_mixed(
            store.pool().clone(),
            ports.clone(),
            sink.clone(),
            roots,
        )
        .map_err(|e| Disposition::Operational(format!("Scanner configuration: {e:?}")))?;
        let scanner = crate::scanner_files_worker::Worker::new_tracked(
            scan,
            ports.clone(),
            clock.clone(),
            registry.clone(),
        );
        let m3u_ports = M3uPorts::from_shared(jobs.clone(), ports.marker_transport());
        let service = crate::scanner_m3u::Service::new_owned_shared(
            store.clone(),
            auth.clone(),
            sink.clone(),
            locks.clone(),
            m3u_ports.clone(),
            m3u_roots.clone(),
            date,
            clock,
        )
        .map_err(|e| Disposition::Operational(format!("M3U configuration: {e:?}")))?;
        let m3u = crate::scanner_m3u_worker::Worker::new_normal(
            service.clone(),
            m3u_ports.clone(),
            registry.clone(),
        );
        let xc = crate::normal_xc_live::Worker::new(
            jobs.clone(),
            m3u_ports.clone(),
            store.clone(),
            auth.clone(),
            sink.clone(),
            locks.clone(),
            m3u.admission(),
            revoked.clone(),
            m3u_roots.cache.clone(),
        );
        let remote = crate::normal_m3u_remote::Worker::new(
            jobs,
            m3u_ports,
            store,
            auth,
            sink,
            locks,
            revoked,
            m3u.admission(),
            m3u_roots.cache,
        );
        Ok(Self {
            remote,
            xc,
            scanner,
            m3u,
            service,
            _ports: ports,
            registry,
        })
    }
    pub fn registry(&self) -> Registry {
        self.registry.clone()
    }
    pub fn entries(&self) -> Vec<(Family, Arc<dyn Handler>)> {
        let m3u: Arc<dyn Handler> =
            Arc::new(M3u(self.m3u.clone(), self.xc.clone(), self.remote.clone()));
        vec![
            (Family::Scanner, Arc::new(Scanner(self.scanner.clone()))),
            (Family::M3uGroups, m3u.clone()),
            (Family::M3uSingle, m3u),
        ]
    }
    /// All queue lanes must stop first. Shared Jobs is closed only by Main after
    /// every other family has physically drained; this assembly never closes it.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Disposition> {
        let end = tokio::time::Instant::now() + timeout;
        self.scanner.stop_admission();
        self.m3u.stop_admission();
        self.xc.stop();
        self.remote.stop();
        if !self
            .scanner
            .shutdown_shared(end.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return Err(Disposition::Operational("Scanner drain".into()));
        }
        self.m3u
            .shutdown(end.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .map_err(|e| Disposition::Operational(format!("M3U intake drain: {e:?}")))?;
        if !self
            .xc
            .shutdown(end.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return Err(Disposition::Operational("XC live drain".into()));
        }
        if !self
            .remote
            .shutdown(end.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return Err(Disposition::Operational("Remote M3U drain".into()));
        }
        self.service
            .shutdown(end.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .map_err(|e| Disposition::Operational(format!("M3U session drain: {e:?}")))?;
        // Redis commands retain the same intake/operation owner until their
        // actual response; service drains precede Main's shared Jobs shutdown.
        // Existing Redis ConnectionManager has no explicit socket-close API;
        // connection-drop teardown remains independently observed by Compose.
        Ok(())
    }
}
struct Scanner(crate::scanner_files_worker::Worker);
impl Lifecycle for Scanner {
    fn stop_admission(&self) {
        self.0.stop_admission();
    }
    fn shutdown(&self, t: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
        Box::pin(async move {
            if self.0.shutdown_shared(t).await {
                Ok(())
            } else {
                Err(Disposition::Operational("Scanner drain".into()))
            }
        })
    }
}
impl Scanner {
    fn mapped(
        &self,
        result: Result<crate::scanner_files_worker::Run, crate::scanner_files::Error>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            use crate::scanner_files_worker::Run;
            match result {
                Ok(Run::Empty) => Ok(Disposition::Empty),
                Ok(Run::UnsupportedHead) => Err(Disposition::UnsupportedHead),
                Ok(Run::Failed) => Ok(Disposition::TaskFailedWithoutId),
                Ok(_) => Ok(Disposition::CompletedWithoutId),
                Err(crate::scanner_files::Error::Unsupported) => {
                    Err(Disposition::UnsupportedDomain)
                }
                Err(crate::scanner_files::Error::HeadChanged) => Ok(Disposition::HeadChanged),
                Err(crate::scanner_files::Error::Capacity) => Ok(Disposition::Capacity),
                Err(e) => Err(Disposition::Operational(format!("Scanner: {e:?}"))),
            }
        })
    }
}
impl Handler for Scanner {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            self.mapped(self.0.run_selected(expected, owner).await)
                .await
        })
    }
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move { self.mapped(self.0.run_once(owner).await).await })
    }
}

struct M3u(
    crate::scanner_m3u_worker::Worker,
    crate::normal_xc_live::Worker,
    crate::normal_m3u_remote::Worker,
);
impl Lifecycle for M3u {
    fn stop_admission(&self) {
        self.0.stop_admission();
        self.1.stop();
        self.2.stop();
    }
    fn shutdown(&self, t: Duration) -> BoxFuture<'_, Result<(), Disposition>> {
        Box::pin(async move {
            self.0
                .shutdown(t)
                .await
                .map_err(|e| Disposition::Operational(format!("M3U drain: {e:?}")))
        })
    }
}
impl M3u {
    fn mapped(
        &self,
        result: Result<crate::scanner_m3u_worker::Run, crate::scanner_m3u_worker::Error>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            use crate::scanner_m3u_worker::{Error, Run};
            match result {
                Ok(Run::Empty) => Ok(Disposition::Empty),
                Ok(Run::Failed(id)) => Ok(Disposition::TaskFailed(id)),
                Ok(Run::Completed(id) | Run::ResultAlreadySucceeded(id)) => {
                    Ok(Disposition::Completed(id))
                }
                Err(Error::Transport(crate::transport::Error::UnsupportedTask)) => {
                    Err(Disposition::UnsupportedHead)
                }
                Err(Error::Domain(crate::scanner_m3u::Error::Unsupported)) => {
                    Err(Disposition::UnsupportedDomain)
                }
                Err(crate::scanner_m3u_worker::Error::HeadChanged) => Ok(Disposition::HeadChanged),
                Err(crate::scanner_m3u_worker::Error::Capacity) => Ok(Disposition::Capacity),
                Err(e) => Err(Disposition::Operational(format!("M3U: {e:?}"))),
            }
        })
    }
}

impl Handler for M3u {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            if let Some(result) = self.1.run(Some(expected.clone()), owner.clone()).await? {
                return Ok(result);
            }
            if let Some(result) = self.2.run(Some(expected.clone()), owner.clone()).await? {
                return Ok(result);
            }
            self.mapped(self.0.run_selected(expected, owner).await)
                .await
        })
    }
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Disposition, Disposition>> {
        Box::pin(async move {
            if let Some(result) = self.1.run(None, owner.clone()).await? {
                return Ok(result);
            }
            if let Some(result) = self.2.run(None, owner.clone()).await? {
                return Ok(result);
            }
            self.mapped(self.0.run_once(owner).await).await
        })
    }
}
