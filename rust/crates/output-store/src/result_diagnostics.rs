//! Fixed-vocabulary result/artwork diagnostics, never request/Redis error text.
//! Original Dispatcharr/dependency authors retain application/protocol credit.
use std::{
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};
#[derive(Clone, Copy)]
pub enum CommandDiagnostic {
    DvrResult { task: u128 },
    ArtworkPublication { task: u128 },
    RecordingPublication { task: u128 },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum Kind {
    None,
    Success,
    LogicalTimeout,
    RedisDisconnect,
    RedisTimeout,
    RedisIo,
    RedisDecode,
    RedisAuth,
    RedisClient,
    RedisServer,
    Capacity,
    Configuration,
    SupervisorFailure,
    Unobserved,
}
impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Success => "success",
            Self::LogicalTimeout => "logical_timeout",
            Self::RedisDisconnect => "redis_disconnect",
            Self::RedisTimeout => "redis_timeout",
            Self::RedisIo => "redis_io",
            Self::RedisDecode => "redis_decode",
            Self::RedisAuth => "redis_auth",
            Self::RedisClient => "redis_client",
            Self::RedisServer => "redis_server",
            Self::Capacity => "capacity",
            Self::Configuration => "configuration",
            Self::SupervisorFailure => "supervisor_failure",
            Self::Unobserved => "unobserved",
        }
    }
}
pub(super) fn redis_kind(error: &redis::RedisError) -> Kind {
    if error.is_connection_dropped() {
        Kind::RedisDisconnect
    } else if error.is_timeout() {
        Kind::RedisTimeout
    } else if error.is_io_error() {
        Kind::RedisIo
    } else {
        match error.kind() {
            redis::ErrorKind::ParseError | redis::ErrorKind::TypeError => Kind::RedisDecode,
            redis::ErrorKind::AuthenticationFailed => Kind::RedisAuth,
            redis::ErrorKind::InvalidClientConfig | redis::ErrorKind::ClientError => {
                Kind::RedisClient
            }
            _ => Kind::RedisServer,
        }
    }
}
#[derive(Clone, Copy)]
pub(super) enum Stage {
    Admission,
    AdmissionRejected,
    CommandStarted,
    PhysicalReturned,
    LogicalDeadline,
    ObserverDropped,
    PhysicalObserverDropped,
    SupervisorFailed,
}
impl Stage {
    fn name(self) -> &'static str {
        match self {
            Self::Admission => "admission",
            Self::AdmissionRejected => "admission_rejected",
            Self::CommandStarted => "command_started",
            Self::PhysicalReturned => "physical_returned",
            Self::LogicalDeadline => "logical_deadline",
            Self::ObserverDropped => "observer_dropped",
            Self::PhysicalObserverDropped => "physical_observer_dropped",
            Self::SupervisorFailed => "supervisor_failed",
        }
    }
}
static RECORDS: AtomicUsize = AtomicUsize::new(0);
pub(super) struct Trace {
    scope: CommandDiagnostic,
    start: Instant,
    // queued -> invocation running -> actual reply/error observed, or dropped
    // without an observed reply. This is NOT proof all service owners drained.
    phase: AtomicU8,
    pub(super) physical_kind: AtomicU8,
}
impl Trace {
    pub(super) fn new(scope: CommandDiagnostic) -> Arc<Self> {
        Arc::new(Self {
            scope,
            start: Instant::now(),
            phase: AtomicU8::new(0),
            physical_kind: AtomicU8::new(Kind::None as u8),
        })
    }
    pub(super) fn emit(&self, stage: Stage, kind: Kind) {
        if RECORDS
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < 64).then_some(n + 1)
            })
            .is_err()
        {
            return;
        }
        let phase = self.phase.load(Ordering::Acquire);
        let physical = match phase {
            0 => "queued",
            1 => "running",
            2 => "returned",
            _ => "unobserved_drop",
        };
        let pending = matches!(phase, 0 | 1);
        let (scope, task) = match self.scope {
            CommandDiagnostic::DvrResult { task } => ("dvr_result", task),
            CommandDiagnostic::ArtworkPublication { task } => ("artwork_publication", task),
            CommandDiagnostic::RecordingPublication { task } => ("recording_publication", task),
        };
        eprintln!("{scope} task_hex={task:032x} stage={} kind={} elapsed_ms={} physical={physical} physical_pending={pending}",stage.name(),kind.name(),self.start.elapsed().as_millis());
    }
}
pub(super) struct PhysicalObservation {
    trace: Option<Arc<Trace>>,
    finished: bool,
}
impl PhysicalObservation {
    pub(super) fn start(trace: Option<Arc<Trace>>) -> Self {
        if let Some(t) = &trace {
            t.phase.store(1, Ordering::Release);
            t.emit(Stage::CommandStarted, Kind::None);
        }
        Self {
            trace,
            finished: false,
        }
    }
    pub(super) fn returned<T>(&mut self, result: &redis::RedisResult<T>) {
        if let Some(t) = &self.trace {
            let kind = match result {
                Ok(_) => Kind::Success,
                Err(error) => redis_kind(error),
            };
            t.physical_kind.store(kind as u8, Ordering::Release);
            t.phase.store(2, Ordering::Release);
            t.emit(Stage::PhysicalReturned, kind);
        }
        self.finished = true;
    }
}
impl Drop for PhysicalObservation {
    fn drop(&mut self) {
        if !self.finished {
            if let Some(t) = &self.trace {
                t.phase.store(3, Ordering::Release);
                t.emit(Stage::PhysicalObserverDropped, Kind::Unobserved);
            }
        }
    }
}
pub(super) struct LogicalObservation {
    trace: Option<Arc<Trace>>,
    pub(super) finished: bool,
}
impl LogicalObservation {
    pub(super) fn new(trace: Option<Arc<Trace>>) -> Self {
        Self {
            trace,
            finished: false,
        }
    }
}
impl Drop for LogicalObservation {
    fn drop(&mut self) {
        if !self.finished {
            if let Some(t) = &self.trace {
                t.emit(Stage::ObserverDropped, Kind::Unobserved);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use tokio::sync::{oneshot, Notify, Semaphore};
    struct Guard(Arc<AtomicBool>, Arc<Notify>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
            self.1.notify_one();
        }
    }
    #[tokio::test]
    async fn logical_timeout_keeps_real_owned_task_until_late_disconnect() {
        let trace = Trace::new(CommandDiagnostic::DvrResult { task: 1 });
        let dropped = Arc::new(AtomicBool::new(false));
        let done = Arc::new(Notify::new());
        let guard = Guard(dropped.clone(), done.clone());
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        let (release, wait) = oneshot::channel::<()>();
        let actual_trace = trace.clone();
        let task = tokio::spawn(async move {
            let mut physical = PhysicalObservation::start(Some(actual_trace));
            wait.await.unwrap();
            let result: redis::RedisResult<i64> = Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "SENTINEL_NEVER_LOG_ERROR_TEXT",
            )
            .into());
            physical.returned(&result);
            (result, guard, permit)
        });
        let result = super::super::await_owned_command(
            task,
            std::time::Duration::from_millis(10),
            Some(trace.clone()),
        )
        .await;
        assert!(matches!(result, Err(super::super::OperationError::Backend)));
        assert!(!dropped.load(Ordering::Acquire));
        assert_eq!(trace.phase.load(Ordering::Acquire), 1);
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !dropped.load(Ordering::Acquire) {
                done.notified().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(trace.phase.load(Ordering::Acquire), 2);
        assert_eq!(
            trace.physical_kind.load(Ordering::Acquire),
            Kind::RedisDisconnect as u8
        );
    }
    #[test]
    fn physical_failures_have_only_fixed_kinds() {
        let error: redis::RedisError =
            std::io::Error::new(std::io::ErrorKind::TimedOut, "SENTINEL_URL_AND_PAYLOAD").into();
        assert_eq!(redis_kind(&error), Kind::RedisTimeout);
        let error: redis::RedisError =
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "SENTINEL_CREDENTIAL").into();
        assert_eq!(redis_kind(&error), Kind::RedisIo);
        let error =
            redis::RedisError::from((redis::ErrorKind::ResponseError, "SENTINEL_SERVER_PAYLOAD"));
        assert_eq!(redis_kind(&error), Kind::RedisServer);
        let error =
            redis::RedisError::from((redis::ErrorKind::TypeError, "SENTINEL_SECRET_RESULT_BYTES"));
        assert_eq!(redis_kind(&error), Kind::RedisDecode);
        let error = redis::RedisError::from((
            redis::ErrorKind::AuthenticationFailed,
            "SENTINEL_SECRET_PASSWORD",
        ));
        assert_eq!(redis_kind(&error), Kind::RedisAuth);
        let error =
            redis::RedisError::from((redis::ErrorKind::InvalidClientConfig, "SENTINEL_SECRET_URL"));
        assert_eq!(redis_kind(&error), Kind::RedisClient);
    }
}
