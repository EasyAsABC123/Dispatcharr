//! Isolated, app-unwired recovery transition prototype.
//! Original Dispatcharr authors retain application/algorithm credit.
//!
//! Source172 keeps the old provider body unrepresented after Redis loss. A new
//! request creates a new public generation with active_streams=1. This component
//! deliberately does not rebuild public counts from old local bodies. It does
//! not yet integrate body delivery, lifecycle events or partial-state recovery.
use dispatcharr_clients::vod::{admit_fenced_physical, Admission, AdmissionFence};
use dispatcharr_contracts::UserLimitPolicy;
use dispatcharr_coordination::{AcquireRequest, Coordinator, Lease};
use redis::{aio::ConnectionManager, Script};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Capacity,
    Backend,
    ObserverTimeout,
    StillOwned,
    PartialState,
    ProfileFull,
    UnprovenEntry,
    ForeignOwner,
    Stopped,
    NotLost,
    Admission(dispatcharr_clients::Error),
    Acquisition(dispatcharr_coordination::Error),
}
#[derive(Clone)]
pub struct Recovery {
    inner: Arc<Inner>,
}
struct Inner {
    coordinator: Coordinator,
    connection: ConnectionManager,
    namespace: String,
    jobs: Arc<Semaphore>,
    observer_timeout: Duration,
    cleanup_failed: AtomicBool,
}
struct Context {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
/// Opaque local retired generation. It does not own public metadata or quota in
/// the new Redis state. The original body/byte cursor remains owned by its caller.
pub struct Retired {
    inner: Arc<Inner>,
    context: Arc<Context>,
    old: Arc<Lease>,
    id: String,
}
#[derive(Debug, PartialEq, Eq)]
pub enum MetadataCleanup {
    Deleted,
    NoMatchingMetadata,
    OwnerNotCurrent,
}
#[derive(Debug)]
pub struct CloseReport {
    pub metadata: MetadataCleanup,
    pub lease_released: bool,
}
pub struct NewGeneration {
    cleanup: Option<Cleanup>,
    _retired: Retired,
}
impl NewGeneration {
    pub fn metadata_token(&self) -> &str {
        &self.cleanup.as_ref().expect("live generation").token
    }
    pub fn owner_token(&self) -> &str {
        self.cleanup
            .as_ref()
            .expect("live generation")
            .lease
            .token()
    }
    pub async fn close(mut self) -> Result<CloseReport, Error> {
        self.cleanup.take().expect("live generation").finish().await
    }
}
impl Drop for NewGeneration {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            tokio::spawn(async move {
                let _ = cleanup.finish().await;
            });
        }
    }
}
struct Cleanup {
    inner: Arc<Inner>,
    context: Arc<Context>,
    pending_caller: Option<Arc<dyn Send + Sync>>,
    lease: Arc<Lease>,
    id: String,
    token: String,
}
impl Cleanup {
    async fn finish(self) -> Result<CloseReport, Error> {
        let _context = self.context;
        let _pending_caller = self.pending_caller;
        let mut connection = self.inner.connection.clone();
        // The prototype has exactly one new modeled body. This is owned final
        // teardown, not production idle cleanup or a generic session deletion.
        let metadata: Result<i64, _> = Script::new("if redis.call('GET',KEYS[2])~=ARGV[2] then return -1 end if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1] then return 0 end return redis.call('DEL',KEYS[1])")
            .key(format!("vod_persistent_connection:{}",self.id))
            .key(format!("{}:vod:session:{}:owner",self.inner.namespace,self.id))
            .arg(self.token).arg(self.lease.token())
            .invoke_async(&mut connection).await;
        let released = self.lease.release().await;
        self.lease.settlement().wait().await;
        if metadata.is_err() || released.is_err() {
            self.inner.cleanup_failed.store(true, Ordering::Release);
            return Err(Error::Backend);
        }
        let metadata = match metadata.map_err(|_| Error::Backend)? {
            1 => MetadataCleanup::Deleted,
            0 => MetadataCleanup::NoMatchingMetadata,
            -1 => MetadataCleanup::OwnerNotCurrent,
            _ => {
                self.inner.cleanup_failed.store(true, Ordering::Release);
                return Err(Error::Backend);
            }
        };
        Ok(CloseReport {
            metadata,
            lease_released: released.map_err(|_| Error::Backend)?,
        })
    }
}
/// Caller-supplied current authorization policy and exact Source metadata. The
/// new generation owns one public body, regardless of retired local bodies.
pub struct NewAdmission {
    pub fields: BTreeMap<String, String>,
    pub limit: u32,
    pub policy: UserLimitPolicy,
}
const CLASSIFY: &str = r#"
if redis.call('EXISTS',KEYS[4])~=0 then return 'STOP' end
local owner=redis.call('GET',KEYS[1])
local metadata=redis.call('HGET',KEYS[2],'rust_owner_token')
if owner and owner~=ARGV[1] then return 'FOREIGN' end
if redis.call('EXISTS',KEYS[2])~=0 and metadata~=ARGV[2] then return 'FOREIGN' end
if owner and metadata then return 'OWNED' end
if owner or metadata or redis.call('EXISTS',KEYS[3])~=0 then return 'PARTIAL' end
return 'ABSENT'
"#;
impl Recovery {
    pub fn new(
        coordinator: Coordinator,
        connection: ConnectionManager,
        namespace: String,
        max_pending: usize,
        observer_timeout: Duration,
    ) -> Result<Self, Error> {
        if namespace.is_empty()
            || namespace.len() > 128
            || max_pending == 0
            || max_pending > 1024
            || observer_timeout.is_zero()
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                coordinator,
                connection,
                namespace,
                jobs: Arc::new(Semaphore::new(max_pending)),
                observer_timeout,
                cleanup_failed: AtomicBool::new(false),
            }),
        })
    }
    pub fn cleanup_succeeded(&self) -> bool {
        !self.inner.cleanup_failed.load(Ordering::Acquire)
    }
    pub fn available_slots(&self) -> usize {
        self.inner.jobs.available_permits()
    }
    pub async fn retire(
        &self,
        id: String,
        old: Arc<Lease>,
        metadata_token: String,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Retired, Error> {
        if id.is_empty() || id.len() > 128 || id.contains(':') || metadata_token.len() > 128 {
            return Err(Error::Configuration);
        }
        // Sticky loss means automatic old heartbeat has stopped or is stopping.
        // Its receipt must still settle before classifying immutable identities.
        if !*old.lost().borrow() {
            return Err(Error::NotLost);
        }
        let slot = self
            .inner
            .jobs
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let context = Arc::new(Context {
            _slot: slot,
            _caller: caller,
        });
        let inner = self.inner.clone();
        let task = tokio::spawn(async move {
            old.settlement().wait().await;
            let mut connection = inner.connection.clone();
            let observed: String = Script::new(CLASSIFY)
                .key(format!("{}:vod:session:{id}:owner", inner.namespace))
                .key(format!("vod_persistent_connection:{id}"))
                .key(format!("{}:lease:{}", inner.namespace, old.token()))
                .key(format!("vod_proxy:client:{id}:stop"))
                .arg(old.token())
                .arg(metadata_token)
                .invoke_async(&mut connection)
                .await
                .map_err(|_| Error::Backend)?;
            match observed.as_str() {
                "ABSENT" => Ok(Retired {
                    inner,
                    context,
                    old,
                    id,
                }),
                "OWNED" => Err(Error::StillOwned),
                "PARTIAL" => Err(Error::PartialState),
                "FOREIGN" => Err(Error::ForeignOwner),
                "STOP" => Err(Error::Stopped),
                _ => Err(Error::Backend),
            }
        });
        tokio::time::timeout(self.inner.observer_timeout, task)
            .await
            .map_err(|_| Error::ObserverTimeout)?
            .map_err(|_| Error::Backend)?
    }
}
impl Retired {
    /// Fresh normal acquisition plus atomic current-owner/stop/user admission.
    /// No provider is reopened here and no old cursor is changed. The future is
    /// supervised so cancellation only abandons the observer.
    pub async fn reopen(
        self,
        request: AcquireRequest,
        admission: NewAdmission,
    ) -> Result<NewGeneration, Error> {
        self.reopen_scoped(request, admission, None, false).await
    }
    /// Retain this new request's scope through actual setup/ambiguous cleanup.
    /// Successful delivery releases this extra setup reference: the caller must
    /// independently hand its body permit into the delivered body/Ticket.
    pub async fn reopen_retained(
        self,
        request: AcquireRequest,
        admission: NewAdmission,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<NewGeneration, Error> {
        self.reopen_scoped(request, admission, Some(caller), false)
            .await
    }
    /// Opt-in Source body-stop admission; retain physical setup/cleanup owners.
    pub async fn reopen_source_retained(
        self,
        request: AcquireRequest,
        admission: NewAdmission,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<NewGeneration, Error> {
        self.reopen_scoped(request, admission, Some(caller), true)
            .await
    }
    async fn reopen_scoped(
        self,
        request: AcquireRequest,
        mut admission: NewAdmission,
        caller: Option<Arc<dyn Send + Sync>>,
        source_policy: bool,
    ) -> Result<NewGeneration, Error> {
        if request.channel_id != self.id
            || admission.fields.get("session_id") != Some(&self.id)
            || admission.fields.get("active_streams").map(String::as_str) != Some("1")
            || admission.fields.get("request_count").map(String::as_str) != Some("1")
        {
            return Err(Error::Configuration);
        }
        // Bound retained metadata before spawning/serializing any work. The
        // extra256 bytes cover the new private token inserted below.
        let metadata_bytes = admission
            .fields
            .iter()
            .try_fold(256usize, |total, (key, value)| {
                total.checked_add(key.len())?.checked_add(value.len())
            });
        if admission.fields.len() > 64
            || admission.fields.keys().any(|key| key.len() > 128)
            || metadata_bytes.is_none_or(|bytes| bytes > 65536)
        {
            return Err(Error::Capacity);
        }
        let timeout = self.inner.observer_timeout;
        let task = tokio::spawn(async move {
            let retained: Arc<dyn Send + Sync> = Arc::new((self.context.clone(), caller.clone()));
            let lease = Arc::new(
                self.inner
                    .coordinator
                    .acquire_vod_retained(request, retained)
                    .await
                    .map_err(Error::Acquisition)?,
            );
            let token = format!("rust-vod-metadata:{}", uuid::Uuid::new_v4());
            admission
                .fields
                .insert("rust_owner_token".into(), token.clone());
            let mut cleanup = Some(Cleanup {
                inner: self.inner.clone(),
                context: self.context.clone(),
                pending_caller: caller.clone(),
                lease: lease.clone(),
                id: self.id.clone(),
                token: token.clone(),
            });
            let owner = format!("{}:vod:session:{}:owner", self.inner.namespace, self.id);
            let stop = format!("vod_proxy:client:{}:stop", self.id);
            let mut connection = self.inner.connection.clone();
            let request = Admission {
                session_id: &self.id,
                fields: &admission.fields,
                limit: admission.limit,
                policy: &admission.policy,
                create: true,
                token: &token,
                scan_limit: 50000,
                timeout,
                ttl: Duration::from_secs(3600),
            };
            let fence = AdmissionFence {
                owner_key: &owner,
                owner_token: lease.token(),
                stop_key: &stop,
            };
            let outcome = if source_policy {
                dispatcharr_clients::vod::admit_source_owner_physical(
                    &mut connection,
                    request,
                    fence,
                )
                .await
            } else {
                admit_fenced_physical(&mut connection, request, fence).await
            };
            match outcome {
                Ok(true) => {
                    cleanup.as_mut().expect("owned setup").pending_caller.take();
                    Ok(ReopenDelivery {
                        generation: Some(NewGeneration {
                            cleanup: cleanup.take(),
                            _retired: self,
                        }),
                        caller,
                    })
                }
                error => {
                    let _ = cleanup.take().expect("owned setup").finish().await;
                    match error {
                        Err(error) => Err(Error::Admission(error)),
                        _ => Err(Error::ForeignOwner),
                    }
                }
            }
        });
        let delivery = tokio::time::timeout(timeout, task)
            .await
            .map_err(|_| Error::ObserverTimeout)?
            .map_err(|_| Error::Backend)??;
        Ok(delivery.finish())
    }
    pub fn old_token(&self) -> &str {
        self.old.token()
    }
}

// The receiver can disappear after actual ADMIT but before observing success.
// Keep the new request scope in delivery until an observer explicitly claims it.
struct ReopenDelivery {
    generation: Option<NewGeneration>,
    caller: Option<Arc<dyn Send + Sync>>,
}
impl ReopenDelivery {
    fn finish(mut self) -> NewGeneration {
        self.caller.take();
        self.generation
            .take()
            .expect("successful generation delivery")
    }
}
impl Drop for ReopenDelivery {
    fn drop(&mut self) {
        if let Some(generation) = self.generation.as_mut() {
            if let Some(cleanup) = generation.cleanup.as_mut() {
                cleanup.pending_caller = self.caller.take();
            }
        }
    }
}

// Included by multiple component fixtures; the dedicated LocalScope fixture
// exercises this opt-in surface. No production library/route registers it yet.
#[path = "generation_scope.rs"]
#[allow(dead_code)]
pub(crate) mod local;
