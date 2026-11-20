//! Narrow aggregate read/CAS/publication draft on SAME Main Jobs Redis ports.
//! Original Dispatcharr/Celery/Kombu authors retain application/protocol credit.
use super::*;
use crate::m3u_refresh_all_protocol as aggregate;
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::{xc_account_http, AuthService, ProducerIdentitySource, StoreError};
use serde_json::json;

async fn publish_m3u_frame<G: Send + 'static>(
    broker: &RedisTransport,
    raw: &[u8],
    id: Uuid,
    max_queued_jobs: usize,
    owner: G,
) -> Result<Published, Error> {
    let maximum = max_queued_jobs.to_string();
    match broker
        .command_classified::<i64, _>(
            PUBLISH,
            &["celery".into(), "_kombu.binding.celery".into()],
            &[raw, maximum.as_bytes()],
            owner,
        )
        .await
    {
        Ok((n, _)) if n > 0 => Ok(Published {
            task_id: id,
            queue_length: n as u64,
        }),
        Ok((-2, _)) => Err(Error::Capacity),
        Ok(_) => Err(Error::Corrupt),
        Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(id)),
        Err(error) => Err(map(error)),
    }
}

/// Aggregate HTTP publication for an explicitly retained Original worker.
/// This owns only the broker connection. It never starts queue intake or
/// connects to a result backend.
#[derive(Clone)]
pub struct OriginalM3uRefreshAll {
    http: xc_account_http::Http,
    publisher: Arc<OriginalPublisher>,
}

struct OriginalPublisher {
    auth: AuthService,
    identity: ProducerIdentitySource,
    broker: RedisTransport,
    max_queued_jobs: usize,
    uncertain: AtomicBool,
}

impl OriginalM3uRefreshAll {
    pub async fn connect(auth: AuthService, broker: &str) -> Result<Self, Error> {
        if broker.trim().is_empty() || broker.len() > 4096 {
            return Err(Error::Configuration);
        }
        let config = Config::default();
        let broker = RedisTransport::connect_classified(
            broker,
            TransportConfig {
                max_operations: config.operations,
                max_queued_operations: config.operations,
                command_timeout: config.command_timeout,
            },
        )
        .await
        .map_err(map)?;
        let publisher = Arc::new(OriginalPublisher {
            identity: auth.producer_identity_source(),
            auth: auth.clone(),
            broker,
            max_queued_jobs: config.max_queued_jobs,
            uncertain: AtomicBool::new(false),
        });
        let http = xc_account_http::Http::prepare(auth, publisher.clone());
        Ok(Self { http, publisher })
    }

    pub fn router(&self) -> axum::Router {
        self.http.clone().refresh_all_router()
    }

    pub fn close_admission(&self) {
        self.http.close_admission();
    }

    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.http.shutdown(timeout).await && !self.publisher.uncertain.load(Ordering::Acquire)
    }
}

impl xc_account_http::Publisher for OriginalPublisher {
    fn publish_all_refresh(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            if self.uncertain.load(Ordering::Acquire) {
                return Err(publication_unsettled());
            }
            dispatcharr_store::m3u_refresh_all::require_supported_domain(&self.auth, owner.clone())
                .await?;
            let identity = self.identity.current()?;
            let id = Uuid::new_v4();
            let raw = aggregate::encode_root(id, &identity)
                .map_err(|_| StoreError::Unsupported("M3U aggregate root envelope"))?;
            if self.uncertain.load(Ordering::Acquire) {
                return Err(publication_unsettled());
            }
            // The shared transport holds the HTTP admission guard until the
            // physical reply or disconnection, including after a deadline.
            match publish_m3u_frame(&self.broker, &raw, id, self.max_queued_jobs, owner).await {
                Ok(receipt) if receipt.task_id == id => Ok(()),
                Err(Error::Capacity) => Err(publication_unsettled()),
                _ => {
                    self.uncertain.store(true, Ordering::Release);
                    Err(publication_unsettled())
                }
            }
        })
    }

    fn publish_info(
        &self,
        _profile: i64,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async { Err(StoreError::Unsupported("XC account publisher unavailable")) })
    }
}

fn publication_unsettled() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"error":"M3U aggregate broker publication did not acknowledge"}),
    )
}

pub(crate) struct RefreshAllCandidate {
    pub id: Uuid,
    pub raw: Vec<u8>,
    origin: Arc<Inner>,
    _held: Lease,
}

impl Jobs {
    pub(crate) async fn peek_m3u_all(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<RefreshAllCandidate>, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let (reply, held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &["celery".into()],
                &[],
                self.lease(owner)?,
            )
            .await
            .map_err(map)?;
        if reply.0 == 0 {
            return Ok(None);
        }
        if reply.0 != 1 || expected.is_some_and(|bytes| bytes.is_empty() || reply.1 != bytes) {
            return Err(Error::ClaimRace);
        }
        Ok(Some(RefreshAllCandidate {
            id: aggregate::parse(&reply.1).map_err(|_| Error::UnsupportedTask)?,
            raw: reply.1,
            origin: self.inner.clone(),
            _held: held,
        }))
    }

    pub(crate) async fn claim_m3u_all(
        &self,
        candidate: RefreshAllCandidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Uuid, Error> {
        if !self.allows_defaults() || !Arc::ptr_eq(&self.inner, &candidate.origin) {
            return Err(Error::UnsupportedTask);
        }
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &["celery".into()],
                &[&candidate.raw],
                self.lease(owner)?,
            )
            .await
        {
            Ok((1, _)) => Ok(candidate.id),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(candidate.id)),
            Err(error) => Err(map(error)),
        }
    }

    /// Root producers pass None. Actual claimed aggregate publishers pass its
    /// exact ID. Unlike publish_xc_live_root, this admits authenticated children.
    /// The caller still MUST keep real acknowledged UUIDs in its actual trail.
    pub(crate) async fn publish_m3u_all_frame(
        &self,
        raw: &[u8],
        parent: Option<Uuid>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let id = if let Some(parent) = parent {
            let child = crate::normal_xc_live::parse(raw).map_err(|_| Error::Protocol)?;
            if child.kind != crate::scanner_m3u_protocol::Kind::Single
                || child.parent != Some(parent)
                || child.root != parent
            {
                return Err(Error::Protocol);
            }
            child.id
        } else {
            aggregate::parse(raw).map_err(|_| Error::Protocol)?
        };
        publish_m3u_frame(
            &self.inner.broker,
            raw,
            id,
            self.inner.config.max_queued_jobs,
            self.lease(owner)?,
        )
        .await
    }

    pub(crate) async fn write_m3u_all_terminal(
        &self,
        id: Uuid,
        terminal: aggregate::Terminal,
        children: Vec<Uuid>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let(status,result,traceback)=match terminal {
            aggregate::Terminal::Revoked => ("REVOKED",serde_json::json!({"exc_type":"TaskRevokedError","exc_message":["revoked"],"exc_module":"celery.exceptions"}),serde_json::Value::Null),
            aggregate::Terminal::Failure(message) => ("FAILURE",serde_json::json!({"exc_type":"ProgrammingError","exc_message":[message],"exc_module":"django.db.utils"}),serde_json::json!("Native Dispatcharr M3U aggregate diagnostic: actual settled PostgreSQL P0001; Python traceback is not reproduced.")),
            aggregate::Terminal::Success(result) => ("SUCCESS",result,serde_json::Value::Null),
        };
        let children: Vec<_> = children
            .into_iter()
            .map(|id| serde_json::json!([[id, null], null]))
            .collect();
        let bytes=serde_json::to_vec(&serde_json::json!({"status":status,"result":result,"traceback":traceback,"children":children,"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":id})).map_err(|_|Error::Protocol)?;
        crate::decode::decode(&bytes, id).map_err(|_| Error::Protocol)?;
        self.write_metadata(id, bytes, owner).await
    }
}
