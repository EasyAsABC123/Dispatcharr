//! Dedicated opaque per-channel wrapper, not a new generic Jobs authority.
//! Registered as a CHILD of transport so only this implementation can access
//! its private inner transports. No public extraction/clone-as-Jobs method.
use super::*;
use crate::tvg_programme_protocol::{self, Envelope};
#[derive(Clone)]
pub struct TvgJobs {
    jobs: Jobs,
    normal: Arc<std::sync::atomic::AtomicBool>,
    shared: Arc<std::sync::OnceLock<Shared>>,
}
struct Shared {
    jobs: Jobs,
    identity: dispatcharr_store::ProducerIdentitySource,
}
pub struct Candidate {
    pub(crate) message: Envelope,
    parent: Option<Uuid>,
    normal_message: Option<crate::xml_tvg_protocol::Message>,
    reserved: Option<crate::xml_tvg_eta::Reservation>,
    raw: Vec<u8>,
    origin: Arc<Inner>,
}
pub struct Claimed {
    message: Envelope,
    parent: Option<Uuid>,
    origin: Arc<Inner>,
}
#[derive(Clone, Copy, Debug)]
pub enum Normal {
    Null,
    MissingEpg,
    Busy,
}
impl TvgJobs {
    pub async fn connect(broker: &str, result: &str, config: Config) -> Result<Self, Error> {
        // Private reuse of the existing bounded connection/lease machinery;
        // wrapper methods expose ONLY this family. No Authority enum widening.
        Ok(Self {
            jobs: Jobs::connect_series(broker, result, config).await?,
            normal: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            shared: Arc::new(std::sync::OnceLock::new()),
        })
    }
    pub(crate) async fn publish_channel_root(
        &self,
        publication: dispatcharr_store::TvgProgrammePublication,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        let message = tvg_programme_protocol::parse_initial(publication.bytes())
            .map_err(|_| Error::Protocol)?;
        if message.task_id != publication.task_id() {
            return Err(Error::Protocol);
        }
        self.jobs
            .publish_xml_tvg(publication.bytes(), owner)
            .await?;
        Ok(())
    }

    fn active_jobs(&self) -> &Jobs {
        self.shared.get().map_or(&self.jobs, |s| &s.jobs)
    }
    pub(crate) fn bind_shared(
        &self,
        jobs: Jobs,
        identity: dispatcharr_store::ProducerIdentitySource,
    ) -> Result<(), Error> {
        if !jobs.allows_defaults() {
            return Err(Error::Configuration);
        }
        self.shared
            .set(Shared { jobs, identity })
            .map_err(|_| Error::Configuration)
    }
    pub(crate) fn reserved_candidate(
        &self,
        reservation: crate::xml_tvg_eta::Reservation,
    ) -> Result<Candidate, Error> {
        let jobs = self.active_jobs();
        jobs.owns_xml_tvg_reservation(&reservation)?;
        let normal = reservation.message().clone();
        Ok(Candidate {
            message: Envelope {
                task_id: normal.common.id,
                epg_data_id: normal.target,
                force: normal.force,
                defer_retry: normal.defer,
            },
            parent: normal.common.parent,
            normal_message: Some(normal),
            raw: reservation.receipt.queued_bytes.clone(),
            reserved: Some(reservation),
            origin: jobs.inner.clone(),
        })
    }
    pub(crate) fn enable_normal_intake(&self) {
        self.normal
            .store(true, std::sync::atomic::Ordering::Release);
    }
    pub async fn peek(&self, owner: Arc<dyn Send + Sync>) -> Result<Option<Candidate>, Error> {
        self.peek_selected(None, owner).await
    }
    pub(crate) async fn peek_selected(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Candidate>, Error> {
        let lease = self.active_jobs().lease(owner)?;
        let (reply, _held) = self
            .active_jobs()
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &["celery".into()], &[], lease)
            .await
            .map_err(map)?;
        if let Some(expected) = expected {
            if expected.is_empty() || reply.0 != 1 || reply.1 != expected {
                return Err(Error::ClaimRace);
            }
        }
        match reply.0 {
            0 => Ok(None),
            1 => {
                let (message, parent) = if self.normal.load(std::sync::atomic::Ordering::Acquire) {
                    tvg_programme_protocol::parse_normal(&reply.1)
                } else {
                    tvg_programme_protocol::parse_initial(&reply.1).map(|message| (message, None))
                }
                .map_err(|_| Error::UnsupportedTask)?;
                Ok(Some(Candidate {
                    message,
                    parent,
                    normal_message: if self.shared.get().is_some() {
                        Some(
                            crate::xml_tvg_protocol::parse(&reply.1)
                                .map_err(|_| Error::UnsupportedTask)?,
                        )
                    } else {
                        None
                    },
                    reserved: None,
                    raw: reply.1,
                    origin: self.active_jobs().inner.clone(),
                }))
            }
            _ => Err(Error::Corrupt),
        }
    }
    pub async fn claim(
        &self,
        candidate: Candidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Claimed, Error> {
        if !Arc::ptr_eq(&candidate.origin, &self.active_jobs().inner) {
            return Err(Error::UnsupportedTask);
        }
        if let Some(reserved) = candidate.reserved.as_ref() {
            self.active_jobs().ack_xml_tvg_eta(reserved, owner).await?;
            return Ok(Claimed {
                message: candidate.message,
                parent: candidate.parent,
                origin: candidate.origin,
            });
        }
        let lease = self.active_jobs().lease(owner)?;
        match self
            .active_jobs()
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &["celery".into()],
                &[candidate.raw.as_slice()],
                lease,
            )
            .await
        {
            Ok((1, _)) => Ok(Claimed {
                message: candidate.message,
                parent: candidate.parent,
                origin: candidate.origin,
            }),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(candidate.message.task_id)),
            Err(error) => Err(map(error)),
        }
    }
    pub(crate) fn supports_deferral(&self) -> bool {
        self.shared.get().is_some()
    }
    pub(crate) async fn complete_deferred(
        &self,
        candidate: Candidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Uuid, Error> {
        let shared = self.shared.get().ok_or(Error::UnsupportedTask)?;
        let message = candidate
            .normal_message
            .clone()
            .ok_or(Error::UnsupportedTask)?;
        let retry = if crate::xml_tvg_protocol::deferral(message.defer)
            == crate::xml_tvg_protocol::Deferral::Exhausted
        {
            None
        } else {
            let id = Uuid::new_v4();
            let identity = shared
                .identity
                .current()
                .map_err(|_| Error::Configuration)?;
            let raw = crate::xml_tvg_protocol::retry_frame(
                &message,
                id,
                identity.reply_to,
                &identity.origin,
                chrono::Utc::now(),
            )
            .map_err(|_| Error::Protocol)?;
            Some((id, raw))
        };
        let (claimed, children) = if let Some((id, raw)) = retry {
            let claimed = self
                .replace_with_retry(candidate, id, &raw, owner.clone())
                .await?;
            (claimed, vec![id])
        } else {
            (self.claim(candidate, owner.clone()).await?, Vec::new())
        };
        let value = if crate::xml_tvg_protocol::deferral(message.defer)
            == crate::xml_tvg_protocol::Deferral::Exhausted
        {
            serde_json::json!("Deferred too many times: source refresh in progress")
        } else {
            serde_json::json!("Deferred")
        };
        shared
            .jobs
            .write_result(
                claimed.message.task_id,
                value,
                claimed.parent,
                children,
                owner,
            )
            .await?;
        Ok(message.common.id)
    }
    async fn replace_with_retry(
        &self,
        candidate: Candidate,
        child: Uuid,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Claimed, Error> {
        let jobs = self.active_jobs();
        if !jobs.allows_defaults() || !Arc::ptr_eq(&candidate.origin, &jobs.inner) {
            return Err(Error::UnsupportedTask);
        }
        let (mode, parent_bytes, tag) = if let Some(reserved) = candidate.reserved.as_ref() {
            jobs.owns_xml_tvg_reservation(reserved)?;
            (
                b"reserved".as_slice(),
                reserved.tuple_bytes(),
                reserved.delivery_tag().to_string(),
            )
        } else {
            (b"ready".as_slice(), candidate.raw.as_slice(), String::new())
        };
        let lease = jobs.lease(Arc::new((owner, candidate.reserved.clone())))?;
        let maximum = jobs.inner.config.max_queued_jobs.to_string();
        let keys = [
            "celery".to_owned(),
            "_kombu.binding.celery".to_owned(),
            "unacked".to_owned(),
            "unacked_index".to_owned(),
        ];
        match jobs
            .inner
            .broker
            .command_classified::<i64, _>(
                REPLACE_WITH_RETRY,
                &keys,
                &[mode, parent_bytes, tag.as_bytes(), raw, maximum.as_bytes()],
                lease,
            )
            .await
        {
            Ok((1, _)) => Ok(Claimed {
                message: candidate.message,
                parent: candidate.parent,
                origin: candidate.origin,
            }),
            Ok((0, _)) if candidate.reserved.is_some() => Err(Error::LostReservation),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(child)),
            Err(error) => Err(map(error)),
        }
    }

    pub async fn write_normal(
        &self,
        claimed: Claimed,
        value: Normal,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !Arc::ptr_eq(&claimed.origin, &self.active_jobs().inner) {
            return Err(Error::UnsupportedTask);
        }
        let result = match value {
            Normal::Null => serde_json::Value::Null,
            Normal::MissingEpg => serde_json::json!("EPG not found"),
            Normal::Busy => serde_json::json!("Task already running"),
        };
        self.active_jobs()
            .write_result(
                claimed.message.task_id,
                result,
                claimed.parent,
                Vec::new(),
                owner,
            )
            .await
    }
    pub(crate) async fn write_insert_failure(
        &self,
        claimed: Claimed,
        failure: crate::tvg_programme_terminal::InsertFailure,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !Arc::ptr_eq(&claimed.origin, &self.active_jobs().inner) {
            return Err(Error::UnsupportedTask);
        }
        let date = chrono::Utc::now()
            .format("%Y-%m-%dT%H:%M:%S%.6f+00:00")
            .to_string();
        let mut bytes = failure
            .metadata(claimed.message.task_id, &date)
            .map_err(|_| Error::Protocol)?;
        if let Some(parent) = claimed.parent {
            let mut value: serde_json::Value =
                serde_json::from_slice(&bytes).map_err(|_| Error::Protocol)?;
            value["parent_id"] = serde_json::json!(parent);
            bytes = serde_json::to_vec(&value).map_err(|_| Error::Protocol)?;
            if bytes.len() > 262144 {
                return Err(Error::UnsupportedTask);
            }
        }
        self.active_jobs()
            .write_metadata(claimed.message.task_id, bytes, owner)
            .await
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.jobs.shutdown(timeout).await
    }
}

// Compound exact parent-to-retry exchange. Capacity changes neither delivery.
// Publish before retirement inside the script so a rejected write cannot drop
// the parent. Other clients observe both successful effects together.
const REPLACE_WITH_RETRY: &str = r#"
local q=redis.call('TYPE',KEYS[1]).ok
local binding=redis.call('TYPE',KEYS[2]).ok
if (q~='none' and q~='list') or (binding~='none' and binding~='set') then return -3 end
local maximum=tonumber(ARGV[5])
if not maximum or maximum<1 or maximum>10000 or #ARGV[4]>65536 then return -3 end
local queued=redis.call('LLEN',KEYS[1])
if ARGV[1]=='ready' then
 if #ARGV[2]>65536 or #ARGV[3]~=0 then return -3 end
 if redis.call('LINDEX',KEYS[1],-1)~=ARGV[2] then return 0 end
 if queued>maximum then return -2 end
elseif ARGV[1]=='reserved' then
 local h=redis.call('TYPE',KEYS[3]).ok
 local z=redis.call('TYPE',KEYS[4]).ok
 if h=='none' and z=='none' then return 0 end
 if h~='hash' or z~='zset' or #ARGV[3]~=36 or #ARGV[2]>131072 then return -3 end
 if redis.call('HSTRLEN',KEYS[3],ARGV[3])~=#ARGV[2] then return 0 end
 if redis.call('HGET',KEYS[3],ARGV[3])~=ARGV[2] then return 0 end
 if not redis.call('ZSCORE',KEYS[4],ARGV[3]) then return 0 end
 if queued>=maximum then return -2 end
else
 return -3
end
redis.call('SADD',KEYS[2],'celery'..string.char(6,22)..string.char(6,22)..'celery')
redis.call('LPUSH',KEYS[1],ARGV[4])
if ARGV[1]=='ready' then
 redis.call('RPOP',KEYS[1])
else
 redis.call('ZREM',KEYS[4],ARGV[3])
 redis.call('HDEL',KEYS[3],ARGV[3])
end
return 1
"#;
