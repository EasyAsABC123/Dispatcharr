//! Cleanup-only bounded reservation/result/ACK composition. Original Celery,
//! Kombu and Dispatcharr authors retain protocol/algorithm/license credit.
//! This uses accepted Source117/119 reservation Lua and Source cleanup ACK3.
use super::*;
use crate::default_protocol::{Kind, Message};
const FRAME_BYTES: usize = 65536 + 131072 + 65536;
pub(super) struct Pool {
    pub(super) count: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    pub(super) maximum: usize,
}
struct Budget {
    _count: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
#[derive(Clone)]
pub(crate) struct Reservation {
    receipt: Arc<Receipt>,
    _budget: Arc<Budget>,
}
struct Receipt {
    message: Message,
    origin: Arc<Inner>,
    tag: Uuid,
    tuple: Vec<u8>,
    restored: Vec<u8>,
}
impl Reservation {
    pub(crate) fn message(&self) -> &Message {
        &self.receipt.message
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Ownership {
    Owned,
    Absent,
    Foreign,
}
// Scalar owner comparison, bounded before HGET; no foreign frame transfer.
const OWNER: &str = r#"
local h=redis.call('TYPE',KEYS[1]).ok;local z=redis.call('TYPE',KEYS[2]).ok
if h=='none'and z=='none'then return 0 end
if(h~='none'and h~='hash')or(z~='none'and z~='zset')or #ARGV[1]~=36 or #ARGV[2]>131072 then return -3 end
local found=redis.call('HEXISTS',KEYS[1],ARGV[1]);local score=redis.call('ZSCORE',KEYS[2],ARGV[1])
if found==0 and not score then return 0 end
if found~=1 or not score then return -3 end
if redis.call('HSTRLEN',KEYS[1],ARGV[1])~=#ARGV[2]then return 2 end
if redis.call('HGET',KEYS[1],ARGV[1])~=ARGV[2]then return 2 end
return 1
"#;
impl Jobs {
    pub(crate) fn cleanup_parallel(&self) -> bool {
        self.inner.config.operations >= 2
    }
    fn cleanup_pool(&self) -> Result<Arc<Pool>, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let mut slot = self
            .inner
            .cleanup_receipts
            .lock()
            .map_err(|_| Error::Backend)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        Ok(slot
            .get_or_insert_with(|| {
                Arc::new(Pool {
                    count: Arc::new(Semaphore::new(self.inner.config.operations)),
                    bytes: Arc::new(Semaphore::new(self.inner.config.operations * FRAME_BYTES)),
                    maximum: self.inner.config.operations,
                })
            })
            .clone())
    }
    fn owns_cleanup(&self, receipt: &Reservation) -> Result<(), Error> {
        if !self.allows_defaults() || !Arc::ptr_eq(&receipt.receipt.origin, &self.inner) {
            return Err(Error::Configuration);
        }
        Ok(())
    }
    async fn cleanup_owner(&self, receipt: &Reservation) -> Result<Ownership, Error> {
        self.owns_cleanup(receipt)?;
        let lease = self.cleanup_lease(Arc::new(receipt.clone()))?;
        let tag = receipt.receipt.tag.to_string();
        let (result, _held) = self
            .inner
            .broker
            .command_classified::<i64, _>(
                OWNER,
                &["unacked".into(), "unacked_index".into()],
                &[tag.as_bytes(), &receipt.receipt.tuple],
                lease,
            )
            .await
            .map_err(map)?;
        match result {
            1 => Ok(Ownership::Owned),
            0 => Ok(Ownership::Absent),
            2 => Ok(Ownership::Foreign),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn reserve_cleanup(
        &self,
        candidate: default_transport::Candidate,
    ) -> Result<Reservation, Error> {
        if !Arc::ptr_eq(&candidate.origin, &self.inner)
            || candidate.message.kind != Kind::RedisBackendCleanup
        {
            return Err(Error::Configuration);
        }
        let pool = self.cleanup_pool()?;
        let budget = Arc::new(Budget {
            _count: pool
                .count
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::Capacity)?,
            _bytes: pool
                .bytes
                .clone()
                .try_acquire_many_owned(FRAME_BYTES as u32)
                .map_err(|_| Error::Capacity)?,
            _caller: candidate.held._caller.clone(),
        });
        let envelope: serde_json::Value =
            serde_json::from_slice(&candidate.bytes).map_err(|_| Error::Protocol)?;
        let tag = envelope["properties"]["delivery_tag"]
            .as_str()
            .and_then(|v| Uuid::parse_str(v).ok())
            .ok_or(Error::Protocol)?;
        let tuple = serde_json::to_vec(&serde_json::json!([envelope, "", "celery"]))
            .map_err(|_| Error::Protocol)?;
        let mut restored: serde_json::Value =
            serde_json::from_slice(&candidate.bytes).map_err(|_| Error::Protocol)?;
        restored["headers"]["redelivered"] = serde_json::Value::Bool(true);
        restored["properties"]["delivery_info"]["redelivered"] = serde_json::Value::Bool(true);
        let restored = serde_json::to_vec(&restored).map_err(|_| Error::Protocol)?;
        if candidate.bytes.len() > 65536 || tuple.len() > 131072 || restored.len() > 65536 {
            return Err(Error::Capacity);
        }
        let receipt = Reservation {
            receipt: Arc::new(Receipt {
                message: candidate.message,
                origin: self.inner.clone(),
                tag,
                tuple,
                restored,
            }),
            _budget: budget,
        };
        let score = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Configuration)?
            .as_secs_f64()
            .to_string();
        let tag = tag.to_string();
        let maximum = "128";
        let bytes = "16777216";
        let (done, completed) = tokio::sync::oneshot::channel();
        let held = Arc::new((
            candidate.held,
            receipt.clone(),
            PhysicalCompletion(Some(done)),
        ));
        let result = self
            .inner
            .broker
            .command_classified::<i64, _>(
                crate::eta::RESERVE,
                &["celery".into(), "unacked".into(), "unacked_index".into()],
                &[
                    &candidate.bytes,
                    tag.as_bytes(),
                    &receipt.receipt.tuple,
                    score.as_bytes(),
                    maximum.as_bytes(),
                    bytes.as_bytes(),
                ],
                held.clone(),
            )
            .await;
        drop(held);
        match result {
            Ok((1, _)) => Ok(receipt),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok((-4, _)) => Err(Error::Capacity),
            Ok((-2, _)) => Err(Error::LostReservation),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => {
                let _ = completed.await;
                loop {
                    match self.cleanup_owner(&receipt).await {
                        Ok(Ownership::Owned) => return Ok(receipt),
                        Ok(Ownership::Absent) => return Err(Error::ClaimRace),
                        Ok(Ownership::Foreign) => return Err(Error::LostReservation),
                        Err(Error::Backend | Error::Capacity) => {
                            tokio::time::sleep(Duration::from_millis(100)).await
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            Err(error) => Err(map(error)),
        }
    }
    async fn finish_cleanup(&self, receipt: &Reservation, restore: bool) -> Result<bool, Error> {
        self.owns_cleanup(receipt)?;
        loop {
            let lease = match self.cleanup_lease(Arc::new(receipt.clone())) {
                Ok(v) => v,
                Err(Error::Capacity) => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let tag = receipt.receipt.tag.to_string();
            let (done, completed) = tokio::sync::oneshot::channel();
            let held = Arc::new((lease, PhysicalCompletion(Some(done))));
            let script = if restore {
                crate::eta::RESTORE
            } else {
                crate::eta::ACK
            };
            let mut args = vec![tag.as_bytes(), receipt.receipt.tuple.as_slice()];
            if restore {
                args.push(&receipt.receipt.restored);
            }
            let result = self
                .inner
                .broker
                .command_classified::<i64, _>(
                    script,
                    &["celery".into(), "unacked".into(), "unacked_index".into()],
                    &args,
                    held.clone(),
                )
                .await;
            drop(held);
            match result {
                Ok((1, _)) => return Ok(true),
                Ok((0, _)) => return Ok(false),
                Ok(_) => return Err(Error::Corrupt),
                Err(ClassifiedError::Backend) => {
                    let _ = completed.await;
                    loop {
                        match self.cleanup_owner(receipt).await {
                            Ok(Ownership::Owned) => break,
                            // Exact receipt gone: physical work is settled. Never change a foreign
                            // replacement. The caller distinguishes unproved ACK from normal success.
                            Ok(Ownership::Absent | Ownership::Foreign) => return Ok(false),
                            Err(Error::Backend | Error::Capacity) => {
                                tokio::time::sleep(Duration::from_millis(100)).await
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
                Err(error) => return Err(map(error)),
            }
        }
    }
    pub(crate) async fn ack_cleanup(&self, receipt: &Reservation) -> Result<bool, Error> {
        self.finish_cleanup(receipt, false).await
    }
    pub(crate) async fn restore_cleanup(&self, receipt: &Reservation) -> Result<bool, Error> {
        self.finish_cleanup(receipt, true).await
    }
}
