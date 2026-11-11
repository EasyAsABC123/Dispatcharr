//! Source255/265 normal public-idle transition; bounded pending followers use their own body receipts.
//! Original Dispatcharr authors retain application and algorithm credit.
use super::*;
use dispatcharr_coordination::{Error as CoordinationError, ProviderReservation};

// All writes require the same private owner, public generation, identity,
// profile, and ledger. A receipt proves only this original-generation claim.
const CLAIM: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] or redis.call('HGET',KEYS[2],'rust_owner_token')~=ARGV[2]
 or redis.call('HGET',KEYS[3],'token')~=ARGV[1] then return {'FOREIGN'} end
if redis.call('HGET',KEYS[2],'user_id')~=ARGV[3] or redis.call('HGET',KEYS[2],'content_uuid')~=ARGV[4]
 or redis.call('HGET',KEYS[2],'content_obj_type')~=ARGV[5] then return {'IDENTITY'} end
local count=tonumber(redis.call('HGET',KEYS[2],'active_streams')or'-1')
local requests=tonumber(redis.call('HGET',KEYS[2],'request_count')or'-1')
if count~=0 or not requests or requests<0 or requests~=math.floor(requests) or requests>=9007199254740991
 or redis.call('HGET',KEYS[3],'profile')~=ARGV[6] or redis.call('HGET',KEYS[3],'reserved')~='0'
 or redis.call('HGET',KEYS[2],'m3u_profile_id')~=ARGV[8] then return {'PARTIAL'} end
local kind=redis.call('TYPE',KEYS[4]).ok
if kind~='none' and kind~='hash' then return {'PARTIAL'} end
if redis.call('HLEN',KEYS[4])>=tonumber(ARGV[10]) then return {'CAPACITY'} end
local size=redis.call('MEMORY','USAGE',KEYS[4],'SAMPLES',0)or 0
if size>tonumber(ARGV[11]) then return {'CAPACITY'} end
redis.call('HINCRBY',KEYS[2],'active_streams',1)
redis.call('HSET',KEYS[2],'last_activity',ARGV[7]);redis.call('EXPIRE',KEYS[2],3600)
redis.call('HSET',KEYS[4],ARGV[9],ARGV[2]);redis.call('EXPIRE',KEYS[4],3600)
return {'CLAIMED'}
"#;
const COMMIT: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] or redis.call('HGET',KEYS[2],'rust_owner_token')~=ARGV[2]
 or redis.call('HGET',KEYS[3],'token')~=ARGV[1] then return {'FOREIGN'} end
if redis.call('HGET',KEYS[2],'user_id')~=ARGV[3] or redis.call('HGET',KEYS[2],'content_uuid')~=ARGV[4]
 or redis.call('HGET',KEYS[2],'content_obj_type')~=ARGV[5] then return {'IDENTITY'} end
if redis.call('HGET',KEYS[3],'profile')~=ARGV[6] or redis.call('HGET',KEYS[2],'m3u_profile_id')~=ARGV[8]
 then return {'PARTIAL'} end
local active=tonumber(redis.call('HGET',KEYS[2],'active_streams')or'-1')
if not active or active<1 or active~=math.floor(active) or active>=9007199254740991 then return {'PARTIAL'} end
if ARGV[10]=='1' and redis.call('HGET',KEYS[3],'reserved')~='1' then return {'PARTIAL'} end
if redis.call('HSTRLEN',KEYS[4],ARGV[9])~=string.len(ARGV[2]) or redis.call('HGET',KEYS[4],ARGV[9])~=ARGV[2] then return {'RECEIPT_ABSENT'} end
local requests=tonumber(redis.call('HGET',KEYS[2],'request_count')or'-1')
if not requests or requests<0 or requests~=math.floor(requests) or requests>=9007199254740991 then return {'PARTIAL'} end
redis.call('HINCRBY',KEYS[2],'request_count',1)
redis.call('HSET',KEYS[2],'last_activity',ARGV[7]);redis.call('EXPIRE',KEYS[2],3600)
return {'ENTERED'}
"#;
struct Pending(Arc<Inner>);
impl Drop for Pending {
    fn drop(&mut self) {
        self.0.pending_followers.store(false, Ordering::Release);
        self.0.idle_pending.store(false, Ordering::Release);
    }
}
fn status(reply: &[String], expected: &str) -> Result<(), Error> {
    match reply.first().map(String::as_str) {
        Some(value) if value == expected => Ok(()),
        Some("FOREIGN") => Err(Error::ForeignOwner),
        Some("IDENTITY") => Err(Error::Configuration),
        Some("CAPACITY") => Err(Error::Capacity),
        Some("RECEIPT_ABSENT") => Err(Error::UnprovenEntry),
        _ => Err(Error::PartialState),
    }
}
async fn rollback(
    inner: &Arc<Inner>,
    gen: &Generation,
    receipt: &EntryReceipt,
    reserved: bool,
) -> Result<(), Error> {
    let reply = mutate(inner, gen, "abandon", "0", Some(receipt)).await;
    // Release only this unique private ledger, never a guessed public counter.
    // Await actual physical reserve/release settlement before releasing guards.
    let remaining = reply
        .as_ref()
        .ok()
        .and_then(|value| value.get(1))
        .and_then(|value| value.parse::<i64>().ok());
    // A successful reservation belongs to this immutable generation, including
    // its admitted followers; never release it while a follower remains.
    let release = if reserved && remaining == Some(0) {
        let result = gen.lease.release_provider().await;
        gen.lease.settlement().wait().await;
        result.map(|_| ()).map_err(|_| Error::Backend)
    } else {
        Ok(())
    };
    let forget = receipt
        .forget(&mut inner.recovery.inner.connection.clone())
        .await
        .map_err(|_| Error::Backend);
    status(&reply?, "LEFT")?;
    release?;
    forget
}
impl LocalScope {
    /// Component-only normal idle adoption. Concurrent requests while the
    /// reservation is pending own receipts and share the retained transition.
    /// The Source270 public-counter undercount is not copied to private leases.
    /// Observer timeout never cancels physical claim/reserve/commit/cleanup.
    pub async fn enter_idle(
        &self,
        body: &Body,
        admission: NewAdmission,
        profile_max: u32,
    ) -> Result<(), Error> {
        self.enter_idle_inner(body, admission, profile_max, false)
            .await
    }
    // Only the same owned active-entry selector can attest its one completed
    // Source user-policy evaluation. Never rerun its stop-oldest side effects.
    pub(super) async fn enter_idle_after_policy(
        &self,
        body: &Body,
        admission: NewAdmission,
        profile_max: u32,
    ) -> Result<(), Error> {
        self.enter_idle_inner(body, admission, profile_max, true)
            .await
    }
    async fn enter_idle_inner(
        &self,
        body: &Body,
        admission: NewAdmission,
        profile_max: u32,
        user_policy_checked: bool,
    ) -> Result<(), Error> {
        if !Weak::ptr_eq(&body.state.scope, &Arc::downgrade(&self.inner))
            || body.is_closed()
            || body.state.admitted.load(Ordering::Acquire)
            || admission.fields.get("content_uuid") != Some(&self.inner.identity.content_uuid)
            || admission.fields.get("content_obj_type") != Some(&self.inner.identity.content_kind)
            || admission.fields.get("m3u_profile_id") != Some(&self.inner.profile.to_string())
        {
            return Err(Error::Configuration);
        }
        self.inner
            .idle_pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::PartialState)?;
        body.state.idle_claim.store(true, Ordering::Release);
        let pending = Pending(self.inner.clone());
        let hold = body
            .state
            .hold
            .lock()
            .unwrap()
            .clone()
            .ok_or(Error::Configuration)?;
        let body = body.clone();
        let inner = self.inner.clone();
        let task = tokio::spawn(async move {
            let _pending = pending;
            let guard = control(&inner).await?;
            let gen = inner.state.lock().await.current.clone();
            gen.lease.settlement().wait().await;
            if *inner.stop.borrow() || *inner.shutdown.borrow() || body.is_closed() {
                return Err(Error::Stopped);
            }
            if !admission
                .fields
                .get("user_id")
                .is_some_and(|user| user == &gen.user_id || user == "unknown")
            {
                return Err(Error::Configuration);
            }
            let owner_key = owner(&inner);
            let stop_key = format!("vod_proxy:client:{}:stop", inner.id);
            if admission.limit > 0 && !user_policy_checked {
                dispatcharr_clients::vod::admit_source_owner_physical(
                    &mut inner.recovery.inner.connection.clone(),
                    dispatcharr_clients::vod::Admission {
                        session_id: &inner.id,
                        fields: &admission.fields,
                        limit: admission.limit,
                        policy: &admission.policy,
                        create: false,
                        token: &gen.token,
                        scan_limit: 50000,
                        timeout: inner.recovery.inner.observer_timeout,
                        ttl: Duration::from_secs(3600),
                    },
                    dispatcharr_clients::vod::AdmissionFence {
                        owner_key: &owner_key,
                        owner_token: gen.lease.token(),
                        stop_key: &stop_key,
                    },
                )
                .await
                .map_err(Error::Admission)?;
            }
            if *inner.stop.borrow() || *inner.shutdown.borrow() || body.is_closed() {
                return Err(Error::Stopped);
            }
            let receipt = Arc::new(EntryReceipt::new(
                &inner.recovery.inner.namespace,
                &inner.id,
                &inner.receipt_owner,
                gen.lease.token(),
                &gen.token,
                inner.limits.bodies,
            ));
            {
                let mut entry = hold.receipt.lock().unwrap();
                if entry.is_some() {
                    return Err(Error::Configuration);
                }
                *entry = Some(receipt.clone());
            }
            let claim: redis::RedisResult<Vec<String>> = Script::new(CLAIM)
                .key(owner_key.clone())
                .key(key(&inner.id))
                .key(ledger(&inner, &gen.lease))
                .key(&receipt.key)
                .arg(gen.lease.token())
                .arg(&gen.token)
                .arg(&gen.user_id)
                .arg(&inner.identity.content_uuid)
                .arg(&inner.identity.content_kind)
                .arg(format!("profile_connections:{}", inner.profile))
                .arg(timestamp())
                .arg(inner.profile)
                .arg(&receipt.field)
                .arg(receipt.max_fields)
                .arg(receipt.max_bytes)
                .invoke_async(&mut inner.recovery.inner.connection.clone())
                .await;
            match claim {
                Ok(reply) => status(&reply, "CLAIMED")?,
                Err(_) => {
                    let cleanup = rollback(&inner, &gen, &receipt, false).await;
                    inner.failed.store(true, Ordering::Release);
                    cleanup?;
                    return Err(Error::Backend);
                }
            }
            if *inner.stop.borrow() || *inner.shutdown.borrow() || body.is_closed() {
                rollback(&inner, &gen, &receipt, false).await?;
                return Err(Error::Stopped);
            }
            // CLAIM is now acknowledged. Keep the control/caller/body owner,
            // but lend serialization while the provider mutation settles.
            // A held/lost CLAIM reply never reaches this phase.
            let Control { _permit, _serial } =
                Arc::try_unwrap(guard).map_err(|_| Error::Configuration)?;
            drop(_serial);
            let _retained_control = _permit;
            {
                let _hold = body.state.hold.lock().unwrap();
                inner
                    .pending_followers
                    .store(!body.is_closed(), Ordering::Release);
            }
            let reserve = gen
                .lease
                .reserve_provider(ProviderReservation {
                    profile_id: Some(inner.profile),
                    max_streams: profile_max,
                    credential_pool: None,
                })
                .await;
            gen.lease.settlement().wait().await;
            let transition_serial = inner.serial.clone().lock_owned().await;
            if let Err(error) = reserve {
                let definite_full = error == CoordinationError::ProfileFull;
                rollback(&inner, &gen, &receipt, !definite_full).await?;
                return Err(if definite_full {
                    Error::ProfileFull
                } else {
                    Error::Backend
                });
            }
            if *inner.stop.borrow() || *inner.shutdown.borrow() || body.is_closed() {
                rollback(&inner, &gen, &receipt, true).await?;
                return Err(Error::Stopped);
            }
            let commit: redis::RedisResult<Vec<String>> = Script::new(COMMIT)
                .key(owner_key)
                .key(key(&inner.id))
                .key(ledger(&inner, &gen.lease))
                .key(&receipt.key)
                .arg(gen.lease.token())
                .arg(&gen.token)
                .arg(&gen.user_id)
                .arg(&inner.identity.content_uuid)
                .arg(&inner.identity.content_kind)
                .arg(format!("profile_connections:{}", inner.profile))
                .arg(timestamp())
                .arg(inner.profile)
                .arg(&receipt.field)
                .arg(u8::from(profile_max > 0))
                .invoke_async(&mut inner.recovery.inner.connection.clone())
                .await;
            let result = commit
                .map_err(|_| Error::Backend)
                .and_then(|reply| status(&reply, "ENTERED"));
            if let Err(error) = result {
                rollback(&inner, &gen, &receipt, true).await?;
                return Err(error);
            }
            let abandoned = {
                let _lock = body.state.hold.lock().unwrap();
                if body.is_closed() {
                    true
                } else {
                    body.state.admitted.store(true, Ordering::Release);
                    false
                }
            };
            if abandoned {
                drop(transition_serial);
                close_owned(inner, hold, true, body.state.hook.clone()).await?;
                return Err(Error::Stopped);
            }
            Ok(())
        });
        tokio::time::timeout(self.inner.recovery.inner.observer_timeout, task)
            .await
            .map_err(|_| Error::ObserverTimeout)?
            .map_err(|_| Error::Backend)?
    }
}
