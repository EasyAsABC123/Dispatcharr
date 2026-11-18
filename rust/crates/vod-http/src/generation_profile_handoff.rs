//! Source2's owned idle inactive-profile transition, same account/credential.
//! Original Dispatcharr and dependency authors retain all application credit.
//! Metadata loss, borrowed ledgers, capped users and concurrent followers remain
//! separate authorities. No public counter is guessed or manually decremented.
use super::*;

const CLAIM: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] or redis.call('HGET',KEYS[2],'rust_owner_token')~=ARGV[2]
 or redis.call('HGET',KEYS[3],'token')~=ARGV[1] then return {'FOREIGN'} end
if redis.call('HGET',KEYS[3],'source_borrowed')=='1' or redis.call('HGET',KEYS[3],'source_slot_shared')=='1' then return {'PARTIAL'} end
if redis.call('HGET',KEYS[2],'user_id')~=ARGV[3] or redis.call('HGET',KEYS[2],'content_uuid')~=ARGV[4]
 or redis.call('HGET',KEYS[2],'content_obj_type')~=ARGV[5] then return {'IDENTITY'} end
if redis.call('HGET',KEYS[2],'active_streams')~='0' or redis.call('HGET',KEYS[3],'reserved')~='0'
 or redis.call('HGET',KEYS[3],'profile')~='profile_connections:'..ARGV[6]
 or redis.call('HGET',KEYS[2],'m3u_profile_id')~=ARGV[6] then return {'PARTIAL'} end
local requests=tonumber(redis.call('HGET',KEYS[2],'request_count')or'-1')
if not requests or requests<0 or requests~=math.floor(requests) or requests>=9007199254740991 then return {'PARTIAL'} end
local kind=redis.call('TYPE',KEYS[4]).ok
if kind~='none' and kind~='hash' then return {'PARTIAL'} end
if redis.call('HLEN',KEYS[4])>=tonumber(ARGV[9]) then return {'CAPACITY'} end
if (redis.call('MEMORY','USAGE',KEYS[4],'SAMPLES',0)or 0)>tonumber(ARGV[10]) then return {'CAPACITY'} end
redis.call('HINCRBY',KEYS[2],'active_streams',1)
redis.call('HSET',KEYS[2],'last_activity',ARGV[7]);redis.call('EXPIRE',KEYS[2],3600)
redis.call('HSET',KEYS[4],ARGV[8],ARGV[2]);redis.call('EXPIRE',KEYS[4],3600)
return {'CLAIMED'}
"#;
const COMMIT: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] or redis.call('HGET',KEYS[2],'rust_owner_token')~=ARGV[2]
 or redis.call('HGET',KEYS[3],'token')~=ARGV[1] then return {'FOREIGN'} end
if redis.call('HGET',KEYS[3],'source_borrowed')=='1' or redis.call('HGET',KEYS[3],'source_slot_shared')=='1' then return {'PARTIAL'} end
if redis.call('HGET',KEYS[2],'user_id')~=ARGV[3] or redis.call('HGET',KEYS[2],'content_uuid')~=ARGV[4]
 or redis.call('HGET',KEYS[2],'content_obj_type')~=ARGV[5] then return {'IDENTITY'} end
if redis.call('HGET',KEYS[2],'active_streams')~='1' or redis.call('HGET',KEYS[3],'reserved')~='1'
 or redis.call('HGET',KEYS[3],'profile')~='profile_connections:'..ARGV[7]
 or redis.call('HGET',KEYS[2],'m3u_profile_id')~=ARGV[6]
 or redis.call('HGET',KEYS[4],ARGV[9])~=ARGV[2] then return {'PARTIAL'} end
local requests=tonumber(redis.call('HGET',KEYS[2],'request_count')or'-1')
if not requests or requests<0 or requests~=math.floor(requests) or requests>=9007199254740991 then return {'PARTIAL'} end
redis.call('HSET',KEYS[2],'m3u_profile_id',ARGV[7],'last_activity',ARGV[8])
redis.call('HINCRBY',KEYS[2],'request_count',1);redis.call('EXPIRE',KEYS[2],3600)
return {'ENTERED'}
"#;
// A failed/abandoned setup may retire only its own single claim. Do not replay
// CLAIM/COMMIT or infer that a lost reply means no write. The private ledger's
// clear_provider operation, not this script, releases its exact reservation.
const ABANDON: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] or redis.call('HGET',KEYS[3],'token')~=ARGV[1]
 or redis.call('HGET',KEYS[3],'source_borrowed')=='1' or redis.call('HGET',KEYS[3],'source_slot_shared')=='1' then return {'FOREIGN'} end
local kind=redis.call('TYPE',KEYS[2]).ok
if kind=='none' then return {'ABSENT'} end
if kind~='hash' or redis.call('HGET',KEYS[2],'rust_owner_token')~=ARGV[2]
 or redis.call('HGET',KEYS[2],'user_id')~=ARGV[3] or redis.call('HGET',KEYS[2],'content_uuid')~=ARGV[4]
 or redis.call('HGET',KEYS[2],'content_obj_type')~=ARGV[5] then return {'FOREIGN'} end
local profile=redis.call('HGET',KEYS[2],'m3u_profile_id')
local held=redis.call('HGET',KEYS[3],'profile')
if (profile~=ARGV[6] and profile~=ARGV[7]) or
 (held~='profile_connections:'..ARGV[6] and held~='profile_connections:'..ARGV[7])
 or redis.call('HGET',KEYS[2],'active_streams')~='1'
 or redis.call('HGET',KEYS[4],ARGV[8])~=ARGV[2] then return {'PARTIAL'} end
redis.call('DEL',KEYS[2]);redis.call('HDEL',KEYS[4],ARGV[8])
return {'REMOVED'}
"#;
async fn abandon_handoff(
    inner: &Arc<Inner>,
    gen: &Generation,
    receipt: &EntryReceipt,
    old: i64,
    selected: i64,
) -> Result<(), Error> {
    let result = async {
        let answer: Vec<String> = Script::new(ABANDON)
            .key(owner(inner))
            .key(key(&inner.id))
            .key(ledger(inner, &gen.lease))
            .key(&receipt.key)
            .arg(gen.lease.token())
            .arg(&gen.token)
            .arg(&gen.user_id)
            .arg(&inner.identity.content_uuid)
            .arg(&inner.identity.content_kind)
            .arg(old)
            .arg(selected)
            .arg(&receipt.field)
            .invoke_async(&mut inner.recovery.inner.connection.clone())
            .await
            .map_err(|_| Error::Backend)?;
        if !matches!(
            answer.first().map(String::as_str),
            Some("REMOVED" | "ABSENT")
        ) {
            return Err(Error::PartialState);
        }
        // The same fenced single-claim proof has removed (or observed absent)
        // our metadata. Its idle timer can no longer finalize that metadata, so
        // retire the immutable owner/ledger as well as its provider allocation.
        let released = gen.lease.release().await;
        gen.lease.settlement().wait().await;
        released.map_err(|_| Error::Backend)?;
        gen.retired.store(true, Ordering::Release);
        disarm_recovered(gen);
        receipt
            .forget(&mut inner.recovery.inner.connection.clone())
            .await
            .map_err(|_| Error::Backend)
    }
    .await;
    if result.is_err() {
        // Keep uncertain quota under the real fenced lease until supervised
        // retirement; no new request may treat this partial scope as healthy.
        inner.failed.store(true, Ordering::Release);
        inner.stop.send_replace(true);
    }
    result
}
impl LocalScope {
    /// Only SessionPool's fresh SQL inactive-profile and same-fingerprint policy
    /// admits this captured same-account transition. One serial owner spans the
    /// complete claim/reservation/commit; followers cannot borrow this handoff.
    pub async fn enter_idle_profile_handoff(
        &self,
        body: &Body,
        admission: NewAdmission,
        selected: i64,
        maximum: u32,
    ) -> Result<(), Error> {
        let old = self.profile_id();
        if selected <= 0
            || selected == old
            || maximum == 0
            || admission.limit != 0
            || !Weak::ptr_eq(&body.state.scope, &Arc::downgrade(&self.inner))
            || body.is_closed()
            || body.state.admitted.load(Ordering::Acquire)
            || admission.fields.get("content_uuid") != Some(&self.inner.identity.content_uuid)
            || admission.fields.get("content_obj_type") != Some(&self.inner.identity.content_kind)
            || admission.fields.get("m3u_profile_id") != Some(&selected.to_string())
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
        let observer = observation::Observer::new(inner.recovery.inner.observer_timeout);
        let phase_observer = observer.clone();
        let task = tokio::spawn(async move {
            let _pending = pending;
            let _control = control(&inner).await?;
            let gen = {
                let state = inner.state.lock().await;
                // Source2's safe profile change has no lost/retired predecessor
                // bodies. Keep the counter-discrepancy/recovery matrix separate.
                if !state.retired.is_empty()
                    || state.pending.is_some()
                    || state.current.recovered.lock().unwrap().is_some()
                {
                    return Err(Error::PartialState);
                }
                state.current.clone()
            };
            gen.lease.settlement().wait().await;
            if inner.profile.load(Ordering::Acquire) != old
                || *inner.stop.borrow()
                || *inner.shutdown.borrow()
                || body.is_closed()
                || admission.fields.get("user_id") != Some(&gen.user_id)
            {
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
                let mut retained = hold.receipt.lock().unwrap();
                if retained.is_some() {
                    return Err(Error::Configuration);
                }
                *retained = Some(receipt.clone());
            }
            let claim: redis::RedisResult<Vec<String>> = Script::new(CLAIM)
                .key(owner(&inner))
                .key(key(&inner.id))
                .key(ledger(&inner, &gen.lease))
                .key(&receipt.key)
                .arg(gen.lease.token())
                .arg(&gen.token)
                .arg(&gen.user_id)
                .arg(&inner.identity.content_uuid)
                .arg(&inner.identity.content_kind)
                .arg(old)
                .arg(timestamp())
                .arg(&receipt.field)
                .arg(receipt.max_fields)
                .arg(receipt.max_bytes)
                .invoke_async(&mut inner.recovery.inner.connection.clone())
                .await;
            match claim {
                Ok(value) => status(&value, "CLAIMED")?,
                Err(_) => {
                    inner.failed.store(true, Ordering::Release);
                    abandon_handoff(&inner, &gen, &receipt, old, selected).await?;
                    return Err(Error::Backend);
                }
            }
            // Same existing owner/ledger, physically cleared before this idle
            // transition. The qualified retained-result API owns the one reserve.
            let phase = phase_observer.reserve();
            let reserved = gen
                .lease
                .reserve_provider_retained_result(ProviderReservation {
                    profile_id: Some(selected),
                    max_streams: maximum,
                    credential_pool: inner.credential_pool.clone(),
                })
                .await;
            gen.lease.settlement().wait().await;
            drop(phase);
            if let Err(error) = reserved {
                if error == CoordinationError::ProfileFull {
                    // This acknowledged refusal allocated nothing. Remove only
                    // our receipt-backed claim, preserving the old idle metadata
                    // and its original timer's authority to retire this lease.
                    rollback(&inner, &gen, &receipt, false).await?;
                    return Err(Error::ProfileFull);
                }
                inner.failed.store(true, Ordering::Release);
                abandon_handoff(&inner, &gen, &receipt, old, selected).await?;
                return Err(Error::Backend);
            }
            if *inner.stop.borrow() || *inner.shutdown.borrow() || body.is_closed() {
                abandon_handoff(&inner, &gen, &receipt, old, selected).await?;
                return Err(Error::Stopped);
            }
            let committed: redis::RedisResult<Vec<String>> = Script::new(COMMIT)
                .key(owner(&inner))
                .key(key(&inner.id))
                .key(ledger(&inner, &gen.lease))
                .key(&receipt.key)
                .arg(gen.lease.token())
                .arg(&gen.token)
                .arg(&gen.user_id)
                .arg(&inner.identity.content_uuid)
                .arg(&inner.identity.content_kind)
                .arg(old)
                .arg(selected)
                .arg(timestamp())
                .arg(&receipt.field)
                .invoke_async(&mut inner.recovery.inner.connection.clone())
                .await;
            let committed = committed
                .map_err(|_| Error::Backend)
                .and_then(|reply| status(&reply, "ENTERED"));
            if let Err(error) = committed {
                inner.failed.store(true, Ordering::Release);
                abandon_handoff(&inner, &gen, &receipt, old, selected).await?;
                return Err(error);
            }
            inner.profile.store(selected, Ordering::Release);
            let abandoned = {
                let _hold = body.state.hold.lock().unwrap();
                if body.is_closed() {
                    true
                } else {
                    body.state.admitted.store(true, Ordering::Release);
                    false
                }
            };
            if abandoned {
                abandon_handoff(&inner, &gen, &receipt, old, selected).await?;
                return Err(Error::Stopped);
            }
            // Existing receipt close/progress logic now uses the acknowledged
            // selected profile; created_at and the retained ID were never reset.
            drop(hold);
            Ok(())
        });
        observer.observe(task).await
    }
}
