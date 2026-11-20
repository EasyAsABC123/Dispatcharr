//! Retain one local archive reservation across an explicit programme change.
//! Original application and algorithm credit belongs to Dispatcharr contributors.
use super::{
    check_existing, keys, pool, start_setup_refresh, timestamp, unavailable, ArchivePool,
    ArchivePoolRequest, ArchiveTicket, Entry, Inner, Operation, Rollback, CLEANUP,
};
use crate::CatchupCandidate;
use dispatcharr_coordination::Lease;
use dispatcharr_store::StoreError;
use serde_json::json;
use std::{
    sync::{atomic::Ordering, Arc, LazyLock},
    time::Duration,
};

const CLAIM: &str = include_str!("claim_programme_hop.lua");
const STOP: &str = r#"
if redis.call('HGET',KEYS[1],'rust_owner_token')~=ARGV[1]
 or redis.call('HGET',KEYS[1],'busy')~='1' or redis.call('GET',KEYS[2])~=ARGV[2]
 or redis.call('EXISTS',KEYS[3])~=0 then return 0 end
redis.call('SET',KEYS[3],ARGV[2],'EX',60)
return 1
"#;
static ABORT: LazyLock<String> =
    LazyLock::new(|| format!("{}\n{CLEANUP}", include_str!("abort_prefix.lua")));

pub(crate) struct ProgrammeHop {
    old: Arc<Entry>,
    epoch: u64,
    generation: u64,
}

impl ProgrammeHop {
    pub(crate) fn matches(&self, candidate: &CatchupCandidate) -> bool {
        let fields = &self.old.request.pool_fields;
        self.old.request.provider_profile_id == candidate.profile_id
            && fields.get("account_id").and_then(|id| id.parse().ok()) == Some(candidate.account_id)
            && fields
                .get("dispatcharr_stream_id")
                .and_then(|id| id.parse().ok())
                == Some(candidate.stream_id)
            && fields.get("stream_id") == Some(&candidate.provider_stream_id)
            && fields
                .get("provider_timestamp")
                .is_some_and(|time| time != &candidate.provider_timestamp)
    }

    fn supported(&self, target: &ArchivePoolRequest) -> bool {
        let prior = &self.old.request;
        let identity = [
            "user_id",
            "account_id",
            "profile_id",
            "stream_id",
            "dispatcharr_stream_id",
        ];
        target.session_id == prior.session_id
            && target.channel_id == prior.channel_id
            && target.provider_profile_id == prior.provider_profile_id
            && target.virtual_channel_id != prior.virtual_channel_id
            && identity.iter().all(|field| {
                prior.pool_fields.get(*field).is_some_and(|value| {
                    !value.is_empty() && target.pool_fields.get(*field) == Some(value)
                })
            })
            && ["media_id", "provider_timestamp"].iter().all(|field| {
                target.pool_fields.get(*field).is_some_and(|value| {
                    !value.is_empty() && prior.pool_fields.get(*field) != Some(value)
                })
            })
            && target.virtual_channel_id.len() <= 512
            && !target.virtual_channel_id.contains(':')
            && target
                .pool_fields
                .get("client_user_agent")
                .is_some_and(|agent| agent.len() <= 8192)
    }
}

fn busy() -> StoreError {
    StoreError::StatusBody(503, json!("Stream slot busy"))
}

impl ArchivePool {
    pub(crate) fn active_programme_hop(
        &self,
        sid: &str,
        channel: i64,
        user: i64,
        media: &str,
    ) -> Option<ProgrammeHop> {
        let old = self.inner.entries.lock().unwrap().get(sid).cloned()?;
        let epoch = old.lifecycle.load(Ordering::SeqCst);
        let generation = old.generation.load(Ordering::SeqCst);
        let fields = &old.request.pool_fields;
        (epoch & 3 == 0
            && generation > 0
            && old.streaming.load(Ordering::SeqCst)
            && old.active.load(Ordering::SeqCst)
            && !*old.stop.borrow()
            && old.request.channel_id == channel
            && fields.get("user_id").and_then(|id| id.parse().ok()) == Some(user)
            && fields.get("media_id").is_some_and(|id| id != media))
        .then_some(ProgrammeHop {
            old,
            epoch,
            generation,
        })
    }

    pub(crate) async fn open_programme_hop(
        &self,
        hop: &ProgrammeHop,
        request: ArchivePoolRequest,
    ) -> Result<ArchiveTicket, StoreError> {
        if !hop.supported(&request) {
            return Err(StoreError::Unsupported(
                "archive programme/profile adoption is not yet implemented",
            ));
        }
        let old = hop.old.clone();
        let next = Entry::new(request, old.permit.clone());
        let attempt = {
            let mut pending = old.programme_hop.lock().unwrap();
            if *self.inner.stopped.borrow()
                || old.lifecycle.load(Ordering::SeqCst) != hop.epoch
                || !old.streaming.load(Ordering::SeqCst)
                || pending.is_some()
                || old
                    .handoff_pending
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
            {
                return Err(busy());
            }
            *pending = Some(next.token.clone());
            HopAttempt { old: old.clone() }
        };
        let inner = self.inner.clone();
        let (epoch, generation) = (hop.epoch, hop.generation);
        // From this point HTTP cancellation drops only its wait. A returned
        // but undelivered ArchiveTicket follows its existing rollback path.
        self.inner
            .runtime
            .spawn(async move { handoff(inner, old, next, epoch, generation, attempt).await })
            .await
            .map_err(|_| unavailable())?
    }
}

struct HopAttempt {
    old: Arc<Entry>,
}

impl Drop for HopAttempt {
    fn drop(&mut self) {
        self.old.programme_hop.lock().unwrap().take();
        self.old.handoff_pending.store(false, Ordering::SeqCst);
        self.old.reopened.notify_waiters();
    }
}

async fn handoff(
    inner: Arc<Inner>,
    old: Arc<Entry>,
    next: Arc<Entry>,
    epoch: u64,
    generation: u64,
    attempt: HopAttempt,
) -> Result<ArchiveTicket, StoreError> {
    check_existing(&inner, &old, &next.request, ()).await?;
    let old_keys = keys(&old);
    let gen = generation.to_string();
    let (stopped, _): (i64, Operation) = inner
        .transport
        .command(
            STOP,
            &[pool(&old), old_keys[5].clone(), old_keys[6].clone()],
            &[old.token.as_bytes(), gen.as_bytes()],
            Operation::new(&old),
        )
        .await
        .map_err(|_| unavailable())?;
    if stopped != 1 {
        return Err(busy());
    }
    old.stop.send_replace(true);
    let deadline = tokio::time::Instant::now() + inner.config.command_timeout;
    loop {
        if *inner.stopped.borrow() || tokio::time::Instant::now() >= deadline {
            return Err(unavailable());
        }
        let state = old.lifecycle.load(Ordering::SeqCst);
        if state & !3 != epoch || state & 3 == 3 {
            return Err(busy());
        }
        if state == (epoch | 2) {
            if !old.programme_hop_drained.load(Ordering::SeqCst)
                || !old.retained_provider.load(Ordering::SeqCst)
                || old
                    .lifecycle
                    .compare_exchange(epoch | 2, epoch + 4, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
            {
                return Err(unavailable());
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    old.reopened.notify_waiters();
    let lease = old.lease.lock().unwrap().take();
    let rollback = ClaimRollback(Some(Retirement {
        inner: inner.clone(),
        old: old.clone(),
        next: next.clone(),
        epoch,
        lease,
        settled: false,
        _attempt: attempt,
    }));
    let lease = rollback
        .0
        .as_ref()
        .and_then(|owner| owner.lease.as_ref())
        .ok_or_else(unavailable)?;
    if *lease.lost().borrow() {
        return Err(unavailable());
    }
    let provider_token = lease.token().to_owned();
    let next_keys = keys(&next);
    let claim_keys = [
        old_keys[0].clone(),
        old_keys[1].clone(),
        old_keys[2].clone(),
        old_keys[3].clone(),
        old_keys[5].clone(),
        old_keys[6].clone(),
        next_keys[5].clone(),
        next_keys[6].clone(),
        inner
            .coordinator
            .timeshift_owner_key(&old.request.session_id),
        old_keys[7].clone(),
    ];
    let field = |entry: &Entry, name| {
        entry
            .request
            .pool_fields
            .get(name)
            .map_or("", String::as_str)
            .to_owned()
    };
    let args = [
        old.token.clone(),
        next.token.clone(),
        provider_token,
        gen,
        old.request.virtual_channel_id.clone(),
        next.request.virtual_channel_id.clone(),
        old.request.session_id.clone(),
        field(&old, "media_id"),
        field(&next, "media_id"),
        field(&next, "provider_timestamp"),
        field(&next, "provider_tz_name"),
        field(&next, "rust_duration_secs"),
        timestamp(),
        field(&next, "user_id"),
        field(&next, "account_id"),
        field(&next, "profile_id"),
        field(&next, "dispatcharr_stream_id"),
        field(&next, "stream_id"),
        field(&next, "client_ip"),
        field(&next, "client_user_agent"),
    ];
    let argv: Vec<_> = args.iter().map(|value| value.as_bytes()).collect();
    // The rollback guard travels through the actual Redis invocation even
    // when its observer times out. Do not repeat this mutation.
    let (claimed, mut rollback): (i64, ClaimRollback) = inner
        .transport
        .command(CLAIM, &claim_keys, &argv, rollback)
        .await
        .map_err(|_| unavailable())?;
    if claimed != 1 {
        return Err(busy());
    }
    let mut entries = inner.entries.lock().unwrap();
    if *inner.stopped.borrow()
        || !entries
            .get(&old.request.session_id)
            .is_some_and(|entry| Arc::ptr_eq(entry, &old))
    {
        return Err(unavailable());
    }
    let mut owner = rollback.0.take().expect("owned archive claim");
    let lease = owner.lease.take().expect("retained archive lease");
    owner.settled = true;
    *next.lease.lock().unwrap() = Some(lease);
    entries.insert(next.request.session_id.clone(), next.clone());
    drop(entries);
    drop(owner);
    start_setup_refresh(&inner, &next);
    Ok(ArchiveTicket {
        refreshed: None,
        adopted_idle: false,
        rollback: Some(Rollback { inner, entry: next }),
    })
}

struct ClaimRollback(Option<Retirement>);

impl Drop for ClaimRollback {
    fn drop(&mut self) {
        if let Some(owner) = self.0.take() {
            owner.inner.runtime.clone().spawn(owner.retire());
        }
    }
}

struct Retirement {
    inner: Arc<Inner>,
    old: Arc<Entry>,
    next: Arc<Entry>,
    epoch: u64,
    lease: Option<Arc<Lease>>,
    settled: bool,
    _attempt: HopAttempt,
}

impl Retirement {
    async fn retire(self) {
        let inner = self.inner.clone();
        let mut owned_keys = keys(&self.old);
        let target_keys = keys(&self.next);
        owned_keys.extend([target_keys[5].clone(), target_keys[6].clone()]);
        let args = [
            self.old.token.clone(),
            self.next.token.clone(),
            self.old
                .request
                .pool_fields
                .get("user_id")
                .cloned()
                .unwrap_or_default(),
            self.old.request.session_id.clone(),
            format!("{}-{}", self.old.token, self.epoch),
            format!("{}-0", self.next.token),
        ];
        let argv: Vec<_> = args.iter().map(|value| value.as_bytes()).collect();
        // Fence both candidate archive tokens in one mutation before releasing
        // the lease. A delayed original claim must lose that same fence.
        let result: Result<(i64, Self), _> = inner
            .transport
            .command(&ABORT, &owned_keys, &argv, self)
            .await;
        let Ok((0 | 1, mut owner)) = result else {
            return;
        };
        owner.settled = true;
        if let Some(lease) = owner.lease.take() {
            if lease.release_retained_result().await.is_err() {
                inner.cleanup_failed.store(true, Ordering::SeqCst);
            }
        }
    }
}

impl Drop for Retirement {
    fn drop(&mut self) {
        if !self.settled {
            self.inner.cleanup_failed.store(true, Ordering::SeqCst);
            if let Some(lease) = self.lease.as_ref() {
                lease.expire_after_unconfirmed_cleanup();
            }
        }
        let mut entries = self.inner.entries.lock().unwrap();
        if entries
            .get(&self.old.request.session_id)
            .is_some_and(|entry| Arc::ptr_eq(entry, &self.old))
        {
            entries.remove(&self.old.request.session_id);
        }
    }
}
