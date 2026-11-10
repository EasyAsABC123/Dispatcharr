//! Actual Redis LocalScope controls; HTTP byte/event adapters remain separate.
//! Original Dispatcharr authors retain full application and algorithm credit.
#[path = "recovery_generation.rs"]
pub(crate) mod functional;
use dispatcharr_clients::vod::{admit_fenced_physical, Admission, AdmissionFence};
use dispatcharr_contracts::BoxFuture;
use functional::{
    prototype::{
        self,
        local::{Identity, Initial, Limits, LocalScope, Progress, StopHook},
    },
    Fixture,
};
use redis::AsyncCommands;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::watch;
struct Stops(AtomicUsize);
impl StopHook for Stops {
    fn emit(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
        })
    }
}
pub(crate) async fn initial() -> (Fixture, LocalScope, watch::Receiver<bool>) {
    initial_with_connection(None).await
}
pub(crate) async fn initial_with_connection(
    proxy: Option<&str>,
) -> (Fixture, LocalScope, watch::Receiver<bool>) {
    initial_from_fixture(Fixture::new().await, proxy).await
}
async fn initial_from_fixture(
    mut f: Fixture,
    proxy: Option<&str>,
) -> (Fixture, LocalScope, watch::Receiver<bool>) {
    if let Some(proxy) = proxy {
        let connection = redis::Client::open(proxy)
            .unwrap()
            .get_connection_manager_with_config(
                redis::aio::ConnectionManagerConfig::new().set_number_of_retries(0),
            )
            .await
            .unwrap();
        f.recovery = prototype::Recovery::new(
            f.coordinator.clone(),
            connection,
            f.namespace.clone(),
            1,
            Duration::from_millis(100),
        )
        .unwrap();
    }
    let mut fields = f.admission().fields;
    fields.insert("rust_owner_token".into(), f.token.clone());
    fields.insert("content_obj_type".into(), "movie".into());
    fields.insert("m3u_profile_id".into(), f.profile.to_string());
    fields.insert("bytes_sent".into(), "0".into());
    let policy = dispatcharr_contracts::UserLimitPolicy::default();
    let owner = f.owner();
    let stop_key = f.stop();
    assert!(admit_fenced_physical(
        &mut f.redis,
        Admission {
            session_id: &f.id,
            fields: &fields,
            limit: 0,
            policy: &policy,
            create: true,
            token: &f.token,
            scan_limit: 50000,
            timeout: Duration::from_secs(2),
            ttl: Duration::from_secs(3600)
        },
        AdmissionFence {
            owner_key: &owner,
            owner_token: f.old.token(),
            stop_key: &stop_key
        }
    )
    .await
    .unwrap());
    let (stop, receiver) = watch::channel(false);
    let scope = LocalScope::adopt(
        f.recovery.clone(),
        Initial {
            id: f.id.clone(),
            lease: f.old.clone(),
            metadata_token: f.token.clone(),
            profile_id: f.profile,
            identity: Identity {
                user_id: "prototype-user".into(),
                content_uuid: f.id.clone(),
                content_kind: "movie".into(),
            },
            lifetime: Arc::new(()),
            stop,
        },
        Limits {
            bodies: 2,
            controls: 2,
            idle_grace: Duration::from_secs(1),
        },
    )
    .await
    .unwrap();
    (f, scope, receiver)
}
fn admission(f: &Fixture) -> prototype::NewAdmission {
    let mut data = f.admission();
    data.fields
        .insert("content_obj_type".into(), "movie".into());
    data.fields
        .insert("m3u_profile_id".into(), f.profile.to_string());
    data
}
async fn wait(mut condition: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
#[ignore = "actual owned Redis Source176/177 current-generation accounting"]
async fn local_scope_old_progress_and_close_delegate_to_current_without_ending_younger_body() {
    let (mut f, scope, stop) = initial().await;
    let hook = Arc::new(Stops(AtomicUsize::new(0)));
    let old = scope
        .attach_reserved(Arc::new(()), true, Some(hook.clone()))
        .await
        .unwrap();
    old.started();
    assert!(matches!(
        scope.retire_missing().await,
        Err(prototype::Error::StillOwned)
    ));
    f.erase_owned_generation().await;
    scope.retire_missing().await.unwrap();
    let young = scope
        .attach_pending(Arc::new(()), Some(hook.clone()))
        .await
        .unwrap();
    scope
        .reopen(&young, f.request(), admission(&f))
        .await
        .unwrap();
    young.started();
    assert_eq!(scope.retired_generations().await, 1);
    assert_eq!(
        scope.progress(&old, 819200).await.unwrap(),
        Progress::Updated
    );
    assert_eq!(
        f.redis
            .hget::<_, _, u64>(f.metadata(), "bytes_sent")
            .await
            .unwrap(),
        819200
    );
    assert_eq!(
        f.redis
            .hget::<_, _, i64>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        1
    );
    let closed = scope.close(&old).await.unwrap();
    assert!(closed.changed && closed.deleted && closed.event_eligible);
    assert_eq!(closed.remaining, 0);
    assert!(!young.is_closed());
    assert!(!*stop.borrow());
    assert_eq!(scope.body_slots(), 1);
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 0);
    assert!(!f.redis.exists::<_, bool>(f.metadata()).await.unwrap());
    assert_eq!(hook.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        scope.progress(&young, 819200).await.unwrap(),
        Progress::Missing
    );
    assert!(!scope.close(&young).await.unwrap().changed);
    assert_eq!(scope.body_slots(), 2);
    assert_eq!(scope.control_slots(), 2);
    assert!(scope.cleanup_succeeded());
    f.empty().await;
}
#[tokio::test]
#[ignore = "actual owned Redis repeated generations and retained closed capabilities"]
async fn local_scope_repeated_generations_do_not_pin_old_jobs_or_closed_body_permits() {
    let (mut f, scope, _) = initial().await;
    let old = scope
        .attach_reserved(Arc::new(()), true, None)
        .await
        .unwrap();
    old.started();
    f.erase_owned_generation().await;
    scope.retire_missing().await.unwrap();
    let mut closed_handles = Vec::new();
    for _ in 0..4 {
        let body = scope.attach_pending(Arc::new(()), None).await.unwrap();
        scope
            .reopen(&body, f.request(), admission(&f))
            .await
            .unwrap();
        body.started();
        assert!(scope.close(&body).await.unwrap().deleted);
        assert_eq!(
            scope.body_slots(),
            1,
            "the closed handle may remain alive without holding body admission"
        );
        assert_eq!(
            f.recovery.available_slots(),
            1,
            "completed generation must release setup job budget"
        );
        closed_handles.push(body);
        scope.retire_missing().await.unwrap();
        assert!(scope.retired_generations().await <= 2);
    }
    assert!(!old.is_closed());
    assert!(!scope.close(&old).await.unwrap().changed);
    drop(closed_handles);
    drop(scope);
    f.empty().await;
}
#[tokio::test]
#[ignore = "actual owned Redis capability bounds, idempotent close, shutdown watch"]
async fn local_scope_body_capability_bounds_and_shutdown_are_independent_of_public_counts() {
    let (mut f, scope, stop) = initial().await;
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let first = scope.attach_reserved(caller, true, None).await.unwrap();
    first.started();
    let second = scope.attach_pending(Arc::new(()), None).await.unwrap();
    assert!(matches!(
        scope.attach_reserved(Arc::new(()), true, None).await,
        Err(prototype::Error::Capacity)
    ));
    scope.stop();
    assert!(*stop.borrow());
    assert!(matches!(
        scope.attach_reserved(Arc::new(()), false, None).await,
        Err(prototype::Error::Stopped)
    ));
    // The local stop watch closes delivery; the Redis stop key is unchanged.
    assert!(!f.redis.exists::<_, bool>(f.stop()).await.unwrap());
    scope.close(&first).await.unwrap();
    assert!(weak.upgrade().is_none());
    assert!(!scope.close(&first).await.unwrap().changed);
    scope.close(&second).await.unwrap();
    assert_eq!(scope.body_slots(), 2);
    f.empty().await;
}
#[tokio::test]
#[ignore = "actual owned Redis global stop, user identity and foreign owner fences"]
async fn local_scope_never_delegates_through_foreign_or_changed_identity() {
    let (mut f, scope, stop) = initial().await;
    let body = scope
        .attach_reserved(Arc::new(()), true, None)
        .await
        .unwrap();
    body.started();
    let _: usize = f
        .redis
        .hset(f.metadata(), "user_id", "another-user")
        .await
        .unwrap();
    assert_eq!(
        scope.progress(&body, 819200).await.unwrap(),
        Progress::IdentityChanged
    );
    assert_eq!(
        f.redis
            .hget::<_, _, u64>(f.metadata(), "bytes_sent")
            .await
            .unwrap(),
        0
    );
    let _: usize = f
        .redis
        .hset(f.metadata(), "user_id", "prototype-user")
        .await
        .unwrap();
    let _: () = f.redis.set(f.stop(), "stop").await.unwrap();
    assert_eq!(
        scope.progress(&body, 819200).await.unwrap(),
        Progress::Stopped
    );
    assert!(*stop.borrow());
    let _: usize = f.redis.del(f.stop()).await.unwrap();
    let _: () = f.redis.set(f.owner(), "foreign-owner").await.unwrap();
    assert_eq!(
        scope.progress(&body, 819200).await.unwrap(),
        Progress::OwnerChanged
    );
    assert_eq!(
        f.redis
            .hget::<_, _, u64>(f.metadata(), "bytes_sent")
            .await
            .unwrap(),
        0
    );
    assert!(!scope.close(&body).await.unwrap().changed);
    assert_eq!(
        f.redis.get::<_, String>(f.owner()).await.unwrap(),
        "foreign-owner"
    );
    // Exclusive fixture teardown: remove foreign test marker, restore no actor.
    let _: usize = f.redis.del(&[f.owner(), f.metadata()]).await.unwrap();
    f.old.release().await.unwrap();
    f.old.settlement().wait().await;
    f.empty().await;
}
#[tokio::test]
#[ignore = "actual owned Redis abandoned body automatically closes once"]
async fn local_scope_body_drop_keeps_caller_until_real_owned_cleanup() {
    let (mut f, scope, _) = initial().await;
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let body = scope.attach_reserved(caller, true, None).await.unwrap();
    body.started();
    drop(body);
    wait(async || weak.upgrade().is_none() && scope.body_slots() == 2).await;
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 0);
    assert!(!f.redis.exists::<_, bool>(f.metadata()).await.unwrap());
    f.empty().await;
}

#[tokio::test]
#[ignore = "actual pending setup close must not consume an existing sibling reservation"]
async fn local_scope_failed_pending_body_never_decrements_an_unrelated_public_body() {
    let (mut f, scope, _) = initial().await;
    let old = scope
        .attach_reserved(Arc::new(()), true, None)
        .await
        .unwrap();
    old.started();
    let pending = scope.attach_pending(Arc::new(()), None).await.unwrap();
    assert!(!scope.close(&pending).await.unwrap().changed);
    assert_eq!(
        f.redis
            .hget::<_, _, i64>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        1
    );
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    scope.close(&old).await.unwrap();
    f.empty().await;
}

#[tokio::test]
#[ignore = "actual persistent stop preserves stop key while cleaning only owned metadata/quota"]
async fn local_scope_persistent_stop_does_not_prevent_owned_teardown() {
    let (mut f, scope, stop) = initial().await;
    let body = scope
        .attach_reserved(Arc::new(()), true, None)
        .await
        .unwrap();
    body.started();
    let _: () = f.redis.set(f.stop(), "stop").await.unwrap();
    assert_eq!(
        scope.progress(&body, 819200).await.unwrap(),
        Progress::Stopped
    );
    assert!(*stop.borrow());
    let closed = scope.close(&body).await.unwrap();
    assert!(closed.deleted);
    assert_eq!(f.redis.get::<_, String>(f.stop()).await.unwrap(), "stop");
    let _: usize = f.redis.del(f.stop()).await.unwrap();
    f.empty().await;
}

async fn receipt_keys(f: &mut Fixture) -> Vec<String> {
    let keys: Vec<String> = f
        .redis
        .keys(format!("{}:vod:entries:{}:*", f.namespace, f.id))
        .await
        .unwrap();
    assert!(keys.len() <= 8, "owned fixture receipt generation bound");
    keys
}
#[tokio::test]
#[ignore = "acknowledged close accounting is independent of expired private receipt"]
async fn local_scope_acknowledged_entry_close_survives_missing_receipt_ttl() {
    let (mut f, scope, _) = initial().await;
    let original = scope
        .attach_reserved(Arc::new(()), true, None)
        .await
        .unwrap();
    original.started();
    let entry = scope
        .attach_entry_pending(Arc::new(()), None)
        .await
        .unwrap();
    scope.enter_current(&entry, admission(&f)).await.unwrap();
    let keys = receipt_keys(&mut f).await;
    assert_eq!(keys.len(), 1);
    let _: usize = f.redis.del(keys).await.unwrap();
    let closed = scope.close(&entry).await.unwrap();
    assert_eq!(closed.remaining, 1);
    assert_eq!(
        f.redis
            .hget::<_, _, i64>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        1
    );
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    scope.stop();
    let _ = scope.close(&original).await;
    scope.drain().await;
    assert!(scope.cleanup_succeeded());
    f.empty().await;
}
#[tokio::test]
#[ignore = "retired acknowledged body's private receipt is forgotten without current public ownership"]
async fn local_scope_retired_entry_private_receipt_is_reclaimed_after_generation_change() {
    let (mut f, scope, _) = initial().await;
    let original = scope
        .attach_reserved(Arc::new(()), true, None)
        .await
        .unwrap();
    original.started();
    let entry = scope
        .attach_entry_pending(Arc::new(()), None)
        .await
        .unwrap();
    scope.enter_current(&entry, admission(&f)).await.unwrap();
    entry.started();
    let keys = receipt_keys(&mut f).await;
    assert_eq!(keys.len(), 1);
    assert_eq!(scope.close(&original).await.unwrap().remaining, 1);
    f.erase_owned_generation().await;
    scope.retire_missing().await.unwrap();
    let young = scope.attach_pending(Arc::new(()), None).await.unwrap();
    scope
        .reopen(&young, f.request(), admission(&f))
        .await
        .unwrap();
    young.started();
    assert!(f.redis.exists::<_, bool>(&keys[0]).await.unwrap());
    let closed = scope.close(&entry).await.unwrap();
    assert!(
        closed.deleted,
        "known old body still delegates Source close to current public generation"
    );
    assert!(
        !f.redis.exists::<_, bool>(&keys[0]).await.unwrap(),
        "old private receipt must not leak until TTL"
    );
    assert!(!young.is_closed());
    assert_eq!(scope.body_slots(), 1);
    scope.stop();
    let _ = scope.close(&young).await;
    scope.drain().await;
    assert!(scope.cleanup_succeeded());
    f.empty().await;
}
#[tokio::test]
#[ignore = "private receipt count/byte capacity rejects before public counter mutation"]
async fn local_scope_entry_receipt_count_and_bytes_are_bounded() {
    let (mut f, scope, _) = initial().await;
    let original = scope
        .attach_reserved(Arc::new(()), true, None)
        .await
        .unwrap();
    original.started();
    let entry = scope
        .attach_entry_pending(Arc::new(()), None)
        .await
        .unwrap();
    scope.enter_current(&entry, admission(&f)).await.unwrap();
    let key = receipt_keys(&mut f).await.pop().unwrap();
    assert_eq!(scope.close(&entry).await.unwrap().remaining, 1);
    for bytes in [false, true] {
        let _: usize = f.redis.del(&key).await.unwrap();
        if bytes {
            let _: usize = f
                .redis
                .hset(&key, "fixture-large", vec![b'x'; 16384])
                .await
                .unwrap();
        } else {
            let _: () = f
                .redis
                .hset_multiple(&key, &[("fixture-a", "x"), ("fixture-b", "x")])
                .await
                .unwrap();
        }
        let candidate = scope
            .attach_entry_pending(Arc::new(()), None)
            .await
            .unwrap();
        assert!(matches!(
            scope.enter_current(&candidate, admission(&f)).await,
            Err(prototype::Error::Capacity)
        ));
        drop(candidate);
        assert_eq!(
            f.redis
                .hget::<_, _, i64>(f.metadata(), "active_streams")
                .await
                .unwrap(),
            1
        );
        assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    }
    let _: usize = f.redis.del(key).await.unwrap();
    let candidate = scope
        .attach_entry_pending(Arc::new(()), None)
        .await
        .unwrap();
    scope
        .enter_current(&candidate, admission(&f))
        .await
        .unwrap();
    assert_eq!(scope.close(&candidate).await.unwrap().remaining, 1);
    scope.stop();
    let _ = scope.close(&original).await;
    scope.drain().await;
    assert!(scope.cleanup_succeeded());
    f.empty().await;
}

#[tokio::test]
#[ignore = "actual retained private receipt memory must include the newly written field"]
async fn local_scope_receipt_byte_guard_includes_new_field_headroom() {
    let mut f = Fixture::new().await;
    f.old.release().await.unwrap();
    f.old.settlement().wait().await;
    f.id = format!(
        "{}{}",
        "long-session-".repeat(7),
        uuid::Uuid::new_v4().simple()
    );
    f.id.truncate(120);
    f.old = Arc::new(f.coordinator.acquire_vod(f.request()).await.unwrap());
    let (mut f, scope, _) = initial_from_fixture(f, None).await;
    let original = scope
        .attach_reserved(Arc::new(()), true, None)
        .await
        .unwrap();
    original.started();
    let entry = scope
        .attach_entry_pending(Arc::new(()), None)
        .await
        .unwrap();
    scope.enter_current(&entry, admission(&f)).await.unwrap();
    let key = receipt_keys(&mut f).await.pop().unwrap();
    scope.close(&entry).await.unwrap();
    for length in (1024..3072).step_by(16) {
        let _: usize = f.redis.del(&key).await.unwrap();
        let _: usize = f
            .redis
            .hset(&key, "fixture-bounded-padding", vec![b'x'; length])
            .await
            .unwrap();
        let before: usize = redis::cmd("MEMORY")
            .arg("USAGE")
            .arg(&key)
            .arg("SAMPLES")
            .arg(0)
            .query_async(&mut f.redis)
            .await
            .unwrap();
        let candidate = scope
            .attach_entry_pending(Arc::new(()), None)
            .await
            .unwrap();
        match scope.enter_current(&candidate, admission(&f)).await {
            Ok(()) => {
                let after: usize = redis::cmd("MEMORY")
                    .arg("USAGE")
                    .arg(&key)
                    .arg("SAMPLES")
                    .arg(0)
                    .query_async(&mut f.redis)
                    .await
                    .unwrap();
                assert!(after<=3072,"retained receipt guard before={before} after={after} threshold=3072 padding={length}");
                assert_eq!(scope.close(&candidate).await.unwrap().remaining, 1);
            }
            Err(prototype::Error::Capacity) => drop(candidate),
            Err(error) => panic!("unexpected receipt capacity result: {error:?}"),
        }
    }
    let _: usize = f.redis.del(&key).await.unwrap();
    scope.stop();
    let _ = scope.close(&original).await;
    scope.drain().await;
    f.empty().await;
}

#[tokio::test]
#[ignore = "missing unique lease cannot remain on the healthy legacy entry path"]
async fn local_scope_missing_unique_ledger_activates_partial_state_guard() {
    let (mut f, scope, _) = initial().await;
    let body = scope
        .attach_reserved(Arc::new(()), true, None)
        .await
        .unwrap();
    body.started();
    let _: usize = f.redis.del(f.ledger()).await.unwrap();
    assert!(matches!(
        scope.retire_missing().await,
        Err(prototype::Error::PartialState)
    ));
    assert!(
        scope.activated(),
        "lost unique ledger must not leave SessionPool on unfenced healthy ENTER path"
    );
    assert!(f.redis.exists::<_, bool>(f.owner()).await.unwrap());
    assert_eq!(
        f.redis
            .hget::<_, _, i64>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        1
    );
    scope.stop();
    let _ = scope.close(&body).await;
    scope.drain().await;
    // Deliberate native-private corruption: no successful provider cleanup claim.
    let _: usize = f
        .redis
        .del(&[f.owner(), f.metadata(), f.counter()])
        .await
        .unwrap();
}
