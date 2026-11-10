//! Actual current-state VOD monitor/reopen cause isolation; acceptance requires owned fixture.
//! Intended child of sessions::session_restart_peer after explicit quiet release.
//! Original Dispatcharr authors retain full application/algorithm credit.
use super::super::SessionPool;
use redis::AsyncCommands;
use std::{sync::atomic::Ordering, time::Duration};

async fn fixture(
    suffix: &str,
) -> (
    SessionPool,
    redis::aio::MultiplexedConnection,
    crate::VodSource,
    crate::VodRequest,
    String,
) {
    let (old, control, source, request, id) = super::physical_fixture(suffix).await;
    let (url, _) = super::config();
    let pool = SessionPool::connect(
        &url,
        old.inner.coordinator.clone(),
        1,
        2,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    drop(old);
    (pool, control, source, request, id)
}
async fn stopped(ticket: &super::super::Ticket) {
    let mut stop = ticket.session.stop.subscribe();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !*stop.borrow_and_update() {
            stop.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}
fn owner(id: &str) -> String {
    format!("{id}:vod:session:{id}:owner")
}
async fn unavailable(result: Result<super::super::Ticket, dispatcharr_store::StoreError>) {
    match result {
        Err(dispatcharr_store::StoreError::StatusBody(503, value)) => {
            assert_eq!(value, serde_json::json!("No available stream"))
        }
        _ => panic!("expected exact stopped-session503"),
    }
}
async fn finished(pool: &SessionPool, control: &mut redis::aio::MultiplexedConnection, id: &str) {
    tokio::time::timeout(Duration::from_secs(5), pool.drain())
        .await
        .unwrap();
    let count: Option<u64> = control.get("profile_connections:777").await.unwrap();
    assert_eq!(count.unwrap_or(0), 0);
    let exists: bool = control.exists(super::super::key(id)).await.unwrap();
    assert!(!exists);
}
#[tokio::test]
#[ignore = "exact current missing metadata monitor/reopen cause"]
async fn missing_metadata_alone_stops_live_ticket_and_reopen_has_free_request_slot() {
    let (pool, mut control, source, request, id) = fixture("cause-missing-metadata").await;
    let ticket = pool.enter(&id, source.clone(), &request).await.unwrap();
    let _: u64 = control.del(super::super::key(&id)).await.unwrap();
    stopped(&ticket).await;
    assert!(!*ticket.session.lease.lost().borrow());
    assert!(!ticket.session.state.lock().await.closed);
    let ping: String = redis::cmd("PING").query_async(&mut control).await.unwrap();
    assert_eq!(ping, "PONG");
    let stop: bool = control
        .exists(format!("vod_proxy:client:{id}:stop"))
        .await
        .unwrap();
    assert!(!stop);
    assert_eq!(pool.inner.requests.available_permits(), 1);
    unavailable(pool.enter(&id, source, &request).await).await;
    assert_eq!(pool.inner.requests.available_permits(), 1);
    drop(ticket);
    finished(&pool, &mut control, &id).await;
}
#[tokio::test]
#[ignore = "isolate lost coordination lease with matching metadata"]
async fn lost_coordination_alone_stops_even_with_matching_metadata() {
    let (pool, mut control, source, request, id) = fixture("cause-lost-lease").await;
    let ticket = pool.enter(&id, source.clone(), &request).await.unwrap();
    let _: u64 = control.del(owner(&id)).await.unwrap();
    assert!(!ticket.session.lease.heartbeat().await.unwrap());
    stopped(&ticket).await;
    let metadata: String = control
        .hget(super::super::key(&id), "rust_owner_token")
        .await
        .unwrap();
    assert_eq!(metadata, ticket.session.metadata_token);
    assert!(*ticket.session.lease.lost().borrow());
    assert_eq!(pool.inner.requests.available_permits(), 1);
    unavailable(pool.enter(&id, source, &request).await).await;
    drop(ticket);
    finished(&pool, &mut control, &id).await;
}
#[tokio::test]
#[ignore = "actual foreign replacement survives old close/ledger release"]
async fn replaced_session_and_provider_survive_late_old_ticket_cleanup() {
    let (pool, mut control, source, request, id) = fixture("cause-replacement").await;
    let ticket = pool.enter(&id, source.clone(), &request).await.unwrap();
    let old_state = ticket.session.state.lock().await;
    let old_ledger = format!("{id}:lease:{}", ticket.session.lease.token());
    let count: u64 = control.get("profile_connections:777").await.unwrap();
    assert_eq!(count, 1);
    // Controlled no-persistence precondition in an exclusive fixture only.
    // Never use these DEL operations as a production recovery implementation.
    let _: u64 = control
        .del(&[
            super::super::key(&id),
            owner(&id),
            old_ledger.clone(),
            "profile_connections:777".to_owned(),
        ])
        .await
        .unwrap();
    let _: u64 = control
        .zrem(format!("{id}:leases"), old_ledger)
        .await
        .unwrap();
    let (url, _) = super::config();
    let foreign = SessionPool::connect(
        &url,
        pool.inner.coordinator.clone(),
        1,
        1,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let next = foreign.enter(&id, source, &request).await.unwrap();
    let new_token = next.session.metadata_token.clone();
    let new_owner = next.session.lease.token().to_owned();
    drop(old_state);
    stopped(&ticket).await;
    drop(ticket);
    tokio::time::timeout(Duration::from_secs(5), pool.drain())
        .await
        .unwrap();
    let metadata: String = control
        .hget(super::super::key(&id), "rust_owner_token")
        .await
        .unwrap();
    assert_eq!(metadata, new_token);
    let actual: String = control.get(owner(&id)).await.unwrap();
    assert_eq!(actual, new_owner);
    let count: u64 = control.get("profile_connections:777").await.unwrap();
    assert_eq!(count, 1);
    assert!(next.session.lease.heartbeat().await.unwrap());
    assert_eq!(foreign.inner.requests.available_permits(), 0);
    assert_eq!(pool.inner.pending_cleanups.load(Ordering::Acquire), 0);
    drop(next);
    finished(&foreign, &mut control, &id).await;
}
#[tokio::test]
#[ignore = "explicit stop must never become a recovery permission"]
async fn explicit_stop_with_owned_metadata_is_terminal() {
    let (pool, mut control, source, request, id) = fixture("cause-explicit-stop").await;
    let ticket = pool.enter(&id, source.clone(), &request).await.unwrap();
    let key = format!("vod_proxy:client:{id}:stop");
    let _: () = control.set_ex(&key, "true", 30).await.unwrap();
    stopped(&ticket).await;
    let metadata: String = control
        .hget(super::super::key(&id), "rust_owner_token")
        .await
        .unwrap();
    assert_eq!(metadata, ticket.session.metadata_token);
    assert!(!*ticket.session.lease.lost().borrow());
    unavailable(pool.enter(&id, source, &request).await).await;
    let exists: bool = control.exists(&key).await.unwrap();
    assert!(exists);
    drop(ticket);
    finished(&pool, &mut control, &id).await;
    let _: u64 = control.del(key).await.unwrap();
}
