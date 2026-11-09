//! Actual Redis ownership, queue, backup and cancellation gates; upstream credit retained.
use dispatcharr_contracts::UpdateEventError;
use dispatcharr_realtime::{BusConfig, RealtimeBus};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::time;
fn url() -> String {
    std::env::var("REDIS_TEST_URL").expect("isolated Redis required")
}
async fn redis() -> redis::aio::MultiplexedConnection {
    redis::Client::open(url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn process_prefixes_recipients_order_expiry_and_acknowledged_binary_reads() {
    let first = RealtimeBus::connect(&url(), BusConfig::default())
        .await
        .unwrap();
    let second = RealtimeBus::connect(&url(), BusConfig::default())
        .await
        .unwrap();
    let a = first.reserve_member().unwrap();
    let b = first.reserve_member().unwrap();
    let c = second.reserve_member().unwrap();
    a.join().await.unwrap();
    b.join().await.unwrap();
    c.join().await.unwrap();
    for number in 0..3 {
        first
            .publish(
                "updates",
                json!({"type":"update","data":{"number":number,"fraction":1.5}}),
            )
            .await
            .unwrap();
    }
    let mut connection = redis().await;
    let ttl: i64 = redis::cmd("TTL")
        .arg(format!("asgi{}", first.nonlocal_name()))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!((1..=60).contains(&ttl));
    for number in 0..3 {
        let incoming = first.pop().await.unwrap().unwrap();
        assert_eq!(
            incoming.message,
            json!({"type":"update","data":{"number":number,"fraction":1.5}})
        );
        assert_eq!(incoming.recipients, vec![a.id(), b.id()]);
        let backup: u64 = redis::cmd("ZCARD")
            .arg(format!("asgi{}$inflight", first.nonlocal_name()))
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(backup, 1);
        assert!(first.available_read_bytes() < first.config().max_read_bytes);
        incoming.ack().await.unwrap();
        let incoming = second.pop().await.unwrap().unwrap();
        assert_eq!(incoming.message["data"]["number"], number);
        assert_eq!(incoming.recipients, vec![c.id()]);
        incoming.ack().await.unwrap();
    }
    assert!(first.pop().await.unwrap().is_none());
    assert_eq!(first.available_read_bytes(), first.config().max_read_bytes);
    a.release().await;
    b.release().await;
    c.release().await;
    assert_eq!(
        first.available_connections(),
        first.config().max_connections
    );
}

#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn abandoned_delivery_replays_in_order_and_capacity_drops_newest_like_channel_layer() {
    let bus = RealtimeBus::connect(
        &url(),
        BusConfig {
            channel_capacity: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let member = bus.reserve_member().unwrap();
    member.join().await.unwrap();
    for number in 0..3 {
        bus.publish("updates", json!({"type":"update","data":{"number":number}}))
            .await
            .unwrap();
    }
    let incoming = bus.pop().await.unwrap().unwrap();
    assert_eq!(incoming.message["data"]["number"], 0);
    drop(incoming);
    let replay = bus.pop().await.unwrap().unwrap();
    assert_eq!(replay.message["data"]["number"], 0);
    replay.ack().await.unwrap();
    let next = bus.pop().await.unwrap().unwrap();
    assert_eq!(next.message["data"]["number"], 1);
    next.ack().await.unwrap();
    assert!(bus.pop().await.unwrap().is_none());
    member.release().await;
}

#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn release_cannot_overtake_cancelled_pending_join_or_release_admission_early() {
    let bus = RealtimeBus::connect(
        &url(),
        BusConfig {
            max_connections: 1,
            command_timeout: Duration::from_millis(40),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let member = Arc::new(bus.reserve_member().unwrap());
    let id = member.id().to_owned();
    let mut connection = redis().await;
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(200)
        .arg("ALL")
        .query_async(&mut connection)
        .await
        .unwrap();
    let joining = member.clone();
    let join = tokio::spawn(async move { joining.join().await });
    time::sleep(Duration::from_millis(10)).await;
    let releasing = member.clone();
    let release = tokio::spawn(async move { releasing.release().await });
    time::sleep(Duration::from_millis(60)).await;
    assert_eq!(bus.available_connections(), 0);
    assert!(matches!(
        bus.reserve_member(),
        Err(UpdateEventError::Capacity)
    ));
    join.abort();
    let _ = join.await;
    time::timeout(Duration::from_secs(2), release)
        .await
        .unwrap()
        .unwrap();
    let present: Option<f64> = redis::cmd("ZSCORE")
        .arg("asgi:group:updates")
        .arg(id)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(present.is_none());
    assert_eq!(bus.available_connections(), 1);
}

#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn cancelled_pop_retains_physical_budget_and_message_backup_until_next_receive() {
    let bus = RealtimeBus::connect(
        &url(),
        BusConfig {
            max_message_bytes: 1024,
            max_read_bytes: 1024,
            max_publish_bytes: 4096,
            command_timeout: Duration::from_millis(40),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let member = bus.reserve_member().unwrap();
    member.join().await.unwrap();
    bus.publish(
        "updates",
        json!({"type":"update","data":{"body":"x".repeat(400)}}),
    )
    .await
    .unwrap();
    let mut connection = redis().await;
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(200)
        .arg("ALL")
        .query_async(&mut connection)
        .await
        .unwrap();
    let reader = bus.clone();
    let task = tokio::spawn(async move { reader.pop().await });
    time::sleep(Duration::from_millis(10)).await;
    task.abort();
    let _ = task.await;
    time::sleep(Duration::from_millis(60)).await;
    assert_eq!(bus.available_read_bytes(), 0);
    time::sleep(Duration::from_millis(240)).await;
    let replay = bus.pop().await.unwrap().unwrap();
    assert_eq!(replay.message["data"]["body"], "x".repeat(400));
    replay.ack().await.unwrap();
    assert!(bus.pop().await.unwrap().is_none());
    member.release().await;
}
