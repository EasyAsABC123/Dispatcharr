//! Real delayed-Redis guard lifetime tests. Original Dispatcharr author credit.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
struct Guard(Arc<AtomicUsize>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
async fn paused() -> (RealtimeBus, redis::aio::MultiplexedConnection) {
    let url = std::env::var("REDIS_TEST_URL").expect("isolated Redis required");
    let bus = RealtimeBus::connect(
        &url,
        BusConfig {
            command_timeout: Duration::from_millis(40),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let connection = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    (bus, connection)
}
async fn pause(connection: &mut redis::aio::MultiplexedConnection) {
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(250)
        .arg("ALL")
        .query_async(connection)
        .await
        .unwrap();
}
async fn released(dropped: &AtomicUsize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while dropped.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(dropped.load(Ordering::Acquire), 1);
}
#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn cancelled_membership_lookup_retains_caller_guard_through_real_reply() {
    let (bus, mut connection) = paused().await;
    let member = bus.reserve_member().unwrap();
    member.join().await.unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard: Arc<dyn Send + Sync> = Arc::new(Guard(dropped.clone()));
    pause(&mut connection).await;
    let result = tokio::time::timeout(
        Duration::from_millis(10),
        bus.send_retained(
            "updates",
            serde_json::json!({"type":"update","data":{"retained":"membership"}}),
            guard,
        ),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(dropped.load(Ordering::Acquire), 0);
    released(&dropped).await;
    member.release().await;
}
#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn timed_out_publish_retains_caller_and_byte_guards_through_real_reply() {
    let (bus, mut connection) = paused().await;
    let member = bus.reserve_member().unwrap();
    member.join().await.unwrap();
    let maximum = bus.available_publish_bytes();
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard: Arc<dyn Send + Sync> = Arc::new(Guard(dropped.clone()));
    let message = serde_json::json!({"type":"update","data":{"retained":"publish"}});
    pause(&mut connection).await;
    let result = bus
        .publish_members(message.clone(), vec![member.id().to_owned()], Some(guard))
        .await;
    assert_eq!(result, Err(UpdateEventError::Unavailable));
    assert_eq!(dropped.load(Ordering::Acquire), 0);
    assert!(bus.available_publish_bytes() < maximum);
    released(&dropped).await;
    assert_eq!(bus.available_publish_bytes(), maximum);
    let event = bus.pop().await.unwrap().unwrap();
    assert_eq!(event.message["data"], message["data"]);
    event.ack().await.unwrap();
    member.release().await;
}
