//! Codec and actual disposable-Redis reservation boundary tests; no Main parity claim.
use super::*;
fn source() -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(include_str!(
        "../datasets/source-plugin-startup-wire.json"
    ))
    .unwrap()["receipt"]["frame"]
        .clone()
}
#[test]
fn source_startup_tuple_exact_ownership_redelivery_and_wrong_family() {
    let packet = source();
    let raw = serde_json::to_vec(&packet).unwrap();
    let receipt = Receipt::prepare(&raw).unwrap();
    let tuple = serde_json::to_vec_pretty(&serde_json::json!([packet, "", "celery"])).unwrap();
    let recovered = Receipt::from_stored(&tuple, receipt.delivery_tag).unwrap();
    assert_eq!(recovered.tuple_bytes, tuple);
    assert_eq!(recovered.queued_bytes, raw);
    assert!(Receipt::from_stored(&tuple, Uuid::new_v4()).is_err());
    let redelivered = Receipt::prepare(&recovered.restored_bytes).unwrap();
    assert_eq!(redelivered.message.id, receipt.message.id);
    assert_eq!(redelivered.message.eta, receipt.message.eta);
    let mut wrong = packet.clone();
    wrong["headers"]["task"] = serde_json::json!("apps.epg.tasks.refresh_epg_data");
    assert!(Receipt::prepare(&serde_json::to_vec(&wrong).unwrap()).is_err());
    wrong = packet.clone();
    wrong["headers"]["eta"] = serde_json::Value::Null;
    assert!(Receipt::prepare(&serde_json::to_vec(&wrong).unwrap()).is_err());
    wrong = packet.clone();
    wrong["headers"]["redelivered"] = serde_json::json!(true);
    assert!(Receipt::prepare(&serde_json::to_vec(&wrong).unwrap()).is_err());
    wrong = packet;
    wrong["headers"]["eta"] = serde_json::json!("not-a-date");
    assert!(Receipt::prepare(&serde_json::to_vec(&wrong).unwrap()).is_err());
}
#[tokio::test]
#[ignore = "requires explicitly owned disposable Redis DB8/9; grouped fixture acceptance remains required"]
async fn real_reservation_frees_fifo_retains_tuple_and_warm_restore_arrival_order() {
    use crate::transport::Jobs;
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let raw = std::env::var("JOB_REDIS_TEST_URL").unwrap();
    let client = redis::Client::open(raw).unwrap();
    let info = client.get_connection_info();
    assert!(
        matches!(&info.addr,redis::ConnectionAddr::Tcp(host,_)if host=="127.0.0.1"||host=="localhost")
    );
    assert_eq!(
        info.redis.password.as_deref(),
        Some("playback-fixture-only")
    );
    assert_eq!(info.redis.db, 8);
    let (host, port) = match &info.addr {
        redis::ConnectionAddr::Tcp(host, port) => (host.clone(), *port),
        _ => unreachable!(),
    };
    assert!(info.redis.username.is_none());
    let mut wire = client.get_multiplexed_async_connection().await.unwrap();
    let absent: i64 = redis::cmd("EXISTS")
        .arg("celery")
        .arg("unacked")
        .arg("unacked_index")
        .query_async(&mut wire)
        .await
        .unwrap();
    assert_eq!(
        absent, 0,
        "owned reservation keys must be absent before any mutation"
    );
    let broker = format!("redis://:playback-fixture-only@{host}:{port}/8");
    let backend = format!("redis://:playback-fixture-only@{host}:{port}/9");
    let jobs = Jobs::connect_defaults(&broker, &backend, Default::default())
        .await
        .unwrap();
    let first = serde_json::to_vec(&source()).unwrap();
    let mut second = source();
    let id = Uuid::new_v4().to_string();
    second["headers"]["id"] = serde_json::json!(id);
    second["headers"]["root_id"] = serde_json::json!(id);
    second["properties"]["correlation_id"] = serde_json::json!(id);
    second["properties"]["delivery_tag"] = serde_json::json!(Uuid::new_v4().to_string());
    let second = serde_json::to_vec(&second).unwrap();
    redis::cmd("LPUSH")
        .arg("celery")
        .arg(&first)
        .arg(&second)
        .query_async::<i64>(&mut wire)
        .await
        .unwrap();
    let owner: std::sync::Arc<dyn Send + Sync> = std::sync::Arc::new(());
    let limits = Limits::default();
    let a = jobs
        .peek_plugin_candidate(Some(&first), owner.clone(), limits)
        .await
        .unwrap()
        .unwrap();
    let a = jobs
        .reserve_plugin_candidate_settled(a, owner.clone(), limits)
        .await
        .unwrap();
    let next: Vec<u8> = redis::cmd("LINDEX")
        .arg("celery")
        .arg(-1)
        .query_async(&mut wire)
        .await
        .unwrap();
    assert_eq!(next, second);
    let tuple: Vec<u8> = redis::cmd("HGET")
        .arg("unacked")
        .arg(a.delivery_tag().to_string())
        .query_async(&mut wire)
        .await
        .unwrap();
    assert_eq!(tuple, a.tuple_bytes());
    assert!(
        jobs.eta_pool(limits).is_err(),
        "Default plugin capability must not grant generic Index ETA"
    );
    let b = jobs
        .peek_plugin_candidate(Some(&second), owner.clone(), limits)
        .await
        .unwrap()
        .unwrap();
    let b = jobs
        .reserve_plugin_candidate_settled(b, owner.clone(), limits)
        .await
        .unwrap();
    jobs.restore_plugin_eta(&a, owner.clone()).await.unwrap();
    jobs.restore_plugin_eta(&b, owner.clone()).await.unwrap();
    let redelivered: Vec<u8> = redis::cmd("RPOP")
        .arg("celery")
        .query_async(&mut wire)
        .await
        .unwrap();
    assert_eq!(redelivered, b.receipt.restored_bytes);
    let c = jobs
        .peek_plugin_candidate(Some(&a.receipt.restored_bytes), owner.clone(), limits)
        .await
        .unwrap()
        .unwrap();
    let c = jobs
        .reserve_plugin_candidate_settled(c, owner.clone(), limits)
        .await
        .unwrap();
    jobs.ack_plugin_eta(&c, owner.clone()).await.unwrap();
    assert!(jobs.ack_plugin_eta(&c, owner).await.is_err());
    let remaining: i64 = redis::cmd("HLEN")
        .arg("unacked")
        .query_async(&mut wire)
        .await
        .unwrap();
    assert_eq!(remaining, 0);
    // Receipts intentionally retain their shared ETA memory budgets until released.
    drop((a, b, c));
    jobs.shutdown(std::time::Duration::from_secs(3))
        .await
        .unwrap();
    let remaining: i64 = redis::cmd("EXISTS")
        .arg("celery")
        .arg("unacked")
        .arg("unacked_index")
        .query_async(&mut wire)
        .await
        .unwrap();
    assert_eq!(
        remaining, 0,
        "only own queue/reservation keys were used and retired"
    );
}
