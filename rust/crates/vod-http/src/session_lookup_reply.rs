//! Unregistered/unexecuted: child of session_restart_peer::admit_reply.
//! Original Dispatcharr and redis-rs authors retain application/client credit.
use super::{arguments, frame};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
struct ReadProxy {
    url: String,
    observed: Arc<AtomicUsize>,
    release: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Drop for ReadProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn read_proxy(id: &str, operation: &'static str, hold: bool) -> ReadProxy {
    let (url, _) = super::super::config();
    assert_eq!(url, "redis://:playback-fixture-only@127.0.0.1:6379/0");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let target = if operation == "EXISTS" {
        format!("vod_persistent_connection:{id}")
    } else {
        format!("vod_session_user:{id}")
    };
    let observed = Arc::new(AtomicUsize::new(0));
    let seen = observed.clone();
    let (release, ready) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut jobs = JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept(),if jobs.len()<4=>{
                    let(mut client,_)=accepted.unwrap();let target=target.clone();let seen=seen.clone();let mut ready=ready.clone();
                    jobs.spawn(async move {
                        let mut server=TcpStream::connect("127.0.0.1:6379").await?;
                        let(mut input,mut output)=(Vec::new(),Vec::new());
                        loop {
                            let command=frame(&mut client,&mut input).await?;
                            let args=arguments(&command).ok_or(std::io::ErrorKind::InvalidData)?;
                            let plain=args.first().is_some_and(|v|v==operation.as_bytes())&&args.get(1).is_some_and(|v|v==target.as_bytes());
                            let script=operation=="GET"&&args.get(3).is_some_and(|v|v==target.as_bytes())&&(
                                args.first().is_some_and(|v|v==b"EVALSHA")&&args.get(1).is_some_and(|v|v==redis::Script::new(super::super::super::session_user_lookup::READ_MAPPING).get_hash().as_bytes())
                                ||args.first().is_some_and(|v|v==b"EVAL")&&args.get(1).is_some_and(|v|v==super::super::super::session_user_lookup::READ_MAPPING.as_bytes()));
                            let matches=plain||script;
                            server.write_all(&command).await?;let reply=frame(&mut server,&mut output).await?;
                            if matches && !reply.starts_with(b"-NOSCRIPT") && seen.fetch_add(1,Ordering::SeqCst)==0 {
                                if !hold { return Err::<(),std::io::Error>(std::io::ErrorKind::ConnectionReset.into()); }
                                if ready.wait_for(|value|*value).await.is_err(){return Ok(());}
                            }
                            client.write_all(&reply).await?;
                        }
                    });
                },
                _=jobs.join_next(),if !jobs.is_empty()=>{}
            }
        }
    });
    ReadProxy {
        url: format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        observed,
        release,
        task,
    }
}
async fn wait(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
#[ignore = "actual first EXISTS reply lost: Source optional mapping catches errors"]
async fn lookup_read_failure_lost_exists_reply_matches_source_optional_none() {
    run_lost("EXISTS").await;
}
#[tokio::test]
#[ignore = "actual first GET reply lost: Source optional mapping catches errors"]
async fn lookup_read_failure_lost_get_reply_matches_source_optional_none() {
    run_lost("GET").await;
}
async fn run_lost(operation: &'static str) {
    let (base, _, _, _, id) =
        super::super::physical_fixture(&format!("lookup-lost-{operation}")).await;
    let proxy = read_proxy(&id, operation, false).await;
    let pool = super::super::super::SessionPool::connect(
        &proxy.url,
        base.inner.coordinator.clone(),
        1,
        1,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let first = pool.user_for_session(&id).await;
    let second = pool.user_for_session(&id).await;
    println!(
        "LOOKUP_READ_PROOF {}",
        serde_json::json!({"operation":operation,"first":format!("{first:?}"),"second":format!("{second:?}"),"target_reads":proxy.observed.load(Ordering::SeqCst)})
    );
    assert!(
        matches!(second, Ok(None)),
        "future read recovers without replaying a mutation"
    );
    assert!(
        matches!(first, Ok(None)),
        "Original optional mapping catches read exceptions; baseline native propagates503"
    );
    assert!(pool.cleanup_succeeded());
}
#[tokio::test]
#[ignore = "actual4s held optional lookup after caller abort retains service/request ownership"]
async fn lookup_read_failure_cancelled_observer_keeps_physical_service_budget() {
    let (base, _, _, _, id) = super::super::physical_fixture("lookup-held").await;
    let proxy = read_proxy(&id, "EXISTS", true).await;
    let pool = super::super::super::SessionPool::connect(
        &proxy.url,
        base.inner.coordinator.clone(),
        1,
        1,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let weak = Arc::downgrade(&pool.inner);
    let worker = pool.clone();
    let task = tokio::spawn(async move { worker.user_for_session(&id).await });
    wait(|| proxy.observed.load(Ordering::SeqCst) == 1).await;
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    let budget_held = pool.inner.requests.available_permits() == 0;
    let drained_early = tokio::time::timeout(Duration::from_millis(50), pool.drain())
        .await
        .is_ok();
    drop(pool);
    let service_held = weak.upgrade().is_some();
    println!(
        "LOOKUP_READ_PROOF {}",
        serde_json::json!({"held_reply":true,"budget_held":budget_held,"drained_early":drained_early,"service_held":service_held})
    );
    tokio::time::sleep(Duration::from_millis(1800)).await;
    proxy.release.send_replace(true);
    wait(|| weak.upgrade().is_none()).await;
    assert!(
        budget_held && service_held && !drained_early,
        "caller cancellation must not report physical lookup idle"
    );
}

#[tokio::test]
#[ignore = "actual Original optional lookup corpus13 IDs/errors; inactive ID is not a playback/auth claim"]
async fn lookup_read_failure_matches_source_supported_corpus() {
    use redis::AsyncCommands;
    let fixture: serde_json::Value =
        serde_json::from_slice(include_bytes!("../datasets/source-optional-user-218.json"))
            .unwrap();
    let cases = fixture["cases"].as_array().unwrap();
    let uid = cases.iter().find(|v| v["name"] == "valid").unwrap()["user_id"]
        .as_i64()
        .unwrap();
    let inactive = cases.iter().find(|v| v["name"] == "inactive").unwrap()["user_id"]
        .as_i64()
        .unwrap();
    let (pool, mut control, _, _, id) = super::super::physical_fixture("lookup-corpus").await;
    let metadata = super::super::super::key(&id);
    let mapping = format!("vod_session_user:{id}");
    let mut compared = 0;
    for case in cases {
        let name = case["name"].as_str().unwrap();
        if name == "client_none" {
            continue;
        }
        let _: usize = control
            .del(&[metadata.clone(), mapping.clone()])
            .await
            .unwrap();
        match name {
            "missing" => {}
            "valid" => {
                let _: () = control.set(&mapping, uid).await.unwrap();
            }
            "spaced_plus" => {
                let _: () = control.set(&mapping, format!("  +{uid}  ")).await.unwrap();
            }
            "underscore" => {
                let _: () = control.set(&mapping, format!("0_{uid}")).await.unwrap();
            }
            "unicode_decimal" => {
                let value: String = uid
                    .to_string()
                    .chars()
                    .map(|c| char::from_u32(u32::from('０') + c.to_digit(10).unwrap()).unwrap())
                    .collect();
                let _: () = control.set(&mapping, value).await.unwrap();
            }
            "invalid_integer" => {
                let _: () = control.set(&mapping, "not-an-integer").await.unwrap();
            }
            "overflow" => {
                let _: () = control.set(&mapping, "9".repeat(100)).await.unwrap();
            }
            "inactive" => {
                let _: () = control.set(&mapping, inactive).await.unwrap();
            }
            "invalid_utf8" => {
                let _: () = control.set(&mapping, vec![255u8]).await.unwrap();
            }
            "mapping_wrong_type" | "existing_metadata_skips_wrong_mapping" => {
                let _: usize = control.rpush(&mapping, "wrong-type").await.unwrap();
                if name == "existing_metadata_skips_wrong_mapping" {
                    let _: () = control
                        .set(&metadata, "owned-existing-marker")
                        .await
                        .unwrap();
                }
            }
            "exists_denied_actual_redis" | "get_denied_actual_redis" => {
                let username = format!("lookup-native-{}", uuid::Uuid::new_v4().simple());
                let denied = if name.starts_with("exists") {
                    "-exists"
                } else {
                    "-get"
                };
                let _: () = control.set(&mapping, uid).await.unwrap();
                let _: () = redis::cmd("ACL")
                    .arg("SETUSER")
                    .arg(&username)
                    .arg("on")
                    .arg(">playback-fixture-only")
                    .arg(format!("~{metadata}"))
                    .arg(format!("~{mapping}"))
                    .arg("+@connection")
                    .arg("+exists")
                    .arg("+get")
                    .arg("+strlen")
                    .arg("+eval")
                    .arg("+evalsha")
                    .arg("+script")
                    .arg(denied)
                    .query_async(&mut control)
                    .await
                    .unwrap();
                let limited = super::super::super::SessionPool::connect(
                    &format!("redis://{username}:playback-fixture-only@127.0.0.1:6379/0"),
                    pool.inner.coordinator.clone(),
                    1,
                    1,
                    Duration::from_millis(10),
                )
                .await
                .unwrap();
                let result = limited.user_for_session(&id).await;
                let _: usize = redis::cmd("ACL")
                    .arg("DELUSER")
                    .arg(&username)
                    .query_async(&mut control)
                    .await
                    .unwrap();
                assert_eq!(result.unwrap(), case["user_id"].as_i64(), "{name}");
                compared += 1;
                continue;
            }
            _ => panic!("unexpected Source case {name}"),
        }
        assert_eq!(
            pool.user_for_session(&id).await.unwrap(),
            case["user_id"].as_i64(),
            "{name}"
        );
        compared += 1;
    }
    assert_eq!(compared, 13);
    let _: usize = control.del(&[metadata, mapping]).await.unwrap();
    println!(
        "LOOKUP_READ_PROOF {}",
        serde_json::json!({"source_id_cases":compared,"unrepresented_source_initialization":"client_none","inactive_playback_accepted":false})
    );
    assert!(pool.cleanup_succeeded());
}
#[tokio::test]
#[ignore = "held optional lookup uses existing request budget and blocks shutdown drain"]
async fn lookup_read_failure_budget_capacity_and_shutdown_are_explicit() {
    let (base, _, _, _, id) = super::super::physical_fixture("lookup-capacity").await;
    let proxy = read_proxy(&id, "EXISTS", true).await;
    let pool = super::super::super::SessionPool::connect(
        &proxy.url,
        base.inner.coordinator.clone(),
        1,
        1,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let worker = pool.clone();
    let worker_id = id.clone();
    let task = tokio::spawn(async move { worker.user_for_session(&worker_id).await });
    wait(|| proxy.observed.load(Ordering::SeqCst) == 1).await;
    assert!(pool.user_for_session(&id).await.is_err());
    assert_eq!(proxy.observed.load(Ordering::SeqCst), 1);
    pool.stop_admission();
    assert!(pool.user_for_session(&id).await.is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), pool.drain())
            .await
            .is_err()
    );
    proxy.release.send_replace(true);
    assert_eq!(task.await.unwrap().unwrap(), None);
    tokio::time::timeout(Duration::from_secs(5), pool.drain())
        .await
        .unwrap();
    assert!(pool.cleanup_succeeded());
}
#[tokio::test]
#[ignore = "oversized otherwise valid user mapping is explicit bounded shape, never silent anonymous fallback"]
async fn lookup_read_failure_oversized_input_is_explicit_guard() {
    use redis::AsyncCommands;
    let (pool, mut control, _, _, id) = super::super::physical_fixture("lookup-overlong").await;
    let mapping = format!("vod_session_user:{id}");
    let _: () = control
        .set(&mapping, format!("{}2", " ".repeat(4096)))
        .await
        .unwrap();
    assert!(matches!(
        pool.user_for_session(&id).await,
        Err(dispatcharr_store::StoreError::StatusBody(503, _))
    ));
    assert_eq!(control.strlen::<_, usize>(&mapping).await.unwrap(), 4097);
    let _: usize = control.del(mapping).await.unwrap();
    assert!(pool.cleanup_succeeded());
}
