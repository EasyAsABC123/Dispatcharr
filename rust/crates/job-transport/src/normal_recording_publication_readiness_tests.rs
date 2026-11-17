//! Actual production normal/named DVR publisher wiring over owned RESP peers.
//! Source business, real Redis and application acceptance remain separate.
//! Original Dispatcharr/Celery/redis-rs authors retain all protocol credit.
use crate::transport::{Authority, Config, Error, Jobs, PUBLISH};
use base64::Engine;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use uuid::Uuid;
struct State {
    mode: u8,
    pings: AtomicUsize,
    loaded: AtomicBool,
    writes: AtomicUsize,
    release: AtomicBool,
    frames: Mutex<Vec<Vec<u8>>>,
}
struct Peer {
    state: Arc<State>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}
async fn command<R: tokio::io::AsyncBufRead + Unpin>(r: &mut R) -> Option<Vec<Vec<u8>>> {
    let mut line = String::new();
    if r.read_line(&mut line).await.unwrap() == 0 {
        return None;
    }
    assert!(line.len() <= 64 && line.starts_with('*'));
    let count: usize = line[1..].trim().parse().unwrap();
    assert!((1..=16).contains(&count));
    let mut out = Vec::new();
    for _ in 0..count {
        line.clear();
        r.read_line(&mut line).await.unwrap();
        assert!(line.len() <= 64 && line.starts_with('$'));
        let n: usize = line[1..].trim().parse().unwrap();
        assert!(n <= 65536);
        let mut bytes = vec![0; n + 2];
        r.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes[n..], b"\r\n");
        bytes.truncate(n);
        out.push(bytes);
    }
    Some(out)
}
async fn start_peer(mode: u8, timeout: Duration) -> (Jobs, Peer) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let state = Arc::new(State {
        mode,
        pings: AtomicUsize::new(0),
        loaded: AtomicBool::new(false),
        writes: AtomicUsize::new(0),
        release: AtomicBool::new(false),
        frames: Mutex::new(Vec::new()),
    });
    let actual = state.clone();
    let (stop, mut stopping) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut handlers = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept(), if handlers.len() < 4 => {
                    let (socket, _) = accepted.unwrap();
                    let state = actual.clone();
                    handlers.spawn(async move {
                        let (read, mut write) = socket.into_split();
                        let mut reader = BufReader::new(read);
                        while let Some(args) = command(&mut reader).await {
                            let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
                            if matches!(name.as_str(), "AUTH" | "CLIENT" | "SELECT") {
                                write.write_all(b"+OK\r\n").await.unwrap();
                                continue;
                            }
                            if name == "PING" {
                                assert_eq!(args.len(), 1);
                                let ping = state.pings.fetch_add(1, Ordering::SeqCst) + 1;
                                if state.mode == 0 && ping == 1 {
                                    break; // settled dropped read-only readiness command
                                }
                                while state.mode == 2 && !state.release.load(Ordering::Acquire) {
                                    tokio::time::sleep(Duration::from_millis(1)).await;
                                }
                                write.write_all(b"+PONG\r\n").await.unwrap();
                                continue;
                            }
                            if name == "SCRIPT" {
                                assert_eq!(args.len(), 3);
                                assert_eq!(args[1], b"LOAD");
                                assert_eq!(args[2], PUBLISH.as_bytes());
                                let hash = redis::Script::new(PUBLISH).get_hash().to_owned();
                                state.loaded.store(true, Ordering::Release);
                                write.write_all(format!("${}\r\n{}\r\n", hash.len(), hash).as_bytes()).await.unwrap();
                                continue;
                            }
                            assert_eq!(name, "EVALSHA");
                            assert!(state.pings.load(Ordering::SeqCst) > 0, "publication requires readiness first");
                            assert_eq!(args[1], redis::Script::new(PUBLISH).get_hash().as_bytes());
                            assert_eq!(args[2], b"2");
                            assert_eq!(args[3], b"dvr");
                            assert_eq!(args[4], b"_kombu.binding.dvr");
                            assert_eq!(args[6], b"1");
                            if !state.loaded.load(Ordering::Acquire) {
                                write.write_all(b"-NOSCRIPT owned test requires actual Lua\r\n").await.unwrap();
                                continue;
                            }
                            state.frames.lock().unwrap().push(args[5].clone());
                            state.writes.fetch_add(1, Ordering::SeqCst);
                            if state.mode == 1 {
                                break; // LPUSH reply lost: never replay this mutation
                            }
                            let _ = write.write_all(b":1\r\n").await;
                        }
                    });
                }
                joined = handlers.join_next(), if !handlers.is_empty() => { joined.unwrap().unwrap(); }
                _ = stopping.changed() => { if *stopping.borrow() { break; } }
            }
        }
        handlers.abort_all();
        while handlers.join_next().await.is_some() {}
    });
    let jobs = Jobs::connect_owned(
        &format!("redis://127.0.0.1:{port}/1"),
        &format!("redis://127.0.0.1:{port}/2"),
        Config {
            max_queued_jobs: 1,
            command_timeout: timeout,
            ..Config::default()
        },
        String::new(),
        Authority::Dvr,
    )
    .await
    .unwrap();
    (jobs, Peer { state, stop, task })
}
fn frame(named: bool) -> Vec<u8> {
    let raw = crate::protocol::encode(
        &crate::protocol::Task::CreateBackup,
        Uuid::new_v4(),
        Uuid::new_v3(&Uuid::NAMESPACE_OID, b"owned-recording-readiness"),
        Uuid::new_v4(),
        "gen1@owned",
    )
    .unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let start = "2030-01-02 03:04:05+00:00";
    let end = "2030-01-02 03:14:05+00:00";
    value["headers"]["task"] = serde_json::json!("apps.channels.tasks.run_recording");
    value["headers"]["periodic_task_name"] = serde_json::json!("dvr-recording-7");
    value["headers"]["argsrepr"] = serde_json::json!(format!("[7, 11, '{start}', '{end}']"));
    value["properties"]["delivery_info"]["routing_key"] = serde_json::json!("dvr");
    value["body"] = serde_json::json!(base64::engine::general_purpose::STANDARD.encode(
        serde_json::to_vec(&serde_json::json!([[7,11,start,end],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).unwrap()
    ));
    if named {
        let key = format!("dvr-now-7-{}", Uuid::new_v4().simple());
        value["headers"]
            .as_object_mut()
            .unwrap()
            .remove("periodic_task_name");
        value["headers"]["id"] = serde_json::json!(key);
        value["headers"]["root_id"] = serde_json::json!(key);
        value["properties"]["correlation_id"] = serde_json::json!(key);
    }
    serde_json::to_vec(&value).unwrap()
}
async fn publish(
    jobs: &Jobs,
    raw: &[u8],
    named: bool,
    owner: Arc<dyn Send + Sync>,
) -> Result<(), Error> {
    if named {
        let message = crate::normal_named_recording::parse_head(raw).unwrap();
        let reply = jobs.publish_named_recording(&message, owner).await?;
        assert!(reply.confirms(&message));
    } else {
        let message = crate::clocked_recording_envelope::parse_normal_head(raw).unwrap();
        let reply = jobs.publish_normal_recording(&message, owner).await?;
        assert_eq!(reply.task_id, message.task_id());
        assert_eq!(reply.queue_length, 1);
    }
    Ok(())
}
async fn until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}
async fn finish(jobs: Jobs, peer: Peer) {
    jobs.shutdown(Duration::from_secs(1)).await.unwrap();
    drop(jobs);
    peer.stop.send_replace(true);
    tokio::time::timeout(Duration::from_secs(3), peer.task)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn due_and_named_producers_preflight_before_one_publication_and_never_replay_reply_loss() {
    for named in [false, true] {
        for mode in [0, 1] {
            let (jobs, peer) = start_peer(mode, Duration::from_secs(2)).await;
            let raw = frame(named);
            let result = publish(&jobs, &raw, named, Arc::new(())).await;
            if mode == 0 {
                result.unwrap();
                assert_eq!(peer.state.pings.load(Ordering::SeqCst), 2);
            } else {
                assert!(matches!(
                    (named, result),
                    (false, Err(Error::AmbiguousPublication(_)))
                        | (true, Err(Error::AmbiguousNamedRecordingPublication(7)))
                ));
                assert_eq!(peer.state.pings.load(Ordering::SeqCst), 1);
            }
            assert_eq!(peer.state.writes.load(Ordering::SeqCst), 1);
            assert_eq!(*peer.state.frames.lock().unwrap(), vec![raw]);
            finish(jobs, peer).await;
        }
    }
}
#[tokio::test]
async fn due_and_named_pending_readiness_keeps_physical_owner_until_single_completion() {
    struct Owner(Arc<AtomicBool>);
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    for named in [false, true] {
        let (jobs, peer) = start_peer(2, Duration::from_millis(75)).await;
        let raw = frame(named);
        let retired = Arc::new(AtomicBool::new(false));
        let result = publish(&jobs, &raw, named, Arc::new(Owner(retired.clone()))).await;
        assert!(result.is_err());
        assert_eq!(peer.state.pings.load(Ordering::SeqCst), 1);
        assert_eq!(peer.state.writes.load(Ordering::SeqCst), 0);
        assert!(!retired.load(Ordering::Acquire));
        assert!(matches!(
            jobs.shutdown(Duration::from_millis(20)).await,
            Err(Error::DrainTimeout)
        ));
        assert!(!retired.load(Ordering::Acquire));
        peer.state.release.store(true, Ordering::Release);
        until(|| retired.load(Ordering::Acquire)).await;
        assert_eq!(peer.state.writes.load(Ordering::SeqCst), 1);
        assert_eq!(*peer.state.frames.lock().unwrap(), vec![raw]);
        finish(jobs, peer).await;
    }
}
