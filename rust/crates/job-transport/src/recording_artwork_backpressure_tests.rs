//! Owned RESP reply peer exercising the production root-artwork PUBLISH call.
//! These transport controls are not application/Source functional acceptance.
use super::*;
use base64::Engine;
use std::sync::{atomic::AtomicUsize, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
struct State {
    mode: u8,
    calls: AtomicUsize,
    script_loaded: AtomicBool,
    writes: AtomicUsize,
    release: AtomicBool,
    frames: Mutex<Vec<Vec<u8>>>,
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
async fn start_peer(mode: u8) -> (Jobs, Arc<State>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let state = Arc::new(State {
        mode,
        calls: AtomicUsize::new(0),
        script_loaded: AtomicBool::new(false),
        writes: AtomicUsize::new(0),
        release: AtomicBool::new(false),
        frames: Mutex::new(Vec::new()),
    });
    let actual = state.clone();
    let task = tokio::spawn(async move {
        let mut handlers = Vec::new();
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let state = actual.clone();
            handlers.push(tokio::spawn(async move {
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
                        write.write_all(b"+PONG\r\n").await.unwrap();
                        continue;
                    }
                    if name == "SCRIPT" {
                        assert_eq!(args.len(), 3);
                        assert_eq!(args[1], b"LOAD");
                        assert_eq!(
                            args[2],
                            PUBLISH.as_bytes(),
                            "only the real production script may be loaded"
                        );
                        let script = redis::Script::new(PUBLISH);
                        let hash = script.get_hash();
                        let response = format!("${}\r\n{}\r\n", hash.len(), hash);
                        state.script_loaded.store(true, Ordering::Release);
                        write.write_all(response.as_bytes()).await.unwrap();
                        continue;
                    }
                    if name == "EVALSHA" && !state.script_loaded.load(Ordering::Acquire) {
                        write
                            .write_all(b"-NOSCRIPT owned test requires actual script bytes\r\n")
                            .await
                            .unwrap();
                        continue;
                    }
                    assert!(
                        matches!(name.as_str(), "EVAL" | "EVALSHA"),
                        "unexpected owned peer command {name}"
                    );
                    if name == "EVAL" {
                        assert_eq!(args[1], PUBLISH.as_bytes());
                    } else {
                        assert_eq!(args[1], redis::Script::new(PUBLISH).get_hash().as_bytes());
                    }
                    assert_eq!(args[2], b"2");
                    assert_eq!(args[3], b"celery");
                    assert_eq!(args[4], b"_kombu.binding.celery");
                    assert_eq!(args[6], b"1");
                    state.frames.lock().unwrap().push(args[5].clone());
                    let call = state.calls.fetch_add(1, Ordering::SeqCst) + 1;
                    if state.mode == 2 {
                        state.writes.fetch_add(1, Ordering::SeqCst);
                        break;
                    }
                    if state.mode == 3 {
                        while !state.release.load(Ordering::Acquire) {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    } else if call == 1 || state.mode == 1 {
                        write.write_all(b":-2\r\n").await.unwrap();
                        continue;
                    }
                    state.writes.fetch_add(1, Ordering::SeqCst);
                    let _ = write.write_all(b":1\r\n").await;
                }
            }));
        }
        for handler in handlers {
            handler.await.unwrap();
        }
    });
    let jobs = Jobs::connect_owned(
        &format!("redis://127.0.0.1:{port}/1"),
        &format!("redis://127.0.0.1:{port}/2"),
        Config {
            max_queued_jobs: 1,
            ..Config::default()
        },
        String::new(),
        Authority::LocalArtwork,
    )
    .await
    .unwrap();
    (jobs, state, task)
}
fn frame() -> Vec<u8> {
    let raw = crate::protocol::encode(
        &crate::protocol::Task::CreateBackup,
        Uuid::new_v4(),
        Uuid::parse_str("00000000-0000-3000-8000-000000000000").unwrap(),
        Uuid::new_v4(),
        "gen1@owned",
    )
    .unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    value["headers"]["task"] = serde_json::json!("apps.channels.tasks.prefetch_recording_artwork");
    value["headers"]["argsrepr"] = serde_json::json!("[7]");
    value["body"] = serde_json::json!(base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&serde_json::json!([[7],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).unwrap()));
    serde_json::to_vec(&value).unwrap()
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
#[tokio::test]
async fn actual_publish_capacity_retries_identical_frame_once_and_ambiguity_never_replays() {
    for mode in [0, 2] {
        let (jobs, state, peer) = start_peer(mode).await;
        let publisher = ArtworkRootPublisher::new(jobs.clone()).unwrap();
        let raw = frame();
        let stopped = AtomicBool::new(false);
        let result = publisher
            .publish_when_ready(&raw, Arc::new(()), &stopped)
            .await;
        if mode == 0 {
            assert_eq!(
                result.unwrap().task_id,
                recording_artwork_root::parse_signal(&raw).unwrap().task_id
            );
            assert_eq!(state.calls.load(Ordering::SeqCst), 2);
        } else {
            assert!(matches!(result, Err(Error::AmbiguousPublication(_))));
            assert_eq!(state.calls.load(Ordering::SeqCst), 1);
        }
        assert_eq!(state.writes.load(Ordering::SeqCst), 1);
        assert!(state
            .frames
            .lock()
            .unwrap()
            .iter()
            .all(|actual| actual == &raw));
        drop(publisher);
        drop(jobs);
        tokio::time::timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }
}
#[tokio::test]
async fn actual_publish_backpressure_stops_without_extra_write_or_releasing_physical_owner_early() {
    let (jobs, state, peer) = start_peer(1).await;
    let publisher = ArtworkRootPublisher::new(jobs.clone()).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let signal = stop.clone();
    let pending = tokio::spawn(async move {
        publisher
            .publish_when_ready(&frame(), Arc::new(()), &signal)
            .await
    });
    until(|| state.calls.load(Ordering::SeqCst) == 1).await;
    stop.store(true, Ordering::Release);
    assert!(matches!(pending.await.unwrap(), Err(Error::Closed)));
    assert_eq!(state.calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.writes.load(Ordering::SeqCst), 0);
    jobs.shutdown(Duration::from_secs(1)).await.unwrap();
    drop(jobs);
    tokio::time::timeout(Duration::from_secs(3), peer)
        .await
        .unwrap()
        .unwrap();

    struct Owner(Arc<AtomicBool>);
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let (jobs, state, peer) = start_peer(3).await;
    let publisher = ArtworkRootPublisher::new(jobs.clone()).unwrap();
    let retired = Arc::new(AtomicBool::new(false));
    let owner = Arc::new(Owner(retired.clone()));
    let pending = tokio::spawn(async move {
        publisher
            .publish_when_ready(&frame(), owner, &AtomicBool::new(false))
            .await
    });
    until(|| state.calls.load(Ordering::SeqCst) == 1).await;
    pending.abort();
    let _ = pending.await;
    assert!(
        !retired.load(Ordering::Acquire),
        "actual held command lease owns caller after waiter cancellation"
    );
    assert!(matches!(
        jobs.shutdown(Duration::from_millis(20)).await,
        Err(Error::DrainTimeout)
    ));
    assert!(!retired.load(Ordering::Acquire));
    state.release.store(true, Ordering::Release);
    until(|| retired.load(Ordering::Acquire)).await;
    jobs.shutdown(Duration::from_secs(1)).await.unwrap();
    assert_eq!(state.calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.writes.load(Ordering::SeqCst), 1);
    drop(jobs);
    tokio::time::timeout(Duration::from_secs(3), peer)
        .await
        .unwrap()
        .unwrap();
}
