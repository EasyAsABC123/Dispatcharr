//! Physical production DVR candidate reads over owned RESP peers.
//! Source behavior, real Redis and natural CUT acceptance remain separate.
//! Original Dispatcharr/Celery/redis-rs authors retain all protocol credit.
use crate::transport::{Authority, Config, Error, Jobs, PEEK_CREATE};
use base64::Engine;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
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
    reads: AtomicUsize,
    loaded: AtomicBool,
    release: AtomicBool,
    raw: Vec<u8>,
}
struct Peer {
    port: u16,
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
async fn start_peer(mode: u8, named: bool, timeout: Duration) -> (Jobs, Peer) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let state = Arc::new(State {
        mode,
        pings: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
        loaded: AtomicBool::new(false),
        release: AtomicBool::new(false),
        raw: frame(named),
    });
    let actual = state.clone();
    let (stop, mut stopping) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut handlers = JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept(),if handlers.len()<4=>{
                    let(socket,_)=accepted.unwrap();let state=actual.clone();
                    handlers.spawn(async move {
                        let(read,mut write)=socket.into_split();let mut reader=BufReader::new(read);
                        while let Some(args)=command(&mut reader).await {
                            let name=String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
                            if matches!(name.as_str(),"AUTH"|"CLIENT"|"SELECT") {write.write_all(b"+OK\r\n").await.unwrap();continue;}
                            if name=="PING" {
                                assert_eq!(args.len(),1);let n=state.pings.fetch_add(1,Ordering::SeqCst)+1;
                                if state.mode==1&&n==1 {break;}
                                while state.mode==3&&!state.release.load(Ordering::Acquire){tokio::time::sleep(Duration::from_millis(1)).await;}
                                write.write_all(b"+PONG\r\n").await.unwrap();continue;
                            }
                            if name=="SCRIPT" {
                                assert_eq!(args.len(),3);assert_eq!(args[1],b"LOAD");assert_eq!(args[2],PEEK_CREATE.as_bytes());
                                state.loaded.store(true,Ordering::Release);let hash=redis::Script::new(PEEK_CREATE).get_hash().to_owned();
                                write.write_all(format!("${}\r\n{}\r\n",hash.len(),hash).as_bytes()).await.unwrap();continue;
                            }
                            // Any RPOP, publication or other mutation is a test failure.
                            assert_eq!(name,"EVALSHA");assert_eq!(args.len(),4);assert_eq!(args[1],redis::Script::new(PEEK_CREATE).get_hash().as_bytes());assert_eq!(args[2],b"1");assert_eq!(args[3],b"dvr");
                            if state.mode!=0 {assert!(state.pings.load(Ordering::SeqCst)>0);}
                            if !state.loaded.load(Ordering::Acquire){write.write_all(b"-NOSCRIPT owned read requires actual Lua\r\n").await.unwrap();continue;}
                            state.reads.fetch_add(1,Ordering::SeqCst);
                            if matches!(state.mode,0|2){break;}
                            let reply=format!("*2\r\n:1\r\n${}\r\n",state.raw.len());write.write_all(reply.as_bytes()).await.unwrap();write.write_all(&state.raw).await.unwrap();write.write_all(b"\r\n").await.unwrap();
                        }
                    });
                }
                joined=handlers.join_next(),if !handlers.is_empty()=>{joined.unwrap().unwrap();}
                _=stopping.changed()=>{if *stopping.borrow(){break;}}
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
    (
        jobs,
        Peer {
            port,
            state,
            stop,
            task,
        },
    )
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
async fn read(
    jobs: &Jobs,
    raw: &[u8],
    named: bool,
    owner: Arc<dyn Send + Sync>,
) -> Result<(), Error> {
    if named {
        let candidate = jobs
            .peek_named_recording_dvr(Some(raw), owner)
            .await?
            .unwrap();
        assert_eq!(candidate.message.raw_frame(), raw);
    } else {
        let candidate = jobs
            .peek_normal_recording_dvr(Some(raw), owner)
            .await?
            .unwrap();
        assert_eq!(candidate.message.raw_frame(), raw);
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
async fn ordinary_unprotected_dvr_read_reports_settled_disconnect_class() {
    let (jobs, peer) = start_peer(0, false, Duration::from_secs(2)).await;
    assert!(matches!(
        jobs.peek_recording_dvr(Arc::new(())).await,
        Err(Error::Backend)
    ));
    assert_eq!(peer.state.pings.load(Ordering::SeqCst), 0);
    assert_eq!(peer.state.reads.load(Ordering::SeqCst), 1);
    finish(jobs, peer).await;
}
#[tokio::test]
async fn normal_and_named_candidates_preflight_dropped_ping_once_then_one_unchanged_peek() {
    for named in [false, true] {
        let (jobs, peer) = start_peer(1, named, Duration::from_secs(2)).await;
        read(&jobs, &peer.state.raw, named, Arc::new(()))
            .await
            .unwrap();
        assert_eq!(peer.state.pings.load(Ordering::SeqCst), 2);
        assert_eq!(peer.state.reads.load(Ordering::SeqCst), 1);
        finish(jobs, peer).await;
    }
}
#[tokio::test]
async fn normal_and_named_candidate_executed_read_reply_loss_is_not_replayed() {
    for named in [false, true] {
        let (jobs, peer) = start_peer(2, named, Duration::from_secs(2)).await;
        assert!(matches!(
            read(&jobs, &peer.state.raw, named, Arc::new(())).await,
            Err(Error::Backend)
        ));
        assert_eq!(peer.state.pings.load(Ordering::SeqCst), 1);
        assert_eq!(peer.state.reads.load(Ordering::SeqCst), 1);
        finish(jobs, peer).await;
    }
}
#[tokio::test]
async fn pending_candidate_readiness_retains_owner_and_operation_through_physical_completion() {
    struct Owner(Arc<AtomicBool>);
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    for named in [false, true] {
        let (jobs, peer) = start_peer(3, named, Duration::from_millis(75)).await;
        let retired = Arc::new(AtomicBool::new(false));
        assert!(read(
            &jobs,
            &peer.state.raw,
            named,
            Arc::new(Owner(retired.clone()))
        )
        .await
        .is_err());
        assert_eq!(peer.state.pings.load(Ordering::SeqCst), 1);
        assert_eq!(peer.state.reads.load(Ordering::SeqCst), 0);
        assert!(!retired.load(Ordering::Acquire));
        assert!(matches!(
            jobs.shutdown(Duration::from_millis(20)).await,
            Err(Error::DrainTimeout)
        ));
        assert!(!retired.load(Ordering::Acquire));
        peer.state.release.store(true, Ordering::Release);
        until(|| retired.load(Ordering::Acquire)).await;
        assert_eq!(peer.state.reads.load(Ordering::SeqCst), 1);
        finish(jobs, peer).await;
    }
}

// Exercise the actual publisher adoption entry, including its independent Jobs
// connection and detached owner. SQL is deliberately closed: a successful read
// must reach the real SQL acquisition boundary, never fabricate adoption proof.
async fn publisher(peer: &Peer) -> crate::normal_recording_publication::NormalRecordingPublisher {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://fixture:fixture@127.0.0.1:1/fixture")
        .unwrap();
    pool.close().await;
    crate::normal_recording_publication::NormalRecordingPublisher::prepare(
        pool,
        &format!("redis://127.0.0.1:{}/1", peer.port),
        &format!("redis://127.0.0.1:{}/2", peer.port),
    )
    .await
    .unwrap()
    .0
}
#[tokio::test]
async fn actual_publisher_adoption_preflights_and_reaches_sql_without_mutation() {
    for named in [false, true] {
        let (jobs, peer) = start_peer(1, named, Duration::from_secs(2)).await;
        let publisher = publisher(&peer).await;
        let result = publisher
            .adopt_existing(Arc::from(peer.state.raw.clone()), Arc::new(()))
            .await;
        assert!(matches!(
            result,
            Err(crate::normal_recording_publication::Error::Database {
                primary: Some(_),
                ..
            })
        ));
        assert_eq!(peer.state.pings.load(Ordering::SeqCst), 2);
        assert_eq!(peer.state.reads.load(Ordering::SeqCst), 1);
        publisher.shutdown(Duration::from_secs(1)).await.unwrap();
        finish(jobs, peer).await;
    }
}
#[tokio::test]
async fn actual_publisher_adoption_executed_read_loss_is_not_replayed() {
    for named in [false, true] {
        let (jobs, peer) = start_peer(2, named, Duration::from_secs(2)).await;
        let publisher = publisher(&peer).await;
        let result = publisher
            .adopt_existing(Arc::from(peer.state.raw.clone()), Arc::new(()))
            .await;
        assert!(matches!(
            result,
            Err(crate::normal_recording_publication::Error::Transport(
                Error::Backend
            ))
        ));
        assert_eq!(peer.state.pings.load(Ordering::SeqCst), 1);
        assert_eq!(peer.state.reads.load(Ordering::SeqCst), 1);
        publisher.shutdown(Duration::from_secs(1)).await.unwrap();
        finish(jobs, peer).await;
    }
}
#[tokio::test]
async fn actual_publisher_cancelled_adoption_retains_owner_until_physical_read_finishes() {
    struct Owner(Arc<AtomicBool>);
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    for named in [false, true] {
        let (jobs, peer) = start_peer(3, named, Duration::from_secs(2)).await;
        let publisher = publisher(&peer).await;
        let retired = Arc::new(AtomicBool::new(false));
        let raw: Arc<[u8]> = Arc::from(peer.state.raw.clone());
        let caller = Arc::new(Owner(retired.clone()));
        let accepted = publisher.clone();
        let task = tokio::spawn(async move { accepted.adopt_existing(raw, caller).await });
        until(|| peer.state.pings.load(Ordering::SeqCst) == 1).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(peer.state.reads.load(Ordering::SeqCst), 0);
        assert!(!retired.load(Ordering::Acquire));
        assert!(matches!(
            publisher.shutdown(Duration::from_millis(20)).await,
            Err(crate::normal_recording_publication::Error::DrainTimeout)
        ));
        assert!(!retired.load(Ordering::Acquire));
        peer.state.release.store(true, Ordering::Release);
        until(|| retired.load(Ordering::Acquire)).await;
        assert_eq!(peer.state.reads.load(Ordering::SeqCst), 1);
        publisher.shutdown(Duration::from_secs(1)).await.unwrap();
        finish(jobs, peer).await;
    }
}
#[tokio::test]
async fn direct_publisher_peek_preflights_and_preserves_literal_frame() {
    for named in [false, true] {
        let (jobs, peer) = start_peer(1, named, Duration::from_secs(2)).await;
        let publisher = publisher(&peer).await;
        let raw = publisher
            .peek_existing(Arc::new(()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(raw.as_ref(), peer.state.raw.as_slice());
        assert_eq!(peer.state.pings.load(Ordering::SeqCst), 2);
        assert_eq!(peer.state.reads.load(Ordering::SeqCst), 1);
        publisher.shutdown(Duration::from_secs(1)).await.unwrap();
        finish(jobs, peer).await;
    }
}
