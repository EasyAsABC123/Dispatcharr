//! Independent real-Redis refresh ETA frame/count/byte/ownership/cancellation boundaries.
//! Original Dispatcharr/Celery/Kombu authors retain algorithm/application credit.
use crate::{
    eta::Limits,
    refresh_protocol::{RefreshCall, RefreshMessage},
    transport::{Config, Error, Jobs},
};
use chrono::Timelike;
use serde_json::Value;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Weak,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
use uuid::Uuid;
struct Proxy {
    base: String,
    hold: watch::Sender<bool>,
    blocked: watch::Receiver<bool>,
    reply_bytes: Arc<AtomicU64>,
    observation: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn proxy() -> Proxy {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let raw = std::env::var("JOB_REDIS_TEST_URL").unwrap();
    let info = redis::Client::open(raw)
        .unwrap()
        .get_connection_info()
        .clone();
    assert_eq!(
        info.redis.password.as_deref(),
        Some("playback-fixture-only")
    );
    let address = match info.addr {
        redis::ConnectionAddr::Tcp(host, port)
            if matches!(host.as_str(), "127.0.0.1" | "localhost") =>
        {
            format!("{host}:{port}")
        }
        _ => panic!("Owned loopback only"),
    };
    let mut bound = None;
    for port in 61200..61328 {
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)).await {
            bound = Some(listener);
            break;
        }
    }
    let listener = bound.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (hold, receiver) = watch::channel(false);
    let (blocked, observed) = watch::channel(false);
    let bytes = Arc::new(AtomicU64::new(0));
    let captured = bytes.clone();
    let observation = blocked.clone();
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {accepted=listener.accept()=>{let(client,_)=accepted.unwrap();let address=address.clone();let mut hold=receiver.clone();let blocked=blocked.clone();let bytes=captured.clone();connections.spawn(async move{let server=TcpStream::connect(address).await.unwrap();let(mut cr,mut cw)=client.into_split();let(mut sr,mut sw)=server.into_split();let incoming=async move{let mut buf=[0u8;16384];loop{let count=sr.read(&mut buf).await?;if count==0{return Ok::<(),std::io::Error>(())}bytes.fetch_add(count as u64,Ordering::SeqCst);if *hold.borrow(){blocked.send_replace(true);}
            if hold.wait_for(|paused|!*paused).await.is_err(){return Ok(())}cw.write_all(&buf[..count]).await?;}};let _=tokio::try_join!(async{tokio::io::copy(&mut cr,&mut sw).await.map(|_|())},incoming);});},_=connections.join_next(),if !connections.is_empty()=>{}}
        }
    });
    Proxy {
        base: format!("redis://:playback-fixture-only@127.0.0.1:{port}"),
        hold,
        blocked: observed,
        reply_bytes: bytes,
        observation,
        task,
    }
}
async fn connect_jobs(p: &Proxy) -> Jobs {
    Jobs::connect_fixture(
        &format!("{}/8", p.base),
        &format!("{}/9", p.base),
        &std::env::var("POSTGRES_DB").unwrap(),
        Config {
            operations: 1,
            command_timeout: Duration::from_millis(200),
            max_queued_jobs: 100,
        },
    )
    .await
    .unwrap()
}
async fn raw() -> redis::aio::MultiplexedConnection {
    let base = std::env::var("JOB_REDIS_TEST_URL").unwrap();
    let prefix = base.rsplit_once('/').unwrap().0;
    redis::Client::open(format!("{prefix}/8"))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
async fn reset(redis: &mut redis::aio::MultiplexedConnection) {
    let _: () = redis::cmd("FLUSHDB").query_async(redis).await.unwrap();
}
async fn publish(jobs: &Jobs) -> Uuid {
    let id = Uuid::new_v4();
    let parent = Uuid::new_v4();
    let call = RefreshCall::deferred(1, true, 1);
    jobs.publish_refresh(
        &RefreshMessage {
            task_id: id,
            source_id: 1,
            force: true,
            retry: 1,
            root: parent,
            parent: Some(parent),
            eta: Some({
                let now = chrono::Utc::now() + chrono::Duration::seconds(60);
                now.with_nanosecond(now.nanosecond() / 1000 * 1000).unwrap()
            }),
            call,
        },
        Arc::new(()),
    )
    .await
    .unwrap();
    id
}
async fn head(redis: &mut redis::aio::MultiplexedConnection) -> Vec<u8> {
    redis::cmd("LINDEX")
        .arg("celery")
        .arg(-1)
        .query_async(redis)
        .await
        .unwrap()
}
async fn foreign(redis: &mut redis::aio::MultiplexedConnection, n: usize) -> Uuid {
    let tag = Uuid::new_v4();
    let _: () = redis::cmd("HSET")
        .arg("unacked")
        .arg(tag.to_string())
        .arg(vec![b'x'; n])
        .query_async(redis)
        .await
        .unwrap();
    let _: () = redis::cmd("ZADD")
        .arg("unacked_index")
        .arg(1)
        .arg(tag.to_string())
        .query_async(redis)
        .await
        .unwrap();
    tag
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
struct Caller;
async fn held(jobs: &Jobs, weak: &Weak<Caller>) {
    assert_eq!(jobs.available_slots(), 0);
    assert!(weak.upgrade().is_some());
    assert_eq!(
        jobs.shutdown(Duration::from_millis(20)).await,
        Err(Error::DrainTimeout)
    );
}
#[tokio::test]
#[ignore = "Owned copied fixture and real RedisDB8/9 only"]
async fn eta_frames_global_count_bytes_and_unknown_values_are_bounded_before_pop_or_transfer() {
    let p = proxy().await;
    let mut redis = raw().await;
    for mode in [
        "queue-frame",
        "stored-frame",
        "global-count",
        "global-bytes",
    ] {
        reset(&mut redis).await;
        let jobs = connect_jobs(&p).await;
        let limits = match mode {
            "global-count" => Limits {
                max_pending: 2,
                max_pending_bytes: 1024 * 1024,
            },
            "global-bytes" => Limits {
                max_pending: 4,
                max_pending_bytes: 4096,
            },
            _ => Limits::default(),
        };
        if mode == "queue-frame" {
            let _: () = redis::cmd("LPUSH")
                .arg("celery")
                .arg(vec![b'x'; 65537])
                .query_async(&mut redis)
                .await
                .unwrap();
        } else {
            publish(&jobs).await;
        }
        let tag = match mode {
            "stored-frame" => Some(foreign(&mut redis, 131073).await),
            "global-count" => {
                foreign(&mut redis, 20).await;
                Some(foreign(&mut redis, 20).await)
            }
            "global-bytes" => Some(foreign(&mut redis, 4000).await),
            _ => None,
        };
        let before = head(&mut redis).await;
        let bytes = p.reply_bytes.load(Ordering::SeqCst);
        let result = jobs.reserve_refresh_eta(Arc::new(()), limits).await;
        assert!(
            matches!(result, Err(Error::Capacity | Error::Corrupt)),
            "{mode}"
        );
        assert_eq!(head(&mut redis).await, before, "{mode}");
        assert!(
            p.reply_bytes.load(Ordering::SeqCst) - bytes < 8192,
            "Oversized data crossed native reply transport: {mode}"
        );
        assert_eq!(jobs.available_slots(), 1);
        assert_eq!(jobs.eta_available_slots(), Some(limits.max_pending));
        if let Some(tag) = tag {
            let size: usize = redis::cmd("HSTRLEN")
                .arg("unacked")
                .arg(tag.to_string())
                .query_async(&mut redis)
                .await
                .unwrap();
            assert_eq!(
                size,
                match mode {
                    "stored-frame" => 131073,
                    "global-count" => 20,
                    _ => 4000,
                }
            );
        }
        jobs.shutdown(Duration::from_secs(1)).await.unwrap();
    }
    reset(&mut redis).await;
}
#[tokio::test]
#[ignore = "Owned copied fixture and real RedisDB8/9 only"]
async fn eta_candidates_clones_owner_replacement_and_empty_head_race_keep_admission_and_queue() {
    let p = proxy().await;
    let mut redis = raw().await;
    reset(&mut redis).await;
    let jobs = connect_jobs(&p).await;
    let limits = Limits {
        max_pending: 2,
        max_pending_bytes: 1024 * 1024,
    };
    let id = publish(&jobs).await;
    let one = jobs
        .peek_refresh_candidate(Arc::new(()), limits)
        .await
        .unwrap()
        .unwrap();
    let clone = one.clone();
    let two = jobs
        .peek_refresh_candidate(Arc::new(()), limits)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(jobs.eta_available_slots(), Some(0));
    assert!(matches!(
        jobs.peek_refresh_candidate(Arc::new(()), limits).await,
        Err(Error::Capacity)
    ));
    drop(one);
    assert_eq!(jobs.eta_available_slots(), Some(0));
    let other = connect_jobs(&p).await;
    assert!(matches!(
        other
            .reserve_refresh_candidate(two.clone(), Arc::new(()), limits)
            .await,
        Err(Error::Configuration)
    ));
    other.shutdown(Duration::from_secs(1)).await.unwrap();
    let reservation = jobs
        .reserve_refresh_candidate(two, Arc::new(()), limits)
        .await
        .unwrap();
    assert_eq!(reservation.message().task_id, id);
    assert_eq!(jobs.eta_available_slots(), Some(0));
    assert!(matches!(
        jobs.reserve_refresh_candidate(clone, Arc::new(()), limits)
            .await,
        Err(Error::ClaimRace)
    ));
    assert_eq!(jobs.eta_available_slots(), Some(1));
    let tag = reservation.delivery_tag().to_string();
    let _: () = redis::cmd("HSET")
        .arg("unacked")
        .arg(&tag)
        .arg(vec![b'z'; 1024 * 1024])
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(
        jobs.ack_refresh_eta(&reservation, Arc::new(())).await,
        Err(Error::LostReservation)
    );
    assert_eq!(
        jobs.restore_refresh_eta(&reservation, Arc::new(())).await,
        Err(Error::LostReservation)
    );
    let size: usize = redis::cmd("HSTRLEN")
        .arg("unacked")
        .arg(&tag)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(size, 1024 * 1024);
    let _: () = redis::cmd("HSET")
        .arg("unacked")
        .arg(&tag)
        .arg(reservation.tuple_bytes())
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(
        jobs.shutdown(Duration::from_millis(20)).await,
        Err(Error::DrainTimeout)
    );
    jobs.restore_refresh_eta(&reservation, Arc::new(()))
        .await
        .unwrap();
    let restored: Value = serde_json::from_slice(&head(&mut redis).await).unwrap();
    assert_eq!(restored["headers"]["redelivered"], true);
    assert_eq!(restored["properties"]["delivery_info"]["redelivered"], true);
    drop(reservation);
    jobs.shutdown(Duration::from_secs(1)).await.unwrap();
    reset(&mut redis).await;
}
#[tokio::test]
#[ignore = "Owned copied fixture and real RedisDB8/9 only"]
async fn eta_reserve_ack_restore_real_replies_retain_callers_after_cancel_or_ambiguity() {
    let mut p = proxy().await;
    let mut redis = raw().await;
    for phase in ["reserve-cancel", "ack-timeout", "restore-cancel"] {
        reset(&mut redis).await;
        let jobs = connect_jobs(&p).await;
        let limits = Limits {
            max_pending: 1,
            max_pending_bytes: 1024 * 1024,
        };
        publish(&jobs).await;
        let candidate = jobs
            .peek_refresh_candidate(Arc::new(()), limits)
            .await
            .unwrap()
            .unwrap();
        let reservation = if phase == "reserve-cancel" {
            None
        } else {
            Some(
                jobs.reserve_refresh_candidate(candidate.clone(), Arc::new(()), limits)
                    .await
                    .unwrap(),
            )
        };
        drop(candidate.clone());
        let caller = Arc::new(Caller);
        let weak = Arc::downgrade(&caller);
        p.observation.send_replace(false);
        p.hold.send_replace(true);
        let active = jobs.clone();
        if phase == "reserve-cancel" {
            let task = tokio::spawn(async move {
                active
                    .reserve_refresh_candidate(candidate, caller, limits)
                    .await
            });
            p.blocked.wait_for(|v| *v).await.unwrap();
            task.abort();
            let Err(error) = task.await else {
                panic!("Caller task unexpectedly completed")
            };
            assert!(error.is_cancelled());
            held(&jobs, &weak).await;
            p.hold.send_replace(false);
            until(|| jobs.available_slots() == 1 && weak.upgrade().is_none()).await;
            assert_eq!(jobs.eta_available_slots(), Some(1));
        } else if phase == "ack-timeout" {
            drop(candidate);
            let receipt = reservation.unwrap();
            let result = jobs.ack_refresh_eta(&receipt, caller).await;
            assert!(matches!(result, Err(Error::AmbiguousAck(_))));
            drop(receipt);
            held(&jobs, &weak).await;
            p.hold.send_replace(false);
            until(|| jobs.available_slots() == 1 && weak.upgrade().is_none()).await;
            assert_eq!(jobs.eta_available_slots(), Some(1));
        } else {
            drop(candidate);
            let receipt = reservation.unwrap();
            let task =
                tokio::spawn(async move { active.restore_refresh_eta(&receipt, caller).await });
            p.blocked.wait_for(|v| *v).await.unwrap();
            task.abort();
            let Err(error) = task.await else {
                panic!("Caller task unexpectedly completed")
            };
            assert!(error.is_cancelled());
            held(&jobs, &weak).await;
            p.hold.send_replace(false);
            until(|| jobs.available_slots() == 1 && weak.upgrade().is_none()).await;
            assert_eq!(jobs.eta_available_slots(), Some(1));
        }
        jobs.shutdown(Duration::from_secs(1)).await.unwrap();
    }
    reset(&mut redis).await;
}
