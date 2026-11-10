//! One-pass index-only recovery, derived from pinned Kombu/Source119 ordering.
//! Draft: unregistered until its physical Redis/resource gates pass.
//! Original Dispatcharr/Celery/Kombu authors retain full algorithm credit.
use crate::{
    eta,
    transport::{Error, Jobs},
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub visibility_timeout: Duration,
    pub mutex_expiration: Duration,
    pub batch: usize,
    pub limits: eta::Limits,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            visibility_timeout: Duration::from_secs(3600),
            mutex_expiration: Duration::from_secs(300),
            batch: 10,
            limits: eta::Limits::default(),
        }
    }
}
impl Config {
    fn validate(self) -> Result<(), Error> {
        if self.batch == 0
            || self.batch > 10
            || self.visibility_timeout.is_zero()
            || self.visibility_timeout > Duration::from_secs(86400 * 30)
            || self.mutex_expiration.is_zero()
            || self.mutex_expiration > Duration::from_secs(3600)
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    MutexHeld,
    Scanned {
        selected: usize,
        restored: usize,
        foreign: usize,
        lost: usize,
    },
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
struct Inner {
    jobs: Jobs,
    config: Config,
    slot: Arc<Semaphore>,
    closed: AtomicBool,
    last_error: Mutex<Option<Error>>,
}
#[derive(Clone)]
pub struct VisibilityRecovery {
    inner: Arc<Inner>,
}
impl VisibilityRecovery {
    pub fn new(jobs: Jobs, config: Config) -> Result<Self, Error> {
        config.validate()?;
        jobs.eta_pool(config.limits)?;
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                config,
                slot: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
                last_error: Mutex::new(None),
            }),
        })
    }
    /// Supervises exactly one bounded scan. Caller cancellation never drops the
    /// mutex/receipt/physical command owner. This does not start a polling loop.
    pub async fn run_once(&self, caller: Arc<dyn Send + Sync>) -> Result<Outcome, Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let slot = self
            .inner
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let owner = Arc::new(Owner {
            _slot: slot,
            _caller: caller,
        });
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let result = recover(inner.clone(), owner.clone()).await;
            *inner.last_error.lock().unwrap() = result.err();
            drop(owner);
            result
        })
        .await
        .map_err(|_| Error::Corrupt)?
    }
    pub fn last_error(&self) -> Option<Error> {
        *self.inner.last_error.lock().unwrap()
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.inner.closed.store(true, Ordering::Release);
        let _owned = tokio::time::timeout(timeout, self.inner.slot.clone().acquire_owned())
            .await
            .map_err(|_| Error::DrainTimeout)?
            .map_err(|_| Error::Closed)?;
        Ok(())
    }
}
async fn recover(inner: Arc<Inner>, owner: Arc<Owner>) -> Result<Outcome, Error> {
    let token = Uuid::new_v4().simple().to_string();
    let mutex = vec!["unacked_mutex".to_owned()];
    let expire = inner.config.mutex_expiration.as_millis().to_string();
    let acquire = inner
        .jobs
        .broker_settled::<i64>(
            ACQUIRE,
            &mutex,
            &[token.as_bytes(), expire.as_bytes()],
            owner.clone(),
        )
        .await;
    let locked = match acquire {
        Ok(1) => true,
        Ok(0) => false,
        Ok(_) => return Err(Error::Corrupt),
        Err(Error::Backend) => {
            // The physical acquisition has completed. Reconcile exact token;
            // no restore is permitted on an ambiguous lock acquisition.
            match inner
                .jobs
                .broker_settled::<i64>(OWNS, &mutex, &[token.as_bytes()], owner.clone())
                .await?
            {
                1 => true,
                0 => false,
                _ => return Err(Error::Corrupt),
            }
        }
        Err(e) => return Err(e),
    };
    if !locked {
        return Ok(Outcome::MutexHeld);
    }
    let result = recover_locked(&inner, owner.clone(), &token).await;
    // Never release ahead of a detached destructive restore. broker_settled
    // waits through the actual physical command, even on a logical timeout.
    let release = inner
        .jobs
        .broker_settled::<i64>(RELEASE, &mutex, &[token.as_bytes()], owner)
        .await;
    match (result, release) {
        (Err(e), _) => Err(e),
        (_, Err(e)) => Err(e),
        (Ok(value), Ok(0 | 1)) => Ok(value),
        _ => Err(Error::Corrupt),
    }
}
async fn recover_locked(inner: &Inner, owner: Arc<Owner>, token: &str) -> Result<Outcome, Error> {
    let ceiling = chrono::Utc::now().timestamp_micros() as f64 / 1_000_000.0
        - inner.config.visibility_timeout.as_secs_f64();
    let ceiling = format!("{ceiling:.6}");
    let count = inner.config.batch.to_string();
    let keys = vec!["unacked_index".to_owned()];
    let (code, items): (i64, Vec<(String, String)>) = inner
        .jobs
        .broker_settled(
            SELECT,
            &keys,
            &[ceiling.as_bytes(), count.as_bytes()],
            owner.clone(),
        )
        .await?;
    if code != 1 {
        return Err(Error::Corrupt);
    }
    let mut restored = 0;
    let mut foreign = 0;
    let mut lost = 0;
    for (tag, score) in &items {
        let tag = Uuid::parse_str(tag).map_err(|_| Error::Corrupt)?;
        let receipt = match inner
            .jobs
            .read_index_eta(tag, owner.clone(), inner.config.limits)
            .await
        {
            Ok(Some(receipt)) => receipt,
            Ok(None) => {
                lost += 1;
                continue;
            }
            Err(Error::Corrupt | Error::UnsupportedTask) => {
                foreign += 1;
                continue;
            }
            Err(error) => return Err(error),
        };

        // Transport helper includes exact mutex token, selected score and tuple
        // fences. It holds receipt/frame and caller through actual Redis reply.
        let value = inner
            .jobs
            .restore_visible_owned(&receipt, score, &ceiling, token, owner.clone())
            .await?;
        match value {
            true => restored += 1,
            false => lost += 1,
        }
    }
    Ok(Outcome::Scanned {
        selected: items.len(),
        restored,
        foreign,
        lost,
    })
}
const ACQUIRE: &str = r#"local n=redis.call('SET',KEYS[1],ARGV[1],'NX','PX',ARGV[2]);if n then return 1 end;return 0"#;
const OWNS: &str = r#"local t=redis.call('TYPE',KEYS[1]).ok;if t=='none'then return 0 end;if t~='string'then return -1 end;if redis.call('STRLEN',KEYS[1])~=#ARGV[1]then return 0 end;if redis.call('GET',KEYS[1])==ARGV[1]then return 1 end;return 0"#;
const RELEASE: &str = r#"if redis.call('TYPE',KEYS[1]).ok=='none'then return 0 end;if redis.call('TYPE',KEYS[1]).ok~='string'then return -1 end;if redis.call('STRLEN',KEYS[1])~=#ARGV[1]then return 0 end;if redis.call('GET',KEYS[1])==ARGV[1]then redis.call('DEL',KEYS[1]);return 1 end;return 0"#;
const SELECT: &str = r#"local t=redis.call('TYPE',KEYS[1]).ok;if t=='none'then return{1,{}}end;if t~='zset'then return{-1,{}}end;local r=redis.call('ZREVRANGEBYSCORE',KEYS[1],ARGV[1],0,'WITHSCORES','LIMIT',0,ARGV[2]);local out={};for i=1,#r,2 do if #r[i]~=36 or #r[i+1]>64 then return{-1,{}}end;out[#out+1]={r[i],r[i+1]};end;return{1,out}"#;
pub(crate) const RESTORE_VISIBLE: &str = r#"
local m=redis.call('TYPE',KEYS[4]).ok
if m~='string'then return -5 end
if redis.call('STRLEN',KEYS[4])~=#ARGV[6]then return -5 end
if redis.call('GET',KEYS[4])~=ARGV[6]then return -5 end
local h=redis.call('TYPE',KEYS[2]).ok;local z=redis.call('TYPE',KEYS[3]).ok
if h=='none'and z=='none'then return 0 end
if h~='hash'or z~='zset'then return -1 end
local score=redis.call('ZSCORE',KEYS[3],ARGV[1]);if not score then return 0 end
if tonumber(score)~=tonumber(ARGV[4])or tonumber(score)>tonumber(ARGV[5])then return 0 end
if #ARGV[1]~=36 or #ARGV[2]>131072 or #ARGV[3]>65536 then return -1 end
if redis.call('HSTRLEN',KEYS[2],ARGV[1])~=#ARGV[2]then return 0 end
if redis.call('HGET',KEYS[2],ARGV[1])~=ARGV[2]then return 0 end
local q=redis.call('TYPE',KEYS[1]).ok;if q~='none'and q~='list'then return -1 end
redis.call('ZREM',KEYS[3],ARGV[1]);redis.call('HDEL',KEYS[2],ARGV[1]);redis.call('RPUSH',KEYS[1],ARGV[3]);return 1
"#;
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_defaults_are_bounded_component_not_worker_sweep() {
        let config = Config::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.visibility_timeout, Duration::from_secs(3600));
        assert_eq!(config.mutex_expiration, Duration::from_secs(300));
        let mut bad = config;
        bad.batch = usize::MAX;
        assert_eq!(bad.validate(), Err(Error::Configuration));
    }
    async fn fixture() -> (Jobs, redis::aio::MultiplexedConnection) {
        assert_eq!(
            std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
        );
        let raw = std::env::var("JOB_REDIS_TEST_URL").unwrap();
        let client = redis::Client::open(raw).unwrap();
        let info = client.get_connection_info();
        assert_eq!(
            info.redis.password.as_deref(),
            Some("playback-fixture-only")
        );
        assert!(
            matches!(&info.addr,redis::ConnectionAddr::Tcp(h,6379)if h=="127.0.0.1"||h=="localhost")
        );
        let base = "redis://:playback-fixture-only@127.0.0.1:6379";
        let jobs = Jobs::connect_fixture(
            &format!("{base}/8"),
            &format!("{base}/9"),
            &std::env::var("POSTGRES_DB").unwrap(),
            Default::default(),
        )
        .await
        .unwrap();
        let mut raw = redis::Client::open(format!("{base}/8"))
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let _: () = redis::cmd("FLUSHDB").query_async(&mut raw).await.unwrap();
        (jobs, raw)
    }
    async fn reserve(jobs: &Jobs, id: i64) -> eta::Reservation {
        jobs.publish_index(
            &crate::index_protocol::IndexTask::Initial { source_id: id },
            Uuid::new_v4(),
            Arc::new(()),
        )
        .await
        .unwrap();
        let candidate = jobs
            .peek_index_candidate(Arc::new(()), eta::Limits::default())
            .await
            .unwrap()
            .unwrap();
        jobs.reserve_index_candidate(candidate, Arc::new(()), eta::Limits::default())
            .await
            .unwrap()
    }
    async fn age(
        raw: &mut redis::aio::MultiplexedConnection,
        receipt: &eta::Reservation,
        score: f64,
    ) {
        let _: i64 = redis::cmd("ZADD")
            .arg("unacked_index")
            .arg(score)
            .arg(receipt.delivery_tag().to_string())
            .query_async(raw)
            .await
            .unwrap();
    }
    async fn delivery(raw: &mut redis::aio::MultiplexedConnection) -> Vec<Uuid> {
        let rows: Vec<Vec<u8>> = redis::cmd("LRANGE")
            .arg("celery")
            .arg(0)
            .arg(-1)
            .query_async(raw)
            .await
            .unwrap();
        rows.into_iter()
            .rev()
            .map(|raw| {
                let value: serde_json::Value = serde_json::from_slice(&raw).unwrap();
                Uuid::parse_str(value["headers"]["id"].as_str().unwrap()).unwrap()
            })
            .collect()
    }
    #[tokio::test]
    #[ignore = "Owned copiedDB/internalRedis required; Source119 score and lexical-tie order."]
    async fn visibility_source119_score_order_and_tie_geometry() {
        let (jobs, mut raw) = fixture().await;
        for tied in [false, true] {
            let _: () = redis::cmd("FLUSHDB").query_async(&mut raw).await.unwrap();
            let a = reserve(&jobs, 99101).await;
            let b = reserve(&jobs, 99102).await;
            let fresh = reserve(&jobs, 99103).await;
            let ceiling = chrono::Utc::now().timestamp() as f64 - 3605.;
            age(&mut raw, &a, ceiling).await;
            age(&mut raw, &b, ceiling + if tied { 0. } else { 1. }).await;
            let mut expected = if tied {
                let mut receipts = [a.clone(), b.clone()];
                receipts.sort_by_key(|r| r.delivery_tag());
                receipts
                    .iter()
                    .map(|r| r.message().task_id)
                    .collect::<Vec<_>>()
            } else {
                vec![a.message().task_id, b.message().task_id]
            };
            for id in [99104, 99105] {
                let task_id = Uuid::new_v4();
                jobs.publish_index(
                    &crate::index_protocol::IndexTask::Initial { source_id: id },
                    task_id,
                    Arc::new(()),
                )
                .await
                .unwrap();
                expected.push(task_id);
            }
            let before: Vec<u8> = redis::cmd("HGET")
                .arg("unacked")
                .arg(fresh.delivery_tag().to_string())
                .query_async(&mut raw)
                .await
                .unwrap();
            let recovery = VisibilityRecovery::new(jobs.clone(), Config::default()).unwrap();
            assert_eq!(
                recovery.run_once(Arc::new(())).await.unwrap(),
                Outcome::Scanned {
                    selected: 2,
                    restored: 2,
                    foreign: 0,
                    lost: 0
                }
            );
            assert_eq!(delivery(&mut raw).await, expected);
            let after: Vec<u8> = redis::cmd("HGET")
                .arg("unacked")
                .arg(fresh.delivery_tag().to_string())
                .query_async(&mut raw)
                .await
                .unwrap();
            assert_eq!(before, after);
            let exists: bool = redis::cmd("EXISTS")
                .arg("unacked_mutex")
                .query_async(&mut raw)
                .await
                .unwrap();
            assert!(!exists);
            assert_eq!(recovery.last_error(), None);
            recovery.shutdown(Duration::from_secs(2)).await.unwrap();
            jobs.ack_index_eta(&fresh, Arc::new(())).await.unwrap();
            drop((a, b, fresh));
        }
        jobs.shutdown(Duration::from_secs(2)).await.unwrap();
    }
    #[tokio::test]
    #[ignore = "Owned copiedDB/internalRedis required; foreign preservation and token/score fences."]
    async fn visibility_foreign_reservations_mutex_and_score_fences() {
        let (jobs, mut raw) = fixture().await;
        let known = reserve(&jobs, 99201).await;
        let score = chrono::Utc::now().timestamp() as f64 - 3605.;
        age(&mut raw, &known, score).await;
        let foreign_tag = Uuid::new_v4();
        let encoded = crate::protocol::encode(
            &crate::protocol::Task::CreateBackup,
            Uuid::new_v4(),
            Uuid::new_v4(),
            foreign_tag,
            "rust-probe@visibility",
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        let tuple = serde_json::to_vec(&serde_json::json!([value, "", "celery"])).unwrap();
        let _: i64 = redis::cmd("HSET")
            .arg("unacked")
            .arg(foreign_tag.to_string())
            .arg(&tuple)
            .query_async(&mut raw)
            .await
            .unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("unacked_index")
            .arg(score + 1.)
            .arg(foreign_tag.to_string())
            .query_async(&mut raw)
            .await
            .unwrap();
        let recovery = VisibilityRecovery::new(jobs.clone(), Config::default()).unwrap();
        let _: () = redis::cmd("SET")
            .arg("unacked_mutex")
            .arg("foreign-owner")
            .arg("EX")
            .arg(300)
            .query_async(&mut raw)
            .await
            .unwrap();
        assert_eq!(
            recovery.run_once(Arc::new(())).await.unwrap(),
            Outcome::MutexHeld
        );
        let _: i64 = redis::cmd("DEL")
            .arg("unacked_mutex")
            .query_async(&mut raw)
            .await
            .unwrap();
        assert_eq!(
            recovery.run_once(Arc::new(())).await.unwrap(),
            Outcome::Scanned {
                selected: 2,
                restored: 1,
                foreign: 1,
                lost: 0
            }
        );
        let unchanged: Vec<u8> = redis::cmd("HGET")
            .arg("unacked")
            .arg(foreign_tag.to_string())
            .query_async(&mut raw)
            .await
            .unwrap();
        assert_eq!(tuple, unchanged);
        assert_eq!(delivery(&mut raw).await, vec![known.message().task_id]);
        let active = reserve(&jobs, 99202).await; // consume restored known first: compare task below is intentionally not assumed.
        let token = Uuid::new_v4().simple().to_string();
        let _: () = redis::cmd("SET")
            .arg("unacked_mutex")
            .arg(&token)
            .arg("EX")
            .arg(300)
            .query_async(&mut raw)
            .await
            .unwrap();
        assert!(!jobs
            .restore_visible_owned(
                &active,
                &score.to_string(),
                &(score + 2.).to_string(),
                &token,
                Arc::new(())
            )
            .await
            .unwrap());
        let _: () = redis::cmd("SET")
            .arg("unacked_mutex")
            .arg(vec![b'x'; 1024 * 1024])
            .query_async(&mut raw)
            .await
            .unwrap();
        assert_eq!(
            jobs.restore_visible_owned(
                &active,
                &score.to_string(),
                &(score + 2.).to_string(),
                &token,
                Arc::new(())
            )
            .await,
            Err(Error::LostMutex)
        );
        jobs.ack_index_eta(&active, Arc::new(())).await.unwrap();
        drop((known, active));
        recovery.shutdown(Duration::from_secs(2)).await.unwrap();
        jobs.shutdown(Duration::from_secs(2)).await.unwrap();
    }
    struct ReplyGate {
        base: String,
        hold: tokio::sync::watch::Sender<bool>,
        blocked: tokio::sync::watch::Receiver<bool>,
        trigger: Arc<std::sync::Mutex<Option<Vec<u8>>>>,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for ReplyGate {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    async fn reply_gate() -> ReplyGate {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::{TcpListener, TcpStream},
            sync::watch,
            task::JoinSet,
        };
        let mut bound = None;
        for port in 61400..61528 {
            if let Ok(l) = TcpListener::bind(("127.0.0.1", port)).await {
                bound = Some(l);
                break;
            }
        }
        let listener = bound.expect("Owned proxy port");
        let port = listener.local_addr().unwrap().port();
        let (hold, receiver) = watch::channel(false);
        let (blocked, observed) = watch::channel(false);
        let trigger = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
        let matching = trigger.clone();
        let switch = hold.clone();
        let task = tokio::spawn(async move {
            let mut peers = JoinSet::new();
            loop {
                tokio::select! {accepted=listener.accept()=>{
                                   let(client,_)=accepted.unwrap();let mut held=receiver.clone();let blocked=blocked.clone();let matching=matching.clone();let switch=switch.clone();
                                   peers.spawn(async move{let server=TcpStream::connect("127.0.0.1:6379").await.unwrap();let(mut cr,mut cw)=client.into_split();let(mut sr,mut sw)=server.into_split();
                                       let outgoing=async move{let mut chunk=[0u8;16384];let mut tail=Vec::new();loop{let n=cr.read(&mut chunk).await?;if n==0{return Ok::<(),std::io::Error>(())}tail.extend_from_slice(&chunk[..n]);{
                                           let mut trigger=matching.lock().unwrap();if trigger.as_ref().is_some_and(|needle|tail.windows(needle.len()).any(|v|v==needle)){switch.send_replace(true);trigger.take();}
                                       }
                if tail.len()>64{tail.drain(..tail.len()-64);}sw.write_all(&chunk[..n]).await?;}};
                                       let incoming=async move{let mut chunk=[0u8;16384];loop{let n=sr.read(&mut chunk).await?;if n==0{return Ok::<(),std::io::Error>(())}
                if *held.borrow(){blocked.send_replace(true);}
                if held.wait_for(|h|!*h).await.is_err(){return Ok(())}cw.write_all(&chunk[..n]).await?;}};
                                       let _=tokio::try_join!(outgoing,incoming);
                                   });
                               },_=peers.join_next(),if !peers.is_empty()=>{}}
            }
        });
        ReplyGate {
            base: format!("redis://:playback-fixture-only@127.0.0.1:{port}"),
            hold,
            blocked: observed,
            trigger,
            task,
        }
    }
    #[tokio::test]
    #[ignore = "Owned copiedDB/internalRedis required; canceled recovery through actual delayed restore reply."]
    async fn visibility_paused_restore_reply_retains_mutex_caller_and_shutdown() {
        let (direct, mut raw) = fixture().await;
        let receipt = reserve(&direct, 99301).await;
        let score = chrono::Utc::now().timestamp() as f64 - 3605.;
        age(&mut raw, &receipt, score).await;
        let digest: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(RESTORE_VISIBLE)
            .query_async(&mut raw)
            .await
            .unwrap();
        let mut gate = reply_gate().await;
        let jobs = Jobs::connect_fixture(
            &format!("{}/8", gate.base),
            &format!("{}/9", gate.base),
            &std::env::var("POSTGRES_DB").unwrap(),
            crate::transport::Config {
                operations: 1,
                command_timeout: Duration::from_millis(200),
                max_queued_jobs: 100,
            },
        )
        .await
        .unwrap();
        let recovery = VisibilityRecovery::new(jobs.clone(), Config::default()).unwrap();
        *gate.trigger.lock().unwrap() = Some(digest.into_bytes());
        let caller = Arc::new(());
        let weak = Arc::downgrade(&caller);
        let task = {
            let recovery = recovery.clone();
            tokio::spawn(async move { recovery.run_once(caller).await })
        };
        tokio::time::timeout(Duration::from_secs(3), gate.blocked.wait_for(|v| *v))
            .await
            .unwrap()
            .unwrap();
        let queued: i64 = redis::cmd("LLEN")
            .arg("celery")
            .query_async(&mut raw)
            .await
            .unwrap();
        assert_eq!(
            queued, 1,
            "The restore executed before its physical reply was released"
        );
        task.abort();
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(weak.upgrade().is_some());
        assert_eq!(jobs.available_slots(), 0);
        assert_eq!(
            recovery.shutdown(Duration::from_millis(30)).await,
            Err(Error::DrainTimeout)
        );
        let mutex: Option<String> = redis::cmd("GET")
            .arg("unacked_mutex")
            .query_async(&mut raw)
            .await
            .unwrap();
        assert!(mutex.is_some());
        gate.hold.send_replace(false);
        recovery.shutdown(Duration::from_secs(3)).await.unwrap();
        assert!(weak.upgrade().is_none());
        assert_eq!(
            recovery.last_error(),
            Some(Error::AmbiguousRestore(receipt.message().task_id))
        );
        let mutex: bool = redis::cmd("EXISTS")
            .arg("unacked_mutex")
            .query_async(&mut raw)
            .await
            .unwrap();
        assert!(!mutex);
        assert_eq!(delivery(&mut raw).await, vec![receipt.message().task_id]);
        jobs.shutdown(Duration::from_secs(2)).await.unwrap();
        drop(receipt);
        direct.shutdown(Duration::from_secs(2)).await.unwrap();
    }
}
