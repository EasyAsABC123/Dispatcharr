//! Immediate pidbox backend marks from pinned Celery _revoke/Backend code.
//! Original Celery/Dispatcharr authors retain all application/protocol credit.
use super::*;
fn metadata(task_key: &str, now: chrono::DateTime<chrono::Utc>) -> Result<Vec<u8>, Error> {
    if !crate::normal_revocation::valid(task_key) {
        return Err(Error::Protocol);
    }
    let date = now.to_rfc3339_opts(
        if now.timestamp_subsec_micros() == 0 {
            chrono::SecondsFormat::Secs
        } else {
            chrono::SecondsFormat::Micros
        },
        false,
    );
    // No request/current task exists in the control receiver. In Source this
    // yields children=None, and no fabricated parent/group/request fields.
    serde_json::to_vec(&serde_json::json!({"status":"REVOKED","result":{"exc_type":"TaskRevokedError","exc_message":["revoked"],"exc_module":"celery.exceptions"},"traceback":null,"children":null,"date_done":date,"task_id":task_key})).map_err(|_|Error::Protocol)
}
impl Jobs {
    pub(crate) async fn write_control_revoked(
        &self,
        task_key: &str,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let bytes = metadata(task_key, chrono::Utc::now())?;
        let (done, completed) = tokio::sync::oneshot::channel();
        // Reuse the existing physical completion primitive and actual Jobs
        // lease. A logical timeout cannot release a still-active operation or
        // let the control reader report the per-ID attempt physically settled.
        let guard: Arc<dyn Send + Sync> = Arc::new((owner, PhysicalCompletion(Some(done))));
        let lease = self.lease(guard.clone())?;
        let observed = self
            .inner
            .result
            .command_classified::<i64, _>(
                WRITE_SUCCESS,
                &[format!("celery-tasks:celery-task-meta-{task_key}")],
                &[&bytes],
                lease,
            )
            .await
            .map(|(value, _lease)| value);
        drop(guard);
        completed.await.map_err(|_| Error::Backend)?;
        match observed {
            Ok(1) => Ok(true),
            Ok(0) => Ok(false), // Source never overwrites an existing SUCCESS.
            Ok(_) => Err(Error::Corrupt),
            Err(error) => Err(map(error)),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_no_request_revocation_is_null_children_and_known_exception_data() {
        let id = Uuid::new_v4();
        let now = chrono::DateTime::parse_from_rfc3339("2026-11-15T01:02:03.000000+00:00")
            .unwrap()
            .to_utc();
        let raw = metadata(&id.to_string(), now).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert!(v["children"].is_null());
        assert_eq!(v.as_object().unwrap().len(), 6);
        assert_eq!(v["date_done"], "2026-11-15T01:02:03+00:00");
        let decoded = crate::decode::decode(&raw, id).unwrap();
        assert_eq!(decoded.state, crate::decode::State::Revoked);
        assert_eq!(
            decoded.exception.unwrap().arguments,
            vec![serde_json::json!("revoked")]
        );
        let mut bad = v.clone();
        bad["status"] = serde_json::json!("SUCCESS");
        assert!(crate::decode::decode(&serde_json::to_vec(&bad).unwrap(), id).is_err());
        bad = v.clone();
        bad["parent_id"] = serde_json::json!(Uuid::new_v4());
        assert!(crate::decode::decode(&serde_json::to_vec(&bad).unwrap(), id).is_err());
        bad = v;
        bad["result"]["exc_message"] = serde_json::json!(["expired"]);
        assert!(crate::decode::decode(&serde_json::to_vec(&bad).unwrap(), id).is_err());
    }
    #[test]
    fn named_task_key_is_preserved_without_uuid_alias_or_request_metadata() {
        let raw = metadata("dvr-recover-37", chrono::Utc::now()).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(v["task_id"], "dvr-recover-37");
        assert!(v.get("parent_id").is_none());
        assert!(v.get("group_id").is_none());
        assert!(metadata("bad\nkey", chrono::Utc::now()).is_err());
    }
    // Same real-Redis reply-gate pattern as existing refresh physical tests.
    // These tests are ignored until Root supplies its owned disposable fixture.
    struct Proxy {
        base: String,
        hold: tokio::sync::watch::Sender<bool>,
        blocked: tokio::sync::watch::Receiver<bool>,
        stop: tokio::sync::watch::Sender<bool>,
        task: Option<tokio::task::JoinHandle<()>>,
    }
    impl Drop for Proxy {
        fn drop(&mut self) {
            if let Some(task) = &self.task {
                task.abort();
            }
        }
    }
    impl Proxy {
        async fn close(mut self) {
            self.stop.send_replace(true);
            if let Some(task) = self.task.take() {
                task.await.unwrap();
            }
        }
    }
    fn owned_info() -> redis::ConnectionInfo {
        assert_eq!(
            std::env::var("JOB_DISPOSABLE_FIXTURE").unwrap(),
            "playback-fixture-only"
        );
        let client = redis::Client::open(
            std::env::var("JOB_REDIS_TEST_URL").expect("Explicit owned JOB_REDIS_TEST_URL"),
        )
        .unwrap();
        let info = client.get_connection_info().clone();
        assert_eq!(
            info.redis.password.as_deref(),
            Some("playback-fixture-only")
        );
        assert!(
            matches!(&info.addr,redis::ConnectionAddr::Tcp(host,_)if matches!(host.as_str(),"127.0.0.1"|"localhost"))
        );
        info
    }
    async fn direct(db: i64) -> redis::aio::MultiplexedConnection {
        let mut info = owned_info();
        info.redis.db = db;
        redis::Client::open(info)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap()
    }
    async fn proxy() -> Proxy {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let info = owned_info();
        let redis::ConnectionAddr::Tcp(host, port) = info.addr else {
            unreachable!()
        };
        let address = format!("{host}:{port}");
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let (hold, receiver) = tokio::sync::watch::channel(false);
        let (blocked, observed) = tokio::sync::watch::channel(false);
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _=stopped.changed()=>break,
                    accepted=listener.accept()=>{
                        let(client,_)=accepted.unwrap();let address=address.clone();let mut hold=receiver.clone();let blocked=blocked.clone();
                        connections.spawn(async move{
                            let server=tokio::net::TcpStream::connect(address).await.unwrap();let(mut cr,mut cw)=client.into_split();let(mut sr,mut sw)=server.into_split();
                            let incoming=async move{
                                let mut buffer=[0u8;16384];loop{
                                    let n=sr.read(&mut buffer).await?;if n==0{return Ok::<(),std::io::Error>(());}
                                    if *hold.borrow(){blocked.send_replace(true);}
                                    if hold.wait_for(|paused|!*paused).await.is_err(){return Ok(());}
                                    cw.write_all(&buffer[..n]).await?;
                                }
                            };
                            let _=tokio::try_join!(async{tokio::io::copy(&mut cr,&mut sw).await.map(|_|())},incoming);
                        });
                    },
                    _=connections.join_next(),if !connections.is_empty()=>{},
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Proxy {
            base: format!("redis://:playback-fixture-only@127.0.0.1:{port}"),
            hold,
            blocked: observed,
            stop,
            task: Some(task),
        }
    }
    async fn jobs(proxy: &Proxy) -> Jobs {
        Jobs::connect_defaults(
            &format!("{}/8", proxy.base),
            &format!("{}/9", proxy.base),
            Config {
                operations: 1,
                command_timeout: Duration::from_secs(30),
                max_queued_jobs: 100,
            },
        )
        .await
        .unwrap()
    }
    async fn actual_metadata(id: Uuid) -> bool {
        let mut c = direct(9).await;
        redis::cmd("EXISTS")
            .arg(format!("celery-tasks:celery-task-meta-{id}"))
            .query_async::<bool>(&mut c)
            .await
            .unwrap()
    }
    async fn wait_for(mut ready: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !ready() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn warm(jobs: &Jobs) {
        let id = Uuid::new_v4();
        assert!(jobs
            .write_control_revoked(&id.to_string(), Arc::new(()))
            .await
            .unwrap());
        let mut c = direct(9).await;
        let _: i64 = redis::cmd("DEL")
            .arg(format!("celery-tasks:celery-task-meta-{id}"))
            .query_async(&mut c)
            .await
            .unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "Requires guarded owned Redis8/9; actual backend reply hold and caller cancellation"]
    async fn actual_held_backend_retains_caller_and_jobs_slot_after_observer_abort() {
        let proxy = proxy().await;
        let jobs = jobs(&proxy).await;
        warm(&jobs).await;
        proxy.hold.send_replace(true);
        let id = Uuid::new_v4();
        let caller = Arc::new(());
        let weak = Arc::downgrade(&caller);
        let worker = jobs.clone();
        let observer =
            tokio::spawn(
                async move { worker.write_control_revoked(&id.to_string(), caller).await },
            );
        wait_for(|| *proxy.blocked.borrow()).await;
        assert!(actual_metadata(id).await);
        observer.abort();
        assert!(observer.await.unwrap_err().is_cancelled());
        assert!(weak.upgrade().is_some());
        assert_eq!(jobs.available_slots(), 0);
        assert_eq!(
            jobs.shutdown(Duration::from_millis(20)).await,
            Err(Error::DrainTimeout)
        );
        proxy.hold.send_replace(false);
        wait_for(|| weak.upgrade().is_none() && jobs.available_slots() == 1).await;
        jobs.shutdown(Duration::from_secs(3)).await.unwrap();
        proxy.close().await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "Requires guarded owned Redis8/9; actual control stop/drain and unchanged raw queue"]
    async fn actual_control_drain_waits_backend_reply_without_consuming_queued_task() {
        let proxy = proxy().await;
        let jobs = jobs(&proxy).await;
        warm(&jobs).await;
        let mut info = owned_info();
        info.redis.db = 8;
        let broker = redis::Client::open(info).unwrap();
        let redis::ConnectionAddr::Tcp(host, port) = owned_info().addr else {
            unreachable!()
        };
        let broker_url = format!("redis://:playback-fixture-only@{host}:{port}/8");
        let revoked = crate::normal_revocation::Revoked::new();
        let control = crate::normal_control::Control::connect(&broker_url, revoked.clone())
            .await
            .unwrap();
        control.bind_backend(jobs.clone()).unwrap();
        let caller = Arc::new(());
        let weak = Arc::downgrade(&caller);
        control.start(caller).unwrap();
        assert!(control.wait_ready(Duration::from_secs(3)).await);
        let mut queue = broker.get_multiplexed_async_connection().await.unwrap();
        let count: i64 = redis::cmd("LLEN")
            .arg("celery")
            .query_async(&mut queue)
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "Owned fixture queue must be empty; never overwrite foreign entries"
        );
        let id = Uuid::new_v4();
        let raw = crate::stats_protocol::encode(id).unwrap();
        let _: i64 = redis::cmd("LPUSH")
            .arg("celery")
            .arg(&raw)
            .query_async(&mut queue)
            .await
            .unwrap();
        proxy.hold.send_replace(true);
        control
            .revoke_one(&id.to_string(), Arc::new(()))
            .await
            .unwrap();
        wait_for(|| *proxy.blocked.borrow()).await;
        assert!(actual_metadata(id).await);
        assert!(revoked.contains(&id.to_string()).unwrap());
        let queued: Vec<u8> = redis::cmd("LINDEX")
            .arg("celery")
            .arg(-1)
            .query_async(&mut queue)
            .await
            .unwrap();
        assert_eq!(queued, raw);
        assert!(!control.shutdown(Duration::from_millis(20)).await);
        assert!(weak.upgrade().is_some());
        assert!(!control.observation().reader_closed);
        assert_eq!(
            jobs.shutdown(Duration::from_millis(20)).await,
            Err(Error::DrainTimeout)
        );
        proxy.hold.send_replace(false);
        assert!(control.shutdown(Duration::from_secs(3)).await);
        assert!(control.observation().reader_closed);
        assert_eq!(control.observation().backend_marked, 1);
        assert!(weak.upgrade().is_none());
        jobs.shutdown(Duration::from_secs(3)).await.unwrap();
        let queued: Vec<u8> = redis::cmd("LINDEX")
            .arg("celery")
            .arg(-1)
            .query_async(&mut queue)
            .await
            .unwrap();
        assert_eq!(queued, raw);
        let removed: i64 = redis::cmd("RPOP")
            .arg("celery")
            .query_async::<Option<Vec<u8>>>(&mut queue)
            .await
            .unwrap()
            .map(|value| {
                assert_eq!(value, raw);
                1
            })
            .unwrap_or(0);
        assert_eq!(removed, 1);
        proxy.close().await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "Requires guarded owned Redis8/9; actual per-ID caught-error continuation and SUCCESS protection"]
    async fn actual_backend_error_keeps_memory_and_later_ids_continue() {
        let proxy = proxy().await;
        let jobs = jobs(&proxy).await;
        let redis::ConnectionAddr::Tcp(host, port) = owned_info().addr else {
            unreachable!()
        };
        let broker_url = format!("redis://:playback-fixture-only@{host}:{port}/8");
        let revoked = crate::normal_revocation::Revoked::new();
        let control = crate::normal_control::Control::connect(&broker_url, revoked.clone())
            .await
            .unwrap();
        control.bind_backend(jobs.clone()).unwrap();
        control.start(Arc::new(())).unwrap();
        assert!(control.wait_ready(Duration::from_secs(3)).await);
        let good = Uuid::new_v4().to_string();
        let success = Uuid::new_v4().to_string();
        let bad = format!("!owned-control-bad-{}", Uuid::new_v4());
        let mut c = direct(9).await;
        let good_key = format!("celery-tasks:celery-task-meta-{good}");
        let success_key = format!("celery-tasks:celery-task-meta-{success}");
        let bad_key = format!("celery-tasks:celery-task-meta-{bad}");
        let old=serde_json::to_vec(&serde_json::json!({"status":"SUCCESS","result":"owned preserved","task_id":success,"traceback":null,"children":[],"date_done":"2026-11-15T00:00:00+00:00"})).unwrap();
        let _: i64 = redis::cmd("LPUSH")
            .arg(&bad_key)
            .arg("owned wrong type")
            .query_async(&mut c)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg(&success_key)
            .arg(&old)
            .query_async(&mut c)
            .await
            .unwrap();
        control
            .revoke_many(
                &[bad.clone(), good.clone(), good.clone(), success.clone()],
                Arc::new(()),
            )
            .await
            .unwrap();
        wait_for(|| control.observation().applied == 1).await;
        let observed = control.observation();
        assert_eq!(observed.backend_failed, 1);
        assert_eq!(observed.backend_marked, 1);
        assert_eq!(observed.backend_preserved_success, 1);
        assert_eq!(control.state(), crate::normal_control::State::Subscribed);
        for key in [&bad, &good, &success] {
            assert!(revoked.contains(key).unwrap());
        }
        let actual: Vec<u8> = redis::cmd("GET")
            .arg(&success_key)
            .query_async(&mut c)
            .await
            .unwrap();
        assert_eq!(actual, old);
        let actual: Vec<u8> = redis::cmd("GET")
            .arg(&good_key)
            .query_async(&mut c)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&actual).unwrap();
        assert_eq!(value["status"], "REVOKED");
        assert!(value["children"].is_null());
        assert!(control.shutdown(Duration::from_secs(3)).await);
        jobs.shutdown(Duration::from_secs(3)).await.unwrap();
        let _: i64 = redis::cmd("DEL")
            .arg(&bad_key)
            .arg(&good_key)
            .arg(&success_key)
            .query_async(&mut c)
            .await
            .unwrap();
        proxy.close().await;
    }
}
