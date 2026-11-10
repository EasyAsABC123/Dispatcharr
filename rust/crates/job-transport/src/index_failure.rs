//! Bounded observed Source136 failure metadata codec; component-only.
//! No classifier, writer, task execution, Python imports or traceback generator.
//! Original Dispatcharr/Celery/Kombu authors retain full application/algorithm credit.
use serde_json::{json, Value};
use uuid::Uuid;
const MAX_BYTES: usize = 262144;
const MAX_MESSAGE: usize = 8192;
const MAX_TRACEBACK: usize = 16384;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Capacity,
    Malformed,
    Identity,
    Unsupported,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Directory,
    Programming,
    CacheConnection,
    CacheAuthentication,
}
impl Kind {
    fn names(self) -> (&'static str, &'static str) {
        match self {
            Self::Directory => ("builtins", "IsADirectoryError"),
            Self::Programming => ("django.db.utils", "ProgrammingError"),
            Self::CacheConnection => ("redis.exceptions", "ConnectionError"),
            Self::CacheAuthentication => ("redis.exceptions", "AuthenticationError"),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedFailure {
    pub task_id: Uuid,
    kind: Kind,
    arguments: Vec<Value>,
    date_done: String,
    /// Opaque supplied observation. This codec never claims to execute/rebuild
    /// Python traceback frames. Native traceback policy remains a separate gate.
    traceback: String,
}
fn object_keys(value: &serde_json::Map<String, Value>, keys: &[&str]) -> bool {
    value.len() == keys.len() && keys.iter().all(|k| value.contains_key(*k))
}
fn text(value: &Value, maximum: usize) -> Result<&str, Error> {
    let text = value.as_str().ok_or(Error::Malformed)?;
    if text.len() > maximum {
        return Err(Error::Capacity);
    }
    Ok(text)
}
impl ObservedFailure {
    pub fn kind(&self) -> Kind {
        self.kind
    }
    pub fn decode(bytes: &[u8], expected: Uuid) -> Result<Self, Error> {
        if bytes.len() > MAX_BYTES {
            return Err(Error::Capacity);
        }
        let value: Value = serde_json::from_slice(bytes).map_err(|_| Error::Malformed)?;
        let root = value.as_object().ok_or(Error::Malformed)?;
        if !object_keys(
            root,
            &[
                "status",
                "result",
                "traceback",
                "children",
                "date_done",
                "task_id",
            ],
        ) {
            return Err(Error::Unsupported);
        }
        if value["status"] != "FAILURE" || value["children"] != json!([]) {
            return Err(Error::Unsupported);
        }
        let id = Uuid::parse_str(text(&value["task_id"], 36)?).map_err(|_| Error::Identity)?;
        if id != expected {
            return Err(Error::Identity);
        }
        let result = value["result"].as_object().ok_or(Error::Malformed)?;
        if !object_keys(result, &["exc_type", "exc_message", "exc_module"]) {
            return Err(Error::Unsupported);
        }
        let module = text(&value["result"]["exc_module"], 64)?;
        let class = text(&value["result"]["exc_type"], 64)?;
        let kind = match (module, class) {
            ("builtins", "IsADirectoryError") => Kind::Directory,
            ("django.db.utils", "ProgrammingError") => Kind::Programming,
            ("redis.exceptions", "ConnectionError") => Kind::CacheConnection,
            ("redis.exceptions", "AuthenticationError") => Kind::CacheAuthentication,
            _ => return Err(Error::Unsupported),
        };
        let arguments = value["result"]["exc_message"]
            .as_array()
            .ok_or(Error::Malformed)?;
        if kind == Kind::Directory {
            if arguments.len() != 2 || arguments[0].as_i64() != Some(21) {
                return Err(Error::Unsupported);
            }
            text(&arguments[1], MAX_MESSAGE)?;
        } else {
            if arguments.len() != 1 {
                return Err(Error::Unsupported);
            }
            text(&arguments[0], MAX_MESSAGE)?;
        }
        let date_done = text(&value["date_done"], 64)?;
        let date = chrono::DateTime::parse_from_rfc3339(date_done).map_err(|_| Error::Malformed)?;
        if date.offset().local_minus_utc() != 0 {
            return Err(Error::Unsupported);
        }
        let traceback = text(&value["traceback"], MAX_TRACEBACK)?;
        if traceback.is_empty() {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            task_id: id,
            kind,
            arguments: arguments.clone(),
            date_done: date_done.to_owned(),
            traceback: traceback.to_owned(),
        })
    }
    /// Semantic metadata encoding only; Source JSON whitespace is not recreated.
    /// No builder accepts an inferred native error or fabricated Source frames.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let (module, class) = self.kind.names();
        let bytes=serde_json::to_vec(&json!({"status":"FAILURE","result":{"exc_type":class,"exc_message":self.arguments,"exc_module":module},"traceback":self.traceback,"children":[],"date_done":self.date_done,"task_id":self.task_id.to_string()})).map_err(|_|Error::Malformed)?;
        if bytes.len() > MAX_BYTES {
            return Err(Error::Capacity);
        }
        Ok(bytes)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn corpus() -> Value {
        serde_json::from_str(include_str!("../datasets/source-index-failure-136.json")).unwrap()
    }
    #[test]
    fn source136_observed_failure_metadata_semantically_roundtrips_without_imports() {
        let source = corpus();
        let mut count = 0;
        for case in source["cases"].as_array().unwrap() {
            if case["result"]["status"] != "FAILURE" {
                continue;
            }
            let result = &case["result"];
            let id = Uuid::parse_str(result["task_id"].as_str().unwrap()).unwrap();
            let decoded =
                ObservedFailure::decode(&serde_json::to_vec(result).unwrap(), id).unwrap();
            let encoded: Value = serde_json::from_slice(&decoded.encode().unwrap()).unwrap();
            assert_eq!(&encoded, result);
            count += 1;
        }
        assert_eq!(count, 5);
    }
    #[test]
    fn unsupported_classes_parent_shapes_ids_and_arguments_fail_closed() {
        let source = corpus();
        let result = source["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == "directory")
            .unwrap()["result"]
            .clone();
        let id = Uuid::parse_str(result["task_id"].as_str().unwrap()).unwrap();
        for mutation in 0..5 {
            let mut value = result.clone();
            match mutation {
                0 => value["result"]["exc_module"] = json!("os"),
                1 => value["result"]["exc_message"][0] = json!(21.0),
                2 => value["traceback"] = Value::Null,
                3 => value["parent_id"] = Value::Null,
                _ => value["result"]["exc_message"][1] = json!("x".repeat(MAX_MESSAGE + 1)),
            }
            assert!(ObservedFailure::decode(&serde_json::to_vec(&value).unwrap(), id).is_err());
        }
        assert_eq!(
            ObservedFailure::decode(&serde_json::to_vec(&result).unwrap(), Uuid::new_v4()),
            Err(Error::Identity)
        );
        assert_eq!(
            ObservedFailure::decode(&vec![b' '; MAX_BYTES + 1], id),
            Err(Error::Capacity)
        );
    }
    #[tokio::test]
    #[ignore = "Guarded copiedDB Source result-reader interoperability peer; no worker or classifier."]
    async fn native_failure_payload_peer() {
        use crate::transport::{Config, Jobs};
        use std::{sync::Arc, time::Duration};
        assert_eq!(
            std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
        );
        let path = std::env::var("JOB_FAILURE_INPUT_PATH").unwrap();
        assert!(
            path.starts_with("/evidence/failure-input-")
                && path.ends_with(".json")
                && !path.contains("..")
                && path.len() < 160
        );
        let metadata = std::fs::metadata(&path).unwrap();
        assert!(metadata.is_file() && metadata.len() <= MAX_BYTES as u64);
        let request: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let id = Uuid::parse_str(request["task_id"].as_str().unwrap()).unwrap();
        let failure =
            ObservedFailure::decode(&serde_json::to_vec(&request["metadata"]).unwrap(), id)
                .unwrap();
        let jobs = Jobs::connect_fixture(
            std::env::var("CELERY_BROKER_URL").unwrap().as_str(),
            std::env::var("CELERY_RESULT_BACKEND").unwrap().as_str(),
            std::env::var("POSTGRES_DB").unwrap().as_str(),
            Config {
                operations: 1,
                command_timeout: Duration::from_millis(200),
                max_queued_jobs: 100,
            },
        )
        .await
        .unwrap();
        let written = jobs
            .write_observed_index_failure(id, &failure, Arc::new(()))
            .await
            .unwrap();
        assert_eq!(written, request["expect_written"].as_bool().unwrap());
        assert_eq!(
            jobs.write_observed_index_failure(Uuid::new_v4(), &failure, Arc::new(()))
                .await,
            Err(crate::transport::Error::Protocol)
        );
        jobs.shutdown(Duration::from_secs(2)).await.unwrap();
    }
    struct Proxy {
        base: String,
        hold: tokio::sync::watch::Sender<bool>,
        blocked: tokio::sync::watch::Receiver<bool>,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Proxy {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    async fn proxy() -> Proxy {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::{TcpListener, TcpStream},
            sync::watch,
            task::JoinSet,
        };
        assert_eq!(
            std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
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
        assert!(
            matches!(&info.addr,redis::ConnectionAddr::Tcp(h,6379)if h=="127.0.0.1"||h=="localhost")
        );
        let mut bound = None;
        for port in 61600..61728 {
            if let Ok(l) = TcpListener::bind(("127.0.0.1", port)).await {
                bound = Some(l);
                break;
            }
        }
        let listener = bound.expect("Owned proxy port");
        let port = listener.local_addr().unwrap().port();
        let (hold, receiver) = watch::channel(false);
        let (blocked, observed) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut peers = JoinSet::new();
            loop {
                tokio::select! {accepted=listener.accept()=>{
                    let(client,_)=accepted.unwrap();let mut held=receiver.clone();let blocked=blocked.clone();
                    peers.spawn(async move{let server=TcpStream::connect("127.0.0.1:6379").await.unwrap();let(mut cr,mut cw)=client.into_split();let(mut sr,mut sw)=server.into_split();
                        let incoming=async move{let mut chunk=[0u8;16384];loop{let n=sr.read(&mut chunk).await?;if n==0{return Ok::<(),std::io::Error>(())}
                            if *held.borrow(){blocked.send_replace(true);}
                            if held.wait_for(|h|!*h).await.is_err(){return Ok(())}cw.write_all(&chunk[..n]).await?;}};
                        let _=tokio::try_join!(async{tokio::io::copy(&mut cr,&mut sw).await.map(|_|())},incoming);
                    });
                },_=peers.join_next(),if !peers.is_empty()=>{}}
            }
        });
        Proxy {
            base: format!("redis://:playback-fixture-only@127.0.0.1:{port}"),
            hold,
            blocked: observed,
            task,
        }
    }
    fn observed() -> ObservedFailure {
        let source = corpus();
        let result = source["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == "directory")
            .unwrap()["result"]
            .clone();
        let id = Uuid::parse_str(result["task_id"].as_str().unwrap()).unwrap();
        let mut failure =
            ObservedFailure::decode(&serde_json::to_vec(&result).unwrap(), id).unwrap();
        failure.task_id = Uuid::new_v4();
        failure
    }
    #[tokio::test]
    #[ignore = "CopiedDB/internalRedis actual delayed failure-result reply and physical caller ownership."]
    async fn failure_writer_delayed_reply_cancel_and_timeout_retain_physical_owner() {
        use crate::transport::{Config, Error as TransportError, Jobs};
        use std::{sync::Arc, time::Duration};
        for cancel in [false, true] {
            let mut proxy = proxy().await;
            let jobs = Jobs::connect_fixture(
                &format!("{}/8", proxy.base),
                &format!("{}/9", proxy.base),
                &std::env::var("POSTGRES_DB").unwrap(),
                Config {
                    operations: 1,
                    command_timeout: Duration::from_millis(200),
                    max_queued_jobs: 100,
                },
            )
            .await
            .unwrap();
            let initial = observed();
            jobs.write_observed_index_failure(initial.task_id, &initial, Arc::new(()))
                .await
                .unwrap();
            let record = observed();
            let id = record.task_id;
            let key = format!("celery-tasks:celery-task-meta-{id}");
            let caller = Arc::new(());
            let weak = Arc::downgrade(&caller);
            proxy.hold.send_replace(true);
            let writing = {
                let jobs = jobs.clone();
                tokio::spawn(
                    async move { jobs.write_observed_index_failure(id, &record, caller).await },
                )
            };
            tokio::time::timeout(Duration::from_secs(3), proxy.blocked.wait_for(|v| *v))
                .await
                .unwrap()
                .unwrap();
            let mut direct = redis::Client::open("redis://:playback-fixture-only@127.0.0.1:6379/9")
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            let stored: Vec<u8> = redis::cmd("GET")
                .arg(&key)
                .query_async(&mut direct)
                .await
                .unwrap();
            let payload: Value = serde_json::from_slice(&stored).unwrap();
            assert_eq!(payload["status"], "FAILURE");
            if cancel {
                writing.abort();
            } else {
                assert_eq!(
                    writing.await.unwrap(),
                    Err(TransportError::AmbiguousResult(id))
                );
            }
            assert_eq!(jobs.available_slots(), 0);
            assert!(weak.upgrade().is_some());
            assert_eq!(
                jobs.shutdown(Duration::from_millis(30)).await,
                Err(TransportError::DrainTimeout)
            );
            proxy.hold.send_replace(false);
            jobs.shutdown(Duration::from_secs(3)).await.unwrap();
            assert!(weak.upgrade().is_none());
            let ttl: i64 = redis::cmd("TTL")
                .arg(&key)
                .query_async(&mut direct)
                .await
                .unwrap();
            assert!((3590..=3600).contains(&ttl));
            let _: i64 = redis::cmd("DEL")
                .arg(key)
                .arg(format!("celery-tasks:celery-task-meta-{}", initial.task_id))
                .query_async(&mut direct)
                .await
                .unwrap();
        }
    }
    #[tokio::test]
    #[ignore = "CopiedDB/internalRedis wrong-type failure backend preservation."]
    async fn failure_writer_wrong_type_does_not_replace_foreign_value() {
        use crate::transport::{Config, Error as TransportError, Jobs};
        use std::{sync::Arc, time::Duration};
        let proxy = proxy().await;
        let jobs = Jobs::connect_fixture(
            &format!("{}/8", proxy.base),
            &format!("{}/9", proxy.base),
            &std::env::var("POSTGRES_DB").unwrap(),
            Config::default(),
        )
        .await
        .unwrap();
        let failure = observed();
        let key = format!("celery-tasks:celery-task-meta-{}", failure.task_id);
        let mut direct = redis::Client::open("redis://:playback-fixture-only@127.0.0.1:6379/9")
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let _: i64 = redis::cmd("HSET")
            .arg(&key)
            .arg("owned-foreign")
            .arg("retained")
            .query_async(&mut direct)
            .await
            .unwrap();
        assert_eq!(
            jobs.write_observed_index_failure(failure.task_id, &failure, Arc::new(()))
                .await,
            Err(TransportError::Corrupt)
        );
        let value: String = redis::cmd("HGET")
            .arg(&key)
            .arg("owned-foreign")
            .query_async(&mut direct)
            .await
            .unwrap();
        assert_eq!(value, "retained");
        jobs.shutdown(Duration::from_secs(2)).await.unwrap();
        let _: i64 = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut direct)
            .await
            .unwrap();
    }
}
