//! Fixture-only fetch-statistics execution using the shared native scanner/sink.
//! Original Dispatcharr authors retain application/algorithm credit and AGPL.
use crate::transport::{Error as TransportError, Jobs};
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use serde_json::{json, Value};
use std::{
    fmt::Write,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Error {
    Configuration,
    Closed,
    Capacity,
    Build,
    InvalidProjection,
    Event,
    Transport(TransportError),
    Panic,
}
pub(crate) trait Source: Send + Sync {
    /// Implementations retain this scope through actual scan replies, including
    /// a logical timeout; returning an error is not permission for task SUCCESS.
    fn build(&self, caller: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<Value, Error>>;
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Run {
    Empty,
    Completed(Uuid),
    AlreadySucceeded(Uuid),
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
struct Attempt {
    done: Option<oneshot::Sender<()>>,
    _caller: Arc<dyn Send + Sync>,
}
impl Drop for Attempt {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}
struct Inner {
    jobs: Jobs,
    source: Arc<dyn Source>,
    sink: Arc<dyn UpdateEventSink>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
    maximum: usize,
}
#[derive(Clone)]
pub(crate) struct StatsWorker {
    inner: Arc<Inner>,
}
impl StatsWorker {
    pub fn new_fixture(
        jobs: Jobs,
        source: Arc<dyn Source>,
        sink: Arc<dyn UpdateEventSink>,
        maximum: usize,
    ) -> Result<Self, Error> {
        let database = jobs.copied_database().ok_or(Error::Configuration)?;
        if std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref() != Ok("playback-fixture-only")
            || std::env::var("POSTGRES_DB").as_deref() != Ok(database)
            || maximum == 0
            || maximum > 3 * 1024 * 1024
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                source,
                sink,
                maximum,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        })
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slots.available_permits()
    }
    pub async fn run_once(&self, caller: Arc<dyn Send + Sync>) -> Result<Run, Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let slot = self
            .inner
            .slots
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
            let Some(message) = inner.jobs.claim_stats_fixture(owner.clone()).await.map_err(Error::Transport)? else { return Ok(Run::Empty) };
            let value = source_event_projection(inner.source.build(owner.clone()).await?)?;
            let text = python_json(&value, inner.maximum)?;
            drop(value);
            let event = json!({"type":"update","data":{"success":true,"type":"channel_stats","stats":text}});
            let (done, settled) = oneshot::channel();
            let attempt = Arc::new(Attempt { done: Some(done), _caller: owner.clone() });
            let sent = inner.sink.send_retained("updates", event, attempt.clone()).await;
            drop(attempt);
            // send_retained may return a logical error before its supervised
            // Redis reply. The physical owner receipt must settle first.
            settled.await.map_err(|_| Error::Event)?;
            match sent {
                Ok(()) | Err(UpdateEventError::Unavailable) => {},
                Err(_) => return Err(Error::Event),
            }
            let written = inner.jobs.write_stats_success_fixture(&message, owner).await.map_err(Error::Transport)?;
            Ok(if written { Run::Completed(message.task_id) } else { Run::AlreadySucceeded(message.task_id) })
        }).await.map_err(|_| Error::Panic)?
    }
    /// Drain this family only. The application closes shared Jobs once every
    /// family has settled; an idle family must not close another's publisher.
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.closed.store(true, Ordering::Release);
        tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned())
            .await
            .is_ok()
    }
}

// Original build_live_channel_stats_data and get_basic_channel_info construct
// these dictionaries in this order. Preserve that raw json.dumps contract in
// the new task wrapper; do not change the shared HTTP snapshot representation.
fn source_event_projection(value: Value) -> Result<Value, Error> {
    fn ordered(value: &Value, keys: &[&str]) -> Result<Value, Error> {
        let object = value.as_object().ok_or(Error::InvalidProjection)?;
        if object.keys().any(|key| !keys.contains(&key.as_str())) {
            return Err(Error::InvalidProjection);
        }
        let mut result = serde_json::Map::new();
        for key in keys {
            if let Some(value) = object.get(*key) {
                result.insert((*key).to_owned(), value.clone());
            }
        }
        Ok(Value::Object(result))
    }
    let mut result = ordered(&value, &["channels", "count"])?;
    let channels = result["channels"]
        .as_array_mut()
        .ok_or(Error::InvalidProjection)?;
    for value in channels {
        *value = ordered(
            value,
            &[
                "channel_id",
                "state",
                "url",
                "stream_profile",
                "owner",
                "buffer_index",
                "client_count",
                "uptime",
                "started_at",
                "channel_name",
                "logo_id",
                "m3u_profile_id",
                "stream_id",
                "stream_name",
                "total_bytes",
                "avg_bitrate_kbps",
                "avg_bitrate",
                "healthy",
                "clients",
                "video_codec",
                "resolution",
                "source_fps",
                "ffmpeg_speed",
                "audio_codec",
                "audio_channels",
                "stream_type",
            ],
        )?;
        if value["started_at"].is_null() && value["uptime"].as_f64() == Some(0.0) {
            // Source's no-positive-init_time branch is integer0, not float0.0.
            value["uptime"] = json!(0);
        }
        let clients = value["clients"]
            .as_array_mut()
            .ok_or(Error::InvalidProjection)?;
        for client in clients {
            *client = ordered(
                client,
                &[
                    "client_id",
                    "user_agent",
                    "output_format",
                    "ip_address",
                    "connected_at",
                    "user_id",
                    "output_profile_id",
                ],
            )?;
        }
    }
    Ok(result)
}

/// Python json.dumps defaults: insertion order, ASCII strings and spaced
/// separators. Exact float spelling remains bounded to observed projections.
pub(crate) fn python_json(value: &Value, maximum: usize) -> Result<String, Error> {
    struct Writer {
        text: String,
        maximum: usize,
        nodes: usize,
    }
    impl Writer {
        fn put(&mut self, text: &str) -> Result<(), Error> {
            if self
                .text
                .len()
                .checked_add(text.len())
                .is_none_or(|n| n > self.maximum)
            {
                return Err(Error::Capacity);
            }
            self.text.push_str(text);
            Ok(())
        }
        fn quote(&mut self, text: &str) -> Result<(), Error> {
            self.put("\"")?;
            for ch in text.chars() {
                match ch {
                    '"' => self.put("\\\"")?,
                    '\\' => self.put("\\\\")?,
                    '\u{8}' => self.put("\\b")?,
                    '\u{c}' => self.put("\\f")?,
                    '\n' => self.put("\\n")?,
                    '\r' => self.put("\\r")?,
                    '\t' => self.put("\\t")?,
                    '\u{20}'..='\u{7e}' => self.put(ch.encode_utf8(&mut [0; 4]))?,
                    _ => {
                        let mut units = [0; 2];
                        for unit in ch.encode_utf16(&mut units) {
                            let mut escaped = String::new();
                            write!(&mut escaped, "\\u{unit:04x}").unwrap();
                            self.put(&escaped)?;
                        }
                    }
                }
            }
            self.put("\"")
        }
        fn value(&mut self, value: &Value, depth: usize) -> Result<(), Error> {
            self.nodes += 1;
            if depth > 32 || self.nodes > 100_000 {
                return Err(Error::Capacity);
            }
            match value {
                Value::Null => self.put("null"),
                Value::Bool(v) => self.put(if *v { "true" } else { "false" }),
                Value::String(v) => self.quote(v),
                Value::Number(v) => {
                    let text = v.to_string();
                    if text.len() > 64 {
                        return Err(Error::InvalidProjection);
                    }
                    self.put(&text)
                }
                Value::Array(values) => {
                    self.put("[")?;
                    for (i, v) in values.iter().enumerate() {
                        if i > 0 {
                            self.put(", ")?;
                        }
                        self.value(v, depth + 1)?;
                    }
                    self.put("]")
                }
                Value::Object(values) => {
                    self.put("{")?;
                    for (i, (k, v)) in values.iter().enumerate() {
                        if i > 0 {
                            self.put(", ")?;
                        }
                        self.quote(k)?;
                        self.put(": ")?;
                        self.value(v, depth + 1)?;
                    }
                    self.put("}")
                }
            }
        }
    }
    let mut writer = Writer {
        text: String::new(),
        maximum,
        nodes: 0,
    };
    writer.value(value, 0)?;
    Ok(writer.text)
}

#[test]
fn captured_source_stats_strings_are_exact_without_json_normalization() {
    let source: Value =
        serde_json::from_str(include_str!("../datasets/source-default-stats-wire.json")).unwrap();
    for row in source["stats_cases"].as_array().unwrap() {
        if let Some(event) = row["events"].as_array().unwrap().first() {
            assert_eq!(
                python_json(&row["stats"], 65536).unwrap(),
                event["data"]["stats"].as_str().unwrap()
            );
        }
    }
    assert_eq!(
        python_json(&json!(["é😀\u{7f}\n", true, null]), 128).unwrap(),
        "[\"\\u00e9\\ud83d\\ude00\\u007f\\n\", true, null]"
    );
    assert_eq!(python_json(&json!(["é"]), 3), Err(Error::Capacity));
    let mut deep = Value::Null;
    for _ in 0..40 {
        deep = json!([deep]);
    }
    assert_eq!(python_json(&deep, 65536), Err(Error::Capacity));
}
