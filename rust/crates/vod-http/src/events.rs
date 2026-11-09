//! Bounded VOD transition -> audit -> full statistics publisher.
//! Original Dispatcharr authors/contributors retain all application credit.
use crate::statistics;
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use dispatcharr_store::{AuthService, Store, StoreError};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, watch};
#[derive(Clone)]
pub(crate) struct Transition {
    pub started: bool,
    pub content_name: String,
    pub content_uuid: String,
    pub client_ip: String,
    pub user_id: String,
    pub username: Option<String>,
}
impl Transition {
    fn bytes(&self) -> usize {
        self.content_name.len()
            + self.content_uuid.len()
            + self.client_ip.len()
            + self.user_id.len()
            + self.username.as_ref().map_or(0, String::len)
    }
    fn event(&self) -> Value {
        json!({"type":"update","data":{"type":if self.started{"vod_started"}else{"vod_stopped"},"content_name":self.content_name,"content_uuid":self.content_uuid,"client_ip":self.client_ip,"user_id":self.user_id}})
    }
    fn details(&self) -> Value {
        json!({"content_name":self.content_name,"content_uuid":self.content_uuid,"client_ip":self.client_ip,"username":self.username})
    }
}
struct Pending {
    count: Arc<AtomicUsize>,
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
    }
}
struct Work {
    transition: Transition,
    pending: Arc<Pending>,
}
struct Inner {
    sender: mpsc::Sender<Work>,
    pending: Arc<AtomicUsize>,
    failed: Arc<AtomicU64>,
    closed: AtomicBool,
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
#[derive(Clone)]
pub(crate) struct Producer {
    inner: Arc<Inner>,
}
struct Context {
    store: Store,
    auth: AuthService,
    transport: RedisTransport,
    sink: Arc<dyn UpdateEventSink>,
    failed: Arc<AtomicU64>,
}
struct Finished(watch::Sender<bool>);
impl Drop for Finished {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}
impl Producer {
    pub async fn connect(
        store: Store,
        auth: AuthService,
        redis: &str,
        sink: Arc<dyn UpdateEventSink>,
        capacity: usize,
    ) -> Result<Self, StoreError> {
        if capacity == 0 || capacity > 4096 {
            return Err(StoreError::Unsupported("Invalid VOD event budget"));
        }
        let transport = RedisTransport::connect(
            redis,
            TransportConfig {
                max_operations: 1,
                max_queued_operations: 1,
                command_timeout: Duration::from_secs(3),
            },
        )
        .await
        .map_err(|_| StoreError::Unsupported("VOD event transport unavailable"))?;
        let (sender, mut receiver) = mpsc::channel::<Work>(capacity);
        let (stop, mut stopped) = watch::channel(false);
        let (done, completed) = watch::channel(false);
        let failed = Arc::new(AtomicU64::new(0));
        let context = Context {
            store,
            auth,
            transport,
            sink,
            failed: failed.clone(),
        };
        tokio::spawn(async move {
            let _finished = Finished(done);
            loop {
                if *stopped.borrow() {
                    break;
                }
                let work = tokio::select! {value=receiver.recv()=>match value{Some(value)=>value,None=>break},_=stopped.changed()=>break};
                let processed = tokio::select! {result=tokio::time::timeout(Duration::from_secs(8),context.process(&work))=>result.is_ok(),_=stopped.changed()=>break};
                if !processed {
                    context.failed.fetch_add(1, Ordering::AcqRel);
                }
            }
        });
        Ok(Self {
            inner: Arc::new(Inner {
                sender,
                pending: Arc::new(AtomicUsize::new(0)),
                failed,
                closed: AtomicBool::new(false),
                stop,
                done: completed,
            }),
        })
    }
    pub async fn emit(&self, transition: Transition) -> bool {
        if self.inner.closed.load(Ordering::Acquire) || transition.bytes() > 4096 {
            self.inner.failed.fetch_add(1, Ordering::AcqRel);
            return false;
        }
        self.inner.pending.fetch_add(1, Ordering::AcqRel);
        let pending = Arc::new(Pending {
            count: self.inner.pending.clone(),
        });
        if self.inner.closed.load(Ordering::Acquire) {
            return false;
        }
        let sent = tokio::time::timeout(
            Duration::from_secs(2),
            self.inner.sender.send(Work {
                transition,
                pending,
            }),
        )
        .await
        .is_ok_and(|result| result.is_ok());
        if !sent {
            self.inner.failed.fetch_add(1, Ordering::AcqRel);
        }
        sent
    }
    pub fn diagnostics(&self) -> Value {
        json!({"pending":self.inner.pending.load(Ordering::Acquire),"failed":self.inner.failed.load(Ordering::Acquire)})
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.closed.store(true, Ordering::Release);
        let deadline = tokio::time::Instant::now() + timeout;
        let drained = tokio::time::timeout_at(deadline, async {
            while self.inner.pending.load(Ordering::Acquire) != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok();
        if !drained {
            self.inner.failed.fetch_add(
                self.inner.pending.load(Ordering::Acquire) as u64,
                Ordering::AcqRel,
            );
        }
        self.inner.stop.send_replace(true);
        let mut done = self.inner.done.clone();
        let completed = *done.borrow()
            || tokio::time::timeout_at(deadline, done.wait_for(|value| *value))
                .await
                .is_ok_and(|result| result.is_ok());
        drained && completed
    }
    pub fn cancel(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.stop.send_replace(true);
    }
}
impl Context {
    async fn send(&self, value: Value) {
        if !tokio::time::timeout(Duration::from_secs(2), self.sink.send("updates", value))
            .await
            .is_ok_and(|result| result.is_ok())
        {
            self.failed.fetch_add(1, Ordering::AcqRel);
        }
    }
    async fn process(&self, work: &Work) {
        self.send(work.transition.event()).await;
        self.audit(&work.transition).await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        match statistics::snapshot(&self.store, &self.transport, now, work.pending.clone()).await {
            Ok(stats) => match serde_json::to_string(&stats) {
                Ok(stats) => {
                    self.send(json!({"type":"update","data":{"type":"vod_stats","stats":stats}}))
                        .await
                }
                Err(_) => {
                    self.failed.fetch_add(1, Ordering::AcqRel);
                }
            },
            Err(_) => {
                self.failed.fetch_add(1, Ordering::AcqRel);
            }
        }
    }
    async fn audit(&self, event: &Transition) {
        let kind = if event.started {
            "vod_start"
        } else {
            "vod_stop"
        };
        if sqlx::query("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details)VALUES($1,now(),NULL,NULL,$2)").bind(kind).bind(event.details()).execute(self.store.pool()).await.is_err(){self.failed.fetch_add(1,Ordering::AcqRel);return;}
        // Preserve original cutoff-row retention; Connect/plugin dispatch is M8.
        let settings = self.auth.system_settings().await.unwrap_or(Value::Null);
        let maximum = match settings.get("max_system_events") {
            Some(Value::Bool(true)) => 1,
            Some(Value::Number(value)) => value
                .as_i64()
                .filter(|v| *v != 0)
                .or_else(|| {
                    value
                        .as_f64()
                        .filter(|v| v.is_finite() && *v != 0.0)
                        .map(|v| v as i64)
                })
                .unwrap_or(100),
            Some(Value::String(value)) if !value.is_empty() => {
                value.trim().replace('_', "").parse().unwrap_or(100)
            }
            _ => 100,
        };
        if maximum < 0 {
            return;
        }
        let result=async {
            let count:i64=sqlx::query_scalar("SELECT count(*) FROM core_systemevent").fetch_one(self.store.pool()).await?;
            if count>maximum {
                let cutoff:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemevent ORDER BY timestamp DESC LIMIT 1 OFFSET $1").bind(maximum).fetch_optional(self.store.pool()).await?;
                if let Some(cutoff)=cutoff{sqlx::query("DELETE FROM core_systemevent WHERE id<$1").bind(cutoff).execute(self.store.pool()).await?;}
            }
            Ok::<_,sqlx::Error>(())
        }.await;
        if result.is_err() {
            self.failed.fetch_add(1, Ordering::AcqRel);
        }
    }
}
