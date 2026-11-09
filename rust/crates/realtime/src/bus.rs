//! Bounded ChannelsRedis4.x group transport, derived from original core/utils.py.
use crate::codec;
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use rand::RngCore;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{
    watch, Mutex as AsyncMutex, Notify, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore,
};

type Result<T> = std::result::Result<T, UpdateEventError>;
#[derive(Clone, Debug)]
pub struct BusConfig {
    pub max_connections: usize,
    pub max_group_members: usize,
    pub max_message_bytes: usize,
    /// Covers serialized request copies, not caller-owned JSON values.
    pub max_publish_bytes: usize,
    pub max_read_bytes: usize,
    pub channel_capacity: usize,
    pub command_timeout: Duration,
    pub poll_interval: Duration,
}
impl Default for BusConfig {
    fn default() -> Self {
        Self {
            max_connections: 512,
            max_group_members: 4096,
            max_message_bytes: 4 * 1024 * 1024,
            max_publish_bytes: 64 * 1024 * 1024,
            max_read_bytes: 64 * 1024 * 1024,
            channel_capacity: 100,
            command_timeout: Duration::from_secs(3),
            poll_interval: Duration::from_millis(100),
        }
    }
}
#[derive(Clone)]
pub struct RealtimeBus {
    inner: Arc<Inner>,
}
struct Inner {
    transport: RedisTransport,
    config: BusConfig,
    nonlocal: String,
    connections: Arc<Semaphore>,
    publish_bytes: Arc<Semaphore>,
    read_bytes: Arc<Semaphore>,
    cleanup_retry: Arc<Semaphore>,
    receive_lock: Arc<AsyncMutex<()>>,
    receive_waiters: Arc<Semaphore>,
}
pub struct Incoming {
    pub recipients: Vec<String>,
    pub message: Value,
    budget: OwnedSemaphorePermit,
    receive_lock: OwnedMutexGuard<()>,
    transport: RedisTransport,
    backup: String,
    raw: Vec<u8>,
}
impl Incoming {
    /// Commit delivery after it entered the bounded local hub. Dropping without
    /// acknowledgement leaves the original sorted-set member for ordered replay.
    pub async fn ack(self) -> Result<()> {
        ack_raw(
            self.transport,
            self.backup,
            self.raw,
            self.budget,
            self.receive_lock,
        )
        .await
    }
}
async fn ack_raw(
    transport: RedisTransport,
    backup: String,
    raw: Vec<u8>,
    budget: OwnedSemaphorePermit,
    receive_lock: OwnedMutexGuard<()>,
) -> Result<()> {
    let _: (u64, _) = transport
        .command(
            "return redis.call('ZREM',KEYS[1],ARGV[1])",
            &[backup],
            &[raw.as_slice()],
            (budget, receive_lock),
        )
        .await
        .map_err(map)?;
    Ok(())
}
impl RealtimeBus {
    pub async fn connect(url: &str, config: BusConfig) -> Result<Self> {
        if config.max_connections == 0
            || config.max_connections > 4096
            || config.max_group_members == 0
            || config.max_group_members > 65536
            || config.max_message_bytes < 1024
            || config.max_message_bytes > 16 * 1024 * 1024
            || config.max_publish_bytes < config.max_message_bytes * 4
            || config.max_publish_bytes > u32::MAX as usize
            || config.max_read_bytes < config.max_message_bytes
            || config.max_read_bytes > u32::MAX as usize
            || config.channel_capacity == 0
            || config.channel_capacity > 10000
            || config.command_timeout.is_zero()
            || config.poll_interval.is_zero()
        {
            return Err(UpdateEventError::InvalidMessage);
        }
        let transport = RedisTransport::connect(
            url,
            TransportConfig {
                max_operations: 16,
                max_queued_operations: 1024,
                command_timeout: config.command_timeout,
            },
        )
        .await
        .map_err(map)?;
        let inner = Inner {
            transport,
            nonlocal: format!("specific.{}!", uuid::Uuid::new_v4().simple()),
            connections: Arc::new(Semaphore::new(config.max_connections)),
            publish_bytes: Arc::new(Semaphore::new(config.max_publish_bytes)),
            read_bytes: Arc::new(Semaphore::new(config.max_read_bytes)),
            cleanup_retry: Arc::new(Semaphore::new(1)),
            receive_lock: Arc::new(AsyncMutex::new(())),
            receive_waiters: Arc::new(Semaphore::new(16)),
            config,
        };
        Ok(Self {
            inner: Arc::new(inner),
        })
    }
    pub fn nonlocal_name(&self) -> &str {
        &self.inner.nonlocal
    }
    pub fn config(&self) -> &BusConfig {
        &self.inner.config
    }
    pub fn available_connections(&self) -> usize {
        self.inner.connections.available_permits()
    }
    pub fn available_read_bytes(&self) -> usize {
        self.inner.read_bytes.available_permits()
    }
    pub fn reserve_member(&self) -> Result<Membership> {
        let slot = self
            .inner
            .connections
            .clone()
            .try_acquire_owned()
            .map_err(|_| UpdateEventError::Capacity)?;
        Ok(Membership {
            inner: Arc::new(MemberInner {
                id: format!("{}{}", self.inner.nonlocal, uuid::Uuid::new_v4().simple()),
                bus: self.clone(),
                slot: Mutex::new(Some(slot)),
                joins: Arc::new(JoinState {
                    state: Mutex::new(JoinStatus {
                        released: false,
                        pending: 0,
                    }),
                    changed: Notify::new(),
                }),
                release_done: watch::channel(false).0,
            }),
        })
    }
    pub async fn publish(&self, group: &str, message: Value) -> Result<()> {
        if !valid_group(group) || !message.is_object() {
            return Err(UpdateEventError::InvalidMessage);
        }
        let maximum = self.inner.config.max_group_members.to_string();
        let (status, members) = self
            .inner
            .transport
            .command::<(u8, Vec<String>), _>(
                MEMBERS,
                &[format!("asgi:group:{group}")],
                &[now().to_string().as_bytes(), maximum.as_bytes()],
                (),
            )
            .await
            .map_err(map)?
            .0;
        if status != 0 {
            return Err(UpdateEventError::Capacity);
        }
        if members.is_empty() {
            return Ok(());
        }
        let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for member in members {
            if !valid_channel(&member) {
                return Err(UpdateEventError::InvalidMessage);
            }
            let key = member
                .find('!')
                .map_or_else(|| member.clone(), |position| member[..=position].to_owned());
            grouped
                .entry(format!("asgi{key}"))
                .or_default()
                .push(member);
        }
        let base = codec::encode(&message, self.inner.config.max_message_bytes)?;
        let estimated = grouped
            .values()
            .try_fold(0usize, |total, members| {
                total.checked_add(base.len() + 48 + members.len() * 102)
            })
            .ok_or(UpdateEventError::Capacity)?;
        drop(base);
        // RedisTransport owns argument and command-buffer copies until the
        // actual response. Fourfold reservation also covers encoding overlap.
        let reserve = estimated
            .checked_mul(4)
            .filter(|value| *value <= self.inner.config.max_publish_bytes)
            .ok_or(UpdateEventError::Capacity)?;
        let budget = self
            .inner
            .publish_bytes
            .clone()
            .try_acquire_many_owned(reserve as u32)
            .map_err(|_| UpdateEventError::Capacity)?;
        let mut keys = Vec::new();
        let mut payloads = Vec::new();
        for (key, members) in grouped {
            let mut event = message.clone();
            event
                .as_object_mut()
                .unwrap()
                .insert("__asgi_channel__".into(), serde_json::json!(members));
            let encoded = codec::encode(
                &event,
                self.inner.config.max_message_bytes.saturating_sub(12),
            )?;
            let mut member = vec![0; 12];
            rand::rngs::OsRng
                .try_fill_bytes(&mut member)
                .map_err(|_| UpdateEventError::Unavailable)?;
            member.extend_from_slice(&encoded);
            keys.push(key);
            payloads.push(member);
        }
        let time = now().to_string();
        let capacity = self.inner.config.channel_capacity.to_string();
        let mut args = vec![time.as_bytes(), capacity.as_bytes()];
        args.extend(payloads.iter().map(Vec::as_slice));
        let _: (usize, _) = self
            .inner
            .transport
            .command(PUBLISH, &keys, &args, budget)
            .await
            .map_err(map)?;
        Ok(())
    }
    /// Atomic size-check/pop avoids BZPOPMIN materializing an arbitrarily large
    /// external member before admission can reject it. Poll once per process.
    pub async fn pop(&self) -> Result<Option<Incoming>> {
        let waiter = self
            .inner
            .receive_waiters
            .clone()
            .try_acquire_owned()
            .map_err(|_| UpdateEventError::Capacity)?;
        let receive_lock = self.inner.receive_lock.clone().lock_owned().await;
        drop(waiter);
        let budget = self
            .inner
            .read_bytes
            .clone()
            .try_acquire_many_owned(self.inner.config.max_message_bytes as u32)
            .map_err(|_| UpdateEventError::Capacity)?;
        let maximum = self.inner.config.max_message_bytes.to_string();
        let key = format!("asgi{}", self.inner.nonlocal);
        let backup = format!("{key}$inflight");
        let ((status, raw), (mut budget, receive_lock)): ((i8, Vec<u8>), _) = self
            .inner
            .transport
            .command(
                POP,
                &[key, backup.clone()],
                &[now().to_string().as_bytes(), maximum.as_bytes()],
                (budget, receive_lock),
            )
            .await
            .map_err(map)?;
        if status == 0 {
            return Ok(None);
        }
        if status != 1 {
            return Err(UpdateEventError::InvalidMessage);
        }
        let retained = budget
            .split(raw.len())
            .ok_or(UpdateEventError::InvalidMessage)?;
        drop(budget);
        let decoded = (|| {
            if raw.len() < 13 {
                return Err(UpdateEventError::InvalidMessage);
            }
            let mut message = codec::decode(&raw[12..], self.inner.config.max_message_bytes)?;
            let Some(object) = message.as_object_mut() else {
                return Err(UpdateEventError::InvalidMessage);
            };
            let targets = object
                .remove("__asgi_channel__")
                .ok_or(UpdateEventError::InvalidMessage)?;
            let targets = match targets {
                Value::String(target) => vec![Value::String(target)],
                Value::Array(targets) => targets,
                _ => return Err(UpdateEventError::InvalidMessage),
            };
            if targets.len() > self.inner.config.max_group_members {
                return Err(UpdateEventError::InvalidMessage);
            }
            let mut recipients = Vec::new();
            for target in targets {
                let Some(target) = target.as_str() else {
                    return Err(UpdateEventError::InvalidMessage);
                };
                if !valid_channel(target) {
                    return Err(UpdateEventError::InvalidMessage);
                }
                if target.starts_with(&self.inner.nonlocal) {
                    recipients.push(target.to_owned());
                }
            }
            Ok((message, recipients))
        })();
        match decoded {
            Ok((message, recipients)) => Ok(Some(Incoming {
                message,
                recipients,
                budget: retained,
                receive_lock,
                transport: self.inner.transport.clone(),
                backup,
                raw,
            })),
            Err(error) => {
                ack_raw(
                    self.inner.transport.clone(),
                    backup,
                    raw,
                    retained,
                    receive_lock,
                )
                .await?;
                Err(error)
            }
        }
    }
}
impl UpdateEventSink for RealtimeBus {
    fn send<'a>(&'a self, group: &'a str, message: Value) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.publish(group, message))
    }
}
pub struct Membership {
    inner: Arc<MemberInner>,
}
struct MemberInner {
    id: String,
    bus: RealtimeBus,
    slot: Mutex<Option<OwnedSemaphorePermit>>,
    joins: Arc<JoinState>,
    release_done: watch::Sender<bool>,
}
struct JoinState {
    state: Mutex<JoinStatus>,
    changed: Notify,
}
struct JoinStatus {
    released: bool,
    pending: usize,
}
struct PendingJoin {
    state: Arc<JoinState>,
    _member: Arc<MemberInner>,
}
impl Drop for PendingJoin {
    fn drop(&mut self) {
        self.state.state.lock().unwrap().pending -= 1;
        self.state.changed.notify_waiters();
    }
}
struct ReleaseCompletion {
    slot: Option<OwnedSemaphorePermit>,
    done: watch::Sender<bool>,
}
impl Drop for ReleaseCompletion {
    fn drop(&mut self) {
        drop(self.slot.take());
        self.done.send_replace(true);
    }
}
impl Membership {
    pub fn id(&self) -> &str {
        &self.inner.id
    }
    pub async fn join(&self) -> Result<()> {
        let pending = {
            let mut state = self.inner.joins.state.lock().unwrap();
            if state.released {
                return Err(UpdateEventError::Unavailable);
            }
            state.pending += 1;
            PendingJoin {
                state: self.inner.joins.clone(),
                _member: self.inner.clone(),
            }
        };
        let maximum = self.inner.bus.inner.config.max_group_members.to_string();
        let ((status, _), _): ((u8, u8), _) = self
            .inner
            .bus
            .inner
            .transport
            .command(
                JOIN,
                &["asgi:group:updates".into()],
                &[
                    now().to_string().as_bytes(),
                    maximum.as_bytes(),
                    self.inner.id.as_bytes(),
                ],
                pending,
            )
            .await
            .map_err(map)?;
        if self.inner.joins.state.lock().unwrap().released {
            return Err(UpdateEventError::Unavailable);
        }
        if status == 0 {
            Ok(())
        } else {
            Err(UpdateEventError::Capacity)
        }
    }
    pub async fn release(&self) {
        let mut done = self.inner.release_done.subscribe();
        self.inner.begin_release();
        while !*done.borrow_and_update() {
            if done.changed().await.is_err() {
                break;
            }
        }
    }
}
impl MemberInner {
    fn begin_release(&self) {
        {
            let mut state = self.joins.state.lock().unwrap();
            if state.released {
                return;
            }
            state.released = true;
        }
        let Some(slot) = self.slot.lock().unwrap().take() else {
            return;
        };
        let transport = self.bus.inner.transport.clone();
        let retry = self.bus.inner.cleanup_retry.clone();
        let joins = self.joins.clone();
        let id = self.id.clone();
        let completion = Arc::new(ReleaseCompletion {
            slot: Some(slot),
            done: self.release_done.clone(),
        });
        // A release cannot overtake an admitted, delayed JOIN. Its marker stays
        // pending until the real Redis reply, including caller cancellation.
        tokio::spawn(async move {
            loop {
                let changed = joins.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if joins.state.lock().unwrap().pending == 0 {
                    break;
                }
                changed.await;
            }
            loop {
                match transport
                    .command::<u64, _>(
                        DISCARD,
                        &["asgi:group:updates".into()],
                        &[id.as_bytes()],
                        completion.clone(),
                    )
                    .await
                {
                    Ok(_) => break,
                    Err(_) => {
                        // A detached physical command still owns completion;
                        // leave admission with it until its actual reply.
                        if Arc::strong_count(&completion) > 1 {
                            break;
                        }
                        // Queue admission failures have no physical command.
                        // Keep the connection lease and retry conservatively;
                        // all failed cleanup tasks share this one backoff lane.
                        let _retry = retry.acquire().await.unwrap();
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
            }
        });
    }
}
impl Drop for MemberInner {
    fn drop(&mut self) {
        if tokio::runtime::Handle::try_current().is_ok() {
            self.begin_release();
        }
    }
}
fn valid_group(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 100
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
}
fn valid_channel(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 100
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.!".contains(&byte))
        && name.bytes().filter(|byte| *byte == b'!').count() <= 1
}
fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
fn map(error: dispatcharr_output_store::Error) -> UpdateEventError {
    match error {
        dispatcharr_output_store::Error::Capacity => UpdateEventError::Capacity,
        _ => UpdateEventError::Unavailable,
    }
}
const MEMBERS: &str = r#"
local t=redis.call('TYPE',KEYS[1]).ok;if t~='none' and t~='zset' then return redis.error_reply('CORRUPT group type') end
redis.call('ZREMRANGEBYSCORE',KEYS[1],0,tonumber(ARGV[1])-86400)
if redis.call('ZCARD',KEYS[1])>tonumber(ARGV[2]) then return {1,{}} end
local rows=redis.call('ZRANGE',KEYS[1],0,-1);for _,member in ipairs(rows) do if #member>100 then return redis.error_reply('CORRUPT channel name') end end
return {0,rows}
"#;
const JOIN: &str = r#"
local t=redis.call('TYPE',KEYS[1]).ok;if t~='none' and t~='zset' then return redis.error_reply('CORRUPT group type') end
redis.call('ZREMRANGEBYSCORE',KEYS[1],0,tonumber(ARGV[1])-86400)
if not redis.call('ZSCORE',KEYS[1],ARGV[3]) and redis.call('ZCARD',KEYS[1])>=tonumber(ARGV[2]) then return {1,0} end
redis.call('ZADD',KEYS[1],ARGV[1],ARGV[3]);redis.call('EXPIRE',KEYS[1],86400);return {0,1}
"#;
const DISCARD: &str = "return redis.call('ZREM',KEYS[1],ARGV[1])";
const PUBLISH: &str = r#"
for _,key in ipairs(KEYS) do local t=redis.call('TYPE',key).ok;if t~='none' and t~='zset' then return redis.error_reply('CORRUPT channel type') end end
local dropped=0;for index,key in ipairs(KEYS) do
 redis.call('ZREMRANGEBYSCORE',key,0,tonumber(ARGV[1])-60)
 if redis.call('ZCARD',key)<tonumber(ARGV[2]) then redis.call('ZADD',key,ARGV[1],ARGV[index+2]);redis.call('EXPIRE',key,60) else dropped=dropped+1 end
end;return dropped
"#;
const POP: &str = r#"
for _,key in ipairs(KEYS) do local t=redis.call('TYPE',key).ok;if t~='none' and t~='zset' then return redis.error_reply('CORRUPT channel type') end end
if redis.call('ZCARD',KEYS[2])>1 then return redis.error_reply('CORRUPT inflight count') end
local old=redis.call('ZRANGE',KEYS[2],0,0,'WITHSCORES')
if #old>0 then
 redis.call('DEL',KEYS[2]);if #old[1]>tonumber(ARGV[2]) then return {-1,''} end
 redis.call('ZADD',KEYS[1],old[2],old[1])
end
redis.call('ZREMRANGEBYSCORE',KEYS[1],0,tonumber(ARGV[1])-60)
local rows=redis.call('ZRANGE',KEYS[1],0,0,'WITHSCORES');if #rows==0 then return {0,''} end
redis.call('ZREM',KEYS[1],rows[1]);if #rows[1]>tonumber(ARGV[2]) then return {-1,''} end
redis.call('ZADD',KEYS[2],rows[2],rows[1]);redis.call('EXPIRE',KEYS[2],60)
return {1,rows[1]}
"#;
