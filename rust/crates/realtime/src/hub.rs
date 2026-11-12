//! One bounded serialized event ring per process, shared by all local sockets.
use crate::policy::{visibility, Visibility};
use bytes::Bytes;
use dispatcharr_contracts::UpdateEventError;
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{watch, Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug)]
pub struct HubConfig {
    pub max_events: usize,
    pub max_buffered_bytes: usize,
    pub max_live_bytes: usize,
    pub max_wire_bytes: usize,
    pub capacity_wait: Duration,
}
impl Default for HubConfig {
    fn default() -> Self {
        Self {
            max_events: 100,
            max_buffered_bytes: 32 * 1024 * 1024,
            max_live_bytes: 64 * 1024 * 1024,
            max_wire_bytes: 25 * 1024 * 1024,
            capacity_wait: Duration::from_secs(2),
        }
    }
}
pub struct Hub {
    state: Mutex<State>,
    changed: Notify,
    activity: Arc<Notify>,
    bytes: Arc<Semaphore>,
    config: HubConfig,
    publisher: AsyncMutex<()>,
}

pub(super) fn serialize(message: &Value, maximum: usize) -> Result<Box<[u8]>, UpdateEventError> {
    let mut output = BoundedWriter {
        data: Vec::new(),
        maximum,
    };
    serde_json::to_writer(&mut output, message).map_err(|_| UpdateEventError::InvalidMessage)?;
    Ok(output.data.into_boxed_slice())
}
pub(super) fn leased_wire(data: Box<[u8]>, budget: OwnedSemaphorePermit) -> Bytes {
    Bytes::from_owner(WireOwner {
        data,
        _budget: budget,
    })
}
struct State {
    events: VecDeque<Arc<Packet>>,
    retained: usize,
    next: u64,
    clients: HashMap<String, watch::Sender<bool>>,
    closed: bool,
}
pub struct Packet {
    pub sequence: u64,
    pub wire: Bytes,
    pub visibility: Visibility,
    recipients: Box<[String]>,
    weight: usize,
}
struct WireOwner {
    data: Box<[u8]>,
    _budget: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for WireOwner {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
pub struct Subscription {
    hub: Arc<Hub>,
    id: String,
    cursor: u64,
    stop: watch::Receiver<bool>,
}
pub enum Delivery {
    Packet(Arc<Packet>),
    Lagged,
    Stopped,
}
impl Hub {
    pub fn new(config: HubConfig) -> Result<Arc<Self>, UpdateEventError> {
        if config.max_events == 0
            || config.max_events > 4096
            || config.max_wire_bytes == 0
            || config.max_buffered_bytes < config.max_wire_bytes
            || config.max_live_bytes < config.max_buffered_bytes
            || config.max_live_bytes > u32::MAX as usize
            || config.capacity_wait.is_zero()
        {
            return Err(UpdateEventError::InvalidMessage);
        }
        Ok(Arc::new(Self {
            bytes: Arc::new(Semaphore::new(config.max_live_bytes)),
            config,
            state: Mutex::new(State {
                events: VecDeque::new(),
                retained: 0,
                next: 1,
                clients: HashMap::new(),
                closed: false,
            }),
            changed: Notify::new(),
            activity: Arc::new(Notify::new()),
            publisher: AsyncMutex::new(()),
        }))
    }
    pub fn register(self: &Arc<Self>, id: String) -> Result<Subscription, UpdateEventError> {
        let mut state = self.state.lock().unwrap();
        if state.closed || state.clients.contains_key(&id) {
            return Err(UpdateEventError::Unavailable);
        }
        let (sender, stop) = watch::channel(false);
        state.clients.insert(id.clone(), sender);
        let cursor = state.next;
        drop(state);
        self.activity.notify_waiters();
        Ok(Subscription {
            hub: self.clone(),
            id,
            cursor,
            stop,
        })
    }
    pub fn clients(&self) -> usize {
        self.state.lock().unwrap().clients.len()
    }
    pub fn live_bytes(&self) -> usize {
        self.config.max_live_bytes - self.bytes.available_permits()
    }
    pub fn buffered_bytes(&self) -> usize {
        self.state.lock().unwrap().retained
    }
    pub fn activity(&self) -> Arc<Notify> {
        self.activity.clone()
    }
    /// Encode once. Metadata and every outstanding wire version keep the same
    /// global byte lease until its final socket/frame reference disappears.
    pub async fn publish(
        &self,
        message: &Value,
        mut recipients: Vec<String>,
    ) -> Result<(), UpdateEventError> {
        let _publisher = self.publisher.lock().await;
        {
            let state = self.state.lock().unwrap();
            if state.closed {
                return Err(UpdateEventError::Unavailable);
            }
            recipients.retain(|id| state.clients.contains_key(id));
        }
        recipients.sort_unstable();
        recipients.dedup();
        if recipients.is_empty() {
            return Ok(());
        }
        let wire = serialize(message, self.config.max_wire_bytes)?;
        let recipients = recipients.into_boxed_slice();
        let weight = wire.len()
            + recipients.len() * std::mem::size_of::<String>()
            + recipients.iter().map(String::capacity).sum::<usize>()
            + 256;
        if weight > self.config.max_buffered_bytes {
            return Err(UpdateEventError::Capacity);
        }
        self.evict_for(weight, false);
        let budget = match self.bytes.clone().try_acquire_many_owned(weight as u32) {
            Ok(budget) => budget,
            Err(_) => {
                // Evict buffered versions before waiting. Otherwise a full
                // ring could itself retain every permit needed for progress.
                self.evict_for(weight, true);
                tokio::time::timeout(
                    self.config.capacity_wait,
                    self.bytes.clone().acquire_many_owned(weight as u32),
                )
                .await
                .map_err(|_| UpdateEventError::Capacity)?
                .map_err(|_| UpdateEventError::Unavailable)?
            }
        };
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(UpdateEventError::Unavailable);
        }
        if !recipients.iter().any(|id| state.clients.contains_key(id)) {
            return Ok(());
        }
        let sequence = state.next;
        state.next = state
            .next
            .checked_add(1)
            .ok_or(UpdateEventError::Unavailable)?;
        let wire = Bytes::from_owner(WireOwner {
            data: wire,
            _budget: budget,
        });
        state.events.push_back(Arc::new(Packet {
            sequence,
            wire,
            visibility: visibility(message),
            recipients,
            weight,
        }));
        state.retained += weight;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }
    fn evict_for(&self, weight: usize, all: bool) {
        let mut evicted = Vec::new();
        {
            let mut state = self.state.lock().unwrap();
            while !state.events.is_empty()
                && (all
                    || state.events.len() >= self.config.max_events
                    || state.retained + weight > self.config.max_buffered_bytes)
            {
                let event = state.events.pop_front().unwrap();
                state.retained -= event.weight;
                evicted.push(event);
            }
        }
        drop(evicted);
    }
    pub fn shutdown(&self) {
        let (senders, events) = {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            state.retained = 0;
            (
                state.clients.values().cloned().collect::<Vec<_>>(),
                std::mem::take(&mut state.events),
            )
        };
        drop(events);
        for sender in senders {
            sender.send_replace(true);
        }
        self.changed.notify_waiters();
        self.activity.notify_waiters();
        self.bytes.close();
    }
}

struct BoundedWriter {
    data: Vec<u8>,
    maximum: usize,
}
impl std::io::Write for BoundedWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if data.len() > self.maximum.saturating_sub(self.data.len()) {
            return Err(std::io::Error::other("wire budget exceeded"));
        }
        self.data.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Subscription {
    pub fn stopped(&self) -> watch::Receiver<bool> {
        self.stop.clone()
    }
    pub async fn next(&mut self) -> Delivery {
        loop {
            let changed = self.hub.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.hub.state.lock().unwrap();
                if state.closed {
                    return Delivery::Stopped;
                }
                let oldest = state
                    .events
                    .front()
                    .map_or(state.next, |event| event.sequence);
                if self.cursor < oldest {
                    self.cursor = oldest;
                    return Delivery::Lagged;
                }
                while let Some(event) = usize::try_from(self.cursor - oldest)
                    .ok()
                    .and_then(|offset| state.events.get(offset))
                {
                    self.cursor = event.sequence + 1;
                    if event.recipients.binary_search(&self.id).is_ok() {
                        return Delivery::Packet(event.clone());
                    }
                }
            }
            changed.await;
        }
    }
}
impl Drop for Subscription {
    fn drop(&mut self) {
        let events = {
            let mut state = self.hub.state.lock().unwrap();
            state.clients.remove(&self.id);
            if state.clients.is_empty() {
                state.retained = 0;
                std::mem::take(&mut state.events)
            } else {
                VecDeque::new()
            }
        };
        drop(events);
        self.hub.activity.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn config() -> HubConfig {
        HubConfig {
            max_events: 2,
            max_buffered_bytes: 4096,
            max_live_bytes: 8192,
            max_wire_bytes: 3000,
            capacity_wait: Duration::from_millis(50),
        }
    }
    #[tokio::test]
    async fn hundred_local_readers_share_one_wire_owner_and_targets_are_exact() {
        let hub = Hub::new(config()).unwrap();
        let mut readers = Vec::new();
        let ids: Vec<String> = (0..100).map(|n| format!("connection-{n}")).collect();
        for id in &ids {
            readers.push(hub.register(id.clone()).unwrap());
        }
        hub.publish(
            &json!({"type":"update","data":{"value":"one"}}),
            vec![ids[0].clone(), ids[99].clone()],
        )
        .await
        .unwrap();
        let Delivery::Packet(first) = readers[0].next().await else {
            panic!("missing first")
        };
        let Delivery::Packet(last) = readers[99].next().await else {
            panic!("missing last")
        };
        assert_eq!(first.wire.as_ptr(), last.wire.as_ptr());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), readers[1].next())
                .await
                .is_err()
        );
        let wire = first.wire.clone();
        drop(first);
        drop(last);
        drop(readers);
        assert_eq!(hub.buffered_bytes(), 0);
        assert!(hub.live_bytes() > 0);
        drop(wire);
        assert_eq!(hub.live_bytes(), 0);
    }
    #[tokio::test]
    async fn slow_cursor_lags_without_blocking_fast_reader_or_retaining_old_ring() {
        let hub = Hub::new(config()).unwrap();
        let mut slow = hub.register("slow".into()).unwrap();
        let mut fast = hub.register("fast".into()).unwrap();
        for number in 0..20 {
            hub.publish(
                &json!({"type":"update","data":{"number":number}}),
                vec!["slow".into(), "fast".into()],
            )
            .await
            .unwrap();
            let Delivery::Packet(event) = fast.next().await else {
                panic!("fast missing")
            };
            assert_eq!(
                serde_json::from_slice::<Value>(&event.wire).unwrap()["data"]["number"],
                number
            );
            assert!(hub.buffered_bytes() <= 4096 && hub.live_bytes() <= 8192);
        }
        assert!(matches!(slow.next().await, Delivery::Lagged));
        hub.shutdown();
        assert!(matches!(fast.next().await, Delivery::Stopped));
    }

    #[tokio::test]
    async fn final_unsubscribe_during_byte_wait_cannot_leave_an_idle_retained_event() {
        let hub = Hub::new(HubConfig {
            max_live_bytes: 4096,
            ..config()
        })
        .unwrap();
        let mut subscriber = hub.register("one".into()).unwrap();
        hub.publish(
            &json!({"type":"update","data":"x".repeat(2000)}),
            vec!["one".into()],
        )
        .await
        .unwrap();
        let Delivery::Packet(held) = subscriber.next().await else {
            panic!("held event missing")
        };
        let pending = hub.clone();
        let task = tokio::spawn(async move {
            pending
                .publish(
                    &json!({"type":"update","data":"y".repeat(2000)}),
                    vec!["one".into()],
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(!task.is_finished());
        drop(subscriber);
        drop(held);
        task.await.unwrap().unwrap();
        assert_eq!(hub.buffered_bytes(), 0);
        assert_eq!(hub.live_bytes(), 0);
    }
}
