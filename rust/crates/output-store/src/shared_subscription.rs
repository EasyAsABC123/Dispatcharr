//! One physically owned result subscriber, shared by logical plugin PubSubs.
//! Admission comes from the existing RedisTransport, never another pool. A
//! blocking logical read owns only its queue/Notify and cannot block SQL,
//! unsubscribe, another callback, or the three remaining result-operation slots.
//! Original Redis/Celery/Dispatcharr authors retain all protocol credit.
use super::bounded_subscription::{Error, Receipt, Subscription};
use redis::Value;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch, Notify};
type Owner = Arc<dyn Send + Sync>;
const CLIENTS: usize = 64;
const TOPICS: usize = 1024;
const PER_CLIENT_BYTES: usize = 2 * 1024 * 1024;
const TOTAL_BYTES: usize = 16 * 1024 * 1024;
const CLIENT_MESSAGES: usize = 256;
/// Logical acknowledgements expose their actual upstream basis. A shared topic
/// is not physically unsubscribed while another client still owns a reference.
#[derive(Clone, Debug)]
pub enum Confirmation {
    ActualSubscribe,
    ActualUnsubscribe,
    RetainedSharedReference,
}
#[derive(Clone, Debug)]
pub struct Message {
    pub value: Arc<Value>,
    pub confirmation: Option<Confirmation>,
    bytes: usize,
}
struct Queue {
    values: VecDeque<Message>,
    bytes: usize,
    error: Option<Error>,
}
struct State {
    queue: Mutex<Queue>,
    wake: Notify,
    total: Arc<AtomicUsize>,
    owner: Owner,
    ignore_subscribe_messages: bool,
}
impl State {
    fn push(&self, value: Message) -> Result<(), Error> {
        let mut q = self.queue.lock().map_err(|_| Error::Closed)?;
        if q.error.is_some() {
            return Err(Error::Closed);
        }
        let previous = self.total.fetch_add(value.bytes, Ordering::SeqCst);
        if q.values.len() >= CLIENT_MESSAGES
            || q.bytes + value.bytes > PER_CLIENT_BYTES
            || previous + value.bytes > TOTAL_BYTES
        {
            self.total.fetch_sub(value.bytes, Ordering::SeqCst);
            q.error = Some(Error::Capacity);
            drop(q);
            self.wake.notify_waiters();
            return Err(Error::Capacity);
        }
        q.bytes += value.bytes;
        q.values.push_back(value);
        drop(q);
        self.wake.notify_one();
        Ok(())
    }
    fn fail(&self, error: Error) {
        if let Ok(mut q) = self.queue.lock() {
            q.error = Some(error);
            self.total.fetch_sub(q.bytes, Ordering::SeqCst);
            q.bytes = 0;
            q.values.clear();
        }
        self.wake.notify_waiters();
    }
    async fn read(&self, timeout: Option<Duration>) -> Result<Option<Message>, Error> {
        let work = async {
            loop {
                let wake = self.wake.notified();
                tokio::pin!(wake);
                wake.as_mut().enable();
                {
                    let mut q = self.queue.lock().map_err(|_| Error::Closed)?;
                    if let Some(error) = q.error.clone() {
                        return Err(error);
                    }
                    if let Some(value) = q.values.pop_front() {
                        q.bytes -= value.bytes;
                        self.total.fetch_sub(value.bytes, Ordering::SeqCst);
                        return Ok(Some(value));
                    }
                }
                wake.await;
            }
        };
        match timeout {
            Some(t) if t.is_zero() => {
                let mut q = self.queue.lock().map_err(|_| Error::Closed)?;
                if let Some(error) = q.error.clone() {
                    return Err(error);
                }
                Ok(q.values.pop_front().inspect(|v| {
                    q.bytes -= v.bytes;
                    self.total.fetch_sub(v.bytes, Ordering::SeqCst);
                }))
            }
            Some(t) => match tokio::time::timeout(t, work).await {
                Ok(value) => value,
                Err(_) => Ok(None),
            },
            None => work.await,
        }
    }
}
impl Drop for State {
    fn drop(&mut self) {
        if let Ok(q) = self.queue.lock() {
            self.total.fetch_sub(q.bytes, Ordering::SeqCst);
        }
    }
}
struct Entry {
    state: Arc<State>,
    topics: BTreeSet<Vec<u8>>,
}
enum Request {
    Open {
        state: Arc<State>,
        hub: SharedSubscription,
        reply: oneshot::Sender<Result<LogicalSubscription, Error>>,
    },
    Topics {
        id: u64,
        subscribe: bool,
        topics: Vec<Vec<u8>>,
        owner: Owner,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Close {
        id: u64,
        reply: Option<oneshot::Sender<Result<(), Error>>>,
    },
}
struct Inner {
    sender: mpsc::Sender<Request>,
    stop: watch::Sender<bool>,
    closed: Arc<AtomicBool>,
    done: watch::Receiver<Option<Result<Receipt, Error>>>,
    total: Arc<AtomicUsize>,
    close_timeout: Duration,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
#[derive(Clone)]
pub struct SharedSubscription {
    inner: Arc<Inner>,
}
struct ClientInner {
    id: u64,
    hub: SharedSubscription,
    state: Arc<State>,
    closed: AtomicBool,
}
impl Drop for ClientInner {
    fn drop(&mut self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let sender = self.hub.inner.sender.clone();
        let id = self.id;
        let owner = self.state.owner.clone();
        // Bounded client population limits these cleanup senders. Owner is retained
        // until the actor observes removal/physical unsubscribe, even after caller loss.
        tokio::spawn(async move {
            let _owner = owner;
            let (reply, wait) = oneshot::channel();
            if sender
                .send(Request::Close {
                    id,
                    reply: Some(reply),
                })
                .await
                .is_ok()
            {
                let _ = wait.await;
            }
        });
    }
}
#[derive(Clone)]
pub struct LogicalSubscription {
    inner: Arc<ClientInner>,
}
fn parts(value: &Value) -> Result<(&[u8], &[u8]), Error> {
    let Value::Array(values) = value else {
        return Err(Error::Protocol);
    };
    let [Value::BulkString(kind), Value::BulkString(topic), _] = values.as_slice() else {
        return Err(Error::Protocol);
    };
    Ok((kind, topic))
}
fn message(value: Value, confirmation: Option<Confirmation>) -> Message {
    // Physical parser already enforces before-allocation bounds. The logical
    // queue charges shared payload bytes conservatively for every recipient.
    fn bytes(value: &Value) -> usize {
        match value {
            Value::BulkString(v) => v.len() + 16,
            Value::Array(v) => 16 + v.iter().map(bytes).sum::<usize>(),
            _ => 32,
        }
    }
    let size = bytes(&value);
    Message {
        value: Arc::new(value),
        confirmation,
        bytes: size,
    }
}
fn logical_ack(kind: &[u8], topic: Vec<u8>, count: usize, confirmation: Confirmation) -> Message {
    message(
        Value::Array(vec![
            Value::BulkString(kind.to_vec()),
            Value::BulkString(topic),
            Value::Int(count as i64),
        ]),
        Some(confirmation),
    )
}
fn deliver(value: Value, clients: &BTreeMap<u64, Entry>) -> Result<(), Error> {
    let (kind, topic) = parts(&value)?;
    if kind != b"message" {
        return Err(Error::Protocol);
    }
    let topic = topic.to_vec();
    let value = message(value, None);
    for client in clients.values().filter(|c| c.topics.contains(&topic)) {
        // Overflow remains a visible fault on that actual logical client. It is
        // never translated to a Source timeout/non-result; other clients continue.
        let _ = client.state.push(value.clone());
    }
    Ok(())
}
async fn actual_ack(
    physical: &Subscription,
    kind: &[u8],
    topic: &[u8],
    clients: &BTreeMap<u64, Entry>,
    owner: Owner,
    timeout: Duration,
    expected: usize,
) -> Result<(), Error> {
    let started = tokio::time::Instant::now();
    loop {
        let left = timeout
            .checked_sub(started.elapsed())
            .ok_or(Error::Deadline)?;
        let value = physical
            .read(Some(left), owner.clone())
            .await?
            .ok_or(Error::Deadline)?;
        let (actual_kind, actual_topic) = parts(&value)?;
        if actual_kind == b"message" {
            deliver(value, clients)?;
            continue;
        }
        if actual_kind != kind || actual_topic != topic {
            return Err(Error::Protocol);
        }
        let Value::Array(items) = value else {
            return Err(Error::Protocol);
        };
        if !matches!(items.get(2),Some(Value::Int(n))if *n==expected as i64) {
            return Err(Error::Protocol);
        }
        return Ok(());
    }
}
// Explicit physical socket, fanout state, logical owner and emit policy stay
// separate here so no hidden client/owner or second admission is manufactured.
#[allow(clippy::too_many_arguments)]
async fn alter(
    physical: &Subscription,
    clients: &mut BTreeMap<u64, Entry>,
    id: u64,
    subscribe: bool,
    topics: Vec<Vec<u8>>,
    owner: Owner,
    timeout: Duration,
    emit: bool,
) -> Result<(), Error> {
    if topics.len() > TOPICS
        || topics.iter().any(|t| t.len() > 1024)
        || topics.iter().map(Vec::len).sum::<usize>() > 65536
    {
        return Err(Error::Capacity);
    }
    let existing = clients.get(&id).ok_or(Error::Closed)?;
    if !subscribe && topics.is_empty() && existing.topics.is_empty() {
        return Err(Error::Unsupported);
    }
    let topics = if !subscribe && topics.is_empty() {
        existing.topics.iter().cloned().collect()
    } else {
        topics
    };
    if subscribe && topics.is_empty() {
        return Err(Error::Protocol);
    }
    for topic in topics {
        let all = clients
            .values()
            .flat_map(|c| c.topics.iter())
            .collect::<BTreeSet<_>>();
        if subscribe && !all.contains(&topic) && all.len() >= TOPICS {
            return Err(Error::Capacity);
        }
        let another = clients
            .iter()
            .any(|(other, c)| *other != id && c.topics.contains(&topic));
        let confirmation = if subscribe {
            // Even an already-shared subscribe waits for a fresh ACTUAL Redis ack.
            physical
                .command(vec![b"SUBSCRIBE".to_vec(), topic.clone()], owner.clone())
                .await?;
            actual_ack(
                physical,
                b"subscribe",
                &topic,
                clients,
                owner.clone(),
                timeout,
                all.len() + usize::from(!all.contains(&topic)),
            )
            .await?;
            Confirmation::ActualSubscribe
        } else if !another {
            physical
                .command(vec![b"UNSUBSCRIBE".to_vec(), topic.clone()], owner.clone())
                .await?;
            actual_ack(
                physical,
                b"unsubscribe",
                &topic,
                clients,
                owner.clone(),
                timeout,
                all.len() - usize::from(all.contains(&topic)),
            )
            .await?;
            Confirmation::ActualUnsubscribe
        } else {
            Confirmation::RetainedSharedReference
        };
        let client = clients.get_mut(&id).ok_or(Error::Closed)?;
        if subscribe {
            client.topics.insert(topic.clone());
        } else {
            client.topics.remove(&topic);
        }
        if emit && !client.state.ignore_subscribe_messages {
            client.state.push(logical_ack(
                if subscribe {
                    b"subscribe"
                } else {
                    b"unsubscribe"
                },
                topic,
                client.topics.len(),
                confirmation,
            ))?;
        }
    }
    Ok(())
}
impl SharedSubscription {
    pub(super) fn spawn(physical: Subscription, timeout: Duration) -> Self {
        let (sender, mut requests) = mpsc::channel(64);
        let (stop, mut stopping) = watch::channel(false);
        let (done, receipt) = watch::channel(None);
        let closed = Arc::new(AtomicBool::new(false));
        let active = closed.clone();
        let total = Arc::new(AtomicUsize::new(0));
        tokio::spawn(async move {
            let mut clients = BTreeMap::<u64, Entry>::new();
            let mut next = 1_u64;
            let outcome=async{loop{tokio::select!{
    biased;
    changed=stopping.changed()=>{if changed.is_err()||*stopping.borrow(){break Ok(())}},
    request=requests.recv()=>{let Some(request)=request else{break Ok(())};match request{
     Request::Open{state,hub,reply}=>{if clients.len()>=CLIENTS{let _=reply.send(Err(Error::Capacity));}else{
      let id=next;next=next.checked_add(1).ok_or(Error::Capacity)?;clients.insert(id,Entry{state:state.clone(),topics:BTreeSet::new()});
      let client=LogicalSubscription{inner:Arc::new(ClientInner{id,hub,state,closed:AtomicBool::new(false)})};
      let _=reply.send(Ok(client));
     }},
     Request::Topics{id,subscribe,topics,owner,reply}=>{
      let result=alter(&physical,&mut clients,id,subscribe,topics,owner,timeout,true).await;
      let fatal=result.as_ref().err().filter(|e|!matches!(e,Error::Capacity|Error::Closed|Error::Unsupported)).cloned();let _=reply.send(result);
      if let Some(error)=fatal{break Err(error)}
     },
     Request::Close{id,reply}=>{
      if let Some(entry)=clients.get(&id){let owner=entry.state.owner.clone();let result=if clients[&id].topics.is_empty(){Ok(())}else{alter(&physical,&mut clients,id,false,Vec::new(),owner,timeout,false).await};
       let removed=clients.remove(&id);if let Some(removed)=removed{removed.state.fail(Error::Closed)}
       let failed=result.clone().err();if let Some(reply)=reply{let _=reply.send(result);}
       if let Some(error)=failed{break Err(error)}
      }else if let Some(reply)=reply{let _=reply.send(Ok(()));}
     },
    }},
    incoming=physical.read(None,Arc::new(()))=>{match incoming{Ok(Some(value))=>deliver(value,&clients)?,_=>break Err(Error::Closed)}},
   }}}.await;
            active.store(true, Ordering::SeqCst);
            requests.close();
            let error = outcome.err().unwrap_or(Error::Closed);
            for client in clients.values() {
                client.state.fail(error.clone())
            }
            // Owners and logical client state stay retained until the actual single
            // physical reader/socket retirement settles, not merely until stop is sent.
            let result = physical.close(timeout).await;
            done.send_replace(Some(result));
            drop(clients);
        });
        Self {
            inner: Arc::new(Inner {
                sender,
                stop,
                closed,
                done: receipt,
                total,
                close_timeout: timeout,
            }),
        }
    }
    pub fn closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }
    pub async fn open(
        &self,
        owner: Owner,
        ignore_subscribe_messages: bool,
    ) -> Result<LogicalSubscription, Error> {
        if self.closed() {
            return Err(Error::Closed);
        }
        // Never wait on a persistent subscription while holding another Redis slot.
        let state = Arc::new(State {
            queue: Mutex::new(Queue {
                values: VecDeque::new(),
                bytes: 0,
                error: None,
            }),
            wake: Notify::new(),
            total: self.inner.total.clone(),
            owner,
            ignore_subscribe_messages,
        });
        let (reply, wait) = oneshot::channel();
        self.inner
            .sender
            .send(Request::Open {
                state,
                hub: self.clone(),
                reply,
            })
            .await
            .map_err(|_| Error::Closed)?;
        wait.await.map_err(|_| Error::Closed)?
    }
    pub fn begin_close(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
        self.inner.stop.send_replace(true);
    }
    pub async fn close(&self, timeout: Duration) -> Result<Receipt, Error> {
        self.begin_close();
        let mut done = self.inner.done.clone();
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(value) = done.borrow_and_update().clone() {
                    return value;
                }
                done.changed().await.map_err(|_| Error::Closed)?;
            }
        })
        .await
        .map_err(|_| Error::Deadline)?
    }
}
impl LogicalSubscription {
    pub async fn subscribe(&self, topics: Vec<Vec<u8>>, owner: Owner) -> Result<(), Error> {
        self.topics(true, topics, owner).await
    }
    pub async fn unsubscribe(&self, topics: Vec<Vec<u8>>, owner: Owner) -> Result<(), Error> {
        self.topics(false, topics, owner).await
    }
    async fn topics(
        &self,
        subscribe: bool,
        topics: Vec<Vec<u8>>,
        owner: Owner,
    ) -> Result<(), Error> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        let (reply, wait) = oneshot::channel();
        self.inner
            .hub
            .inner
            .sender
            .send(Request::Topics {
                id: self.inner.id,
                subscribe,
                topics,
                owner,
                reply,
            })
            .await
            .map_err(|_| Error::Closed)?;
        wait.await.map_err(|_| Error::Closed)?
    }
    pub async fn read(
        &self,
        timeout: Option<Duration>,
        owner: Owner,
    ) -> Result<Option<Message>, Error> {
        let _owner = owner;
        self.inner.state.read(timeout).await
    }
    pub async fn close(&self) -> Result<(), Error> {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        if self.inner.hub.closed() {
            let receipt = self
                .inner
                .hub
                .close(self.inner.hub.inner.close_timeout)
                .await?;
            if receipt.reader_joined && receipt.socket_retired {
                return Ok(());
            }
            return Err(Error::Closed);
        }
        let (reply, wait) = oneshot::channel();
        self.inner
            .hub
            .inner
            .sender
            .send(Request::Close {
                id: self.inner.id,
                reply: Some(reply),
            })
            .await
            .map_err(|_| Error::Closed)?;
        wait.await.map_err(|_| Error::Closed)?
    }
}
