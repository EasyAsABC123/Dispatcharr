//! Source live:events output-profile owner requests. Original authors retain credit.
//! One application-scoped reader; all writes share OutputStore's admitted transport.
use super::{Error, OutputStore, RemoteSwitchWake};
use serde_json::{json, Value};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::watch,
    time,
};

const MAX_PAYLOAD: usize = 256 * 1024;
const PATTERN: &[u8] = b"live:events:*";
const MAX_CHANNEL: usize = 256;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProfileEventState {
    Prepared,
    Connecting,
    Subscribed,
    Stopped,
    Fault,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputProfileRequest {
    pub channel: String,
    pub profile_id: i64,
    pub command: Vec<String>,
}
impl OutputProfileRequest {
    fn validate(&self) -> Result<(), Error> {
        if self.channel.is_empty()
            || self.channel.len() > MAX_CHANNEL
            || self.channel.contains(':')
            || self.channel.contains('\0')
            || self.profile_id <= 0
            || self.command.is_empty()
            || self.command.len() > 4096
            || self.command[0].is_empty()
            || self.command.iter().any(|v| v.contains('\0'))
            || self
                .command
                .iter()
                .try_fold(0usize, |n, v| n.checked_add(v.len()))
                .is_none_or(|n| n > MAX_PAYLOAD)
        {
            return Err(Error::InvalidKey);
        }
        Ok(())
    }
    #[cfg(test)]
    fn decode(topic: &[u8], raw: &[u8]) -> Result<Option<Self>, Error> {
        let value = event_value(raw)?;
        Self::from_value(topic, &value)
    }
    fn from_value(topic: &[u8], value: &Value) -> Result<Option<Self>, Error> {
        if value["event"] != "ensure_output_profile" {
            return Ok(None);
        }
        let channel = value["channel_id"]
            .as_str()
            .ok_or(Error::Corrupt)?
            .to_owned();
        let profile_id = value["profile_id"].as_i64().ok_or(Error::Corrupt)?;
        let argv = value["command"].as_array().ok_or(Error::Corrupt)?;
        if argv.len() > 4096 {
            return Err(Error::Capacity);
        }
        let command = argv
            .iter()
            .map(|v| v.as_str().map(str::to_owned).ok_or(Error::Corrupt))
            .collect::<Result<Vec<_>, _>>()?;
        let request = Self {
            channel,
            profile_id,
            command,
        };
        request.validate()?;
        if topic != format!("live:events:{}", request.channel).as_bytes() {
            return Err(Error::Corrupt);
        }
        Ok(Some(request))
    }
}
fn event_value(raw: &[u8]) -> Result<Value, Error> {
    if raw.len() > MAX_PAYLOAD {
        return Err(Error::Capacity);
    }
    serde_json::from_slice(raw).map_err(|_| Error::Corrupt)
}

/// The consumer must independently inspect its real local raw-input session and
/// current Redis owner. This envelope grants no TS-owner or process authority.
pub trait OutputProfileOwner: Send + Sync + 'static {
    fn ensure_owned(
        &self,
        request: OutputProfileRequest,
        owner: Arc<dyn Send + Sync>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>>;

    /// The wake frame is only a hint. Receivers must independently claim
    /// the exact pending request on the current raw owner before applying it.
    fn switch_source_owned(
        &self,
        _wake: RemoteSwitchWake,
        _owner: Arc<dyn Send + Sync>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}
struct StopOnLastHandle(watch::Sender<bool>);
impl Drop for StopOnLastHandle {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}
struct Inner {
    store: OutputStore,
    address: redis::ConnectionInfo,
    started: AtomicBool,
    state: watch::Sender<ProfileEventState>,
    done: watch::Sender<bool>,
    stop: watch::Sender<bool>,
    observed: AtomicU64,
    applied: AtomicU64,
    ignored: AtomicU64,
    rejected: AtomicU64,
}
#[derive(Clone)]
pub struct ProfileEvents {
    inner: Arc<Inner>,
    _handles: Arc<StopOnLastHandle>,
}
#[derive(Clone, Debug)]
pub struct ProfileEventObservation {
    pub state: ProfileEventState,
    pub observed: u64,
    pub applied: u64,
    pub ignored: u64,
    pub rejected: u64,
    pub reader_closed: bool,
}
impl OutputStore {
    /// Use the already established store's exact address/credentials, never a
    /// per-viewer caller URL or a second writer pool.
    pub fn profile_events(&self) -> Result<ProfileEvents, Error> {
        if matches!(
            self.inner.address.addr,
            redis::ConnectionAddr::TcpTls { .. }
        ) {
            return Err(Error::Configuration);
        }
        let (state, _) = watch::channel(ProfileEventState::Prepared);
        let (done, _) = watch::channel(false);
        let (stop, _) = watch::channel(false);
        Ok(ProfileEvents {
            inner: Arc::new(Inner {
                store: self.clone(),
                address: self.inner.address.clone(),
                started: AtomicBool::new(false),
                state,
                done,
                stop: stop.clone(),
                observed: AtomicU64::new(0),
                applied: AtomicU64::new(0),
                ignored: AtomicU64::new(0),
                rejected: AtomicU64::new(0),
            }),
            _handles: Arc::new(StopOnLastHandle(stop)),
        })
    }
}
impl ProfileEvents {
    pub fn start(
        &self,
        consumer: Arc<dyn OutputProfileOwner>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if *self.inner.stop.borrow() || self.inner.started.swap(true, Ordering::AcqRel) {
            return Err(Error::Configuration);
        }
        let own = self.inner.clone();
        tokio::spawn(async move {
            let mut stop = own.stop.subscribe();
            loop {
                if *stop.borrow() {
                    break;
                }
                own.state.send_replace(ProfileEventState::Connecting);
                // This owner is retained even if shutdown times out while a real
                // accepted ensure_owned callback is draining its physical work.
                let result = run(own.clone(), consumer.clone(), owner.clone()).await;
                if result.is_ok() || *stop.borrow() {
                    break;
                }
                own.state.send_replace(ProfileEventState::Fault);
                // Original server event listener recreates its failed physical
                // pubsub connection after five seconds. No message is replayed.
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = time::sleep(Duration::from_secs(5)) => {},
                }
            }
            own.state.send_replace(ProfileEventState::Stopped);
            own.done.send_replace(true);
        });
        Ok(())
    }
    pub async fn wait_ready(&self, budget: Duration) -> bool {
        let mut state = self.inner.state.subscribe();
        time::timeout(budget, async {
            loop {
                match *state.borrow_and_update() {
                    ProfileEventState::Subscribed => return true,
                    ProfileEventState::Fault | ProfileEventState::Stopped => return false,
                    _ => {}
                };
                if state.changed().await.is_err() {
                    return false;
                }
            }
        })
        .await
        .unwrap_or(false)
    }
    pub fn observation(&self) -> ProfileEventObservation {
        ProfileEventObservation {
            state: *self.inner.state.borrow(),
            observed: self.inner.observed.load(Ordering::Acquire),
            applied: self.inner.applied.load(Ordering::Acquire),
            ignored: self.inner.ignored.load(Ordering::Acquire),
            rejected: self.inner.rejected.load(Ordering::Acquire),
            reader_closed: *self.inner.done.borrow(),
        }
    }
    pub async fn request_retained<G: Send + 'static>(
        &self,
        request: &OutputProfileRequest,
        guard: G,
    ) -> Result<(u64, G), Error> {
        request.validate()?;
        if *self.inner.stop.borrow() {
            return Err(Error::Configuration);
        }
        let payload=serde_json::to_vec(&json!({"event":"ensure_output_profile","channel_id":request.channel,
            "profile_id":request.profile_id,"command":request.command,
            "timestamp":SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_|Error::Configuration)?.as_secs_f64()})).map_err(|_|Error::Corrupt)?;
        if payload.len() > MAX_PAYLOAD {
            return Err(Error::Capacity);
        }
        let topic = format!("live:events:{}", request.channel);
        let (receivers, (_own, guard)): (u64, (Self, G)) = self
            .inner
            .store
            .command(
                "return redis.call('PUBLISH',KEYS[1],ARGV[1])",
                &[topic],
                &[&payload],
                (self.clone(), guard),
            )
            .await?;
        // Receiver count is a physical PUBLISH receipt, not proof that the TS
        // owner started a process. The caller still waits for actual profile state.
        Ok((receivers, guard))
    }
    pub fn request_shutdown(&self) {
        self.inner.stop.send_replace(true);
    }
    pub async fn shutdown(&self, budget: Duration) -> bool {
        self.inner.stop.send_replace(true);
        if !self.inner.started.load(Ordering::Acquire) {
            self.inner.done.send_replace(true);
            return true;
        }
        let mut done = self.inner.done.subscribe();
        time::timeout(budget, async {
            while !*done.borrow_and_update() {
                if done.changed().await.is_err() {
                    return false;
                }
            }
            true
        })
        .await
        .unwrap_or(false)
    }
}
trait Socket: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Socket for T {}
type Connection = Box<dyn Socket>;
async fn connect(address: &redis::ConnectionInfo) -> Result<Connection, Error> {
    match &address.addr {
        redis::ConnectionAddr::Tcp(host, port) => Ok(Box::new(
            tokio::net::TcpStream::connect((host.as_str(), *port))
                .await
                .map_err(|_| Error::Unavailable)?,
        )),
        #[cfg(unix)]
        redis::ConnectionAddr::Unix(path) => Ok(Box::new(
            tokio::net::UnixStream::connect(path)
                .await
                .map_err(|_| Error::Unavailable)?,
        )),
        _ => Err(Error::Configuration),
    }
}
async fn write(socket: &mut Connection, parts: &[&[u8]]) -> Result<(), Error> {
    let mut command = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        command.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        command.extend_from_slice(part);
        command.extend_from_slice(b"\r\n");
    }
    socket
        .write_all(&command)
        .await
        .map_err(|_| Error::Unavailable)
}
async fn line(socket: &mut Connection) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    loop {
        if bytes.len() >= 128 {
            return Err(Error::Corrupt);
        }
        let b = socket.read_u8().await.map_err(|_| Error::Unavailable)?;
        bytes.push(b);
        if bytes.ends_with(b"\r\n") {
            bytes.truncate(bytes.len() - 2);
            return Ok(bytes);
        }
    }
}
async fn bulk(socket: &mut Connection, budget: &mut usize) -> Result<Vec<u8>, Error> {
    let header = line(socket).await?;
    if header.first() != Some(&b'$') {
        return Err(Error::Corrupt);
    }
    let length = std::str::from_utf8(&header[1..])
        .map_err(|_| Error::Corrupt)?
        .parse::<usize>()
        .map_err(|_| Error::Corrupt)?;
    if length > *budget {
        return Err(Error::Capacity);
    }
    *budget -= length;
    let mut data = vec![0; length];
    socket
        .read_exact(&mut data)
        .await
        .map_err(|_| Error::Unavailable)?;
    let mut end = [0; 2];
    socket
        .read_exact(&mut end)
        .await
        .map_err(|_| Error::Unavailable)?;
    if end != *b"\r\n" {
        return Err(Error::Corrupt);
    }
    Ok(data)
}
async fn handshake(socket: &mut Connection, address: &redis::ConnectionInfo) -> Result<(), Error> {
    if let Some(password) = address.redis.password.as_deref() {
        let mut args = vec![b"AUTH".as_slice()];
        if let Some(user) = address.redis.username.as_deref() {
            args.push(user.as_bytes());
        }
        args.push(password.as_bytes());
        write(socket, &args).await?;
        if line(socket).await? != b"+OK" {
            return Err(Error::Unavailable);
        }
    }
    let db = address.redis.db.to_string();
    write(socket, &[b"SELECT", db.as_bytes()]).await?;
    if line(socket).await? != b"+OK" {
        return Err(Error::Unavailable);
    }
    write(socket, &[b"PSUBSCRIBE", PATTERN]).await?;
    if line(socket).await? != b"*3" {
        return Err(Error::Corrupt);
    }
    let mut budget = 256;
    if bulk(socket, &mut budget).await? != b"psubscribe"
        || bulk(socket, &mut budget).await? != PATTERN
        || line(socket).await? != b":1"
    {
        return Err(Error::Corrupt);
    }
    Ok(())
}
async fn read_message(socket: &mut Connection) -> Result<(Vec<u8>, Vec<u8>), Error> {
    if line(socket).await? != b"*4" {
        return Err(Error::Corrupt);
    }
    let mut budget = MAX_PAYLOAD + 1024;
    if bulk(socket, &mut budget).await? != b"pmessage"
        || bulk(socket, &mut budget).await? != PATTERN
    {
        return Err(Error::Corrupt);
    }
    let topic = bulk(socket, &mut budget).await?;
    if topic.len() > MAX_CHANNEL + 12 {
        return Err(Error::Capacity);
    }
    let data = bulk(socket, &mut budget).await?;
    Ok((topic, data))
}
async fn run(
    inner: Arc<Inner>,
    consumer: Arc<dyn OutputProfileOwner>,
    owner: Arc<dyn Send + Sync>,
) -> Result<(), Error> {
    let _owner = owner.clone();
    let mut stop = inner.stop.subscribe();
    let mut socket = tokio::select! {
        _=stop.changed()=>return Ok(()),
        result=time::timeout(inner.store.inner.config.command_timeout,async {let mut socket=connect(&inner.address).await?;handshake(&mut socket,&inner.address).await?;Ok::<_,Error>(socket)})=>result.map_err(|_|Error::Unavailable)??,
    };
    inner.state.send_replace(ProfileEventState::Subscribed);
    let result = loop {
        if *stop.borrow() {
            break Ok(());
        }
        let message = tokio::select! {_=stop.changed()=>break Ok(()),message=read_message(&mut socket)=>message};
        let (topic, data) = match message {
            Ok(message) => message,
            Err(error) => break Err(error),
        };
        inner.observed.fetch_add(1, Ordering::AcqRel);
        let value = match event_value(&data) {
            Ok(value) => value,
            Err(_) => {
                inner.rejected.fetch_add(1, Ordering::AcqRel);
                continue;
            }
        };
        match RemoteSwitchWake::decode(&topic, &value) {
            Ok(Some(wake)) => {
                // Switch receipts belong to the separate Redis mailbox.
                // Keep output profile application counters scoped to profiles.
                let _ = consumer.switch_source_owned(wake, owner.clone()).await;
                inner.ignored.fetch_add(1, Ordering::AcqRel);
                continue;
            }
            Ok(None) => {}
            Err(_) => {
                inner.rejected.fetch_add(1, Ordering::AcqRel);
                continue;
            }
        }
        match OutputProfileRequest::from_value(&topic, &value) {
            Ok(Some(request)) => {
                // Never race an accepted owner callback against shutdown and
                // drop its work. Shutdown reports false until it actually settles.
                if consumer.ensure_owned(request, owner.clone()).await.is_err() {
                    inner.rejected.fetch_add(1, Ordering::AcqRel);
                } else {
                    inner.applied.fetch_add(1, Ordering::AcqRel);
                }
            }
            Ok(None) => {
                inner.ignored.fetch_add(1, Ordering::AcqRel);
            }
            Err(_) => {
                inner.rejected.fetch_add(1, Ordering::AcqRel);
            }
        }
    };
    // No spawned untracked reader, queue or reconnect survives this physical FD.
    // A bounded shutdown write cannot hold the observed reader FD indefinitely.
    let _ = time::timeout(inner.store.inner.config.command_timeout, socket.shutdown()).await;
    drop(socket);
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_source_shape_requires_topic_identity_and_literal_argv() {
        let raw=br#"{"event":"ensure_output_profile","channel_id":"channel-a","profile_id":7,"command":["ffmpeg","-i","pipe:0","-metadata","{userAgent}","pipe:1"],"timestamp":1}"#;
        let request = OutputProfileRequest::decode(b"live:events:channel-a", raw)
            .unwrap()
            .unwrap();
        assert_eq!(request.command[4], "{userAgent}");
        assert!(OutputProfileRequest::decode(b"live:events:channel-b", raw).is_err());
        assert!(OutputProfileRequest::decode(b"live:events:channel-a",br#"{"event":"ensure_output_profile","channel_id":"channel-a","profile_id":true,"command":["ffmpeg"]}"#).is_err());
        assert!(OutputProfileRequest::decode(
            b"live:events:channel-a",
            br#"{"event":"channel_stop","channel_id":"channel-a"}"#
        )
        .unwrap()
        .is_none());
    }
    #[tokio::test]
    async fn reader_rejects_oversized_bulk_before_allocating_payload() {
        use tokio::io::duplex;
        let (mut writer, reader) = duplex(1024);
        let mut reader: Connection = Box::new(reader);
        writer.write_all(b"$999999999\r\n").await.unwrap();
        let mut budget = MAX_PAYLOAD;
        assert_eq!(
            bulk(&mut reader, &mut budget).await.unwrap_err(),
            Error::Capacity
        );
        assert_eq!(budget, MAX_PAYLOAD);
    }
}
