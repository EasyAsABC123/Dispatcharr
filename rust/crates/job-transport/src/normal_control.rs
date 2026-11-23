//! Real bounded Celery pidbox revoke transport for normal recovery.
//! Original Dispatcharr/Celery/Kombu authors retain application/protocol credit.
use crate::{normal_queue::DefaultQueueControl, normal_revocation::Revoked};
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use std::{
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
#[path = "normal_control_wire.rs"]
mod wire;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Unsupported,
    Capacity,
    Closed,
    NotReady,
    Protocol,
    Io,
    Timeout,
    Idle,
    Revoked,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Prepared,
    Starting,
    Subscribed,
    Stopped,
    Fault(Error),
}
#[derive(Clone, Debug)]
pub struct RevokeReceipt {
    pub receivers: i64,
    pub clock: u64,
}
pub struct Observation {
    pub state: State,
    pub received: u64,
    pub applied: u64,
    pub rejected: u64,
    pub backend_marked: u64,
    pub backend_preserved_success: u64,
    pub backend_failed: u64,
    pub redis_client_id: Option<u64>,
    pub reader_closed: bool,
}
struct Inner {
    address: redis::ConnectionInfo,
    url: String,
    topic: String,
    revoked: Revoked,
    backend: Mutex<Option<crate::transport::Jobs>>,
    default_queue: Mutex<Option<(String, DefaultQueueControl)>>,
    default_queue_enabled_at: AtomicU64,
    clock: AtomicU64,
    stop: Arc<AtomicBool>,
    started: AtomicBool,
    state: watch::Sender<State>,
    done: watch::Sender<bool>,
    writer: Mutex<Option<RedisTransport>>,
    queued: Arc<Semaphore>,
    active: Arc<Semaphore>,
    received: AtomicU64,
    applied: AtomicU64,
    rejected: AtomicU64,
    backend_marked: AtomicU64,
    backend_preserved_success: AtomicU64,
    backend_failed: AtomicU64,
    client_id: AtomicU64,
    reader_closed: AtomicBool,
}
struct Lifetime(Arc<AtomicBool>);
impl Drop for Lifetime {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
#[derive(Clone)]
pub struct Control {
    inner: Arc<Inner>,
    _lifetime: Arc<Lifetime>,
}
struct SenderOwner {
    _queued: OwnedSemaphorePermit,
    _active: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
fn now() -> Result<f64, Error> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|v| v.as_secs_f64())
        .map_err(|_| Error::Configuration)
}
impl Control {
    /// Prepares configuration only. start + wait_ready proves actual AUTH/SELECT/
    /// SUBSCRIBE; no caller-visible readiness is inferred from this constructor.
    pub async fn connect(broker: &str, revoked: Revoked) -> Result<Self, Error> {
        if broker.len() > 8192 {
            return Err(Error::Configuration);
        }
        let client = redis::Client::open(broker).map_err(|_| Error::Configuration)?;
        let address = client.get_connection_info().clone();
        if address.redis.protocol != redis::ProtocolVersion::RESP2
            || matches!(address.addr, redis::ConnectionAddr::TcpTls { .. })
        {
            return Err(Error::Unsupported);
        }
        if address
            .redis
            .username
            .as_ref()
            .is_some_and(|v| v.len() > 4096)
            || address
                .redis
                .password
                .as_ref()
                .is_some_and(|v| v.len() > 4096)
        {
            return Err(Error::Capacity);
        }
        let topic = wire::topic(address.redis.db)?;
        let stop = Arc::new(AtomicBool::new(false));
        Ok(Self {
            inner: Arc::new(Inner {
                address,
                url: broker.into(),
                topic,
                revoked,
                backend: Mutex::new(None),
                default_queue: Mutex::new(None),
                default_queue_enabled_at: AtomicU64::new(0),
                clock: AtomicU64::new(0),
                stop: stop.clone(),
                started: AtomicBool::new(false),
                state: watch::channel(State::Prepared).0,
                done: watch::channel(false).0,
                writer: Mutex::new(None),
                queued: Arc::new(Semaphore::new(8)),
                active: Arc::new(Semaphore::new(1)),
                received: AtomicU64::new(0),
                applied: AtomicU64::new(0),
                rejected: AtomicU64::new(0),
                backend_marked: AtomicU64::new(0),
                backend_preserved_success: AtomicU64::new(0),
                backend_failed: AtomicU64::new(0),
                client_id: AtomicU64::new(0),
                reader_closed: AtomicBool::new(false),
            }),
            _lifetime: Arc::new(Lifetime(stop)),
        })
    }
    /// Cold-bind Main's EXISTING shared result port, before receiver start.
    /// Legacy memory-only constructors remain scoped to their prior contract.
    pub fn bind_backend(&self, jobs: crate::transport::Jobs) -> Result<(), Error> {
        if !jobs.allows_defaults() || self.inner.started.load(Ordering::Acquire) {
            return Err(Error::Configuration);
        }
        let mut backend = self
            .inner
            .backend
            .lock()
            .map_err(|_| Error::Configuration)?;
        if backend.is_some() {
            return Err(Error::Configuration);
        }
        *backend = Some(jobs);
        Ok(())
    }
    pub fn bind_default_queue_control(
        &self,
        destination: String,
        admission: DefaultQueueControl,
    ) -> Result<(), Error> {
        let host = destination
            .strip_prefix("default@")
            .ok_or(Error::Configuration)?;
        if self.inner.started.load(Ordering::Acquire)
            || self
                .inner
                .address
                .redis
                .password
                .as_deref()
                .is_none_or(str::is_empty)
            || host.is_empty()
            || host.len() > 120
            || !host
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        {
            return Err(Error::Configuration);
        }
        let mut configured = self
            .inner
            .default_queue
            .lock()
            .map_err(|_| Error::Configuration)?;
        if configured.is_some() || !admission.configure() {
            return Err(Error::Configuration);
        }
        *configured = Some((destination, admission));
        Ok(())
    }

    pub fn enable_default_queue_control(&self) -> Result<(), Error> {
        if let Some((_, admission)) = &*self
            .inner
            .default_queue
            .lock()
            .map_err(|_| Error::Configuration)?
        {
            let enabled_at = now()?;
            if !admission.enable() {
                return Err(Error::Configuration);
            }
            self.inner
                .default_queue_enabled_at
                .store(enabled_at.to_bits(), Ordering::Release);
        }
        Ok(())
    }

    pub fn state(&self) -> State {
        *self.inner.state.borrow()
    }
    pub fn observation(&self) -> Observation {
        let id = self.inner.client_id.load(Ordering::Acquire);
        Observation {
            state: self.state(),
            received: self.inner.received.load(Ordering::Acquire),
            applied: self.inner.applied.load(Ordering::Acquire),
            rejected: self.inner.rejected.load(Ordering::Acquire),
            backend_marked: self.inner.backend_marked.load(Ordering::Acquire),
            backend_preserved_success: self.inner.backend_preserved_success.load(Ordering::Acquire),
            backend_failed: self.inner.backend_failed.load(Ordering::Acquire),
            redis_client_id: (id != 0).then_some(id),
            reader_closed: self.inner.reader_closed.load(Ordering::Acquire),
        }
    }
    pub fn start(&self, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        if self.inner.stop.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if self.inner.started.swap(true, Ordering::AcqRel) {
            return Err(Error::Configuration);
        }
        self.inner.state.send_replace(State::Starting);
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let outcome = async {
                let writer = RedisTransport::connect(
                    &inner.url,
                    TransportConfig {
                        max_operations: 1,
                        max_queued_operations: 8,
                        command_timeout: Duration::from_secs(3),
                    },
                )
                .await
                .map_err(|_| Error::Io)?;
                *inner.writer.lock().map_err(|_| Error::Configuration)? = Some(writer);
                let reader = inner.clone();
                let physical = owner.clone();
                tokio::task::spawn_blocking(move || {
                    read_loop(&reader, physical, tokio::runtime::Handle::current())
                })
                .await
                .map_err(|_| Error::Io)?
            }
            .await;
            inner.reader_closed.store(true, Ordering::Release);
            inner.state.send_replace(match outcome {
                Ok(()) => State::Stopped,
                Err(Error::Closed) => State::Stopped,
                Err(e) => State::Fault(e),
            });
            drop(owner);
            inner.done.send_replace(true);
        });
        Ok(())
    }
    pub async fn wait_ready(&self, budget: Duration) -> bool {
        let mut state = self.inner.state.subscribe();
        tokio::time::timeout(budget, async {
            loop {
                match *state.borrow_and_update() {
                    State::Subscribed => return true,
                    State::Fault(_) | State::Stopped => return false,
                    _ => {}
                }
                if state.changed().await.is_err() {
                    return false;
                }
            }
        })
        .await
        .unwrap_or(false)
    }
    pub async fn revoke_one(
        &self,
        key: &str,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<RevokeReceipt, Error> {
        if key.is_empty() || key.len() > 255 || !key.bytes().all(|v| v.is_ascii_graphic()) {
            return Err(Error::Capacity);
        }
        self.publish(&[key.into()], true, owner).await
    }
    pub async fn revoke_many(
        &self,
        keys: &[String],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<RevokeReceipt, Error> {
        self.publish(keys, false, owner).await
    }
    async fn publish(
        &self,
        keys: &[String],
        single: bool,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<RevokeReceipt, Error> {
        if self.inner.stop.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if self.state() != State::Subscribed {
            return Err(Error::NotReady);
        }
        if !wire::valid_keys(keys) {
            return Err(Error::Capacity);
        }
        let queued = self
            .inner
            .queued
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let active = self
            .inner
            .active
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Closed)?;
        if self.inner.stop.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let clock = self
            .inner
            .clock
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| v.checked_add(1))
            .map_err(|_| Error::Capacity)?
            + 1;
        let raw = wire::encode(keys, single, clock, now()?)?;
        let guard = Arc::new(SenderOwner {
            _queued: queued,
            _active: active,
            _caller: owner,
        });
        let writer = self
            .inner
            .writer
            .lock()
            .map_err(|_| Error::Configuration)?
            .clone()
            .ok_or(Error::NotReady)?;
        let (receivers, guard) = writer
            .command::<i64, _>(
                "return redis.call('PUBLISH',KEYS[1],ARGV[1])",
                std::slice::from_ref(&self.inner.topic),
                &[&raw],
                guard,
            )
            .await
            .map_err(|_| Error::Io)?;
        drop(guard);
        Ok(RevokeReceipt { receivers, clock })
    }
    /// Drain actual receive work and retained PUBLISH replies. The shared
    /// ConnectionManager's idle writer socket is closed when final handles drop;
    /// this method does not fabricate an independent writer-socket close proof.
    pub async fn shutdown(&self, budget: Duration) -> bool {
        self.inner.stop.store(true, Ordering::Release);
        self.inner.queued.close();
        self.inner.active.close();
        let mut done = self.inner.done.subscribe();
        tokio::time::timeout(budget, async {
            if self.inner.started.load(Ordering::Acquire) {
                loop {
                    if *done.borrow_and_update() {
                        break;
                    }
                    if done.changed().await.is_err() {
                        return false;
                    }
                }
            }
            while self.inner.queued.available_permits() != 8
                || self.inner.active.available_permits() != 1
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            self.inner
                .writer
                .lock()
                .map_err(|_| ())
                .map(|mut v| {
                    v.take();
                })
                .is_ok()
        })
        .await
        .unwrap_or(false)
    }
}

enum Socket {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(std::os::unix::net::UnixStream),
}
impl Read for Socket {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Tcp(s) => s.read(b),
            #[cfg(unix)]
            Self::Unix(s) => s.read(b),
        }
    }
}
impl Write for Socket {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Tcp(s) => s.write(b),
            #[cfg(unix)]
            Self::Unix(s) => s.write(b),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Tcp(s) => s.flush(),
            #[cfg(unix)]
            Self::Unix(s) => s.flush(),
        }
    }
}
fn socket(info: &redis::ConnectionInfo, stop: &AtomicBool) -> Result<Socket, Error> {
    let value = match &info.addr {
        redis::ConnectionAddr::Tcp(host, port) => {
            // DNS and every socket attempt stay inside this owned blocking task.
            // Shutdown cannot report settled while getaddrinfo is still active.
            let addresses = (host.as_str(), *port)
                .to_socket_addrs()
                .map_err(|_| Error::Io)?;
            let until = Instant::now() + Duration::from_secs(3);
            let mut found = None;
            for address in addresses.take(8) {
                if stop.load(Ordering::Acquire) {
                    return Err(Error::Closed);
                }
                let Some(left) = until.checked_duration_since(Instant::now()) else {
                    break;
                };
                if let Ok(stream) = TcpStream::connect_timeout(&address, left) {
                    found = Some(stream);
                    break;
                }
            }
            let stream = found.ok_or(Error::Io)?;
            stream
                .set_read_timeout(Some(Duration::from_millis(200)))
                .map_err(|_| Error::Io)?;
            stream
                .set_write_timeout(Some(Duration::from_secs(3)))
                .map_err(|_| Error::Io)?;
            Socket::Tcp(stream)
        }
        #[cfg(unix)]
        redis::ConnectionAddr::Unix(path) => {
            let stream = std::os::unix::net::UnixStream::connect(path).map_err(|_| Error::Io)?;
            stream
                .set_read_timeout(Some(Duration::from_millis(200)))
                .map_err(|_| Error::Io)?;
            stream
                .set_write_timeout(Some(Duration::from_secs(3)))
                .map_err(|_| Error::Io)?;
            Socket::Unix(stream)
        }
        _ => return Err(Error::Unsupported),
    };
    Ok(value)
}
fn send(stream: &mut Socket, args: &[&[u8]]) -> Result<(), Error> {
    let size = args
        .iter()
        .try_fold(0usize, |sum, v| sum.checked_add(v.len() + 32))
        .ok_or(Error::Capacity)?;
    if size > wire::MAX_FRAME {
        return Err(Error::Capacity);
    }
    let mut bytes = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        bytes.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        bytes.extend_from_slice(arg);
        bytes.extend_from_slice(b"\r\n");
    }
    stream.write_all(&bytes).map_err(|_| Error::Io)
}
enum Resp {
    ServerError,
    Simple(Vec<u8>),
    Number(i64),
    Bulk(Vec<u8>),
    Array(Vec<Resp>),
    Nil,
}
fn read_exact(
    stream: &mut Socket,
    bytes: &mut [u8],
    stop: &AtomicBool,
    until: Instant,
    idle: bool,
) -> Result<(), Error> {
    let mut count = 0;
    while count < bytes.len() {
        if stop.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if !idle && Instant::now() >= until {
            return Err(Error::Timeout);
        }
        match stream.read(&mut bytes[count..]) {
            Ok(0) => return Err(Error::Io),
            Ok(n) => count += n,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) =>
            {
                if idle && count == 0 {
                    return Err(Error::Idle);
                }
                if Instant::now() >= until {
                    return Err(Error::Timeout);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => return Err(Error::Io),
        }
    }
    Ok(())
}
fn line(stream: &mut Socket, stop: &AtomicBool, until: Instant) -> Result<Vec<u8>, Error> {
    let mut value = Vec::new();
    loop {
        let mut b = [0];
        read_exact(stream, &mut b, stop, until, false)?;
        if value.len() >= 256 {
            return Err(Error::Capacity);
        }
        value.push(b[0]);
        if value.ends_with(b"\r\n") {
            value.truncate(value.len() - 2);
            return Ok(value);
        }
    }
}
fn resp(
    stream: &mut Socket,
    stop: &AtomicBool,
    budget: &mut usize,
    depth: usize,
    until: Instant,
    idle: bool,
) -> Result<Resp, Error> {
    if depth > 4 || *budget == 0 {
        return Err(Error::Capacity);
    }
    let mut kind = [0];
    read_exact(stream, &mut kind, stop, until, idle && depth == 0)?;
    let header = line(stream, stop, until)?;
    *budget = budget
        .checked_sub(header.len() + 3)
        .ok_or(Error::Capacity)?;
    let number = || {
        std::str::from_utf8(&header)
            .map_err(|_| Error::Protocol)?
            .parse::<i64>()
            .map_err(|_| Error::Protocol)
    };
    match kind[0] {
        b'+' => Ok(Resp::Simple(header)),
        b'-' => Ok(Resp::ServerError),
        b':' => Ok(Resp::Number(number()?)),
        b'$' => {
            let size = number()?;
            if size == -1 {
                return Ok(Resp::Nil);
            }
            let size = usize::try_from(size).map_err(|_| Error::Protocol)?;
            *budget = budget.checked_sub(size + 2).ok_or(Error::Capacity)?;
            let mut bytes = vec![0; size];
            read_exact(stream, &mut bytes, stop, until, false)?;
            let mut end = [0; 2];
            read_exact(stream, &mut end, stop, until, false)?;
            if end != *b"\r\n" {
                return Err(Error::Protocol);
            }
            Ok(Resp::Bulk(bytes))
        }
        b'*' => {
            let count = usize::try_from(number()?).map_err(|_| Error::Protocol)?;
            if count > 8 {
                return Err(Error::Capacity);
            }
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(resp(stream, stop, budget, depth + 1, until, false)?);
            }
            Ok(Resp::Array(values))
        }
        _ => Err(Error::Protocol),
    }
}
fn response(stream: &mut Socket, stop: &AtomicBool, idle: bool) -> Result<Resp, Error> {
    let mut budget = wire::MAX_FRAME;
    resp(
        stream,
        stop,
        &mut budget,
        0,
        Instant::now() + Duration::from_secs(3),
        idle,
    )
}
fn ok(stream: &mut Socket, stop: &AtomicBool) -> Result<(), Error> {
    match response(stream, stop, false)? {
        Resp::Simple(v) if v == b"OK" => Ok(()),
        _ => Err(Error::Protocol),
    }
}
fn read_loop(
    inner: &Arc<Inner>,
    owner: Arc<dyn Send + Sync>,
    runtime: tokio::runtime::Handle,
) -> Result<(), Error> {
    let mut stream = socket(&inner.address, &inner.stop)?;
    if let Some(password) = &inner.address.redis.password {
        if let Some(username) = &inner.address.redis.username {
            send(
                &mut stream,
                &[b"AUTH", username.as_bytes(), password.as_bytes()],
            )?;
        } else {
            send(&mut stream, &[b"AUTH", password.as_bytes()])?;
        }
        ok(&mut stream, &inner.stop)?;
    }
    let db = inner.address.redis.db.to_string();
    send(&mut stream, &[b"SELECT", db.as_bytes()])?;
    ok(&mut stream, &inner.stop)?;
    send(&mut stream, &[b"CLIENT", b"ID"])?;
    match response(&mut stream, &inner.stop, false)? {
        Resp::Number(id) if id > 0 => inner.client_id.store(id as u64, Ordering::Release),
        Resp::ServerError => {}
        _ => return Err(Error::Protocol),
    }
    send(&mut stream, &[b"SUBSCRIBE", inner.topic.as_bytes()])?;
    match response(&mut stream, &inner.stop, false)? {
        Resp::Array(v)
            if v.len() == 3
                && matches!(&v[0],Resp::Bulk(x)if x==b"subscribe")
                && matches!(&v[1],Resp::Bulk(x)if x==inner.topic.as_bytes())
                && matches!(&v[2], Resp::Number(1)) => {}
        _ => return Err(Error::Protocol),
    }
    let queue_control = inner
        .default_queue
        .lock()
        .map_err(|_| Error::Configuration)?
        .clone();
    inner.state.send_replace(State::Subscribed);
    while !inner.stop.load(Ordering::Acquire) {
        let value = match response(&mut stream, &inner.stop, true) {
            Ok(v) => v,
            Err(Error::Idle) => continue,
            Err(Error::Closed) => break,
            Err(e) => return Err(e),
        };
        let Resp::Array(values) = value else {
            return Err(Error::Protocol);
        };
        if values.len() != 3
            || !matches!(&values[0],Resp::Bulk(x)if x==b"message")
            || !matches!(&values[1],Resp::Bulk(x)if x==inner.topic.as_bytes())
        {
            continue;
        }
        let Resp::Bulk(raw) = &values[2] else {
            return Err(Error::Protocol);
        };
        inner.received.fetch_add(1, Ordering::AcqRel);
        let enabled = queue_control.as_ref().and_then(|(destination, _)| {
            let enabled_at = inner.default_queue_enabled_at.load(Ordering::Acquire);
            (enabled_at != 0).then(|| (destination.as_str(), f64::from_bits(enabled_at)))
        });
        match wire::decode_for(raw, now()?, enabled) {
            Ok(decoded) => {
                let _ = inner
                    .clock
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |local| {
                        Some(local.max(decoded.clock).saturating_add(1))
                    });
                if let (Some(pause), Some((_, admission))) = (decoded.pause_default, &queue_control)
                {
                    if !inner.stop.load(Ordering::Acquire) && admission.request(pause) {
                        inner.applied.fetch_add(1, Ordering::AcqRel);
                    } else {
                        inner.rejected.fetch_add(1, Ordering::AcqRel);
                    }
                }
                if let Some(keys) = decoded.keys {
                    inner
                        .revoked
                        .apply(&keys, owner.clone())
                        .map_err(|_| Error::Revoked)?;
                    let backend = inner
                        .backend
                        .lock()
                        .map_err(|_| Error::Configuration)?
                        .clone();
                    if let Some(jobs) = backend {
                        // Source applies its memory set FIRST, then attempts
                        // each distinct ID's backend mark. Errors are caught
                        // per ID; they neither undo memory nor stop later IDs.
                        let unique = keys.iter().collect::<std::collections::BTreeSet<_>>();
                        for key in unique {
                            match runtime.block_on(jobs.write_control_revoked(key, owner.clone())) {
                                Ok(true) => {
                                    inner.backend_marked.fetch_add(1, Ordering::AcqRel);
                                }
                                Ok(false) => {
                                    inner
                                        .backend_preserved_success
                                        .fetch_add(1, Ordering::AcqRel);
                                }
                                Err(_) => {
                                    inner.backend_failed.fetch_add(1, Ordering::AcqRel);
                                    eprintln!("Pidbox revoke: caught settled backend mark failure; continuing command");
                                }
                            }
                        }
                    }
                    inner.applied.fetch_add(1, Ordering::AcqRel);
                }
            }
            Err(_) => {
                inner.rejected.fetch_add(1, Ordering::AcqRel);
            }
        }
    }
    drop(stream);
    Ok(())
}

#[cfg(test)]
mod default_queue_tests {
    use super::*;

    #[tokio::test]
    async fn maintenance_binding_requires_explicit_target_and_password() {
        // Construction parses the configured endpoint without opening a socket.
        let no_password = Control::connect("redis://127.0.0.1:6379/1", Revoked::new())
            .await
            .unwrap();
        assert_eq!(
            no_password
                .bind_default_queue_control("default@owned".into(), DefaultQueueControl::new()),
            Err(Error::Configuration)
        );
        let parsed_only_fixture = format!(
            "redis://:owned-test-{}@127.0.0.1:6379/1",
            std::process::id()
        );
        let with_password = Control::connect(&parsed_only_fixture, Revoked::new())
            .await
            .unwrap();
        for invalid in ["", "dvr@owned", "default@", "default@owned*"] {
            assert_eq!(
                with_password
                    .bind_default_queue_control(invalid.into(), DefaultQueueControl::new()),
                Err(Error::Configuration)
            );
        }
        with_password
            .bind_default_queue_control("default@owned".into(), DefaultQueueControl::new())
            .unwrap();
        assert_eq!(
            with_password
                .bind_default_queue_control("default@another".into(), DefaultQueueControl::new()),
            Err(Error::Configuration)
        );
    }
}
