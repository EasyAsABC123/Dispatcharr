//! Owned RESP2 result subscription on an EXISTING configured transport budget.
//! No independent pool/client, silent message loss, unbounded frame allocation or
//! fabricated result. Original Redis/Dispatcharr authors retain all credit.
use redis::{ConnectionAddr, ConnectionInfo, Value};
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, oneshot, watch, Mutex, OwnedSemaphorePermit},
};
type Owner = Arc<dyn Send + Sync>;
const FRAME: usize = 2 * 1024 * 1024;
const BUFFER: usize = 16 * 1024 * 1024;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Unsupported,
    Capacity,
    Closed,
    Io,
    Protocol,
    Deadline,
    Server(Vec<u8>),
}
#[derive(Clone, Debug)]
pub struct Receipt {
    pub actual_read_eof: bool,
    pub reader_forced_closed: bool,
    pub reader_joined: bool,
    pub socket_retired: bool,
}
trait Socket: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Socket for T {}
type Stream = Box<dyn Socket>;
struct Frame {
    value: Value,
    bytes: usize,
}
async fn line<R: AsyncRead + Unpin>(reader: &mut R, budget: &mut usize) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    loop {
        let byte = reader.read_u8().await.map_err(|_| Error::Io)?;
        *budget += 1;
        if *budget > FRAME || out.len() >= 4096 {
            return Err(Error::Capacity);
        }
        out.push(byte);
        if out.ends_with(b"\r\n") {
            out.truncate(out.len() - 2);
            return Ok(out);
        }
    }
}
async fn scalar<R: AsyncRead + Unpin>(
    reader: &mut R,
    tag: u8,
    budget: &mut usize,
) -> Result<Value, Error> {
    match tag {
        b':' => {
            let raw = line(reader, budget).await?;
            Ok(Value::Int(
                std::str::from_utf8(&raw)
                    .map_err(|_| Error::Protocol)?
                    .parse()
                    .map_err(|_| Error::Protocol)?,
            ))
        }
        b'+' => {
            let raw = line(reader, budget).await?;
            Ok(Value::BulkString(raw))
        }
        b'$' => {
            let raw = line(reader, budget).await?;
            let size = std::str::from_utf8(&raw)
                .map_err(|_| Error::Protocol)?
                .parse::<i64>()
                .map_err(|_| Error::Protocol)?;
            if size == -1 {
                return Ok(Value::Nil);
            }
            let size = usize::try_from(size).map_err(|_| Error::Protocol)?;
            *budget = budget.checked_add(size + 2).ok_or(Error::Capacity)?;
            if *budget > FRAME {
                return Err(Error::Capacity);
            }
            let mut bytes = vec![0; size];
            reader.read_exact(&mut bytes).await.map_err(|_| Error::Io)?;
            if reader.read_u16().await.map_err(|_| Error::Io)? != 0x0d0a {
                return Err(Error::Protocol);
            }
            Ok(Value::BulkString(bytes))
        }
        // Actual authentication/ACL/server errors fail the owned connection. They
        // are never reinterpreted as a missing task result or successful subscribe.
        b'-' => {
            let actual_error = line(reader, budget).await?;
            Err(Error::Server(actual_error))
        }
        _ => Err(Error::Protocol),
    }
}
async fn frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Frame>, Error> {
    let mut tag = [0];
    if reader.read(&mut tag).await.map_err(|_| Error::Io)? == 0 {
        return Ok(None);
    }
    let mut budget = 1;
    let value = if tag[0] == b'*' {
        let raw = line(reader, &mut budget).await?;
        let count = std::str::from_utf8(&raw)
            .map_err(|_| Error::Protocol)?
            .parse::<usize>()
            .map_err(|_| Error::Protocol)?;
        if !(1..=4).contains(&count) {
            return Err(Error::Protocol);
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let tag = reader.read_u8().await.map_err(|_| Error::Io)?;
            budget += 1;
            values.push(scalar(reader, tag, &mut budget).await?);
        }
        Value::Array(values)
    } else {
        scalar(reader, tag[0], &mut budget).await?
    };
    Ok(Some(Frame {
        value,
        bytes: budget,
    }))
}
fn packed(args: &[Vec<u8>]) -> Vec<u8> {
    let mut cmd = redis::cmd(std::str::from_utf8(&args[0]).expect("validated ASCII command"));
    for arg in &args[1..] {
        cmd.arg(arg);
    }
    cmd.get_packed_command()
}
struct Write {
    args: Vec<Vec<u8>>,
    caller: Owner,
    reply: oneshot::Sender<Result<(), Error>>,
}
struct Inner {
    sender: mpsc::Sender<Write>,
    stop: watch::Sender<bool>,
    closed: Arc<AtomicBool>,
    messages: Mutex<mpsc::Receiver<Frame>>,
    bytes: Arc<AtomicUsize>,
    done: watch::Receiver<Option<Receipt>>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
#[derive(Clone)]
pub struct Subscription {
    inner: Arc<Inner>,
}
impl Subscription {
    /// Factory-only: the caller supplies the actual configured target and a slot
    /// from THAT existing transport. Never called with a new private semaphore.
    pub(super) async fn connect(
        info: ConnectionInfo,
        timeout: Duration,
        operation: OwnedSemaphorePermit,
        host: Owner,
        caller: Owner,
    ) -> Result<Self, Error> {
        if info.redis.protocol != redis::ProtocolVersion::RESP2 {
            return Err(Error::Unsupported);
        }
        let task = tokio::spawn(async move {
            let _caller = caller;
            let connected = async {
                let mut stream: Stream = match &info.addr {
                    ConnectionAddr::Tcp(host, port) => {
                        let socket = tokio::net::TcpStream::connect((host.as_str(), *port))
                            .await
                            .map_err(|_| Error::Io)?;
                        socket.set_nodelay(true).map_err(|_| Error::Io)?;
                        Box::new(socket)
                    }
                    ConnectionAddr::Unix(path) => Box::new(
                        tokio::net::UnixStream::connect(path)
                            .await
                            .map_err(|_| Error::Io)?,
                    ),
                    _ => return Err(Error::Unsupported),
                };
                if let Some(password) = info.redis.password {
                    let mut args = vec![b"AUTH".to_vec()];
                    if let Some(username) = info.redis.username {
                        args.push(username.into_bytes())
                    }
                    args.push(password.into_bytes());
                    stream
                        .write_all(&packed(&args))
                        .await
                        .map_err(|_| Error::Io)?;
                    stream.flush().await.map_err(|_| Error::Io)?;
                    let reply = frame(&mut stream).await?.ok_or(Error::Closed)?;
                    if reply.value != Value::BulkString(b"OK".to_vec()) {
                        return Err(Error::Protocol);
                    }
                }
                if info.redis.db != 0 {
                    let args = vec![b"SELECT".to_vec(), info.redis.db.to_string().into_bytes()];
                    stream
                        .write_all(&packed(&args))
                        .await
                        .map_err(|_| Error::Io)?;
                    stream.flush().await.map_err(|_| Error::Io)?;
                    let reply = frame(&mut stream).await?.ok_or(Error::Closed)?;
                    if reply.value != Value::BulkString(b"OK".to_vec()) {
                        return Err(Error::Protocol);
                    }
                }
                Ok::<_, Error>(stream)
            };
            let stream = tokio::time::timeout(timeout, connected)
                .await
                .map_err(|_| Error::Deadline)??;
            Ok::<_, Error>(Self::drive(stream, timeout, operation, host))
        });
        // Dropping this observer detaches the connect task. Its real slot/owner live
        // until connect settles; an unobserved result handle signals physical close.
        task.await.map_err(|_| Error::Closed)?
    }
    fn drive(
        stream: Stream,
        timeout: Duration,
        operation: OwnedSemaphorePermit,
        host: Owner,
    ) -> Self {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let (messages, rx) = mpsc::channel::<Frame>(2048);
        let (sender, mut writes) = mpsc::channel::<Write>(8);
        let (stop, mut stopped) = watch::channel(false);
        let (done, receipt) = watch::channel(None);
        let (fault, mut failed) = watch::channel(false);
        let closed = Arc::new(AtomicBool::new(false));
        let bytes = Arc::new(AtomicUsize::new(0));
        let count = bytes.clone();
        let mut reading = tokio::spawn(async move {
            loop {
                match frame(&mut reader).await {
                    Ok(Some(value)) => {
                        let old = count.fetch_add(value.bytes, Ordering::SeqCst);
                        if old + value.bytes > BUFFER {
                            count.fetch_sub(value.bytes, Ordering::SeqCst);
                            fault.send_replace(true);
                            return false;
                        }
                        if let Err(error) = messages.try_send(value) {
                            count.fetch_sub(error.into_inner().bytes, Ordering::SeqCst);
                            fault.send_replace(true);
                            return false;
                        }
                    }
                    Ok(None) => {
                        fault.send_replace(true);
                        return true;
                    }
                    Err(_) => {
                        fault.send_replace(true);
                        return false;
                    }
                }
            }
        });
        let active = closed.clone();
        tokio::spawn(async move {
            let _operation = operation;
            let _host = host;
            let mut topics = BTreeSet::new();
            loop {
                tokio::select! {
                 changed=stopped.changed()=>{if changed.is_err()||*stopped.borrow(){break}},
                 changed=failed.changed()=>{if changed.is_err()||*failed.borrow(){break}},
                 command=writes.recv()=>{
                  let Some(command)=command else{break};let _caller=command.caller;
                  let result=validate(&command.args,&topics);
                  match result{Err(error)=>{let _=command.reply.send(Err(error));},Ok(next)=>{
                   let bytes=packed(&command.args);
                   // Partial writes are cancelled ONLY when retiring this whole socket,
                   // never while continuing to reuse its Redis protocol stream.
                   let result=tokio::select!{
                    result=async{writer.write_all(&bytes).await?;writer.flush().await}=>result.map_err(|_|Error::Io),
                    _=stopped.changed()=>Err(Error::Closed),
                    _=failed.changed()=>Err(Error::Closed),
                   };
                   if result.is_ok(){topics=next;}
                   let failed=result.is_err();let _=command.reply.send(result);if failed{break}
                  }}
                 }
                }
            }
            active.store(true, Ordering::SeqCst);
            let _ = writer.shutdown().await;
            drop(writer);
            let (eof, forced, joined) = match tokio::time::timeout(timeout, &mut reading).await {
                Ok(Ok(eof)) => (eof, false, true),
                Ok(Err(_)) => (false, false, true),
                Err(_) => {
                    reading.abort();
                    let _ = reading.await;
                    (false, true, true)
                }
            };
            // Both actual split socket halves are dropped before budget/host release.
            done.send_replace(Some(Receipt {
                actual_read_eof: eof,
                reader_forced_closed: forced,
                reader_joined: joined,
                socket_retired: true,
            }));
        });
        Self {
            inner: Arc::new(Inner {
                sender,
                stop,
                closed,
                messages: Mutex::new(rx),
                bytes,
                done: receipt,
            }),
        }
    }
    pub async fn command(&self, args: Vec<Vec<u8>>, caller: Owner) -> Result<(), Error> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        let (reply, wait) = oneshot::channel();
        self.inner
            .sender
            .send(Write {
                args,
                caller,
                reply,
            })
            .await
            .map_err(|_| Error::Closed)?;
        wait.await.map_err(|_| Error::Closed)?
    }
    pub async fn read(
        &self,
        timeout: Option<Duration>,
        caller: Owner,
    ) -> Result<Option<Value>, Error> {
        let _caller = caller;
        let work = async {
            let mut reader = self.inner.messages.lock().await;
            match reader.recv().await {
                Some(frame) => {
                    self.inner.bytes.fetch_sub(frame.bytes, Ordering::SeqCst);
                    Ok(Some(frame.value))
                }
                None => Err(Error::Closed),
            }
        };
        match timeout {
            Some(timeout) if timeout.is_zero() => {
                let mut reader = self
                    .inner
                    .messages
                    .try_lock()
                    .map_err(|_| Error::Capacity)?;
                match reader.try_recv() {
                    Ok(frame) => {
                        self.inner.bytes.fetch_sub(frame.bytes, Ordering::SeqCst);
                        Ok(Some(frame.value))
                    }
                    Err(mpsc::error::TryRecvError::Empty) => Ok(None),
                    Err(_) => Err(Error::Closed),
                }
            }
            Some(timeout) => match tokio::time::timeout(timeout, work).await {
                Ok(value) => value,
                Err(_) => Ok(None),
            },
            None => work.await,
        }
    }
    pub fn begin_close(&self) {
        self.inner.stop.send_replace(true);
    }
    pub async fn close(&self, timeout: Duration) -> Result<Receipt, Error> {
        self.begin_close();
        let mut done = self.inner.done.clone();
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(value) = done.borrow_and_update().clone() {
                    return Ok(value);
                }
                done.changed().await.map_err(|_| Error::Closed)?
            }
        })
        .await
        .map_err(|_| Error::Deadline)?
    }
}
fn validate(args: &[Vec<u8>], current: &BTreeSet<Vec<u8>>) -> Result<BTreeSet<Vec<u8>>, Error> {
    if args.is_empty() || args.len() > 1025 || args.iter().map(Vec::len).sum::<usize>() > 65536 {
        return Err(Error::Capacity);
    }
    let mut next = current.clone();
    match args[0].as_slice() {
        b"SUBSCRIBE" if args.len() > 1 => {
            for topic in &args[1..] {
                if topic.len() > 1024 {
                    return Err(Error::Capacity);
                }
                next.insert(topic.clone());
            }
        }
        b"UNSUBSCRIBE" => {
            if args.len() == 1 {
                next.clear()
            } else {
                for topic in &args[1..] {
                    next.remove(topic);
                }
            }
        }
        b"PING" if args.len() <= 2 => {}
        _ => return Err(Error::Unsupported),
    }
    if next.len() > 1024 {
        return Err(Error::Capacity);
    }
    Ok(next)
}
