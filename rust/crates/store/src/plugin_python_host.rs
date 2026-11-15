//! Lazy owned regex dependency protocol actor. No process starts on prepare(), no Source
//! server listens on a second port, and no request cancellation aborts a child
//! wait/SQL callback. Original authors retain Dispatcharr/CPython credit.
use dispatcharr_contracts::BoxFuture;
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    process::{Child, ChildStdin},
    sync::{mpsc, oneshot, watch, Mutex},
};
const FRAME: usize = 8 * 1024 * 1024;
type Owner = Arc<dyn Send + Sync>;
// The regex-only actor retains the actual admitted caller Arc directly.
// It has no SQL connection/session callback or unfinished plugin host ports.
#[derive(Debug)]
pub enum Error {
    Closed,
    Capacity,
    Protocol,
    Child,
    Callback,
    Drain,
    Unavailable,
    Remote(Value),
}
#[derive(Clone, Debug)]
pub struct Identity {
    pub pid: u32,
    pub start_generation: String,
    pub pid_namespace: PathBuf,
}
#[derive(Clone, Debug)]
pub struct Receipt {
    pub identity: Identity,
    pub actual_exit: Option<i32>,
    pub wait_observed: bool,
    pub stdout_eof: bool,
    pub stderr_eof: bool,
    pub stderr_bytes: u64,
    pub forced: bool,
    pub descendants_absent: bool,
    pub callbacks_settled: bool,
}
/// Implementations use EXISTING shared SQL/Jobs/cache/event/child ports and
/// must retain owner through actual operations. Missing ports cannot report
/// ready: enabling a Python plugin remains pre-effect guarded by Main.
pub trait Ports: Send + Sync {
    fn ready_for_execution(&self) -> bool;
    fn child_started(&self, identity: Identity, owner: Owner) -> BoxFuture<'_, Result<(), Error>>;
    fn callback(
        &self,
        port: String,
        method: String,
        params: Value,
        scope: Owner,
    ) -> BoxFuture<'_, Result<Value, Error>>;
    /// Must join/reap/verify every actually registered descendant generation,
    /// including process groups created by plugin subprocess APIs. No noop.
    /// Join actual shared SQL/cache/Jobs/event callbacks and close any owned
    /// background SQL sessions after child EOF. No observer-only success.
    fn settle_callbacks(
        &self,
        identity: Identity,
        owner: Owner,
    ) -> BoxFuture<'_, Result<bool, Error>>;
    fn settle_descendants(
        &self,
        identity: Identity,
        forced: bool,
        owner: Owner,
    ) -> BoxFuture<'_, Result<bool, Error>>;
}
struct Request {
    method: String,
    args: Value,
    scope: Owner,
    reply: oneshot::Sender<Result<Value, Error>>,
}
enum Command {
    Call(Request),
    Stop(oneshot::Sender<Result<Option<Receipt>, Error>>),
}
struct Inner {
    sender: mpsc::Sender<Command>,
    closed: Arc<AtomicBool>,
    receipt: Arc<Mutex<Option<Receipt>>>,
    stop: watch::Sender<Option<tokio::time::Instant>>,
}
#[derive(Clone)]
pub struct Host {
    inner: Arc<Inner>,
}
#[derive(Clone)]
pub struct Config {
    pub executable: PathBuf,
}
struct Running {
    child: Child,
    input: Option<ChildStdin>,
    output: mpsc::Receiver<Result<Value, Error>>,
    stdout: tokio::task::JoinHandle<bool>,
    stderr: tokio::task::JoinHandle<(bool, u64)>,
    identity: Identity,
    owner: Owner,
    ports: Arc<dyn Ports>,
}
async fn send<W: AsyncWrite + Unpin>(writer: &mut W, value: &Value) -> Result<(), Error> {
    let raw = serde_json::to_vec(value).map_err(|_| Error::Protocol)?;
    if raw.len() > FRAME {
        return Err(Error::Capacity);
    }
    writer
        .write_u32(raw.len() as u32)
        .await
        .map_err(|_| Error::Child)?;
    writer.write_all(&raw).await.map_err(|_| Error::Child)?;
    writer.flush().await.map_err(|_| Error::Child)
}
async fn receive<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Value>, Error> {
    let mut header = [0; 4];
    let count = reader
        .read(&mut header[..1])
        .await
        .map_err(|_| Error::Child)?;
    if count == 0 {
        return Ok(None);
    }
    reader
        .read_exact(&mut header[1..])
        .await
        .map_err(|_| Error::Child)?;
    let length = u32::from_be_bytes(header) as usize;
    if length > FRAME {
        return Err(Error::Capacity);
    }
    let mut raw = vec![0; length];
    reader
        .read_exact(&mut raw)
        .await
        .map_err(|_| Error::Child)?;
    serde_json::from_slice(&raw)
        .map(Some)
        .map_err(|_| Error::Protocol)
}
fn identity(pid: u32) -> Result<Identity, Error> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).map_err(|_| Error::Child)?;
    let start_generation = stat
        .rsplit_once(')')
        .and_then(|v| v.1.split_whitespace().nth(19))
        .ok_or(Error::Child)?
        .to_owned();
    Ok(Identity {
        pid,
        start_generation,
        pid_namespace: std::fs::read_link("/proc/self/ns/pid").map_err(|_| Error::Child)?,
    })
}
impl Running {
    async fn spawn(
        config: &Config,
        ports: Arc<dyn Ports>,
        owner: Owner,
        stopping: &mut watch::Receiver<Option<tokio::time::Instant>>,
    ) -> Result<Self, Error> {
        use std::os::unix::process::CommandExt;
        if !ports.ready_for_execution() {
            return Err(Error::Unavailable);
        }
        let mut command = tokio::process::Command::new(&config.executable);
        command
            .arg("--mode")
            .arg("regex")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command.as_std_mut().process_group(0);
        let mut child = command.spawn().map_err(|_| Error::Child)?;
        let pid = child.id().ok_or(Error::Child)?;
        let identity = match identity(pid) {
            Ok(identity) => identity,
            Err(error) => {
                // A process can exit before /proc identity is readable. Keep
                // ownership through its real wait and both pipe drains anyway.
                drop(child.stdin.take());
                let _ = child.start_kill();
                let stdout = child.stdout.take();
                let stderr = child.stderr.take();
                let drain = |pipe| async move {
                    if let Some(mut pipe) = pipe {
                        let mut sink = tokio::io::sink();
                        let _ = tokio::io::copy(&mut pipe, &mut sink).await;
                    }
                };
                let (_, _, _) = tokio::join!(child.wait(), drain(stdout), async move {
                    if let Some(mut pipe) = stderr {
                        let mut sink = tokio::io::sink();
                        let _ = tokio::io::copy(&mut pipe, &mut sink).await;
                    }
                });
                return Err(error);
            }
        };
        let input = child.stdin.take().ok_or(Error::Child)?;
        let mut output = child.stdout.take().ok_or(Error::Child)?;
        // One uncancelled reader owns the physical stream. Selecting between
        // a command and a partial read_exact would corrupt the next frame.
        let (frames, output_rx) = mpsc::channel(2);
        let scope = owner.clone();
        let stdout = tokio::spawn(async move {
            let _scope = scope;
            loop {
                match receive(&mut output).await {
                    Ok(Some(value)) => {
                        if frames.send(Ok(value)).await.is_err() {
                            return false;
                        }
                    }
                    Ok(None) => return true,
                    Err(error) => {
                        let _ = frames.send(Err(error)).await;
                        return false;
                    }
                }
            }
        });
        let mut error = child.stderr.take().ok_or(Error::Child)?;
        let scope = owner.clone();
        let stderr = tokio::spawn(async move {
            let _scope = scope;
            let mut bytes = 0u64;
            let mut frame = [0; 8192];
            loop {
                match error.read(&mut frame).await {
                    Ok(0) => return (true, bytes),
                    Ok(count) => bytes = bytes.saturating_add(count as u64),
                    Err(_) => return (false, bytes),
                }
            }
        });
        let mut running = Self {
            child,
            input: Some(input),
            output: output_rx,
            stdout,
            stderr,
            identity: identity.clone(),
            owner: owner.clone(),
            ports: ports.clone(),
        };
        if ports.child_started(identity, owner.clone()).await.is_err() {
            let _ = running.retire(true).await;
            return Err(Error::Child);
        }
        let ready = async {
            loop {
                let frame = running.next_frame().await?;
                if frame["kind"] == "callback" {
                    running.callback(frame, owner.clone()).await?;
                } else {
                    return Ok::<_, Error>(frame);
                }
            }
        };
        let ready = tokio::select! {
            ready=tokio::time::timeout(Duration::from_secs(90), ready) => ready.map_err(|_| Error::Child).and_then(|v|v),
            _=stop_deadline(stopping) => Err(Error::Drain),
        };
        match ready {
            Ok(value) if value["kind"] == "ready" && value["actual_pid"] == pid => Ok(running),
            _ => {
                let _ = running.retire(true).await;
                Err(Error::Protocol)
            }
        }
    }
    async fn next_frame(&mut self) -> Result<Value, Error> {
        self.output.recv().await.ok_or(Error::Child)?
    }
    async fn callback(&mut self, frame: Value, scope: Owner) -> Result<(), Error> {
        let id = frame["id"].as_u64().ok_or(Error::Protocol)?;
        let port = frame["port"].as_str().ok_or(Error::Protocol)?.to_owned();
        let method = frame["method"].as_str().ok_or(Error::Protocol)?.to_owned();
        let result = self
            .ports
            .callback(port, method, frame["params"].clone(), scope)
            .await;
        let value = match result {
            Ok(value) => json!({"kind":"callback_result","id":id,"result":value,"error":null}),
            Err(_) => {
                json!({"kind":"callback_result","id":id,"result":null,"error":"Native compatibility callback unavailable"})
            }
        };
        send(self.input.as_mut().ok_or(Error::Closed)?, &value).await
    }
    async fn call(&mut self, method: &str, args: Value, scope: Owner) -> Result<Value, Error> {
        let id = uuid::Uuid::new_v4().to_string();
        send(
            self.input.as_mut().ok_or(Error::Closed)?,
            &json!({"kind":"call","id":id,"method":method,"args":args}),
        )
        .await?;
        loop {
            let frame = self.next_frame().await?;
            match frame["kind"].as_str() {
                Some("callback") => {
                    let scope = if frame["operation"] == id {
                        scope.clone()
                    } else {
                        self.owner.clone()
                    };
                    // Background callbacks retain the whole host scope; the
                    // port MUST admit from existing budgets before new work.
                    self.callback(frame, scope).await?;
                }
                Some("result") if frame["id"] == id => {
                    return if frame["error"].is_null() {
                        Ok(frame["result"].clone())
                    } else {
                        Err(Error::Remote(frame["error"].clone()))
                    }
                }
                _ => return Err(Error::Protocol),
            }
        }
    }
    async fn retire(mut self, mut forced: bool) -> Result<Receipt, Error> {
        if let Some(mut input) = self.input.take() {
            let _ = input.shutdown().await;
            drop(input);
        }
        if forced {
            let _ = self.child.start_kill();
        }
        let status = match tokio::time::timeout(Duration::from_secs(30), self.child.wait()).await {
            Ok(result) => result.map_err(|_| Error::Drain)?,
            Err(_) => {
                forced = true;
                let _ = self.child.start_kill();
                self.child.wait().await.map_err(|_| Error::Drain)?
            }
        };
        // Registered descendants must settle before inherited pipe EOF.
        let mut descendants = self
            .ports
            .settle_descendants(self.identity.clone(), forced, self.owner.clone())
            .await?;
        if !descendants && !forced {
            forced = true;
            descendants = self
                .ports
                .settle_descendants(self.identity.clone(), true, self.owner.clone())
                .await?;
        }
        let mut trailing = false;
        while let Some(frame) = self.output.recv().await {
            trailing = true;
            if frame.is_err() {
                forced = true;
            }
        }
        let stdout = self.stdout.await.map_err(|_| Error::Drain)?;
        if trailing {
            forced = true;
        }
        let (stderr, stderr_bytes) = self.stderr.await.map_err(|_| Error::Drain)?;
        let callbacks = self
            .ports
            .settle_callbacks(self.identity.clone(), self.owner.clone())
            .await?;
        let receipt = Receipt {
            identity: self.identity,
            actual_exit: status.code(),
            wait_observed: true,
            stdout_eof: stdout,
            stderr_eof: stderr,
            stderr_bytes,
            forced,
            descendants_absent: descendants,
            callbacks_settled: callbacks,
        };
        Ok(receipt)
    }
}
async fn stop_deadline(receiver: &mut watch::Receiver<Option<tokio::time::Instant>>) {
    loop {
        let deadline = *receiver.borrow();
        if let Some(deadline) = deadline {
            tokio::select! {
                _=tokio::time::sleep_until(deadline)=>return,
                changed=receiver.changed()=>{
                    if changed.is_err(){tokio::time::sleep_until(deadline).await;return;}
                },
            }
            continue;
        }
        if receiver.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}
impl Host {
    pub fn prepare(config: Config, ports: Arc<dyn Ports>, lifetime: Owner) -> Self {
        let (sender, mut receiver) = mpsc::channel::<Command>(16);
        let (stop, mut stopping) = watch::channel(None);
        let inner = Arc::new(Inner {
            sender,
            closed: Arc::new(AtomicBool::new(false)),
            receipt: Arc::new(Mutex::new(None)),
            stop,
        });
        let closed = inner.closed.clone();
        let receipts = inner.receipt.clone();
        // Only an inert protocol actor starts here, not CPython/Source. The
        // first admitted regex operation is the sole child spawn point.
        tokio::spawn(async move {
            let mut running: Option<Running> = None;
            loop {
                let command = if let Some(child) = &mut running {
                    tokio::select! {
                        command=receiver.recv()=>command,
                        _=stop_deadline(&mut stopping)=>{closed.store(true,Ordering::Release);break;},
                        frame=child.next_frame()=>{
                            match frame{Ok(frame)if frame["kind"]=="callback"=>{
                                let completed=tokio::select!{result=child.callback(frame,lifetime.clone())=>result,_=stop_deadline(&mut stopping)=>Err(Error::Drain)};
                                if completed.is_err(){closed.store(true,Ordering::Release);break;}
                            },_=>{closed.store(true,Ordering::Release);break;}}
                            continue;
                        }
                    }
                } else {
                    tokio::select! {
                        command=receiver.recv()=>command,
                        _=stop_deadline(&mut stopping)=>{closed.store(true,Ordering::Release);break;},
                    }
                };
                match command {
                    Some(Command::Call(request)) => {
                        if closed.load(Ordering::Acquire) {
                            let _ = request.reply.send(Err(Error::Closed));
                            continue;
                        }
                        let caller = request.scope;
                        if running.is_none() {
                            match Running::spawn(
                                &config,
                                ports.clone(),
                                lifetime.clone(),
                                &mut stopping,
                            )
                            .await
                            {
                                Ok(child) => running = Some(child),
                                Err(error) => {
                                    let _ = request.reply.send(Err(error));
                                    continue;
                                }
                            }
                        }
                        let child = running.as_mut().expect("owned spawned child");
                        let result = tokio::select! {
                            result=child.call(&request.method,request.args,caller.clone())=>result,
                            _=stop_deadline(&mut stopping)=>Err(Error::Drain),
                        };
                        let failed = result.is_err() && !matches!(&result, Err(Error::Remote(_)));
                        if failed {
                            // Preserve observer/caller ownership until actual child,
                            // pipe and descendant settlement, even on failure.
                            if let Some(child) = running.take() {
                                if let Ok(receipt) = child.retire(true).await {
                                    *receipts.lock().await = Some(receipt);
                                }
                            }
                            closed.store(true, Ordering::Release);
                        }
                        let _ = request.reply.send(result);
                        // Retain the SAME admitted caller through all physical
                        // replies, or forced child wait/EOF on observer loss.
                        drop(caller);
                        if failed {
                            break;
                        }
                    }
                    Some(Command::Stop(reply)) => {
                        let result = if let Some(mut child) = running.take() {
                            let stopped = tokio::select! {
                                result=child.call("shutdown",Value::Null,lifetime.clone())=>result.is_ok(),
                                _=stop_deadline(&mut stopping)=>false,
                            };
                            child.retire(!stopped).await.map(Some)
                        } else {
                            Ok(None)
                        };
                        if let Ok(Some(receipt)) = &result {
                            *receipts.lock().await = Some(receipt.clone());
                        }
                        let _ = reply.send(result);
                        break;
                    }
                    None => break,
                }
            }
            if let Some(child) = running {
                if let Ok(receipt) = child.retire(true).await {
                    *receipts.lock().await = Some(receipt);
                }
            }
        });
        Self { inner }
    }
    pub async fn call(&self, method: String, args: Value, scope: Owner) -> Result<Value, Error> {
        if self.inner.closed.load(Ordering::Acquire)
            || self
                .inner
                .stop
                .borrow()
                .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
        {
            return Err(Error::Closed);
        }
        let (reply, receive) = oneshot::channel();
        self.inner
            .sender
            .try_send(Command::Call(Request {
                method,
                args,
                scope,
                reply,
            }))
            .map_err(|_| Error::Capacity)?;
        // Losing this observer does not abort the actor, callback or child.
        receive.await.map_err(|_| Error::Child)?
    }
    pub async fn last_receipt(&self) -> Option<Receipt> {
        self.inner.receipt.lock().await.clone()
    }
    pub fn stop_admission(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }
    /// Deadline signal only. Already admitted task operations remain allowed
    /// until this actual deadline; no child/EOF/completion receipt is minted.
    pub fn interrupt_at(&self, deadline: tokio::time::Instant) {
        // The actor's normal completion and Main drain can race. Update the
        // SAME watch value under its lock so a later call cannot extend it.
        self.inner.stop.send_if_modified(|current| {
            if current.is_some_and(|old| old <= deadline) {
                return false;
            }
            *current = Some(deadline);
            true
        });
        if self
            .inner
            .stop
            .borrow()
            .is_some_and(|next| tokio::time::Instant::now() >= next)
        {
            self.stop_admission();
        }
    }

    pub async fn shutdown(&self, budget: Duration) -> Result<Option<Receipt>, Error> {
        self.stop_admission();
        self.interrupt_at(tokio::time::Instant::now() + budget);
        let (reply, receive) = oneshot::channel();
        self.inner
            .sender
            .send(Command::Stop(reply))
            .await
            .map_err(|_| Error::Closed)?;
        let value = tokio::time::timeout(budget, receive)
            .await
            .map_err(|_| Error::Drain)?
            .map_err(|_| Error::Child)??;
        if value.as_ref().is_some_and(|r| {
            r.forced
                || r.actual_exit != Some(0)
                || !r.stdout_eof
                || !r.stderr_eof
                || !r.descendants_absent
                || !r.callbacks_settled
        }) {
            return Err(Error::Drain);
        }
        Ok(value)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn earlier_owned_drain_deadline_interrupts_an_already_waiting_actor() {
        let (sender, mut receiver) = watch::channel(Some(
            tokio::time::Instant::now() + Duration::from_secs(3600),
        ));
        let waiter = tokio::spawn(async move {
            stop_deadline(&mut receiver).await;
        });
        tokio::task::yield_now().await;
        sender.send_replace(Some(tokio::time::Instant::now()));
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn rpc_fragmented_header_is_owned_until_complete_and_truncation_is_not_eof() {
        let (mut writer, mut reader) = tokio::io::duplex(128);
        let output = tokio::spawn(async move {
            let raw = br#"{"kind":"ready"}"#;
            for byte in (raw.len() as u32).to_be_bytes() {
                writer.write_all(&[byte]).await.unwrap();
                tokio::task::yield_now().await;
            }
            writer.write_all(raw).await.unwrap();
        });
        assert_eq!(
            receive(&mut reader).await.unwrap(),
            Some(json!({"kind":"ready"}))
        );
        assert!(receive(&mut reader).await.unwrap().is_none());
        output.await.unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer.write_all(&[0, 1]).await.unwrap();
        drop(writer);
        assert!(matches!(receive(&mut reader).await, Err(Error::Child)));
    }
    #[tokio::test]
    async fn rpc_rejects_oversized_header_before_reading_or_allocating_body() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer.write_u32(FRAME as u32 + 1).await.unwrap();
        assert!(matches!(receive(&mut reader).await, Err(Error::Capacity)));
    }
}
