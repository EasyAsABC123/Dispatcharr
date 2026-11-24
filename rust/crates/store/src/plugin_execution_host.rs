//! Lazy owned CPython protocol actor. No process starts on prepare(), no Source
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
const DELETE_ENABLED: &str = "delete_enabled";
const FRAME: usize = 8 * 1024 * 1024;
#[path = "plugin_host_diagnostics.rs"]
pub(crate) mod diagnostics;
type Owner = Arc<dyn Send + Sync>;
/// Actual invocation admission. Existing HTTP/Jobs callers supply their SAME
/// retained session; background callbacks have none and must acquire from the
/// same Auth4/pool8 before any SQL. No callback may guess an operation's pool.
#[derive(Clone)]
pub struct Invocation {
    pub owner: Owner,
    pub sql: Option<crate::TaskSqlSession>,
}

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
/// Private actual same-thread callback reentry, created only by the owned
/// actor's live pipe/receipt registry. It cannot be minted by a JSON caller.
pub trait Reentry: Send + Sync {
    fn invoke(&self, method: String, args: Value) -> BoxFuture<'_, Result<Value, Error>>;
}
#[derive(Default)]
struct NestedState {
    serial: std::sync::atomic::AtomicU64,
    pending:
        std::sync::Mutex<std::collections::BTreeMap<u64, oneshot::Sender<Result<Value, Error>>>>,
}
struct NestedInvoker {
    state: Arc<NestedState>,
    input: Arc<Mutex<ChildStdin>>,
    callback: u64,
}
impl Reentry for NestedInvoker {
    fn invoke(&self, method: String, args: Value) -> BoxFuture<'_, Result<Value, Error>> {
        Box::pin(async move {
            let id = self
                .state
                .serial
                .fetch_add(1, Ordering::AcqRel)
                .checked_add(1)
                .ok_or(Error::Capacity)?;
            let (send_result, receive) = oneshot::channel();
            {
                let mut pending = self.state.pending.lock().map_err(|_| Error::Protocol)?;
                if pending.len() >= 64 {
                    return Err(Error::Capacity);
                }
                pending.insert(id, send_result);
            }
            let result=send(&mut*self.input.lock().await,&json!({"kind":"callback_invoke","callback":self.callback,"id":id,"method":method,"args":args})).await;
            if result.is_err() {
                self.state
                    .pending
                    .lock()
                    .map_err(|_| Error::Protocol)?
                    .remove(&id);
                return result.map(|_| Value::Null);
            }
            receive.await.map_err(|_| Error::Child)?
        })
    }
}
pub trait Ports: Send + Sync {
    fn ready_for_execution(&self) -> bool;
    fn child_started(&self, identity: Identity, owner: Owner) -> BoxFuture<'_, Result<(), Error>>;
    fn callback(
        &self,
        port: String,
        method: String,
        params: Value,
        scope: Invocation,
    ) -> BoxFuture<'_, Result<Value, Error>>;
    fn callback_reentrant(
        &self,
        port: String,
        method: String,
        params: Value,
        scope: Invocation,
        _reentry: Arc<dyn Reentry>,
    ) -> BoxFuture<'_, Result<Value, Error>> {
        self.callback(port, method, params, scope)
    }
    /// Must join/reap/verify every actually registered descendant generation,
    /// including process groups created by plugin subprocess APIs. No noop.
    /// Join actual shared SQL/cache/Jobs/event callbacks and close any owned
    /// background SQL sessions after child EOF. No observer-only success.
    fn close_callback_admission(
        &self,
        identity: Identity,
        owner: Owner,
    ) -> BoxFuture<'_, Result<(), Error>>;
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
    scope: Invocation,
    reply: oneshot::Sender<Result<Value, Error>>,
    required_child: Option<Identity>,
    _activity: Option<Owner>,
}
impl Request {
    fn may_use(&self, running: Option<&Running>) -> bool {
        self.required_child.as_ref().is_none_or(|expected| {
            running.is_some_and(|actual| {
                super::plugin_python_registry::same(&actual.identity, expected)
                    && super::plugin_python_registry::live(&actual.identity)
            })
        })
    }
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
    activity: Arc<tokio::sync::RwLock<()>>,
}
/// Exclusive process-generation reservation for the existing action host only.
pub(crate) struct TasklessHostHold {
    host: Arc<Inner>,
    identity: Identity,
    permit: Arc<tokio::sync::OwnedRwLockWriteGuard<()>>,
}
impl TasklessHostHold {
    pub(crate) fn is_live(&self) -> bool {
        !self.host.closed.load(Ordering::Acquire)
            && super::plugin_python_registry::live(&self.identity)
    }
}
#[derive(Clone)]
pub struct Host {
    inner: Arc<Inner>,
}
#[derive(Clone, Copy)]
pub enum Mode {
    Plugin,
    Regex,
}
#[derive(Clone)]
pub struct Config {
    pub mode: Mode,
    pub(crate) role: super::plugin_python_registry::HostRole,
    pub executable: PathBuf,
    pub source: PathBuf,
    pub plugins: PathBuf,
}
struct Running {
    child: Child,
    input: Option<Arc<Mutex<ChildStdin>>>,
    callbacks: tokio::task::JoinSet<Result<(), Error>>,
    nested: Arc<NestedState>,
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
        startup_scope: Invocation,
    ) -> Result<Self, Error> {
        let result = Self::spawn_owned(config, ports, owner, startup_scope).await;
        if let Err(error) = &result {
            diagnostics::failure("spawn_result", "startup", error);
        }
        result
    }
    async fn spawn_owned(
        config: &Config,
        ports: Arc<dyn Ports>,
        owner: Owner,
        startup_scope: Invocation,
    ) -> Result<Self, Error> {
        use std::os::unix::process::CommandExt;
        if !ports.ready_for_execution() {
            diagnostics::failure("ports_ready", "startup", &Error::Unavailable);
            return Err(Error::Unavailable);
        }
        diagnostics::stage(
            "spawn_requested",
            match config.role {
                super::plugin_python_registry::HostRole::Actions => "actions",
                super::plugin_python_registry::HostRole::Tasks => "tasks",
            },
        );
        let mut command = tokio::process::Command::new(&config.executable);
        command
            .arg("--mode")
            .arg(match config.mode {
                Mode::Plugin => "plugin",
                Mode::Regex => "regex",
            })
            .arg("--role")
            .arg(match config.role {
                super::plugin_python_registry::HostRole::Actions => "actions",
                super::plugin_python_registry::HostRole::Tasks => "tasks",
            })
            .arg("--source")
            .arg(&config.source)
            .arg("--plugins")
            .arg(&config.plugins)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command.env(
            "DISPATCHARR_NATIVE_PORTS_READY",
            "sql,cache,jobs,events,process",
        );
        command.as_std_mut().process_group(0);
        let mut child = command.spawn().map_err(|_| {
            diagnostics::failure("spawn_process", "startup", &Error::Child);
            Error::Child
        })?;
        let pid = child.id().ok_or(Error::Child)?;
        let identity = identity(pid).inspect_err(|error| {
            diagnostics::failure("spawn_identity", "startup", error);
        })?;
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
            let mut diagnostic = diagnostics::StderrScanner::new();
            loop {
                match error.read(&mut frame).await {
                    Ok(0) => {
                        diagnostic.finish();
                        return (true, bytes);
                    }
                    Ok(count) => {
                        bytes = bytes.saturating_add(count as u64);
                        diagnostic.push(&frame[..count]);
                    }
                    Err(_) => {
                        diagnostic.finish();
                        return (false, bytes);
                    }
                }
            }
        });
        let mut running = Self {
            child,
            input: Some(Arc::new(Mutex::new(input))),
            callbacks: tokio::task::JoinSet::new(),
            nested: Arc::new(NestedState::default()),
            output: output_rx,
            stdout,
            stderr,
            identity: identity.clone(),
            owner: owner.clone(),
            ports: ports.clone(),
        };
        if let Err(error) = ports.child_started(identity, owner.clone()).await {
            diagnostics::failure("child_started_port", "startup", &error);
            let _ = running.retire(true).await;
            return Err(Error::Child);
        }
        let ready = async {
            loop {
                let frame = running.next_frame().await?;
                if frame["kind"] == "callback" {
                    let scope = if frame["actual_tid"].as_u64() == Some(u64::from(pid)) {
                        startup_scope.clone()
                    } else {
                        Invocation {
                            owner: owner.clone(),
                            sql: None,
                        }
                    };
                    running.callback(frame, scope).await?;
                } else {
                    return Ok::<_, Error>(frame);
                }
            }
        };
        let ready = tokio::time::timeout(Duration::from_secs(90), ready)
            .await
            .map_err(|_| Error::Child)
            .and_then(|v| v);
        match &ready {
            Err(error) => diagnostics::failure("startup_ready_wait", "startup", error),
            Ok(value) if value["kind"] != "ready" || value["actual_pid"] != pid => {
                diagnostics::failure("startup_ready_frame", "startup", &Error::Protocol)
            }
            _ => diagnostics::stage("startup_ready", "validated"),
        }
        match ready {
            Ok(value) if value["kind"] == "ready" && value["actual_pid"] == pid => Ok(running),
            _ => {
                let _ = running.retire(true).await;
                Err(Error::Protocol)
            }
        }
    }
    async fn next_frame(&mut self) -> Result<Value, Error> {
        loop {
            tokio::select! {
                biased;
                completion=self.callbacks.join_next(),if !self.callbacks.is_empty()=>{
                    completion.ok_or(Error::Callback)?.map_err(|_|Error::Callback)??;
                },
                frame=self.output.recv()=>{
                    let frame=frame.ok_or(Error::Child)??;
                    if frame["kind"]=="callback_nested_result" {
                        let id=frame["id"].as_u64().ok_or(Error::Protocol)?;
                        let sender=self.nested.pending.lock().map_err(|_|Error::Protocol)?.remove(&id).ok_or(Error::Protocol)?;
                        let result=if frame["error"].is_null(){Ok(frame["result"].clone())}else{Err(Error::Remote(frame["error"].clone()))};
                        let _=sender.send(result);continue;
                    }
                    return Ok(frame);
                },
            }
        }
    }
    async fn callback(&mut self, frame: Value, scope: Invocation) -> Result<(), Error> {
        if self.callbacks.len() >= 64 {
            return Err(Error::Capacity);
        }
        let id = frame["id"].as_u64().ok_or(Error::Protocol)?;
        let port = frame["port"].as_str().ok_or(Error::Protocol)?.to_owned();
        let method = frame["method"].as_str().ok_or(Error::Protocol)?.to_owned();
        let params = frame["params"].clone();
        let diagnostic_stage = match port.as_str() {
            "sql" => "callback_sql",
            "cache" => "callback_cache",
            "jobs" => "callback_jobs",
            "jobs_backend" => "callback_jobs_backend",
            "events" => "callback_events",
            "process" => "callback_process",
            _ => "callback_other",
        };
        let diagnostic_method = diagnostics::operation(&method);
        let ports = self.ports.clone();
        let input = self.input.as_ref().ok_or(Error::Closed)?.clone();
        let reentry: Arc<dyn Reentry> = Arc::new(NestedInvoker {
            state: self.nested.clone(),
            input: input.clone(),
            callback: id,
        });
        self.callbacks.spawn(async move{
            // Each actual callback retains invocation owner/session through its
            // physical port result AND actual framed reply flush. An infinite
            // logical PubSub read cannot block receipt of unsubscribe/SQL frames.
            let retained_scope=scope.clone();
            let result=ports.callback_reentrant(port,method,params,scope,reentry).await;
            if let Err(error)=&result { diagnostics::failure(diagnostic_stage,diagnostic_method,error); }
            let value=match result{Ok(value)=>json!({"kind":"callback_result","id":id,"result":value,"error":null}),Err(Error::Remote(error))=>json!({"kind":"callback_result","id":id,"result":null,"error":error}),Err(_)=>json!({"kind":"callback_result","id":id,"result":null,"error":"Native compatibility callback unavailable"})};
            let flushed=send(&mut*input.lock().await,&value).await;
            drop(retained_scope);flushed
        });
        Ok(())
    }
    fn thread_generation(&self, tid: u32) -> Result<String, Error> {
        let raw = std::fs::read_to_string(format!("/proc/{}/task/{tid}/stat", self.identity.pid))
            .map_err(|_| Error::Protocol)?;
        let (_, tail) = raw.rsplit_once(')').ok_or(Error::Protocol)?;
        tail.split_whitespace()
            .nth(19)
            .map(str::to_owned)
            .ok_or(Error::Protocol)
    }
    async fn call(&mut self, method: &str, args: Value, scope: Invocation) -> Result<Value, Error> {
        let id = uuid::Uuid::new_v4().to_string();
        send(
            &mut *self.input.as_ref().ok_or(Error::Closed)?.lock().await,
            &json!({"kind":"call","id":id,"method":method,"args":args}),
        )
        .await?;
        let mut invocation_thread: Option<(u32, String)> = None;
        loop {
            let frame = self.next_frame().await?;
            match frame["kind"].as_str() {
                Some("operation_started") if frame["id"] == id => {
                    let tid = frame["actual_tid"]
                        .as_u64()
                        .and_then(|v| u32::try_from(v).ok())
                        .ok_or(Error::Protocol)?;
                    invocation_thread = Some((tid, self.thread_generation(tid)?));
                }
                Some("callback") => {
                    let same_thread =
                        invocation_thread.as_ref().is_some_and(|(tid, generation)| {
                            frame["actual_tid"].as_u64() == Some(u64::from(*tid))
                                && self.thread_generation(*tid).ok().as_ref() == Some(generation)
                        });
                    let scope = if frame["operation"] == id && same_thread {
                        scope.clone()
                    } else {
                        Invocation {
                            owner: self.owner.clone(),
                            sql: None,
                        }
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
    async fn retire(self, forced: bool) -> Result<Receipt, Error> {
        let result = self.retire_owned(forced).await;
        if let Err(error) = &result {
            diagnostics::failure("physical_retire_error", "shutdown", error);
        }
        result
    }
    async fn retire_owned(mut self, mut forced: bool) -> Result<Receipt, Error> {
        // Wake nested native callbacks before joining them; pipe/child ownership
        // still remains with this retirement supervisor through actual wait/EOF.
        self.nested
            .pending
            .lock()
            .map_err(|_| Error::Protocol)?
            .clear();
        let callbacks_closed = self
            .ports
            .close_callback_admission(self.identity.clone(), self.owner.clone())
            .await
            .is_ok();
        if !callbacks_closed {
            forced = true;
        }
        let mut callback_joined = callbacks_closed;
        while let Some(result) = self.callbacks.join_next().await {
            if !matches!(result, Ok(Ok(()))) {
                callback_joined = false;
                forced = true;
            }
        }
        if let Some(input) = self.input.take() {
            let _ = input.lock().await.shutdown().await;
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
            callbacks_settled: callbacks && callback_joined,
        };
        diagnostics::retired(&receipt);
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
            activity: Arc::new(tokio::sync::RwLock::new(())),
        });
        let closed = inner.closed.clone();
        let receipts = inner.receipt.clone();
        // Only an inert protocol actor starts here, not CPython/Source. The
        // first admitted enabled operation is the sole child spawn point.
        tokio::spawn(async move {
            if config.role == super::plugin_python_registry::HostRole::Actions {
                drive_actions(
                    config, ports, lifetime, receiver, stopping, closed, receipts,
                )
                .await;
                return;
            }
            let mut running: Option<Running> = None;
            loop {
                let command = if let Some(child) = &mut running {
                    tokio::select! {
                        _=stop_deadline(&mut stopping)=>{closed.store(true,Ordering::Release);break;},
                        command=receiver.recv()=>command,
                        frame=child.next_frame()=>{
                            match frame{Ok(frame)if frame["kind"]=="callback"=>{
                                let completed=tokio::select!{result=child.callback(frame,Invocation{owner:lifetime.clone(),sql:None})=>result,_=stop_deadline(&mut stopping)=>Err(Error::Drain)};
                                if completed.is_err(){closed.store(true,Ordering::Release);break;}
                            },_=>{closed.store(true,Ordering::Release);break;}}
                            continue;
                        }
                    }
                } else {
                    tokio::select! {_ =stop_deadline(&mut stopping)=>{closed.store(true,Ordering::Release);break;},command=receiver.recv()=>command}
                };
                match command {
                    Some(Command::Call(request)) => {
                        if !request.may_use(running.as_ref()) {
                            let _ = request.reply.send(Err(Error::Closed));
                            continue;
                        }
                        if running.is_none() {
                            match Running::spawn(
                                &config,
                                ports.clone(),
                                lifetime.clone(),
                                request.scope.clone(),
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
                            result=child.call(&request.method,request.args,request.scope.clone())=>result,
                            _=stop_deadline(&mut stopping)=>Err(Error::Drain),
                        };
                        if let Err(error) = &result {
                            diagnostics::failure("call_result", &request.method, error);
                        }
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
                        if failed {
                            break;
                        }
                    }
                    Some(Command::Stop(reply)) => {
                        let result = if let Some(mut child) = running.take() {
                            let stopped = tokio::select! {
                                result=child.call("shutdown",Value::Null,Invocation{owner:lifetime.clone(),sql:None})=>result.is_ok(),
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
    pub async fn call(
        &self,
        method: String,
        args: Value,
        scope: Invocation,
    ) -> Result<Value, Error> {
        if method == DELETE_ENABLED {
            return Err(Error::Unavailable);
        }
        self.call_bound(method, args, scope, None, None).await
    }
    pub(crate) fn try_hold_taskless_child(
        &self,
        identity: Identity,
    ) -> Result<TasklessHostHold, Error> {
        let permit = self
            .inner
            .activity
            .clone()
            .try_write_owned()
            .map_err(|_| Error::Capacity)?;
        let hold = TasklessHostHold {
            host: self.inner.clone(),
            identity,
            permit: Arc::new(permit),
        };
        if !hold.is_live() {
            return Err(Error::Closed);
        }
        Ok(hold)
    }
    pub(crate) async fn observe_taskless_module(
        &self,
        key: String,
        identity: Identity,
        scope: Invocation,
        hold: Option<&TasklessHostHold>,
    ) -> Result<Value, Error> {
        if let Some(hold) = hold {
            if !Arc::ptr_eq(&self.inner, &hold.host)
                || !hold.is_live()
                || !super::plugin_python_registry::same(&identity, &hold.identity)
            {
                return Err(Error::Closed);
            }
        }
        self.call_bound(
            "observe_taskless_module".into(),
            json!({"key": key}),
            scope,
            Some(identity),
            hold.map(|hold| hold.permit.clone() as Owner),
        )
        .await
    }
    pub(crate) async fn delete_enabled(
        &self,
        owner: Arc<super::plugin_runtime::EnabledDeleteOwner>,
        load_sequence: u64,
    ) -> Result<Value, Error> {
        let (key, session, hold) = owner.dispatch().map_err(|_| Error::Unavailable)?;
        if !Arc::ptr_eq(&self.inner, &hold.host) || !hold.is_live() {
            return Err(Error::Closed);
        }
        let args = json!({
            "key": key,
            "load_sequence": load_sequence,
            "mutation_lock_held": true
        });
        let required_child = hold.identity.clone();
        let permit = hold.permit.clone();
        let sql = session.clone();
        self.call_bound(
            DELETE_ENABLED.into(),
            args,
            Invocation {
                owner,
                sql: Some(sql),
            },
            Some(required_child),
            Some(permit),
        )
        .await
    }

    async fn call_bound(
        &self,
        method: String,
        args: Value,
        scope: Invocation,
        required_child: Option<Identity>,
        held: Option<Owner>,
    ) -> Result<Value, Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let activity = match held {
            Some(hold) => hold,
            None => Arc::new(
                self.inner
                    .activity
                    .clone()
                    .try_read_owned()
                    .map_err(|_| Error::Capacity)?,
            ),
        };
        let (reply, receive) = oneshot::channel();
        self.inner
            .sender
            .try_send(Command::Call(Request {
                method,
                args,
                scope,
                reply,
                required_child,
                _activity: Some(activity),
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

struct ActionPending {
    request: Request,
    thread: Option<(u32, String)>,
}
enum ActionEvent {
    Command(Option<Command>),
    Frame(Result<Value, Error>),
    Deadline,
}
/// Multiplex already-admitted actions without opening another backend server.
/// Four is the SAME original Auth4 admission, not another semaphore/pool. The
/// Source reference web role is gevent-concurrent even with one process.
async fn drive_actions(
    config: Config,
    ports: Arc<dyn Ports>,
    lifetime: Owner,
    mut receiver: mpsc::Receiver<Command>,
    mut stopping: watch::Receiver<Option<tokio::time::Instant>>,
    closed: Arc<AtomicBool>,
    receipts: Arc<Mutex<Option<Receipt>>>,
) {
    let mut running: Option<Running> = None;
    let mut pending = std::collections::BTreeMap::<String, ActionPending>::new();
    let mut stop_reply = None;
    let mut forced = false;
    loop {
        if stop_reply.is_some() && pending.is_empty() {
            break;
        }
        let event = if let Some(child) = running.as_mut() {
            tokio::select! {
             biased;
             _=stop_deadline(&mut stopping)=>ActionEvent::Deadline,
             command=receiver.recv()=>ActionEvent::Command(command),
             frame=child.next_frame()=>ActionEvent::Frame(frame),
            }
        } else {
            tokio::select! {_=stop_deadline(&mut stopping)=>ActionEvent::Deadline,command=receiver.recv()=>ActionEvent::Command(command)}
        };
        match event {
            ActionEvent::Deadline => {
                forced = true;
                break;
            }
            ActionEvent::Command(None) => {
                forced = !pending.is_empty();
                break;
            }
            ActionEvent::Command(Some(Command::Stop(reply))) => {
                if stop_reply.is_some() {
                    let _ = reply.send(Err(Error::Closed));
                } else {
                    stop_reply = Some(reply);
                }
            }
            ActionEvent::Command(Some(Command::Call(request))) => {
                if !request.may_use(running.as_ref()) {
                    let _ = request.reply.send(Err(Error::Closed));
                    continue;
                }
                if stop_reply.is_some() {
                    let _ = request.reply.send(Err(Error::Closed));
                    continue;
                }
                if pending.len() >= 4 {
                    let _ = request.reply.send(Err(Error::Capacity));
                    continue;
                }
                if running.is_none() {
                    match Running::spawn(
                        &config,
                        ports.clone(),
                        lifetime.clone(),
                        request.scope.clone(),
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
                let id = uuid::Uuid::new_v4().to_string();
                let child = running.as_mut().expect("actual spawned action child");
                let sent = match child.input.as_ref() {
                    Some(input) => send(
                        &mut *input.lock().await,
                        &json!({"kind":"call","id":id,"method":request.method.clone(),"args":request.args.clone()}),
                    )
                    .await,
                    None => Err(Error::Closed),
                };
                pending.insert(
                    id,
                    ActionPending {
                        request,
                        thread: None,
                    },
                );
                if sent.is_err() {
                    forced = true;
                    break;
                }
            }
            ActionEvent::Frame(Err(error)) => {
                diagnostics::failure("actions_frame", "other", &error);
                forced = true;
                break;
            }
            ActionEvent::Frame(Ok(frame)) => {
                let child = running.as_mut().expect("actual action frame owner");
                match frame["kind"].as_str() {
                    Some("operation_started") => {
                        let Some(id) = frame["id"].as_str() else {
                            forced = true;
                            break;
                        };
                        let Some(operation) = pending.get_mut(id) else {
                            forced = true;
                            break;
                        };
                        let Some(tid) = frame["actual_tid"]
                            .as_u64()
                            .and_then(|n| u32::try_from(n).ok())
                        else {
                            forced = true;
                            break;
                        };
                        match child.thread_generation(tid) {
                            Ok(generation) => operation.thread = Some((tid, generation)),
                            Err(_) => {
                                forced = true;
                                break;
                            }
                        }
                    }
                    Some("callback") => {
                        let operation = frame["operation"].as_str().and_then(|id| pending.get(id));
                        let same_thread = operation.and_then(|p| p.thread.as_ref()).is_some_and(
                            |(tid, generation)| {
                                frame["actual_tid"].as_u64() == Some(u64::from(*tid))
                                    && child.thread_generation(*tid).ok().as_ref()
                                        == Some(generation)
                            },
                        );
                        let scope = if same_thread {
                            operation
                                .expect("verified active operation")
                                .request
                                .scope
                                .clone()
                        } else {
                            Invocation {
                                owner: lifetime.clone(),
                                sql: None,
                            }
                        };
                        if child.callback(frame, scope).await.is_err() {
                            forced = true;
                            break;
                        }
                    }
                    Some("result") => {
                        let Some(id) = frame["id"].as_str() else {
                            forced = true;
                            break;
                        };
                        let Some(operation) = pending.remove(id) else {
                            forced = true;
                            break;
                        };
                        let result = if frame["error"].is_null() {
                            Ok(frame["result"].clone())
                        } else {
                            Err(Error::Remote(frame["error"].clone()))
                        };
                        if let Err(error) = &result {
                            diagnostics::failure(
                                "actions_call_result",
                                &operation.request.method,
                                error,
                            );
                        }
                        let _ = operation.request.reply.send(result);
                    }
                    _ => {
                        forced = true;
                        break;
                    }
                }
            }
        }
    }
    closed.store(true, Ordering::Release);
    receiver.close();
    let outcome = if let Some(mut child) = running {
        if !forced && pending.is_empty() {
            let scope = Invocation {
                owner: lifetime,
                sql: None,
            };
            let shutdown = tokio::select! {result=child.call("shutdown",json!({}),scope)=>result,_=stop_deadline(&mut stopping)=>Err(Error::Drain)};
            if shutdown.is_err() {
                forced = true;
            }
        }
        match child.retire(forced).await {
            Ok(receipt) => {
                let success = !receipt.forced
                    && receipt.actual_exit == Some(0)
                    && receipt.wait_observed
                    && receipt.stdout_eof
                    && receipt.stderr_eof
                    && receipt.descendants_absent
                    && receipt.callbacks_settled;
                *receipts.lock().await = Some(receipt.clone());
                if success {
                    Ok(Some(receipt))
                } else {
                    Err(Error::Drain)
                }
            }
            Err(error) => Err(error),
        }
    } else {
        Ok(None)
    };
    // Pending caller/session owners remain retained until the ACTUAL child and
    // callbacks retire, including when every HTTP response observer disappeared.
    for operation in pending.into_values() {
        let _ = operation.request.reply.send(Err(Error::Drain));
    }
    if let Some(reply) = stop_reply {
        let _ = reply.send(outcome);
    }
}

#[cfg(test)]
mod deadline_tests {
    use super::*;
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn exclusive_child_gate_retains_active_request_after_observer_drops() {
        let (sender, mut commands) = mpsc::channel(2);
        let (stop, _) = watch::channel(None);
        let host = Host {
            inner: Arc::new(Inner {
                sender,
                closed: Arc::new(AtomicBool::new(false)),
                receipt: Arc::new(Mutex::new(None)),
                stop,
                activity: Arc::new(tokio::sync::RwLock::new(())),
            }),
        };
        let proc = std::fs::read_to_string("/proc/self/stat").unwrap();
        let identity = Identity {
            pid: std::process::id(),
            start_generation: proc
                .rsplit_once(')')
                .unwrap()
                .1
                .split_whitespace()
                .nth(19)
                .unwrap()
                .into(),
            pid_namespace: std::fs::read_link("/proc/self/ns/pid").unwrap(),
        };
        let scope = Invocation {
            owner: Arc::new(()),
            sql: None,
        };
        assert!(matches!(
            host.call(
                DELETE_ENABLED.into(),
                json!({"mutation_lock_held": true}),
                scope.clone()
            )
            .await,
            Err(Error::Unavailable)
        ));
        assert!(matches!(
            commands.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        let prior = host.inner.activity.clone().try_read_owned().unwrap();
        assert!(matches!(
            host.try_hold_taskless_child(identity.clone()),
            Err(Error::Capacity)
        ));
        drop(prior);
        let guard = host.try_hold_taskless_child(identity.clone()).unwrap();
        assert!(matches!(
            host.call("discover".into(), Value::Null, scope.clone())
                .await,
            Err(Error::Capacity)
        ));
        let keep = guard.permit.clone();
        let owned = host.clone();
        let observer = tokio::spawn(async move {
            owned
                .call_bound(
                    "observe_taskless_module".into(),
                    Value::Null,
                    scope,
                    Some(identity),
                    Some(keep),
                )
                .await
        });
        let pending = commands.recv().await.unwrap();
        observer.abort();
        let _ = observer.await;
        drop(guard);
        assert!(host.inner.activity.try_read().is_err());
        drop(pending);
        assert!(host.inner.activity.try_read().is_ok());
    }

    #[test]
    fn observation_requires_an_existing_pinned_child() {
        let (reply, _observe) = oneshot::channel();
        let mut request = Request {
            method: "observe_taskless_module".into(),
            args: Value::Null,
            scope: Invocation {
                owner: Arc::new(()),
                sql: None,
            },
            reply,
            _activity: None,
            required_child: Some(Identity {
                pid: 1,
                start_generation: String::new(),
                pid_namespace: PathBuf::new(),
            }),
        };
        assert!(!request.may_use(None));
        request.required_child = None;
        assert!(request.may_use(None));
    }

    #[tokio::test(start_paused = true)]
    async fn owned_deadline_shortening_wakes_current_physical_wait() {
        let (sender, mut receiver) =
            watch::channel(Some(tokio::time::Instant::now() + Duration::from_secs(30)));
        let wait = tokio::spawn(async move {
            stop_deadline(&mut receiver).await;
        });
        tokio::task::yield_now().await;
        sender.send_replace(Some(tokio::time::Instant::now() + Duration::from_secs(2)));
        tokio::time::advance(Duration::from_secs(2)).await;
        wait.await.unwrap();
    }
    #[tokio::test]
    async fn shutdown_signal_cannot_extend_an_existing_actual_deadline() {
        let (sender, _commands) = mpsc::channel(1);
        let (stop, _receiver) = watch::channel(None);
        let host = Host {
            inner: Arc::new(Inner {
                sender,
                closed: Arc::new(AtomicBool::new(false)),
                receipt: Arc::new(Mutex::new(None)),
                stop,
                activity: Arc::new(tokio::sync::RwLock::new(())),
            }),
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        host.interrupt_at(deadline);
        host.interrupt_at(deadline + Duration::from_secs(20));
        assert_eq!(*host.inner.stop.borrow(), Some(deadline));
        assert!(!host.inner.closed.load(Ordering::Acquire));
    }
}
