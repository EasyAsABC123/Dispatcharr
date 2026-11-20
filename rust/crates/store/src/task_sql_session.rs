//! One actual borrowed Main pool connection, retained through physical closure.
//! Original Dispatcharr application/SQL algorithms retain their authors' credit.
use crate::{AuthService, StoreError};
use dispatcharr_contracts::BoxFuture;
use serde_json::Value;
use sqlx::{Connection, PgConnection, Row};
use std::sync::{
    atomic::{AtomicBool, AtomicU8, Ordering},
    Arc, OnceLock, Weak,
};
use tokio::sync::{mpsc, oneshot, Notify, OwnedMutexGuard, OwnedSemaphorePermit};

type Owner = Arc<dyn Send + Sync>;
fn unavailable() -> StoreError {
    StoreError::Unsupported("Owned task SQL session unavailable")
}
trait Operation: Send {
    fn run<'a>(
        &'a mut self,
        connection: &'a mut PgConnection,
    ) -> BoxFuture<'a, Result<(), StoreError>>;
}
struct Query<F, T> {
    // Actual existing writer remains exclusive through work AND Ready/ping.
    _writer_guard: Option<OwnedMutexGuard<()>>,
    state: Arc<State>,
    work: Option<F>,
    reply: Option<oneshot::Sender<T>>,
}
impl<F, T> Operation for Query<F, T>
where
    F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, T> + Send + 'static,
    T: Send + 'static,
{
    fn run<'a>(
        &'a mut self,
        connection: &'a mut PgConnection,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        let work = self.work.take();
        let reply = self.reply.take();
        Box::pin(async move {
            self.state.actual_ready_idle.store(false, Ordering::Release);
            let value = work.ok_or_else(unavailable)?(connection).await;
            // Observe Ready on THAT connection, including any helper rollback,
            // before returning even a caught SQL ErrorResponse to its observer.
            connection.ping().await?;
            let idle = connection.protocol_transaction_is_idle().await?;
            self.state.actual_ready_idle.store(idle, Ordering::Release);
            if let Some(reply) = reply {
                let _ = reply.send(value);
            }
            Ok(())
        })
    }
}
enum Command {
    Query(Box<dyn Operation>),
    Close,
}
struct State {
    phase: AtomicU8,
    // False until the actual owned connection has reported ReadyForQuery idle.
    actual_ready_idle: AtomicBool,
    causal_root: OnceLock<TaskSqlPublicationWitness>,
    borrowed_child: AtomicBool,
    interrupted: AtomicBool,
    interrupt: Notify,
    done: Notify,
    writer: Arc<OwnedSemaphorePermit>,
    writer_gate: Arc<tokio::sync::Mutex<()>>,
}
#[derive(Clone)]
pub struct TaskSqlSession {
    sender: mpsc::Sender<Command>,
    state: Arc<State>,
}
async fn wait_interruption(state: &State) {
    loop {
        let wake = state.interrupt.notified();
        if state.interrupted.load(Ordering::Acquire) {
            return;
        }
        wake.await;
    }
}

impl AuthService {
    /// HTTP callers already own their validated writer. Main task callers do
    /// not have request-local state: admit from the SAME four-slot budget.
    pub(crate) fn task_sql_writer(&self) -> Result<Arc<OwnedSemaphorePermit>, StoreError> {
        if self.request_writer_permit.is_some() {
            return self.rest_writer();
        }
        self.user_agent_workers
            .clone()
            .try_acquire_owned()
            .map(Arc::new)
            .map_err(|_| {
                StoreError::StatusBody(
                    503,
                    serde_json::json!({"detail":"Owned task SQL writer capacity is unavailable."}),
                )
            })
    }
    /// Borrow exactly one connection from the existing Store and one of the
    /// SAME original four writer slots. No new pool or transaction is created.
    pub async fn borrow_task_sql_session(
        &self,
        owner: Owner,
    ) -> Result<TaskSqlSession, StoreError> {
        self.borrow_task_sql_session_with_gate(owner, None).await
    }
    async fn borrow_task_sql_session_with_gate(
        &self,
        owner: Owner,
        writer_gate: Option<Arc<tokio::sync::Mutex<()>>>,
    ) -> Result<TaskSqlSession, StoreError> {
        let writer = self.task_sql_writer()?;
        // Keep the actual PoolConnection slot until physical close. Detaching
        // would let the pool replace it while this worker still owns a socket.
        let connection = self.store.pool().acquire().await?;
        let (sender, mut receiver) = mpsc::channel::<Command>(1);
        let state = Arc::new(State {
            phase: AtomicU8::new(0),
            actual_ready_idle: AtomicBool::new(false),
            causal_root: OnceLock::new(),
            borrowed_child: AtomicBool::new(false),
            interrupted: AtomicBool::new(false),
            interrupt: Notify::new(),
            done: Notify::new(),
            writer,
            writer_gate: writer_gate.unwrap_or_else(|| Arc::new(tokio::sync::Mutex::new(()))),
        });
        let retained = state.clone();
        tokio::spawn(async move {
            let _owner = owner;
            let mut connection = connection;
            let mut failed = false;
            let mut interrupted = false;
            loop {
                let command = tokio::select! {biased;_ = wait_interruption(&retained)=>{interrupted=true;break;},command=receiver.recv()=>command};
                let Some(command) = command else { break };
                match command {
                    Command::Close => break,
                    Command::Query(mut operation) => {
                        use futures_util::FutureExt;
                        let result = tokio::select! {biased;_ =wait_interruption(&retained)=>{interrupted=true;break;},result=std::panic::AssertUnwindSafe(operation.run(&mut connection)).catch_unwind()=>result};
                        if !matches!(result, Ok(Ok(()))) {
                            failed = true;
                            break;
                        }
                    }
                }
            }
            retained.phase.store(1, Ordering::Release);
            if interrupted {
                // A dropped query future or client socket is NOT evidence the
                // owned server query/transaction ended. Quarantine the ACTUAL
                // PoolConnection, writer and caller; never permit replacement.
                // Operator/process retirement is required: no normal close or
                // server-side completion is certified by this error receipt.
                retained.phase.store(3, Ordering::Release);
                retained.done.notify_waiters();
                let quarantine = (connection, retained, _owner);
                std::future::pending::<()>().await;
                drop(quarantine);
                return;
            }
            let ready = connection.ping().await.is_ok();
            let closed = connection.close().await;
            retained.phase.store(
                if interrupted || failed || !ready || closed.is_err() {
                    3
                } else {
                    2
                },
                Ordering::Release,
            );
            retained.done.notify_waiters();
            // The original caller and writer remain owned until actual close.
        });
        Ok(TaskSqlSession { sender, state })
    }
}
impl TaskSqlSession {
    pub(crate) fn shares_auth_writer(&self, auth: &AuthService) -> bool {
        Arc::ptr_eq(self.state.writer.semaphore(), &auth.user_agent_workers)
    }
    pub async fn run<T, F>(&self, work: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, T> + Send + 'static,
    {
        let guard = self.state.writer_gate.clone().lock_owned().await;
        self.enqueue(work, Some(guard)).await
    }
    async fn enqueue<T, F>(
        &self,
        work: F,
        guard: Option<OwnedMutexGuard<()>>,
    ) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, T> + Send + 'static,
    {
        if self.state.phase.load(Ordering::Acquire) != 0 {
            return Err(unavailable());
        }
        let (reply, receiver) = oneshot::channel();
        self.sender
            .send(Command::Query(Box::new(Query {
                work: Some(work),
                reply: Some(reply),
                _writer_guard: guard,
                state: self.state.clone(),
            })))
            .await
            .map_err(|_| unavailable())?;
        receiver.await.map_err(|_| unavailable())
    }
    /// Private pure transaction termination ONLY. A Source-requested rollback or
    /// commit must release its own locks even while a causal child is borrowing
    /// the admitted writer. No hidden transaction control/business SQL is added.
    pub(crate) async fn python_transaction_control<T, F>(&self, work: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, T> + Send + 'static,
    {
        self.enqueue(work, None).await
    }
    /// Native plugin runtime calls this ONLY for an actual settled publication
    /// owned by this parent invocation, never a caller-supplied parent/task ID.
    /// Exactly ONE live child session borrows this existing writer. Its SQL
    /// shares the parent query gate; extra children need remaining original
    /// Auth admission. It acquires a
    /// distinct physical connection from the SAME pool8; it never detaches or
    /// borrows the parent's connection/transaction. The same actual query gate
    /// serializes parent and borrowed-child SQL, without fencing a timed-out
    /// parent until its entire child task completes. Explicit control remains separate.
    pub(crate) fn publication_provenance(&self, owner: &Owner) -> TaskSqlPublicationWitness {
        if let Some(root) = self
            .state
            .causal_root
            .get()
            .filter(|r| r.parent.strong_count() != 0)
        {
            return root.clone();
        }
        TaskSqlPublicationWitness {
            writer: TaskSqlWriterWitness {
                writer: Arc::downgrade(&self.state.writer),
                state: Arc::downgrade(&self.state),
                gate: self.state.writer_gate.clone(),
            },
            parent: Arc::downgrade(owner),
        }
    }
    pub(crate) fn inherit_publication_provenance(
        &self,
        root: TaskSqlPublicationWitness,
    ) -> Result<(), StoreError> {
        // Only the sealed actual-publication adapter invokes this; a JSON root
        // UUID cannot choose a permit, owner or gate. No strong parent survives
        // in a delayed retry record after the actual waiting parent disappears.
        self.state.causal_root.set(root).map_err(|_| unavailable())
    }
    pub async fn fence(&self) -> Result<(), StoreError> {
        self.run(|_connection| Box::pin(async {})).await
    }
    /// Forced-error path: actual admission/session/caller are QUARANTINED.
    /// The pending server outcome is unknown; no local close releases capacity.
    /// This always errors and requires operator/process retirement.
    pub(crate) async fn abort_physical(&self) -> Result<(), StoreError> {
        self.state.interrupted.store(true, Ordering::Release);
        self.state.interrupt.notify_waiters();
        loop {
            let done = self.state.done.notified();
            if matches!(self.state.phase.load(Ordering::Acquire), 2 | 3) {
                return Err(unavailable());
            }
            done.await;
        }
    }
    pub async fn close(&self) -> Result<(), StoreError> {
        if self
            .state
            .phase
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let _ = self.sender.send(Command::Close).await;
        }
        loop {
            let notified = self.state.done.notified();
            match self.state.phase.load(Ordering::Acquire) {
                2 => return Ok(()),
                3 => return Err(unavailable()),
                _ => notified.await,
            }
        }
    }
    /// Same shared settings cache/version fence, with its SQL loader routed to
    /// this actual connection rather than an arbitrary second pool checkout.
    pub async fn system_settings(&self, auth: AuthService) -> Result<Value, StoreError> {
        self.system_settings_mode(auth, false).await
    }
    pub(crate) async fn observe_system_settings(
        &self,
        auth: AuthService,
    ) -> Result<Value, StoreError> {
        self.system_settings_mode(auth, true).await
    }
    async fn system_settings_mode(
        &self,
        auth: AuthService,
        observe_only: bool,
    ) -> Result<Value, StoreError> {
        if !Arc::ptr_eq(self.state.writer.semaphore(), &auth.user_agent_workers) {
            return Err(unavailable());
        }
        let auth = auth.with_request_writer_permit(self.state.writer.clone())?;
        let session = self.clone();
        let load = move || async move {
            session.run(|connection|Box::pin(async move {
                let row=sqlx::query("SELECT octet_length(value::text) AS bytes, CASE WHEN octet_length(value::text)<=65536 THEN value::text END AS raw FROM core_coresettings WHERE key='system_settings'").fetch_optional(&mut *connection).await?;
                let Some(row)=row else{return Ok(crate::source_system_settings_defaults())};
                let size:i32=row.try_get("bytes")?;let raw:Option<String>=row.try_get("raw")?;let raw=raw.ok_or_else(unavailable)?;
                if size<0 || size as usize!=raw.len() || raw.len()>65536 {return Err(unavailable());}
                let (value,_)=crate::xc_bounded_user::checked_properties(&raw,0,crate::XcUserReadLimits{max_raw_bytes:65536,max_nodes:2048,max_depth:32,work_bytes:4*1024*1024})?;
                Ok::<_,StoreError>(if value.as_object().is_some_and(|v|!v.is_empty()){value}else{crate::source_system_settings_defaults()})
            })).await?
        };
        match &auth.notification_cache {
            Some(cache) if observe_only => {
                cache
                    .settings_group_observe("system_settings", load, false)
                    .await
            }
            Some(cache) => cache.settings_group("system_settings", load).await,
            None => load().await,
        }
    }
}

/// Restore-only fence over ALL existing Main pool capacity. Every physical
/// connection keeps the existing Ready/close/quarantine supervisor; this type
/// cannot mint a connection, task owner, extra writer permit or transaction.
pub(crate) enum BackupPoolFenceError {
    Capacity,
    Store(Box<StoreError>),
}
impl From<StoreError> for BackupPoolFenceError {
    fn from(value: StoreError) -> Self {
        Self::Store(Box::new(value))
    }
}
pub(crate) struct BackupPoolFence {
    sessions: Vec<TaskSqlSession>,
}
impl BackupPoolFence {
    pub(crate) fn is_open(&self) -> bool {
        !self.sessions.is_empty()
            && self
                .sessions
                .iter()
                .all(|session| session.state.phase.load(Ordering::Acquire) == 0)
    }
    pub(crate) fn primary(&self) -> &TaskSqlSession {
        &self.sessions[0]
    }
    pub(crate) async fn clear_prepared_statements(&self) -> Result<(), StoreError> {
        for session in &self.sessions {
            session
                .run(|c| {
                    Box::pin(async move {
                        if !c.protocol_transaction_is_idle().await? {
                            return Err(sqlx::Error::Protocol(
                                "Restore requires idle Main pool connections".into(),
                            ));
                        }
                        c.clear_cached_statements().await
                    })
                })
                .await??;
        }
        Ok(())
    }
    pub(crate) async fn close(&self) -> Result<(), StoreError> {
        let mut failed = false;
        for session in &self.sessions {
            if session.close().await.is_err() {
                failed = true;
            }
        }
        if failed {
            Err(unavailable())
        } else {
            Ok(())
        }
    }
}
impl AuthService {
    pub(crate) async fn borrow_backup_pool_fence(
        &self,
        owner: Owner,
        deadline: tokio::time::Instant,
    ) -> Result<BackupPoolFence, BackupPoolFenceError> {
        // This is the SAME admitted writer, reused under one shared gate. The
        // private constructor validates it belongs to this actual Auth budget.
        let writer = match self.task_sql_writer() {
            Ok(writer) => writer,
            Err(StoreError::StatusBody(503, _)) => return Err(BackupPoolFenceError::Capacity),
            Err(error) => return Err(error.into()),
        };
        let auth = self.clone().with_request_writer_permit(writer)?;
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let maximum = self.store.pool().options().get_max_connections();
        let mut fence = BackupPoolFence {
            sessions: Vec::with_capacity(maximum as usize),
        };
        for _ in 0..maximum {
            let next = tokio::time::timeout_at(
                deadline,
                auth.borrow_task_sql_session_with_gate(owner.clone(), Some(gate.clone())),
            )
            .await;
            match next {
                Ok(Ok(session)) => fence.sessions.push(session),
                Ok(Err(error)) => {
                    fence.close().await?;
                    return Err(
                        if matches!(&error, StoreError::Database(sqlx::Error::PoolTimedOut)) {
                            BackupPoolFenceError::Capacity
                        } else {
                            error.into()
                        },
                    );
                }
                Err(_) => {
                    fence.close().await?;
                    return Err(BackupPoolFenceError::Capacity);
                }
            }
        }
        if fence.sessions.is_empty() {
            return Err(unavailable().into());
        }
        // Bound admission precedes all destructive restore work. A real busy
        // owner can make this fail without dropping the schema or being killed.
        if let Err(error) = fence.clear_prepared_statements().await {
            fence.close().await?;
            return Err(error.into());
        }
        Ok(fence)
    }
}

#[cfg(test)]
mod main_worker_admission_regression {
    use super::*;
    fn main_auth() -> AuthService {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("unused-test-signing".into()).unwrap(),
        )
    }
    #[tokio::test]
    async fn main_m3u_bare_auth_admits_real_shared_writer_before_sql() {
        let auth = main_auth();
        assert!(auth.request_writer_permit.is_none());
        auth.store.pool().close().await;
        // A genuinely closed pool is observed only AFTER admission. Before the
        // fix this exact Main entry failed immediately as missing REST ingress.
        assert!(matches!(
            auth.borrow_task_sql_session(Arc::new(())).await,
            Err(StoreError::Database(sqlx::Error::PoolClosed))
        ));
        assert_eq!(auth.user_agent_workers.available_permits(), 4);
    }
    #[tokio::test]
    async fn main_m3u_ingress_writer_is_reused_and_exhaustion_is_capacity() {
        let auth = main_auth();
        auth.store.pool().close().await;
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(Arc::new(
                auth.user_agent_workers.clone().try_acquire_owned().unwrap(),
            ));
        }
        assert!(matches!(
            auth.borrow_task_sql_session(Arc::new(())).await,
            Err(StoreError::StatusBody(503, _))
        ));
        let scoped = auth
            .clone()
            .with_request_writer_permit(held[0].clone())
            .unwrap();
        let reused = scoped.task_sql_writer().unwrap();
        assert!(Arc::ptr_eq(&reused, &held[0]));
        assert_eq!(auth.user_agent_workers.available_permits(), 0);
        assert!(matches!(
            scoped.borrow_task_sql_session(Arc::new(())).await,
            Err(StoreError::Database(sqlx::Error::PoolClosed))
        ));
        drop(reused);
        drop(scoped);
        drop(held);
        assert_eq!(auth.user_agent_workers.available_permits(), 4);
        let foreign = Arc::new(
            Arc::new(tokio::sync::Semaphore::new(1))
                .try_acquire_owned()
                .unwrap(),
        );
        let mut invalid = auth.clone();
        invalid.request_writer_permit = Some(foreign);
        assert!(invalid.task_sql_writer().is_err());
        assert_eq!(auth.user_agent_workers.available_permits(), 4);
    }
}

/// Privately captured from an ACTUAL admitted parent session only after native
/// publication provenance. The queue map holds no strong writer/connection:
/// delayed jobs cannot keep all four writers alive after their parent exits.
#[derive(Clone)]
pub(crate) struct TaskSqlWriterWitness {
    writer: Weak<OwnedSemaphorePermit>,
    state: Weak<State>,
    gate: Arc<tokio::sync::Mutex<()>>,
}
#[derive(Clone)]
pub(crate) struct TaskSqlPublicationWitness {
    pub writer: TaskSqlWriterWitness,
    pub parent: Weak<dyn Send + Sync>,
}
struct BorrowedTaskWriter {
    parent: Weak<State>,
}
impl Drop for BorrowedTaskWriter {
    fn drop(&mut self) {
        if let Some(parent) = self.parent.upgrade() {
            parent.borrowed_child.store(false, Ordering::Release);
        }
    }
}
impl TaskSqlWriterWitness {
    pub(crate) async fn try_borrow(
        &self,
        auth: AuthService,
        owner: Owner,
    ) -> Result<Option<TaskSqlSession>, StoreError> {
        let Some(writer) = self.writer.upgrade() else {
            return Ok(None);
        };
        if !Arc::ptr_eq(writer.semaphore(), &auth.user_agent_workers) {
            return Err(unavailable());
        }
        let gate = match self.gate.clone().try_lock_owned() {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        let Some(state) = self.state.upgrade() else {
            return Ok(None);
        };
        if state.phase.load(Ordering::Acquire) != 0
            || !state.actual_ready_idle.load(Ordering::Acquire)
        {
            // Never loan an active transaction's writer. Source may issue more
            // parent business SQL after get() timeout, before atomic rollback.
            // Independent remaining Auth admission may still execute that task;
            // otherwise report real capacity, not a fake match/result/commit.
            return Ok(None);
        }
        if state
            .borrowed_child
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(None);
        }
        let marker = BorrowedTaskWriter {
            parent: Arc::downgrade(&state),
        };
        let shared_gate = self.gate.clone();
        // Reserve one actual borrowed session, but release the query gate now.
        // Both physical sessions acquire that SAME gate for each actual command.
        // CPU work/result waiting after a query cannot fence parent continuation.
        drop(gate);
        let auth = auth.with_request_writer_permit(writer)?;
        tokio::spawn(async move {
            auth.borrow_task_sql_session_with_gate(Arc::new((owner, marker)), Some(shared_gate))
                .await
        })
        .await
        .map_err(|_| unavailable())?
        .map(Some)
    }
}
