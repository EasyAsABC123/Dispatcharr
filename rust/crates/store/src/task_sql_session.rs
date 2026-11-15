//! One actual borrowed Main pool connection, retained through physical closure.
//! Original Dispatcharr application/SQL algorithms retain their authors' credit.
use crate::{AuthService, StoreError};
use dispatcharr_contracts::BoxFuture;
use serde_json::Value;
use sqlx::{Connection, PgConnection, Row};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Weak,
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
            let value = work.ok_or_else(unavailable)?(connection).await;
            // Observe Ready on THAT connection, including any helper rollback,
            // before returning even a caught SQL ErrorResponse to its observer.
            connection.ping().await?;
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
    done: Notify,
    writer: Arc<OwnedSemaphorePermit>,
    writer_gate: Arc<tokio::sync::Mutex<()>>,
}
#[derive(Clone)]
pub struct TaskSqlSession {
    sender: mpsc::Sender<Command>,
    state: Arc<State>,
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
        let writer = self.task_sql_writer()?;
        // Keep the actual PoolConnection slot until physical close. Detaching
        // would let the pool replace it while this worker still owns a socket.
        let connection = self.store.pool().acquire().await?;
        let (sender, mut receiver) = mpsc::channel::<Command>(1);
        let state = Arc::new(State {
            phase: AtomicU8::new(0),
            done: Notify::new(),
            writer,
            writer_gate: Arc::new(tokio::sync::Mutex::new(())),
        });
        let retained = state.clone();
        tokio::spawn(async move {
            let _owner = owner;
            let mut connection = connection;
            let mut failed = false;
            while let Some(command) = receiver.recv().await {
                match command {
                    Command::Close => break,
                    Command::Query(mut operation) => {
                        use futures_util::FutureExt;
                        let result = std::panic::AssertUnwindSafe(operation.run(&mut connection))
                            .catch_unwind()
                            .await;
                        if !matches!(result, Ok(Ok(()))) {
                            failed = true;
                            break;
                        }
                    }
                }
            }
            retained.phase.store(1, Ordering::Release);
            let ready = connection.ping().await;
            let closed = connection.close().await;
            retained.phase.store(
                if failed || ready.is_err() || closed.is_err() {
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
    /// Exactly ONE child borrows the existing writer at a time. It acquires a
    /// distinct physical connection from the SAME pool8; it never detaches or
    /// borrows the parent's connection/transaction. Parent business SQL waits
    /// for physical child closure. Explicit parent rollback/commit is separate.
    pub(crate) fn publication_writer(&self) -> TaskSqlWriterWitness {
        TaskSqlWriterWitness {
            writer: Arc::downgrade(&self.state.writer),
            gate: self.state.writer_gate.clone(),
        }
    }
    pub async fn fence(&self) -> Result<(), StoreError> {
        self.run(|_connection| Box::pin(async {})).await
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
            Some(cache) => cache.settings_group("system_settings", load).await,
            None => load().await,
        }
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
    gate: Arc<tokio::sync::Mutex<()>>,
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
        let gate = self.gate.clone().try_lock_owned().map_err(|_| {
            StoreError::StatusBody(
                503,
                serde_json::json!({"detail":"Causal SQL writer is still active."}),
            )
        })?;
        let auth = auth.with_request_writer_permit(writer)?;
        // Losing the initializer observer never drops a physical connection or
        // its writer loan early. An unobserved returned Session closes its actor.
        tokio::spawn(async move { auth.borrow_task_sql_session(Arc::new((owner, gate))).await })
            .await
            .map_err(|_| unavailable())?
            .map(Some)
    }
}
