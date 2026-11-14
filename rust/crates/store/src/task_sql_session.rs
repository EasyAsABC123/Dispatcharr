//! One actual borrowed Main pool connection, retained through physical closure.
//! Original Dispatcharr application/SQL algorithms retain their authors' credit.
use crate::{AuthService, StoreError};
use dispatcharr_contracts::BoxFuture;
use serde_json::Value;
use sqlx::{Connection, PgConnection, Row};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use tokio::sync::{mpsc, oneshot, Notify, OwnedSemaphorePermit};

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
}
#[derive(Clone)]
pub struct TaskSqlSession {
    sender: mpsc::Sender<Command>,
    state: Arc<State>,
}
impl AuthService {
    /// Borrow exactly one connection from the existing Store and one of the
    /// SAME original four writer slots. No new pool or transaction is created.
    pub async fn borrow_task_sql_session(
        &self,
        owner: Owner,
    ) -> Result<TaskSqlSession, StoreError> {
        let writer = self.rest_writer()?;
        let connection = self.store.pool().acquire().await?.detach();
        let (sender, mut receiver) = mpsc::channel::<Command>(1);
        let state = Arc::new(State {
            phase: AtomicU8::new(0),
            done: Notify::new(),
            writer,
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
        if self.state.phase.load(Ordering::Acquire) != 0 {
            return Err(unavailable());
        }
        let (reply, receiver) = oneshot::channel();
        self.sender
            .send(Command::Query(Box::new(Query {
                work: Some(work),
                reply: Some(reply),
            })))
            .await
            .map_err(|_| unavailable())?;
        receiver.await.map_err(|_| unavailable())
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
