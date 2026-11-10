//! UNREGISTERED/UNCOMPILED ownership adapter draft; no routes or acceptance.
//! Original Dispatcharr authors retain all application/algorithm credit.
//! Caller cancellation is a logical event. Physical SQL/rollback and detached
//! frames retain separate permits. This file must pass real-PG gates before use.
use bytes::Bytes;
use futures_util::FutureExt;
use sqlx::{pool::PoolConnection, PgPool, Postgres};
use std::{future::Future, io::{self, Write}, panic::AssertUnwindSafe,
    sync::{Arc, atomic::{AtomicBool, Ordering}}, time::Duration};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub(crate) enum ReadError {
    Source(crate::StoreError), Capacity, Deadline, Cancelled, Shutdown, WorkerFailed,
    Unsupported(&'static str),
}
impl From<sqlx::Error> for ReadError { fn from(value:sqlx::Error)->Self {Self::Source(value.into())} }
impl From<crate::StoreError> for ReadError { fn from(value:crate::StoreError)->Self {Self::Source(value)} }
#[derive(Clone,Copy)]
pub(crate) struct ReadLimits {
    pub workers:usize, pub memory_bytes:usize, pub work_bytes:usize,
    pub response_bytes:usize, pub row_bytes:usize, pub input_bytes:usize,
    pub logical_deadline:Duration, pub statement_deadline:Duration,
}
impl Default for ReadLimits {
    fn default()->Self {Self {workers:2,memory_bytes:192*1024*1024,work_bytes:52*1024*1024,
        response_bytes:16*1024*1024,row_bytes:128*1024,input_bytes:64*1024,
        logical_deadline:Duration::from_secs(5),statement_deadline:Duration::from_secs(3)}}
}
#[derive(Clone)]
pub(crate) struct ReadRuntime {inner:Arc<RuntimeInner>}
struct RuntimeInner {limits:ReadLimits,workers:Arc<Semaphore>,memory:Arc<Semaphore>,closing:AtomicBool}
struct WorkLease {_worker:OwnedSemaphorePermit,_memory:OwnedSemaphorePermit}
struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {fn drop(&mut self){self.0.store(true,Ordering::Release);}}
pub(crate) struct ReadWork {
    lease:Arc<WorkLease>, inner:Arc<RuntimeInner>, cancelled:Arc<AtomicBool>,
    deadline:tokio::time::Instant,
}
struct OwnedDocument {data:Vec<u8>,_memory:OwnedSemaphorePermit}
impl AsRef<[u8]> for OwnedDocument {fn as_ref(&self)->&[u8]{&self.data}}
impl ReadRuntime {
    pub fn new(limits:ReadLimits)->Result<Self,ReadError> {
        // Conservative draft JSON decode/clone allowance. An allocator/node
        // amplification gate is still required; this is not measured proof.
        let minimum=limits.row_bytes.checked_mul(256+100).and_then(|v|v.checked_add(4*1024*1024+100*4096)).ok_or(ReadError::Capacity)?;
        let total=limits.work_bytes.checked_add(limits.response_bytes).ok_or(ReadError::Capacity)?;
        if !(1..=16).contains(&limits.workers)||!(4096..=1024*1024).contains(&limits.row_bytes)
            ||limits.work_bytes<minimum||limits.response_bytes<128||limits.response_bytes>64*1024*1024
            ||limits.memory_bytes>512*1024*1024||total>limits.memory_bytes
            ||limits.input_bytes>1024*1024||limits.logical_deadline.is_zero()||limits.statement_deadline.is_zero()
            ||limits.logical_deadline>Duration::from_secs(30)||limits.statement_deadline>Duration::from_secs(30) {
            return Err(ReadError::Unsupported("Invalid draft VOD read limits"));
        }
        Ok(Self {inner:Arc::new(RuntimeInner {limits,workers:Arc::new(Semaphore::new(limits.workers)),
            memory:Arc::new(Semaphore::new(limits.memory_bytes)),closing:AtomicBool::new(false)})})
    }
    /// Acquire before auth/cache/SQL/serialization. The closure is internal
    /// component work and must use ReadWork's bounded document/SQL ownership.
    /// Input extraction has a separate outer HTTP-server byte/admission gate.
    pub async fn run<F,Fut>(&self,input_bytes:usize,work:F)->Result<Bytes,ReadError>
    where F:FnOnce(ReadWork)->Fut+Send+'static,Fut:Future<Output=Result<Vec<u8>,ReadError>>+Send+'static {
        if self.inner.closing.load(Ordering::Acquire){return Err(ReadError::Shutdown);}
        let worker=self.inner.workers.clone().try_acquire_owned().map_err(|_|ReadError::Capacity)?;
        let memory=self.inner.memory.clone().try_acquire_many_owned(self.inner.limits.work_bytes as u32).map_err(|_|ReadError::Capacity)?;
        let mut response=self.inner.memory.clone().try_acquire_many_owned(self.inner.limits.response_bytes as u32).map_err(|_|ReadError::Capacity)?;
        if input_bytes>self.inner.limits.input_bytes{return Err(ReadError::Capacity);}
        let lease=Arc::new(WorkLease {_worker:worker,_memory:memory});
        let cancelled=Arc::new(AtomicBool::new(false));let _caller=CancelOnDrop(cancelled.clone());
        let deadline=tokio::time::Instant::now()+self.inner.limits.logical_deadline;
        let context=ReadWork {lease:lease.clone(),inner:self.inner.clone(),cancelled,deadline};
        let (sender,receiver)=oneshot::channel();
        tokio::spawn(async move {
            // An owned SQL connection's Drop queues a supervised rollback;
            // its own lease keeps admission through that physical completion.
            let result=AssertUnwindSafe(async {work(context).await}).catch_unwind().await;
            let result=match result {Ok(value)=>value,Err(_)=>Err(ReadError::WorkerFailed)};
            let result=result.and_then(|data| {
                if data.capacity()>response.num_permits(){return Err(ReadError::Capacity);}
                let retained=response.split(data.capacity()).ok_or(ReadError::Capacity)?;
                Ok(Bytes::from_owner(OwnedDocument {data,_memory:retained}))
            });
            drop(response);drop(lease);
            let _=sender.send(result);
        });
        tokio::time::timeout_at(deadline,receiver).await.map_err(|_|ReadError::Deadline)?
            .map_err(|_|ReadError::WorkerFailed)?
    }
    pub fn physical_workers(&self)->usize {self.inner.limits.workers-self.inner.workers.available_permits()}
    pub fn retained_memory_bytes(&self)->usize {self.inner.limits.memory_bytes-self.inner.memory.available_permits()}
    /// Workers/rollback drained, not a claim that detached body frames vanished.
    pub async fn shutdown(&self,budget:Duration)->bool {
        self.inner.closing.store(true,Ordering::Release);self.inner.workers.close();
        let end=tokio::time::Instant::now()+budget;
        while self.physical_workers()!=0&&tokio::time::Instant::now()<end {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.physical_workers()==0
    }
}
impl ReadWork {
    pub fn check(&self)->Result<(),ReadError> {
        if self.inner.closing.load(Ordering::Acquire)||self.cancelled.load(Ordering::Acquire){return Err(ReadError::Cancelled);}
        if tokio::time::Instant::now()>=self.deadline{return Err(ReadError::Deadline);}
        Ok(())
    }
    pub fn row_bytes(&self)->usize {self.inner.limits.row_bytes}
    /// The authentication adapter must retain this through its own physical
    /// SQL/Redis/cache tasks, including operations surviving logical timeout.
    pub fn physical_guard(&self)->Arc<dyn Send+Sync> {self.lease.clone()}
    pub fn document(&self)->Document {Document {data:Vec::new(),maximum:self.inner.limits.response_bytes}}
    pub async fn connection(&self,pool:&PgPool)->Result<OwnedConnection,ReadError> {
        self.check()?;
        // A pool acquire is supervised by the request actor; aborting its
        // caller does not drop this future while the physical work continues.
        let connection=pool.acquire().await?;
        let mut owner=OwnedConnection {connection:Some(connection),lease:self.lease.clone(),original_timeout:None};
        owner.configure(self).await?;
        Ok(owner)
    }
}
pub(crate) struct Document {data:Vec<u8>,maximum:usize}
impl Document {pub fn finish(self)->Vec<u8>{self.data}}
impl Write for Document {
    fn write(&mut self,bytes:&[u8])->io::Result<usize> {
        let needed=self.data.len().checked_add(bytes.len()).filter(|v|*v<=self.maximum)
            .ok_or_else(||io::Error::other("VOD document exceeds response capacity"))?;
        if needed>self.data.capacity() {
            let next=needed.max(self.data.capacity().saturating_mul(2)).min(self.maximum);
            self.data.try_reserve_exact(next-self.data.len()).map_err(io::Error::other)?;
        }
        self.data.extend_from_slice(bytes);Ok(bytes.len())
    }
    fn flush(&mut self)->io::Result<()>{Ok(())}
}

pub(crate) struct OwnedConnection {connection:Option<PoolConnection<Postgres>>,lease:Arc<WorkLease>,original_timeout:Option<String>}
impl OwnedConnection {
    pub fn raw(&mut self)->&mut sqlx::PgConnection {self.connection.as_deref_mut().expect("owned open SQL connection")}
    /// Session scope also bounds autocommit get_or_create mutations. Physical
    /// timeout is separate from the caller's logical deadline: one already
    /// submitted statement can outlive it while retaining admission. Restore
    /// the original pool timeout, not an assumed zero/default, on success.
    pub async fn configure(&mut self,work:&ReadWork)->Result<(),ReadError> {
        work.check()?;
        if self.original_timeout.is_none() {
            self.original_timeout=Some(sqlx::query_scalar::<_,String>("SHOW statement_timeout").persistent(false).fetch_one(self.raw()).await?);
        }
        let timeout=work.inner.limits.statement_deadline.as_millis().max(1);
        sqlx::query("SELECT set_config('statement_timeout',$1,false)").bind(format!("{timeout}ms")).execute(self.raw()).await?;
        Ok(())
    }
    pub async fn finish(mut self)->Result<(),ReadError> {
        sqlx::query("ROLLBACK").execute(self.raw()).await?;
        if let Some(original)=self.original_timeout.take() {
            sqlx::query("SELECT set_config('statement_timeout',$1,false)").persistent(false).bind(original).execute(self.raw()).await?;
        }
        self.connection.take();Ok(())
    }
}
impl Drop for OwnedConnection {
    fn drop(&mut self) {
        if let Some(mut connection)=self.connection.take() {
            let lease=self.lease.clone();
            // Own the connection and permit until pending replies + rollback
            // complete. No timeout drops a still-in-flight cleanup operation.
            // On error, retire the connection instead of recycling dirty state.
            if let Ok(runtime)=tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _=sqlx::query("ROLLBACK").execute(&mut *connection).await;
                    let _=connection.close().await;
                    drop(lease);
                });
            } else {
                // Runtime teardown is not successful drain; close-on-drop
                // prevents an implicitly returned dirty pooled connection.
                connection.close_on_drop();
            }
        }
    }
}
