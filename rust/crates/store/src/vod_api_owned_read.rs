//! Bounded ordinary VOD ownership validated with actual PostgreSQL/Redis.
//! Original Dispatcharr authors retain all application/algorithm credit.
//! Caller cancellation is logical: physical SQL/rollback, scoped cache work
//! and detached frames retain independent leases. Allocator-wide amplification
//! and canonical packaged shutdown acceptance remain separate gates.
use bytes::Bytes;
use super::{vod_api_phase_scope::Scope as PhaseScope,vod_api_phase_children::Kind as PhaseChildKind};
use futures_util::FutureExt;
use sqlx::{pool::PoolConnection, PgPool, Postgres};
use std::{future::Future, io::{self, Write}, panic::AssertUnwindSafe,
    sync::{Arc, atomic::{AtomicBool, Ordering}}, time::Duration};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

#[path="vod_api_fair_runtime.rs"]mod fair;
pub(crate) use fair::HttpScope;
#[derive(Debug)]
pub(crate) enum ReadError {
    Source(crate::StoreError), Capacity, Deadline, Cancelled, Shutdown, WorkerFailed,
    Unsupported(&'static str),
    /// Captured uncaught Source exception; only category dispatch constructs it.
    SourceUnhandledFault(&'static str),
}
impl From<sqlx::Error> for ReadError { fn from(value:sqlx::Error)->Self {Self::Source(value.into())} }
impl From<crate::StoreError> for ReadError { fn from(value:crate::StoreError)->Self {Self::Source(value)} }
#[derive(Clone,Copy)]
pub struct ReadLimits {
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
struct RuntimeInner {limits:ReadLimits,workers:Arc<Semaphore>,memory:Arc<Semaphore>,closing:AtomicBool,fair:Option<Arc<fair::FairRuntime>>}
struct WorkLease {_worker:Option<OwnedSemaphorePermit>,_memory:Option<OwnedSemaphorePermit>,_request:Option<Arc<fair::RequestScope>>}
struct CancelOnDrop(Arc<AtomicBool>,Arc<tokio::sync::Notify>);
impl Drop for CancelOnDrop {fn drop(&mut self){self.0.store(true,Ordering::Release);self.1.notify_waiters();}}
pub(crate) struct ReadWork {
    lease:Arc<WorkLease>, inner:Arc<RuntimeInner>, cancelled:Arc<AtomicBool>,
    deadline:tokio::sync::watch::Sender<tokio::time::Instant>,
    request_started:tokio::time::Instant,
    // None preserves the legacy component until the atomic runtime is attached.
    phase:Option<PhaseScope>,
    cancelled_changed:Arc<tokio::sync::Notify>,
}
/// Owned, non-cloneable refresh descriptor. No row/connection/Auth capture.
/// Budget ownership is transferred by the scheduler, not inferred from Arc counts.
pub(super) struct FrozenSnapshot {
    bytes:Vec<u8>,
    runtime:Arc<RuntimeInner>,
    request:Arc<AtomicBool>,
    _physical:IoGuard,
}
impl FrozenSnapshot {
    pub(super) fn as_bytes(&self)->&[u8]{&self.bytes}
    pub(super) fn retained_bytes(&self)->Result<usize,ReadError>{
        self.bytes.capacity().checked_add(std::mem::size_of::<Self>()).ok_or(ReadError::Capacity)
    }
}
impl std::fmt::Debug for FrozenSnapshot {
    fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result{
        f.debug_struct("FrozenSnapshot").field("length",&self.bytes.len()).field("capacity",&self.bytes.capacity()).finish_non_exhaustive()
    }
}
pub(super) struct IoGuard{owned:Arc<dyn Send+Sync>}
impl IoGuard{pub(super) fn erased(&self)->Arc<dyn Send+Sync>{self.owned.clone()}}
fn phase_error(error:super::vod_api_phase_scope::Error)->ReadError{
    use super::vod_api_phase_scope::Error;use super::vod_api_fair_budget::Error as Budget;
    match error{Error::Budget(Budget::Closed)=>ReadError::Shutdown,Error::Budget(Budget::Deadline)=>ReadError::Deadline,Error::Child(super::vod_api_phase_children::Error::Deadline)=>ReadError::Deadline,Error::Capacity{..}|Error::Budget(Budget::Capacity)=>ReadError::Capacity,_=>ReadError::Unsupported("Invalid refresh phase ownership")}
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
            memory:Arc::new(Semaphore::new(limits.memory_bytes)),closing:AtomicBool::new(false),fair:None})})
    }
    /// Private opt-in prototype. Ordinary constructor semantics remain intact.
    pub(super) fn new_fair(limits:ReadLimits)->Result<Self,ReadError>{let mut runtime=Self::new(limits)?;if limits.memory_bytes.saturating_sub(limits.work_bytes+limits.response_bytes)<limits.input_bytes{return Ok(runtime)}Arc::get_mut(&mut runtime.inner).expect("new runtime").fair=Some(fair::FairRuntime::new(limits)?);Ok(runtime)}
    pub(super) fn is_fair(&self)->bool{self.inner.fair.is_some()}
    pub(super) async fn acquire_http(&self,input:usize,deadline:tokio::time::Instant)->Result<HttpScope,ReadError>{
        if self.inner.closing.load(Ordering::Acquire){return Err(ReadError::Shutdown)}
        self.inner.fair.as_ref().ok_or(ReadError::Unsupported("Atomic HTTP runtime is not configured"))?.admit_http(self.inner.limits,input,deadline).await
    }
    /// Acquire before auth/cache/SQL/serialization. The closure is internal
    /// component work and must use ReadWork's bounded document/SQL ownership.
    /// Input extraction has a separate outer HTTP-server byte/admission gate.
    pub async fn run<F,Fut>(&self,input_bytes:usize,work:F)->Result<Bytes,ReadError>
    where F:FnOnce(ReadWork)->Fut+Send+'static,Fut:Future<Output=Result<Vec<u8>,ReadError>>+Send+'static {
        self.run_until(input_bytes,None,work).await
    }
    pub async fn run_until<F,Fut>(&self,input_bytes:usize,arrival_deadline:Option<tokio::time::Instant>,work:F)->Result<Bytes,ReadError>
    where F:FnOnce(ReadWork)->Fut+Send+'static,Fut:Future<Output=Result<Vec<u8>,ReadError>>+Send+'static {
        self.run_scoped(input_bytes,arrival_deadline,None,work).await
    }
    pub(super) async fn run_scoped<F,Fut>(&self,input_bytes:usize,arrival_deadline:Option<tokio::time::Instant>,scope:Option<HttpScope>,work:F)->Result<Bytes,ReadError>
    where F:FnOnce(ReadWork)->Fut+Send+'static,Fut:Future<Output=Result<Vec<u8>,ReadError>>+Send+'static {
        if self.inner.closing.load(Ordering::Acquire){if let(Some(fair),Some(scope))=(&self.inner.fair,&scope){scope.reject_unstarted(fair);}return Err(ReadError::Shutdown);}
        if let Some(fair)=&self.inner.fair{return fair.clone().run(self.clone(),input_bytes,arrival_deadline,scope,work).await}
        if matches!(&scope,Some(HttpScope::Fair(_))){return Err(ReadError::Unsupported("Atomic HTTP scope cannot enter a legacy runtime"))}
        let worker=self.inner.workers.clone().try_acquire_owned().map_err(|_|ReadError::Capacity)?;
        let memory=self.inner.memory.clone().try_acquire_many_owned(self.inner.limits.work_bytes as u32).map_err(|_|ReadError::Capacity)?;
        let mut response=self.inner.memory.clone().try_acquire_many_owned(self.inner.limits.response_bytes as u32).map_err(|_|ReadError::Capacity)?;
        if input_bytes>self.inner.limits.input_bytes{return Err(ReadError::Capacity);}
        let lease=Arc::new(WorkLease {_worker:Some(worker),_memory:Some(memory),_request:None});
        let cancelled=Arc::new(AtomicBool::new(false));let cancelled_changed=Arc::new(tokio::sync::Notify::new());let _caller=CancelOnDrop(cancelled.clone(),cancelled_changed.clone());
        let local_deadline=tokio::time::Instant::now()+self.inner.limits.logical_deadline;
        let deadline=arrival_deadline.map_or(local_deadline,|arrival|arrival.min(local_deadline));
        let request_started=deadline.checked_sub(self.inner.limits.logical_deadline).ok_or(ReadError::Deadline)?;
        let (deadline,mut deadlines)=tokio::sync::watch::channel(deadline);
        let context=ReadWork {lease:lease.clone(),inner:self.inner.clone(),cancelled,deadline,request_started,phase:None,cancelled_changed};
        let (sender,mut receiver)=oneshot::channel();
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
        let mut watching=true;
        loop {
            let deadline=*deadlines.borrow();
            tokio::select! {
                biased;
                result=&mut receiver=>return result.map_err(|_|ReadError::WorkerFailed)?,
                changed=deadlines.changed(),if watching=>{if changed.is_err(){watching=false;}},
                _=tokio::time::sleep_until(deadline)=>{
                    if tokio::time::Instant::now()>=*deadlines.borrow(){return Err(ReadError::Deadline);}
                }
            }
        }
    }
    #[cfg(test)]
    pub(super) fn scheduler_state(&self)->serde_json::Value{match &self.inner.fair{Some(fair)=>fair.scheduler_state(),None=>serde_json::json!({"atomic":false,"workers":self.physical_workers(),"bytes":self.retained_memory_bytes()})}}
    pub fn physical_workers(&self)->usize {self.inner.fair.as_ref().map_or_else(||self.inner.limits.workers-self.inner.workers.available_permits(),|fair|fair.budget.used().workers)}
    pub fn retained_memory_bytes(&self)->usize {self.inner.fair.as_ref().map_or_else(||self.inner.limits.memory_bytes-self.inner.memory.available_permits(),|fair|fair.budget.used().bytes)}
    /// Workers/rollback drained, not a claim that detached body frames vanished.
    pub async fn shutdown(&self,budget:Duration)->bool {
        self.inner.closing.store(true,Ordering::Release);if let Some(fair)=&self.inner.fair{return fair.shutdown(budget).await}self.inner.workers.close();
        let end=tokio::time::Instant::now()+budget;
        while self.physical_workers()!=0&&tokio::time::Instant::now()<end {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.physical_workers()==0
    }
}
impl ReadWork {
    pub fn check(&self)->Result<(),ReadError> {
        if (self.inner.closing.load(Ordering::Acquire)||self.cancelled.load(Ordering::Acquire))&&!self.phase.as_ref().is_some_and(PhaseScope::observer_independent_session){return Err(ReadError::Cancelled);}
        if tokio::time::Instant::now()>=*self.deadline.borrow(){return Err(ReadError::Deadline);}
        Ok(())
    }
    /// Only after Source policy/metadata admits a real provider refresh. The
    /// Source uWSGI http/socket deadline is600s; ordinary/cached requests retain
    /// their existing5s bound. Anchoring to original arrival prevents repeated
    /// extension. No worker/memory/queue/statement budget changes.
    pub(super) fn promote_provider_refresh(&self)->Result<(),ReadError>{
        self.check()?;
        let next=self.request_started+Duration::from_secs(600);let mut expired=false;
        self.deadline.send_if_modified(|current|{
            if tokio::time::Instant::now()>=*current{expired=true;false}
            else if next>*current{*current=next;true}else{false}
        });
        if expired{Err(ReadError::Deadline)}else{Ok(())}
    }
    /// Snapshot writer uses existing work allowance, not response output.
    pub(super) fn snapshot_document(&self)->Document{self.phase_document(self.work_bytes(),false)}
    pub(super) fn freeze_snapshot(&self,bytes:Vec<u8>)->Result<FrozenSnapshot,ReadError>{
        self.check()?;
        let physical=match &self.phase{Some(phase)=>IoGuard{owned:phase.snapshot_guard().map_err(phase_error)?.erased()},None=>IoGuard{owned:self.lease.clone()}};
        let snapshot=FrozenSnapshot{bytes,runtime:self.inner.clone(),request:self.cancelled.clone(),_physical:physical};
        if snapshot.retained_bytes()?>self.work_bytes(){return Err(ReadError::Capacity)}
        // Only the Source-qualified preparation seam constructs this descriptor.
        // Promote before old SQL/cache cleanup can spend the ordinary deadline;
        // this operation lends no credits and still rejects late eligibility.
        self.promote_provider_refresh()?;
        Ok(snapshot)
    }
    async fn wait_cancelled(&self){loop{let changed=self.cancelled_changed.notified();tokio::pin!(changed);changed.as_mut().enable();if self.cancelled.load(Ordering::Acquire){return}changed.await}}
    pub(super) async fn admit_refresh(&self,snapshot:FrozenSnapshot)->Result<FrozenSnapshot,ReadError>{
        self.check()?;
        if !Arc::ptr_eq(&self.inner,&snapshot.runtime)||!Arc::ptr_eq(&self.cancelled,&snapshot.request){return Err(ReadError::Unsupported("Refresh snapshot belongs to another runtime or request"))}
        let phase=self.phase.as_ref().ok_or(ReadError::Unsupported("Refresh scheduling is not attached"))?;
        if !phase.refresh_enabled(){return Err(ReadError::Capacity)}
        let deadline=*self.deadline.borrow();let retained=snapshot.retained_bytes()?;
        if let Some(request)=&self.lease._request{request.mark_input(true)?;}
        tokio::select!{biased;_ = self.wait_cancelled()=>return Err(ReadError::Cancelled),result=phase.queue_refresh(retained,deadline)=>result.map_err(phase_error)?}
        self.check()?;
        tokio::select!{biased;_ = self.wait_cancelled()=>return Err(ReadError::Cancelled),result=phase.admit_refresh(deadline)=>result.map_err(phase_error)?}
        if let Some(request)=&self.lease._request{request.mark_input(false)?;}
        self.check()?;Ok(snapshot)
    }
    /// Call only after refresh admission has restored active work credits.
    /// The phase-aware scheduler will additionally enforce Active state here.
    pub(super) fn consume_snapshot(&self,snapshot:FrozenSnapshot)->Result<Vec<u8>,ReadError>{
        self.check()?;
        if !Arc::ptr_eq(&self.inner,&snapshot.runtime)||!Arc::ptr_eq(&self.cancelled,&snapshot.request){
            return Err(ReadError::Unsupported("Refresh snapshot belongs to another runtime or request"))
        }
        if let Some(phase)=&self.phase{phase.require_refresh_active().map_err(phase_error)?;}
        Ok(snapshot.bytes)
    }
    pub(super) fn control_deadline(&self)->tokio::time::Instant{*self.deadline.borrow()}
    pub fn row_bytes(&self)->usize {self.inner.limits.row_bytes}
    /// Independent unified reader partitions this existing work reservation;
    /// it does not acquire a second budget or impose a fixed page-row limit.
    pub(super) fn work_bytes(&self)->usize {self.inner.limits.work_bytes}
    pub(super) fn response_bytes(&self)->usize {self.inner.limits.response_bytes}
    /// The authentication adapter must retain this through its own physical
    /// SQL/Redis/cache tasks, including operations surviving logical timeout.
    pub fn physical_guard(&self)->Arc<dyn Send+Sync> {
        assert!(self.phase.is_none(),"phase-aware physical work requires io_guard or transport_guard");
        self.lease.clone()
    }
    /// Explicit physical registration, rather than an Arc-count heuristic.
    pub(super) fn io_guard(&self)->Result<IoGuard,ReadError>{self.io_guard_kind(PhaseChildKind::Auth)}
    fn io_guard_kind(&self,kind:PhaseChildKind)->Result<IoGuard,ReadError>{
        self.check()?;
        let owned=match &self.phase{Some(scope)=>scope.io_guard(kind).map_err(phase_error)?.erased(),None=>self.lease.clone()};
        Ok(IoGuard{owned})
    }
    pub(super) fn transport_guard(&self)->Result<IoGuard,ReadError>{
        self.check()?;
        let owned=match &self.phase{Some(scope)=>scope.transport_guard().map_err(phase_error)?.erased(),None=>self.lease.clone()};
        Ok(IoGuard{owned})
    }
    pub(super) async fn before_headers(&self)->Result<(),ReadError>{
        self.check()?;
        match &self.phase{Some(scope)=>scope.before_headers().await.map_err(phase_error),None=>Ok(())}
    }
    pub(super) fn start_auth_attempt(&self)->Result<(),ReadError>{
        self.check()?;
        match &self.phase{Some(scope)=>scope.start_auth_attempt().map_err(phase_error),None=>Ok(())}
    }
    pub(super) async fn resume_active(&self)->Result<(),ReadError>{
        match &self.phase{Some(scope)=>scope.resume_body().await.map_err(phase_error),None=>Ok(())}
    }
    fn phase_document(&self,maximum:usize,output:bool)->Document{
        if let Some(phase)=&self.phase{
            let result=(||{if output{phase.mark_output().map_err(phase_error)?;}self.io_guard_kind(PhaseChildKind::Document)})();
            return match result{Ok(guard)=>Document{data:Vec::new(),maximum,_physical:Some(guard)},Err(_)=>Document{data:Vec::new(),maximum:0,_physical:None}}
        }
        Document::bounded(maximum)
    }
    pub fn document(&self)->Document {self.phase_document(self.inner.limits.response_bytes,true)}
    pub async fn connection(&self,pool:&PgPool)->Result<OwnedConnection,ReadError> {
        self.check()?;
        // A pool acquire is supervised by the request actor; aborting its
        // caller does not drop this future while the physical work continues.
        let physical=self.io_guard_kind(PhaseChildKind::Sql)?.erased();
        let connection=pool.acquire().await?;
        let mut owner=OwnedConnection {connection:Some(connection),lease:physical,original_timeout:None};
        owner.configure(self).await?;
        Ok(owner)
    }
}
pub(crate) struct Document {data:Vec<u8>,maximum:usize,_physical:Option<IoGuard>}
impl Document {
    pub(crate) fn bounded(maximum:usize)->Self {Self {data:Vec::new(),maximum,_physical:None}}
    pub fn finish(self)->Vec<u8>{self.data}
}
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

pub(crate) struct OwnedConnection {connection:Option<PoolConnection<Postgres>>,lease:Arc<dyn Send+Sync>,original_timeout:Option<String>}
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
        sqlx::Connection::shrink_buffers(self.raw());
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

#[cfg(test)]
mod refresh_deadline_tests {
    use super::*;
    #[tokio::test(start_paused=true)]
    async fn ordinary_deadline_and_physical_ownership_stay_unchanged(){
        let runtime=ReadRuntime::new(ReadLimits::default()).unwrap();
        let result=runtime.run(1,|_work|async{tokio::time::sleep(Duration::from_secs(6)).await;Ok(vec![1])}).await;
        assert!(matches!(result,Err(ReadError::Deadline)));assert_eq!(runtime.physical_workers(),1);assert!(runtime.retained_memory_bytes()>0);
        assert!(runtime.shutdown(Duration::from_secs(2)).await);assert_eq!(runtime.retained_memory_bytes(),0);
    }
    #[tokio::test(start_paused=true)]
    async fn only_source_admitted_refresh_extends_existing_request(){
        let runtime=ReadRuntime::new(ReadLimits::default()).unwrap();
        let frame=runtime.run(1,|work|async move{work.promote_provider_refresh()?;tokio::time::sleep(Duration::from_secs(6)).await;work.check()?;Ok(vec![1])}).await.unwrap();
        assert_eq!(runtime.inner.limits.logical_deadline,Duration::from_secs(5));assert_eq!(frame.as_ref(),&[1]);drop(frame);assert!(runtime.shutdown(Duration::from_secs(1)).await);assert_eq!(runtime.retained_memory_bytes(),0);
    }
    #[tokio::test(start_paused=true)]
    async fn expired_request_cannot_promote_late(){
        let runtime=ReadRuntime::new(ReadLimits::default()).unwrap();let rejected=Arc::new(AtomicBool::new(false));let observe=rejected.clone();
        let result=runtime.run(1,move|work|async move{tokio::time::sleep(Duration::from_secs(6)).await;observe.store(matches!(work.promote_provider_refresh(),Err(ReadError::Deadline)|Err(ReadError::Cancelled)),Ordering::Release);Ok(vec![1])}).await;
        assert!(matches!(result,Err(ReadError::Deadline)));assert!(runtime.shutdown(Duration::from_secs(2)).await);assert!(rejected.load(Ordering::Acquire));assert_eq!(runtime.retained_memory_bytes(),0);
    }
}
