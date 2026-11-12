//! Explicit bounded first-scan Index helper; no Main/default registration.
//! Original Dispatcharr/Django/Celery authors retain behavior/code/license credit.
use crate::{default_application::{DefaultApplication,Drain},default_domains::{Domains,Error as DomainError},default_protocol::{Kind,Message,Normal},default_task_terminal::{Terminal,ScannerFailure},default_worker::DefaultWorker,logo_scanner::{Scanner,Effects,Roots,Limits,Error,Receipt,FirstScanHook},logo_task::Clock,transport::Jobs};
use dispatcharr_contracts::BoxFuture;
use sqlx::{Connection,PgPool};
use std::{sync::{Arc,Mutex},time::Duration};
use uuid::Uuid;
pub(crate) enum Publication{Committed(Uuid),CaughtServer}
const MAX_SOURCES:usize=32;
const SELECT:&str="SELECT s.id::bigint FROM epg_epgsource s LEFT OUTER JOIN epg_epgsourceindex i ON s.id=i.source_id WHERE s.is_active AND NOT(s.source_type IN ('dummy','schedules_direct')) AND (i.source_id IS NULL OR i.data IS NULL) LIMIT 33";
/// Constructed only from physically committed child publication replies.
#[derive(Debug)]
pub struct Children{ids:Vec<Uuid>}
impl Children{pub fn ids(&self)->&[Uuid]{&self.ids}}
struct Helper{pool:PgPool,jobs:Jobs}
impl Helper{
 async fn sources(&self,receipt:Arc<Receipt>)->Result<Option<Vec<i64>>,Error>{
  let _receipt=receipt;let mut connection=self.pool.acquire().await.map_err(|_|Error::Operational)?.detach();
  let result=sqlx::query_scalar::<_,i64>(SELECT).fetch_all(&mut connection).await;
  // ErrorResponse alone is not a physical ReadyForQuery barrier. Keep the
  // caller/scan admission until the trailing reply settles and close completes.
  let barrier=connection.ping().await;let closed=connection.close().await;
  if barrier.is_err()||closed.is_err(){return Err(Error::Operational)}
  match result{
   Ok(ids)if ids.len()<=MAX_SOURCES&&ids.iter().all(|id|*id>0)=>Ok(Some(ids)),
   Ok(_)=>Err(Error::Unsupported),
   Err(error)if error.as_database_error().and_then(|v|v.try_downcast_ref::<sqlx::postgres::PgDatabaseError>()).is_some_and(|v|v.severity()==sqlx::postgres::PgSeverity::Error&&v.code()=="P0001")=>Ok(None),
   Err(_)=>Err(Error::Operational)
  }
 }
}
struct Execution{helper:Arc<Helper>,parent:Uuid,children:Mutex<Vec<Uuid>>}
impl Execution{
 fn new(helper:Arc<Helper>,parent:Uuid)->Self{Self{helper,parent,children:Mutex::new(Vec::new())}}
 fn children(&self)->Result<Children,Error>{Ok(Children{ids:self.children.lock().map_err(|_|Error::Operational)?.clone()})}
}
impl FirstScanHook for Execution{
 fn admit<'a>(&'a self,receipt:Arc<Receipt>)->std::pin::Pin<Box<dyn std::future::Future<Output=Result<(),Error>>+Send+'a>>{Box::pin(async move{self.helper.sources(receipt).await.map(|_|())})}
 fn finish<'a>(&'a self,receipt:Arc<Receipt>)->std::pin::Pin<Box<dyn std::future::Future<Output=Result<(),Error>>+Send+'a>>{Box::pin(async move{
  let Some(ids)=self.helper.sources(receipt.clone()).await? else{return Ok(())};
  for source in ids{
   // Preserve database iteration; the Source query has no global ORDER BY.
   // The same receipt remains owned through each actual Redis reply.
   match self.helper.jobs.publish_scanner_index(self.parent,source,receipt.clone()).await{
    Ok(Publication::Committed(id))=>{let mut children=self.children.lock().map_err(|_|Error::Operational)?;if children.len()==MAX_SOURCES{return Err(Error::Operational)}children.push(id)},
    Ok(Publication::CaughtServer)=>break,
    Err(_)=>return Err(Error::Operational)
   }
  }Ok(())
 })}
}
struct Domain{scanner:Scanner,helper:Arc<Helper>,clock:Arc<dyn Clock>}
fn mapped(error:Error)->DomainError{match error{Error::Unsupported=>DomainError::Unsupported,_=>DomainError::ScannerOperational}}
impl Domains for Domain{
 // This new context-dependent domain cannot publish without an actual message.
 // Existing LogoTask and every legacy domain/method remain unchanged.
 fn preflight(&self,_:Kind,_:Arc<dyn Send+Sync>)->BoxFuture<'_,Result<(),DomainError>>{Box::pin(async{Err(DomainError::Unsupported)})}
 fn execute(&self,_:Kind,_:Arc<dyn Send+Sync>)->BoxFuture<'_,Result<Normal,DomainError>>{Box::pin(async{Err(DomainError::Unsupported)})}
 fn preflight_message(&self,message:Message,owner:Arc<dyn Send+Sync>)->BoxFuture<'_,Result<(),DomainError>>{Box::pin(async move{
  if message.kind!=Kind::LogoScanner||!self.clock.now().is_finite()||!(100_000_000.0..10_000_000_000.0).contains(&self.clock.now()){return Err(DomainError::Unsupported)}
  self.scanner.preflight_with_first_scan(owner,Arc::new(Execution::new(self.helper.clone(),message.task_id))).await.map_err(mapped)
 })}
 fn execute_message(&self,message:Message,owner:Arc<dyn Send+Sync>)->BoxFuture<'_,Result<Terminal,DomainError>>{Box::pin(async move{
  if message.kind!=Kind::LogoScanner{return Err(DomainError::Unsupported)}
  let execution=Arc::new(Execution::new(self.helper.clone(),message.task_id));
  match self.scanner.scan_with_first_scan(self.clock.now(),owner,execution.clone()).await{
   Ok(_)=>Ok(Terminal::ScannerChildren(execution.children().map_err(mapped)?)),
   Err(Error::InvalidMarker)=>Ok(Terminal::Scanner(ScannerFailure::observed_invalid_marker())),
   Err(error)=>Err(mapped(error))
  }
 })}
}
/// Explicit helper-enabled assembly; old LogoApplication keeps no-index scope.
/// Caller prepares/starts a separately scoped Index consumer before real intake
/// so published descendants cannot permanently block the shared strict FIFO.
#[derive(Clone)]
pub struct IndexedLogoApplication{scanner:Scanner,application:DefaultApplication}
impl IndexedLogoApplication{
 pub fn prepare(jobs:Jobs,pool:PgPool,effects:Arc<dyn Effects>,roots:Roots,limits:Limits,clock:Arc<dyn Clock>,poll:Duration)->Result<Self,Error>{
  if !jobs.allows_defaults(){return Err(Error::Unsupported)}
  let scanner=Scanner::new(effects,roots,limits)?;let helper=Arc::new(Helper{pool,jobs:jobs.clone()});let domain=Arc::new(Domain{scanner:scanner.clone(),helper,clock});let worker=DefaultWorker::new_logo_scanner(jobs,domain).map_err(|_|Error::Unsupported)?;let application=DefaultApplication::prepare(worker,poll).map_err(|_|Error::Unsupported)?;Ok(Self{scanner,application})
 }
 pub fn start(&self)->Result<(),crate::default_application::LifecycleError>{self.application.start()}
 pub fn state(&self)->crate::default_application::State{self.application.state()}
 pub fn first_scan_completed(&self)->bool{self.scanner.first_scan_completed()}
 pub async fn shutdown(&self,timeout:Duration)->Drain{let Some(deadline)=tokio::time::Instant::now().checked_add(timeout)else{return Drain{drained:false,state:self.state()}};let mut drain=self.application.shutdown(deadline.saturating_duration_since(tokio::time::Instant::now())).await;if drain.drained{drain.drained=self.scanner.shutdown(deadline.saturating_duration_since(tokio::time::Instant::now())).await}drain}
}

#[cfg(test)]
#[path="scanner_index_resources.rs"]
mod resources;
