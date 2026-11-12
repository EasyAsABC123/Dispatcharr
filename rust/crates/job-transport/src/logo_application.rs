//! Inert application attachment: prepare every dependency before explicit start.
//! No Main/environment flag, default registration or shared transport close.
use crate::{default_application::{DefaultApplication,Drain,LifecycleError},default_worker::{DefaultWorker,Error as WorkerError,Run},logo_scanner::{Scanner,Effects,Roots,Limits},logo_task::{LogoTask,Clock},transport::Jobs};
use std::{sync::Arc,time::Duration};
#[derive(Debug)]
pub enum Error{Scanner(crate::logo_scanner::Error),Worker(WorkerError),Lifecycle(LifecycleError)}
#[derive(Clone)]
pub struct LogoApplication{scanner:Scanner,worker:DefaultWorker,application:DefaultApplication}
impl LogoApplication{
 pub fn prepare(jobs:Jobs,effects:Arc<dyn Effects>,roots:Roots,limits:Limits,clock:Arc<dyn Clock>,poll:Duration)->Result<Self,Error>{
  let scanner=Scanner::new(effects,roots,limits).map_err(Error::Scanner)?;let domain=Arc::new(LogoTask::new(scanner.clone(),clock));let worker=DefaultWorker::new_logo_scanner(jobs,domain).map_err(Error::Worker)?;let application=DefaultApplication::prepare(worker.clone(),poll).map_err(Error::Lifecycle)?;Ok(Self{scanner,worker,application})
 }
 /// Explicit bounded disk-spool profile; old prepare() remains128 files.
 pub fn prepare_spooled(jobs:Jobs,effects:Arc<dyn Effects>,options:crate::logo_scanner::SpoolOptions,clock:Arc<dyn Clock>,poll:Duration)->Result<Self,Error>{
  let scanner=Scanner::new_spooled(effects,options.roots,options.limits,options.scratch).map_err(Error::Scanner)?;let domain=Arc::new(LogoTask::new(scanner.clone(),clock));let worker=DefaultWorker::new_logo_scanner(jobs,domain).map_err(Error::Worker)?;let application=DefaultApplication::prepare(worker.clone(),poll).map_err(Error::Lifecycle)?;Ok(Self{scanner,worker,application})
 }
 pub fn start(&self)->Result<(),LifecycleError>{self.application.start()}
 pub fn first_scan_completed(&self)->bool{self.scanner.first_scan_completed()}
 pub fn state(&self)->crate::default_application::State{self.application.state()}
 /// Shared-dispatcher or fixture entrypoint; normal application polling uses
 /// start(). The same worker slot prevents concurrent manual/poller bodies.
 pub async fn run_once(&self,owner:Arc<dyn Send+Sync>)->Result<Run,WorkerError>{self.worker.run_once(owner).await}
 pub async fn shutdown(&self,timeout:Duration)->Drain{
  let Some(deadline)=tokio::time::Instant::now().checked_add(timeout)else{return Drain{drained:false,state:self.state()}};
  let mut drained=self.application.shutdown(deadline.saturating_duration_since(tokio::time::Instant::now())).await;
  if drained.drained{drained.drained=self.scanner.shutdown(deadline.saturating_duration_since(tokio::time::Instant::now())).await;}
  // The outer assembly closes shared Jobs/SQL/cache ONLY after all families
  // drain. Closing it here could break another family's admitted result write.
  drained
 }
}
