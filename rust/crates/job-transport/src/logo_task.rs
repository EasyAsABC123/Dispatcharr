//! Explicit logo-only domain, not a general scanner implementation.
//! Original Dispatcharr/Celery authors retain algorithm/code credit.
use crate::{default_domains::{Domains,Error},default_protocol::{Kind,Normal},default_task_terminal::{Terminal,ScannerFailure},logo_scanner::{Scanner,Error as ScanError}};
use dispatcharr_contracts::BoxFuture;
use std::sync::Arc;
pub trait Clock:Send+Sync{fn now(&self)->f64;}
pub struct SystemClock;
impl Clock for SystemClock{fn now(&self)->f64{std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|v|v.as_secs_f64()).unwrap_or(f64::NAN)}}
pub struct LogoTask{scanner:Scanner,clock:Arc<dyn Clock>}
impl LogoTask{pub fn new(scanner:Scanner,clock:Arc<dyn Clock>)->Self{Self{scanner,clock}}}
fn operational(error:ScanError)->Error{match error{ScanError::Unsupported=>Error::Unsupported,_=>Error::ScannerOperational}}
impl Domains for LogoTask{
 fn preflight(&self,kind:Kind,owner:Arc<dyn Send+Sync>)->BoxFuture<'_,Result<(),Error>>{Box::pin(async move{if kind!=Kind::LogoScanner{return Err(Error::Unsupported)}let now=self.clock.now();if !now.is_finite()||!(100_000_000.0..10_000_000_000.0).contains(&now){return Err(Error::Unsupported)}self.scanner.preflight(owner).await.map_err(operational)})}
 fn execute(&self,kind:Kind,owner:Arc<dyn Send+Sync>)->BoxFuture<'_,Result<Normal,Error>>{Box::pin(async move{if kind!=Kind::LogoScanner{return Err(Error::Unsupported)}self.scanner.scan(self.clock.now(),owner).await.map(|_|Normal::Null).map_err(operational)})}
 fn execute_task_terminal(&self,kind:Kind,owner:Arc<dyn Send+Sync>)->BoxFuture<'_,Result<Terminal,Error>>{Box::pin(async move{if kind!=Kind::LogoScanner{return Err(Error::Unsupported)}match self.scanner.scan(self.clock.now(),owner).await{Ok(_)=>Ok(Terminal::Normal(Normal::Null)),Err(ScanError::InvalidMarker)=>Ok(Terminal::Scanner(ScannerFailure::observed_invalid_marker())),Err(error)=>Err(operational(error))}})}
}
