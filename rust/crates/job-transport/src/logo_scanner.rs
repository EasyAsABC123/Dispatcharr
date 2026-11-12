//! Inert bounded logo-only scanner; no task/intake authority.
//! Original Dispatcharr/Django/Channels authors retain behavior/code credit.
//! No M3U/EPG creation, Index publication, consumer or terminal-result authority.
use std::{future::Future, path::{Path, PathBuf}, pin::Pin, sync::{Arc, atomic::{AtomicBool, Ordering}}, time::{Duration, UNIX_EPOCH}};
use serde_json::{json, Value};
use tokio::sync::{oneshot, Semaphore};

type FutureResult<'a, T> = Pin<Box<dyn Future<Output=Result<T, Error>> + Send + 'a>>;
pub type Owner = Arc<dyn Send + Sync>;
#[derive(Debug)]
pub enum Error { Capacity, Closed, Unsupported, Filesystem, Operational, InvalidMarker }
/// Only physically observed, settled per-file server failures can be caught.
/// IO/timeouts/lost replies/uncertain rollback MUST return Operational instead.
#[derive(Debug)]
pub enum FileEffect<T> { Settled(T), CaughtServerError }
#[derive(Clone, Copy, Debug)]
pub struct Limits { pub files: usize, pub path_bytes: usize, pub total_path_bytes: usize, pub directories: usize }
impl Default for Limits {
 fn default()->Self {Self{files:128,path_bytes:4096,total_path_bytes:65536,directories:32}}
}
/// A physical port must keep this receipt through ALL actual downstream work,
/// even after its returned future settles/errors. The supervisor waits for drop.
/// There is no blanket adapter to unretained SQL/Redis/event APIs.
pub struct Receipt { _owner: Owner, _slot: Arc<tokio::sync::OwnedSemaphorePermit>, done: Option<oneshot::Sender<()>> }
impl Drop for Receipt {fn drop(&mut self){if let Some(done)=self.done.take(){let _=done.send(());}}}
/// Explicit injectable boundary; concrete retained adapters and real Redis/SQL
/// gates are required before this component can gain any runtime authority.
pub trait Effects: Send+Sync {
 /// Preflight empty eligible Index domain; repeat after summary on first scan.
 /// Must never publish an unqualified child. Concurrent SQL mutation unsupported.
 fn no_index_sources<'a>(&'a self, receipt: Arc<Receipt>)->FutureResult<'a,bool>;
 /// Bounded raw UTF8 marker, <=64bytes, without TTL mutation. Oversize is Unsupported.
 fn marker<'a>(&'a self,path:&'a str,receipt:Arc<Receipt>)->FutureResult<'a,Option<String>>;
 fn exists<'a>(&'a self,path:&'a str,receipt:Arc<Receipt>)->FutureResult<'a,bool>;
 /// Exact get_or_create(url, defaults{name}); source catches settled SQL error.
 /// No ON CONFLICT update/name overwrite. Transaction rollback and physical close
 /// must settle before returning CaughtServerError and releasing the receipt.
 fn get_or_create<'a>(&'a self,path:&'a str,name:&'a str,receipt:Arc<Receipt>)->FutureResult<'a,FileEffect<()>>;
 /// SET processed_file:<absolute-path> <Python repr(mtime)> EX259200.
 fn set_marker<'a>(&'a self,path:&'a str,value:&'a str,receipt:Arc<Receipt>)->FutureResult<'a,FileEffect<()>>;
 /// Original helper catches exceptions. Settled event failure is swallowed;
 /// ambiguous downstream lifetime remains Operational, not synthetic success.
 fn summary<'a>(&'a self,event:Value,receipt:Arc<Receipt>)->FutureResult<'a,FileEffect<()>>;
}
/// Immutable per-execution first-scan helper. The scanner supervisor retains
/// its receipt through physical admission/read/publication and flag transition.
pub trait FirstScanHook: Send+Sync {
 fn admit<'a>(&'a self,receipt:Arc<Receipt>)->FutureResult<'a,()>;
 fn finish<'a>(&'a self,receipt:Arc<Receipt>)->FutureResult<'a,()>;
}
#[derive(Clone)]
pub struct Scanner { effects:Arc<dyn Effects>, roots:Roots, limits:Limits, slot:Arc<Semaphore>, first:Arc<AtomicBool> }
#[derive(Clone)]
pub struct Roots {pub m3u:PathBuf,pub epg:PathBuf,pub logos:PathBuf}
#[derive(Debug,Default,PartialEq,Eq)]
pub struct Counts {pub processed:usize,pub skipped:usize,pub errors:usize,pub total_files:usize}
struct File {path:String,stem:String,supported:bool,mtime:f64}
impl Scanner {
 pub fn new(effects:Arc<dyn Effects>,roots:Roots,limits:Limits)->Result<Self,Error>{
  if limits.files==0||limits.files>128||limits.directories==0||limits.directories>32||limits.path_bytes==0||limits.path_bytes>4096||limits.total_path_bytes==0||limits.total_path_bytes>65536{return Err(Error::Unsupported)}
  let paths=[&roots.m3u,&roots.epg,&roots.logos];
  for (index,path) in paths.iter().enumerate(){
   // Admission restriction, not Original path normalization. Reject lexical
   // aliases/overlapping roots before any filesystem operation.
   let normal:PathBuf=path.components().collect();
   if !path.is_absolute()||path.to_str().is_none()||normal.as_os_str()!=path.as_os_str()||path.components().any(|c|matches!(c,std::path::Component::ParentDir|std::path::Component::CurDir)){return Err(Error::Unsupported)}
   if paths.iter().skip(index+1).any(|other|path.starts_with(other)||other.starts_with(path)){return Err(Error::Unsupported)}
  }
  Ok(Self{effects,roots,limits,slot:Arc::new(Semaphore::new(1)),first:Arc::new(AtomicBool::new(false))})
 }
 pub fn first_scan_completed(&self)->bool{self.first.load(Ordering::Acquire)}
 fn receipt(&self,owner:Owner,slot:Arc<tokio::sync::OwnedSemaphorePermit>)->(Arc<Receipt>,oneshot::Receiver<()>){let(tx,rx)=oneshot::channel();(Arc::new(Receipt{_owner:owner,_slot:slot,done:Some(tx)}),rx)}
 /// Caller cancellation detaches only its result receiver; admitted owned work
 /// continues through filesystem, each port receipt, and final state transition.
 pub async fn scan(&self,now:f64,owner:Owner)->Result<Counts,Error>{
  if !now.is_finite()||!(100_000_000.0..10_000_000_000.0).contains(&now){return Err(Error::Unsupported)}
  let slot=Arc::new(self.slot.clone().try_acquire_owned().map_err(|_|if self.slot.is_closed(){Error::Closed}else{Error::Capacity})?);
  let this=self.clone();let(tx,rx)=oneshot::channel();tokio::spawn(async move{let answer=this.owned(now,owner,slot,None).await;let _=tx.send(answer);});rx.await.map_err(|_|Error::Operational)?
 }
 /// Explicit helper-enabled run; the original scan() keeps its no-index domain.
 pub async fn scan_with_first_scan(&self,now:f64,owner:Owner,hook:Arc<dyn FirstScanHook>)->Result<Counts,Error>{
  if !now.is_finite()||!(100_000_000.0..10_000_000_000.0).contains(&now){return Err(Error::Unsupported)}
  let slot=Arc::new(self.slot.clone().try_acquire_owned().map_err(|_|if self.slot.is_closed(){Error::Closed}else{Error::Capacity})?);let this=self.clone();let(tx,rx)=oneshot::channel();tokio::spawn(async move{let result=this.owned(now,owner,slot,Some(hook)).await;let _=tx.send(result);});rx.await.map_err(|_|Error::Operational)?
 }
 /// Conservative read-only intake admission. Unknown marker grammar and
 /// unported directories/helper domains are refused BEFORE destructive claim.
 /// This is not the Source body; scan() rechecks before mutations. Stable owned
 /// directories/configuration and no external concurrent mutation are required.
 pub async fn preflight(&self,owner:Owner)->Result<(),Error>{self.preflight_hook(owner,None).await}
 pub async fn preflight_with_first_scan(&self,owner:Owner,hook:Arc<dyn FirstScanHook>)->Result<(),Error>{self.preflight_hook(owner,Some(hook)).await}
 async fn preflight_hook(&self,owner:Owner,hook:Option<Arc<dyn FirstScanHook>>)->Result<(),Error>{
  let slot=Arc::new(self.slot.clone().try_acquire_owned().map_err(|_|if self.slot.is_closed(){Error::Closed}else{Error::Capacity})?);
  let this=self.clone();let(tx,rx)=oneshot::channel();tokio::spawn(async move{
   let result=async{
    let(receipt,done)=this.receipt(owner.clone(),slot.clone());let result=if let Some(hook)=&hook{hook.admit(receipt.clone()).await.map(|_|true)}else{this.effects.no_index_sources(receipt.clone()).await};drop(receipt);done.await.map_err(|_|Error::Operational)?;if !result?{return Err(Error::Unsupported)}
    let roots=this.roots.clone();let limits=this.limits;let keep=(owner.clone(),slot.clone());let files=tokio::task::spawn_blocking(move||{let _keep=keep;inspect(roots,limits)}).await.map_err(|_|Error::Operational)??;
    for file in files{if !file.supported{continue}let(receipt,done)=this.receipt(owner.clone(),slot.clone());let result=this.effects.marker(&file.path,receipt.clone()).await;drop(receipt);done.await.map_err(|_|Error::Operational)?;
     if let Some(value)=result?{if !value.is_empty(){match parse_marker(&value){Ok(_)|Err(Error::InvalidMarker)=>{},Err(error)=>return Err(error)}}}
    }Ok(())
   }.await;let _=tx.send(result);
  });rx.await.map_err(|_|Error::Operational)?
 }
 async fn owned(&self,now:f64,owner:Owner,slot:Arc<tokio::sync::OwnedSemaphorePermit>,hook:Option<Arc<dyn FirstScanHook>>)->Result<Counts,Error>{
  macro_rules! physical {($method:ident($($argument:expr),*))=>{{
   let(receipt,done)=self.receipt(owner.clone(),slot.clone());
   let result=self.effects.$method($($argument,)*receipt.clone()).await;drop(receipt);
   done.await.map_err(|_|Error::Operational)?;result?
  }}}
  // Bounded admission checks precede every mutation. This intentionally excludes
  // concurrent changes and first-scan children; it is not full scanner parity.
  if let Some(hook)=&hook{let(receipt,done)=self.receipt(owner.clone(),slot.clone());let result=hook.admit(receipt.clone()).await;drop(receipt);done.await.map_err(|_|Error::Operational)?;result?;}else if !physical!(no_index_sources()){return Err(Error::Unsupported)}
  let roots=self.roots.clone();let limits=self.limits;let keep=(owner.clone(),slot.clone());
  let files=tokio::task::spawn_blocking(move||{let _keep=keep;inspect(roots,limits)}).await.map_err(|_|Error::Operational)??;
  let mut counts=Counts{total_files:files.len(),..Counts::default()};
  for file in files {
   if !file.supported {counts.skipped+=1;continue}
   // Original tests the truth value of decoded GET: both absent and empty
   // string enter the old-existing branch and bypass float conversion.
   let marker=physical!(marker(&file.path)).filter(|value|!value.is_empty());let age=now-file.mtime;
   if marker.is_none()&&age>30.0&&physical!(exists(&file.path)){
    // Original startup SET is outside the per-file catch; no typed failure
    // authority exists here, so a caught server error remains operational.
    match physical!(set_marker(&file.path,&marker_bytes(file.mtime))){FileEffect::Settled(())=>{},FileEffect::CaughtServerError=>return Err(Error::Operational)}
    counts.skipped+=1;continue
   }
   if age<6.0 {counts.skipped+=1;continue}
   if let Some(value)=marker {
    let stored=parse_marker(&value)?;
    if stored>=file.mtime{counts.skipped+=1;continue}
   }
   match physical!(get_or_create(&file.path,&file.stem)){FileEffect::CaughtServerError=>{counts.errors+=1;continue},FileEffect::Settled(())=>{}}
   match physical!(set_marker(&file.path,&marker_bytes(file.mtime))){FileEffect::CaughtServerError=>{counts.errors+=1;continue},FileEffect::Settled(())=>{}}
   counts.processed+=1;
  }
  if counts.processed>0||counts.errors>0 {
   let data=json!({"type":"update","data":{"success":true,"type":"logo_processing_summary","processed":counts.processed,"skipped":counts.skipped,"errors":counts.errors,"total_files":counts.total_files,"message":format!("Logo processing complete: {} processed, {} skipped, {} errors",counts.processed,counts.skipped,counts.errors)}});
   let _settled=physical!(summary(data));
  }
  if !self.first_scan_completed(){if let Some(hook)=hook{let(receipt,done)=self.receipt(owner.clone(),slot.clone());let result=hook.finish(receipt.clone()).await;drop(receipt);done.await.map_err(|_|Error::Operational)?;result?;}else if !physical!(no_index_sources()){return Err(Error::Unsupported)}self.first.store(true,Ordering::Release);}
  Ok(counts)
 }
 pub async fn shutdown(&self,timeout:Duration)->bool{
  self.slot.close();tokio::time::timeout(timeout,async{loop{if self.slot.available_permits()==1{break}tokio::time::sleep(Duration::from_millis(1)).await;}}).await.is_ok()
 }
}
fn parse_marker(value:&str)->Result<f64,Error>{
 if value.len()>64{return Err(Error::Unsupported)}
 if value=="owned-invalid-float"{return Err(Error::InvalidMarker)}
 if value.is_empty()||!value.bytes().all(|b|b.is_ascii_digit()||b==b'.'){return Err(Error::Unsupported)}
 let value=value.parse::<f64>().map_err(|_|Error::Unsupported)?;
 if !value.is_finite(){return Err(Error::Unsupported)}Ok(value)
}
fn marker_bytes(value:f64)->String{dispatcharr_store::source_scanner_epoch_string(value).expect("filesystem preflight bounds finite epoch")}
fn inspect(roots:Roots,limits:Limits)->Result<Vec<File>,Error>{
 fn empty(path:&Path)->Result<bool,Error>{Ok(std::fs::read_dir(path).map_err(|_|Error::Filesystem)?.next().is_none())}
 // Validate roots and ancestors without following symlinks before opening
 // watch directories. Concurrent replacement after this check is unsupported;
 // this is not a descriptor-relative security/race guarantee.
 for root in [&roots.m3u,&roots.epg,&roots.logos]{
  for ancestor in root.ancestors(){
   let metadata=std::fs::symlink_metadata(ancestor).map_err(|_|Error::Filesystem)?;
   if !metadata.is_dir()||metadata.file_type().is_symlink(){return Err(Error::Unsupported)}
  }
 }
 if !empty(&roots.m3u)?||!empty(&roots.epg)?{return Err(Error::Unsupported)}
 let mut files=Vec::new();let mut bytes=0;let mut directories=0;
 fn walk(root:&Path,limits:Limits,files:&mut Vec<File>,bytes:&mut usize,directories:&mut usize)->Result<(),Error>{
  *directories+=1;if *directories>limits.directories{return Err(Error::Unsupported)}
  let mut dirs=Vec::new();
  // No sorting: preserve this filesystem's observed read_dir order. Cross-
  // runtime os.walk ordering still needs the actual paired fixture gate.
  for entry in std::fs::read_dir(root).map_err(|_|Error::Filesystem)? {
   let entry=entry.map_err(|_|Error::Filesystem)?;let meta=entry.metadata().map_err(|_|Error::Filesystem)?;
   if meta.file_type().is_symlink(){return Err(Error::Unsupported)}
   let path=entry.path();let text=path.to_str().ok_or(Error::Unsupported)?;
   if text.len()>limits.path_bytes{return Err(Error::Unsupported)}
   *bytes=bytes.checked_add(text.len()).ok_or(Error::Unsupported)?;if *bytes>limits.total_path_bytes{return Err(Error::Unsupported)}
   if meta.is_dir(){dirs.push(path);if dirs.len()>limits.directories{return Err(Error::Unsupported)}continue}
   if !meta.is_file(){return Err(Error::Unsupported)}
   if files.len()==limits.files{return Err(Error::Unsupported)}
   let extension=path.extension().and_then(|s|s.to_str()).unwrap_or("").to_ascii_lowercase();
   let supported=matches!(extension.as_str(),"jpg"|"jpeg"|"png"|"gif"|"webp"|"bmp"|"svg");
   // Source checks isfile, then extension, before getmtime/GET/name handling.
   // Unsupported extensions still consume the common file/path memory budget.
   if !supported{files.push(File{path:text.to_owned(),stem:String::new(),supported:false,mtime:0.0});continue}
   let modified=meta.modified().map_err(|_|Error::Filesystem)?.duration_since(UNIX_EPOCH).map_err(|_|Error::Unsupported)?;
   // Normal finite epoch nanoseconds are admitted. Source/native stat-bit
   // and formatter vectors are separately bound; no special marker grammar
   // or cross-filesystem precision normalization is inferred.
   let mtime=modified.as_secs_f64();
   if !(100_000_000.0..10_000_000_000.0).contains(&mtime){return Err(Error::Unsupported)}
   let stem=path.file_stem().and_then(|s|s.to_str()).ok_or(Error::Unsupported)?.to_owned();
   if stem.chars().count()>255{return Err(Error::Unsupported)}
   files.push(File{path:text.to_owned(),stem,supported:true,mtime});
  }
  for dir in dirs{walk(&dir,limits,files,bytes,directories)?}Ok(())
 }
 walk(&roots.logos,limits,&mut files,&mut bytes,&mut directories)?;Ok(files)
}
