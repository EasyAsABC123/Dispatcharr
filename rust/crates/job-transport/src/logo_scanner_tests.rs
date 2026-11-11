//! Draft algorithm/ownership controls, UNEXECUTED. Not real SQL/Redis parity.
use super::logo_scanner::*;
use std::{collections::HashMap,sync::{Arc,Mutex,atomic::{AtomicBool,Ordering}},time::{Duration,UNIX_EPOCH},path::PathBuf,future::Future,pin::Pin};
type FutureResult<'a,T> = Pin<Box<dyn Future<Output=Result<T,Error>>+Send+'a>>;
use tokio::sync::Notify;
struct Ports { markers:Mutex<HashMap<String,String>>, rows:Mutex<Vec<(String,String)>>, events:Mutex<Vec<serde_json::Value>>, held:Mutex<Option<Arc<Receipt>>>, entered:Notify, hold:AtomicBool, denied:AtomicBool }
impl Ports {fn new()->Self{Self{markers:Mutex::new(HashMap::new()),rows:Mutex::new(Vec::new()),events:Mutex::new(Vec::new()),held:Mutex::new(None),entered:Notify::new(),hold:AtomicBool::new(false),denied:AtomicBool::new(false)}}}
impl Effects for Ports {
 fn no_index_sources<'a>(&'a self,_receipt:Arc<Receipt>)->FutureResult<'a,bool>{Box::pin(async{Ok(true)})}
 fn marker<'a>(&'a self,path:&'a str,receipt:Arc<Receipt>)->FutureResult<'a,Option<String>>{Box::pin(async move{if self.hold.swap(false,Ordering::AcqRel){*self.held.lock().unwrap()=Some(receipt);self.entered.notify_one()}Ok(self.markers.lock().unwrap().get(path).cloned())})}
 fn exists<'a>(&'a self,path:&'a str,_receipt:Arc<Receipt>)->FutureResult<'a,bool>{Box::pin(async move{Ok(self.rows.lock().unwrap().iter().any(|(p,_)|p==path))})}
 fn get_or_create<'a>(&'a self,path:&'a str,name:&'a str,_receipt:Arc<Receipt>)->FutureResult<'a,FileEffect<()>>{Box::pin(async move{let mut rows=self.rows.lock().unwrap();if !rows.iter().any(|(p,_)|p==path){rows.push((path.into(),name.into()))}Ok(FileEffect::Settled(()))})}
 fn set_marker<'a>(&'a self,path:&'a str,value:&'a str,_receipt:Arc<Receipt>)->FutureResult<'a,FileEffect<()>>{Box::pin(async move{if self.denied.load(Ordering::Acquire){return Ok(FileEffect::CaughtServerError)}self.markers.lock().unwrap().insert(path.into(),value.into());Ok(FileEffect::Settled(()))})}
 fn summary<'a>(&'a self,event:serde_json::Value,_receipt:Arc<Receipt>)->FutureResult<'a,FileEffect<()>>{Box::pin(async move{self.events.lock().unwrap().push(event);Ok(FileEffect::Settled(()))})}
}
fn fixture()->(PathBuf,Roots){let root=std::env::temp_dir().join(format!("rust-owned-logo-draft-{}",uuid::Uuid::new_v4()));let roots=Roots{m3u:root.join("m3us"),epg:root.join("epgs"),logos:root.join("logos")};for p in [&roots.m3u,&roots.epg,&roots.logos]{std::fs::create_dir_all(p).unwrap()}(root,roots)}
fn file(roots:&Roots,name:&str,mtime:f64)->String{let p=roots.logos.join(name);if let Some(parent)=p.parent(){std::fs::create_dir_all(parent).unwrap()}let f=std::fs::File::create(&p).unwrap();f.set_times(std::fs::FileTimes::new().set_modified(UNIX_EPOCH+Duration::from_secs_f64(mtime))).unwrap();p.to_str().unwrap().into()}
#[tokio::test]
async fn observed_positive_then_skip_and_marker_error_keeps_row(){
 let(root,roots)=fixture();let path=file(&roots,"nested/owned-café-⏰.SVG",1699999992.125);let ports=Arc::new(Ports::new());let scanner=Scanner::new(ports.clone(),roots,Limits::default()).unwrap();
 let first=scanner.scan(1700000000.125,Arc::new(())).await.unwrap();assert_eq!(first,Counts{processed:1,skipped:0,errors:0,total_files:1});assert_eq!(ports.markers.lock().unwrap()[&path],"1699999992.125");assert!(scanner.first_scan_completed());
 let second=scanner.scan(1700000000.125,Arc::new(())).await.unwrap();assert_eq!(second.skipped,1);assert_eq!(ports.events.lock().unwrap().len(),1);
 ports.markers.lock().unwrap().clear();ports.denied.store(true,Ordering::Release);let third=scanner.scan(1700000000.125,Arc::new(())).await.unwrap();assert_eq!(third.errors,1);assert_eq!(ports.rows.lock().unwrap().len(),1);assert!(ports.markers.lock().unwrap().is_empty());std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn late_receipt_holds_admission_after_caller_abort(){
 let(root,roots)=fixture();file(&roots,"owned.png",1699999992.125);let ports=Arc::new(Ports::new());ports.hold.store(true,Ordering::Release);let scanner=Scanner::new(ports.clone(),roots,Limits::default()).unwrap();let copied=scanner.clone();let owner=Arc::new(());let weak=Arc::downgrade(&owner);
 let task=tokio::spawn(async move{copied.scan(1700000000.125,owner).await});ports.entered.notified().await;assert!(!task.is_finished());task.abort();let _=task.await;assert!(weak.upgrade().is_some());assert!(!scanner.shutdown(Duration::from_millis(5)).await);assert!(ports.rows.lock().unwrap().is_empty());drop(ports.held.lock().unwrap().take());assert!(scanner.shutdown(Duration::from_secs(1)).await);assert!(weak.upgrade().is_none());assert_eq!(ports.rows.lock().unwrap().len(),1);std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn unsupported_watch_inputs_and_caps_precede_mutations(){
 let(root,roots)=fixture();std::fs::write(roots.epg.join("owned.xml"),b"owned").unwrap();let ports=Arc::new(Ports::new());let scanner=Scanner::new(ports.clone(),roots.clone(),Limits::default()).unwrap();assert!(matches!(scanner.scan(1700000000.125,Arc::new(())).await,Err(Error::Unsupported)));assert!(ports.rows.lock().unwrap().is_empty());std::fs::remove_file(roots.epg.join("owned.xml")).unwrap();file(&roots,"owned1.png",1699999992.125);file(&roots,"owned2.png",1699999992.125);let scanner=Scanner::new(ports.clone(),roots,Limits{files:1,..Limits::default()}).unwrap();assert!(matches!(scanner.scan(1700000000.125,Arc::new(())).await,Err(Error::Unsupported)));assert!(ports.rows.lock().unwrap().is_empty());std::fs::remove_dir_all(root).unwrap();
}

// Source guard-order controls added during quiet; UNEXECUTED and not a new
// actual Original capture. These cannot qualify transport/SQL parity alone.
#[tokio::test]
async fn empty_markers_follow_absent_branch_without_float_conversion(){
 let(root,roots)=fixture();let old=file(&roots,"old-existing.png",1699999969.125);let current=file(&roots,"fresh-empty-marker.svg",1699999992.125);let ports=Arc::new(Ports::new());
 ports.markers.lock().unwrap().insert(old.clone(),String::new());ports.markers.lock().unwrap().insert(current.clone(),String::new());ports.rows.lock().unwrap().push((old.clone(),"Preserve existing name".into()));
 let scanner=Scanner::new(ports.clone(),roots,Limits::default()).unwrap();let counts=scanner.scan(1700000000.125,Arc::new(())).await.unwrap();
 assert_eq!(counts,Counts{processed:1,skipped:1,errors:0,total_files:2});assert_eq!(ports.markers.lock().unwrap()[&old],"1699999969.125");assert_eq!(ports.markers.lock().unwrap()[&current],"1699999992.125");assert!(ports.rows.lock().unwrap().iter().any(|(path,name)|path==&old&&name=="Preserve existing name"));assert_eq!(ports.events.lock().unwrap().len(),1);std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn unsupported_extension_skips_unqualified_mtime_and_marker(){
 let(root,roots)=fixture();let path=file(&roots,"ignored.txt",1.0);let ports=Arc::new(Ports::new());ports.markers.lock().unwrap().insert(path.clone(),"owned-invalid-float".into());let scanner=Scanner::new(ports.clone(),roots,Limits::default()).unwrap();
 let counts=scanner.scan(1700000000.125,Arc::new(())).await.unwrap();assert_eq!(counts,Counts{processed:0,skipped:1,errors:0,total_files:1});assert_eq!(ports.markers.lock().unwrap()[&path],"owned-invalid-float");assert!(ports.events.lock().unwrap().is_empty());assert!(ports.rows.lock().unwrap().is_empty());std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn aliased_and_overlapping_roots_are_rejected_without_filesystem_access(){
 let ports=Arc::new(Ports::new());
 for roots in [Roots{m3u:"/owned/m3us".into(),epg:"/owned/m3us".into(),logos:"/owned/logos".into()},Roots{m3u:"/owned/m3us".into(),epg:"/owned/epgs".into(),logos:"/owned/epgs/logos".into()},Roots{m3u:"/owned/m3us/../m3us".into(),epg:"/owned/epgs".into(),logos:"/owned/logos".into()}]{assert!(matches!(Scanner::new(ports.clone(),roots,Limits::default()),Err(Error::Unsupported)));}
}
#[cfg(unix)]
#[tokio::test]
async fn dangling_root_symlink_is_rejected_before_directory_open(){
 let(root,roots)=fixture();std::fs::remove_dir(&roots.m3u).unwrap();std::os::unix::fs::symlink(root.join("nonexistent-owned-target"),&roots.m3u).unwrap();let ports=Arc::new(Ports::new());let scanner=Scanner::new(ports.clone(),roots,Limits::default()).unwrap();
 // Opening the dangling target would instead produce Filesystem. The desired
 // Unsupported result establishes the pre-open symlink/type guard ordering.
 assert!(matches!(scanner.scan(1700000000.125,Arc::new(())).await,Err(Error::Unsupported)));assert!(ports.rows.lock().unwrap().is_empty());assert!(ports.events.lock().unwrap().is_empty());std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn ordinary_nanosecond_mtime_is_processed_without_eighth_restriction(){
 let(root,roots)=fixture();let path=roots.logos.join("ordinary.png");let file=std::fs::File::create(&path).unwrap();file.set_times(std::fs::FileTimes::new().set_modified(UNIX_EPOCH+Duration::new(1699999993,123456789))).unwrap();let ports=Arc::new(Ports::new());let scanner=Scanner::new(ports.clone(),roots,Limits::default()).unwrap();let counts=scanner.scan(1700000000.125,Arc::new(())).await.unwrap();assert_eq!(counts.processed,1);assert_eq!(ports.markers.lock().unwrap()[path.to_str().unwrap()],"1699999993.1234567");assert_eq!(ports.events.lock().unwrap().len(),1);std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn bounded_formatter_reuses_existing_decimal_rules_and_rejects_outside_domain(){
 assert_eq!(dispatcharr_store::source_scanner_epoch_string(f64::from_bits(0x4197d78400020000)).as_deref(),Some("100000000.00195312"));
 assert_eq!(dispatcharr_store::source_scanner_epoch_string(1699999993.1234567).as_deref(),Some("1699999993.1234567"));assert_eq!(dispatcharr_store::source_scanner_epoch_string(100000000.0).as_deref(),Some("100000000.0"));for value in [f64::NAN,f64::INFINITY,f64::NEG_INFINITY,-0.0,99999999.0,10000000000.0]{assert!(dispatcharr_store::source_scanner_epoch_string(value).is_none())}
}

#[tokio::test]
async fn task_preflight_is_readonly_and_rejects_unqualified_marker_scope(){
 let(root,roots)=fixture();let path=file(&roots,"ordinary.png",1699999992.125);let ports=Arc::new(Ports::new());let scanner=Scanner::new(ports.clone(),roots,Limits::default()).unwrap();scanner.preflight(Arc::new(())).await.unwrap();assert!(ports.rows.lock().unwrap().is_empty());assert!(ports.events.lock().unwrap().is_empty());assert!(!scanner.first_scan_completed());ports.markers.lock().unwrap().insert(path.clone(),"NaN".into());assert!(matches!(scanner.preflight(Arc::new(())).await,Err(Error::Unsupported)));ports.markers.lock().unwrap().insert(path,"owned-invalid-float".into());scanner.preflight(Arc::new(())).await.unwrap();assert!(ports.rows.lock().unwrap().is_empty());assert!(matches!(scanner.scan(1700000000.125,Arc::new(())).await,Err(Error::InvalidMarker)));std::fs::remove_dir_all(root).unwrap();
}
