//! Disposable copied-DB component peer, not a worker/task/consumer.
use super::{logo_scanner::{Scanner, Roots, Limits},logo_scanner_effects::RetainedEffects};
use dispatcharr_output_store::transport::{RedisTransport,TransportConfig};
use std::{io::{BufRead,Write},sync::Arc,time::Duration};
#[tokio::test]
#[ignore]
async fn native_logo_spool_peer(){
 assert_eq!(std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),Ok("playback-fixture-only"));let database=std::env::var("POSTGRES_DB").unwrap();assert!(database.starts_with("rust_probe_backup_jobs_"));
 let pool=sqlx::postgres::PgPoolOptions::new().max_connections(1).connect(&std::env::var("DATABASE_URL").unwrap()).await.unwrap();
 let sink=Arc::new(dispatcharr_realtime::RealtimeBus::connect("redis://:playback-fixture-only@localhost:6379/0",dispatcharr_realtime::BusConfig::default()).await.unwrap());
 async fn make(pool:sqlx::PgPool,sink:Arc<dispatcharr_realtime::RealtimeBus>,username:Option<&str>)->Scanner{
  let url=if let Some(username)=username{assert_eq!(username,"rust-owned-scanner-no-set");format!("redis://{username}:playback-fixture-only@localhost:6379/0")}else{"redis://:playback-fixture-only@localhost:6379/0".into()};
  let cache=RedisTransport::connect(&url,TransportConfig{max_operations:4,max_queued_operations:4,command_timeout:Duration::from_secs(3)}).await.unwrap();
  Scanner::new_spooled(Arc::new(RetainedEffects::new(pool,cache,sink)),Roots{m3u:"/data/m3us".into(),epg:"/data/epgs".into(),logos:"/data/logos".into()},Limits{files:4096,..Limits::default()},crate::logo_scanner::SpoolConfig{directory:"/data/scanner-work".into(),max_bytes:256*1024}).unwrap()
 }
 std::fs::create_dir_all("/data/scanner-work").unwrap();
 let scanner=make(pool.clone(),sink.clone(),None).await;println!("NATIVE_LOGO_JSON={{\"ready\":true}}");std::io::stdout().flush().unwrap();
 let stdin=std::io::stdin();let mut input=stdin.lock();loop{
  let mut bytes=Vec::new();while bytes.len()<8193{let buffer=input.fill_buf().unwrap();if buffer.is_empty(){break}let count=buffer.iter().position(|b|*b==b'\n').map_or(buffer.len(),|v|v+1).min(8193-bytes.len());let newline=buffer[count-1]==b'\n';bytes.extend_from_slice(&buffer[..count]);input.consume(count);if newline{break}}
  if bytes.is_empty(){break}assert!(bytes.len()<=8192&&bytes.ends_with(b"\n"));let request:serde_json::Value=serde_json::from_slice(&bytes).unwrap();
  if request["action"]=="shutdown"{assert!(scanner.shutdown(Duration::from_secs(5)).await);println!("NATIVE_LOGO_JSON={{\"shutdown\":true}}");std::io::stdout().flush().unwrap();break}
  let temporary=if let Some(username)=request["marker_acl_user"].as_str(){Some(make(pool.clone(),sink.clone(),Some(username)).await)}else{None};let current=temporary.as_ref().unwrap_or(&scanner);let before=current.first_scan_completed();
  let answer=current.scan(request["now"].as_f64().unwrap(),Arc::new(())).await;
  assert!(std::fs::read_dir("/data/scanner-work").unwrap().filter_map(Result::ok).all(|v|v.file_name()==".scanner-owner-v2"));
  let response=match answer{Ok(counts)=>serde_json::json!({"ok":true,"processed":counts.processed,"skipped":counts.skipped,"errors":counts.errors,"total_files":counts.total_files,"first_before":before,"first_after":current.first_scan_completed()}),Err(error)=>serde_json::json!({"ok":false,"error":format!("{error:?}"),"first_before":before,"first_after":current.first_scan_completed()})};
  if let Some(temporary)=temporary{assert!(temporary.shutdown(Duration::from_secs(5)).await)}println!("NATIVE_LOGO_JSON={response}");std::io::stdout().flush().unwrap();
 }
 pool.close().await;
}
