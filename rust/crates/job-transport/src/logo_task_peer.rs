//! Actual Source producer/consumer peer for inert logo application, copied only.
use crate::{logo_application::LogoApplication,logo_scanner::{Roots,Limits},logo_scanner_effects::RetainedEffects,logo_task::Clock,transport::{Jobs,Config}};
use dispatcharr_output_store::transport::{RedisTransport,TransportConfig};
use std::{io::{BufRead,Write},sync::{Arc,atomic::{AtomicU64,Ordering}},time::Duration};
struct Fixed(AtomicU64);
impl Clock for Fixed{fn now(&self)->f64{f64::from_bits(self.0.load(Ordering::Acquire))}}
fn line(reader:&mut impl BufRead)->Option<serde_json::Value>{let mut bytes=Vec::new();while bytes.len()<8193{let input=reader.fill_buf().unwrap();if input.is_empty(){break}let count=input.iter().position(|b|*b==b'\n').map_or(input.len(),|i|i+1).min(8193-bytes.len());let done=input[count-1]==b'\n';bytes.extend_from_slice(&input[..count]);reader.consume(count);if done{break}}if bytes.is_empty(){return None}assert!(bytes.len()<=8192&&bytes.ends_with(b"\n"));Some(serde_json::from_slice(&bytes).unwrap())}
fn output(value:serde_json::Value){println!("NATIVE_LOGO_TASK_JSON={value}");std::io::stdout().flush().unwrap()}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore]
async fn native_logo_task_peer(){
 assert_eq!(std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),Ok("playback-fixture-only"));let database=std::env::var("POSTGRES_DB").unwrap();assert!(database.starts_with("rust_probe_backup_jobs_"));crate::default_peer::validate_fixture_redis();
 let stdin=std::io::stdin();let mut reader=stdin.lock();let config=line(&mut reader).unwrap();let clock=Arc::new(Fixed(AtomicU64::new(config["now"].as_f64().unwrap().to_bits())));
 let broker=std::env::var("CELERY_BROKER_URL").unwrap();let result=std::env::var("CELERY_RESULT_BACKEND").unwrap();let jobs=if config["index_only"]==true{Jobs::connect_index(&broker,&result,Config::default()).await.unwrap()}else{Jobs::connect_defaults(&broker,&result,Config::default()).await.unwrap()};
 let cache_url=if let Some(name)=config["marker_acl_user"].as_str(){assert_eq!(name,"rust-owned-scanner-no-set");format!("redis://{name}:playback-fixture-only@localhost:6379/0")}else{"redis://:playback-fixture-only@localhost:6379/0".into()};
 let pool=sqlx::postgres::PgPoolOptions::new().max_connections(1).connect(&std::env::var("DATABASE_URL").unwrap()).await.unwrap();let cache=RedisTransport::connect(&cache_url,TransportConfig{max_operations:4,max_queued_operations:4,command_timeout:Duration::from_secs(3)}).await.unwrap();let sink=Arc::new(dispatcharr_realtime::RealtimeBus::connect("redis://:playback-fixture-only@localhost:6379/0",Default::default()).await.unwrap());
 if config["legacy_notifications"]==true{
  let domains=Arc::new(crate::default_domains::KnownDomains::with_active_notifications(pool.clone(),sink.clone()));let worker=crate::default_worker::DefaultWorker::new(jobs.clone(),domains).unwrap();let application=crate::default_application::DefaultApplication::prepare(worker.clone(),Duration::from_millis(50)).unwrap();output(serde_json::json!({"ready":true,"state":application.state().summary(),"legacy_notifications":true}));
  while let Some(request)=line(&mut reader){match request["action"].as_str().unwrap(){
   "start"=>{application.start().unwrap();output(serde_json::json!({"started":true}));},
   "state"=>output(serde_json::json!({"state":application.state().summary()})),
   "run"=>{let value=worker.run_once(Arc::new(())).await.unwrap();output(serde_json::json!({"run":value.summary()}));},
   "shutdown"=>{let drain=application.shutdown(Duration::from_secs(5)).await;assert!(drain.drained);jobs.shutdown(Duration::from_secs(5)).await.unwrap();output(serde_json::json!({"drained":true}));break},_=>panic!("unsupported legacy fixture action")
  }}pool.close().await;return
 }
 let effects=Arc::new(RetainedEffects::new(pool.clone(),cache,sink));let prepared=LogoApplication::prepare(jobs.clone(),effects,Roots{m3u:"/data/m3us".into(),epg:"/data/epgs".into(),logos:"/data/logos".into()},Limits::default(),clock.clone(),Duration::from_millis(50));
 let application=match prepared{Ok(value)=>value,Err(error)=>{output(serde_json::json!({"ready":false,"error":format!("{error:?}")}));jobs.shutdown(Duration::from_secs(5)).await.unwrap();pool.close().await;return}};
 output(serde_json::json!({"ready":true,"state":application.state().summary(),"first":application.first_scan_completed()}));
 while let Some(request)=line(&mut reader){if let Some(now)=request["now"].as_f64(){clock.0.store(now.to_bits(),Ordering::Release)}match request["action"].as_str().unwrap(){
  "run"=>{let before=application.first_scan_completed();let value=application.run_once(Arc::new(())).await;output(match value{Ok(value)=>serde_json::json!({"ok":true,"run":value.summary(),"first_before":before,"first_after":application.first_scan_completed()}),Err(error)=>serde_json::json!({"ok":false,"error":error.to_string(),"first_before":before,"first_after":application.first_scan_completed()})});},
  "start"=>{let result=application.start();output(serde_json::json!({"started":result.is_ok(),"state":application.state().summary()}));},
  "state"=>output(serde_json::json!({"state":application.state().summary(),"first":application.first_scan_completed()})),
  "shutdown"=>{let drain=application.shutdown(Duration::from_secs(5)).await;assert!(drain.drained);jobs.shutdown(Duration::from_secs(5)).await.unwrap();output(serde_json::json!({"drained":true,"state":drain.state.summary()}));break},_=>panic!("unsupported owned fixture action")
 }}pool.close().await;
}
