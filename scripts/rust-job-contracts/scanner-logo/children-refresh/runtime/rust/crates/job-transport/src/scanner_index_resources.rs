//! Real SQL and child-publication ownership; disposable copied fixture only.
use crate::{scanner_index_helper::{Helper,Execution},logo_scanner::{Scanner,Roots,Limits,FirstScanHook,Error,Receipt},logo_scanner_effects::RetainedEffects,transport::{Jobs,Config}};
use dispatcharr_output_store::transport::{RedisTransport,TransportConfig};
use std::{sync::{Arc,Weak},time::Duration};
use tokio::{io::{AsyncReadExt,AsyncWriteExt},net::{TcpListener,TcpStream},sync::watch,task::{JoinHandle,JoinSet}};
use sqlx::Connection;
fn reply_frame<'a, R: tokio::io::AsyncRead + Unpin + Send + 'a>(
    reader: &'a mut R,
    budget: &'a mut usize,
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<Vec<u8>>> + Send + 'a>> {
    Box::pin(async move {
        let invalid =
            || std::io::Error::new(std::io::ErrorKind::InvalidData, "Owned RESP2 reply bound");
        if depth > 8 {
            return Err(invalid());
        }
        let mut line = Vec::new();
        loop {
            if *budget == 0 || line.len() >= 1024 {
                return Err(invalid());
            }
            let byte = reader.read_u8().await?;
            *budget -= 1;
            line.push(byte);
            if byte == b'\n' {
                break;
            }
        }
        if !line.ends_with(b"\r\n") || line.len() < 3 {
            return Err(invalid());
        }
        match line[0] {
            b'+' | b'-' | b':' => Ok(line),
            b'$' => {
                let size = std::str::from_utf8(&line[1..line.len() - 2])
                    .map_err(|_| invalid())?
                    .parse::<i64>()
                    .map_err(|_| invalid())?;
                if size == -1 {
                    return Ok(line);
                }
                if size < 0 || size as usize > budget.saturating_sub(2) {
                    return Err(invalid());
                }
                let mut bytes = vec![0; size as usize + 2];
                reader.read_exact(&mut bytes).await?;
                *budget -= bytes.len();
                if !bytes.ends_with(b"\r\n") {
                    return Err(invalid());
                }
                line.extend(bytes);
                Ok(line)
            }
            b'*' => {
                let count = std::str::from_utf8(&line[1..line.len() - 2])
                    .map_err(|_| invalid())?
                    .parse::<i64>()
                    .map_err(|_| invalid())?;
                if count == -1 {
                    return Ok(line);
                }
                if !(0..=128).contains(&count) {
                    return Err(invalid());
                }
                for _ in 0..count {
                    line.extend(reply_frame(reader, budget, depth + 1).await?);
                }
                Ok(line)
            }
            _ => Err(invalid()),
        }
    })
}
struct Proxy {
    base: String,
    hold: watch::Sender<bool>,
    blocked: watch::Receiver<bool>,
    discard: watch::Sender<bool>,
    deny_after_first: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn proxy() -> Proxy {
    let raw = std::env::var("CELERY_BROKER_URL").expect("Explicit owned fixture Redis required");
    let client = redis::Client::open(raw).unwrap();
    let info = client.get_connection_info();
    assert_eq!(
        info.redis.password.as_deref(),
        Some("playback-fixture-only")
    );
    let address = match &info.addr {
        redis::ConnectionAddr::Tcp(host, port)
            if matches!(host.as_str(), "localhost" | "127.0.0.1") =>
        {
            format!("{host}:{port}")
        }
        _ => panic!("Owned loopback fixture required"),
    };
    let mut bound = None;
    for port in 61000..61128 {
        if let Ok(value) = TcpListener::bind(("127.0.0.1", port)).await {
            bound = Some(value);
            break;
        }
    }
    let listener = bound.expect("Proxy port bound");
    let port = listener.local_addr().unwrap().port();
    let (hold, receiver) = watch::channel(false);
    let (discard, discarded) = watch::channel(false);
    let (blocked, observed) = watch::channel(false);
    let (deny_after_first,denial)=watch::channel(false);
    let arm=deny_after_first.clone();
    let observed_positive=Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept()=>{
                    let(client,_)=accepted.unwrap();let address=address.clone();let mut hold=receiver.clone();let blocked=blocked.clone();let discard=discarded.clone();let denial=denial.clone();let armed=arm.clone();let positives=observed_positive.clone();
                    connections.spawn(async move{
                        let server=TcpStream::connect(address).await.unwrap();
                        let(mut cr,mut cw)=client.into_split();let(sr,mut sw)=server.into_split();
                        let incoming=async move{
                            let mut sr=tokio::io::BufReader::new(sr);
                            loop{
                                let mut budget=524288;let buffer=reply_frame(&mut sr,&mut budget,0).await?;
                                if *denial.borrow()&&buffer.first()==Some(&b':')&&std::str::from_utf8(&buffer[1..buffer.len()-2]).ok().and_then(|v|v.parse::<i64>().ok()).is_some_and(|v|v>0)&&positives.fetch_add(1,std::sync::atomic::Ordering::AcqRel)==1{
                                    armed.send_replace(false);
                                    let _:String=redis::cmd("ACL").arg("SETUSER").arg("default").arg("-lpush").query_async(&mut direct(0).await).await.unwrap();
                                }
                                if *discard.borrow(){blocked.send_replace(true);return Err(std::io::Error::from(std::io::ErrorKind::ConnectionReset));}
                                if *hold.borrow(){
                                    blocked.send_replace(true);
                                    if hold.wait_for(|paused|!*paused).await.is_err(){return Ok::<(),std::io::Error>(());}
                                }
                                cw.write_all(&buffer).await?;
                            }
                        };
                        let _=tokio::try_join!(async{tokio::io::copy(&mut cr,&mut sw).await.map(|_|())},incoming);
                    });
                },
                _=connections.join_next(),if !connections.is_empty()=>{},
            }
        }
    });
    Proxy {
        base: format!("redis://:playback-fixture-only@127.0.0.1:{port}"),
        hold,
        blocked: observed,
        discard,
        deny_after_first,
        task,
    }
}

struct Caller;
async fn direct(db:i64)->redis::aio::MultiplexedConnection{redis::Client::open(format!("redis://:playback-fixture-only@localhost:6379/{db}")).unwrap().get_multiplexed_async_connection().await.unwrap()}
async fn queue()->Vec<Vec<u8>>{redis::cmd("LRANGE").arg("celery").arg(0).arg(-1).query_async(&mut direct(8).await).await.unwrap()}
async fn setup(broker:&str)->(Scanner,Arc<Helper>,sqlx::PgPool,i64){
 assert_eq!(std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),Ok("playback-fixture-only"));assert!(std::env::var("POSTGRES_DB").unwrap().starts_with("rust_probe_backup_jobs_"));crate::default_peer::validate_fixture_redis();
 for path in ["/data/m3us","/data/epgs","/data/logos"]{std::fs::create_dir_all(path).unwrap();assert!(std::fs::read_dir(path).unwrap().next().is_none())}
 let pool=sqlx::postgres::PgPoolOptions::new().max_connections(1).connect(&std::env::var("DATABASE_URL").unwrap()).await.unwrap();sqlx::query("UPDATE epg_epgsource SET is_active=false").execute(&pool).await.unwrap();let id:i64=sqlx::query_scalar("INSERT INTO epg_epgsource(name,source_type,is_active,refresh_interval,custom_properties,priority,status,created_at) VALUES($1,'xmltv',true,0,'{}'::jsonb,0,'idle',NOW()) RETURNING id::bigint").bind(format!("rust-owned-helper-resource-{}",uuid::Uuid::new_v4())).fetch_one(&pool).await.unwrap();
 let jobs=Jobs::connect_defaults(broker,&std::env::var("CELERY_RESULT_BACKEND").unwrap(),Config{operations:1,max_queued_jobs:64,command_timeout:Duration::from_secs(2)}).await.unwrap();
 // Warm the exact script before holding replies (do not mistake NOSCRIPT for
 // actual committed publication). Remove only this setup message by raw value.
 let warm=uuid::Uuid::new_v4();assert!(matches!(jobs.publish_scanner_index(warm,id,Arc::new(())).await.unwrap(),crate::scanner_index_helper::Publication::Committed(_)));let frames=queue().await;let raw=frames.into_iter().find(|v|serde_json::from_slice::<serde_json::Value>(v).unwrap()["headers"]["parent_id"]==warm.to_string()).unwrap();let removed:i64=redis::cmd("LREM").arg("celery").arg(1).arg(raw).query_async(&mut direct(8).await).await.unwrap();assert_eq!(removed,1);
 let cache=RedisTransport::connect("redis://:playback-fixture-only@localhost:6379/0",TransportConfig{max_operations:2,max_queued_operations:2,command_timeout:Duration::from_secs(3)}).await.unwrap();let sink=Arc::new(dispatcharr_realtime::RealtimeBus::connect("redis://:playback-fixture-only@localhost:6379/0",Default::default()).await.unwrap());let effects=Arc::new(RetainedEffects::new(pool.clone(),cache,sink));let scanner=Scanner::new(effects,Roots{m3u:"/data/m3us".into(),epg:"/data/epgs".into(),logos:"/data/logos".into()},Limits::default()).unwrap();let helper=Arc::new(Helper{pool:pool.clone(),jobs,publication:None});(scanner,helper,pool,id)
}
struct Held{execution:Arc<Execution>,hold:watch::Sender<bool>,discard:watch::Sender<bool>,lost:bool}
impl FirstScanHook for Held{
 fn admit<'a>(&'a self,r:Arc<Receipt>)->std::pin::Pin<Box<dyn std::future::Future<Output=Result<(),Error>>+Send+'a>>{self.execution.admit(r)}
 fn finish<'a>(&'a self,r:Arc<Receipt>)->std::pin::Pin<Box<dyn std::future::Future<Output=Result<(),Error>>+Send+'a>>{Box::pin(async move{if self.lost{self.discard.send_replace(true);}else{self.hold.send_replace(true);}self.execution.finish(r).await})}
}
async fn cleanup(scanner:Scanner,helper:Arc<Helper>,pool:sqlx::PgPool,id:i64){assert!(scanner.shutdown(Duration::from_secs(5)).await);helper.jobs.shutdown(Duration::from_secs(5)).await.unwrap();sqlx::query("UPDATE epg_epgsource SET is_active=false WHERE id=$1").bind(id).execute(&pool).await.unwrap();pool.close().await;}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore]
async fn b_child_reply_caller_abort_retains_flag_receipt_and_admission(){
 let p=proxy().await;let(scanner,helper,pool,id)=setup(&format!("{}/8",p.base)).await;let parent=uuid::Uuid::new_v4();let execution=Arc::new(Execution::new(helper.clone(),parent));let hook=Arc::new(Held{execution:execution.clone(),hold:p.hold.clone(),discard:p.discard.clone(),lost:false});let caller=Arc::new(Caller);let weak:Weak<Caller>=Arc::downgrade(&caller);let owned=scanner.clone();let task=tokio::spawn(async move{owned.scan_with_first_scan(1700000000.125,caller,hook).await});let mut blocked=p.blocked.clone();tokio::time::timeout(Duration::from_secs(5),blocked.wait_for(|v|*v)).await.unwrap().unwrap();task.abort();let _=task.await;
 assert!(!scanner.first_scan_completed());assert!(!scanner.shutdown(Duration::from_millis(20)).await);assert!(weak.upgrade().is_some());assert_eq!(helper.jobs.available_slots(),0);assert!(queue().await.iter().any(|v|serde_json::from_slice::<serde_json::Value>(v).unwrap()["headers"]["parent_id"]==parent.to_string()));p.hold.send_replace(false);assert!(scanner.shutdown(Duration::from_secs(5)).await);assert!(scanner.first_scan_completed());assert!(weak.upgrade().is_none());assert_eq!(execution.children().unwrap().ids().len(),1);cleanup(scanner,helper,pool,id).await;
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore]
async fn c_lost_child_reply_never_marks_first_scan_or_guesses_child_success(){
 let p=proxy().await;let(scanner,helper,pool,id)=setup(&format!("{}/8",p.base)).await;let parent=uuid::Uuid::new_v4();let execution=Arc::new(Execution::new(helper.clone(),parent));let hook=Arc::new(Held{execution:execution.clone(),hold:p.hold.clone(),discard:p.discard.clone(),lost:true});let result=scanner.scan_with_first_scan(1700000000.125,Arc::new(()),hook).await;assert!(matches!(result,Err(Error::Operational)));assert!(!scanner.first_scan_completed());assert!(execution.children().unwrap().ids().is_empty());assert!(queue().await.iter().any(|v|serde_json::from_slice::<serde_json::Value>(v).unwrap()["headers"]["parent_id"]==parent.to_string()));cleanup(scanner,helper,pool,id).await;
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore]
async fn d_actual_sql_wait_retains_caller_admission_and_settles_after_cancel(){
 let(scanner,helper,pool,id)=setup(&std::env::var("CELERY_BROKER_URL").unwrap()).await;let mut blocker=sqlx::PgConnection::connect(&std::env::var("DATABASE_URL").unwrap()).await.unwrap();let mut lock=blocker.begin().await.unwrap();sqlx::query("LOCK TABLE epg_epgsource IN ACCESS EXCLUSIVE MODE").execute(&mut*lock).await.unwrap();let execution=Arc::new(Execution::new(helper.clone(),uuid::Uuid::new_v4()));let caller=Arc::new(Caller);let weak:Weak<Caller>=Arc::downgrade(&caller);let owned=scanner.clone();let task=tokio::spawn(async move{owned.scan_with_first_scan(1700000000.125,caller,execution).await});
 tokio::time::timeout(Duration::from_secs(5),async{loop{sqlx::query("SELECT pg_stat_clear_snapshot()").execute(&mut*lock).await.unwrap();let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%epg_epgsource%')").fetch_one(&mut*lock).await.unwrap();if waiting{break}tokio::time::sleep(Duration::from_millis(10)).await;}}).await.unwrap();task.abort();let _=task.await;assert!(!scanner.shutdown(Duration::from_millis(20)).await);assert!(!scanner.first_scan_completed());assert!(weak.upgrade().is_some());lock.commit().await.unwrap();blocker.close().await.unwrap();assert!(scanner.shutdown(Duration::from_secs(5)).await);assert!(scanner.first_scan_completed());assert!(weak.upgrade().is_none());cleanup(scanner,helper,pool,id).await;
}

struct Fixed;impl crate::logo_task::Clock for Fixed{fn now(&self)->f64{1700000000.125}}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore]
async fn a_actual_second_child_denial_keeps_first_child_and_source_parent_null(){
 let p=proxy().await;let(scanner,helper,pool,id)=setup(&format!("{}/8",p.base)).await;
 for _ in 0..2{let _:i64=sqlx::query_scalar("INSERT INTO epg_epgsource(name,source_type,is_active,refresh_interval,custom_properties,priority,status,created_at) VALUES($1,'xmltv',true,0,'{}'::jsonb,0,'idle',NOW()) RETURNING id::bigint").bind(format!("rust-owned-helper-resource-{}",uuid::Uuid::new_v4())).fetch_one(&pool).await.unwrap();}
 let parent=uuid::Uuid::parse_str(&std::env::var("HELPER_PARTIAL_TASK_ID").unwrap()).unwrap();let worker=crate::default_worker::DefaultWorker::new_logo_scanner(helper.jobs.clone(),Arc::new(super::Domain{scanner:scanner.clone(),helper:helper.clone(),clock:Arc::new(Fixed)})).unwrap();p.deny_after_first.send_replace(true);let outcome=worker.run_once(Arc::new(())).await;
 let _:String=redis::cmd("ACL").arg("SETUSER").arg("default").arg("+lpush").query_async(&mut direct(0).await).await.unwrap();
 assert!(matches!(outcome,Ok(crate::default_worker::Run::Completed(id))if id==parent));assert!(scanner.first_scan_completed());let frames=queue().await;let children:Vec<_>=frames.iter().map(|v|serde_json::from_slice::<serde_json::Value>(v).unwrap()).filter(|v|v["headers"]["parent_id"]==parent.to_string()).collect();assert_eq!(children.len(),1);let raw:Vec<u8>=redis::cmd("GET").arg(format!("celery-tasks:celery-task-meta-{parent}")).query_async(&mut direct(9).await).await.unwrap();let meta:serde_json::Value=serde_json::from_slice(&raw).unwrap();assert_eq!(meta["status"],"SUCCESS");assert!(meta["result"].is_null());assert_eq!(meta["children"],serde_json::json!([[[children[0]["headers"]["id"],null],null]]));assert!(worker.shutdown(Duration::from_secs(5)).await);cleanup(scanner,helper,pool,id).await;
}
