//! Actual logo-task metadata reply ownership; copied fixture only.
use crate::{logo_application::LogoApplication,logo_scanner::{Roots,Limits},logo_scanner_effects::RetainedEffects,logo_task::Clock,default_protocol::{Kind,Message,Normal},transport::{Jobs,Config}};
use dispatcharr_output_store::transport::{RedisTransport,TransportConfig};
use std::{sync::{Arc,Weak},time::Duration};
use tokio::{io::{AsyncReadExt,AsyncWriteExt},net::{TcpListener,TcpStream},sync::watch,task::{JoinHandle,JoinSet}};
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
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept()=>{
                    let(client,_)=accepted.unwrap();let address=address.clone();let mut hold=receiver.clone();let blocked=blocked.clone();let discard=discarded.clone();
                    connections.spawn(async move{
                        let server=TcpStream::connect(address).await.unwrap();
                        let(mut cr,mut cw)=client.into_split();let(sr,mut sw)=server.into_split();
                        let incoming=async move{
                            let mut sr=tokio::io::BufReader::new(sr);
                            loop{
                                let mut budget=524288;let buffer=reply_frame(&mut sr,&mut budget,0).await?;
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
        task,
    }
}

struct Fixed;
impl Clock for Fixed{fn now(&self)->f64{1700000000.125}}
struct Caller;
const CACHE:&str="redis://:playback-fixture-only@localhost:6379/0";
async fn direct(db:i64)->redis::aio::MultiplexedConnection{redis::Client::open(format!("redis://:playback-fixture-only@localhost:6379/{db}")).unwrap().get_multiplexed_async_connection().await.unwrap()}
async fn raw(id:uuid::Uuid)->Option<Vec<u8>>{redis::cmd("GET").arg(format!("celery-tasks:celery-task-meta-{id}")).query_async(&mut direct(9).await).await.unwrap()}
async fn setup(result:&str,failure:bool)->(LogoApplication,Jobs,sqlx::PgPool,String){
 assert_eq!(std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),Ok("playback-fixture-only"));assert!(std::env::var("POSTGRES_DB").unwrap().starts_with("rust_probe_backup_jobs_"));crate::default_peer::validate_fixture_redis();
 let jobs=Jobs::connect_defaults(&std::env::var("CELERY_BROKER_URL").unwrap(),result,Config{operations:1,command_timeout:Duration::from_millis(50),max_queued_jobs:64}).await.unwrap();
 let pool=sqlx::postgres::PgPoolOptions::new().max_connections(1).connect(&std::env::var("DATABASE_URL").unwrap()).await.unwrap();sqlx::query("UPDATE epg_epgsource SET is_active=false").execute(&pool).await.unwrap();
 let path=format!("/data/logos/rust-owned-task-{}.png",uuid::Uuid::new_v4());let file=std::fs::File::create(&path).unwrap();file.set_times(std::fs::FileTimes::new().set_modified(std::time::UNIX_EPOCH+Duration::from_millis(1699999992125))).unwrap();
 if failure{let _:()=redis::cmd("SET").arg(format!("processed_file:{path}")).arg("owned-invalid-float").query_async(&mut direct(0).await).await.unwrap();}
 let cache=RedisTransport::connect(CACHE,TransportConfig{max_operations:2,max_queued_operations:2,command_timeout:Duration::from_secs(2)}).await.unwrap();let sink=Arc::new(dispatcharr_realtime::RealtimeBus::connect(CACHE,Default::default()).await.unwrap());let effects=Arc::new(RetainedEffects::new(pool.clone(),cache,sink));let app=LogoApplication::prepare(jobs.clone(),effects,Roots{m3u:"/data/m3us".into(),epg:"/data/epgs".into(),logos:"/data/logos".into()},Limits::default(),Arc::new(Fixed),Duration::from_millis(50)).unwrap();(app,jobs,pool,path)
}
async fn cleanup(app:LogoApplication,jobs:Jobs,pool:sqlx::PgPool,path:String){assert!(app.shutdown(Duration::from_secs(5)).await.drained);jobs.shutdown(Duration::from_secs(5)).await.unwrap();sqlx::query("DELETE FROM dispatcharr_channels_logo WHERE url=$1").bind(&path).execute(&pool).await.unwrap();let _:i64=redis::cmd("DEL").arg(format!("processed_file:{path}")).query_async(&mut direct(0).await).await.unwrap();std::fs::remove_file(path).unwrap();pool.close().await;}
async fn warm(jobs:&Jobs){jobs.write_default_success(&Message{task_id:uuid::Uuid::new_v4(),kind:Kind::LogoScanner,expires:None},Normal::Null,Arc::new(())).await.unwrap();}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore]
async fn a_failure_result_reply_abort_retains_worker_and_caller_until_physical_done(){
 let p=proxy().await;let(app,jobs,pool,path)=setup(&format!("{}/9",p.base),true).await;warm(&jobs).await;let message=Message{task_id:uuid::Uuid::new_v4(),kind:Kind::LogoScanner,expires:None};jobs.publish_default(&message,Arc::new(())).await.unwrap();p.hold.send_replace(true);let caller=Arc::new(Caller);let weak:Weak<Caller>=Arc::downgrade(&caller);let owned=app.clone();let task=tokio::spawn(async move{owned.run_once(caller).await});let mut blocked=p.blocked.clone();tokio::time::timeout(Duration::from_secs(5),blocked.wait_for(|v|*v)).await.unwrap().unwrap();task.abort();let _=task.await;tokio::time::sleep(Duration::from_millis(80)).await;let value:serde_json::Value=serde_json::from_slice(&raw(message.task_id).await.unwrap()).unwrap();assert_eq!(value["status"],"FAILURE");assert_eq!(value["result"]["exc_type"],"ValueError");assert!(!app.shutdown(Duration::from_millis(20)).await.drained);assert_eq!(jobs.available_slots(),0);assert!(weak.upgrade().is_some());p.hold.send_replace(false);assert!(app.shutdown(Duration::from_secs(5)).await.drained);assert!(weak.upgrade().is_none());cleanup(app,jobs,pool,path).await;
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore]
async fn b_lost_result_reply_is_operational_even_if_metadata_committed(){
 let p=proxy().await;let(app,jobs,pool,path)=setup(&format!("{}/9",p.base),true).await;warm(&jobs).await;let message=Message{task_id:uuid::Uuid::new_v4(),kind:Kind::LogoScanner,expires:None};jobs.publish_default(&message,Arc::new(())).await.unwrap();p.discard.send_replace(true);let value=app.run_once(Arc::new(())).await;assert!(value.is_err());let value:serde_json::Value=serde_json::from_slice(&raw(message.task_id).await.unwrap()).unwrap();assert_eq!(value["status"],"FAILURE");assert!(app.shutdown(Duration::from_secs(5)).await.drained);cleanup(app,jobs,pool,path).await;
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore]
async fn c_backend_denial_never_fabricates_terminal_after_committed_logo(){
 let _:String=redis::cmd("ACL").arg("SETUSER").arg("rust-owned-logo-result-denied").arg("on").arg(">playback-fixture-only").arg("~celery-tasks:*").arg("&celery-tasks:*").arg("+@all").arg("-setex").query_async(&mut direct(0).await).await.unwrap();let(app,jobs,pool,path)=setup("redis://rust-owned-logo-result-denied:playback-fixture-only@localhost:6379/9",false).await;let message=Message{task_id:uuid::Uuid::new_v4(),kind:Kind::LogoScanner,expires:None};jobs.publish_default(&message,Arc::new(())).await.unwrap();assert!(app.run_once(Arc::new(())).await.is_err());assert!(raw(message.task_id).await.is_none());let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_logo WHERE url=$1)").bind(&path).fetch_one(&pool).await.unwrap();assert!(exists);cleanup(app,jobs,pool,path).await;let _:i64=redis::cmd("ACL").arg("DELUSER").arg("rust-owned-logo-result-denied").query_async(&mut direct(0).await).await.unwrap();
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore]
async fn d_publish_denial_partial_metadata_is_operational_without_parity_claim(){
 let _:String=redis::cmd("ACL").arg("SETUSER").arg("rust-owned-logo-publish-denied").arg("on").arg(">playback-fixture-only").arg("~celery-tasks:*").arg("resetchannels").arg("+@all").query_async(&mut direct(0).await).await.unwrap();let(app,jobs,pool,path)=setup("redis://rust-owned-logo-publish-denied:playback-fixture-only@localhost:6379/9",false).await;let message=Message{task_id:uuid::Uuid::new_v4(),kind:Kind::LogoScanner,expires:None};jobs.publish_default(&message,Arc::new(())).await.unwrap();assert!(app.run_once(Arc::new(())).await.is_err());let metadata:serde_json::Value=serde_json::from_slice(&raw(message.task_id).await.unwrap()).unwrap();assert_eq!(metadata["status"],"SUCCESS");assert!(metadata["result"].is_null());cleanup(app,jobs,pool,path).await;let _:i64=redis::cmd("ACL").arg("DELUSER").arg("rust-owned-logo-publish-denied").query_async(&mut direct(0).await).await.unwrap();
 // This distinguishes actual partial native storage from observed completion;
 // Original pipeline-vs-Lua ACL semantics need a separate Source fault gate.
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore]
async fn z_foreign_expired_cleanup_is_preserved_before_expiry_side_effects(){
 let(app,jobs,pool,path)=setup(&std::env::var("CELERY_RESULT_BACKEND").unwrap(),false).await;let when=chrono::DateTime::from_timestamp(chrono::Utc::now().timestamp()-1,0).unwrap();let message=Message{task_id:uuid::Uuid::new_v4(),kind:Kind::RedisBackendCleanup,expires:Some(crate::cleanup_expiry::CleanupExpiration::new(when,0).unwrap())};jobs.publish_default(&message,Arc::new(())).await.unwrap();let before:Vec<Vec<u8>>=redis::cmd("LRANGE").arg("celery").arg(0).arg(-1).query_async(&mut direct(8).await).await.unwrap();let value=app.run_once(Arc::new(())).await.unwrap();assert!(matches!(value,crate::default_worker::Run::Unsupported(Kind::RedisBackendCleanup)));let after:Vec<Vec<u8>>=redis::cmd("LRANGE").arg("celery").arg(0).arg(-1).query_async(&mut direct(8).await).await.unwrap();assert_eq!(before,after);assert!(raw(message.task_id).await.is_none());assert!(!app.first_scan_completed());cleanup(app,jobs,pool,path).await;
 // This owned foreign message is deliberately left intact until project cleanup.
}
