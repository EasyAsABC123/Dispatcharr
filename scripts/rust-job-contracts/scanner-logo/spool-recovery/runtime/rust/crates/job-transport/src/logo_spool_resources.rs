//! Actual bounded-spool physical ownership; disposable fixture only.
use crate::{logo_scanner::{Scanner,Roots,Limits,SpoolConfig},logo_scanner_effects::RetainedEffects};
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
                                let script_discovery=buffer.starts_with(b"-NOSCRIPT")||(buffer.starts_with(b"$40\r\n")&&buffer.len()==47&&buffer[5..45].iter().all(u8::is_ascii_hexdigit));
                                if script_discovery{cw.write_all(&buffer).await?;continue}
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

const CACHE:&str="redis://:playback-fixture-only@localhost:6379/0";
const SCRATCH:&str="/data/scanner-work";
struct Caller;
async fn setup(cache_url:&str,sink_url:&str)->(Scanner,sqlx::PgPool,Vec<String>){
 assert_eq!(std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),Ok("playback-fixture-only"));assert!(std::env::var("POSTGRES_DB").unwrap().starts_with("rust_probe_backup_jobs_"));crate::default_peer::validate_fixture_redis();
 for path in ["/data/m3us","/data/epgs","/data/logos",SCRATCH]{std::fs::create_dir_all(path).unwrap()}
 assert!(std::fs::read_dir(SCRATCH).unwrap().filter_map(Result::ok).all(|v|v.file_name()==".scanner-owner-v2"));let pool=sqlx::postgres::PgPoolOptions::new().max_connections(1).connect(&std::env::var("DATABASE_URL").unwrap()).await.unwrap();sqlx::query("UPDATE epg_epgsource SET is_active=false").execute(&pool).await.unwrap();let mut paths=Vec::new();
 for n in 0..257{let path=format!("/data/logos/rust-owned-spool-resource-{n:03}.png");let file=std::fs::File::create(&path).unwrap();file.set_times(std::fs::FileTimes::new().set_modified(std::time::UNIX_EPOCH+Duration::from_secs(1700000000))).unwrap();paths.push(path)}
 let cache=RedisTransport::connect(cache_url,TransportConfig{max_operations:4,max_queued_operations:4,command_timeout:Duration::from_secs(3)}).await.unwrap();let sink=Arc::new(dispatcharr_realtime::RealtimeBus::connect(sink_url,Default::default()).await.unwrap());let effects=Arc::new(RetainedEffects::new(pool.clone(),cache,sink));let scanner=Scanner::new_spooled(effects,Roots{m3u:"/data/m3us".into(),epg:"/data/epgs".into(),logos:"/data/logos".into()},Limits{files:4096,..Limits::default()},SpoolConfig{directory:SCRATCH.into(),max_bytes:256*1024}).unwrap();(scanner,pool,paths)
}
fn private_file_present(){use std::os::unix::fs::PermissionsExt;let paths=std::fs::read_dir(SCRATCH).unwrap().filter(|v|v.as_ref().map_or(true,|e|e.file_name()!=".scanner-owner-v2")).collect::<Result<Vec<_>,_>>().unwrap();assert_eq!(paths.len(),1);let meta=paths[0].metadata().unwrap();assert_eq!(meta.permissions().mode()&0o777,0o600);assert!(meta.len()>8&&meta.len()<=256*1024);}
async fn cleanup(scanner:Scanner,pool:sqlx::PgPool,paths:Vec<String>){assert!(scanner.shutdown(Duration::from_secs(10)).await);assert!(std::fs::read_dir(SCRATCH).unwrap().filter_map(Result::ok).all(|v|v.file_name()==".scanner-owner-v2"));sqlx::query("DELETE FROM dispatcharr_channels_logo WHERE url LIKE '/data/logos/rust-owned-spool-resource-%'").execute(&pool).await.unwrap();let mut direct=redis::Client::open(CACHE).unwrap().get_multiplexed_async_connection().await.unwrap();for path in paths{let _:i64=redis::cmd("DEL").arg(format!("processed_file:{path}")).query_async(&mut direct).await.unwrap();std::fs::remove_file(path).unwrap();}pool.close().await;}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore]
async fn a_actual_sql_lock_keeps_spool_caller_and_admission_until_close(){
 let(scanner,pool,paths)=setup(CACHE,CACHE).await;let mut blocker=sqlx::PgConnection::connect(&std::env::var("DATABASE_URL").unwrap()).await.unwrap();let mut lock=blocker.begin().await.unwrap();sqlx::query("LOCK TABLE dispatcharr_channels_logo IN ACCESS EXCLUSIVE MODE").execute(&mut*lock).await.unwrap();let caller=Arc::new(Caller);let weak:Weak<Caller>=Arc::downgrade(&caller);let owned=scanner.clone();let task=tokio::spawn(async move{owned.scan(1700000010.0,caller).await});
 tokio::time::timeout(Duration::from_secs(5),async{loop{sqlx::query("SELECT pg_stat_clear_snapshot()").execute(&mut*lock).await.unwrap();let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%dispatcharr_channels_logo%')").fetch_one(&mut*lock).await.unwrap();if blocked{break}tokio::time::sleep(Duration::from_millis(5)).await}}).await.unwrap();task.abort();let _=task.await;private_file_present();assert!(weak.upgrade().is_some());assert!(!scanner.first_scan_completed());assert!(!scanner.shutdown(Duration::from_millis(20)).await);lock.commit().await.unwrap();blocker.close().await.unwrap();assert!(scanner.shutdown(Duration::from_secs(10)).await);assert!(weak.upgrade().is_none());assert!(scanner.first_scan_completed());cleanup(scanner,pool,paths).await;
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore]
async fn b_actual_cache_reply_keeps_spool_caller_and_admission_until_close(){
 let p=proxy().await;let(scanner,pool,paths)=setup(&format!("{}/0",p.base),CACHE).await;p.hold.send_replace(true);let caller=Arc::new(Caller);let weak:Weak<Caller>=Arc::downgrade(&caller);let owned=scanner.clone();let task=tokio::spawn(async move{owned.scan(1700000010.0,caller).await});let mut blocked=p.blocked.clone();tokio::time::timeout(Duration::from_secs(5),blocked.wait_for(|v|*v)).await.unwrap().unwrap();task.abort();let _=task.await;private_file_present();assert!(weak.upgrade().is_some());assert!(!scanner.first_scan_completed());assert!(!scanner.shutdown(Duration::from_millis(20)).await);p.hold.send_replace(false);assert!(scanner.shutdown(Duration::from_secs(10)).await);assert!(weak.upgrade().is_none());assert!(scanner.first_scan_completed());cleanup(scanner,pool,paths).await;
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore]
async fn c_actual_channels_reply_after_commits_keeps_spool_until_delivery_settles(){
 let p=proxy().await;let(scanner,pool,paths)=setup(CACHE,&format!("{}/0",p.base)).await;p.hold.send_replace(true);let caller=Arc::new(Caller);let weak:Weak<Caller>=Arc::downgrade(&caller);let owned=scanner.clone();let task=tokio::spawn(async move{owned.scan(1700000010.0,caller).await});let mut blocked=p.blocked.clone();tokio::time::timeout(Duration::from_secs(10),blocked.wait_for(|v|*v)).await.unwrap().unwrap();let rows:i64=sqlx::query_scalar("SELECT count(*) FROM dispatcharr_channels_logo WHERE url LIKE '/data/logos/rust-owned-spool-resource-%'").fetch_one(&pool).await.unwrap();assert_eq!(rows,257);task.abort();let _=task.await;private_file_present();assert!(weak.upgrade().is_some());assert!(!scanner.first_scan_completed());assert!(!scanner.shutdown(Duration::from_millis(20)).await);p.hold.send_replace(false);assert!(scanner.shutdown(Duration::from_secs(10)).await);assert!(weak.upgrade().is_none());assert!(scanner.first_scan_completed());cleanup(scanner,pool,paths).await;
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore]
async fn d_lost_cache_reply_is_operational_with_spool_closed_no_guessed_effects(){
 let p=proxy().await;let(scanner,pool,paths)=setup(&format!("{}/0",p.base),CACHE).await;p.discard.send_replace(true);let result=scanner.scan(1700000010.0,Arc::new(())).await;assert!(matches!(result,Err(crate::logo_scanner::Error::Operational)));assert!(!scanner.first_scan_completed());let count:i64=sqlx::query_scalar("SELECT count(*) FROM dispatcharr_channels_logo WHERE url LIKE '/data/logos/rust-owned-spool-resource-%'").fetch_one(&pool).await.unwrap();assert_eq!(count,0);cleanup(scanner,pool,paths).await;
}
