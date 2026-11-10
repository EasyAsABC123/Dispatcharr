//! Fixture-only before-RELEASE intervention after all domain work has settled.
//! Original Dispatcharr authors retain application/algorithm credit and AGPL.
use dispatcharr_store::Store;
use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
const RELEASE: &str =
    "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end";
pub(super) struct Proxy {
    pub url: String,
    pub open: watch::Sender<bool>,
    pub blocked: watch::Receiver<bool>,
    pub intervened: Arc<AtomicBool>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn line(data: &[u8], offset: usize) -> io::Result<Option<(usize, usize)>> {
    let Some(end) = data
        .get(offset..)
        .and_then(|s| s.windows(2).position(|x| x == b"\r\n"))
        .map(|n| n + offset)
    else {
        return Ok(None);
    };
    let value = std::str::from_utf8(&data[offset..end])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or(io::ErrorKind::InvalidData)?;
    Ok(Some((value, end + 2)))
}
type Command<'a> = Option<(usize, Vec<&'a [u8]>)>;
fn command(data: &[u8]) -> io::Result<Command<'_>> {
    if data.is_empty() {
        return Ok(None);
    }
    if data[0] != b'*' {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let Some((count, mut offset)) = line(data, 1)? else {
        return Ok(None);
    };
    if count > 128 {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let Some(tag) = data.get(offset) else {
            return Ok(None);
        };
        if *tag != b'$' {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let Some((size, start)) = line(data, offset + 1)? else {
            return Ok(None);
        };
        if size > 65536 {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let end = start
            .checked_add(size)
            .and_then(|n| n.checked_add(2))
            .ok_or(io::ErrorKind::InvalidData)?;
        if end > data.len() {
            return Ok(None);
        }
        if &data[end - 2..end] != b"\r\n" {
            return Err(io::ErrorKind::InvalidData.into());
        }
        args.push(&data[start..end - 2]);
        offset = end;
    }
    Ok(Some((offset, args)))
}
pub(super) async fn proxy(store: Store, source: i64, mode: &str, hold_reply: bool) -> Proxy {
    assert!(source > 0 && matches!(mode, "replace" | "absent"));
    let mode = mode.to_owned();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (open, receiver) = watch::channel(!hold_reply);
    let (blocked, observed) = watch::channel(false);
    let intervened = Arc::new(AtomicBool::new(false));
    let flag = intervened.clone();
    let task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
            accepted=listener.accept(),if tasks.len()<4=>{
                let(client,_)=accepted.unwrap();let store=store.clone();let mode=mode.clone();let flag=flag.clone();let mut open=receiver.clone();let blocked=blocked.clone();
                tasks.spawn(async move{
                    let server=TcpStream::connect("127.0.0.1:6379").await?;let(mut cr,mut cw)=client.into_split();let(mut sr,mut sw)=server.into_split();
                    let inbound_flag=flag.clone();let inbound=async move{let mut buffer=[0u8;16384];loop{
                        let count=sr.read(&mut buffer).await?;if count==0{return Ok::<(),io::Error>(())}
                        if inbound_flag.load(Ordering::Acquire){blocked.send_replace(true);if open.wait_for(|x|*x).await.is_err(){return Ok(())}}
                        cw.write_all(&buffer[..count]).await?;
                    }};
                    let outbound=async move{
                        let sha=redis::Script::new(RELEASE).get_hash().to_owned();let key=format!("task_lock_refresh_epg_data_{source}");let mut data=Vec::with_capacity(65536);let mut buffer=[0u8;8192];
                        loop{let count=cr.read(&mut buffer).await?;if count==0{return Ok::<(),io::Error>(())}
                        if data.len()+count>65536{return Err(io::ErrorKind::InvalidData.into())}
                        data.extend_from_slice(&buffer[..count]);
                            while let Some((end,args))=command(&data)?{
                                let target=args.len()>=5&&args[0].eq_ignore_ascii_case(b"EVALSHA")&&args[1]==sha.as_bytes()&&args[2]==b"1"&&args[3]==key.as_bytes();
                                if target{
                                    if flag.compare_exchange(false,true,Ordering::AcqRel,Ordering::Acquire).is_err(){return Err(io::ErrorKind::InvalidData.into())}
                                    let mut cache=redis::Client::open("redis://:playback-fixture-only@127.0.0.1:6379/0").unwrap().get_multiplexed_async_connection().await.unwrap();
                                    if mode=="replace"{let _:()=redis::cmd("SET").arg(&key).arg("owned-foreign-token").arg("EX").arg(300).query_async(&mut cache).await.unwrap();}
                                    else{let _:i64=redis::cmd("DEL").arg(&key).query_async(&mut cache).await.unwrap();}
                                    let table=format!("rust_native_race_{source}");sqlx::query(&format!("INSERT INTO {table}(kind,affected)VALUES('intervention',NULL)")).execute(store.pool()).await.unwrap();
                                }
                                sw.write_all(&data[..end]).await?;data.drain(..end);
                            }
                        }
                    };
                    tokio::try_join!(inbound,outbound).map(|_|())
                });
            },_=tasks.join_next(),if !tasks.is_empty()=>{}}
        }
    });
    Proxy {
        url: format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        open,
        blocked: observed,
        intervened,
        task,
    }
}
#[test]
fn release_command_parser_is_bounded() {
    let data = b"*2\r\n$4\r\nPING\r\n$1\r\nx\r\n";
    let (end, args) = command(data).unwrap().unwrap();
    assert_eq!(end, data.len());
    assert_eq!(args, vec![b"PING".as_slice(), b"x".as_slice()]);
    assert!(command(b"*129\r\n").is_err());
    assert!(command(b"*1\r\n$65537\r\n").is_err());
    assert!(command(&data[..data.len() - 1]).unwrap().is_none());
}
