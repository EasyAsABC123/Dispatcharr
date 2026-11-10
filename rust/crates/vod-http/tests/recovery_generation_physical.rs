//! Actual held/lost admission replies for the app-unwired transition component.
//! Original Dispatcharr and redis-rs authors retain application/client credit.
#[path = "recovery_generation.rs"]
mod functional;
use functional::{
    prototype::{Error, Recovery},
    Fixture,
};
use redis::{aio::ConnectionManagerConfig, AsyncCommands};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
fn end_frame(data: &[u8], start: usize, depth: usize) -> std::io::Result<Option<usize>> {
    if depth > 16 {
        return Err(std::io::ErrorKind::InvalidData.into());
    }
    let Some(&tag) = data.get(start) else {
        return Ok(None);
    };
    let Some(line) = data[start..]
        .windows(2)
        .position(|v| v == b"\r\n")
        .map(|v| v + start)
    else {
        return Ok(None);
    };
    let next = line + 2;
    if matches!(tag, b'+' | b'-' | b':' | b'_') {
        return Ok(Some(next));
    }
    let value = std::str::from_utf8(&data[start + 1..line])
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .ok_or(std::io::ErrorKind::InvalidData)?;
    if value < 0 {
        return Ok(Some(next));
    }
    if value > 65536 {
        return Err(std::io::ErrorKind::InvalidData.into());
    }
    match tag {
        b'$' => {
            let end = next
                .checked_add(value as usize)
                .and_then(|n| n.checked_add(2))
                .ok_or(std::io::ErrorKind::InvalidData)?;
            Ok((end <= data.len()).then_some(end))
        }
        b'*' => {
            if value > 1024 {
                return Err(std::io::ErrorKind::InvalidData.into());
            }
            let mut end = next;
            for _ in 0..value {
                let Some(value) = end_frame(data, end, depth + 1)? else {
                    return Ok(None);
                };
                end = value;
            }
            Ok(Some(end))
        }
        _ => Err(std::io::ErrorKind::InvalidData.into()),
    }
}
async fn frame(stream: &mut TcpStream, data: &mut Vec<u8>) -> std::io::Result<Vec<u8>> {
    loop {
        if let Some(end) = end_frame(data, 0, 0)? {
            return Ok(data.drain(..end).collect());
        }
        let mut chunk = [0u8; 4096];
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        if data.len() + count > 65536 {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        data.extend_from_slice(&chunk[..count]);
    }
}
fn arguments(frame: &[u8]) -> Option<Vec<Vec<u8>>> {
    if frame.first() != Some(&b'*') {
        return None;
    }
    let end = frame.windows(2).position(|x| x == b"\r\n")?;
    let count = std::str::from_utf8(&frame[1..end])
        .ok()?
        .parse::<usize>()
        .ok()?;
    if count > 1024 {
        return None;
    }
    let mut offset = end + 2;
    let mut args = Vec::new();
    for _ in 0..count {
        if frame.get(offset) != Some(&b'$') {
            return None;
        }
        let end = offset + frame[offset..].windows(2).position(|x| x == b"\r\n")?;
        let size = std::str::from_utf8(&frame[offset + 1..end])
            .ok()?
            .parse::<usize>()
            .ok()?;
        let start = end + 2;
        let finish = start.checked_add(size)?;
        if frame.get(finish..finish + 2) != Some(b"\r\n") {
            return None;
        }
        args.push(frame[start..finish].to_vec());
        offset = finish + 2;
    }
    Some(args)
}

struct Proxy {
    url: String,
    admitted: Arc<AtomicUsize>,
    cleaned: Arc<AtomicUsize>,
    admit_release: watch::Sender<bool>,
    cleanup_release: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn proxy(id: String, drop_admit: bool, drop_cleanup: bool) -> Proxy {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let admitted = Arc::new(AtomicUsize::new(0));
    let cleaned = Arc::new(AtomicUsize::new(0));
    let admit_count = admitted.clone();
    let cleanup_count = cleaned.clone();
    let (admit_release, admit_rx) = watch::channel(false);
    let (cleanup_release, cleanup_rx) = watch::channel(false);
    let admit_once = Arc::new(AtomicBool::new(true));
    let cleanup_once = Arc::new(AtomicBool::new(true));
    let task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
             accepted=listener.accept(),if tasks.len()<4=>{
              let(mut client,_)=accepted.unwrap();let metadata=format!("vod_persistent_connection:{id}");
              let admitted=admit_count.clone();let cleaned=cleanup_count.clone();let aonce=admit_once.clone();let conce=cleanup_once.clone();
              let mut admit=admit_rx.clone();let mut cleanup=cleanup_rx.clone();
              tasks.spawn(async move {
               let mut server=TcpStream::connect("127.0.0.1:6379").await?;let mut inbound=Vec::new();let mut outbound=Vec::new();
               loop {
                let request=frame(&mut client,&mut inbound).await?;let args=arguments(&request).ok_or(std::io::ErrorKind::InvalidData)?;
                let script=args.first().is_some_and(|x|x==b"EVALSHA")&&args.get(3).is_some_and(|x|x==metadata.as_bytes());
                server.write_all(&request).await?;let response=frame(&mut server,&mut outbound).await?;
                if script&&args.get(2).is_some_and(|x|x==b"3")&&response.starts_with(b"*2\r\n$2\r\nOK\r\n") {
                 admitted.fetch_add(1,Ordering::SeqCst);
                 if aonce.swap(false,Ordering::SeqCst) {
                  if drop_admit{return Err::<(),std::io::Error>(std::io::ErrorKind::ConnectionReset.into());}
                  if admit.wait_for(|ready|*ready).await.is_err(){return Ok(());}
                 }
                }
                if script&&args.get(2).is_some_and(|x|x==b"2")&&response==b":1\r\n" {
                 cleaned.fetch_add(1,Ordering::SeqCst);
                 if conce.swap(false,Ordering::SeqCst) {
                  if drop_cleanup{return Err::<(),std::io::Error>(std::io::ErrorKind::ConnectionReset.into());}
                  if cleanup.wait_for(|ready|*ready).await.is_err(){return Ok(());}
                 }
                }
                client.write_all(&response).await?;
               }
              });
             },_=tasks.join_next(),if !tasks.is_empty()=>{},
            }
        }
    });
    Proxy {
        url: format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        admitted,
        cleaned,
        admit_release,
        cleanup_release,
        task,
    }
}
async fn wait(mut condition: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
async fn setup(drop_admit: bool, drop_cleanup: bool) -> (Fixture, Proxy) {
    let mut f = Fixture::new().await;
    f.erase_owned_generation().await;
    let proxy = proxy(f.id.clone(), drop_admit, drop_cleanup).await;
    let connection = redis::Client::open(proxy.url.clone())
        .unwrap()
        .get_connection_manager_with_config(ConnectionManagerConfig::new().set_number_of_retries(0))
        .await
        .unwrap();
    f.recovery = Recovery::new(
        f.coordinator.clone(),
        connection,
        f.namespace.clone(),
        1,
        Duration::from_millis(100),
    )
    .unwrap();
    (f, proxy)
}
#[tokio::test]
#[ignore = "actual held admission/cleanup replies and canceled observers"]
async fn physical_generation_admission_and_cleanup_retain_slot_and_caller_beyond_two_seconds() {
    for cancel in [false, true] {
        let (mut f, proxy) = setup(false, false).await;
        let caller = Arc::new(());
        let weak = Arc::downgrade(&caller);
        let retired = f
            .recovery
            .retire(f.id.clone(), f.old.clone(), f.token.clone(), caller)
            .await
            .unwrap();
        let request = f.request();
        let admission = f.admission();
        let entry = Arc::new(());
        let entry_weak = Arc::downgrade(&entry);
        let call =
            tokio::spawn(async move { retired.reopen_retained(request, admission, entry).await });
        wait(async || proxy.admitted.load(Ordering::SeqCst) == 1).await;
        if cancel {
            call.abort();
            assert!(call.await.is_err());
        } else {
            assert!(matches!(call.await.unwrap(), Err(Error::ObserverTimeout)));
        }
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert_eq!(f.recovery.available_slots(), 0);
        assert!(weak.upgrade().is_some());
        assert!(entry_weak.upgrade().is_some());
        assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
        assert_eq!(
            f.redis
                .hget::<_, _, i64>(f.metadata(), "active_streams")
                .await
                .unwrap(),
            1
        );
        assert!(matches!(f.retire().await, Err(Error::Capacity)));
        proxy.admit_release.send_replace(true);
        wait(async || proxy.cleaned.load(Ordering::SeqCst) == 1).await;
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert!(!f.redis.exists::<_, bool>(f.metadata()).await.unwrap());
        assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
        assert_eq!(f.recovery.available_slots(), 0);
        assert!(weak.upgrade().is_some());
        assert!(entry_weak.upgrade().is_some());
        proxy.cleanup_release.send_replace(true);
        wait(async || {
            weak.upgrade().is_none()
                && entry_weak.upgrade().is_none()
                && f.recovery.available_slots() == 1
        })
        .await;
        assert_eq!(proxy.admitted.load(Ordering::SeqCst), 1);
        assert_eq!(proxy.cleaned.load(Ordering::SeqCst), 1);
        f.empty().await;
    }
}
#[tokio::test]
#[ignore = "actual lost admission/cleanup replies without mutation replay"]
async fn physical_generation_lost_reply_is_not_replayed_and_cleanup_diagnostic_is_truthful() {
    for drop_admit in [true, false] {
        let (mut f, proxy) = setup(drop_admit, !drop_admit).await;
        let caller = Arc::new(());
        let weak = Arc::downgrade(&caller);
        let retired = f
            .recovery
            .retire(f.id.clone(), f.old.clone(), f.token.clone(), caller)
            .await
            .unwrap();
        let request = f.request();
        let admission = f.admission();
        let entry = Arc::new(());
        let entry_weak = Arc::downgrade(&entry);
        let call =
            tokio::spawn(async move { retired.reopen_retained(request, admission, entry).await });
        wait(async || proxy.admitted.load(Ordering::SeqCst) == 1).await;
        if !drop_admit {
            assert!(matches!(call.await.unwrap(), Err(Error::ObserverTimeout)));
            proxy.admit_release.send_replace(true);
        } else {
            proxy.cleanup_release.send_replace(true);
            assert!(matches!(
                call.await.unwrap(),
                Err(Error::Admission(_)) | Err(Error::ObserverTimeout)
            ));
        }
        wait(async || {
            weak.upgrade().is_none()
                && entry_weak.upgrade().is_none()
                && f.recovery.available_slots() == 1
        })
        .await;
        assert_eq!(proxy.admitted.load(Ordering::SeqCst), 1);
        assert_eq!(proxy.cleaned.load(Ordering::SeqCst), 1);
        assert!(!f.redis.exists::<_, bool>(f.metadata()).await.unwrap());
        assert!(!f.redis.exists::<_, bool>(f.owner()).await.unwrap());
        assert_eq!(
            f.redis
                .get::<_, Option<i64>>(f.counter())
                .await
                .unwrap()
                .unwrap_or(0),
            0
        );
        assert_eq!(f.recovery.cleanup_succeeded(), drop_admit);
    }
}
