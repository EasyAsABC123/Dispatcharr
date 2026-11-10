//! Actual held/lost replies for ordinary coordinator lease operations.
//! Original Dispatcharr and redis-rs authors retain algorithm/client credit.
use dispatcharr_coordination::{
    AcquireRequest, Coordinator, CredentialPool, LeaseConfig, ProviderReservation,
};
use redis::AsyncCommands;
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
#[allow(dead_code)]
mod source {
    include!("../src/scripts.rs");
}
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

#[derive(Clone, Copy, Debug)]
enum Operation {
    Heartbeat,
    Clear,
    Reserve,
    Release,
    DropRelease,
}
impl Operation {
    fn sha(self) -> String {
        let body = match self {
            Self::Heartbeat => source::HEARTBEAT,
            Self::Clear => source::CLEAR_PROVIDER,
            Self::Reserve => source::RESERVE_PROVIDER,
            Self::Release | Self::DropRelease => source::RELEASE,
        };
        redis::Script::new(&format!("{}{}", source::REAP, body))
            .get_hash()
            .to_owned()
    }
}
struct Proxy {
    url: String,
    effects: Arc<AtomicUsize>,
    release: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn proxy(operation: Operation, drop_reply: bool) -> Proxy {
    assert_eq!(
        std::env::var("REDIS_RESTART_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    assert_eq!(
        std::env::var("REDIS_TEST_URL").unwrap(),
        "redis://:playback-fixture-only@127.0.0.1:6379/0"
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let effects = Arc::new(AtomicUsize::new(0));
    let written = effects.clone();
    let (held, receiver) = watch::channel(false);
    let once = Arc::new(AtomicBool::new(true));
    let sha = operation.sha();
    let task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {accepted=listener.accept(),if tasks.len()<4=>{let(mut client,_)=accepted.unwrap();let sha=sha.clone();let once=once.clone();let written=written.clone();let mut release=receiver.clone();tasks.spawn(async move{let mut server=TcpStream::connect("127.0.0.1:6379").await?;let mut incoming=Vec::new();let mut outgoing=Vec::new();loop{let request=frame(&mut client,&mut incoming).await?;let args=arguments(&request).ok_or(std::io::ErrorKind::InvalidData)?;let selected=args.first().is_some_and(|v|v==b"EVALSHA")&&args.get(1).is_some_and(|v|v==sha.as_bytes());server.write_all(&request).await?;let response=frame(&mut server,&mut outgoing).await?;
            if selected&&response==b":1\r\n"{written.fetch_add(1,Ordering::SeqCst);if once.swap(false,Ordering::SeqCst){if drop_reply{return Err::<(),std::io::Error>(std::io::ErrorKind::ConnectionReset.into());}
            if release.wait_for(|v|*v).await.is_err(){return Ok(());}}}
            client.write_all(&response).await?;
            }});},_=tasks.join_next(),if !tasks.is_empty()=>{}}
        }
    });
    Proxy {
        url: format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        effects,
        release: held,
        task,
    }
}
async fn wait(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
async fn run(operation: Operation, drop_reply: bool, cancel: bool) {
    let proxy = proxy(operation, drop_reply).await;
    let namespace = format!("lease-physical-{}", uuid::Uuid::new_v4().simple());
    let profile = 3_001_000
        + (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos() as i64);
    let credential = format!(
        "server_group_connections:lease-physical:{}",
        uuid::Uuid::new_v4().simple()
    );
    let coordinator = Coordinator::connect(
        &proxy.url,
        LeaseConfig {
            namespace: namespace.clone(),
            command_timeout: Duration::from_millis(100),
            max_inflight_commands: 1,
            heartbeat_interval: None,
            reap_interval: None,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut control = redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let provider = || ProviderReservation {
        profile_id: Some(profile),
        max_streams: 1,
        credential_pool: Some(CredentialPool {
            counter_key: credential.clone(),
            max_streams: 1,
        }),
    };
    let reserve = matches!(operation, Operation::Reserve);
    let lease = coordinator
        .acquire_vod(AcquireRequest {
            channel_id: namespace.clone(),
            provider_profile_id: if reserve { None } else { Some(profile) },
            provider_max_streams: if reserve { 0 } else { 1 },
            credential_pool: if reserve {
                None
            } else {
                Some(CredentialPool {
                    counter_key: credential.clone(),
                    max_streams: 1,
                })
            },
        })
        .await
        .unwrap();
    let ledger = format!("{namespace}:lease:{}", lease.token());
    let owner = format!("{namespace}:vod:session:{namespace}:owner");
    let lease = Arc::new(lease);
    let settlement = lease.settlement();
    let mut retained = Some(lease.clone());
    let outcome = if cancel {
        let current = lease.clone();
        let reservation = provider();
        let task = tokio::spawn(async move {
            match operation {
                Operation::Heartbeat => format!("{:?}", current.heartbeat().await),
                Operation::Clear => format!("{:?}", current.release_provider().await),
                Operation::Reserve => format!("{:?}", current.reserve_provider(reservation).await),
                Operation::Release => format!("{:?}", current.release().await),
                Operation::DropRelease => unreachable!(),
            }
        });
        wait(|| proxy.effects.load(Ordering::SeqCst) == 1).await;
        task.abort();
        let _ = task.await;
        "cancelled".to_owned()
    } else {
        match operation {
            Operation::Heartbeat => format!("{:?}", lease.heartbeat().await),
            Operation::Clear => format!("{:?}", lease.release_provider().await),
            Operation::Reserve => format!("{:?}", lease.reserve_provider(provider()).await),
            Operation::Release => format!("{:?}", lease.release().await),
            Operation::DropRelease => {
                drop(retained.take());
                drop(lease);
                wait(|| proxy.effects.load(Ordering::SeqCst) == 1).await;
                tokio::time::sleep(Duration::from_millis(180)).await;
                "drop".to_owned()
            }
        }
    };
    wait(|| proxy.effects.load(Ordering::SeqCst) == 1).await;
    let early_idle = coordinator.is_idle();
    if !drop_reply {
        assert!(
            !settlement.is_settled(),
            "lease receipt must retain physical work"
        );
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert!(
            !coordinator.is_idle(),
            "physical budget must survive more than2s"
        );
        assert!(!settlement.is_settled());
        assert!(
            coordinator.reap_expired().await.is_err(),
            "held command must retain its sole admission slot"
        );
    }
    let actual_profile: Option<u64> = control
        .get(format!("profile_connections:{profile}"))
        .await
        .unwrap();
    let actual_credential: Option<u64> = control.get(&credential).await.unwrap();
    let expected = if matches!(operation, Operation::Heartbeat | Operation::Reserve) {
        1
    } else {
        0
    };
    assert_eq!(actual_profile.unwrap_or(0), expected);
    assert_eq!(actual_credential.unwrap_or(0), expected);
    println!("PHYSICAL_LEASE operation={operation:?} dropped={drop_reply} early_idle={early_idle} effects={} outcome={outcome}",proxy.effects.load(Ordering::SeqCst));
    proxy.release.send_replace(true);
    tokio::time::timeout(Duration::from_secs(3), settlement.wait())
        .await
        .unwrap();
    // These are future safe operations, not a replay of the selected mutation.
    for _ in 0..5 {
        if coordinator.reap_expired().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if let Some(lease) = retained.take() {
        let _ = lease.release().await;
    }
    wait(|| coordinator.is_idle()).await;
    let actual_profile: Option<u64> = control
        .get(format!("profile_connections:{profile}"))
        .await
        .unwrap();
    let actual_credential: Option<u64> = control.get(&credential).await.unwrap();
    assert_eq!(actual_profile.unwrap_or(0), 0);
    assert_eq!(actual_credential.unwrap_or(0), 0);
    let exists: bool = control.exists(&owner).await.unwrap();
    assert!(!exists);
    let exists: bool = control.exists(&ledger).await.unwrap();
    assert!(!exists);
    assert_eq!(
        proxy.effects.load(Ordering::SeqCst),
        1,
        "a successful uncertain mutation must never be replayed"
    );
    if !drop_reply {
        assert!(
            outcome.contains("RedisUnavailable")
                || cancel
                || matches!(operation, Operation::DropRelease)
        );
        assert!(
            !early_idle,
            "logical observer timeout must not free the pending physical command slot"
        );
    }
}
macro_rules! case {
    ($name:ident,$operation:ident,$drop:expr) => {
        #[tokio::test]
        #[ignore = "actual owned Redis held/lost reply physical command budget"]
        async fn $name() {
            run(Operation::$operation, $drop, false).await;
        }
    };
}
case!(held_heartbeat_retains_command_budget, Heartbeat, false);
case!(held_clear_provider_retains_command_budget, Clear, false);
case!(held_reserve_provider_retains_command_budget, Reserve, false);
case!(held_explicit_release_retains_command_budget, Release, false);
case!(held_drop_release_retains_command_budget, DropRelease, false);
case!(lost_heartbeat_is_not_replayed, Heartbeat, true);
case!(lost_clear_provider_is_not_replayed, Clear, true);
case!(lost_reserve_provider_is_not_replayed, Reserve, true);
case!(lost_explicit_release_is_not_replayed, Release, true);

macro_rules! cancelled_case {
    ($name:ident,$operation:ident) => {
        #[tokio::test]
        #[ignore = "actual owned Redis canceled observer physical command budget"]
        async fn $name() {
            run(Operation::$operation, false, true).await;
        }
    };
}
cancelled_case!(cancelled_heartbeat_retains_command_budget, Heartbeat);
cancelled_case!(cancelled_clear_provider_retains_command_budget, Clear);
cancelled_case!(cancelled_reserve_provider_retains_command_budget, Reserve);
cancelled_case!(cancelled_release_retains_command_budget, Release);
