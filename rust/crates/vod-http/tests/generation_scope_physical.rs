//! Actual LocalScope held replies/caller cancellation; no application acceptance.
//! Original Dispatcharr and redis-rs authors retain application/client credit.
#[path = "generation_scope.rs"]
mod scoped;
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
    effects: Arc<AtomicUsize>,
    release: watch::Sender<bool>,
    kill: watch::Sender<bool>,
    cleanup_effects: Arc<AtomicUsize>,
    hold_cleanup: Arc<AtomicBool>,
    cleanup_release: watch::Sender<bool>,
    forget_effects: Arc<AtomicUsize>,
    hold_forget: Arc<AtomicBool>,
    forget_release: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn proxy() -> Proxy {
    proxy_for(scoped::functional::prototype::local::MUTATE, false, false).await
}
async fn proxy_for(script: &str, entry: bool, lose: bool) -> Proxy {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let effects = Arc::new(AtomicUsize::new(0));
    let cleanup_effects = Arc::new(AtomicUsize::new(0));
    let forget_effects = Arc::new(AtomicUsize::new(0));
    let hold_cleanup = Arc::new(AtomicBool::new(false));
    let hold_forget = Arc::new(AtomicBool::new(false));
    let (release, receiver) = watch::channel(false);
    let (kill, killed) = watch::channel(false);
    let (cleanup_release, cleanup_ready) = watch::channel(false);
    let (forget_release, forget_ready) = watch::channel(false);
    let written = effects.clone();
    let cleaned = cleanup_effects.clone();
    let forgotten = forget_effects.clone();
    let wait_cleanup = hold_cleanup.clone();
    let wait_forget = hold_forget.clone();
    let scripts = Arc::new(
        [
            script.to_owned(),
            scoped::functional::prototype::local::MUTATE.into(),
            scoped::functional::prototype::local::receipt::FORGET.into(),
        ]
        .map(|body: String| (redis::Script::new(&body).get_hash().to_owned(), body)),
    );
    let task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept(),if tasks.len()<4=>{
                    let (mut client,_) = accepted.unwrap();
                    let scripts = scripts.clone();let written=written.clone();let cleaned=cleaned.clone();let forgotten=forgotten.clone();
                    let wait_cleanup=wait_cleanup.clone();let wait_forget=wait_forget.clone();
                    let mut release=receiver.clone();let mut killed=killed.clone();let mut cleanup_ready=cleanup_ready.clone();let mut forget_ready=forget_ready.clone();
                    tasks.spawn(async move {
                        let mut server=TcpStream::connect("127.0.0.1:6379").await?;
                        let mut incoming=Vec::new();let mut outgoing=Vec::new();
                        loop {
                            let request=frame(&mut client,&mut incoming).await?;
                            let args=arguments(&request).ok_or(std::io::ErrorKind::InvalidData)?;
                            let uses_script = |index:usize| (args.first().is_some_and(|x|x==b"EVALSHA")&&args.get(1).is_some_and(|x|x==scripts[index].0.as_bytes())) || (args.first().is_some_and(|x|x==b"EVAL")&&args.get(1).is_some_and(|x|x==scripts[index].1.as_bytes()));
                            server.write_all(&request).await?;
                            let response=frame(&mut server,&mut outgoing).await?;
                            if uses_script(0)&&response.starts_with(if entry {b"*1\r\n$7\r\nENTERED\r\n"}else{b"*1\r\n$7\r\nUPDATED\r\n"}) {
                                written.fetch_add(1,Ordering::SeqCst);
                                if lose { return Ok::<(),std::io::Error>(()); }
                                tokio::select! {
                                    ready=release.wait_for(|value|*value)=>{if ready.is_err(){return Ok(());}},
                                    _=killed.wait_for(|value|*value)=>return Ok(()),
                                }
                            }
                            if uses_script(1)&&args.iter().any(|x|x==b"abandon")&&response.starts_with(b"*2\r\n$4\r\nLEFT\r\n") {
                                cleaned.fetch_add(1,Ordering::SeqCst);
                                if wait_cleanup.load(Ordering::SeqCst)&&cleanup_ready.wait_for(|value|*value).await.is_err(){return Ok(());}
                            }
                            if uses_script(2)&&response.starts_with(b":") {
                                forgotten.fetch_add(1,Ordering::SeqCst);
                                if wait_forget.load(Ordering::SeqCst)&&forget_ready.wait_for(|value|*value).await.is_err(){return Ok(());}
                            }
                            client.write_all(&response).await?;
                        }
                    });
                },
                _=tasks.join_next(),if !tasks.is_empty()=>{},
            }
        }
    });
    Proxy {
        url: format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        effects,
        release,
        kill,
        cleanup_effects,
        hold_cleanup,
        cleanup_release,
        forget_effects,
        hold_forget,
        forget_release,
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
#[tokio::test]
#[ignore = "actual held progress reply and shutdown drainage of true body caller scope"]
async fn local_physical_progress_timeout_keeps_body_and_control_through_queued_close() {
    let proxy = proxy().await;
    let (mut f, scope, stop) = scoped::initial_with_connection(Some(&proxy.url)).await;
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let body = scope.attach_reserved(caller, true, None).await.unwrap();
    body.started();
    assert!(scope.progress(&body, 819200).await.is_err());
    wait(async || proxy.effects.load(Ordering::SeqCst) == 1).await;
    assert_eq!(scope.control_slots(), 1);
    assert!(weak.upgrade().is_some());
    // Close is owned and queued behind the physically pending progress command.
    assert!(scope.close(&body).await.is_err());
    scope.stop();
    assert!(*stop.borrow());
    assert!(
        tokio::time::timeout(Duration::from_millis(60), scope.drain())
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert_eq!(scope.body_slots(), 1);
    assert!(weak.upgrade().is_some());
    assert_eq!(
        f.redis
            .hget::<_, _, u64>(f.metadata(), "bytes_sent")
            .await
            .unwrap(),
        819200
    );
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    proxy.release.send_replace(true);
    tokio::time::timeout(Duration::from_secs(5), scope.drain())
        .await
        .unwrap();
    assert!(weak.upgrade().is_none());
    assert_eq!(scope.body_slots(), 2);
    assert_eq!(scope.control_slots(), 2);
    assert_eq!(proxy.effects.load(Ordering::SeqCst), 1);
    assert!(scope.cleanup_succeeded());
    f.empty().await;
}
#[tokio::test]
#[ignore = "actual caller cancellation of held progress cannot detach physical body ownership"]
async fn local_physical_cancelled_progress_and_body_drop_keep_owned_cleanup() {
    let proxy = proxy().await;
    let (mut f, scope, _) = scoped::initial_with_connection(Some(&proxy.url)).await;
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let body = scope.attach_reserved(caller, true, None).await.unwrap();
    body.started();
    let worker_scope = scope.clone();
    let worker_body = body.clone();
    let call = tokio::spawn(async move { worker_scope.progress(&worker_body, 819200).await });
    wait(async || proxy.effects.load(Ordering::SeqCst) == 1).await;
    call.abort();
    let _ = call.await;
    drop(body);
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert_eq!(scope.body_slots(), 1);
    assert!(weak.upgrade().is_some());
    proxy.release.send_replace(true);
    tokio::time::timeout(Duration::from_secs(5), scope.drain())
        .await
        .unwrap();
    assert!(weak.upgrade().is_none());
    assert_eq!(proxy.effects.load(Ordering::SeqCst), 1);
    assert!(scope.cleanup_succeeded());
    f.empty().await;
}

fn entry_admission(f: &scoped::functional::Fixture) -> scoped::functional::prototype::NewAdmission {
    let mut value = f.admission();
    value
        .fields
        .insert("content_obj_type".into(), "movie".into());
    value
        .fields
        .insert("m3u_profile_id".into(), f.profile.to_string());
    value
}
#[tokio::test]
#[ignore = "actual held current ENTER keeps caller/control and compensates explicit pending close"]
async fn local_physical_current_enter_held_reply_pending_close_is_exactly_once() {
    let proxy = proxy_for(
        scoped::functional::prototype::local::ENTER_CURRENT,
        true,
        false,
    )
    .await;
    let (mut f, scope, _) = scoped::initial_with_connection(Some(&proxy.url)).await;
    let original = scope
        .attach_reserved(Arc::new(()), true, None)
        .await
        .unwrap();
    original.started();
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let body = scope.attach_entry_pending(caller, None).await.unwrap();
    assert!(scope
        .enter_current(&body, entry_admission(&f))
        .await
        .is_err());
    wait(async || proxy.effects.load(Ordering::SeqCst) == 1).await;
    assert!(weak.upgrade().is_some());
    assert_eq!(
        f.redis
            .hget::<_, _, i64>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        2
    );
    assert!(!scope.close(&body).await.unwrap().changed);
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert!(weak.upgrade().is_some());
    assert_eq!(scope.control_slots(), 1);
    proxy.release.send_replace(true);
    wait(async || scope.control_slots() == 2 && weak.upgrade().is_none()).await;
    assert_eq!(
        f.redis
            .hget::<_, _, i64>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        1
    );
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    assert_eq!(proxy.effects.load(Ordering::SeqCst), 1);
    scope.stop();
    scope
        .close(&original)
        .await
        .unwrap_or_else(|_| panic!("initial close"));
    scope.drain().await;
    f.empty().await;
}
#[tokio::test]
#[ignore = "actual successful current ENTER reply loss must not leak an abandoned public increment"]
async fn local_physical_current_enter_lost_reply_removes_only_its_increment() {
    let proxy = proxy_for(
        scoped::functional::prototype::local::ENTER_CURRENT,
        true,
        true,
    )
    .await;
    let (mut f, scope, _) = scoped::initial_with_connection(Some(&proxy.url)).await;
    let original = scope
        .attach_reserved(Arc::new(()), true, None)
        .await
        .unwrap();
    original.started();
    let body = scope
        .attach_entry_pending(Arc::new(()), None)
        .await
        .unwrap();
    assert!(scope
        .enter_current(&body, entry_admission(&f))
        .await
        .is_err());
    wait(async || proxy.effects.load(Ordering::SeqCst) == 1).await;
    drop(body);
    wait(async || scope.body_slots() == 1 && scope.control_slots() == 2).await;
    assert_eq!(
        proxy.effects.load(Ordering::SeqCst),
        1,
        "never replay uncertain ENTER"
    );
    assert_eq!(
        f.redis
            .hget::<_, _, i64>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        1,
        "abandoned new entry must leave initial sibling accounting untouched"
    );
    scope.stop();
    scope
        .close(&original)
        .await
        .unwrap_or_else(|_| panic!("initial close"));
    scope.drain().await;
    f.empty().await;
}

async fn receipt_key(f: &mut scoped::functional::Fixture) -> String {
    let keys: Vec<String> = f
        .redis
        .keys(format!("{}:vod:entries:{}:*", f.namespace, f.id))
        .await
        .unwrap();
    assert_eq!(keys.len(), 1);
    keys[0].clone()
}
#[tokio::test]
#[ignore = "aborted observer retains true body/control through cleanup and independent private forget replies"]
async fn local_physical_current_enter_cleanup_and_forget_replies_keep_caller_owned() {
    let proxy = proxy_for(
        scoped::functional::prototype::local::ENTER_CURRENT,
        true,
        false,
    )
    .await;
    proxy.hold_cleanup.store(true, Ordering::SeqCst);
    proxy.hold_forget.store(true, Ordering::SeqCst);
    let (mut f, scope, _) = scoped::initial_with_connection(Some(&proxy.url)).await;
    let original = scope
        .attach_reserved(Arc::new(()), true, None)
        .await
        .unwrap();
    original.started();
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let body = scope.attach_entry_pending(caller, None).await.unwrap();
    let worker_scope = scope.clone();
    let worker_body = body.clone();
    let admission = entry_admission(&f);
    let call =
        tokio::spawn(async move { worker_scope.enter_current(&worker_body, admission).await });
    wait(async || proxy.effects.load(Ordering::SeqCst) == 1).await;
    call.abort();
    let _ = call.await;
    drop(body);
    proxy.kill.send_replace(true);
    wait(async || proxy.cleanup_effects.load(Ordering::SeqCst) == 1).await;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert!(weak.upgrade().is_some());
    assert_eq!(scope.control_slots(), 1);
    assert_eq!(scope.body_slots(), 0);
    assert_eq!(
        f.redis
            .hget::<_, _, i64>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        1
    );
    proxy.cleanup_release.send_replace(true);
    wait(async || proxy.forget_effects.load(Ordering::SeqCst) == 1).await;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert!(weak.upgrade().is_some());
    assert_eq!(scope.control_slots(), 1);
    assert_eq!(scope.body_slots(), 0);
    proxy.forget_release.send_replace(true);
    wait(async || {
        weak.upgrade().is_none() && scope.body_slots() == 1 && scope.control_slots() == 2
    })
    .await;
    assert_eq!(proxy.effects.load(Ordering::SeqCst), 1);
    assert_eq!(proxy.cleanup_effects.load(Ordering::SeqCst), 1);
    assert_eq!(proxy.forget_effects.load(Ordering::SeqCst), 1);
    assert!(scope.cleanup_succeeded());
    scope.stop();
    let _ = scope.close(&original).await;
    scope.drain().await;
    f.empty().await;
}
#[tokio::test]
#[ignore = "missing/wrong-entry/wrong-generation receipts never authorize a blind public decrement"]
async fn local_physical_current_enter_missing_or_foreign_receipt_is_unproven() {
    for variant in ["missing", "different-entry", "different-generation"] {
        let proxy = proxy_for(
            scoped::functional::prototype::local::ENTER_CURRENT,
            true,
            false,
        )
        .await;
        let (mut f, scope, _) = scoped::initial_with_connection(Some(&proxy.url)).await;
        let original = scope
            .attach_reserved(Arc::new(()), true, None)
            .await
            .unwrap();
        original.started();
        let body = scope
            .attach_entry_pending(Arc::new(()), None)
            .await
            .unwrap();
        assert!(scope
            .enter_current(&body, entry_admission(&f))
            .await
            .is_err());
        wait(async || proxy.effects.load(Ordering::SeqCst) == 1).await;
        let key = receipt_key(&mut f).await;
        let values: std::collections::BTreeMap<String, String> =
            f.redis.hgetall(&key).await.unwrap();
        assert_eq!(values.len(), 1);
        let (field, value) = values.first_key_value().unwrap();
        let _: usize = f.redis.del(&key).await.unwrap();
        if variant == "different-entry" {
            let _: usize = f.redis.hset(&key, "another-entry", value).await.unwrap();
        }
        if variant == "different-generation" {
            let _: usize = f
                .redis
                .hset(&key, field, "different-generation")
                .await
                .unwrap();
        }
        drop(body);
        proxy.kill.send_replace(true);
        wait(async || scope.body_slots() == 1 && scope.control_slots() == 2).await;
        assert_eq!(
            f.redis
                .hget::<_, _, i64>(f.metadata(), "active_streams")
                .await
                .unwrap(),
            2,
            "no receipt proof means no blind decrement: {variant}"
        );
        assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
        assert_eq!(proxy.effects.load(Ordering::SeqCst), 1);
        assert!(
            !scope.cleanup_succeeded(),
            "unproven cleanup must remain diagnostic"
        );
        if variant != "missing" {
            assert!(
                f.redis.exists::<_, bool>(&key).await.unwrap(),
                "unrelated receipt value must survive"
            );
        }
        scope.stop();
        let _ = scope.close(&original).await;
        scope.drain().await;
        // Explicit owned-corruption teardown, not a service cleanup success.
        f.old.release().await.unwrap();
        f.old.settlement().wait().await;
        let _: usize = f.redis.del(&[f.metadata(), key]).await.unwrap();
        f.empty().await;
    }
}
#[tokio::test]
#[ignore = "stop and foreign replacement preserve fences while own old private receipt is independently forgotten"]
async fn local_physical_current_enter_stop_and_foreign_owner_fences() {
    for foreign in [false, true] {
        let proxy = proxy_for(
            scoped::functional::prototype::local::ENTER_CURRENT,
            true,
            false,
        )
        .await;
        let (mut f, scope, _) = scoped::initial_with_connection(Some(&proxy.url)).await;
        let original = scope
            .attach_reserved(Arc::new(()), true, None)
            .await
            .unwrap();
        original.started();
        let body = scope
            .attach_entry_pending(Arc::new(()), None)
            .await
            .unwrap();
        assert!(scope
            .enter_current(&body, entry_admission(&f))
            .await
            .is_err());
        wait(async || proxy.effects.load(Ordering::SeqCst) == 1).await;
        let key = receipt_key(&mut f).await;
        let replacement = if foreign {
            f.old.release().await.unwrap();
            f.old.settlement().wait().await;
            let lease = f.coordinator.acquire_vod(f.request()).await.unwrap();
            let _: () = f
                .redis
                .hset_multiple(
                    f.metadata(),
                    &[
                        ("rust_owner_token", "foreign-owned"),
                        ("active_streams", "7"),
                    ],
                )
                .await
                .unwrap();
            Some(lease)
        } else {
            let _: () = f.redis.set(f.stop(), "fixture-stop").await.unwrap();
            None
        };
        drop(body);
        proxy.kill.send_replace(true);
        wait(async || scope.body_slots() == 1 && scope.control_slots() == 2).await;
        assert!(
            !f.redis.exists::<_, bool>(&key).await.unwrap(),
            "own old receipt may be forgotten without current public ownership"
        );
        assert_eq!(proxy.effects.load(Ordering::SeqCst), 1);
        assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
        if let Some(lease) = replacement {
            assert_eq!(
                f.redis
                    .hget::<_, _, i64>(f.metadata(), "active_streams")
                    .await
                    .unwrap(),
                7
            );
            assert_eq!(
                f.redis.get::<_, String>(f.owner()).await.unwrap(),
                lease.token()
            );
            scope.stop();
            let _ = scope.close(&original).await;
            scope.drain().await;
            assert_eq!(
                f.redis
                    .hget::<_, _, i64>(f.metadata(), "active_streams")
                    .await
                    .unwrap(),
                7
            );
            lease.release().await.unwrap();
            lease.settlement().wait().await;
            let _: usize = f.redis.del(f.metadata()).await.unwrap();
        } else {
            assert_eq!(
                f.redis
                    .hget::<_, _, i64>(f.metadata(), "active_streams")
                    .await
                    .unwrap(),
                1
            );
            assert!(f.redis.exists::<_, bool>(f.stop()).await.unwrap());
            scope.stop();
            let _ = scope.close(&original).await;
            scope.drain().await;
            assert!(f.redis.exists::<_, bool>(f.stop()).await.unwrap());
        }
        f.empty().await;
    }
}

#[tokio::test]
#[ignore = "actual paused Redis reply retains fresh-entry caller after observer cancellation"]
async fn local_physical_fresh_entry_classification_keeps_request_through_actual_reply() {
    let (mut f, scope, _) = scoped::initial().await;
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(4000)
        .arg("ALL")
        .query_async(&mut f.redis)
        .await
        .unwrap();
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let worker_scope = scope.clone();
    let task = tokio::spawn(async move { worker_scope.retire_missing_retained(caller).await });
    wait(async || scope.control_slots() == 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert!(
        weak.upgrade().is_some(),
        "request released before physical reply"
    );
    assert_eq!(scope.control_slots(), 1);
    tokio::time::timeout(Duration::from_secs(5), scope.drain())
        .await
        .unwrap();
    assert!(weak.upgrade().is_none());
    assert_eq!(scope.control_slots(), 2);
    assert!(!scope.activated());
    assert!(scope.cleanup_succeeded());
    // The initial fixture admitted a real public body; retire it explicitly.
    let original = scope
        .attach_reserved(Arc::new(()), true, None)
        .await
        .unwrap();
    scope.close(&original).await.unwrap();
    scope.drain().await;
    f.empty().await;
}
