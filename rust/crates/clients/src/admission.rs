//! Supervised client admission/cleanup; original Dispatcharr policy semantics are retained.
use super::*;

pub(super) struct Registration {
    pub keys: [String; 4],
    pub data: String,
    pub policy: String,
    pub stream_limit: u32,
}

/// This job outlives a cancelled HTTP caller. Its ClientInner retains local
/// admission until REGISTER replies; an undelivered lease then schedules a
/// fenced RELEASE whose task retains the same admission through its reply.
pub(super) async fn register_job(
    store: Arc<Inner>,
    client: Arc<ClientInner>,
    request: Registration,
) -> Result<ClientLease, Error> {
    let result = register_actual(store, client.clone(), request).await;
    if result.is_err() && !client.released.swap(true, Ordering::AcqRel) {
        // A channel-stop supervisor may also retain this pending client; do
        // not rely on final Arc Drop to clean a rejected/failed registration.
        let _ = release_job(&client).await;
    }
    result
}

async fn register_actual(
    store: Arc<Inner>,
    client: Arc<ClientInner>,
    request: Registration,
) -> Result<ClientLease, Error> {
    if *client.stop.borrow() || store.stopped.load(Ordering::Acquire) {
        client.released.store(true, Ordering::Release);
        client.cleanup_done.send_replace(true);
        return Err(Error::Stopped);
    }
    let mut connection = client.connection.clone();
    let result: Vec<String> = store
        .register
        .key(&request.keys[0])
        .key(&request.keys[1])
        .key(&request.keys[2])
        .key(&request.keys[3])
        .arg(request.data)
        .arg(request.policy)
        .arg(request.stream_limit)
        .arg(client.config.ttl.as_millis() as u64)
        .arg(client.config.admission_scan_limit)
        .arg(&client.id)
        .invoke_async(&mut connection)
        .await
        .map_err(|_| Error::RedisUnavailable)?;
    match result.first().map(String::as_str) {
        Some("OK") => {}
        Some("LIMIT") => return Err(Error::UserLimit),
        Some("STOPPED") => return Err(Error::Stopped),
        Some("SCAN_LIMIT") => return Err(Error::ScanLimit),
        Some("CORRUPT") => return Err(Error::CorruptMetadata),
        _ => return Err(Error::RedisUnavailable),
    }
    let stopped = {
        let mut clients = store.clients.lock().unwrap();
        client.registered.store(true, Ordering::Release);
        if store.stopped.load(Ordering::Acquire) || *client.stop.borrow() {
            true
        } else {
            clients.insert(client.id.clone(), Arc::downgrade(&client));
            false
        }
    };
    if stopped {
        let _ = release(&client).await;
        return Err(Error::Stopped);
    }
    Ok(ClientLease { inner: client })
}

fn release_job(client: &ClientInner) -> tokio::task::JoinHandle<Result<(), Error>> {
    let (metadata, set, stop) = keys(&client.channel, &client.id);
    let token = client.token.clone();
    let id = client.id.clone();
    let admission = client._permit.clone();
    let done = client.cleanup_done.clone();
    let failed = client.cleanup_error.clone();
    let mut connection = client.connection.clone();
    client.runtime.spawn(async move {
        let _admission = admission;
        let result: Result<i64, redis::RedisError> = Script::new(scripts::RELEASE)
            .key(metadata)
            .key(set)
            .key(stop)
            .arg(token)
            .arg(id)
            .invoke_async(&mut connection)
            .await;
        failed.store(result.is_err(), Ordering::Release);
        done.send_replace(true);
        result.map_err(|_| Error::RedisUnavailable)?;
        Ok(())
    })
}

pub(super) async fn release(client: &ClientInner) -> Result<(), Error> {
    client.stop.send_replace(true);
    if !client.registered.load(Ordering::Acquire) {
        return Ok(());
    }
    if client.released.swap(true, Ordering::AcqRel) {
        return Ok(());
    }
    // Timeout limits the caller, not the supervised Redis command lifetime.
    time::timeout(client.config.command_timeout, release_job(client))
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?
}

pub(super) fn drop_client(client: &mut ClientInner) {
    if let Some(store) = client.store.upgrade() {
        store.clients.lock().unwrap().remove(&client.id);
    }
    if client.released.swap(true, Ordering::AcqRel) {
        return;
    }
    // Use the creation runtime even if the final lease is dropped on a plain
    // thread. Dropping the JoinHandle detaches the bounded supervised task.
    std::mem::drop(release_job(client));
}

/// Keep strong references to the bounded registry snapshot, then signal outside
/// its mutex. Pending REGISTER jobs observe the stop and clean their result;
/// jobs that already handed off are released directly without body polling.
pub(super) async fn stop_local_channel(store: Arc<Inner>, channel: &str) -> Result<usize, Error> {
    if channel.is_empty() || channel.len() > 256 || channel.contains(':') {
        return Err(Error::InvalidClient);
    }
    // Upgrade under the mutex, but never filter/drop a possible final Arc
    // there: ClientInner::drop reacquires the same registry mutex.
    let all_clients = {
        let map = store.clients.lock().unwrap();
        map.values().filter_map(Weak::upgrade).collect::<Vec<_>>()
    };
    let clients = all_clients
        .into_iter()
        .filter(|client| client.channel == channel)
        .collect::<Vec<_>>();
    for client in &clients {
        client.stop.send_replace(true);
    }
    let timeout = store.config.command_timeout;
    let count = clients.len();
    let task = store.runtime.spawn(async move {
        // At most32 cleanup commands are in flight. This supervisor owns the
        // bounded snapshot even when its caller cancels or reaches a deadline.
        let mut failed = false;
        for batch in clients.chunks(32) {
            let mut jobs = Vec::new();
            for client in batch {
                if client.registered.load(Ordering::Acquire)
                    && !client.released.swap(true, Ordering::AcqRel)
                {
                    jobs.push((client.clone(), release_job(client)));
                }
            }
            for (client, job) in jobs {
                match job.await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => failed = true,
                    Err(_) => {
                        failed = true;
                        client.cleanup_error.store(true, Ordering::Release);
                        client.cleanup_done.send_replace(true);
                    }
                }
            }
        }
        for client in &clients {
            let mut done = client.cleanup_done.subscribe();
            while !*done.borrow() {
                done.changed().await.map_err(|_| Error::RedisUnavailable)?;
            }
            if client.cleanup_error.load(Ordering::Acquire) {
                failed = true;
            }
        }
        if failed {
            Err(Error::RedisUnavailable)
        } else {
            Ok(count)
        }
    });
    time::timeout(timeout, task)
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?
}
