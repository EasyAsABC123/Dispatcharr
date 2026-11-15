//! Actual normal publisher/codec/transport/registry path with a bounded RESP
//! reply peer. No PostgreSQL/Redis application and no Source execution authority.
use super::*;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

async fn command<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<Vec<Vec<u8>>>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    let invalid = || std::io::Error::from(std::io::ErrorKind::InvalidData);
    if line.len() > 64 || !line.starts_with('*') {
        return Err(invalid());
    }
    let count: usize = line[1..].trim().parse().map_err(|_| invalid())?;
    if count == 0 || count > 16 {
        return Err(invalid());
    }
    let mut args = Vec::new();
    for _ in 0..count {
        line.clear();
        reader.read_line(&mut line).await?;
        if line.len() > 64 || !line.starts_with('$') {
            return Err(invalid());
        }
        let size: usize = line[1..].trim().parse().map_err(|_| invalid())?;
        if size > 65536 {
            return Err(invalid());
        }
        let mut value = vec![0; size + 2];
        reader.read_exact(&mut value).await?;
        if &value[size..] != b"\r\n" {
            return Err(invalid());
        }
        value.truncate(size);
        args.push(value);
    }
    Ok(Some(args))
}

#[tokio::test]
async fn recovery_waits_for_actual_publisher_then_survives_cancellation_and_drains() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}/1",
        listener.local_addr().unwrap().port()
    );
    let frames = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let seen = frames.clone();
    let peer = tokio::spawn(async move {
        let mut sockets = Vec::new();
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let seen = seen.clone();
            sockets.push(tokio::spawn(async move {
                let (read, mut write) = socket.into_split();
                let mut reader = BufReader::new(read);
                while let Some(args) = command(&mut reader).await.unwrap() {
                    let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
                    if matches!(name.as_str(), "CLIENT" | "AUTH" | "SELECT") {
                        write.write_all(b"+OK\r\n").await.unwrap();
                    } else if name == "PING" {
                        write.write_all(b"+PONG\r\n").await.unwrap();
                    } else if name == "EVALSHA"
                        && args.len() == 7
                        && args[2] == b"2"
                        && args[3] == b"dvr"
                        && args[4] == b"_kombu.binding.dvr"
                    {
                        crate::normal_named_recording::parse_head(&args[5])
                            .expect("production named frame");
                        seen.lock().unwrap().push(args[5].clone());
                        write.write_all(b":1\r\n").await.unwrap();
                    } else {
                        panic!("unexpected publication peer command {name}");
                    }
                }
            }));
        }
        for socket in sockets {
            socket.await.unwrap();
        }
    });
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgresql://unused@127.0.0.1/unused")
        .unwrap();
    let (publisher, _registry) = NormalRecordingPublisher::prepare(pool, &url, &url)
        .await
        .unwrap();
    let blocked = publisher.inner.slot.clone().acquire_owned().await.unwrap();
    let caller: Arc<dyn Send + Sync> = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let start = DateTime::from_timestamp(1800000000, 123456000).unwrap();
    let end = start + chrono::Duration::seconds(347);
    let mut attempt = Box::pin(publisher.publish_named(
        17,
        42,
        start,
        end,
        start,
        crate::normal_named_recording::Kind::Recovery,
        Uuid::new_v4(),
        Uuid::new_v4(),
        caller,
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut attempt)
            .await
            .is_err(),
        "ordinary DVR publisher pressure must wait, not return Capacity after interruption"
    );
    assert!(
        frames.lock().unwrap().is_empty(),
        "no publication before actual admission"
    );
    drop(attempt);
    assert!(
        weak.upgrade().is_some(),
        "detached accepted work retains its physical caller"
    );
    drop(blocked);
    tokio::time::timeout(Duration::from_secs(2), async {
        while publisher.inner.named.pending().unwrap() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let raw = frames.lock().unwrap()[0].clone();
    let proof = publisher.inner.named.matching(&raw).unwrap().unwrap();
    assert_eq!(proof.task_key(), "dvr-recover-17");
    assert_eq!(proof.recording_start(), start);
    assert_eq!(proof.recording_end(), end);
    assert_eq!(
        frames.lock().unwrap().len(),
        1,
        "exactly one settled actual writer call"
    );
    publisher.inner.named.complete(&proof).unwrap();
    drop(proof);
    tokio::time::timeout(Duration::from_secs(2), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    // Close while a second accepted waiter is blocked. Drain must time out,
    // not ignore that work; after real release it settles Closed with no write.
    let blocked = publisher.inner.slot.clone().acquire_owned().await.unwrap();
    let mut closing = Box::pin(publisher.publish_named(
        18,
        42,
        start,
        end,
        start,
        crate::normal_named_recording::Kind::Recovery,
        Uuid::new_v4(),
        Uuid::new_v4(),
        Arc::new(()),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut closing)
            .await
            .is_err()
    );
    assert!(matches!(
        publisher.shutdown(Duration::from_millis(20)).await,
        Err(Error::DrainTimeout)
    ));
    drop(blocked);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), &mut closing)
            .await
            .unwrap(),
        Err(Error::Closed)
    ));
    drop(closing);
    assert_eq!(
        frames.lock().unwrap().len(),
        1,
        "closed waiter must not publish"
    );
    publisher.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(publisher);
    tokio::time::timeout(Duration::from_secs(3), peer)
        .await
        .expect("actual peer socket settlement")
        .unwrap();
}
