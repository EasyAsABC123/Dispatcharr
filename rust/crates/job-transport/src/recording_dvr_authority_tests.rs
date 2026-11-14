//! UNCOMPILED QA text. Actual transport entrypoint refusals after real owned
//! loopback handshake; no Docker/Redis app, not Source task execution authority.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

async fn command<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<Vec<Vec<u8>>>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    let error = || std::io::Error::new(std::io::ErrorKind::InvalidData, "owned test RESP frame");
    if line.len() > 64 || !line.starts_with('*') {
        return Err(error());
    }
    let count: usize = line[1..].trim().parse().map_err(|_| error())?;
    if count == 0 || count > 16 {
        return Err(error());
    }
    let mut args = Vec::new();
    for _ in 0..count {
        line.clear();
        reader.read_line(&mut line).await?;
        if line.len() > 64 || !line.starts_with('$') {
            return Err(error());
        }
        let size: usize = line[1..].trim().parse().map_err(|_| error())?;
        if size > 4096 {
            return Err(error());
        }
        let mut raw = vec![0_u8; size + 2];
        reader.read_exact(&mut raw).await?;
        if &raw[size..] != b"\r\n" {
            return Err(error());
        }
        raw.truncate(size);
        args.push(raw);
    }
    Ok(Some(args))
}
#[tokio::test]
async fn actual_all_fourteen_generic_methods_refuse_dvr_before_application_network() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("owned listener");
    let port = listener.local_addr().expect("owned address").port();
    let application = Arc::new(AtomicUsize::new(0));
    let handshake = Arc::new(AtomicUsize::new(0));
    let calls = application.clone();
    let setup = handshake.clone();
    let peer = tokio::spawn(async move {
        let mut children = Vec::new();
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.expect("broker/result constructors");
            let calls = calls.clone();
            let setup = setup.clone();
            children.push(tokio::spawn(async move {
                let (read, mut write) = socket.into_split();
                let mut reader = BufReader::new(read);
                while let Some(args) = command(&mut reader).await.expect("bounded owned RESP") {
                    let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
                    if matches!(name.as_str(), "CLIENT" | "AUTH" | "SELECT") {
                        setup.fetch_add(1, Ordering::SeqCst);
                        write.write_all(b"+OK\r\n").await.expect("handshake reply");
                    } else {
                        calls.fetch_add(1, Ordering::SeqCst);
                        write
                            .write_all(b"-ERR forbidden application command\r\n")
                            .await
                            .expect("negative diagnostic");
                    }
                }
            }));
        }
        for child in children {
            child.await.expect("owned peer join");
        }
    });
    let mut jobs = Jobs::connect_recording_dvr(
        &format!("redis://127.0.0.1:{port}/8"),
        &format!("redis://127.0.0.1:{port}/9"),
        Config::default(),
    )
    .await
    .expect("real typed constructor handshake");
    assert!(handshake.load(Ordering::SeqCst) >= 2);
    assert_eq!(application.load(Ordering::SeqCst), 0);
    // Actual local method preserves each pre-existing role's behavior. This
    // tests the cached-pool path as well, not a generated copy of the predicate.
    for (role, allowed) in [
        (Authority::Fixture, true),
        (Authority::Index, true),
        (Authority::Refresh, true),
        (Authority::LocalArtworkFixture, true),
        (Authority::LocalArtwork, false),
        (Authority::Series, false),
        (Authority::DefaultServices, false),
        (Authority::Backup, false),
        (Authority::Dvr, false),
    ] {
        Arc::get_mut(&mut jobs.inner)
            .expect("no candidate/inner aliases")
            .authority = role;
        let result = jobs.eta_pool(crate::eta::Limits::default());
        assert_eq!(result.is_ok(), allowed);
    }
    assert!(jobs.allows_recording_dvr());
    let limits = crate::eta::Limits::default();
    let caller: Arc<dyn Send + Sync> = Arc::new(());
    let task_id = Uuid::new_v4();
    let parent = Uuid::new_v4();
    let initial = crate::index_protocol::IndexTask::Initial { source_id: 11 };
    let deferred = crate::index_protocol::IndexTask::Deferred {
        source_id: 11,
        retry: 1,
        root: parent,
        parent,
        eta: chrono::Utc::now() + chrono::Duration::seconds(60),
    };
    let frame = crate::index_protocol::encode(
        &deferred,
        task_id,
        Uuid::parse_str("00000000-0000-3000-8000-000000000000").expect("reply3"),
        Uuid::new_v4(),
        "gen1@owned",
    )
    .expect("real bounded index codec");
    let receipt =
        Arc::new(crate::eta::Receipt::prepare(&frame).expect("valid actual receipt codec"));
    let pool = Arc::new(crate::eta::Pool::new(limits).expect("local receipt pool"));
    let budget = Arc::new(pool.acquire().expect("actual bounded receipt admission"));
    let candidate = crate::eta::Candidate {
        receipt: receipt.clone(),
        budget: budget.clone(),
        pool: pool.clone(),
    };
    let reservation = crate::eta::Reservation::new_shared(receipt, budget, pool);
    assert!(matches!(jobs.eta_pool(limits), Err(Error::UnsupportedTask)));
    assert!(matches!(
        jobs.publish_index(&initial, task_id, caller.clone()).await,
        Err(Error::UnsupportedTask)
    ));
    assert!(matches!(
        jobs.read_index(task_id, caller.clone()).await,
        Err(Error::UnsupportedTask)
    ));
    assert!(matches!(
        jobs.peek_index_candidate(caller.clone(), limits).await,
        Err(Error::UnsupportedTask)
    ));
    assert!(matches!(
        jobs.reserve_index_candidate(candidate.clone(), caller.clone(), limits)
            .await,
        Err(Error::UnsupportedTask)
    ));
    assert!(matches!(
        jobs.reserve_index_candidate_settled(candidate, caller.clone(), limits)
            .await,
        Err(Error::UnsupportedTask)
    ));
    assert!(matches!(
        jobs.reserve_index_eta(caller.clone(), limits).await,
        Err(Error::UnsupportedTask)
    ));
    assert!(matches!(
        jobs.read_index_eta(task_id, caller.clone(), limits).await,
        Err(Error::UnsupportedTask)
    ));
    assert!(matches!(
        jobs.ack_index_eta(&reservation, caller.clone()).await,
        Err(Error::UnsupportedTask)
    ));
    assert!(matches!(
        jobs.restore_index_eta(&reservation, caller.clone()).await,
        Err(Error::UnsupportedTask)
    ));
    assert!(matches!(
        jobs.claim_index(caller.clone()).await,
        Err(Error::UnsupportedTask)
    ));
    assert!(matches!(
        jobs.write_index_success(task_id, None, vec![], caller.clone())
            .await,
        Err(Error::UnsupportedTask)
    ));
    assert!(matches!(
        jobs.write_index_terminal(
            task_id,
            None,
            vec![],
            crate::index_terminal::Terminal::NullSuccess,
            caller
        )
        .await,
        Err(Error::UnsupportedTask)
    ));
    let refresh = crate::refresh_protocol::RefreshMessage {
        task_id,
        source_id: 11,
        force: false,
        retry: 0,
        root: task_id,
        parent: None,
        eta: None,
        call: crate::refresh_protocol::RefreshCall::initial(11),
    };
    assert!(matches!(
        jobs.read_refresh_lineage(&refresh, Arc::new(())).await,
        Err(Error::UnsupportedTask)
    ));
    assert_eq!(
        application.load(Ordering::SeqCst),
        0,
        "no command may reach either broker/backend after construction"
    );
    assert_eq!(
        jobs.inner.slots.available_permits(),
        jobs.inner.config.operations
    );
    drop(reservation);
    drop(jobs);
    tokio::time::timeout(Duration::from_secs(3), peer)
        .await
        .expect("actual owned socket close")
        .expect("actual peer completion");
}
