//! Real owned OS children, no FFmpeg/media/Docker/Source application.
//! These test the production signal/receipt/Drop code, not result semantics.
use super::*;
use std::sync::mpsc;

fn owned_sleep(caller: Arc<dyn Send + Sync>) -> (OwnedChild, Identity, DateTime<Utc>, Vec<String>) {
    let argv = vec!["/bin/sleep".to_owned(), "30".to_owned()];
    let started = Utc::now();
    let transcript = Arc::new(Transcript::new());
    let record = transcript
        .begin(argv.clone(), started)
        .expect("real owned record");
    let mut command = Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .process_group(0);
    let child = command.spawn().expect("owned local sleep");
    let mut owned = OwnedChild {
        child,
        reader: None,
        reaped: false,
        transcript,
        record,
        _caller: caller,
    };
    let identity = owned.identity().expect("actual unreaped child identity");
    let stderr = owned.child.stderr.take().expect("actual child stderr pipe");
    owned.reader = Some(thread::spawn(move || drain(stderr)));
    (owned, identity, started, argv)
}
fn bounded_status(child: &mut OwnedChild) -> ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(status) = child.try_wait().expect("actual wait poll") {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "owned child failed to settle within2s; emergency Drop still kills/waits"
        );
        thread::sleep(Duration::from_millis(5));
    }
}
#[test]
fn real_owned_child_wrong_identity_refuses_then_actual_sigint_is_observed() {
    let owner: Arc<dyn Send + Sync> = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let (mut owned, identity, started, argv) = owned_sleep(owner);
    let pid = identity.pid;
    let mut wrong = identity.clone();
    wrong.start_ticks += 1;
    assert!(matches!(
        signal_int(&mut owned.child, &wrong),
        Err(Error::Unsupported)
    ));
    assert!(owned
        .try_wait()
        .expect("still actual owned child")
        .is_none());
    assert!(weak.upgrade().is_some());
    signal_int(&mut owned.child, &identity).expect("actual confined SIGINT syscall");
    let status = bounded_status(&mut owned);
    assert_eq!(status.signal(), Some(libc::SIGINT));
    let receipt = owned
        .receipt(status, identity, argv, started, true, false)
        .expect("real wait and stderr join");
    assert_eq!(receipt.signal, Some(libc::SIGINT));
    assert!(receipt.sigint_requested);
    assert!(!receipt.forced_kill);
    assert!(receipt.stderr.read_error.is_none());
    assert!(weak.upgrade().is_some());
    drop(owned);
    assert!(weak.upgrade().is_none());
    assert!(proc_identity(pid).is_err());
}
#[test]
fn real_emergency_drop_waits_reader_and_child_before_releasing_original_caller() {
    let owner: Arc<dyn Send + Sync> = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let (owned, identity, _, _) = owned_sleep(owner);
    let transcript = owned.transcript.clone();
    assert!(weak.upgrade().is_some());
    drop(owned);
    assert!(weak.upgrade().is_none());
    assert!(proc_identity(identity.pid).is_err());
    let (records, poisoned, pending) = transcript.snapshot();
    assert!(!poisoned);
    assert!(!pending);
    assert_eq!(records.len(), 1);
    assert!(records[0].emergency);
    assert_eq!(records[0].signal, Some(libc::SIGKILL));
    assert!(records[0].wait_error.is_none());
    assert!(records[0].reader_error.is_none());
    assert!(records[0].stderr.is_some());
    assert!(records[0].settled_utc.is_some());
}
#[test]
fn already_reaped_owned_child_refuses_identity_lookup_without_claiming_syscall_esrch() {
    let owner: Arc<dyn Send + Sync> = Arc::new(());
    let (mut owned, identity, started, argv) = owned_sleep(owner);
    owned.kill().expect("owned setup kill");
    let status = owned.wait().expect("actual setup reap");
    // This is genuine post-reap identity refusal, not a manufactured syscall
    // ESRCH race. No arbitrary PID or stale envelope is signaled.
    assert!(matches!(
        signal_int(&mut owned.child, &identity),
        Err(Error::Identity(_)) | Err(Error::Unsupported)
    ));
    let receipt = owned
        .receipt(status, identity, argv, started, false, true)
        .expect("actual remaining reader join");
    assert_eq!(receipt.signal, Some(libc::SIGKILL));
    assert!(receipt.forced_kill);
}
#[test]
fn original_caller_remains_held_while_actual_receipt_waits_for_stderr_join() {
    let owner: Arc<dyn Send + Sync> = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let (mut owned, identity, started, argv) = owned_sleep(owner);
    let original_reader = owned.reader.take().expect("actual reader");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    owned.reader = Some(thread::spawn(move || {
        entered_tx.send(()).expect("owned reader barrier");
        release_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("bounded reader release");
        original_reader
            .join()
            .expect("actual production stderr drain join")
    }));
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("actual held join entered");
    owned.kill().expect("owned setup kill");
    let status = owned.wait().expect("actual setup wait");
    let transcript = owned.transcript.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let observer = thread::spawn(move || {
        let receipt = owned.receipt(status, identity, argv, started, false, true);
        drop(owned);
        done_tx.send(receipt).expect("owned receipt result");
    });
    // The marker is written by the production receipt method after actual
    // wait, before its real reader join. A merely scheduled observer is not a
    // successful stage-reach receipt.
    let deadline = Instant::now() + Duration::from_secs(1);
    let reached = loop {
        let (rows, _, _) = transcript.snapshot();
        if rows[0].signal == Some(libc::SIGKILL) && rows[0].settled_utc.is_none() {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        thread::sleep(Duration::from_millis(5));
    };
    let before_release = done_rx.recv_timeout(Duration::from_millis(250));
    let pending = matches!(before_release, Err(mpsc::RecvTimeoutError::Timeout));
    let held = weak.upgrade().is_some();
    let released = release_tx.send(()).is_ok();
    let receipt = match before_release {
        Ok(value) => Some(value),
        Err(mpsc::RecvTimeoutError::Timeout) => done_rx.recv_timeout(Duration::from_secs(2)).ok(),
        Err(_) => None,
    };
    let joined = observer.join();
    assert!(
        reached && pending && held && released,
        "actual production method/reader stage must hold the same caller for250ms"
    );
    joined.expect("owned receipt thread joined");
    assert!(weak.upgrade().is_none());
    assert!(receipt
        .expect("actual joined receipt")
        .expect("actual receipt")
        .stderr
        .read_error
        .is_none());
}
