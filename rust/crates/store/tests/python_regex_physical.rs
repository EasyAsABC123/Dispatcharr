//! Explicitly scheduled real packaged-dependency component controls.
//! No Source backend/task function or DB/cache operation is executed.
//! Original Dispatcharr/CPython/regex authors retain all algorithm credit.
use dispatcharr_store::{PythonRegexError, PythonRegexReceipt, RegexExecutor};
use serde_json::{json, Value};
use std::{
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
fn executable() -> PathBuf {
    assert_eq!(
        std::env::var("DISPATCHARR_REGEX_PHYSICAL_OWNED").as_deref(),
        Ok("1")
    );
    let path = PathBuf::from("/usr/local/bin/dispatcharr-regex-python");
    assert!(
        path.is_file(),
        "requires actual candidate image's packaged interpreter"
    );
    path
}
fn emit(case: &str, value: Value) {
    println!("{}", json!({"regex_physical_case":case,"actual":value}));
}
fn receipt(value: &PythonRegexReceipt) -> Value {
    json!({"pid":value.identity.pid,"generation":value.identity.start_generation,"namespace":value.identity.pid_namespace,"exit":value.actual_exit,"wait":value.wait_observed,"stdout_EOF":value.stdout_eof,"stderr_EOF":value.stderr_eof,"forced":value.forced,"descendants_absent":value.descendants_absent,"callbacks_settled":value.callbacks_settled})
}
async fn until<T>(seconds: u64, mut predicate: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        if let Some(value) = predicate() {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "actual predicate deadline expired"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
fn process(pid: u32) -> Option<(String, u64)> {
    let raw = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields: Vec<_> = raw.rsplit_once(')')?.1.split_whitespace().collect();
    Some((
        fields.get(19)?.to_string(),
        fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?,
    ))
}
fn direct_regex_children() -> Vec<u32> {
    let mut result = Vec::new();
    for task in std::fs::read_dir("/proc/self/task").unwrap().flatten() {
        if let Ok(raw) = std::fs::read_to_string(task.path().join("children")) {
            for pid in raw.split_whitespace().filter_map(|p| p.parse::<u32>().ok()) {
                let cmd = std::fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
                if cmd
                    .windows(b"/bridge/regex_bootstrap.py".len())
                    .any(|v| v == b"/bridge/regex_bootstrap.py")
                {
                    result.push(pid);
                }
            }
        }
    }
    result.sort_unstable();
    result.dedup();
    result
}
struct Caller(Arc<AtomicBool>);
impl Drop for Caller {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
#[ignore = "Root schedules inside exact candidate image; no app/Cargo self-dispatch"]
async fn actual_packaged_grammar_remote_error_timeout_and_normal_wait_eof() {
    let executor = RegexExecutor::prepare(executable(), Arc::new(()));
    let filter = executor
        .execute(
            json!({"kind":"filter","names":["ß","SS","no"],"config":{"pattern":"(?V1)^(?i:ss)$"}}),
            Arc::new(()),
        )
        .await
        .unwrap();
    emit("actual-fullcase-search", filter.clone());
    assert_eq!(filter["matches"], json!([true, true, false]));
    let version_newer = executor
        .execute(
            json!({"kind":"version_compare","current":"0.33.0","latest":"1!0.1"}),
            Arc::new(()),
        )
        .await
        .unwrap();
    emit("actual-pinned-packaging-epoch-order", version_newer.clone());
    assert_eq!(
        version_newer,
        json!({"kind":"parsed","newer":true,"ordering":1})
    );
    let version_invalid = executor
        .execute(
            json!({"kind":"version_compare","current":"0.33.0","latest":"1.2?"}),
            Arc::new(()),
        )
        .await
        .unwrap();
    emit(
        "actual-pinned-packaging-InvalidVersion",
        version_invalid.clone(),
    );
    assert_eq!(
        version_invalid,
        json!({"kind":"invalid","exception":"InvalidVersion"})
    );
    // Literal cases derived from Original create_dummy_epg_data's primitive
    // ordering; these are dependency controls, never HTTP/model qualification.
    for (name, expected) in [
        ("İ", "dummy_i\u{307}"),
        ("Α ΟΣ", "dummy_α_ος"),
        ("\u{345}\u{5b0}A\u{301}", "dummy_a"),
        ("²Ⅷ四١", "dummy_²ⅷ四١"),
        ("X\tY\n Z", "dummy_xy_z"),
        ("♥!$", "dummy_"),
        ("Foo-Bar Baz", "dummy_foo_bar_baz"),
    ] {
        let actual = executor.dummy_epg_name(name, Arc::new(())).await.unwrap();
        emit(
            "actual-pinned-dummy-name-primitives",
            json!({"input":name,"actual":actual}),
        );
        assert_eq!(actual, expected);
    }
    // Never truncate or reject a post-save ID just for exceeding the model's
    // varchar255 limit; the caller must reach that DB operation after save.
    let maximum = "A".repeat(255);
    assert_eq!(
        executor
            .dummy_epg_name(&maximum, Arc::new(()))
            .await
            .unwrap(),
        format!("dummy_{}", "a".repeat(255))
    );
    assert!(matches!(
        executor
            .dummy_epg_name(&"A".repeat(256), Arc::new(()))
            .await,
        Err(PythonRegexError::Capacity)
    ));
    let invalid = executor
        .execute(
            json!({"kind":"filter","names":["x"],"config":{"pattern":"("}}),
            Arc::new(()),
        )
        .await
        .unwrap();
    emit("actual-invalid-filter", invalid.clone());
    assert_eq!(invalid["active"], false);
    assert!(invalid["caught"].is_object());
    let error = executor
        .execute(
            json!({"kind":"filter","names":["x"],"config":{"pattern":12}}),
            Arc::new(()),
        )
        .await
        .unwrap_err();
    emit("actual-TypeError", json!({"error":format!("{error:?}")}));
    assert!(matches!(error,PythonRegexError::Remote(ref v)if v["actual_class"]=="TypeError"));
    let rename=executor.execute(json!({"kind":"rename","names":["Auto 123","Straße"],"config":{"pattern":"(?V1)^(?|Auto (\\d+)|(Straße))$","replacement":"Selected $1"}}),Arc::new(())).await.unwrap();
    emit("actual-branchreset-replacement-after-error", rename.clone());
    assert_eq!(rename["items"][0]["name"], "Selected 123");
    assert_eq!(rename["items"][1]["name"], "Selected Straße");
    let long = format!("{}b", "a".repeat(511));
    let timeout=executor.execute(json!({"kind":"rename","names":[long],"config":{"pattern":"(?:a|aa)+$","replacement":"changed"}}),Arc::new(())).await.unwrap();
    emit("actual100ms-substitution-timeout", timeout.clone());
    assert_eq!(timeout["items"][0]["name"], long);
    assert_eq!(
        timeout["items"][0]["caught"]["actual_class"],
        "TimeoutError"
    );
    let actual = executor
        .shutdown(Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    emit("normal-physical-wait", receipt(&actual));
    assert_eq!(actual.actual_exit, Some(0));
    assert!(
        actual.wait_observed
            && actual.stdout_eof
            && actual.stderr_eof
            && !actual.forced
            && actual.descendants_absent
            && actual.callbacks_settled
    );
}

#[tokio::test]
#[ignore = "Root schedules isolated actual held search; CPU/owner/kill/wait/EOF diagnostic"]
async fn actual_unbounded_search_caller_loss_retains_owner_until_physical_retirement() {
    let executor = RegexExecutor::prepare(executable(), Arc::new(()));
    // Establish actual ready/result first, so later CPU progression cannot be
    // mistaken for cold interpreter/module import work.
    executor
        .execute(
            json!({"kind":"filter","names":[],"config":{}}),
            Arc::new(()),
        )
        .await
        .unwrap();
    let pid = until(5, || direct_regex_children().into_iter().next()).await;
    let first = process(pid).unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let owner = Arc::new(Caller(dropped.clone()));
    let active = executor.clone();
    let call = tokio::spawn(async move {
        active.execute(json!({"kind":"filter","names":[format!("{}b","a".repeat(511))],"config":{"pattern":"(?:a|aa)+$"}}),owner).await
    });
    let observed = Instant::now();
    let later = until(5, || {
        process(pid).filter(|v| {
            observed.elapsed() >= Duration::from_millis(250) && v.0 == first.0 && v.1 > first.1
        })
    })
    .await;
    emit(
        "actual-held-search",
        json!({"pid":pid,"generation":first.0,"CPUticks_before":first.1,"CPUticks_later":later.1}),
    );
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    assert!(!dropped.load(Ordering::SeqCst));
    let stop = executor.shutdown(Duration::from_millis(300)).await;
    emit(
        "interrupted-search-not-nonmatch",
        json!({"shutdown":format!("{stop:?}")}),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let actual = loop {
        if let Some(value) = executor.last_receipt().await {
            break value;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    emit("actual-interrupted-physical-wait", receipt(&actual));
    assert!(
        actual.forced
            && actual.wait_observed
            && actual.stdout_eof
            && actual.stderr_eof
            && actual.descendants_absent
            && actual.callbacks_settled
    );
    until(5, || dropped.load(Ordering::SeqCst).then_some(())).await;
    assert!(process(pid).is_none_or(|v| v.0 != first.0));
}

#[tokio::test]
#[ignore = "Component-only injected pre-ready FIFO; Root owns invocation/cleanup"]
async fn actual_pre_ready_stop_releases_caller_only_after_child_absence() {
    let _packaged_executable = executable();
    let root = std::env::temp_dir().join(format!("owned-regex-startup-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let gate = root.join("gate");
    let marker = root.join("pid");
    let shim = root.join("pre-ready");
    let c = std::ffi::CString::new(gate.as_os_str().as_encoded_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
    // Only owned UUID-derived paths are inserted; no user strings/shell input.
    std::fs::write(&shim,format!("#!/bin/sh\nprintf '%s' \"$$\" > '{}'\nread gate < '{}'\nexec /usr/local/bin/dispatcharr-regex-python \"$@\"\n",marker.display(),gate.display())).unwrap();
    std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o700)).unwrap();
    let executor = RegexExecutor::prepare(shim, Arc::new(()));
    let dropped = Arc::new(AtomicBool::new(false));
    let owner = Arc::new(Caller(dropped.clone()));
    let active = executor.clone();
    let call = tokio::spawn(async move {
        active
            .execute(json!({"kind":"filter","names":[],"config":{}}), owner)
            .await
    });
    let pid = until(5, || {
        std::fs::read_to_string(&marker)
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
    })
    .await;
    let generation = process(pid).unwrap().0;
    assert!(!dropped.load(Ordering::SeqCst));
    let stop = executor.shutdown(Duration::from_millis(200)).await;
    let result = tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .unwrap()
        .unwrap();
    emit(
        "actual-startup-interruption",
        json!({"pid":pid,"generation":generation,"shutdown":format!("{stop:?}"),"call":format!("{result:?}"),"receipt":executor.last_receipt().await.as_ref().map(receipt),"component_only_injected_startup_gate":true}),
    );
    assert!(result.is_err());
    until(5, || dropped.load(Ordering::SeqCst).then_some(())).await;
    assert!(process(pid).is_none_or(|v| v.0 != generation));
    std::fs::remove_dir_all(root).unwrap();
}
