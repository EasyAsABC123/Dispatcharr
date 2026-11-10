//! Pipeline fault tests; application credit remains with original authors.
use super::*;
use std::{fs, thread};
struct Fixture(PathBuf);
impl Fixture {
    fn new(xml: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "dispatcharr-guide-import-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&path, xml).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
fn xml(count: usize) -> String {
    format!("<tv>{}</tv>",(0..count).map(|n|format!("<programme channel=\"mapped\" start=\"20261109110000 +0000\" stop=\"20261109120000 +0000\"><title>Programme{n}</title></programme>")).collect::<String>())
}
fn mapping() -> HashMap<String, i64> {
    HashMap::from([("mapped".into(), 17)])
}
fn config() -> ImportConfig {
    ImportConfig {
        batch_rows: 1,
        batch_bytes: 65536,
        ..ImportConfig::default()
    }
}
#[test]
fn mapped_projection_errors_and_unmapped_skips_are_separate() {
    let fixture=Fixture::new("<tv><channel id=\"mapped\"/><programme channel=\"mapped\" start=\"20261109110000 +0000\" stop=\"20261109120000 +0000\"><title>Valid</title></programme><programme channel=\"unmapped\"/><programme channel=\"mapped\" start=\"invalid\"/><programme channel=\"mapped\" start=\"20261109120000 +0000\" stop=\"20261109130000 +0000\"><title>Other</title></programme></tv>");
    let (tx, mut rx) = mpsc::channel(2);
    let path = fixture.0.clone();
    let worker = thread::spawn(move || {
        parse_file(
            path,
            mapping(),
            config(),
            Arc::new(AtomicBool::new(false)),
            tx,
        )
    });
    let mut rows = Vec::new();
    while let Some(batch) = rx.blocking_recv() {
        if let Batch::Rows {
            rows: batch_rows, ..
        } = batch
        {
            assert_eq!(batch_rows.len(), 1);
            rows.extend(batch_rows);
        }
    }
    let parsed = worker.join().unwrap().unwrap();
    assert_eq!(
        (
            parsed.report.parsed_programmes,
            parsed.report.skipped_unmapped,
            parsed.report.invalid_programmes
        ),
        (2, 1, 1)
    );
    assert_eq!(parsed.report.channels_with_programmes, 1);
    assert_eq!(parsed.report.parser.programmes, 4);
    assert_eq!(
        rows.iter()
            .map(|row| row.title.as_str())
            .collect::<Vec<_>>(),
        ["Valid", "Other"]
    );
}
#[test]
fn incomplete_file_cannot_send_a_staging_batch() {
    let fixture = Fixture::new("<tv><programme channel=\"mapped\"/>");
    let (tx, mut rx) = mpsc::channel(1);
    assert!(matches!(
        parse_file(
            fixture.0.clone(),
            mapping(),
            config(),
            Arc::new(AtomicBool::new(false)),
            tx
        ),
        Err(ImportError::Incomplete)
    ));
    assert!(rx.blocking_recv().is_none());
}
struct Witness(Arc<AtomicBool>);
impl Drop for Witness {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
#[test]
fn blocked_parser_keeps_physical_guard_until_queue_is_released() {
    let fixture = Fixture::new(&xml(100));
    let (tx, mut rx) = mpsc::channel(1);
    let cancelled = Arc::new(AtomicBool::new(false));
    let flag = cancelled.clone();
    let path = fixture.0.clone();
    let dropped = Arc::new(AtomicBool::new(false));
    let witness = Witness(dropped.clone());
    let worker = thread::spawn(move || {
        let _physical_guard = witness;
        parse_file(path, mapping(), config(), flag, tx)
    });
    assert!(rx.blocking_recv().is_some());
    cancelled.store(true, Ordering::Release);
    thread::sleep(Duration::from_millis(20));
    // The queue/reader can be blocked even after the logical cancel flag flips.
    // Dropping its receiver is the bounded release, never abandoning the thread.
    if !worker.is_finished() {
        assert!(!dropped.load(Ordering::Acquire));
    }
    drop(rx);
    assert!(matches!(
        worker.join().unwrap(),
        Err(ImportError::Cancelled)
    ));
    assert!(dropped.load(Ordering::Acquire));
}
#[test]
fn changed_open_file_never_reports_success_after_staging_started() {
    let fixture = Fixture::new(&xml(1000));
    let path = fixture.0.clone();
    let (tx, mut rx) = mpsc::channel(1);
    let worker = thread::spawn(move || {
        parse_file(
            path,
            mapping(),
            config(),
            Arc::new(AtomicBool::new(false)),
            tx,
        )
    });
    loop {
        if matches!(rx.blocking_recv(), Some(Batch::Rows { .. })) {
            break;
        }
    }
    use std::io::Write;
    fs::OpenOptions::new()
        .append(true)
        .open(&fixture.0)
        .unwrap()
        .write_all(b"incomplete replacement tail")
        .unwrap();
    while rx.blocking_recv().is_some() {}
    assert!(matches!(
        worker.join().unwrap(),
        Err(ImportError::FileChanged)
    ));
}
#[test]
fn capacity_charges_retained_string_allocation_and_rejects_invalid_queue_limits() {
    let mut row = XmltvProgramme {
        epg_id: 17,
        start_time: Utc::now(),
        end_time: Utc::now(),
        title: String::with_capacity(1024 * 1024),
        sub_title: None,
        description: None,
        tvg_id: "mapped".into(),
        custom_properties: None,
    };
    row.title.push('x');
    assert!(row_charge(&row).unwrap() >= 1024 * 1024);
    for config in [
        ImportConfig {
            max_jobs: 0,
            ..ImportConfig::default()
        },
        ImportConfig {
            queue_batches: 5,
            ..ImportConfig::default()
        },
        ImportConfig {
            batch_rows: 2501,
            ..ImportConfig::default()
        },
        ImportConfig {
            max_file_bytes: 4 * 1024 * 1024 * 1024,
            ..ImportConfig::default()
        },
    ] {
        assert!(config.validate().is_err());
    }
}

#[test]
fn progress_boundary_follows_all_five_thousand_queued_rows() {
    let fixture = Fixture::new(&xml(5001));
    let (tx, mut rx) = mpsc::channel(2);
    let path = fixture.0.clone();
    let worker = thread::spawn(move || {
        parse_file(
            path,
            mapping(),
            ImportConfig {
                batch_rows: 2500,
                ..ImportConfig::default()
            },
            Arc::new(AtomicBool::new(false)),
            tx,
        )
    });
    assert!(matches!(rx.blocking_recv(), Some(Batch::Validated)));
    let mut count = 0;
    let mut progress = Vec::new();
    while let Some(batch) = rx.blocking_recv() {
        if let Batch::Rows {
            rows,
            progress: tick,
        } = batch
        {
            count += rows.len();
            if let Some(value) = tick {
                assert_eq!(value, count as u64);
                progress.push(value);
            }
        }
    }
    assert_eq!(count, 5001);
    assert_eq!(progress, [5000]);
    assert_eq!(
        worker.join().unwrap().unwrap().report.parsed_programmes,
        5001
    );
}
