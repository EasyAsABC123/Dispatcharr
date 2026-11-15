//! Reuses qualified XMLTV parser/row projection on an owned local descriptor.
//! Original Dispatcharr authors retain application/algorithm credit.
//! Does not acquire a task lease, ACK, fetch a provider or choose a path.
use dispatcharr_ingest_core::{
    project_xmltv_programme, xmltv_file_handle_is_complete, XmltvProgramme, XmltvReader,
};
use dispatcharr_xmltv_parser::parse_xmltv_reader;
use std::{
    fs::File,
    io::Read,
    os::unix::fs::MetadataExt,
    sync::{atomic::AtomicBool, Arc},
};
#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Capacity,
    Incomplete,
    Changed,
    Parser(String),
    Supervisor,
    Cancelled,
}
/// At most one queued page and one producer page; each has at most 256 rows.
/// Page bytes are flushed at 512KiB, plus ONE admitted parser node (4MiB).
/// The native parser independently bounds nodes/depth and feeds 32KiB at a time.
pub struct ProgrammeStream {
    receiver: tokio::sync::mpsc::Receiver<Vec<XmltvProgramme>>,
    producer: Option<tokio::task::JoinHandle<Result<u64, Error>>>,
    cancelled: Arc<AtomicBool>,
}
impl Drop for ProgrammeStream {
    fn drop(&mut self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Release);
        self.receiver.close();
        // The blocking producer owns File+caller until it actually exits. Drop
        // never reports that physical work as settled; finish must be awaited.
    }
}
impl ProgrammeStream {
    pub async fn next(&mut self) -> Option<Vec<XmltvProgramme>> {
        self.receiver.recv().await
    }
    /// Always call after EOF OR consumer SQL failure. Closing wakes a producer
    /// blocked on the one-page channel, then join fences actual parser/FD exit.
    pub async fn finish(mut self) -> Result<u64, Error> {
        if !self.receiver.is_closed() {
            self.cancelled
                .store(true, std::sync::atomic::Ordering::Release);
        }
        self.receiver.close();
        self.producer
            .take()
            .ok_or(Error::Supervisor)?
            .await
            .map_err(|_| Error::Supervisor)?
    }
}
pub fn stream(
    file: File,
    epg_id: i64,
    tvg_id: String,
    owner: Arc<dyn Send + Sync>,
) -> ProgrammeStream {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let cancelled = Arc::new(AtomicBool::new(false));
    let flag = cancelled.clone();
    let producer = tokio::task::spawn_blocking(move || {
        let _owner = owner;
        let mut file = file;
        let before = file.metadata()?;
        if !before.is_file() || before.len() > INPUT_BYTES || epg_id <= 0 || tvg_id.len() > 1024 {
            return Err(Error::Capacity);
        }
        let original = identity(&before);
        if !xmltv_file_handle_is_complete(&mut file) {
            return Err(Error::Incomplete);
        }
        let monitor = file.try_clone()?;
        let reader = XmltvReader::from_file(file)?.take(INPUT_BYTES + 65536 + 1);
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        let mut count = 0u64;
        let stats = parse_xmltv_reader(reader, &flag, |node| {
            if node.tag != "programme" || node.attrs.get("channel") != Some(&tvg_id) {
                return Ok::<_, Error>(());
            }
            // Same Source per-programme catch as the captured reader. Global
            // parser/FD/capacity errors never install a truncated staged guide.
            if let Ok(row) = project_xmltv_programme(&node, epg_id, 255) {
                bytes = bytes.saturating_add(
                    row.title.len()
                        + row.tvg_id.len()
                        + row.description.as_ref().map_or(0, String::len)
                        + row.sub_title.as_ref().map_or(0, String::len)
                        + row
                            .custom_properties
                            .as_ref()
                            .map_or(0, |v| v.to_string().len()),
                );
                rows.push(row);
                count += 1;
                if rows.len() == 256 || bytes >= 512 * 1024 {
                    sender
                        .blocking_send(std::mem::take(&mut rows))
                        .map_err(|_| Error::Cancelled)?;
                    bytes = 0;
                }
            }
            Ok(())
        })
        .map_err(|e| Error::Parser(format!("{e:?}")))?;
        if stats.prepared_input_bytes > INPUT_BYTES + 65536 {
            return Err(Error::Capacity);
        }
        if identity(&monitor.metadata()?) != original {
            return Err(Error::Changed);
        }
        if !rows.is_empty() {
            sender.blocking_send(rows).map_err(|_| Error::Cancelled)?;
        }
        Ok(count)
    });
    ProgrammeStream {
        receiver,
        producer: Some(producer),
        cancelled,
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
const INPUT_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const ROW_BYTES: usize = 4 * 1024 * 1024;
fn identity(m: &std::fs::Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
    (
        m.dev(),
        m.ino(),
        m.len(),
        m.mtime(),
        m.mtime_nsec(),
        m.ctime(),
        m.ctime_nsec(),
    )
}
/// Caller must supply a descriptor opened under its configured local root.
/// Its actual owner is moved into blocking work and survives observer drop.
pub async fn read(
    file: File,
    epg_id: i64,
    tvg_id: String,
    owner: Arc<dyn Send + Sync>,
) -> Result<Vec<XmltvProgramme>, Error> {
    tokio::task::spawn_blocking(move || {
        let _owner = owner;
        let mut file = file;
        let before = file.metadata()?;
        if !before.is_file() || before.len() > INPUT_BYTES || epg_id <= 0 || tvg_id.len() > 1024 {
            return Err(Error::Capacity);
        }
        let original = identity(&before);
        if !xmltv_file_handle_is_complete(&mut file) {
            return Err(Error::Incomplete);
        }
        let monitor = file.try_clone()?;
        let reader = XmltvReader::from_file(file)?.take(INPUT_BYTES + 65536 + 1);
        let mut result = Vec::new();
        let mut bytes = 0usize;
        let stats = parse_xmltv_reader(reader, &AtomicBool::new(false), |node| {
            if node.tag != "programme" || node.attrs.get("channel") != Some(&tvg_id) {
                return Ok::<_, Error>(());
            }
            // Source skips an invalid individual programme rather than erasing
            // valid siblings. Reuse existing Source-qualified projection.
            if let Ok(row) = project_xmltv_programme(&node, epg_id, 255) {
                bytes = bytes.saturating_add(
                    row.title.len()
                        + row.tvg_id.len()
                        + row.description.as_ref().map_or(0, String::len)
                        + row.sub_title.as_ref().map_or(0, String::len)
                        + row
                            .custom_properties
                            .as_ref()
                            .map_or(0, |x| x.to_string().len()),
                );
                if result.len() >= 4096 || bytes > ROW_BYTES {
                    return Err(Error::Capacity);
                }
                result.push(row);
            }
            Ok(())
        })
        .map_err(|e| Error::Parser(format!("{e:?}")))?;
        if stats.prepared_input_bytes > INPUT_BYTES + 65536 {
            return Err(Error::Capacity);
        }
        if identity(&monitor.metadata()?) != original {
            return Err(Error::Changed);
        }
        Ok(result)
    })
    .await
    .map_err(|_| Error::Supervisor)?
}

#[cfg(test)]
mod large_input_control {
    use super::*;
    use std::io::Write;
    #[tokio::test]
    async fn guide_larger_than_legacy_four_mib_is_streamed_while_only_target_rows_are_retained() {
        let path = std::env::temp_dir().join(format!("tvg-large-{}.xml", uuid_for_test()));
        let mut file = std::io::BufWriter::new(File::create(&path).unwrap());
        file.write_all(b"<?xml version=\"1.0\"?><tv>").unwrap();
        let ignored=b"<programme channel=\"other\" start=\"20300101100000 +0000\" stop=\"20300101103000 +0000\"><title>Not selected</title></programme>";
        for _ in 0..50000 {
            file.write_all(ignored).unwrap();
        }
        file.write_all(b"<programme channel=\"target\" start=\"20300101100000 +0000\" stop=\"20300101103000 +0000\"><title>Selected</title></programme></tv>").unwrap();
        file.flush().unwrap();
        drop(file);
        assert!(std::fs::metadata(&path).unwrap().len() > 4 * 1024 * 1024);
        let rows = read(File::open(&path).unwrap(), 7, "target".into(), Arc::new(()))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].title, "Selected");
        std::fs::remove_file(path).unwrap();
    }
    #[tokio::test]
    async fn selected_rows_cross_both_legacy_caps_without_a_whole_result_vector() {
        let path = std::env::temp_dir().join(format!("tvg-selected-{}.xml", uuid_for_test()));
        let mut file = std::io::BufWriter::new(File::create(&path).unwrap());
        file.write_all(b"<tv>").unwrap();
        let desc = "d".repeat(1024);
        for number in 0..6001 {
            write!(file,"<programme channel=\"target\" start=\"20300101100000 +0000\" stop=\"20300101103000 +0000\"><title>Programme {number}</title><desc>{desc}</desc></programme>").unwrap();
        }
        // The Source loop skips an individual programme with no stop time.
        file.write_all(b"<programme channel=\"target\" start=\"20300101100000 +0000\"><title>Invalid sibling</title></programme></tv>").unwrap();
        file.flush().unwrap();
        drop(file);
        let mut input = stream(File::open(&path).unwrap(), 7, "target".into(), Arc::new(()));
        let mut count = 0;
        let mut selected_bytes = 0;
        while let Some(page) = input.next().await {
            assert!(page.len() <= 256);
            for row in page {
                assert_eq!(row.title, format!("Programme {count}"));
                selected_bytes += row.description.unwrap().len();
                count += 1;
            }
        }
        assert_eq!(input.finish().await.unwrap(), 6001);
        assert_eq!(count, 6001);
        assert!(selected_bytes > 4 * 1024 * 1024);
        std::fs::remove_file(path).unwrap();
    }
    #[tokio::test]
    async fn consumer_stop_joins_the_actual_bounded_channel_producer_and_owner() {
        struct Lifetime(Arc<AtomicBool>);
        impl Drop for Lifetime {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::Release);
            }
        }
        let path = std::env::temp_dir().join(format!("tvg-stop-{}.xml", uuid_for_test()));
        let mut file = std::io::BufWriter::new(File::create(&path).unwrap());
        file.write_all(b"<tv>").unwrap();
        for _ in 0..6001 {
            file.write_all(b"<programme channel=\"target\" start=\"20300101100000 +0000\" stop=\"20300101103000 +0000\"><title>Selected</title></programme>").unwrap();
        }
        file.write_all(b"</tv>").unwrap();
        file.flush().unwrap();
        drop(file);
        let retired = Arc::new(AtomicBool::new(false));
        let mut input = stream(
            File::open(&path).unwrap(),
            7,
            "target".into(),
            Arc::new(Lifetime(retired.clone())),
        );
        assert!(input.next().await.is_some());
        assert!(!retired.load(std::sync::atomic::Ordering::Acquire));
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), input.finish())
                .await
                .unwrap()
                .is_err()
        );
        assert!(retired.load(std::sync::atomic::Ordering::Acquire));
        std::fs::remove_file(path).unwrap();
    }
    fn uuid_for_test() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }
}
