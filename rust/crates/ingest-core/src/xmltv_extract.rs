//! XMLTV format and owned-file extraction, ported from original Dispatcharr.
//! Original authors retain credit. Fetch/job integration is a separate gate.
use crate::{
    uploads::{check_zip_directory, SourceLzma},
    UploadError, UploadLimits,
};
use std::{
    fs::{self, File},
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct XmltvFormat {
    pub kind: &'static str,
    pub compressed: bool,
    pub extension: &'static str,
}
fn format(kind: &'static str, compressed: bool, extension: &'static str) -> XmltvFormat {
    XmltvFormat {
        kind,
        compressed,
        extension,
    }
}
/// The caller supplies the separately validated original `mimetypes.guess_type`
/// result. This component does not claim host MIME-table or fetch-job parity.
pub fn detect_xmltv_format(
    path: Option<&str>,
    content: Option<&[u8]>,
    mime_hint: Option<&str>,
) -> XmltvFormat {
    if let Some(content) = content.filter(|content| !content.is_empty()) {
        let header = &content[..content.len().min(20)];
        if header.starts_with(b"\x1f\x8b") {
            return format("gzip", true, ".gz");
        }
        if header.starts_with(b"PK") {
            return format("zip", true, ".zip");
        }
        if header.starts_with(b"\xfd7zXZ\0") {
            return format("xz", true, ".xz");
        }
        if header.len() >= 5
            && (header.windows(5).any(|part| part == b"<?xml")
                || header.windows(4).any(|part| part == b"<tv>"))
        {
            return format("xml", false, ".xml");
        }
    }
    if let Some(path) = path.filter(|path| !path.is_empty()) {
        // Only the literal ASCII suffix characters below can match. Source
        // Unicode lowercase does not map another character to any of them.
        let path = path.to_ascii_lowercase();
        if path.ends_with(".gz") || path.ends_with(".gzip") {
            return format("gzip", true, ".gz");
        }
        if path.ends_with(".zip") {
            return format("zip", true, ".zip");
        }
        if path.ends_with(".xz") {
            return format("xz", true, ".xz");
        }
        if path.ends_with(".xml") {
            return format("xml", false, ".xml");
        }
        match mime_hint {
            Some("application/gzip" | "application/x-gzip") => return format("gzip", true, ".gz"),
            Some("application/zip") => return format("zip", true, ".zip"),
            Some("application/x-xz") => return format("xz", true, ".xz"),
            Some("application/xml" | "text/xml") => return format("xml", false, ".xml"),
            _ => {}
        }
    }
    format("unknown", false, ".tmp")
}

#[derive(Debug)]
pub enum XmltvExtractError {
    Io(io::Error),
    Archive(UploadError),
    Unsupported,
    MissingXml,
    Cancelled,
    Capacity,
}
impl From<io::Error> for XmltvExtractError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<UploadError> for XmltvExtractError {
    fn from(value: UploadError) -> Self {
        Self::Archive(value)
    }
}
impl From<zip::result::ZipError> for XmltvExtractError {
    fn from(value: zip::result::ZipError) -> Self {
        Self::Archive(value.into())
    }
}
impl std::fmt::Display for XmltvExtractError {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(out, "{self:?}")
    }
}
impl std::error::Error for XmltvExtractError {}
fn check_cancel(cancel: &AtomicBool) -> Result<(), XmltvExtractError> {
    if cancel.load(Ordering::Acquire) {
        Err(XmltvExtractError::Cancelled)
    } else {
        Ok(())
    }
}
struct Work {
    used: u64,
    maximum: u64,
}
impl Work {
    fn charge(&mut self, count: usize) -> Result<(), XmltvExtractError> {
        self.used = self
            .used
            .checked_add(count as u64)
            .filter(|total| *total <= self.maximum)
            .ok_or(XmltvExtractError::Capacity)?;
        Ok(())
    }
}
// Python binary read(n) fills its requested chunk or reaches EOF. On an error
// it raises without returning the partially filled chunk. Preserve that boundary
// so a bad trailer leaves exactly the same prior complete64KiB output blocks.
fn fill(
    reader: &mut dyn Read,
    out: &mut [u8],
    cancel: &AtomicBool,
    work: &mut Work,
) -> Result<usize, XmltvExtractError> {
    let mut used = 0;
    while used < out.len() {
        check_cancel(cancel)?;
        match reader.read(&mut out[used..]) {
            Ok(0) => break,
            Ok(count) if count <= out.len() - used => {
                work.charge(count)?;
                used += count;
            }
            Ok(_) => return Err(XmltvExtractError::Capacity),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(used)
}
fn copy(
    reader: &mut dyn Read,
    out: &mut dyn Write,
    maximum: u64,
    cancel: &AtomicBool,
    work: &mut Work,
) -> Result<u64, XmltvExtractError> {
    let mut buffer = [0u8; 65536];
    let mut written = 0u64;
    loop {
        let count = fill(reader, &mut buffer, cancel, work)?;
        if count == 0 {
            return Ok(written);
        }
        written = written
            .checked_add(count as u64)
            .filter(|count| *count <= maximum)
            .ok_or(XmltvExtractError::Capacity)?;
        out.write_all(&buffer[..count])?;
    }
}
fn decoder(
    mut file: File,
    kind: &str,
    limits: UploadLimits,
) -> Result<Box<dyn Read>, XmltvExtractError> {
    file.seek(SeekFrom::Start(0))?;
    if kind == "gzip" {
        if file.metadata()?.len() == 0 {
            return Ok(Box::new(file));
        }
        Ok(Box::new(flate2::read::MultiGzDecoder::new(file)))
    } else {
        Ok(Box::new(SourceLzma::new(
            BufReader::with_capacity(8192, file),
            u64::from(limits.xz_memory_kib) * 1024,
        )?))
    }
}

/// Low-level original helper semantics for an EXPLICIT, CALLER-OWNED output.
/// It removes an existing output before detection and may leave partial output
/// on a bad trailer, as Source does. A refresh job must use owned incoming paths
/// and validate/promote separately; never pass its previous good guide here.
/// Archive member names are read as keys and never extracted to filesystem paths.
/// Caller must retain blocking-worker/job admission through the actual return.
pub fn extract_xmltv_owned(
    source: &Path,
    output: &Path,
    delete_original: bool,
    limits: UploadLimits,
    cancel: &AtomicBool,
) -> Result<u64, XmltvExtractError> {
    check_cancel(cancel)?;
    if limits.source_bytes == 0 || limits.decoded_bytes == 0 || limits.xz_memory_kib == 0 {
        return Err(XmltvExtractError::Capacity);
    }
    if output.exists() {
        let _ = fs::remove_file(output);
    }
    let mut file = File::open(source)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > limits.source_bytes {
        return Err(XmltvExtractError::Capacity);
    }
    // Bound total expanded work across every inspected member, including
    // corrupt members and the selected member's replay. A per-member quota
    // alone could permit thousands of near-limit failing decompressions.
    let mut work = Work {
        used: 0,
        maximum: limits
            .decoded_bytes
            .checked_mul(2)
            .and_then(|size| size.checked_add(8192))
            .ok_or(XmltvExtractError::Capacity)?,
    };
    let mut head = [0u8; 4096];
    let length = fill(&mut file, &mut head, cancel, &mut work)?;
    file.seek(SeekFrom::Start(0))?;
    let name = source.to_string_lossy();
    let format = detect_xmltv_format(Some(&name), Some(&head[..length]), None);
    let result = match format.kind {
        "gzip" | "xz" => {
            let mut sample = decoder(file.try_clone()?, format.kind, limits)?;
            let mut prefix = [0u8; 4096];
            let _ = fill(sample.as_mut(), &mut prefix, cancel, &mut work)?;
            drop(sample);
            file.seek(SeekFrom::Start(0))?;
            let mut reader = decoder(file, format.kind, limits)?;
            let mut destination = File::create(output)?;
            copy(
                reader.as_mut(),
                &mut destination,
                limits.decoded_bytes,
                cancel,
                &mut work,
            )
        }
        "zip" => {
            check_zip_directory(&mut file, limits)?;
            let mut archive = zip::ZipArchive::new(file)?;
            let mut names = Vec::new();
            for name in archive.file_names() {
                names.push(name?.into_owned());
            }
            let mut selected = names
                .iter()
                .find(|name| name.to_ascii_lowercase().ends_with(".xml"))
                .cloned();
            if selected.is_none() {
                let mut inspected = 0u64;
                for name in &names {
                    check_cancel(cancel)?;
                    if name.ends_with('/') {
                        continue;
                    }
                    let mut member = match archive.by_name(name) {
                        Ok(member) => member,
                        Err(_) => continue,
                    };
                    let mut prefix = [0u8; 4096];
                    let length = match fill(&mut member, &mut prefix, cancel, &mut work) {
                        Ok(length) => length,
                        Err(XmltvExtractError::Cancelled) => {
                            return Err(XmltvExtractError::Cancelled)
                        }
                        Err(XmltvExtractError::Capacity) => {
                            return Err(XmltvExtractError::Capacity)
                        }
                        Err(_) => continue,
                    };
                    // Source ZipFile.read(name,4096) still reads/validates the
                    // entire unencrypted member. Stream the remainder instead
                    // of buffering it; corrupted candidates must be skipped.
                    let mut sink = io::sink();
                    let rest = match copy(
                        &mut member,
                        &mut sink,
                        limits.decoded_bytes,
                        cancel,
                        &mut work,
                    ) {
                        Ok(rest) => rest,
                        Err(XmltvExtractError::Cancelled) => {
                            return Err(XmltvExtractError::Cancelled)
                        }
                        Err(XmltvExtractError::Capacity) => {
                            return Err(XmltvExtractError::Capacity)
                        }
                        Err(_) => continue,
                    };
                    inspected = inspected
                        .checked_add(length as u64)
                        .and_then(|count| count.checked_add(rest))
                        .filter(|count| *count <= limits.decoded_bytes)
                        .ok_or(XmltvExtractError::Capacity)?;
                    if detect_xmltv_format(None, Some(&prefix[..length]), None).kind == "xml" {
                        selected = Some(name.clone());
                        break;
                    }
                }
            }
            let selected = selected.ok_or(XmltvExtractError::MissingXml)?;
            let mut member = archive.by_name(&selected)?;
            let mut destination = File::create(output)?;
            copy(
                &mut member,
                &mut destination,
                limits.decoded_bytes,
                cancel,
                &mut work,
            )
        }
        _ => Err(XmltvExtractError::Unsupported),
    }?;
    check_cancel(cancel)?;
    if delete_original {
        let _ = fs::remove_file(source);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Cursor,
        sync::atomic::AtomicU64,
        time::{SystemTime, UNIX_EPOCH},
    };
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Directory(std::path::PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "dispatcharr-extract-test-{}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn gzip(path: &Path, data: &[u8]) {
        let mut writer = flate2::write::GzEncoder::new(
            File::create(path).unwrap(),
            flate2::Compression::default(),
        );
        writer.write_all(data).unwrap();
        writer.finish().unwrap();
    }
    #[test]
    fn cancelled_before_start_preserves_both_owned_files() {
        let dir = Directory::new();
        let source = dir.0.join("input.gz");
        let output = dir.0.join("output.xml");
        gzip(&source, b"<tv></tv>");
        fs::write(&output, b"previous").unwrap();
        assert!(matches!(
            extract_xmltv_owned(
                &source,
                &output,
                true,
                UploadLimits::default(),
                &AtomicBool::new(true)
            ),
            Err(XmltvExtractError::Cancelled)
        ));
        assert!(source.exists());
        assert_eq!(fs::read(output).unwrap(), b"previous");
    }
    #[test]
    fn expanded_limit_keeps_compressed_source_and_bounds_output() {
        let dir = Directory::new();
        let source = dir.0.join("input.gz");
        let output = dir.0.join("output.xml");
        gzip(&source, &vec![b'x'; 200_000]);
        let limits = UploadLimits {
            decoded_bytes: 128,
            ..Default::default()
        };
        assert!(matches!(
            extract_xmltv_owned(&source, &output, true, limits, &AtomicBool::new(false)),
            Err(XmltvExtractError::Capacity)
        ));
        assert!(source.exists());
        assert!(output.metadata().map_or(true, |value| value.len() <= 128));
    }
    #[test]
    fn corrupt_extensionless_members_cannot_restart_expanded_work_budget() {
        let dir = Directory::new();
        let source = dir.0.join("input.zip");
        let output = dir.0.join("output.xml");
        let mut archive = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for index in 0..30 {
            archive
                .start_file(format!("bad-{index}.dat"), options)
                .unwrap();
            archive.write_all(&vec![b'x'; 5000]).unwrap();
        }
        archive.start_file("last.dat", options).unwrap();
        archive.write_all(b"<tv></tv>").unwrap();
        let mut bytes = archive.finish().unwrap().into_inner();
        let positions = bytes
            .windows(4)
            .enumerate()
            .filter_map(|(index, header)| (header == b"PK\x03\x04").then_some(index))
            .collect::<Vec<_>>();
        for offset in positions.into_iter().take(30) {
            let name = u16::from_le_bytes([bytes[offset + 26], bytes[offset + 27]]) as usize;
            let extra = u16::from_le_bytes([bytes[offset + 28], bytes[offset + 29]]) as usize;
            bytes[offset + 30 + name + extra] ^= 1;
        }
        fs::write(&source, bytes).unwrap();
        let limits = UploadLimits {
            decoded_bytes: 10_000,
            ..Default::default()
        };
        assert!(matches!(
            extract_xmltv_owned(&source, &output, true, limits, &AtomicBool::new(false)),
            Err(XmltvExtractError::Capacity)
        ));
        assert!(source.exists());
        assert!(!output.exists());
    }
    #[test]
    fn cancellation_during_partial_read_discards_uncompleted_block() {
        struct CancelAfterRead<'a>(&'a AtomicBool);
        impl Read for CancelAfterRead<'_> {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                out[0] = b'x';
                self.0.store(true, Ordering::Release);
                Ok(1)
            }
        }
        let flag = AtomicBool::new(false);
        let mut reader = CancelAfterRead(&flag);
        let mut output = Vec::new();
        let mut work = Work {
            used: 0,
            maximum: 100_000,
        };
        assert!(matches!(
            copy(&mut reader, &mut output, 100_000, &flag, &mut work),
            Err(XmltvExtractError::Cancelled)
        ));
        assert!(output.is_empty());
        assert_eq!(work.used, 1);
    }
}
