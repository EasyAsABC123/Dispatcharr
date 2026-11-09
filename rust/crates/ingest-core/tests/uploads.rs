use dispatcharr_ingest_core::{with_uploaded_playlist, UploadError, UploadLimits};
use std::{
    fs,
    io::Write,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "dispatcharr-upload-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn file(&self, suffix: &str, bytes: &[u8]) -> PathBuf {
        let path = self.0.join(format!("fixture{suffix}"));
        fs::write(&path, bytes).unwrap();
        path
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn consume(reader: &mut dyn std::io::BufRead) -> Result<Vec<u8>, UploadError> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    Ok(bytes)
}

#[test]
fn gzip_expansion_and_plain_input_are_limited_at_the_decoded_boundary() {
    let fixture = Fixture::new();
    let bytes = vec![b'x'; 65536];
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&bytes).unwrap();
    let path = fixture.file(".gz", &encoder.finish().unwrap());
    let mut limits = UploadLimits {
        decoded_bytes: 65535,
        ..UploadLimits::default()
    };
    assert!(with_uploaded_playlist(&path, limits, consume)
        .unwrap_err()
        .to_string()
        .contains("decoded-byte"));
    limits.decoded_bytes = 65536;
    assert_eq!(
        with_uploaded_playlist(&path, limits, consume).unwrap(),
        bytes
    );
    let path = fixture.file(".m3u", &bytes);
    limits.source_bytes = 65535;
    assert!(matches!(
        with_uploaded_playlist(&path, limits, consume),
        Err(UploadError::Capacity("source-byte"))
    ));
}

#[test]
fn zip_metadata_limits_apply_before_opening_members() {
    let fixture = Fixture::new();
    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = zip::ZipWriter::new(cursor);
    for name in ["first.m3u", "second.m3u"] {
        writer
            .start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        writer.write_all(b"#EXTM3U").unwrap();
    }
    let bytes = writer.finish().unwrap().into_inner();
    let path = fixture.file(".zip", &bytes);
    let limits = UploadLimits {
        zip_entries: 1,
        ..UploadLimits::default()
    };
    assert!(matches!(
        with_uploaded_playlist(&path, limits, consume),
        Err(UploadError::Capacity("ZIP entry-count"))
    ));
    let limits = UploadLimits {
        zip_directory_bytes: 1,
        ..UploadLimits::default()
    };
    assert!(matches!(
        with_uploaded_playlist(&path, limits, consume),
        Err(UploadError::Capacity("ZIP directory-byte"))
    ));
    assert_eq!(
        with_uploaded_playlist(&path, UploadLimits::default(), consume).unwrap(),
        b"#EXTM3U"
    );
    let mut forged = bytes.clone();
    let eocd = forged
        .windows(4)
        .rposition(|bytes| bytes == b"PK\x05\x06")
        .unwrap();
    forged[eocd + 8..eocd + 12].copy_from_slice(&[1, 0, 1, 0]);
    let path = fixture.file(".zip", &forged);
    let limits = UploadLimits {
        zip_entries: 1,
        ..UploadLimits::default()
    };
    assert!(matches!(
        with_uploaded_playlist(&path, limits, consume),
        Err(UploadError::Capacity("ZIP entry-count"))
    ));
}

#[test]
fn xz_dictionary_memory_limit_is_not_swallowed_as_trailing_garbage() {
    let fixture = Fixture::new();
    let mut encoder = xz2::write::XzEncoder::new(Vec::new(), 6);
    encoder.write_all(b"#EXTM3U\n").unwrap();
    let bytes = encoder.finish().unwrap();
    let path = fixture.file(".xz", &bytes);
    let limits = UploadLimits {
        xz_memory_kib: 1024,
        ..UploadLimits::default()
    };
    assert!(with_uploaded_playlist(&path, limits, consume).is_err());
    assert_eq!(
        with_uploaded_playlist(&path, UploadLimits::default(), consume).unwrap(),
        b"#EXTM3U\n"
    );
}
