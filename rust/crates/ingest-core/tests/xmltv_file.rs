//! Same-descriptor preparation and footer checks preserve original Source rules.
use dispatcharr_ingest_core::{xmltv_file_handle_is_complete, xmltv_file_is_complete, XmltvReader};
use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
};
struct Fixture(PathBuf);
impl Fixture {
    fn new(bytes: &[u8]) -> Self {
        let path = std::env::temp_dir().join(format!(
            "dispatcharr-xmltv-open-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&path, bytes).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
#[test]
fn footer_and_prefix_use_original_open_file_after_atomic_path_replacement() {
    let fixture = Fixture::new(b"<?xml version=\"1.0\"?><tv><channel id=\"original\"/></tv>\n");
    let mut opened = File::open(&fixture.0).unwrap();
    opened.seek(SeekFrom::Start(7)).unwrap();
    assert!(xmltv_file_handle_is_complete(&mut opened));
    assert_eq!(opened.stream_position().unwrap(), 7);
    let replacement = Fixture::new(b"<tv><channel id=\"replacement\"/>");
    fs::rename(&replacement.0, &fixture.0).unwrap();
    assert!(!xmltv_file_is_complete(&fixture.0));
    assert!(xmltv_file_handle_is_complete(&mut opened));
    let mut prepared = Vec::new();
    XmltvReader::from_file(opened)
        .unwrap()
        .read_to_end(&mut prepared)
        .unwrap();
    assert!(prepared.starts_with(b"<?xml version=\"1.0\"?>\n<!DOCTYPE"));
    assert!(prepared.ends_with(b"<tv><channel id=\"original\"/></tv>\n"));
    assert!(!prepared
        .windows(b"replacement".len())
        .any(|w| w == b"replacement"));
}
#[test]
fn existing_doctype_is_preserved_and_caller_offset_is_ignored() {
    let bytes = b"<!DOCTYPE tv []><tv><programme/></tv>";
    let fixture = Fixture::new(bytes);
    let mut file = File::open(&fixture.0).unwrap();
    file.seek(SeekFrom::End(0)).unwrap();
    let mut prepared = Vec::new();
    XmltvReader::from_file(file)
        .unwrap()
        .read_to_end(&mut prepared)
        .unwrap();
    assert_eq!(prepared, bytes);
}
#[test]
fn suffix_is_literal_and_io_failure_remains_false() {
    for (bytes, expected) in [
        (b"<tv></tv> \r\n".as_slice(), true),
        (b"<tv></TV>".as_slice(), false),
        (b"".as_slice(), false),
        (b"<tv></tv>\x00".as_slice(), false),
    ] {
        let fixture = Fixture::new(bytes);
        let mut file = File::open(&fixture.0).unwrap();
        assert_eq!(xmltv_file_handle_is_complete(&mut file), expected);
        assert_eq!(xmltv_file_is_complete(&fixture.0), expected);
    }
    assert!(!xmltv_file_is_complete(std::path::Path::new(
        "/tmp/no-such-dispatcharr-xmltv-fixture"
    )));
    let mut directory = File::open(std::env::temp_dir()).unwrap();
    assert!(!xmltv_file_handle_is_complete(&mut directory));
}
