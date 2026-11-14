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
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
const BYTES: u64 = 4 * 1024 * 1024;
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
        if !before.is_file() || before.len() > BYTES || epg_id <= 0 || tvg_id.len() > 1024 {
            return Err(Error::Capacity);
        }
        let original = identity(&before);
        if !xmltv_file_handle_is_complete(&mut file) {
            return Err(Error::Incomplete);
        }
        let monitor = file.try_clone()?;
        let reader = XmltvReader::from_file(file)?.take(BYTES + 65536 + 1);
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
                if result.len() >= 4096 || bytes > BYTES as usize {
                    return Err(Error::Capacity);
                }
                result.push(row);
            }
            Ok(())
        })
        .map_err(|e| Error::Parser(format!("{e:?}")))?;
        if stats.prepared_input_bytes > BYTES + 65536 {
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
