//! Draft XMLTV compression-format decision, copied from original Dispatcharr.
//! Original authors retain credit. Unregistered until actual original replay.

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
