//! Bounded local standard-M3U file/cache work, called only under retained worker ownership.
use crate::scanner_m3u::Error;
use dispatcharr_ingest_core::{Entry, Limits, Parser};
use serde_json::{json, Value};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};
pub(crate) const INPUT_BYTES: u64 = 16384;
const CACHE_BYTES: u64 = 131072;
#[derive(Clone)]
pub(crate) struct Catalog {
    pub entries: Vec<Entry>,
    pub groups: Vec<String>,
    pub raw: Vec<u8>,
}
fn printable(v: &str, max: usize) -> bool {
    v.len() <= max && v.bytes().all(|b| (32..=126).contains(&b))
}
fn quote(v: &str) -> String {
    serde_json::to_string(v).expect("bounded JSON string")
}
fn attrs_order(header: &str, entry: &Entry) -> Result<Vec<String>, Error> {
    let mut order = Vec::new();
    let bytes = header.as_bytes();
    let mut at = 0;
    while at < bytes.len() {
        let Some(delta) = header[at..].find('=') else {
            break;
        };
        let eq = at + delta;
        let start = header[..eq].rfind(char::is_whitespace).map_or(0, |v| v + 1);
        let key = &header[start..eq];
        let q = *bytes.get(eq + 1).ok_or(Error::Unsupported)?;
        if !matches!(q, b'\'' | b'"') {
            return Err(Error::Unsupported);
        }
        let end = header[eq + 2..]
            .find(char::from(q))
            .map(|v| v + eq + 2)
            .ok_or(Error::Unsupported)?;
        if !entry.attributes.contains_key(key) {
            return Err(Error::Unsupported);
        }
        if !order.iter().any(|v| v == key) {
            order.push(key.to_owned());
        }
        at = end + 1;
    }
    if order.len() != entry.attributes.len() {
        return Err(Error::Unsupported);
    }
    Ok(order)
}
fn encode(entries: &[Entry], orders: &[Vec<String>], groups: &[String]) -> Result<Vec<u8>, Error> {
    let mut items = Vec::new();
    for (entry, order) in entries.iter().zip(orders) {
        let attrs = order
            .iter()
            .map(|key| format!("{}: {}", quote(key), quote(&entry.attributes[key])))
            .collect::<Vec<_>>()
            .join(", ");
        items.push(format!(
            "{{\"attributes\": {{{attrs}}}, \"display_name\": {}, \"name\": {}, \"url\": {}}}",
            quote(&entry.display_name),
            quote(&entry.name),
            quote(entry.url.as_deref().ok_or(Error::Unsupported)?)
        ));
    }
    let groups = groups
        .iter()
        .map(|v| format!("{}: {{}}", quote(v)))
        .collect::<Vec<_>>()
        .join(", ");
    let bytes = format!(
        "{{\"extinf_data\": [{}], \"groups\": {{{groups}}}}}",
        items.join(", ")
    )
    .into_bytes();
    if bytes.len() > CACHE_BYTES as usize {
        return Err(Error::Capacity);
    }
    Ok(bytes)
}
fn regular(path: &Path, limit: u64) -> Result<fs::File, Error> {
    use std::os::unix::fs::MetadataExt;
    let m = fs::symlink_metadata(path).map_err(|_| Error::Operational)?;
    if !m.is_file() || m.file_type().is_symlink() || m.nlink() != 1 || m.len() > limit {
        return Err(Error::Unsupported);
    }
    let f = fs::File::open(path).map_err(|_| Error::Operational)?;
    let opened = f.metadata().map_err(|_| Error::Operational)?;
    if m.dev() != opened.dev()
        || m.ino() != opened.ino()
        || m.len() != opened.len()
        || m.mtime() != opened.mtime()
        || m.mtime_nsec() != opened.mtime_nsec()
    {
        return Err(Error::Operational);
    }
    Ok(f)
}
pub(crate) enum InputProbe {
    Present(Catalog),
    Missing,
}
/// Only ENOENT for an exact absent leaf under an existing owned directory.
/// No bytes are read from an unexpectedly appearing/replaced path. General
/// concurrent filesystem mutation remains outside the admitted local domain.
pub(crate) fn confirm_missing_open(path: &Path) -> Result<std::io::Error, Error> {
    let parent = fs::symlink_metadata(path.parent().ok_or(Error::Unsupported)?)
        .map_err(|_| Error::Unsupported)?;
    if !parent.is_dir() || parent.file_type().is_symlink() {
        return Err(Error::Unsupported);
    }
    match fs::symlink_metadata(path) {
        Err(error) if error.raw_os_error() == Some(2) => {}
        _ => return Err(Error::Operational),
    }
    match fs::File::open(path) {
        Err(error) if error.raw_os_error() == Some(2) => Ok(error),
        _ => Err(Error::Operational),
    }
}
pub(crate) fn probe_input(path: &Path) -> Result<InputProbe, Error> {
    match fs::symlink_metadata(path) {
        Err(error) if error.raw_os_error() == Some(2) => {
            confirm_missing_open(path)?;
            Ok(InputProbe::Missing)
        }
        Err(_) => Err(Error::Operational),
        Ok(_) => input(path).map(InputProbe::Present),
    }
}
pub(crate) fn input(path: &Path) -> Result<Catalog, Error> {
    let file = regular(path, INPUT_BYTES)?;
    let mut bytes = Vec::new();
    file.take(INPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Operational)?;
    if bytes.len() > INPUT_BYTES as usize || !bytes.is_ascii() {
        return Err(Error::Unsupported);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| Error::Unsupported)?;
    if !text.starts_with("#EXTM3U\n") {
        return Err(Error::Unsupported);
    }
    let mut parser = Parser::new(Limits {
        max_line_bytes: 8192,
        max_entry_bytes: 8192,
        max_attributes: 2,
        max_vlc_options: 1,
    })
    .map_err(|_| Error::Unsupported)?;
    let mut entries = Vec::new();
    let mut groups = vec!["Default Group".to_owned()];
    let mut orders = Vec::new();
    let mut header = "";
    for line in text.lines() {
        if line.starts_with("#EXTINF") {
            header = line;
        } else if !(line.is_empty()
            || line == "#EXTM3U"
            || line.starts_with("http://")
            || line.starts_with("https://"))
        {
            return Err(Error::Unsupported);
        }
        if let Some(entry) = parser.feed(line).map_err(|_| Error::Unsupported)? {
            if entries.len() >= 8
                || !printable(&entry.name, 512)
                || !printable(&entry.display_name, 512)
                || !entry.url.as_deref().is_some_and(|v| printable(v, 4096))
                || entry.vlc_opts.is_some()
                || entry.attributes.iter().any(|(k, v)| {
                    !matches!(k.as_str(), "tvg-id" | "group-title") || !printable(v, 512)
                })
            {
                return Err(Error::Unsupported);
            }
            orders.push(attrs_order(header, &entry)?);
            if let Some(group) = entry
                .attributes
                .get("group-title")
                .filter(|v| !v.is_empty())
            {
                if group.len() > 128 {
                    return Err(Error::Unsupported);
                }
                if !groups.contains(group) {
                    groups.push(group.clone());
                }
            }
            entries.push(entry);
        }
    }
    parser.finish();
    let raw = encode(&entries, &orders, &groups)?;
    Ok(Catalog {
        entries,
        groups,
        raw,
    })
}
pub(crate) fn cached(path: &Path) -> Result<Option<Catalog>, Error> {
    if !path.try_exists().map_err(|_| Error::Operational)? {
        return Ok(None);
    }
    let f = regular(path, CACHE_BYTES)?;
    let mut raw = Vec::new();
    f.take(CACHE_BYTES + 1)
        .read_to_end(&mut raw)
        .map_err(|_| Error::Operational)?;
    if raw.len() > CACHE_BYTES as usize {
        return Err(Error::Capacity);
    }
    let value: Value = serde_json::from_slice(&raw).map_err(|_| Error::Unsupported)?;
    if value.as_object().is_none_or(|v| v.len() != 2)
        || value["extinf_data"].as_array().is_none_or(|v| v.len() > 8)
        || value["groups"]
            .as_object()
            .is_none_or(|v| v.len() > 9 || v.values().any(|v| *v != json!({})))
    {
        return Err(Error::Unsupported);
    }
    if value["extinf_data"].as_array().unwrap().iter().any(|v| {
        v.as_object().is_none_or(|m| {
            m.len() != 4
                || !["attributes", "display_name", "name", "url"]
                    .iter()
                    .all(|k| m.contains_key(*k))
        })
    }) || value["groups"]
        .as_object()
        .unwrap()
        .keys()
        .any(|v| !printable(v, 128))
    {
        return Err(Error::Unsupported);
    }
    let entries: Vec<Entry> =
        serde_json::from_value(value["extinf_data"].clone()).map_err(|_| Error::Unsupported)?;
    if entries.iter().any(|e| {
        !printable(&e.name, 512)
            || !printable(&e.display_name, 512)
            || !e.url.as_deref().is_some_and(|v| printable(v, 4096))
            || e.vlc_opts.is_some()
            || e.attributes
                .iter()
                .any(|(k, v)| !matches!(k.as_str(), "tvg-id" | "group-title") || !printable(v, 512))
    }) {
        return Err(Error::Unsupported);
    }
    // Cache JSON order is retained in raw bytes. SQL group creation only uses a
    // freshly parsed input catalog; cached groups here provide lookup names.
    let groups = value["groups"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    Ok(Some(Catalog {
        entries,
        groups,
        raw,
    }))
}
pub(crate) fn store(path: PathBuf, raw: &[u8]) -> Result<(), Error> {
    let parent = path.parent().ok_or(Error::Unsupported)?;
    fs::create_dir_all(parent).map_err(|_| Error::Operational)?;
    if path.try_exists().map_err(|_| Error::Operational)? {
        use std::os::unix::fs::MetadataExt;
        let current = fs::symlink_metadata(&path).map_err(|_| Error::Operational)?;
        if !current.is_file() || current.file_type().is_symlink() || current.nlink() != 1 {
            return Err(Error::Unsupported);
        }
    }
    let mut f = fs::File::create(path).map_err(|_| Error::Operational)?;
    f.write_all(raw).map_err(|_| Error::Operational)?;
    f.flush().map_err(|_| Error::Operational)?;
    drop(f);
    Ok(())
}
pub(crate) fn remove(path: &Path) -> Result<(), Error> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(Error::Operational),
    }
}
pub(crate) fn result(catalog: &Catalog) -> Value {
    let mut groups = serde_json::Map::new();
    for group in &catalog.groups {
        groups.insert(group.clone(), json!({}));
    }
    json!([catalog.entries, groups])
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_owned_leaf_is_distinct_from_missing_root_present_file_and_symlink() {
        let root =
            std::env::temp_dir().join(format!("rust-owned-m3u-missing-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let path = root.join("absent.m3u");
        assert!(matches!(probe_input(&path), Ok(InputProbe::Missing)));
        assert_eq!(confirm_missing_open(&path).unwrap().raw_os_error(), Some(2));
        fs::write(&path, "#EXTM3U\n").unwrap();
        assert!(confirm_missing_open(&path).is_err());
        assert!(matches!(probe_input(&path), Ok(InputProbe::Present(_))));
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(root.join("missing-target"), &path).unwrap();
        assert!(confirm_missing_open(&path).is_err());
        assert!(probe_input(&path).is_err());
        fs::remove_dir_all(&root).unwrap();
        assert!(probe_input(&path).is_err());
    }
    #[test]
    fn one_entry_exact_source_cache_bytes_and_domain_negatives() {
        let root =
            std::env::temp_dir().join(format!("rust-owned-m3u-files-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let p = root.join("input.m3u");
        let text="#EXTM3U\n#EXTINF:-1 tvg-id=\"owned.channel\" group-title=\"Owned Group\",Owned stream\nhttp://127.0.0.1:9/owned-never-played.ts\n";
        fs::write(&p, text).unwrap();
        let value = input(&p).unwrap();
        assert_eq!(value.entries.len(), 1);
        assert_eq!(
            std::str::from_utf8(&value.raw).unwrap(),
            r#"{"extinf_data": [{"attributes": {"tvg-id": "owned.channel", "group-title": "Owned Group"}, "display_name": "Owned stream", "name": "Owned stream", "url": "http://127.0.0.1:9/owned-never-played.ts"}], "groups": {"Default Group": {}, "Owned Group": {}}}"#
        );
        let cache = root.join("1.json");
        store(cache.clone(), &value.raw).unwrap();
        assert_eq!(cached(&cache).unwrap().unwrap().entries, value.entries);
        fs::write(&p, text.replace("Owned stream", "Owned télévision")).unwrap();
        assert!(input(&p).is_err());
        fs::write(&p, "#EXTM3U\n#EXTVLCOPT:http-user-agent=unsupported\n").unwrap();
        assert!(input(&p).is_err());
        fs::write(&p, vec![b'x'; INPUT_BYTES as usize + 1]).unwrap();
        assert!(input(&p).is_err());
        fs::remove_file(&p).unwrap();
        std::os::unix::fs::symlink(&cache, &p).unwrap();
        assert!(input(&p).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
