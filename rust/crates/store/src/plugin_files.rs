//! Bounded filesystem portion of Dispatcharr's original plugin loader/installer.
//! Original Dispatcharr authors retain all implementation/algorithm credit.
//! This module NEVER imports or executes Python. Enabled Python requires a host.
use crate::StoreError;
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};
const FILES: usize = 2000;
const BYTES: u64 = 200 * 1024 * 1024;
const MANIFEST: usize = 65536;
const MUTATION_LOCK: &str = ".plugin-mutation.lock";
fn unsupported() -> StoreError {
    StoreError::Unsupported("Plugin filesystem outside bounded Source domain")
}
fn io(_: std::io::Error) -> StoreError {
    unsupported()
}
fn bad(text: &str) -> Value {
    json!({"success":false,"error":text})
}
struct Staging(PathBuf);
impl Drop for Staging {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn stage(root: &Path) -> Result<Staging, StoreError> {
    let base = root.join(".staging");
    match fs::create_dir_all(&base) {
        Ok(()) => {}
        Err(e) if matches!(e.kind(), std::io::ErrorKind::PermissionDenied) => {
            return stage_at(&std::env::temp_dir())
        }
        Err(e) => return Err(io(e)),
    }
    stage_at(&base)
}
fn stage_at(base: &Path) -> Result<Staging, StoreError> {
    let path = base.join(format!("plugin_import_{}", uuid::Uuid::new_v4().simple()));
    fs::create_dir(&path).map_err(io)?;
    Ok(Staging(path))
}
fn normalized(name: &str) -> Result<PathBuf, StoreError> {
    let mut out = PathBuf::new();
    if name.contains('\0') || name.starts_with('/') {
        return Err(unsupported());
    }
    for part in Path::new(name).components() {
        match part {
            Component::Normal(v) => out.push(v),
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    return Err(unsupported());
                }
            }
            _ => return Err(unsupported()),
        }
    }
    if out.to_str().is_none_or(|p| p.starts_with("..")) {
        return Err(unsupported());
    }
    Ok(out)
}
pub(super) fn sanitize(value: &str) -> String {
    sanitize_full(value).chars().take(128).collect()
}
pub(super) fn sanitize_full(value: &str) -> String {
    let base = value.rsplit('/').next().unwrap_or("");
    let key = base
        .to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>();
    let key = key.trim_matches(['.', '_', ' ']);
    if key.is_empty() {
        "plugin".into()
    } else {
        key.to_owned()
    }
}
fn walk(root: &Path, at: &Path, out: &mut Vec<PathBuf>) -> Result<(), StoreError> {
    for entry in fs::read_dir(at).map_err(io)? {
        let entry = entry.map_err(io)?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(io)?;
        if metadata.file_type().is_symlink() {
            return Err(unsupported());
        }
        if path
            .strip_prefix(root)
            .map_err(|_| unsupported())?
            .components()
            .count()
            > 32
        {
            return Err(unsupported());
        }
        out.push(path.clone());
        if out.len() > FILES * 2 {
            return Err(unsupported());
        }
        if metadata.is_dir() {
            walk(root, &path, out)?;
        }
    }
    Ok(())
}
/// New/manual disabled installs only. The HTTP owner checks DB enabled/trust and
/// task ownership BEFORE this function is granted overwrite authority.
pub(super) fn install(
    archive: &Path,
    root: &Path,
    file_name: &str,
    overwrite: bool,
) -> Result<Value, StoreError> {
    let lock = MutationLock::acquire(root)?;
    install_managed(archive, root, file_name, overwrite, None, None, &lock)
}
/// Same Source mutation lock survives async download and all apply/SQL effects.
/// Actual FD is owned; no independent semaphore or writer budget.
pub(super) struct MutationLock {
    _file: File,
}
impl MutationLock {
    pub(super) fn acquire(root: &Path) -> Result<Self, StoreError> {
        fs::create_dir_all(root).map_err(io)?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join(MUTATION_LOCK))
            .map_err(io)?;
        file.lock().map_err(io)?;
        Ok(Self { _file: file })
    }
}
/// The same cooperating filesystem lock as normal plugin mutations, with a
/// nonblocking acquire and a stable-name recheck under an existing SQL fence.
pub(super) struct PinnedMutationLock {
    _file: File,
    path: PathBuf,
}
impl PinnedMutationLock {
    #[cfg(unix)]
    pub(super) fn try_acquire(root: &Path) -> Result<Option<Self>, StoreError> {
        use std::os::unix::fs::OpenOptionsExt;
        let root = root.canonicalize().map_err(io)?;
        let path = root.join(MUTATION_LOCK);
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&path)
            .map_err(io)?;
        if !file.metadata().map_err(io)?.is_file() {
            return Err(unsupported());
        }
        match file.try_lock() {
            Ok(()) => {
                let result = Self { _file: file, path };
                result.validate()?;
                Ok(Some(result))
            }
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(error)) => Err(io(error)),
        }
    }
    #[cfg(not(unix))]
    pub(super) fn try_acquire(_: &Path) -> Result<Option<Self>, StoreError> {
        Err(unsupported())
    }
    pub(super) fn validate(&self) -> Result<(), StoreError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let actual = self._file.metadata().map_err(io)?;
            let named = fs::symlink_metadata(&self.path).map_err(io)?;
            if named.is_file()
                && named.ino() == actual.ino()
                && named.dev() == actual.dev()
                && self.path.canonicalize().map_err(io)? == self.path
            {
                return Ok(());
            }
        }
        Err(unsupported())
    }
}

pub(super) fn install_managed(
    archive: &Path,
    root: &Path,
    file_name: &str,
    overwrite: bool,
    overwrite_key: Option<&str>,
    install_key: Option<&str>,
    _lock: &MutationLock,
) -> Result<Value, StoreError> {
    let file = File::open(archive).map_err(io)?;
    let mut zip = match zip::ZipArchive::new(file) {
        Ok(zip) => zip,
        Err(_) => return Ok(bad("Invalid zip file")),
    };
    let staging = stage(root)?;
    let mut members = Vec::new();
    let mut total = 0u64;
    for index in 0..zip.len() {
        let item = zip.by_index(index).map_err(|_| unsupported())?;
        if item.is_dir() {
            continue;
        }
        members.push(index);
        if members.len() > FILES {
            return Ok(bad("Archive has too many files"));
        }
        if item.size() > BYTES {
            return Ok(bad("Archive contains a file that is too large"));
        }
        total = total.checked_add(item.size()).ok_or_else(unsupported)?;
        if total > BYTES {
            return Ok(bad("Archive is too large"));
        }
    }
    if members.is_empty() {
        return Ok(bad("Archive is empty"));
    }
    for index in members {
        let mut item = zip.by_index(index).map_err(|_| unsupported())?;
        let name = match normalized(item.name().map_err(|_| unsupported())?.as_ref()) {
            Ok(name) => name,
            Err(_) => return Ok(bad("Unsafe path in archive")),
        };
        let destination = staging.0.join(name);
        fs::create_dir_all(destination.parent().ok_or_else(unsupported)?).map_err(io)?;
        let mut output = File::create(&destination).map_err(io)?;
        let mut limited = (&mut item).take(BYTES + 1);
        let copied = std::io::copy(&mut limited, &mut output).map_err(io)?;
        if copied > BYTES {
            return Err(unsupported());
        }
        output.flush().map_err(io)?;
    }
    let mut all = vec![staging.0.clone()];
    walk(&staging.0, &staging.0, &mut all)?;
    let mut candidates = Vec::new();
    let mut logos = Vec::new();
    for path in &all {
        let relative = path.strip_prefix(&staging.0).map_err(|_| unsupported())?;
        let depth = relative.components().count().max(1);
        if path.is_dir() {
            if path.join("plugin.py").is_file() {
                candidates.push((0, depth, path.clone()));
            } else if path.join("__init__.py").is_file() {
                candidates.push((1, depth, path.clone()));
            }
        } else if path
            .file_name()
            .and_then(|v| v.to_str())
            .is_some_and(|v| v.eq_ignore_ascii_case("logo.png"))
        {
            // Source uses the containing directory's depth, not file depth.
            logos.push((
                relative
                    .parent()
                    .map(|p| p.components().count())
                    .unwrap_or(0)
                    .max(1),
                path.clone(),
            ));
        }
    }
    candidates.sort();
    let Some((_, _, chosen)) = candidates.into_iter().next() else {
        return Ok(bad(
            "Invalid plugin: missing plugin.py or package __init__.py",
        ));
    };
    let hint = Path::new(file_name)
        .file_stem()
        .and_then(|v| v.to_str())
        .unwrap_or("plugin");
    let derived_key = sanitize(if chosen == staging.0 {
        hint
    } else {
        chosen
            .file_name()
            .and_then(|v| v.to_str())
            .ok_or_else(unsupported)?
    });
    let key = install_key.unwrap_or(&derived_key).to_owned();
    if key.is_empty()
        || key.len() > 128
        || !matches!(
            (
                Path::new(&key).components().next(),
                Path::new(&key).components().count()
            ),
            (Some(Component::Normal(_)), 1)
        )
    {
        return Err(unsupported());
    }
    let overwrite = overwrite || overwrite_key.is_some_and(|value| value == key);
    logos.sort_by_key(|(depth, path)| (!path.starts_with(&chosen), *depth, path.clone()));
    let logo = match logos.first() {
        None => None,
        Some((_, p)) => Some(fs::read(p).map_err(io)?),
    };
    let target = root.join(&key);
    let backup = root.join(format!("{key}.__backup__"));
    if fs::symlink_metadata(&target).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(unsupported());
    }
    let existing = target.exists();
    if existing
        && !overwrite
        && (target.join("plugin.py").exists() || target.join("__init__.py").exists())
    {
        return Ok(bad(&format!("Plugin '{key}' already exists")));
    }
    if existing {
        if overwrite {
            if backup.exists() {
                fs::remove_dir_all(&backup).map_err(io)?;
            }
            fs::rename(&target, &backup).map_err(io)?;
        } else {
            fs::remove_dir_all(&target).map_err(io)?;
        }
    }
    let moved = (|| {
        // Source shutil.move supports cross-device fallback from system tmp.
        // The bounded importer guards cross-device fallback until separately
        // qualified; don't pretend an incomplete copied tree is installed.
        fs::rename(&chosen, &target).map_err(io)?;
        if let Some(bytes) = logo.filter(|v| !v.is_empty()) {
            let _ = fs::write(target.join("logo.png"), bytes);
        }
        Ok::<_, StoreError>(())
    })();
    if moved.is_err() {
        if existing && overwrite {
            let _ = fs::remove_dir_all(&target);
            let _ = fs::rename(&backup, &target);
        }
        return moved.map(|_| Value::Null);
    }
    if existing && overwrite {
        let _ = fs::remove_dir_all(backup);
    }
    Ok(json!({"success":true,"plugin_key":key}))
}
#[derive(Clone)]
pub(super) struct Discovered {
    pub key: String,
    pub folder: String,
    pub metadata: Value,
    pub legacy: bool,
    pub logo: bool,
}
pub(super) fn discover(root: &Path) -> Result<Vec<Discovered>, StoreError> {
    fs::create_dir_all(root).map_err(io)?;
    let mut dirs = Vec::new();
    for entry in fs::read_dir(root).map_err(io)? {
        let entry = entry.map_err(io)?;
        let metadata = entry.file_type().map_err(io)?;
        if metadata.is_symlink() {
            return Err(unsupported());
        }
        if !metadata.is_dir() {
            continue;
        }
        if dirs.len() >= 256 {
            return Err(unsupported());
        }
        dirs.push(entry.path());
    }
    dirs.sort();
    let mut result = Vec::new();
    let mut keys = BTreeSet::new();
    for path in dirs {
        if !(path.join("plugin.py").is_file() || path.join("__init__.py").is_file()) {
            continue;
        }
        let folder = path
            .file_name()
            .and_then(|v| v.to_str())
            .ok_or_else(unsupported)?
            .to_owned();
        let key = folder.replace(' ', "_").to_lowercase();
        if !keys.insert(key.clone()) {
            return Err(unsupported());
        }
        let manifest = path.join("plugin.json");
        let metadata = if manifest.is_file() {
            let file = File::open(manifest).map_err(io)?;
            let mut bytes = Vec::new();
            file.take(MANIFEST as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(io)?;
            if bytes.len() > MANIFEST {
                return Err(unsupported());
            }
            if nonfinite_literal(&bytes) {
                return Err(unsupported());
            }
            match serde_json::from_slice::<Value>(&bytes) {
                Ok(mut value) if value.is_object() => {
                    crate::json_compat::normalize_input_numbers(&mut value)
                        .map_err(|_| unsupported())?;
                    Some(value)
                }
                Ok(_) => None,
                Err(error) if error.to_string().contains("recursion limit exceeded") => {
                    return Err(unsupported())
                }
                Err(_) => None,
            }
        } else {
            None
        };
        result.push(Discovered {
            key,
            folder,
            legacy: metadata.is_none(),
            metadata: metadata.unwrap_or(json!({})),
            logo: path.join("logo.png").is_file(),
        });
    }
    Ok(result)
}
/// Source clean never-enabled deletion. Recorded/runtime task ownership is
/// fenced by the caller; arbitrary Python stop hooks are never skipped.
pub(super) fn delete_locked(
    root: &Path,
    key: &str,
    folder: Option<&str>,
    _lock: &MutationLock,
) -> Result<(), StoreError> {
    let leaf = folder.unwrap_or(key);
    let valid_leaf =
        |name: &str| !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\\']);
    if !valid_leaf(key) || !valid_leaf(leaf) {
        return Err(unsupported());
    }
    let target = root.join(leaf);
    match fs::symlink_metadata(&target) {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(target).map_err(io),
        Ok(_) => Err(unsupported()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io(error)),
    }
}

#[cfg(test)]
#[path = "plugin_files_tests.rs"]
mod tests;

#[cfg(all(test, unix))]
mod restore_lock_lifetime_tests {
    use super::*;
    #[test]
    fn reserved_lock_excludes_the_canonical_mutator_and_rejects_replaced_name() {
        let root =
            std::env::temp_dir().join(format!("dispatcharr-restore-lock-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let mutator = MutationLock::acquire(&root).unwrap();
        assert!(PinnedMutationLock::try_acquire(&root).unwrap().is_none());
        drop(mutator);
        let held = PinnedMutationLock::try_acquire(&root).unwrap().unwrap();
        assert!(PinnedMutationLock::try_acquire(&root).unwrap().is_none());
        assert!(held.validate().is_ok());
        let path = root.join(MUTATION_LOCK);
        fs::rename(&path, root.join("known-old-lock")).unwrap();
        fs::write(&path, []).unwrap();
        assert!(held.validate().is_err());
        drop(held);
        fs::remove_dir_all(root).unwrap();
    }
}

fn nonfinite_literal(raw: &[u8]) -> bool {
    let (mut quoted, mut escaped) = (false, false);
    for (i, &byte) in raw.iter().enumerate() {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else if byte == b'"' {
            quoted = true;
        } else if raw[i..].starts_with(b"NaN") || raw[i..].starts_with(b"Infinity") {
            return true;
        }
    }
    false
}
