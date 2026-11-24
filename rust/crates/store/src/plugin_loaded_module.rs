//! Internal continuity proof for one already-loaded taskless action module.
//! Original Dispatcharr and CPython authors retain full workflow credit.
use crate::plugin_execution_host::Identity;
use crate::StoreError;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, Metadata, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
};

const LIMIT: u64 = 1024 * 1024;

/// Opaque evidence. The host, file paths and content digests are private.
pub struct LoadedTasklessModule {
    identity: Identity,
    files: InstalledModule,
    load_sequence: u64,
    metadata: [String; 3],
}
impl LoadedTasklessModule {
    pub(super) fn load_sequence(&self) -> u64 {
        self.load_sequence
    }

    /// Callers can compare fresh observations while retaining actual ownership.
    pub fn same_loaded_module(&self, current: &Self) -> bool {
        crate::plugin_python_registry::same(&self.identity, &current.identity)
            && crate::plugin_python_registry::live(&current.identity)
            && self.load_sequence == current.load_sequence
            && self.metadata == current.metadata
            && self.files == current.files
    }
    pub(crate) fn matches_registry_key(&self, row: &serde_json::Value) -> bool {
        // The live module reports class display metadata. Original retains
        // configured SQL display values separately, including across restore.
        self.files
            .source
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .is_some_and(|key| row["key"].as_str() == Some(key))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Observation {
    actual_pid: u32,
    path: PathBuf,
    sha256: String,
    load_sequence: u64,
    metadata: [String; 3],
}

#[derive(PartialEq, Eq)]
pub(super) struct InstalledModule {
    source: PathBuf,
    code: Fingerprint,
    manifest: Fingerprint,
    logo: Option<Fingerprint>,
}

#[derive(PartialEq, Eq)]
struct Fingerprint {
    sha256: String,
    length: u64,
    modified: std::time::SystemTime,
    device_inode: (u64, u64),
    change_time: (i64, i64),
}

fn unavailable() -> StoreError {
    StoreError::Unsupported("Actual configured taskless plugin module witness unavailable")
}

fn stable_metadata(meta: &Metadata) -> Result<Fingerprint, StoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if !meta.is_file() || meta.len() > LIMIT {
            return Err(unavailable());
        }
        Ok(Fingerprint {
            sha256: String::new(),
            length: meta.len(),
            modified: meta.modified().map_err(|_| unavailable())?,
            device_inode: (meta.dev(), meta.ino()),
            change_time: (meta.ctime(), meta.ctime_nsec()),
        })
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        Err(unavailable())
    }
}

fn fingerprint(path: &Path) -> Result<Fingerprint, StoreError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let mut file = options.open(path).map_err(|_| unavailable())?;
    let before = stable_metadata(&file.metadata().map_err(|_| unavailable())?)?;
    let mut bytes = Vec::new();
    (&mut file)
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| unavailable())?;
    let after = stable_metadata(&file.metadata().map_err(|_| unavailable())?)?;
    let named = stable_metadata(&fs::symlink_metadata(path).map_err(|_| unavailable())?)?;
    if before != after || after != named || bytes.len() as u64 != before.length {
        return Err(unavailable());
    }
    Ok(Fingerprint {
        sha256: format!("{:x}", Sha256::digest(bytes)),
        ..before
    })
}

impl InstalledModule {
    pub(super) fn inspect(root: &Path, key: &str, include_logo: bool) -> Result<Self, StoreError> {
        if key.is_empty()
            || key.len() > 128
            || !key
                .bytes()
                .all(|v| v.is_ascii_lowercase() || v.is_ascii_digit() || v == b'_')
        {
            return Err(unavailable());
        }
        let root = root.canonicalize().map_err(|_| unavailable())?;
        let folder = root.join(key);
        if folder.canonicalize().map_err(|_| unavailable())? != folder {
            return Err(unavailable());
        }
        for (index, entry) in fs::read_dir(&root).map_err(|_| unavailable())?.enumerate() {
            let entry = entry.map_err(|_| unavailable())?;
            let kind = entry.file_type().map_err(|_| unavailable())?;
            if index >= 256 || kind.is_symlink() || (kind.is_dir() && entry.path() != folder) {
                return Err(unavailable());
            }
        }
        let mut logo = None;
        for entry in fs::read_dir(&folder).map_err(|_| unavailable())? {
            let entry = entry.map_err(|_| unavailable())?;
            match entry.file_name().to_str() {
                Some("plugin.py" | "plugin.json") => {}
                Some("logo.png") if include_logo => logo = Some(fingerprint(&entry.path())?),
                _ => return Err(unavailable()),
            }
        }
        let source = folder.join("plugin.py");
        let code = fingerprint(&source)?;
        let manifest = fingerprint(&folder.join("plugin.json"))?;
        if folder.canonicalize().map_err(|_| unavailable())? != folder {
            return Err(unavailable());
        }
        Ok(Self {
            source,
            code,
            manifest,
            logo,
        })
    }

    pub(super) fn attest(
        self,
        current: Self,
        identity: Identity,
        result: Observation,
    ) -> Result<LoadedTasklessModule, StoreError> {
        if self != current
            || result.actual_pid != identity.pid
            || result.path != self.source
            || result.sha256 != self.code.sha256
            || result.load_sequence == 0
            || result
                .metadata
                .iter()
                .any(|item| item.len() > LIMIT as usize)
            || !crate::plugin_python_registry::live(&identity)
        {
            return Err(unavailable());
        }
        Ok(LoadedTasklessModule {
            identity,
            files: self,
            load_sequence: result.load_sequence,
            metadata: result.metadata,
        })
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn exact_single_file_layout_detects_change_and_refuses_extra_modules() {
        let root = std::env::temp_dir().join(format!(
            "dispatcharr-module-witness-{}",
            uuid::Uuid::new_v4()
        ));
        let plugin = root.join("existing_plugin");
        fs::create_dir_all(&plugin).unwrap();
        fs::write(plugin.join("plugin.py"), b"class Plugin: pass\n").unwrap();
        fs::write(plugin.join("plugin.json"), b"{}\n").unwrap();
        let before = InstalledModule::inspect(&root, "existing_plugin", false).unwrap();
        let raw = fs::read_to_string("/proc/self/stat").unwrap();
        let (_, tail) = raw.rsplit_once(')').unwrap();
        let identity = Identity {
            pid: std::process::id(),
            start_generation: tail.split_whitespace().nth(19).unwrap().to_owned(),
            pid_namespace: fs::read_link("/proc/self/ns/pid").unwrap(),
        };
        let observe = |pid, sequence, digest: String| Observation {
            actual_pid: pid,
            path: plugin.canonicalize().unwrap().join("plugin.py"),
            sha256: digest,
            load_sequence: sequence,
            metadata: [
                "Live fixture plugin".into(),
                "live-1".into(),
                "Owned original probe".into(),
            ],
        };
        let capture = |observed| {
            InstalledModule::inspect(&root, "existing_plugin", false)
                .unwrap()
                .attest(
                    InstalledModule::inspect(&root, "existing_plugin", false).unwrap(),
                    identity.clone(),
                    observed,
                )
        };
        let digest = before.code.sha256.clone();
        let first = capture(observe(identity.pid, 1, digest.clone())).unwrap();
        let repeated = capture(observe(identity.pid, 1, digest.clone())).unwrap();
        let reloaded = capture(observe(identity.pid, 2, digest.clone())).unwrap();
        assert!(first.same_loaded_module(&repeated));
        assert!(!first.same_loaded_module(&reloaded));
        // Original preserved these stored values despite its live class
        // reporting the different values captured in the witness above.
        let mut configured = serde_json::json!({
            "key": "existing_plugin",
            "name": "Manifest fixture plugin",
            "version": "1.0",
            "description": "",
        });
        assert!(first.matches_registry_key(&configured));
        configured["key"] = serde_json::Value::from("other_plugin");
        assert!(!first.matches_registry_key(&configured));
        assert!(!first.matches_registry_key(&serde_json::json!({})));
        let mut changed_metadata = observe(identity.pid, 1, digest.clone());
        changed_metadata.metadata[1] = "live-2".into();
        assert!(!first.same_loaded_module(&capture(changed_metadata).unwrap()));
        assert!(capture(observe(0, 1, digest.clone())).is_err());
        assert!(capture(observe(identity.pid, 1, "0".repeat(64))).is_err());
        assert!(before == InstalledModule::inspect(&root, "existing_plugin", false).unwrap());
        fs::write(plugin.join("plugin.py"), b"class Plugin: pass\n\n").unwrap();
        assert!(before != InstalledModule::inspect(&root, "existing_plugin", false).unwrap());
        fs::write(plugin.join("logo.png"), b"reviewed owned logo").unwrap();
        assert!(InstalledModule::inspect(&root, "existing_plugin", false).is_err());
        let with_logo = InstalledModule::inspect(&root, "existing_plugin", true).unwrap();
        fs::write(plugin.join("logo.png"), b"changed owned logo").unwrap();
        assert!(with_logo != InstalledModule::inspect(&root, "existing_plugin", true).unwrap());
        fs::remove_file(plugin.join("logo.png")).unwrap();
        std::os::unix::fs::symlink(plugin.join("plugin.json"), plugin.join("logo.png")).unwrap();
        assert!(InstalledModule::inspect(&root, "existing_plugin", true).is_err());
        fs::remove_file(plugin.join("logo.png")).unwrap();
        fs::write(plugin.join("helper.py"), b"pass\n").unwrap();
        assert!(InstalledModule::inspect(&root, "existing_plugin", false).is_err());
        fs::remove_file(plugin.join("helper.py")).unwrap();
        fs::remove_file(plugin.join("plugin.py")).unwrap();
        std::os::unix::fs::symlink(plugin.join("plugin.json"), plugin.join("plugin.py")).unwrap();
        assert!(InstalledModule::inspect(&root, "existing_plugin", false).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
