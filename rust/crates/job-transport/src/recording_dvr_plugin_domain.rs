//! Idle startup capability is distinct from supported recording execution.
//! Original Dispatcharr/plugin authors retain application/algorithm credit.
//! Stable owned namespace only: checks do not claim atomic external rename safety.
use super::{recording_files, Error};
use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

pub(super) struct PluginDomain {
    configured: PathBuf,
    path: PathBuf,
    directory: File,
    dev: u64,
    ino: u64,
    unsupported: AtomicBool,
}
fn io(error: std::io::Error) -> Error {
    Error::Files(recording_files::Error::Io(error))
}
impl PluginDomain {
    pub(super) fn prepare(configured: &str) -> Result<Self, Error> {
        if configured.is_empty() || configured.len() > 4096 {
            return Err(Error::Unsupported("configured plugin path capacity"));
        }
        fs::create_dir_all(configured).map_err(io)?;
        let path = fs::canonicalize(configured).map_err(io)?;
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&path)
            .map_err(io)?;
        let meta = directory.metadata().map_err(io)?;
        let domain = Self {
            configured: configured.into(),
            path,
            directory,
            dev: meta.dev(),
            ino: meta.ino(),
            unsupported: AtomicBool::new(false),
        };
        // A populated directory is an explicit unsupported execution capability,
        // not a reason to prevent unrelated plugin/API families from starting.
        domain.observe_empty()?;
        Ok(domain)
    }
    fn same_directory(&self) -> Result<(), Error> {
        let configured = fs::canonicalize(&self.configured).map_err(io)?;
        let meta = fs::symlink_metadata(&self.path).map_err(io)?;
        let held = self.directory.metadata().map_err(io)?;
        if configured != self.path
            || meta.file_type().is_symlink()
            || !meta.is_dir()
            || (meta.dev(), meta.ino()) != (self.dev, self.ino)
            || (held.dev(), held.ino()) != (self.dev, self.ino)
        {
            return Err(Error::Unsupported(
                "configured plugin directory identity changed",
            ));
        }
        Ok(())
    }
    fn observe_empty(&self) -> Result<bool, Error> {
        self.same_directory()?;
        let empty = match fs::read_dir(&self.path).map_err(io)?.next() {
            None => true,
            Some(Ok(_)) => false,
            Some(Err(error)) => return Err(io(error)),
        };
        self.same_directory()?;
        self.unsupported.store(!empty, Ordering::Release);
        Ok(empty)
    }
    pub(super) fn require_supported(&self) -> Result<(), Error> {
        if self.observe_empty()? {
            Ok(())
        } else {
            Err(Error::Unsupported(
                "populated configured plugin hooks require adapter",
            ))
        }
    }
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
    pub(super) fn last_observed_capability(&self) -> &'static str {
        if self.unsupported.load(Ordering::Acquire) {
            "unsupported_configured_plugin_hooks"
        } else {
            "empty_directory_rechecked_before_recording_effects"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            let path = std::env::temp_dir()
                .join(format!("dispatcharr-dvr-domain-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&path).expect("owned temp directory");
            Self(path)
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn populated_directory_allows_idle_construction_but_refuses_execution() {
        let temp = Temp::new();
        fs::write(temp.0.join("plugin.py"), b"# inert owned fixture").unwrap();
        let guard = PluginDomain::prepare(temp.0.to_str().unwrap()).unwrap();
        assert_eq!(
            guard.last_observed_capability(),
            "unsupported_configured_plugin_hooks"
        );
        assert!(matches!(
            guard.require_supported(),
            Err(Error::Unsupported(_))
        ));
    }
    #[test]
    fn every_admission_rechecks_plugin_addition() {
        let temp = Temp::new();
        let guard = PluginDomain::prepare(temp.0.to_str().unwrap()).unwrap();
        guard.require_supported().unwrap();
        fs::write(temp.0.join("plugin.py"), b"# inert owned fixture").unwrap();
        assert!(matches!(
            guard.require_supported(),
            Err(Error::Unsupported(_))
        ));
        fs::remove_file(temp.0.join("plugin.py")).unwrap();
        guard.require_supported().unwrap();
    }
    #[test]
    fn replaced_directory_is_not_new_empty_capability() {
        let temp = Temp::new();
        let path = temp.0.join("plugins");
        fs::create_dir(&path).unwrap();
        let guard = PluginDomain::prepare(path.to_str().unwrap()).unwrap();
        fs::rename(&path, temp.0.join("old-plugins")).unwrap();
        fs::create_dir(&path).unwrap();
        assert!(matches!(
            guard.require_supported(),
            Err(Error::Unsupported(_))
        ));
    }
    #[test]
    fn configured_symlink_retarget_is_refused() {
        let temp = Temp::new();
        let first = temp.0.join("first");
        let second = temp.0.join("second");
        fs::create_dir(&first).unwrap();
        fs::create_dir(&second).unwrap();
        let alias = temp.0.join("plugins");
        std::os::unix::fs::symlink(&first, &alias).unwrap();
        let guard = PluginDomain::prepare(alias.to_str().unwrap()).unwrap();
        fs::remove_file(&alias).unwrap();
        std::os::unix::fs::symlink(&second, &alias).unwrap();
        assert!(matches!(
            guard.require_supported(),
            Err(Error::Unsupported(_))
        ));
    }
}
