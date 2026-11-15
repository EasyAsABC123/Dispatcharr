//! FD-anchored owned incoming XML download/promotion; Original authors credited.
use dispatcharr_ingest_core::{
    detect_xmltv_format, extract_xmltv_owned, xmltv_file_handle_is_complete, UploadLimits,
};
use std::{
    collections::BTreeMap,
    ffi::CString,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
};
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, "owned XML cache identity")
}
fn identity(file: &File) -> io::Result<(u64, u64)> {
    let m = file.metadata()?;
    Ok((m.dev(), m.ino()))
}
fn proc(fd: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", fd.as_raw_fd()))
}
#[derive(Clone)]
pub(super) struct Directory {
    inner: Arc<Root>,
}
struct Root {
    spelling: PathBuf,
    path: PathBuf,
    fd: File,
}
impl Directory {
    pub fn prepare(path: PathBuf) -> io::Result<Self> {
        if !path.is_absolute() || path == Path::new("/") {
            return Err(invalid());
        }
        std::fs::create_dir_all(&path)?;
        if std::fs::symlink_metadata(&path)?.file_type().is_symlink() {
            return Err(invalid());
        }
        let spelling = path;
        let path = std::fs::canonicalize(&spelling)?;
        let fd = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)?;
        let m = fd.metadata()?;
        if !m.is_dir() || m.uid() != unsafe { libc::geteuid() } {
            return Err(invalid());
        }
        Ok(Self {
            inner: Arc::new(Root { spelling, path, fd }),
        })
    }
    pub fn cache_seed(&self, id: i64) -> PathBuf {
        self.inner.spelling.join(format!("{id}.tmp"))
    }
    pub fn incoming(&self) -> io::Result<Incoming> {
        let name = format!(".native-xml-{}", uuid::Uuid::new_v4().simple());
        let c = CString::new(name.as_bytes()).map_err(|_| invalid())?;
        if unsafe { libc::mkdirat(self.inner.fd.as_raw_fd(), c.as_ptr(), 0o700) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = unsafe {
            libc::openat(
                self.inner.fd.as_raw_fd(),
                c.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = unsafe { File::from_raw_fd(fd) };
        let original = identity(&fd)?;
        let mut incoming = Incoming {
            root: self.clone(),
            name,
            fd,
            original,
            owned: BTreeMap::new(),
        };
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(incoming.path("download.tmp"))?;
        incoming
            .owned
            .insert("download.tmp".into(), identity(&file)?);
        drop(file);
        Ok(incoming)
    }
}
pub(super) enum Promotion {
    Installed(PathBuf, bool),
    Incomplete,
    ExtractionFailed,
}
#[derive(Debug)]
pub(super) enum FinishError {
    Io(io::Error),
    Capacity,
}
impl From<io::Error> for FinishError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
pub(super) struct Incoming {
    root: Directory,
    name: String,
    fd: File,
    original: (u64, u64),
    owned: BTreeMap<String, (u64, u64)>,
}
impl Incoming {
    fn path(&self, name: &str) -> PathBuf {
        proc(&self.fd).join(name)
    }
    pub fn compressed(&self, url: &str) -> io::Result<bool> {
        let mut input = File::open(self.path("download.tmp"))?;
        let mut head = [0; 1024];
        let n = input.read(&mut head)?;
        Ok(detect_xmltv_format(Some(url), Some(&head[..n]), None).compressed)
    }
    pub fn writer(&self) -> io::Result<File> {
        OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(self.path("download.tmp"))
    }
    fn adopt(&mut self, name: &str) -> io::Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(self.path(name))?;
        let m = file.metadata()?;
        if !m.is_file() || m.uid() != unsafe { libc::geteuid() } {
            return Err(invalid());
        }
        self.owned.insert(name.into(), identity(&file)?);
        Ok(())
    }
    /// Only after the real HTTP writer has closed. Extraction owns no previous
    /// good guide; cancellation/failure leaves the installed guide untouched.
    pub fn finish(
        mut self,
        source: i64,
        url: &str,
        cancel: &AtomicBool,
    ) -> Result<Promotion, FinishError> {
        if cancel.load(std::sync::atomic::Ordering::Acquire) {
            return Err(
                io::Error::new(io::ErrorKind::Interrupted, "XML download cancelled").into(),
            );
        }
        let mut input = File::open(self.path("download.tmp"))?;
        let mut head = [0; 1024];
        let count = input.read(&mut head)?;
        let format = detect_xmltv_format(Some(url), Some(&head[..count]), None);
        drop(input);
        let compressed = format.compressed;
        if compressed {
            let result = extract_xmltv_owned(
                &self.path("download.tmp"),
                &self.path("incoming.xml"),
                false,
                UploadLimits::default(),
                cancel,
            );
            if self.path("incoming.xml").exists() {
                self.adopt("incoming.xml")?;
            }
            match result {
                Ok(_) => {}
                Err(dispatcharr_ingest_core::XmltvExtractError::Capacity) => {
                    return Err(FinishError::Capacity)
                }
                Err(_) => return Ok(Promotion::ExtractionFailed),
            }
        } else {
            std::fs::rename(self.path("download.tmp"), self.path("incoming.xml"))?;
            let original = self.owned.remove("download.tmp").ok_or_else(invalid)?;
            self.owned.insert("incoming.xml".into(), original);
        }
        let mut file = File::open(self.path("incoming.xml"))?;
        if !xmltv_file_handle_is_complete(&mut file) {
            return Ok(Promotion::Incomplete);
        }
        if cancel.load(std::sync::atomic::Ordering::Acquire) {
            return Err(
                io::Error::new(io::ErrorKind::Interrupted, "XML promotion cancelled").into(),
            );
        }
        let current = File::open(&self.root.inner.path)?;
        let configured = File::open(&self.root.inner.spelling)?;
        if identity(&configured)? != identity(&self.root.inner.fd)? {
            return Err(invalid().into());
        }

        if identity(&current)? != identity(&self.root.inner.fd)? {
            return Err(invalid().into());
        }
        let original = self.owned.get("incoming.xml").ok_or_else(invalid)?;
        if identity(&file)? != *original {
            return Err(invalid().into());
        }
        file.sync_all()?;
        std::fs::set_permissions(
            self.path("incoming.xml"),
            std::os::unix::fs::PermissionsExt::from_mode(0o644),
        )?;
        let target = format!("{source}.xml");
        std::fs::rename(
            self.path("incoming.xml"),
            proc(&self.root.inner.fd).join(&target),
        )?;
        self.owned.remove("incoming.xml");
        // Source also removes only this source's obsolete compressed cache name.
        let old = if compressed {
            format!("{source}{}", format.extension)
        } else {
            format!("{source}.compressed")
        };
        let _ = std::fs::remove_file(proc(&self.root.inner.fd).join(old));
        Ok(Promotion::Installed(
            self.root.inner.spelling.join(target),
            compressed,
        ))
    }
}
impl Drop for Incoming {
    fn drop(&mut self) {
        for (name, original) in &self.owned {
            if let Ok(file) = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
                .open(self.path(name))
            {
                if identity(&file).ok() == Some(*original) {
                    let _ = std::fs::remove_file(self.path(name));
                }
            }
        }
        let path = proc(&self.root.inner.fd).join(&self.name);
        if let Ok(dir) = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
        {
            if identity(&dir).ok() == Some(self.original) {
                let _ = std::fs::remove_dir(path);
            }
        }
    }
}
/// One bounded disk actor; sends are at most16KiB, queue is2. The actor retains
/// the actual task owner until its real write/flush/close result is observed.
pub(super) fn writer(
    file: File,
    owner: Arc<dyn Send + Sync>,
) -> (
    tokio::sync::mpsc::Sender<Vec<u8>>,
    tokio::task::JoinHandle<io::Result<()>>,
) {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(2);
    let done = tokio::task::spawn_blocking(move || {
        let _owner = owner;
        let mut file = file;
        while let Some(bytes) = rx.blocking_recv() {
            if bytes.len() > 16384 {
                return Err(invalid());
            }
            file.write_all(&bytes)?;
        }
        file.flush()?;
        file.sync_all()?;
        drop(file);
        Ok(())
    });
    (tx, done)
}

#[cfg(test)]
mod physical_controls {
    use super::*;
    use std::os::unix::fs::symlink;
    fn root() -> PathBuf {
        let p = std::env::temp_dir().join(format!("xml-download-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&p).unwrap();
        p
    }
    #[test]
    fn incomplete_pull_preserves_installed_guide_and_retires_only_owned_incoming() {
        let path = root();
        let cache = Directory::prepare(path.clone()).unwrap();
        let old = path.join("7.xml");
        std::fs::write(&old, b"previous-good-guide").unwrap();
        let incoming = cache.incoming().unwrap();
        incoming
            .writer()
            .unwrap()
            .write_all(b"<tv>truncated")
            .unwrap();
        assert!(matches!(
            incoming
                .finish(7, "http://owned.invalid/guide.xml", &AtomicBool::new(false))
                .unwrap(),
            Promotion::Incomplete
        ));
        assert_eq!(std::fs::read(&old).unwrap(), b"previous-good-guide");
        assert_eq!(std::fs::read_dir(&path).unwrap().count(), 1);
        drop(cache);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn configured_media_alias_preserves_spelling_and_retarget_cannot_redirect_promotion() {
        let base = root();
        let actual = base.join("actual");
        let foreign = base.join("foreign");
        std::fs::create_dir(&actual).unwrap();
        std::fs::create_dir(&foreign).unwrap();
        let alias = base.join("media");
        symlink(&actual, &alias).unwrap();
        let cache = Directory::prepare(alias.join("cached_epg")).unwrap();
        let incoming = cache.incoming().unwrap();
        incoming
            .writer()
            .unwrap()
            .write_all(b"<?xml version=\"1.0\"?><tv></tv>")
            .unwrap();
        let Promotion::Installed(path, false) = incoming
            .finish(7, "http://owned.invalid/guide.xml", &AtomicBool::new(false))
            .unwrap()
        else {
            panic!("ordinary valid guide not installed")
        };
        assert_eq!(path, alias.join("cached_epg/7.xml"));
        let old = std::fs::read(actual.join("cached_epg/7.xml")).unwrap();
        let incoming = cache.incoming().unwrap();
        incoming
            .writer()
            .unwrap()
            .write_all(b"<tv><channel id=\"new\"/></tv>")
            .unwrap();
        std::fs::remove_file(&alias).unwrap();
        symlink(&foreign, &alias).unwrap();
        assert!(incoming
            .finish(7, "http://owned.invalid/guide.xml", &AtomicBool::new(false))
            .is_err());
        assert_eq!(std::fs::read(actual.join("cached_epg/7.xml")).unwrap(), old);
        assert!(!foreign.join("cached_epg/7.xml").exists());
        assert_eq!(
            std::fs::read_dir(actual.join("cached_epg"))
                .unwrap()
                .count(),
            1
        );
        drop(cache);
        std::fs::remove_dir_all(base).unwrap();
    }
    #[test]
    fn actual_gzip_extractor_promotes_validated_xml_without_retaining_archive() {
        use base64::Engine;
        let path = root();
        let cache = Directory::prepare(path.clone()).unwrap();
        let incoming = cache.incoming().unwrap();
        let archive=base64::engine::general_purpose::STANDARD.decode("H4sIAAAAAAAC/7Oxr8jNUShLLSrOzM+zVTLUM1Cyt7MpKbOzSc5IzMtLzVHITLFVyi/PS01R0rez0S8pswMAqiRtITMAAAA=").unwrap();
        incoming.writer().unwrap().write_all(&archive).unwrap();
        assert!(incoming
            .compressed("http://owned.invalid/guide.xml.gz")
            .unwrap());
        let Promotion::Installed(installed, true) = incoming
            .finish(
                7,
                "http://owned.invalid/guide.xml.gz",
                &AtomicBool::new(false),
            )
            .unwrap()
        else {
            panic!("gzip not installed")
        };
        assert_eq!(
            std::fs::read(installed).unwrap(),
            b"<?xml version=\"1.0\"?><tv><channel id=\"owned\"/></tv>"
        );
        assert_eq!(std::fs::read_dir(&path).unwrap().count(), 1);
        drop(cache);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[tokio::test]
    async fn disk_actor_retains_real_caller_until_flush_close_after_observer_loss() {
        let path = root();
        let cache = Directory::prepare(path.clone()).unwrap();
        let incoming = cache.incoming().unwrap();
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        struct Lifetime(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Lifetime {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::Release)
            }
        }
        let owner: Arc<dyn Send + Sync> = Arc::new(Lifetime(flag.clone()));
        let weak = Arc::downgrade(&owner);
        let (tx, done) = writer(incoming.writer().unwrap(), owner.clone());
        drop(owner);
        tx.send(b"owned".to_vec()).await.unwrap();
        assert!(weak.upgrade().is_some());
        assert!(!flag.load(std::sync::atomic::Ordering::Acquire));
        drop(tx);
        done.await.unwrap().unwrap();
        assert!(weak.upgrade().is_none());
        assert!(flag.load(std::sync::atomic::Ordering::Acquire));
        drop(incoming);
        drop(cache);
        std::fs::remove_dir_all(path).unwrap();
    }
}
