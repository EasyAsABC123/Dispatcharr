//! Linux directory descriptors for Source's initially-absent recordings root.
//! Read-only before task admission; creation remains at Source create-parent.
//! No unsafe calls or new dependencies. Later FFmpeg global paths still require
//! the declared exclusively-owned namespace; no arbitrary rename parity claim.
use super::{ancestors, Error, Node};
use std::{
    fs::{self, File, OpenOptions},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
};

fn descriptor(path: &Path) -> Result<File, Error> {
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)?)
}
fn identity(file: &File) -> Result<Node, Error> {
    let metadata = file.metadata()?;
    if !metadata.is_dir() {
        return Err(Error::Unsupported("owned directory descriptor"));
    }
    Ok(Node {
        dev: metadata.dev(),
        ino: metadata.ino(),
    })
}
fn component(value: &str) -> Result<(), Error> {
    if value.is_empty()
        || matches!(value, "." | "..")
        || value.contains('/')
        || value.as_bytes().contains(&0)
    {
        return Err(Error::Unsupported("owned relative directory component"));
    }
    Ok(())
}
fn child(file: &File, name: &str) -> Result<PathBuf, Error> {
    component(name)?;
    Ok(PathBuf::from(format!(
        "/proc/self/fd/{}/{}",
        file.as_raw_fd(),
        name
    )))
}
fn opened_owned(path: &Path, uid: u32) -> Result<File, Error> {
    let file = descriptor(path)?;
    if file.metadata()?.uid() != uid {
        return Err(Error::Unsupported("foreign media directory UID"));
    }
    Ok(file)
}
pub(super) struct Root {
    configured: PathBuf,
    parent: File,
    parent_node: Node,
    parent_uid: u32,
    root: Option<(File, Node)>,
    uid: u32,
}
impl Root {
    pub(super) fn inspect(configured: &Path, uid: u32) -> Result<Self, Error> {
        if configured.file_name().and_then(|v| v.to_str()) != Some("recordings") {
            return Err(Error::Unsupported("configured recordings basename"));
        }
        let parent_path = configured
            .parent()
            .ok_or(Error::Unsupported("configured recordings parent"))?;
        ancestors(parent_path)?;
        let parent = descriptor(parent_path)?;
        let parent_node = identity(&parent)?;
        let parent_uid = parent.metadata()?.uid();
        let path = child(&parent, "recordings")?;
        let root = match fs::symlink_metadata(&path) {
            Ok(_) => {
                let file = opened_owned(&path, uid)?;
                let node = identity(&file)?;
                Some((file, node))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if parent_uid != uid {
                    return Err(Error::Unsupported(
                        "missing root requires owned configured parent",
                    ));
                }
                None
            }
            Err(error) => return Err(error.into()),
        };
        let prepared = Self {
            configured: configured.into(),
            parent,
            parent_node,
            parent_uid,
            root,
            uid,
        };
        prepared.verify()?;
        Ok(prepared)
    }
    pub(super) fn verify(&self) -> Result<(), Error> {
        let parent_path = self
            .configured
            .parent()
            .ok_or(Error::Unsupported("root parent"))?;
        if super::node(parent_path, true)? != self.parent_node
            || identity(&self.parent)? != self.parent_node
            || self.parent.metadata()?.uid() != self.parent_uid
        {
            return Err(Error::Unsupported("configured parent identity changed"));
        }
        let path = child(&self.parent, "recordings")?;
        match &self.root {
            Some((file, node)) => {
                let actual = opened_owned(&path, self.uid)?;
                if identity(&actual)? != *node
                    || identity(file)? != *node
                    || file.metadata()?.uid() != self.uid
                {
                    return Err(Error::Unsupported("owned root identity changed"));
                }
            }
            None => match fs::symlink_metadata(path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
                Ok(_) => {
                    return Err(Error::Unsupported(
                        "root appeared after read-only admission",
                    ))
                }
            },
        }
        Ok(())
    }
    fn establish(&mut self) -> Result<(), Error> {
        self.verify()?;
        if self.root.is_none() {
            let path = child(&self.parent, "recordings")?;
            // Atomic mkdir must succeed; EEXIST is a foreign/racing creation,
            // never permission to adopt or clear that directory.
            fs::create_dir(&path)?;
            let file = opened_owned(&path, self.uid)?;
            let node = identity(&file)?;
            self.root = Some((file, node));
            self.verify()?;
        }
        Ok(())
    }
    pub(super) fn ensure_directory(
        &mut self,
        relative: &Path,
        existing_last: bool,
    ) -> Result<Node, Error> {
        let raw = relative
            .to_str()
            .ok_or(Error::Unsupported("relative UTF8 directory"))?;
        let parts: Vec<_> = raw.split('/').collect();
        if parts.is_empty() || parts.len() > 3 {
            return Err(Error::Unsupported("bounded media directory depth"));
        }
        for name in &parts {
            component(name)?;
        }
        self.establish()?;
        let mut directory = self
            .root
            .as_ref()
            .ok_or(Error::Unsupported("established root"))?
            .0
            .try_clone()?;
        for (index, name) in parts.iter().enumerate() {
            self.verify()?;
            let path = child(&directory, name)?;
            let exists = match fs::symlink_metadata(&path) {
                Ok(_) => true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => return Err(error.into()),
            };
            if exists {
                if index + 1 == parts.len() && !existing_last {
                    return Err(Error::Unsupported("fresh media directory appeared"));
                }
            } else {
                fs::create_dir(&path)?;
            }
            directory = opened_owned(&path, self.uid)?;
        }
        self.verify()?;
        identity(&directory)
    }
}

#[cfg(test)]
mod controls {
    use super::*;
    use std::os::unix::fs::symlink;
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("dvr-owned-root-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn uid(&self) -> u32 {
            fs::metadata(&self.0).unwrap().uid()
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn absent_root_inspection_is_readonly_then_source_boundary_creates_owned_tree() {
        let temp = Temp::new();
        let path = temp.0.join("recordings");
        let mut prepared = Root::inspect(&path, temp.uid()).unwrap();
        assert!(!path.exists());
        prepared.verify().unwrap();
        assert!(!path.exists());
        let created = prepared
            .ensure_directory(Path::new("TV_Shows/Owned Parented Artwork"), true)
            .unwrap();
        assert_eq!(
            super::super::node(&path.join("TV_Shows/Owned Parented Artwork"), true).unwrap(),
            created
        );
        assert!(path.is_dir());
        assert!(!path
            .join("TV_Shows/Owned Parented Artwork/S01E01.mkv")
            .exists());
    }
    #[test]
    fn existing_root_and_parent_identity_are_preserved() {
        let temp = Temp::new();
        let path = temp.0.join("recordings");
        fs::create_dir(&path).unwrap();
        let before = super::super::node(&path, true).unwrap();
        let mut prepared = Root::inspect(&path, temp.uid()).unwrap();
        prepared
            .ensure_directory(Path::new("TV_Shows/Owned"), true)
            .unwrap();
        assert_eq!(super::super::node(&path, true).unwrap(), before);
    }
    #[test]
    fn unknown_root_appearing_after_inspection_is_retained_and_refused() {
        let temp = Temp::new();
        let path = temp.0.join("recordings");
        let mut prepared = Root::inspect(&path, temp.uid()).unwrap();
        fs::create_dir(&path).unwrap();
        fs::write(path.join("foreign"), b"retain").unwrap();
        assert!(prepared
            .ensure_directory(Path::new("TV_Shows/Owned"), true)
            .is_err());
        assert_eq!(fs::read(path.join("foreign")).unwrap(), b"retain");
        assert!(!path.join("TV_Shows").exists());
    }
    #[test]
    fn symlink_root_is_never_followed() {
        let temp = Temp::new();
        let target = temp.0.join("foreign");
        fs::create_dir(&target).unwrap();
        let path = temp.0.join("recordings");
        symlink(&target, &path).unwrap();
        assert!(Root::inspect(&path, temp.uid()).is_err());
        assert_eq!(fs::read_dir(target).unwrap().count(), 0);
    }
    #[test]
    fn foreign_uid_refuses_both_existing_and_missing_root() {
        let temp = Temp::new();
        let path = temp.0.join("recordings");
        assert!(Root::inspect(&path, temp.uid() + 1).is_err());
        fs::create_dir(&path).unwrap();
        assert!(Root::inspect(&path, temp.uid() + 1).is_err());
    }
    #[test]
    fn relative_escape_is_refused_before_root_creation() {
        let temp = Temp::new();
        let path = temp.0.join("recordings");
        let mut prepared = Root::inspect(&path, temp.uid()).unwrap();
        for value in [
            "../escape",
            "/absolute",
            "TV_Shows/../escape",
            "TV_Shows/./escape",
            "TV_Shows//escape",
        ] {
            assert!(prepared.ensure_directory(Path::new(value), true).is_err());
        }
        assert!(!path.exists());
    }
    #[test]
    fn replaced_configured_parent_refuses_without_writing_foreign_directory() {
        let temp = Temp::new();
        let parent = temp.0.join("data");
        fs::create_dir(&parent).unwrap();
        let mut prepared = Root::inspect(&parent.join("recordings"), temp.uid()).unwrap();
        fs::rename(&parent, temp.0.join("old-data")).unwrap();
        fs::create_dir(&parent).unwrap();
        assert!(prepared
            .ensure_directory(Path::new("TV_Shows/Owned"), true)
            .is_err());
        assert!(!parent.join("recordings").exists());
        assert!(!temp.0.join("old-data/recordings").exists());
    }
    #[test]
    fn symlink_intermediate_is_refused_without_foreign_mutation() {
        let temp = Temp::new();
        let path = temp.0.join("recordings");
        fs::create_dir(&path).unwrap();
        let foreign = temp.0.join("foreign");
        fs::create_dir(&foreign).unwrap();
        symlink(&foreign, path.join("TV_Shows")).unwrap();
        let mut prepared = Root::inspect(&path, temp.uid()).unwrap();
        assert!(prepared
            .ensure_directory(Path::new("TV_Shows/Owned"), true)
            .is_err());
        assert_eq!(fs::read_dir(foreign).unwrap().count(), 0);
    }
}
