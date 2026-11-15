//! Owned configured input roots; serialized task IDs never choose a filesystem root.
//! Existing /app/media symlink may be followed only while opening configured root.
use std::{
    ffi::CString,
    fs::File,
    io,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{ffi::OsStrExt, fs::MetadataExt},
    },
    path::{Component, Path, PathBuf},
};
pub struct Root {
    spelling: PathBuf,
    physical: PathBuf,
    directory: File,
    uid: u32,
}
pub struct LocalFiles {
    roots: Vec<Root>,
}
fn open_at(parent: &File, name: &std::ffi::OsStr, directory: bool) -> io::Result<File> {
    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL path"))?;
    let flags = libc::O_RDONLY
        | libc::O_CLOEXEC
        | libc::O_NOFOLLOW
        | if directory {
            libc::O_DIRECTORY
        } else {
            libc::O_NONBLOCK
        };
    // Exactly the held directory FD + one validated component. No envelope PID,
    // absolute path or symlink traversal enters this OS boundary.
    let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}
impl LocalFiles {
    pub fn prepare(paths: Vec<PathBuf>, uid: u32) -> io::Result<Self> {
        if paths.is_empty() || paths.len() > 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "configured roots",
            ));
        }
        let mut roots = Vec::new();
        for spelling in paths {
            if !spelling.is_absolute() || spelling == Path::new("/") {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "owned root"));
            }
            // Resolve only the trusted configured root once. Source stores
            // /data/... while the packaged configured root is /app/media ->
            // /data. Both spellings must refer to this SAME admitted root FD;
            // descendants still use component-wise O_NOFOLLOW below.
            let physical = std::fs::canonicalize(&spelling)?;
            if physical == Path::new("/") {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "owned root"));
            }
            let directory = File::open(&physical)?;
            let m = directory.metadata()?;
            if !m.is_dir() || m.uid() != uid {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "owned root UID",
                ));
            }
            roots.push(Root {
                spelling,
                physical,
                directory,
                uid,
            });
        }
        Ok(Self { roots })
    }
    pub fn open(&self, path: &str) -> io::Result<File> {
        if path.len() > 4096 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "path bytes"));
        }
        let path = Path::new(path);
        for root in &self.roots {
            let Ok(relative) = path
                .strip_prefix(&root.spelling)
                .or_else(|_| path.strip_prefix(&root.physical))
            else {
                continue;
            };
            let components: Vec<_> = relative.components().collect();
            if components.is_empty()
                || components.len() > 32
                || components
                    .iter()
                    .any(|c| !matches!(c, Component::Normal(_)))
            {
                continue;
            }
            let mut parent = root.directory.try_clone()?;
            for (index, component) in components.iter().enumerate() {
                let Component::Normal(name) = component else {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "component"));
                };
                let final_file = index + 1 == components.len();
                let next = open_at(&parent, name, !final_file)?;
                let m = next.metadata()?;
                if m.uid() != root.uid
                    || (!final_file && !m.is_dir())
                    || (final_file && (!m.is_file() || m.len() > 2 * 1024 * 1024 * 1024))
                {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "owned regular input",
                    ));
                }
                parent = next;
            }
            return Ok(parent);
        }
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "outside configured roots",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::os::unix::fs::symlink;

    #[test]
    fn configured_alias_and_physical_path_share_held_root_without_following_descendants() {
        let base = std::env::temp_dir().join(format!("tvg-root-alias-{}", uuid::Uuid::new_v4()));
        let data = base.join("data");
        let other = base.join("other");
        let alias = base.join("media");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::create_dir(&other).unwrap();
        std::fs::write(data.join("guide.xml"), b"admitted-guide").unwrap();
        std::fs::write(other.join("guide.xml"), b"foreign-guide").unwrap();
        symlink(&data, &alias).unwrap();
        let uid = std::fs::metadata(&data).unwrap().uid();
        let files = LocalFiles::prepare(vec![alias.clone()], uid).unwrap();
        let read = |path: &Path| {
            let mut bytes = Vec::new();
            files
                .open(path.to_str().unwrap())
                .unwrap()
                .read_to_end(&mut bytes)
                .unwrap();
            bytes
        };
        assert_eq!(read(&alias.join("guide.xml")), b"admitted-guide");
        assert_eq!(read(&data.join("guide.xml")), b"admitted-guide");
        // The alias can change after admission, but cannot redirect the held
        // root or its physical spelling to another directory's content.
        std::fs::remove_file(&alias).unwrap();
        symlink(&other, &alias).unwrap();
        assert_eq!(read(&alias.join("guide.xml")), b"admitted-guide");
        assert_eq!(read(&data.join("guide.xml")), b"admitted-guide");
        assert!(files
            .open(other.join("guide.xml").to_str().unwrap())
            .is_err());
        symlink(&other, data.join("escape")).unwrap();
        symlink(other.join("guide.xml"), data.join("file-link.xml")).unwrap();
        assert!(files
            .open(data.join("escape/guide.xml").to_str().unwrap())
            .is_err());
        assert!(files
            .open(data.join("file-link.xml").to_str().unwrap())
            .is_err());
        assert!(files
            .open(alias.join("../other/guide.xml").to_str().unwrap())
            .is_err());
        drop(files);
        std::fs::remove_dir_all(base).unwrap();
    }
}
