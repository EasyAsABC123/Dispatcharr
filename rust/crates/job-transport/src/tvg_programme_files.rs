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
            let directory = File::open(&spelling)?;
            let m = directory.metadata()?;
            if !m.is_dir() || m.uid() != uid {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "owned root UID",
                ));
            }
            roots.push(Root {
                spelling,
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
            let Ok(relative) = path.strip_prefix(&root.spelling) else {
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
                    || (final_file && (!m.is_file() || m.len() > 4 * 1024 * 1024))
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
