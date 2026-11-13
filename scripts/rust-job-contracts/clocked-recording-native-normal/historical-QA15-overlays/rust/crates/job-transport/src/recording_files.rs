//! PRIVATE UNCOMPILED owned normal Source3 filesystem path.
//! Stable exclusively-owned namespace only: path checks do NOT claim atomic
//! protection against a concurrent external directory replacement/rename.
use super::recording_normal::NormalPaths;
use std::{
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::Arc,
};

#[path = "recording_root.rs"]
mod root_context;

const PLAYLIST_LIMIT: u64 = 65_536;
const MEDIA_LIMIT: u64 = 64 * 1024 * 1024;
#[derive(Debug)]
pub enum Error {
    Unsupported(&'static str),
    Io(io::Error),
}
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Node {
    dev: u64,
    ino: u64,
}
fn node(path: &Path, directory: bool) -> Result<Node, Error> {
    let meta = fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink()
        || (directory && !meta.is_dir())
        || (!directory && !meta.is_file())
    {
        return Err(Error::Unsupported("owned non-symlink node"));
    }
    Ok(Node {
        dev: meta.dev(),
        ino: meta.ino(),
    })
}
fn absent(path: &Path) -> Result<(), Error> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
        Ok(_) => Err(Error::Unsupported("fresh output already exists")),
    }
}
fn ancestors(path: &Path) -> Result<(), Error> {
    for parent in path.ancestors() {
        match fs::symlink_metadata(parent) {
            Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => {}
            Ok(_) => return Err(Error::Unsupported("directory ancestry")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
/// Read-only preflight data, never a runnable due capability. Actual root inode
/// and missing outputs are checked again at each physically owned operation.
pub(crate) struct Prepared {
    paths: NormalPaths,
    root: root_context::Root,
    plugins: Node,
    _caller: Arc<dyn Send + Sync>,
}
pub(crate) struct Working {
    prepared: Prepared,
    hls: Node,
    parent: Node,
}
pub(crate) struct Parent {
    prepared: Prepared,
    parent: Node,
}
impl Prepared {
    fn empty_plugins() -> Result<Node, Error> {
        let path = Path::new("/data/plugins");
        let identity = node(path, true)?;
        match fs::read_dir(path)?.next() {
            None => Ok(identity),
            Some(Ok(_)) => Err(Error::Unsupported(
                "configured plugin directory must stay empty",
            )),
            Some(Err(error)) => Err(error.into()),
        }
    }
    pub(crate) fn inspect(
        paths: NormalPaths,
        expected_uid: u32,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Self, Error> {
        let root_path = Path::new("/data/recordings");
        let parent = paths
            .final_path
            .parent()
            .ok_or(Error::Unsupported("output parent"))?;
        if !parent.starts_with(root_path)
            || parent.parent() != Some(Path::new("/data/recordings/TV_Shows"))
            || paths.hls_dir.parent() != Some(parent)
            || paths.playlist.parent() != Some(paths.hls_dir.as_path())
            || paths.segments.parent() != Some(paths.hls_dir.as_path())
        {
            return Err(Error::Unsupported("bounded TV path"));
        }
        ancestors(parent)?;
        let root = root_context::Root::inspect(root_path, expected_uid)?;
        absent(&paths.final_path)?;
        absent(&paths.hls_dir)?;
        let plugins = Self::empty_plugins()?;
        let logo = Path::new("/app/media/rust-owned-clocked/logo.png");
        node(logo, false)?;
        let size = fs::symlink_metadata(logo)?.len();
        if size == 0 || size > 65536 {
            return Err(Error::Unsupported("bounded actual owned local poster file"));
        }
        Ok(Self {
            paths,
            root,
            plugins,
            _caller: owner,
        })
    }
    fn verify_root(&self) -> Result<(), Error> {
        self.root.verify()?;
        if Self::empty_plugins()? != self.plugins {
            return Err(Error::Unsupported("configured plugin directory changed"));
        }
        ancestors(
            self.paths
                .final_path
                .parent()
                .ok_or(Error::Unsupported("parent"))?,
        )
    }
    /// AFTER normal task admission/event ordering. No file placeholder is made.
    pub(crate) fn create_parent(mut self) -> Result<Parent, Error> {
        self.verify_root()?;
        absent(&self.paths.final_path)?;
        absent(&self.paths.hls_dir)?;
        let parent = self
            .paths
            .final_path
            .parent()
            .ok_or(Error::Unsupported("parent"))?;
        let relative = parent
            .strip_prefix("/data/recordings")
            .map_err(|_| Error::Unsupported("owned output path escape"))?;
        let parent = self.root.ensure_directory(relative, true)?;
        self.verify_root()?;
        Ok(Parent {
            prepared: self,
            parent,
        })
    }
}
impl Parent {
    /// Source creates HLS only after metadata save/recording_updated publication.
    pub(crate) fn start_hls(self) -> Result<Working, Error> {
        self.prepared.verify_root()?;
        if node(
            self.prepared
                .paths
                .final_path
                .parent()
                .ok_or(Error::Unsupported("parent"))?,
            true,
        )? != self.parent
        {
            return Err(Error::Unsupported("output parent changed"));
        }
        absent(&self.prepared.paths.final_path)?;
        absent(&self.prepared.paths.hls_dir)?;
        fs::create_dir(&self.prepared.paths.hls_dir)?;
        let hls = node(&self.prepared.paths.hls_dir, true)?;
        Ok(Working {
            prepared: self.prepared,
            hls,
            parent: self.parent,
        })
    }
}
impl Working {
    pub(crate) fn paths(&self) -> &NormalPaths {
        &self.prepared.paths
    }
    fn verify(&self) -> Result<(), Error> {
        self.prepared.verify_root()?;
        if node(
            self.paths()
                .final_path
                .parent()
                .ok_or(Error::Unsupported("parent"))?,
            true,
        )? != self.parent
            || node(&self.paths().hls_dir, true)? != self.hls
        {
            return Err(Error::Unsupported("media directories changed"));
        }
        Ok(())
    }
    /// Only after actual capture child/stderr have settled. Match Source's
    /// read/append-newline/temporary-file/replace algorithm within owned bounds.
    pub(crate) fn ensure_endlist(&self) -> Result<(), Error> {
        self.verify()?;
        let path = &self.paths().playlist;
        let identity = node(path, false)?;
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        let metadata = file.metadata()?;
        if (Node {
            dev: metadata.dev(),
            ino: metadata.ino(),
        }) != identity
            || metadata.len() > PLAYLIST_LIMIT
        {
            return Err(Error::Unsupported("playlist bound/identity"));
        }
        let mut bytes = Vec::new();
        Read::by_ref(&mut file)
            .take(PLAYLIST_LIMIT + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > PLAYLIST_LIMIT {
            return Err(Error::Unsupported("playlist grew"));
        }
        let mut text =
            String::from_utf8(bytes).map_err(|_| Error::Unsupported("Source UTF8 playlist"))?;
        if text.contains("#EXT-X-ENDLIST") {
            return Ok(());
        }
        if !text.is_empty() && !text.ends_with('\n') {
            text.push('\n');
        }
        text.push_str("#EXT-X-ENDLIST\n");
        let temporary = PathBuf::from(format!(
            "{}.endlist.tmp",
            path.to_str()
                .ok_or(Error::Unsupported("playlist UTF8 path"))?
        ));
        let mut out = OpenOptions::new()
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temporary)?;
        out.write_all(text.as_bytes())?;
        out.sync_all()?;
        drop(out);
        self.verify()?;
        if node(path, false)? != identity {
            return Err(Error::Unsupported("playlist changed before replace"));
        }
        fs::rename(temporary, path)?;
        Ok(())
    }
    /// Retain all discovered names; never sort/delete a broad foreign tree.
    /// Source3's initial30s domain is bounded to16segments/64MiB total here.
    pub(crate) fn segment_bytes(&self) -> Result<u64, Error> {
        self.verify()?;
        let mut count = 0_usize;
        let mut bytes = 0_u64;
        for entry in fs::read_dir(&self.paths().hls_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_str().ok_or(Error::Unsupported("filename UTF8"))?;
            if name == "index.m3u8" {
                node(&entry.path(), false)?;
                continue;
            }
            if name.len() != 12
                || !name.starts_with("seg_")
                || !name.ends_with(".ts")
                || !name.as_bytes()[4..9].iter().all(u8::is_ascii_digit)
            {
                return Err(Error::Unsupported("unknown HLS file retained"));
            }
            count += 1;
            if count > 16 {
                return Err(Error::Unsupported("normal segment count bound"));
            }
            node(&entry.path(), false)?;
            let size = fs::symlink_metadata(entry.path())?.len();
            bytes = bytes
                .checked_add(size)
                .filter(|v| *v <= MEDIA_LIMIT)
                .ok_or(Error::Unsupported("normal media byte bound"))?;
        }
        if count == 0 || bytes == 0 {
            return Err(Error::Unsupported("no actual captured segments"));
        }
        Ok(bytes)
    }
    pub(crate) fn final_size(&self) -> Result<u64, Error> {
        self.prepared.verify_root()?;
        if node(
            self.paths()
                .final_path
                .parent()
                .ok_or(Error::Unsupported("parent"))?,
            true,
        )? != self.parent
        {
            return Err(Error::Unsupported("output parent changed"));
        }
        node(&self.paths().final_path, false)?;
        let size = fs::symlink_metadata(&self.paths().final_path)?.len();
        if size == 0 || size > MEDIA_LIMIT {
            return Err(Error::Unsupported("normal MKV size"));
        }
        Ok(size)
    }
    /// AFTER remux success and actual missing-viewer-key confirmation. Unknown
    /// contents are refused/retained; caller must not clear DB_hls_dir on error.
    pub(crate) fn remove_hls(&self) -> Result<(), Error> {
        self.segment_bytes()?;
        // Capture/remux have joined; exclusive owned namespace is still required.
        let mut files = Vec::new();
        for entry in fs::read_dir(&self.paths().hls_dir)? {
            let entry = entry?;
            if files.len() == 17 {
                return Err(Error::Unsupported("HLS entry bound"));
            }
            let name = entry.file_name();
            let name = name.to_str().ok_or(Error::Unsupported("filename UTF8"))?;
            let segment = name.len() == 12
                && name.starts_with("seg_")
                && name.ends_with(".ts")
                && name.as_bytes()[4..9].iter().all(u8::is_ascii_digit);
            if name != "index.m3u8" && !segment {
                return Err(Error::Unsupported("unknown HLS file retained"));
            }
            let path = entry.path();
            let identity = node(&path, false)?;
            files.push((path, identity));
        }
        for (path, identity) in files {
            if node(&path, false)? != identity {
                return Err(Error::Unsupported("HLS file changed"));
            }
            fs::remove_file(path)?;
        }
        self.verify()?;
        fs::remove_dir(&self.paths().hls_dir)?;
        Ok(())
    }
}
