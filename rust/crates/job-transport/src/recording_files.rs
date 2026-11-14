//! PRIVATE UNCOMPILED owned normal Source3 filesystem path.
//! Stable exclusively-owned namespace only: path checks do NOT claim atomic
//! protection against a concurrent external directory replacement/rename.
use super::recording_normal::NormalPaths;
use std::{
    fs::{self, OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
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
    normal: bool,
    existing_hls: Option<Node>,
    root: root_context::Root,
    plugins: Node,
    plugins_path: PathBuf,
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
    pub(crate) fn normal_paths(&self) -> &NormalPaths {
        &self.paths
    }
    fn empty_plugins(path: &Path) -> Result<Node, Error> {
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
        Self::inspect_inner(
            paths,
            expected_uid,
            owner,
            false,
            false,
            PathBuf::from("/data/plugins"),
        )
    }
    /// Normal producer-derived paths, without a captured title/season/poster gate.
    /// Root/path ownership and the stable exclusively-owned namespace are retained.
    pub(crate) fn inspect_normal(
        paths: NormalPaths,
        expected_uid: u32,
        owner: Arc<dyn Send + Sync>,
        reuse_final: bool,
        configured_plugins: PathBuf,
    ) -> Result<Self, Error> {
        Self::inspect_inner(
            paths,
            expected_uid,
            owner,
            true,
            reuse_final,
            configured_plugins,
        )
    }
    fn inspect_inner(
        mut paths: NormalPaths,
        expected_uid: u32,
        owner: Arc<dyn Send + Sync>,
        normal: bool,
        reuse_final: bool,
        plugins_path: PathBuf,
    ) -> Result<Self, Error> {
        let root_path = Path::new("/data/recordings");
        let parent = paths
            .final_path
            .parent()
            .ok_or(Error::Unsupported("output parent"))?;
        if !parent.starts_with(root_path)
            || (!normal && parent.parent() != Some(Path::new("/data/recordings/TV_Shows")))
            || paths.hls_dir.parent() != Some(parent)
            || paths.playlist.parent() != Some(paths.hls_dir.as_path())
            || paths.segments.parent() != Some(paths.hls_dir.as_path())
        {
            return Err(Error::Unsupported("bounded TV path"));
        }
        ancestors(parent)?;
        let root = root_context::Root::inspect(root_path, expected_uid)?;
        if normal && !reuse_final {
            let base = paths.final_path.clone();
            let stem = base
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or(Error::Unsupported("normal output stem"))?;
            let ext = base.extension().and_then(|s| s.to_str()).unwrap_or("");
            let mut counter = 1u64;
            while fs::metadata(&paths.final_path).is_ok_and(|m| m.len() > 0) {
                counter = counter
                    .checked_add(1)
                    .ok_or(Error::Unsupported("normal collision counter capacity"))?;
                paths.final_path = base.with_file_name(format!("{stem}_{counter}.{ext}"));
            }
            paths.filename = paths
                .final_path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or(Error::Unsupported("normal output filename"))?
                .to_string();
        }
        if !reuse_final {
            output_available(&paths.final_path, normal)?;
        }
        let existing_hls = if normal {
            match node(&paths.hls_dir, true) {
                Ok(identity) => Some(identity),
                Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => None,
                Err(error) => return Err(error),
            }
        } else {
            absent(&paths.hls_dir)?;
            None
        };
        let plugins = Self::empty_plugins(&plugins_path)?;
        if !normal {
            let logo = Path::new("/app/media/rust-owned-clocked/logo.png");
            node(logo, false)?;
            let size = fs::symlink_metadata(logo)?.len();
            if size == 0 || size > 65536 {
                return Err(Error::Unsupported("bounded actual owned local poster file"));
            }
        }
        Ok(Self {
            paths,
            normal,
            existing_hls,
            root,
            plugins,
            plugins_path,
            _caller: owner,
        })
    }
    fn verify_root(&self) -> Result<(), Error> {
        self.root.verify()?;
        if Self::empty_plugins(&self.plugins_path)? != self.plugins {
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
        if self.existing_hls.is_none() {
            output_available(&self.paths.final_path, self.normal)?;
            absent(&self.paths.hls_dir)?;
        } else if node(&self.paths.hls_dir, true)?.ne(&self
            .existing_hls
            .ok_or(Error::Unsupported("resume identity"))?)
        {
            return Err(Error::Unsupported("resume HLS changed"));
        }
        let parent = self
            .paths
            .final_path
            .parent()
            .ok_or(Error::Unsupported("parent"))?;
        let relative = parent
            .strip_prefix("/data/recordings")
            .map_err(|_| Error::Unsupported("owned output path escape"))?;
        let parent = if self.normal {
            self.root.ensure_normal_directory(relative, true)?
        } else {
            self.root.ensure_directory(relative, true)?
        };
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
        let hls = if let Some(identity) = self.prepared.existing_hls {
            if node(&self.prepared.paths.hls_dir, true)? != identity {
                return Err(Error::Unsupported("resume HLS changed"));
            }
            identity
        } else {
            output_available(&self.prepared.paths.final_path, self.prepared.normal)?;
            absent(&self.prepared.paths.hls_dir)?;
            fs::create_dir(&self.prepared.paths.hls_dir)?;
            node(&self.prepared.paths.hls_dir, true)?
        };
        Ok(Working {
            prepared: self.prepared,
            hls,
            parent: self.parent,
        })
    }
}
#[derive(Clone, Debug)]
pub(crate) enum Progress {
    Missing,
    Present {
        count: u64,
        max_index: Option<u64>,
        newest_mtime: Option<std::time::SystemTime>,
        playlist_has_segments: bool,
    },
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
    pub(crate) fn normal_progress(&self) -> Result<Progress, Error> {
        match self.verify() {
            Ok(()) => {}
            Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(Progress::Missing)
            }
            Err(error) => return Err(error),
        }
        let mut count = 0u64;
        let mut max_index = None;
        let mut newest: Option<(String, PathBuf)> = None;
        for entry in fs::read_dir(&self.paths().hls_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if !segment_name(name) {
                continue;
            }
            count = count
                .checked_add(1)
                .ok_or(Error::Unsupported("segment count overflow"))?;
            let index = name
                .strip_prefix("seg_")
                .and_then(|s| s.strip_suffix(".ts"))
                .ok_or(Error::Unsupported("segment number"))?
                .parse::<u64>()
                .map_err(|_| Error::Unsupported("segment number overflow"))?;
            max_index = Some(max_index.map_or(index, |prior: u64| prior.max(index)));
            if newest
                .as_ref()
                .is_none_or(|(previous, _)| name > previous.as_str())
            {
                newest = Some((name.into(), entry.path()));
            }
        }
        let newest_mtime = newest.and_then(|(_, path)| fs::metadata(path).ok()?.modified().ok());
        let playlist_has_segments = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&self.paths().playlist)
        {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                let mut line = Vec::with_capacity(8192);
                let mut found = false;
                loop {
                    line.clear();
                    let n = Read::by_ref(&mut reader)
                        .take(8193)
                        .read_until(b'\n', &mut line)?;
                    if n == 0 {
                        break;
                    }
                    if line.len() > 8192 {
                        return Err(Error::Unsupported("playlist line capacity"));
                    }
                    let text = std::str::from_utf8(&line)
                        .map_err(|_| Error::Unsupported("Source UTF8 playlist"))?
                        .trim();
                    if !text.is_empty() && !text.starts_with('#') {
                        found = true;
                        break;
                    }
                }
                found
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        Ok(Progress::Present {
            count,
            max_index,
            newest_mtime,
            playlist_has_segments,
        })
    }
    /// Missing HLS is an observed capture outcome, not invented media. These
    /// methods are called only after the owned capture child/stderr have joined.
    pub(crate) fn normal_playlist_exists(&self) -> Result<bool, Error> {
        match self.verify() {
            Ok(()) => {}
            Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e),
        }
        match node(&self.paths().playlist, false) {
            Ok(_) => Ok(true),
            Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }
    pub(crate) fn normal_segment_bytes(&self) -> Result<u64, Error> {
        if !self.prepared.normal {
            return Err(Error::Unsupported("normal file scope"));
        }
        match self.verify() {
            Ok(()) => {}
            Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        }
        let mut bytes = 0u64;
        for entry in fs::read_dir(&self.paths().hls_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_str().ok_or(Error::Unsupported("filename UTF8"))?;
            if name == "index.m3u8" {
                node(&entry.path(), false)?;
                continue;
            }
            if !segment_name(name) {
                return Err(Error::Unsupported("unknown HLS file retained"));
            }
            node(&entry.path(), false)?;
            bytes = bytes
                .checked_add(fs::symlink_metadata(entry.path())?.len())
                .ok_or(Error::Unsupported("media byte counter overflow"))?;
        }
        Ok(bytes)
    }
    pub(crate) fn normal_final_size(&self) -> Result<Option<u64>, Error> {
        self.prepared.verify_root()?;
        let parent = self
            .paths()
            .final_path
            .parent()
            .ok_or(Error::Unsupported("parent"))?;
        if node(parent, true)? != self.parent {
            return Err(Error::Unsupported("output parent changed"));
        }
        match node(&self.paths().final_path, false) {
            Ok(_) => Ok(Some(fs::symlink_metadata(&self.paths().final_path)?.len())),
            Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
    pub(crate) fn cancel_normal(&self) -> Result<(), Error> {
        if !self.prepared.normal {
            return Err(Error::Unsupported("normal cancellation scope"));
        }
        match self.verify() {
            Ok(()) => self.remove_normal_hls()?,
            Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        // Revalidate root and parent even when another actor already removed
        // HLS. A replacement/symlink is never swept as a cancellation success.
        if self.normal_final_size()?.is_some() {
            fs::remove_file(&self.paths().final_path)?;
        }
        Ok(())
    }
    /// Only after actual capture child/stderr have settled. Match Source's
    /// read/append-newline/temporary-file/replace algorithm within owned bounds.
    pub(crate) fn ensure_endlist(&self) -> Result<(), Error> {
        self.verify()?;
        if self.prepared.normal {
            return self.ensure_normal_endlist();
        }
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
            if (!self.prepared.normal && name.len() != 12)
                || !name.starts_with("seg_")
                || !name.ends_with(".ts")
                || !segment_name(name)
            {
                return Err(Error::Unsupported("unknown HLS file retained"));
            }
            count += 1;
            if !self.prepared.normal && count > 16 {
                return Err(Error::Unsupported("normal segment count bound"));
            }
            node(&entry.path(), false)?;
            let size = fs::symlink_metadata(entry.path())?.len();
            bytes = bytes
                .checked_add(size)
                .filter(|v| self.prepared.normal || *v <= MEDIA_LIMIT)
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
        if size == 0 || (!self.prepared.normal && size > MEDIA_LIMIT) {
            return Err(Error::Unsupported("normal MKV size"));
        }
        Ok(size)
    }
    /// AFTER remux success and actual missing-viewer-key confirmation. Unknown
    /// contents are refused/retained; caller must not clear DB_hls_dir on error.
    pub(crate) fn remove_hls(&self) -> Result<(), Error> {
        if self.prepared.normal {
            return match self.verify() {
                Ok(()) => self.remove_normal_hls(),
                Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => {
                    // A stop/delete API may already have removed HLS. Recheck
                    // the owned root/parent before treating absence as settled.
                    self.normal_final_size()?;
                    Ok(())
                }
                Err(e) => Err(e),
            };
        }
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
    /// Source reads/append/replace semantics with bounded line residency rather
    /// than a whole-playlist or captured-duration cap. Long recordings stream.
    fn ensure_normal_endlist(&self) -> Result<(), Error> {
        self.verify()?;
        let path = &self.paths().playlist;
        let identity = node(path, false)?;
        let open = || {
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(path)
        };
        let mut input = BufReader::new(open()?);
        let mut line = Vec::with_capacity(8192);
        let mut last = None;
        loop {
            line.clear();
            let amount = Read::by_ref(&mut input)
                .take(8193)
                .read_until(b'\n', &mut line)?;
            if amount == 0 {
                break;
            }
            if line.len() > 8192 {
                return Err(Error::Unsupported("playlist line capacity"));
            }
            let text = std::str::from_utf8(&line)
                .map_err(|_| Error::Unsupported("Source UTF8 playlist"))?;
            if text.contains("#EXT-X-ENDLIST") {
                return Ok(());
            }
            last = line.last().copied();
        }
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
        let mut input = open()?;
        io::copy(&mut input, &mut out)?;
        if last.is_some() && last != Some(b'\n') {
            out.write_all(b"\n")?;
        }
        out.write_all(b"#EXT-X-ENDLIST\n")?;
        out.sync_all()?;
        drop(out);
        self.verify()?;
        if node(path, false)? != identity {
            return Err(Error::Unsupported("playlist changed before replace"));
        }
        fs::rename(temporary, path)?;
        Ok(())
    }
    /// Two bounded-residency passes. All entries are qualified before deletion;
    /// capture/remux are physically joined and the caller owns this namespace.
    /// External concurrent replacement remains outside this existing contract.
    fn remove_normal_hls(&self) -> Result<(), Error> {
        for entry in fs::read_dir(&self.paths().hls_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let text = name.to_str().ok_or(Error::Unsupported("filename UTF8"))?;
            if text != "index.m3u8" && !segment_name(text) {
                return Err(Error::Unsupported("unknown HLS file retained"));
            }
            node(&entry.path(), false)?;
        }
        self.verify()?;
        for entry in fs::read_dir(&self.paths().hls_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let text = name.to_str().ok_or(Error::Unsupported("filename UTF8"))?;
            if text != "index.m3u8" && !segment_name(text) {
                return Err(Error::Unsupported("unknown HLS file retained"));
            }
            let path = entry.path();
            let identity = node(&path, false)?;
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

fn segment_name(name: &str) -> bool {
    name.strip_prefix("seg_")
        .and_then(|s| s.strip_suffix(".ts"))
        .is_some_and(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
}

fn output_available(path: &Path, normal: bool) -> Result<(), Error> {
    if !normal {
        return absent(path);
    }
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() && !meta.file_type().is_symlink() && meta.len() == 0 => Ok(()),
        Ok(_) => Err(Error::Unsupported(
            "normal nonempty or unsafe output changed",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Read the final-size observation for expired recovery even when HLS is absent.
/// The normal library root and stable non-symlink ancestry remain explicit.
pub(crate) fn normal_recorded_file_size(
    path: PathBuf,
    uid: u32,
    owner: Arc<dyn Send + Sync>,
) -> Result<Option<u64>, Error> {
    let _owner = owner;
    if !path.starts_with("/data/recordings")
        || path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(Error::Unsupported("normal final path outside root"));
    }
    let root = root_context::Root::inspect(Path::new("/data/recordings"), uid)?;
    ancestors(path.parent().ok_or(Error::Unsupported("final parent"))?)?;
    root.verify()?;
    match node(&path, false) {
        Ok(_) => Ok(Some(fs::symlink_metadata(path)?.len())),
        Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
