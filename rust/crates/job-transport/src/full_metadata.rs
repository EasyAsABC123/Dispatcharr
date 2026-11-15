//! Full normal UUID-task metadata on retained, bounded private files.
//! Original Dispatcharr/Celery/serde authors retain protocol and algorithm credit.
//! This is not a named-task API: existing named recording transports stay intact.
use crate::transport::Error;
use std::{
    fs::{File, OpenOptions},
    future::Future,
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};
use uuid::Uuid;
pub(crate) const MAX_BYTES: u64 = 512 * 1024 * 1024;
pub(crate) const CHUNK: usize = 65536;
type Owner = Arc<dyn Send + Sync>;
pub(crate) type ChildPage<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<(i64, Uuid)>, Error>> + Send + 'a>>;
/// Implement only for sealed actual broker-publication receipt plans. A caller's
/// invented UUID vector is not an implementation of this authority boundary.
pub(crate) trait ActualPublishedChildren: Send + Sync {
    fn parent_task_id(&self) -> Uuid;
    fn root_task_id(&self) -> Uuid;
    fn children(&self, after: i64) -> ChildPage<'_>;
}
struct Scratch {
    file: std::io::BufWriter<File>,
    path: PathBuf,
    identity: (u64, u64),
    directory: PathBuf,
    directory_identity: (u64, u64),
    bytes: u64,
    _owner: Owner,
}
impl Drop for Scratch {
    fn drop(&mut self) {
        // A replaced pathname is not our file and must not be removed.
        if std::fs::symlink_metadata(&self.path).is_ok_and(|m| (m.dev(), m.ino()) == self.identity)
        {
            let _ = std::fs::remove_file(&self.path);
        }
        if std::fs::symlink_metadata(&self.directory)
            .is_ok_and(|m| (m.dev(), m.ino()) == self.directory_identity)
        {
            let _ = std::fs::remove_dir(&self.directory);
        }
    }
}
impl Scratch {
    fn create(directory: &Path, owner: Owner) -> Result<Self, Error> {
        if !directory.is_absolute()
            || directory.components().any(|c| {
                matches!(
                    c,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
        {
            return Err(Error::Configuration);
        }
        for ancestor in directory.ancestors() {
            let m = std::fs::symlink_metadata(ancestor).map_err(|_| Error::Backend)?;
            if !m.is_dir() || m.file_type().is_symlink() {
                return Err(Error::Configuration);
            }
        }
        let directory = directory.join(format!(".full-metadata-{}", Uuid::new_v4()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .map_err(|_| Error::Backend)?;
        let built = (|| {
            let dm = std::fs::symlink_metadata(&directory).map_err(|_| Error::Backend)?;
            // SAFETY: geteuid has no preconditions and only reads process identity.
            if dm.uid() != unsafe { libc::geteuid() }
                || dm.mode() & 0o777 != 0o700
                || !dm.is_dir()
                || dm.file_type().is_symlink()
            {
                return Err(Error::Configuration);
            }
            let path = directory.join("payload.json");
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path)
                .map_err(|_| Error::Backend)?;
            let m = file.metadata().map_err(|_| Error::Backend)?;
            Ok(Self {
                file: std::io::BufWriter::with_capacity(CHUNK, file),
                path,
                identity: (m.dev(), m.ino()),
                directory: directory.clone(),
                directory_identity: (dm.dev(), dm.ino()),
                bytes: 0,
                _owner: owner,
            })
        })();
        if built.is_err() {
            let _ = std::fs::remove_file(directory.join("payload.json"));
            let _ = std::fs::remove_dir(&directory);
        }
        built
    }
    fn rewind(&mut self) -> Result<(), Error> {
        self.file.flush().map_err(|_| Error::Backend)?;
        self.file
            .get_mut()
            .seek(SeekFrom::Start(0))
            .map_err(|_| Error::Backend)?;
        Ok(())
    }
    fn chunk(&mut self) -> Result<Option<Vec<u8>>, Error> {
        let mut b = vec![0; CHUNK];
        let n = self
            .file
            .get_mut()
            .read(&mut b)
            .map_err(|_| Error::Backend)?;
        b.truncate(n);
        Ok((n != 0).then_some(b))
    }
}
impl Write for Scratch {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .checked_add(bytes.len() as u64)
            .filter(|n| *n <= MAX_BYTES)
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::FileTooLarge, "Redis metadata ceiling")
            })?;
        self.file.write_all(bytes)?;
        self.bytes = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
/// Child data can outlive each SQL-backed receipt plan. Only bounded pages are
/// materialized, and the same accepted owner lives until FD close/unlink.
pub(crate) struct ChildrenSpool {
    scratch: Option<Scratch>,
    parent: Uuid,
    root: Uuid,
    count: u64,
    poisoned: bool,
}
impl ChildrenSpool {
    /// Disk operations: invoke in the caller's retained blocking preparation.
    pub(crate) fn create(
        directory: &Path,
        parent: Uuid,
        root: Uuid,
        owner: Owner,
    ) -> Result<Self, Error> {
        Ok(Self {
            scratch: Some(Scratch::create(directory, owner)?),
            parent,
            root,
            count: 0,
            poisoned: false,
        })
    }
    pub(crate) async fn append_actual(
        &mut self,
        actual: &impl ActualPublishedChildren,
    ) -> Result<(), Error> {
        if self.poisoned
            || self.scratch.is_none()
            || actual.parent_task_id() != self.parent
            || actual.root_task_id() != self.root
        {
            self.poisoned = true;
            return Err(Error::Protocol);
        }
        // Persist the incomplete state across every await/cancellation. Only an
        // exhausted actual receipt plan can make this spool sealable again.
        self.poisoned = true;
        let mut after = 0i64;
        loop {
            let page = actual.children(after).await?;
            if page.is_empty() {
                self.poisoned = false;
                return Ok(());
            }
            if page.len() > 500
                || page.iter().any(|(ordinal, _)| *ordinal <= after)
                || page.windows(2).any(|p| p[0].0 >= p[1].0)
            {
                self.poisoned = true;
                return Err(Error::Protocol);
            }
            after = page.last().ok_or(Error::Protocol)?.0;
            let mut scratch = self.scratch.take().ok_or(Error::Protocol)?;
            let count = self.count;
            // Cancellation leaves self without a file, never a publishable prefix;
            // the actual blocking writer retains the FD and accepted owner.
            let (returned, written) = tokio::task::spawn_blocking(move || {
                let result = (|| {
                    for (index, (_, child)) in page.iter().enumerate() {
                        if count != 0 || index != 0 {
                            scratch.write_all(b",").map_err(|_| Error::Backend)?;
                        }
                        serde_json::to_writer(
                            &mut scratch,
                            &serde_json::json!([[child.to_string(), null], null]),
                        )
                        .map_err(|_| Error::Backend)?;
                    }
                    Ok::<u64, Error>(page.len() as u64)
                })();
                (scratch, result)
            })
            .await
            .map_err(|_| Error::Backend)?;
            self.scratch = Some(returned);
            match written {
                Ok(n) => self.count = self.count.checked_add(n).ok_or(Error::Protocol)?,
                Err(e) => {
                    self.poisoned = true;
                    return Err(e);
                }
            }
        }
    }
}
/// Complete JSON value, not an arbitrary public path or unvalidated child list.
pub(crate) struct ResultSpool {
    scratch: Scratch,
    id: Uuid,
}
impl ResultSpool {
    /// The result object already belongs to the task algorithm; serialization
    /// streams to disk and never builds another whole metadata Value/byte vector.
    /// Invoke in retained spawn_blocking, as with GroupsResultFile preparation.
    pub(crate) fn success(
        directory: &Path,
        id: Uuid,
        parent: Option<Uuid>,
        result: &serde_json::Value,
        mut children: ChildrenSpool,
        owner: Owner,
    ) -> Result<Self, Error> {
        if children.poisoned || children.parent != id {
            return Err(Error::Protocol);
        }
        let mut child_file = children.scratch.take().ok_or(Error::Protocol)?;
        child_file.rewind()?;
        let mut scratch = Scratch::create(directory, owner)?;
        scratch
            .write_all(b"{\"status\":\"SUCCESS\",\"result\":")
            .map_err(|_| Error::Backend)?;
        serde_json::to_writer(&mut scratch, result).map_err(|_| Error::Backend)?;
        scratch
            .write_all(b",\"children\":[")
            .map_err(|_| Error::Backend)?;
        while let Some(chunk) = child_file.chunk()? {
            scratch.write_all(&chunk).map_err(|_| Error::Backend)?;
        }
        scratch.write_all(b"],").map_err(|_| Error::Backend)?;
        let mut tail = serde_json::json!({"traceback":null,"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":id.to_string()});
        if let Some(parent) = parent {
            tail["parent_id"] = serde_json::json!(parent.to_string());
        }
        let tail = serde_json::to_vec(&tail).map_err(|_| Error::Protocol)?;
        scratch.write_all(&tail[1..]).map_err(|_| Error::Backend)?;
        scratch.flush().map_err(|_| Error::Backend)?;
        scratch.rewind()?;
        Ok(Self { scratch, id })
    }
    pub(crate) fn task_id(&self) -> Uuid {
        self.id
    }
    pub(crate) fn bytes(&self) -> u64 {
        self.scratch.bytes
    }
    pub(crate) fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, Error> {
        self.scratch.chunk()
    }
}
/// A coherent Redis snapshot copied to a private owned FD. Raw bounded reads are
/// available without allocating a full backend result or child vector.
pub struct MetadataSnapshot {
    scratch: Scratch,
    id: Uuid,
}
impl MetadataSnapshot {
    pub(crate) fn create(directory: &Path, id: Uuid, owner: Owner) -> Result<Self, Error> {
        Ok(Self {
            scratch: Scratch::create(directory, owner)?,
            id,
        })
    }
    pub(crate) fn append(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.scratch.write_all(bytes).map_err(|_| Error::Backend)
    }
    pub(crate) fn seal(&mut self) -> Result<(), Error> {
        self.scratch.rewind()
    }
    pub fn bytes(&self) -> u64 {
        self.scratch.bytes
    }
    pub fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, Error> {
        self.scratch.chunk()
    }
    pub fn task_id(&self) -> Uuid {
        self.id
    }
}
/// Bounded projection: result/traceback/unknown values are validated/skipped,
/// never retained. Each real child tuple is visited individually in file order.
#[derive(Debug, PartialEq, Eq)]
pub struct Projection {
    pub task_id: Uuid,
    pub parent_id: Option<Uuid>,
    pub status: String,
    pub children: u64,
}
// serde's ignored value walker does not retain the skipped result. Its normal
// string parser does retain envelope keys/scalars: cap those lexically BEFORE
// handing bytes to serde, while exempting the literal Source `result` payload.
struct EnvelopeRead<R> {
    inner: R,
    depth: usize,
    string: bool,
    escape: bool,
    length: usize,
    key: Vec<u8>,
    is_key: bool,
    result_key: bool,
    result: bool,
}
impl<R: Read> EnvelopeRead<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            depth: 0,
            string: false,
            escape: false,
            length: 0,
            key: Vec::new(),
            is_key: false,
            result_key: false,
            result: false,
        }
    }
    fn accept(&mut self, byte: u8) -> std::io::Result<()> {
        if self.string {
            self.length += 1;
            if !self.result && self.length > CHUNK {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "oversize metadata envelope token",
                ));
            }
            if self.escape {
                self.escape = false;
                return Ok(());
            }
            if byte == b'\\' {
                self.escape = true;
                if self.depth == 1 && self.is_key {
                    self.key.push(byte);
                }
            } else if byte == b'"' {
                self.string = false;
                if self.depth == 1 && self.is_key {
                    self.result_key = self.key == b"result";
                }
            } else if self.depth == 1 && self.is_key {
                self.key.push(byte);
            }
        } else {
            match byte {
                b'"' => {
                    self.string = true;
                    self.length = 0;
                    self.key.clear();
                }
                b'{' | b'[' => {
                    self.depth += 1;
                    if self.depth == 1 {
                        self.is_key = true;
                    }
                }
                b'}' | b']' => {
                    self.depth = self.depth.saturating_sub(1);
                }
                b':' if self.depth == 1 => {
                    self.is_key = false;
                    self.result = self.result_key;
                }
                b',' if self.depth == 1 => {
                    self.is_key = true;
                    self.result = false;
                    self.result_key = false;
                }
                _ => {}
            }
        }
        Ok(())
    }
}
impl<R: Read> Read for EnvelopeRead<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(out)?;
        for byte in &out[..n] {
            self.accept(*byte)?;
        }
        Ok(n)
    }
}
impl MetadataSnapshot {
    /// Run in retained spawn_blocking. Successful return validates the WHOLE JSON
    /// and EOF. A visitor must not commit side effects before that return: later
    /// malformed bytes can invalidate earlier child callbacks.
    pub fn inspect(
        &mut self,
        child: &mut impl FnMut(Uuid) -> Result<(), Error>,
    ) -> Result<Projection, Error> {
        use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
        use std::fmt;
        struct Children<'a, F>(&'a mut F);
        impl<'de, F: FnMut(Uuid) -> Result<(), Error>> DeserializeSeed<'de> for Children<'_, F> {
            type Value = u64;
            fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<u64, D::Error> {
                struct V<'a, F>(&'a mut F);
                impl<'de, F: FnMut(Uuid) -> Result<(), Error>> Visitor<'de> for V<'_, F> {
                    type Value = u64;
                    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                        f.write_str("Celery child result tuples")
                    }
                    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<u64, A::Error> {
                        let mut n = 0u64;
                        while let Some(((id, backend), result)) =
                            a.next_element::<((String, Option<String>), Option<String>)>()?
                        {
                            if backend.is_some() || result.is_some() {
                                return Err(serde::de::Error::custom("unsupported child backend"));
                            }
                            let id = Uuid::parse_str(&id).map_err(serde::de::Error::custom)?;
                            (self.0)(id)
                                .map_err(|_| serde::de::Error::custom("child visitor refused"))?;
                            n = n
                                .checked_add(1)
                                .ok_or_else(|| serde::de::Error::custom("child count overflow"))?;
                        }
                        Ok(n)
                    }
                }
                d.deserialize_seq(V(self.0))
            }
        }
        struct Envelope<'a, F>(&'a mut F);
        impl<'de, F: FnMut(Uuid) -> Result<(), Error>> DeserializeSeed<'de> for Envelope<'_, F> {
            type Value = Projection;
            fn deserialize<D: serde::Deserializer<'de>>(
                self,
                d: D,
            ) -> Result<Projection, D::Error> {
                struct V<'a, F>(&'a mut F);
                impl<'de, F: FnMut(Uuid) -> Result<(), Error>> Visitor<'de> for V<'_, F> {
                    type Value = Projection;
                    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                        f.write_str("Source result metadata object")
                    }
                    fn visit_map<A: MapAccess<'de>>(
                        self,
                        mut a: A,
                    ) -> Result<Projection, A::Error> {
                        let (mut id, mut parent, mut state, mut children) =
                            (None, None, None, None);
                        let mut parent_seen = false;
                        while let Some(key) = a.next_key::<String>()? {
                            match key.as_str() {
                                "task_id" => {
                                    if id.is_some() {
                                        return Err(serde::de::Error::custom("duplicate task_id"));
                                    }
                                    let raw = a.next_value::<String>()?;
                                    id = Some(
                                        Uuid::parse_str(&raw).map_err(serde::de::Error::custom)?,
                                    );
                                }
                                "parent_id" => {
                                    if parent_seen {
                                        return Err(serde::de::Error::custom(
                                            "duplicate parent_id",
                                        ));
                                    }
                                    parent_seen = true;
                                    parent = a
                                        .next_value::<Option<String>>()?
                                        .map(|s| Uuid::parse_str(&s))
                                        .transpose()
                                        .map_err(serde::de::Error::custom)?;
                                }
                                "status" => {
                                    if state.is_some() {
                                        return Err(serde::de::Error::custom("duplicate status"));
                                    }
                                    let s = a.next_value::<String>()?;
                                    if !matches!(
                                        s.as_str(),
                                        "PENDING"
                                            | "STARTED"
                                            | "RETRY"
                                            | "SUCCESS"
                                            | "FAILURE"
                                            | "REVOKED"
                                    ) {
                                        return Err(serde::de::Error::custom("unsupported status"));
                                    }
                                    state = Some(s);
                                }
                                "children" => {
                                    if children.is_some() {
                                        return Err(serde::de::Error::custom("duplicate children"));
                                    }
                                    children = Some(a.next_value_seed(Children(&mut *self.0))?);
                                }
                                _ => {
                                    a.next_value::<IgnoredAny>()?;
                                }
                            }
                        }
                        Ok(Projection {
                            task_id: id
                                .ok_or_else(|| serde::de::Error::custom("missing task_id"))?,
                            parent_id: parent,
                            status: state
                                .ok_or_else(|| serde::de::Error::custom("missing status"))?,
                            children: children
                                .ok_or_else(|| serde::de::Error::custom("missing children"))?,
                        })
                    }
                }
                d.deserialize_map(V(self.0))
            }
        }
        self.scratch.rewind()?;
        let source = EnvelopeRead::new(std::io::BufReader::with_capacity(
            CHUNK,
            self.scratch.file.get_mut(),
        ));
        let mut d = serde_json::Deserializer::from_reader(source);
        let projection = Envelope(child)
            .deserialize(&mut d)
            .map_err(|_| Error::Protocol)?;
        d.end().map_err(|_| Error::Protocol)?;
        if projection.task_id != self.id {
            return Err(Error::Protocol);
        }
        self.scratch.rewind()?;
        Ok(projection)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    struct Plan {
        parent: Uuid,
        root: Uuid,
        count: i64,
        bad: bool,
    }
    impl ActualPublishedChildren for Plan {
        fn parent_task_id(&self) -> Uuid {
            self.parent
        }
        fn root_task_id(&self) -> Uuid {
            self.root
        }
        fn children(&self, after: i64) -> ChildPage<'_> {
            Box::pin(async move {
                if self.bad {
                    return Ok(vec![(0, Uuid::new_v4())]);
                }
                Ok(((after + 1)..=(after + 500).min(self.count))
                    .map(|i| (i, Uuid::from_u128(i as u128)))
                    .collect())
            })
        }
    }
    fn directory() -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("dispatcharr-full-metadata-{}", Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        path
    }
    #[tokio::test]
    async fn full_children_and_result_survive_old_capacity_without_whole_metadata_vector() {
        let dir = directory();
        let id = Uuid::new_v4();
        let root = Uuid::new_v4();
        let owner: Owner = Arc::new(());
        let mut children = ChildrenSpool::create(&dir, id, root, owner.clone()).unwrap();
        children
            .append_actual(&Plan {
                parent: id,
                root,
                count: 10000,
                bad: false,
            })
            .await
            .unwrap();
        assert_eq!(children.count, 10000);
        let report = serde_json::json!({"description":"x".repeat(300000)});
        let mut result =
            ResultSpool::success(&dir, id, None, &report, children, owner.clone()).unwrap();
        assert!(result.bytes() > crate::decode::MAX_RESULT_BYTES as u64);
        let mut snapshot = MetadataSnapshot::create(&dir, id, owner).unwrap();
        while let Some(chunk) = result.next_chunk().unwrap() {
            assert!(chunk.len() <= CHUNK);
            snapshot.append(&chunk).unwrap();
        }
        snapshot.seal().unwrap();
        let mut seen = 0u64;
        let projection = snapshot
            .inspect(&mut |child| {
                seen += 1;
                assert_eq!(child, Uuid::from_u128(seen as u128));
                Ok(())
            })
            .unwrap();
        assert_eq!(projection.children, 10000);
        assert_eq!(projection.status, "SUCCESS");
        assert_eq!(seen, 10000);
        drop(snapshot);
        drop(result);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        std::fs::remove_dir(dir).unwrap();
    }
    #[tokio::test]
    async fn wrong_parent_or_bad_real_cursor_cannot_seal_children() {
        let dir = directory();
        let id = Uuid::new_v4();
        let root = Uuid::new_v4();
        let owner: Owner = Arc::new(());
        let mut children = ChildrenSpool::create(&dir, id, root, owner.clone()).unwrap();
        assert_eq!(
            children
                .append_actual(&Plan {
                    parent: Uuid::new_v4(),
                    root,
                    count: 1,
                    bad: false
                })
                .await,
            Err(Error::Protocol)
        );
        drop(children);
        let mut children = ChildrenSpool::create(&dir, id, root, owner.clone()).unwrap();
        assert_eq!(
            children
                .append_actual(&Plan {
                    parent: id,
                    root,
                    count: 1,
                    bad: true
                })
                .await,
            Err(Error::Protocol)
        );
        assert!(
            ResultSpool::success(&dir, id, None, &serde_json::Value::Null, children, owner)
                .is_err()
        );
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        std::fs::remove_dir(dir).unwrap();
    }
    #[tokio::test]
    async fn cancelled_receipt_read_cannot_publish_a_complete_child_list() {
        struct Pending(Uuid);
        impl ActualPublishedChildren for Pending {
            fn parent_task_id(&self) -> Uuid {
                self.0
            }
            fn root_task_id(&self) -> Uuid {
                self.0
            }
            fn children(&self, _: i64) -> ChildPage<'_> {
                Box::pin(std::future::pending())
            }
        }
        let dir = directory();
        let id = Uuid::new_v4();
        let owner: Owner = Arc::new(());
        let mut children = ChildrenSpool::create(&dir, id, id, owner.clone()).unwrap();
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(1),
            children.append_actual(&Pending(id))
        )
        .await
        .is_err());
        assert!(
            ResultSpool::success(&dir, id, None, &serde_json::Value::Null, children, owner)
                .is_err()
        );
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        std::fs::remove_dir(dir).unwrap();
    }
    #[test]
    fn full_projection_rejects_wrong_identity_trailing_damage_and_unbounded_envelope_token() {
        let dir = directory();
        let id = Uuid::new_v4();
        for raw in [
            format!("{{\"task_id\":\"{}\",\"status\":\"SUCCESS\",\"children\":[]}}",Uuid::new_v4()),
            format!("{{\"task_id\":\"{id}\",\"status\":\"SUCCESS\",\"children\":[]}}!"),
            format!("{{\"task_id\":\"{id}\",\"status\":\"{}\",\"children\":[]}}","S".repeat(CHUNK+1)),
            format!("{{\"task_id\":\"{id}\",\"status\":\"SUCCESS\",\"children\":[],\"task_id\":\"{id}\"}}"),
        ] {
            let mut snapshot=MetadataSnapshot::create(&dir,id,Arc::new(())).unwrap();snapshot.append(raw.as_bytes()).unwrap();
            assert_eq!(snapshot.inspect(&mut |_|Ok(())),Err(Error::Protocol));
        }
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        std::fs::remove_dir(dir).unwrap();
    }
    #[test]
    fn unlink_guard_never_unlinks_a_replacement_path() {
        let dir = directory();
        let scratch = Scratch::create(&dir, Arc::new(())).unwrap();
        let path = scratch.path.clone();
        let private_directory = scratch.directory.clone();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        drop(scratch);
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(private_directory).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }
}
