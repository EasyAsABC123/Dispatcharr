//! Bounded chunk append/atomic full result publication through shared Jobs.
//! Original Celery/Redis/Dispatcharr authors retain all protocol credit.
use super::*;
use crate::{
    full_metadata::{MetadataSnapshot, ResultSpool},
    m3u_result_file::GroupsResultFile,
};
use std::path::Path;
trait ChunkSource: Send + 'static {
    fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, Error>;
}
impl ChunkSource for GroupsResultFile {
    fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, Error> {
        GroupsResultFile::next_chunk(self)
    }
}
impl ChunkSource for ResultSpool {
    fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, Error> {
        ResultSpool::next_chunk(self)
    }
}
// Temporary data never matches the public celery-task-meta key namespace. The
// actual random owner token and renewed TTL prevent a crashed writer leaking it.
const BEGIN:&str="if redis.call('EXISTS',KEYS[1])~=0 or redis.call('EXISTS',KEYS[2])~=0 then return 0 end redis.call('SET',KEYS[1],ARGV[1],'EX',60);redis.call('SET',KEYS[2],'','EX',60);return 1";
const APPEND:&str="if redis.call('GET',KEYS[1])~=ARGV[1] then return -1 end if redis.call('STRLEN',KEYS[2])~=tonumber(ARGV[2]) then return -1 end redis.call('APPEND',KEYS[2],ARGV[3]);redis.call('EXPIRE',KEYS[1],60);redis.call('EXPIRE',KEYS[2],60);return 1";
const COMMIT: &str = r#"
if redis.call('GET',KEYS[1])~=ARGV[1] then return -1 end
if redis.call('STRLEN',KEYS[2])~=tonumber(ARGV[2]) then return -1 end
local kind=redis.call('TYPE',KEYS[3]).ok
if kind~='none'and kind~='string'then return -1 end
if kind=='string'then
 local ok,old=pcall(cjson.decode,redis.call('GET',KEYS[3]))
 if not ok or type(old)~='table'then return -1 end
 if old.status=='SUCCESS'then redis.call('DEL',KEYS[1],KEYS[2]);return 0 end
end
redis.call('RENAME',KEYS[2],KEYS[3]);redis.call('EXPIRE',KEYS[3],3600)
local value=redis.call('GET',KEYS[3]);redis.call('PUBLISH',KEYS[3],value);redis.call('DEL',KEYS[1]);return 1
"#;
impl Jobs {
    pub(crate) async fn write_m3u_full_result(
        &self,
        id: Uuid,
        parent: Option<Uuid>,
        children: Vec<Uuid>,
        file: GroupsResultFile,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        // Redis itself accepts strings up to 512 MiB. Source's SET/PUBLISH has
        // the same physical ceiling; reject before creating any staging key.
        // Do not retain the unrelated 262 KiB captured-protocol result limit.
        const REDIS_STRING_BYTES: u64 = 512 * 1024 * 1024;
        let prefix = b"{\"status\":\"SUCCESS\",\"result\":".to_vec();
        let mut tail = serde_json::json!({"traceback":null,"children":children.into_iter().map(|child|serde_json::json!([[child.to_string(),null],null])).collect::<Vec<_>>(),"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":id.to_string()});
        if let Some(parent) = parent {
            tail["parent_id"] = serde_json::json!(parent.to_string())
        }
        let tail = serde_json::to_vec(&tail).map_err(|_| Error::Protocol)?;
        let mut suffix = vec![b','];
        suffix.extend_from_slice(&tail[1..]);
        let total = (prefix.len() as u64)
            .checked_add(file.bytes())
            .and_then(|n| n.checked_add(suffix.len() as u64))
            .filter(|n| *n <= REDIS_STRING_BYTES)
            .ok_or(Error::UnsupportedTask)?;
        self.publish_full_metadata_stream(id, prefix, suffix, total, file, caller)
            .await
    }
    pub(crate) async fn write_full_metadata(
        &self,
        file: ResultSpool,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        let id = file.task_id();
        let total = file.bytes();
        self.publish_full_metadata_stream(id, Vec::new(), Vec::new(), total, file, caller)
            .await
    }
    async fn publish_full_metadata_stream<F: ChunkSource>(
        &self,
        id: Uuid,
        prefix: Vec<u8>,
        suffix: Vec<u8>,
        total: u64,
        mut file: F,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if total > crate::full_metadata::MAX_BYTES {
            return Err(Error::UnsupportedTask);
        }
        let token = Uuid::new_v4().to_string();
        let name = format!("native:full-result-stage:{id}:{token}");
        let keys = vec![format!("{name}:owner"), format!("{name}:data")];
        let (begin, _) = self
            .inner
            .result
            .command_classified::<i64, _>(
                BEGIN,
                &keys,
                &[token.as_bytes()],
                self.lease(caller.clone())?,
            )
            .await
            .map_err(|_| Error::AmbiguousResult(id))?;
        if begin != 1 {
            return Err(Error::Corrupt);
        }
        let mut offset = 0u64;
        self.append_full_metadata(id, &keys, &token, &prefix, &mut offset, caller.clone())
            .await?;
        loop {
            // A bounded file-read owner is retained even if its JoinHandle observer is
            // dropped. Never perform a synchronous disk read on the runtime executor.
            let retained = caller.clone();
            let returned = tokio::task::spawn_blocking(move || {
                let _owner = retained;
                let next = file.next_chunk();
                (file, next)
            })
            .await
            .map_err(|_| Error::AmbiguousResult(id))?;
            file = returned.0;
            match returned.1? {
                Some(chunk) => {
                    self.append_full_metadata(
                        id,
                        &keys,
                        &token,
                        &chunk,
                        &mut offset,
                        caller.clone(),
                    )
                    .await?
                }
                None => break,
            }
        }
        for chunk in suffix.chunks(crate::full_metadata::CHUNK) {
            self.append_full_metadata(id, &keys, &token, chunk, &mut offset, caller.clone())
                .await?;
        }
        if offset != total {
            return Err(Error::Corrupt);
        }
        let mut final_keys = keys;
        final_keys.push(format!("celery-tasks:celery-task-meta-{id}"));
        let expected = offset.to_string();
        match self
            .inner
            .result
            .command_classified::<i64, _>(
                COMMIT,
                &final_keys,
                &[token.as_bytes(), expected.as_bytes()],
                self.lease(caller)?,
            )
            .await
        {
            Ok((1, _)) => Ok(true),
            Ok((0, _)) => Ok(false),
            Ok(_) => Err(Error::Corrupt),
            Err(_) => Err(Error::AmbiguousResult(id)),
        }
    }
    async fn append_full_metadata(
        &self,
        id: Uuid,
        keys: &[String],
        token: &str,
        chunk: &[u8],
        offset: &mut u64,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if chunk.len() > crate::full_metadata::CHUNK {
            return Err(Error::Protocol);
        }
        let expected = offset.to_string();
        let (result, _) = self
            .inner
            .result
            .command_classified::<i64, _>(
                APPEND,
                keys,
                &[token.as_bytes(), expected.as_bytes(), chunk],
                self.lease(caller)?,
            )
            .await
            .map_err(|_| Error::AmbiguousResult(id))?;
        if result != 1 {
            return Err(Error::Corrupt);
        }
        *offset = offset
            .checked_add(chunk.len() as u64)
            .ok_or(Error::Protocol)?;
        Ok(())
    }
}
// Snapshot COPY runs atomically with type/size checks. Public metadata is never
// renamed/hidden. Redis temporarily holds one additional whole value (TTL60),
// explicitly a server memory cost, not a Rust heap-memory saving claim.
const SNAPSHOT: &str = r#"
if redis.call('EXISTS',KEYS[2])~=0 then return -1 end
local kind=redis.call('TYPE',KEYS[1]).ok
if kind=='none'then return 0 end
if kind~='string'then return -1 end
local n=redis.call('STRLEN',KEYS[1]);if n>536870912 then return -1 end
if redis.call('COPY',KEYS[1],KEYS[2])~=1 then return -1 end
redis.call('EXPIRE',KEYS[2],60);return n+1
"#;
const SNAPSHOT_CHUNK: &str = r#"
if redis.call('TYPE',KEYS[1]).ok~='string'or redis.call('STRLEN',KEYS[1])~=tonumber(ARGV[1])then return {-1,''}end
redis.call('EXPIRE',KEYS[1],60)
return {1,redis.call('GETRANGE',KEYS[1],ARGV[2],ARGV[3])}
"#;
const DROP_SNAPSHOT: &str = "return redis.call('DEL',KEYS[1])";
impl Jobs {
    /// Coherent full normal UUID result, no full result/children Value allocation.
    /// Legacy `read` retains its captured bounded decode domain. No named IDs are
    /// mapped to UUIDs; existing named transports remain independently scoped.
    pub async fn read_full_metadata(
        &self,
        id: Uuid,
        directory: &Path,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<MetadataSnapshot>, Error> {
        // Prepare the unlink guard before allocating a Redis snapshot. Every
        // blocking operation retains the same actual caller guard.
        let dir = directory.to_path_buf();
        let held = caller.clone();
        let mut file =
            tokio::task::spawn_blocking(move || MetadataSnapshot::create(&dir, id, held))
                .await
                .map_err(|_| Error::Backend)??;
        let key = format!("native:full-result-read:{id}:{}", Uuid::new_v4());
        let keys = vec![format!("celery-tasks:celery-task-meta-{id}"), key.clone()];
        let (n, _) = self
            .inner
            .result
            .command_classified::<i64, _>(SNAPSHOT, &keys, &[], self.lease(caller.clone())?)
            .await
            .map_err(map)?;
        if n == 0 {
            return Ok(None);
        }
        if n < 0 {
            return Err(Error::Corrupt);
        }
        let bytes = u64::try_from(n - 1).map_err(|_| Error::Corrupt)?;
        let result = async {
            let size = bytes.to_string();
            let mut offset = 0u64;
            while offset < bytes {
                let start = offset.to_string();
                let end = (offset + crate::full_metadata::CHUNK as u64 - 1)
                    .min(bytes - 1)
                    .to_string();
                let (reply, _) = self
                    .inner
                    .result
                    .command_classified::<(i64, Vec<u8>), _>(
                        SNAPSHOT_CHUNK,
                        std::slice::from_ref(&key),
                        &[size.as_bytes(), start.as_bytes(), end.as_bytes()],
                        self.lease(caller.clone())?,
                    )
                    .await
                    .map_err(map)?;
                if reply.0 != 1
                    || reply.1.len()
                        != (bytes - offset).min(crate::full_metadata::CHUNK as u64) as usize
                {
                    return Err(Error::Corrupt);
                }
                offset += reply.1.len() as u64;
                file = tokio::task::spawn_blocking(move || {
                    let appended = file.append(&reply.1);
                    (file, appended)
                })
                .await
                .map_err(|_| Error::Backend)
                .and_then(|(returned, result)| result.map(|()| returned))?;
            }
            file = tokio::task::spawn_blocking(move || {
                let sealed = file
                    .seal()
                    .and_then(|()| file.inspect(&mut |_| Ok(())).map(|_| ()));
                (file, sealed)
            })
            .await
            .map_err(|_| Error::Backend)
            .and_then(|(returned, result)| result.map(|()| returned))?;
            Ok::<MetadataSnapshot, Error>(file)
        }
        .await;
        // Physical deletion is awaited on ordinary success/error. Observer loss
        // leaves only a TTL60 private key; the shared command retains its lease.
        let cleanup = self
            .inner
            .result
            .command_classified::<i64, _>(DROP_SNAPSHOT, &[key], &[], self.lease(caller)?)
            .await
            .map_err(map);
        match (result, cleanup) {
            (Ok(file), Ok((1, _))) => Ok(Some(file)),
            (Err(e), _) => Err(e),
            _ => Err(Error::Backend),
        }
    }
}
