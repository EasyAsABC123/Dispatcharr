//! Bounded chunk append/atomic full result publication through shared Jobs.
//! Original Celery/Redis/Dispatcharr authors retain all protocol credit.
use super::*;
use crate::m3u_result_file::GroupsResultFile;
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
        mut file: GroupsResultFile,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        // Redis itself accepts strings up to 512 MiB. Source's SET/PUBLISH has
        // the same physical ceiling; reject before creating any staging key.
        // Do not retain the unrelated 262 KiB captured-protocol result limit.
        const REDIS_STRING_BYTES: u64 = 512 * 1024 * 1024;
        let token = Uuid::new_v4().to_string();
        let name = format!("native:m3u-result-stage:{id}:{token}");
        let keys = vec![format!("{name}:owner"), format!("{name}:data")];
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
        self.append_m3u_result(id, &keys, &token, &prefix, &mut offset, caller.clone())
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
                    self.append_m3u_result(id, &keys, &token, &chunk, &mut offset, caller.clone())
                        .await?
                }
                None => break,
            }
        }
        self.append_m3u_result(id, &keys, &token, &suffix, &mut offset, caller.clone())
            .await?;
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
    async fn append_m3u_result(
        &self,
        id: Uuid,
        keys: &[String],
        token: &str,
        chunk: &[u8],
        offset: &mut u64,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
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
