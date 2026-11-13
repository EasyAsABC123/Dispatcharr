//! Scanner M3U physical ports; no legacy/default intake authority is changed.
use super::*;
use crate::scanner_m3u_protocol::{self, Message, Scope};
use serde_json::Value;
#[derive(Clone)]
pub struct M3uPorts {
    jobs: Jobs,
    cache: RedisTransport,
}
pub(crate) struct Candidate {
    pub message: Message,
    raw: Vec<u8>,
    _held: Lease,
    origin: Arc<Inner>,
}
const PARENT:&str="local t=redis.call('TYPE',KEYS[1]).ok;if t=='none'then return{0,''}end;if t~='string'then return{-1,''}end;if redis.call('STRLEN',KEYS[1])>65536 then return{-2,''}end;return{1,redis.call('GET',KEYS[1])}";
const CACHE: &str = r#"
local cursor='0';local keys={};local bytes=0;local rounds=0
repeat
 local found=redis.call('SCAN',cursor,'MATCH',':1:m3u_content:*','COUNT',256)
 cursor=found[1];rounds=rounds+1
 if rounds>64 then return -2 end
 for _,key in ipairs(found[2])do
  bytes=bytes+#key
  if #keys>=64 or #key>4096 or bytes>16384 then return -2 end
  keys[#keys+1]=key
 end
until cursor=='0'
if ARGV[1]=='delete'then for _,key in ipairs(keys)do redis.call('DEL',key)end end
return #keys
"#;
fn receipt(bytes: &[u8], scope: &Scope, id: Uuid) -> bool {
    let Ok(v) = serde_json::from_slice::<Value>(bytes) else {
        return false;
    };
    v["task_id"] == scope.parent().to_string()
        && v["status"] == "SUCCESS"
        && v.get("result") == Some(&Value::Null)
        && v.get("traceback") == Some(&Value::Null)
        && v.get("parent_id").is_none()
        && v["children"].as_array().is_some_and(|items| {
            items.len() <= 32
                && items
                    .iter()
                    .any(|v| *v == serde_json::json!([[id.to_string(), null], null]))
        })
}
impl M3uPorts {
    pub async fn connect(
        broker: &str,
        result: &str,
        cache: &str,
        config: Config,
    ) -> Result<Self, Error> {
        let jobs = Jobs::connect_defaults(broker, result, config.clone()).await?;
        let client = redis::Client::open(cache).map_err(|_| Error::Configuration)?;
        if client.get_connection_info().redis.db != 0 {
            return Err(Error::Configuration);
        }
        let cache = RedisTransport::connect(
            cache,
            TransportConfig {
                max_operations: config.operations,
                max_queued_operations: config.operations,
                command_timeout: config.command_timeout,
            },
        )
        .await
        .map_err(|_| Error::Backend)?;
        Ok(Self { jobs, cache })
    }
    pub(crate) async fn peek(
        &self,
        scope: &Scope,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Candidate>, Error> {
        let lease = self.jobs.lease(owner.clone())?;
        let keys = vec!["celery".into()];
        let (head, held) = self
            .jobs
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &keys, &[], lease)
            .await
            .map_err(map)?;
        if head.0 == 0 {
            return Ok(None);
        }
        if head.0 != 1 {
            return Err(Error::Corrupt);
        }
        let message = scanner_m3u_protocol::parse(&head.1).map_err(|_| Error::UnsupportedTask)?;
        if !scope.accepts(&message) {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.jobs.lease(owner)?;
        let key = vec![format!("celery-tasks:celery-task-meta-{}", scope.parent())];
        let (parent, _receipt) = self
            .jobs
            .inner
            .result
            .command_classified::<(i64, Vec<u8>), _>(PARENT, &key, &[], lease)
            .await
            .map_err(map)?;
        if parent.0 != 1 || !receipt(&parent.1, scope, message.id) {
            return Err(Error::UnsupportedTask);
        }
        Ok(Some(Candidate {
            message,
            raw: head.1,
            _held: held,
            origin: self.jobs.inner.clone(),
        }))
    }
    pub(crate) async fn claim(
        &self,
        candidate: Candidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Message, Error> {
        if !Arc::ptr_eq(&candidate.origin, &self.jobs.inner) {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.jobs.lease(owner)?;
        let keys = vec!["celery".into()];
        match self
            .jobs
            .inner
            .broker
            .command_classified::<i64, _>(CLAIM_CREATE, &keys, &[&candidate.raw], lease)
            .await
        {
            Ok((1, _)) => Ok(candidate.message),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(candidate.message.id)),
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn write_terminal(
        &self,
        m: &Message,
        terminal: crate::scanner_m3u::Terminal,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        match terminal {
            crate::scanner_m3u::Terminal::Normal(result) => {
                self.jobs
                    .write_result(m.id, result, Some(m.parent), Vec::new(), owner)
                    .await
            }
            crate::scanner_m3u::Terminal::Raised(failure) => {
                let date = chrono::Utc::now()
                    .format("%Y-%m-%dT%H:%M:%S%.6f+00:00")
                    .to_string();
                self.jobs
                    .write_metadata(m.id, failure.metadata(m, &date)?, owner)
                    .await
            }
        }
    }
    pub(crate) async fn groups_lock_present(
        &self,
        account: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if account <= 0 || account > i64::from(i32::MAX) {
            return Err(Error::Protocol);
        }
        let key = format!("task_lock_refresh_m3u_account_groups_{account}");
        let (present, _) = self
            .cache
            .command_classified::<i64, _>("return redis.call('EXISTS',KEYS[1])", &[key], &[], owner)
            .await
            .map_err(map)?;
        Ok(present == 1)
    }
    pub(crate) async fn cache_preflight(&self, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        let (n, _) = self
            .cache
            .command_classified::<i64, _>(CACHE, &[], &[b"check"], owner)
            .await
            .map_err(map)?;
        if n < 0 {
            return Err(Error::Capacity);
        }
        Ok(())
    }
    pub(crate) async fn invalidate(&self, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        // Source swallows definite server errors separately for delete and INCR;
        // native capacity/unknown I/O/logical timeout remain operational, never a
        // fabricated normal result while the physical operation is unsettled.
        match self
            .cache
            .command_classified::<i64, _>(CACHE, &[], &[b"delete"], owner.clone())
            .await
        {
            Ok((n, _)) if n >= 0 => {}
            Ok(_) => return Err(Error::Capacity),
            Err(ClassifiedError::Server) => {}
            Err(e) => return Err(map(e)),
        }
        match self
            .cache
            .command_classified::<i64, _>(
                "return redis.call('INCR',KEYS[1])",
                &["epg_content_generation".into()],
                &[],
                owner,
            )
            .await
        {
            Ok(_) | Err(ClassifiedError::Server) => Ok(()),
            Err(e) => Err(map(e)),
        }
    }
    // Admission-only Source cache reads: no fill and no logical-error DB fallback.
    // TYPE/STRLEN/GET are one script, so replacement cannot race the bulk bound.
    async fn hash_cache_value(
        &self,
        key: &str,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Value>, Error> {
        const READ: &str = "local t=redis.call('TYPE',KEYS[1]).ok;if t=='none'then return{0,''}end;if t~='string'then return{-1,''}end;if redis.call('STRLEN',KEYS[1])>65536 then return{-2,''}end;return{1,redis.call('GET',KEYS[1])}";
        let (reply, _) = self
            .cache
            .command_classified::<(i64, Vec<u8>), _>(READ, &[key.into()], &[], owner)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => dispatcharr_store::decode_source_cache(&reply.1)
                .map(Some)
                .ok_or(Error::Corrupt),
            -2 => Err(Error::Capacity),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn stream_settings_cached(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Value>, Error> {
        Ok(self
            .hash_cache_value(":1:coresettings:group:stream_settings", owner)
            .await?
            .filter(Value::is_object))
    }
    pub(crate) async fn stream_settings_version(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Value, Error> {
        Ok(self
            .hash_cache_value(":1:coresettings:groupver:stream_settings", owner)
            .await?
            .unwrap_or(Value::Null))
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.jobs.shutdown(timeout).await
    }
}
