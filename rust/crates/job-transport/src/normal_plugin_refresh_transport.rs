//! Plain normal plugin-repository refresh root over the existing Main shared Jobs ports.
use super::*;
use crate::normal_plugin_repos::{parse, Message};
pub(crate) struct PluginRefreshCandidate {
    pub message: Message,
    pub(crate) raw: Vec<u8>,
    origin: Arc<Inner>,
}
impl Jobs {
    pub(crate) async fn publish_plugin_startup(
        &self,
        bytes: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let message =
            crate::normal_plugin_repos::parse_eta(bytes).map_err(|_| Error::UnsupportedTask)?;
        let maximum = self.inner.config.max_queued_jobs.to_string();
        let script = r#"local q=redis.call('TYPE',KEYS[1]).ok;local b=redis.call('TYPE',KEYS[2]).ok
if(q~='none'and q~='list')or(b~='none'and b~='set')then return -3 end
if #ARGV[1]>65536 then return -1 end
if redis.call('LLEN',KEYS[1])>=tonumber(ARGV[2])then return -2 end
redis.call('SADD',KEYS[2],ARGV[3]);return redis.call('LPUSH',KEYS[1],ARGV[1])"#;
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                script,
                &["celery".into(), "_kombu.binding.celery".into()],
                &[bytes, maximum.as_bytes(), b"celery\x06\x16\x06\x16celery"],
                self.lease(owner)?,
            )
            .await
        {
            Ok((n, _)) if n > 0 => Ok(()),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(message.id)),
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn peek_plugin_refresh(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<PluginRefreshCandidate>, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &["celery".into()],
                &[],
                self.lease(owner)?,
            )
            .await
            .map_err(map)?;
        if let Some(expected) = expected {
            if expected.is_empty() || reply.0 != 1 || reply.1 != expected {
                return Err(Error::ClaimRace);
            }
        }
        if reply.0 == 0 {
            return Ok(None);
        }
        if reply.0 != 1 {
            return Err(Error::Corrupt);
        }
        Ok(Some(PluginRefreshCandidate {
            message: parse(&reply.1)
                .or_else(|_| {
                    crate::plugin_eta::Receipt::prepare(&reply.1)
                        .map(|r| r.message)
                        .map_err(|_| ())
                })
                .map_err(|_| Error::UnsupportedTask)?,
            raw: reply.1,
            origin: self.inner.clone(),
        }))
    }
    pub(crate) async fn claim_plugin_refresh(
        &self,
        candidate: PluginRefreshCandidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Message, Error> {
        if !self.allows_defaults()
            || candidate.message.eta.is_some()
            || !Arc::ptr_eq(&self.inner, &candidate.origin)
        {
            return Err(Error::UnsupportedTask);
        }
        let id = candidate.message.id;
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &["celery".into()],
                &[&candidate.raw],
                self.lease(owner)?,
            )
            .await
        {
            Ok((1, _)) => Ok(candidate.message),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(id)),
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn write_plugin_refresh(
        &self,
        message: &Message,
        result: serde_json::Value,
        children: Vec<String>,
        revoked: bool,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let status = if revoked { "REVOKED" } else { "SUCCESS" };
        let result = if revoked {
            serde_json::json!({"exc_type":"TaskRevokedError","exc_message":["revoked"],"exc_module":"celery.exceptions"})
        } else {
            result
        };
        let children: Vec<serde_json::Value> = children
            .into_iter()
            .map(|id| serde_json::json!([[id, null], null]))
            .collect();
        let bytes=serde_json::to_vec(&serde_json::json!({"status":status,"result":result,"traceback":null,"children":children,"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":message.id.to_string()})).map_err(|_|Error::Protocol)?;
        self.write_metadata(message.id, bytes, owner).await
    }
}
