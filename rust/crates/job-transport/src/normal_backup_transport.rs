//! Same prepared Main Jobs ports; actual head/CAS and result replies retain caller.
use super::*;
use crate::normal_backup_protocol::{self, Message};
pub(crate) struct BackupCandidate {
    raw: Vec<u8>,
    pub message: Message,
}
impl Jobs {
    pub(crate) async fn peek_normal_backup(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<BackupCandidate>, Error> {
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
        if reply.0 == 0 {
            return if expected.is_some() {
                Err(Error::ClaimRace)
            } else {
                Ok(None)
            };
        }
        if reply.0 != 1 || reply.1.len() > crate::protocol::MAX_ENVELOPE {
            return Err(Error::Corrupt);
        }
        if expected.is_some_and(|v| v != reply.1.as_slice()) {
            return Err(Error::ClaimRace);
        }
        let message =
            normal_backup_protocol::parse(&reply.1).map_err(|_| Error::UnsupportedTask)?;
        Ok(Some(BackupCandidate {
            raw: reply.1,
            message,
        }))
    }
    pub(crate) async fn claim_normal_backup(
        &self,
        candidate: BackupCandidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Message, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
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
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(candidate.message.id)),
            Err(error) => Err(map(error)),
        }
    }
    pub(crate) async fn publish_normal_backup(
        &self,
        id: Uuid,
        reply_to: Uuid,
        origin: &str,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let raw = protocol::encode(&Task::CreateBackup, id, reply_to, Uuid::new_v4(), origin)
            .map_err(|_| Error::Protocol)?;
        let limit = self.inner.config.max_queued_jobs.to_string();
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                PUBLISH,
                &["celery".into(), "_kombu.binding.celery".into()],
                &[&raw, limit.as_bytes()],
                self.lease(owner)?,
            )
            .await
        {
            Ok((n, _)) if n > 0 => Ok(Published {
                task_id: id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(id)),
            Err(error) => Err(map(error)),
        }
    }
    pub(crate) async fn read_normal_backup(
        &self,
        id: &str,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Metadata>, Error> {
        if !self.allows_defaults() || id.is_empty() || id.len() > 1024 || id.contains('\0') {
            return Err(Error::UnsupportedTask);
        }
        let (reply, _held) = self
            .inner
            .result
            .command_classified::<(i64, Vec<u8>), _>(
                READ,
                &[format!("celery-tasks:celery-task-meta-{id}")],
                &[],
                self.lease(owner)?,
            )
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => decode::decode(
                &reply.1,
                Uuid::parse_str(id).map_err(|_| Error::UnsupportedTask)?,
            )
            .map(Some)
            .map_err(Error::Decode),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn write_normal_backup(
        &self,
        message: &Message,
        result: serde_json::Value,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let mut metadata = serde_json::json!({"status":"SUCCESS","result":result,"traceback":null,"children":[],"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":message.id.to_string()});
        if let Some(parent) = message.parent {
            metadata["parent_id"] = serde_json::json!(parent.to_string());
        }
        let bytes = serde_json::to_vec(&metadata).map_err(|_| Error::Protocol)?;
        if bytes.len() > decode::MAX_RESULT_BYTES {
            return Err(Error::Protocol);
        }
        match self.inner.result.command_classified::<i64,_>("redis.call('SETEX',KEYS[1],3600,ARGV[1]);redis.call('PUBLISH',KEYS[1],ARGV[1]);return 1",&[format!("celery-tasks:celery-task-meta-{}",message.id)],&[&bytes],self.lease(owner)?).await{Ok((1,_))=>Ok(true),Ok(_)=>Err(Error::Corrupt),Err(ClassifiedError::Backend)=>Err(Error::AmbiguousResult(message.id)),Err(error)=>Err(map(error))}
    }
    pub(crate) async fn write_normal_backup_revoked(
        &self,
        message: &Message,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let mut metadata = serde_json::json!({"status":"REVOKED","result":{"exc_type":"TaskRevokedError","exc_message":["revoked"],"exc_module":"celery.exceptions"},"traceback":null,"children":[],"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":message.id.to_string()});
        if let Some(parent) = message.parent {
            metadata["parent_id"] = serde_json::json!(parent.to_string());
        }
        let bytes = serde_json::to_vec(&metadata).map_err(|_| Error::Protocol)?;
        match self.inner.result.command_classified::<i64,_>("redis.call('SETEX',KEYS[1],3600,ARGV[1]);redis.call('PUBLISH',KEYS[1],ARGV[1]);return 1",&[format!("celery-tasks:celery-task-meta-{}",message.id)],&[&bytes],self.lease(owner)?).await{Ok((1,_))=>Ok(true),Ok(_)=>Err(Error::Corrupt),Err(ClassifiedError::Backend)=>Err(Error::AmbiguousResult(message.id)),Err(error)=>Err(map(error))}
    }
}
