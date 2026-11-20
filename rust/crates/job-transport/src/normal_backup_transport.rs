//! Same prepared Main Jobs ports; actual head/CAS and result replies retain caller.
use super::*;
use crate::normal_backup_protocol::{self, Kind, Message};
use base64::{engine::general_purpose::STANDARD, Engine};
pub(crate) struct BackupCandidate {
    raw: Vec<u8>,
    pub message: Message,
}
fn backup_kind_allowed(authority: Authority, kind: &Kind) -> bool {
    matches!(authority, Authority::Fixture | Authority::DefaultServices)
        || (authority == Authority::Backup && matches!(kind, Kind::Create))
        || (authority == Authority::BackupRestore && matches!(kind, Kind::Restore(_)))
}

impl Jobs {
    pub(crate) async fn peek_normal_backup(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<BackupCandidate>, Error> {
        if !self.allows_defaults() && !self.allows_backup() && !self.allows_manual_restore() {
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
        if !backup_kind_allowed(self.inner.authority, &message.kind) {
            return Err(Error::UnsupportedTask);
        }
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
        if !backup_kind_allowed(self.inner.authority, &candidate.message.kind) {
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
        if !backup_kind_allowed(self.inner.authority, &Kind::Create) {
            return Err(Error::UnsupportedTask);
        }
        let raw = protocol::encode(&Task::CreateBackup, id, reply_to, Uuid::new_v4(), origin)
            .map_err(|_| Error::Protocol)?;
        self.publish_normal_backup_raw(raw, id, owner).await
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_normal_restore(
        &self,
        filename: String,
        args_repr: String,
        kwargs_repr: String,
        id: Uuid,
        reply_to: Uuid,
        origin: &str,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        if !self.allows_normal_restore() {
            return Err(Error::UnsupportedTask);
        }
        let raw = encode_restore(
            &filename,
            &args_repr,
            &kwargs_repr,
            id,
            reply_to,
            Uuid::new_v4(),
            origin,
        )?;
        self.publish_normal_backup_raw(raw, id, owner).await
    }
    async fn publish_normal_backup_raw(
        &self,
        raw: Vec<u8>,
        id: Uuid,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
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
        if (!self.allows_defaults() && !self.allows_backup() && !self.allows_manual_restore())
            || id.is_empty()
            || id.len() > 1024
            || id.contains('\0')
        {
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
        if !backup_kind_allowed(self.inner.authority, &message.kind) {
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
        if !backup_kind_allowed(self.inner.authority, &message.kind) {
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

// Ordinary HTTP filenames use the SAME qualified envelope skeleton and actual
// ProducerIdentitySource saferepr, without widening the old captured encoder.
#[allow(clippy::too_many_arguments)]
fn encode_restore(
    filename: &str,
    args_repr: &str,
    kwargs_repr: &str,
    id: Uuid,
    reply: Uuid,
    delivery: Uuid,
    origin: &str,
) -> Result<Vec<u8>, Error> {
    if filename.is_empty()
        || filename.len() > 255
        || filename.contains(['/', '\\', '\0'])
        || filename.contains("..")
        || args_repr.len() > 16384
        || kwargs_repr != "{}"
    {
        return Err(Error::Protocol);
    }
    let raw = protocol::encode(&Task::CreateBackup, id, reply, delivery, origin)
        .map_err(|_| Error::Protocol)?;
    let mut envelope: serde_json::Value =
        serde_json::from_slice(&raw).map_err(|_| Error::Protocol)?;
    envelope["headers"]["task"] = serde_json::json!("apps.backups.tasks.restore_backup_task");
    envelope["headers"]["argsrepr"] = serde_json::json!(args_repr);
    let body = serde_json::json!([[filename],{}, {"callbacks":null,"errbacks":null,"chain":null,"chord":null}]);
    envelope["body"] =
        serde_json::json!(STANDARD.encode(serde_json::to_vec(&body).map_err(|_| Error::Protocol)?));
    let raw = serde_json::to_vec(&envelope).map_err(|_| Error::Protocol)?;
    if raw.len() > protocol::MAX_ENVELOPE {
        return Err(Error::Capacity);
    }
    Ok(raw)
}
#[cfg(test)]
mod restore_encoding_tests {
    use super::*;
    #[test]
    fn normal_restore_keeps_actual_filename_and_source_saferepr_separate_from_cas_bytes() {
        let id = Uuid::new_v4();
        let filename = "owned 'single' and \"double\" é.zip";
        let representation = "(actual bounded Source saferepr,)";
        let raw = encode_restore(
            filename,
            representation,
            "{}",
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "gen1@owned",
        )
        .unwrap();
        let header: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(header["headers"]["argsrepr"], representation);
        let actual = normal_backup_protocol::parse(&raw).unwrap();
        assert!(
            matches!(actual.kind,crate::normal_backup_protocol::Kind::Restore(value) if value==filename)
        );
        assert_eq!(actual.id, id);
        for filename in ["../outside.zip", "nested/file.zip", "bad\0name.zip"] {
            assert!(encode_restore(
                filename,
                representation,
                "{}",
                id,
                Uuid::new_v4(),
                Uuid::new_v4(),
                "gen1@owned"
            )
            .is_err());
        }
    }
}

#[cfg(test)]
mod manual_authority_tests {
    use super::*;

    #[test]
    fn manual_backup_authorities_keep_create_and_restore_separate() {
        let create = Kind::Create;
        let restore = Kind::Restore("owned.zip".into());
        let scheduled = Kind::Scheduled(serde_json::json!(1));
        assert!(backup_kind_allowed(Authority::Backup, &create));
        assert!(!backup_kind_allowed(Authority::Backup, &restore));
        assert!(!backup_kind_allowed(Authority::Backup, &scheduled));
        assert!(!backup_kind_allowed(Authority::BackupRestore, &create));
        assert!(backup_kind_allowed(Authority::BackupRestore, &restore));
        assert!(!backup_kind_allowed(Authority::BackupRestore, &scheduled));
        assert!(backup_kind_allowed(Authority::DefaultServices, &create));
        assert!(backup_kind_allowed(Authority::DefaultServices, &restore));
        assert!(backup_kind_allowed(Authority::DefaultServices, &scheduled));
        assert!(!backup_kind_allowed(Authority::Index, &create));
    }
}
