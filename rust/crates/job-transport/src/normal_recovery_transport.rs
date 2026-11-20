//! Normal startup-recovery UUID root, distinct from named recording children.
//! Original Dispatcharr/Celery authors retain all algorithm/protocol credit.
use super::*;
pub(crate) const RECOVERY_TASK: &str = "apps.channels.tasks.recover_recordings_on_startup";

pub(super) async fn publish_startup_root(
    jobs: &Jobs,
    raw: &[u8],
    id: Uuid,
    owner: Arc<dyn Send + Sync>,
) -> Result<Published, Error> {
    let maximum = jobs.inner.config.max_queued_jobs.to_string();
    match jobs
        .inner
        .broker
        .command_classified::<i64, _>(
            PUBLISH,
            &["celery".into(), "_kombu.binding.celery".into()],
            &[raw, maximum.as_bytes()],
            jobs.lease(owner)?,
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
#[derive(Clone, Debug)]
pub(crate) struct RecoveryMessage {
    pub(crate) task_id: Uuid,
}
fn parse(raw: &[u8]) -> Result<RecoveryMessage, Error> {
    let mut value =
        crate::clocked_recording_envelope::json_value(raw).map_err(|_| Error::Protocol)?;
    if value["headers"]["task"] != RECOVERY_TASK {
        return Err(Error::UnsupportedTask);
    }
    value["headers"]["task"] = serde_json::json!("apps.proxy.tasks.fetch_channel_stats");
    let message =
        crate::stats_protocol::parse(&serde_json::to_vec(&value).map_err(|_| Error::Protocol)?)
            .map_err(|_| Error::UnsupportedTask)?;
    Ok(RecoveryMessage {
        task_id: message.task_id,
    })
}
#[derive(Clone)]
pub struct RecoveryJobs {
    jobs: Jobs,
}
impl RecoveryJobs {
    pub async fn connect(broker: &str, backend: &str) -> Result<Self, Error> {
        Ok(Self {
            jobs: Jobs::connect_series(broker, backend, Config::default()).await?,
        })
    }
    pub(crate) async fn claim(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<RecoveryMessage>, Error> {
        let lease = Arc::new(self.jobs.lease(owner)?);
        let (reply, _physical) = self
            .jobs
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &["celery".into()],
                &[],
                lease.clone(),
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
        let message = parse(&reply.1)?;
        match self
            .jobs
            .inner
            .broker
            .command_classified::<i64, _>(CLAIM_CREATE, &["celery".into()], &[&reply.1], lease)
            .await
        {
            Ok((1, _)) => Ok(Some(message)),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(message.task_id)),
            Err(error) => Err(map(error)),
        }
    }
    pub(crate) async fn publish(
        &self,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        // Only these actual startup root names are admitted; never generic app tasks.
        let value =
            crate::clocked_recording_envelope::json_value(raw).map_err(|_| Error::Protocol)?;
        let id = if value["headers"]["task"] == RECOVERY_TASK {
            parse(raw)?.task_id
        } else {
            crate::version_protocol::parse(raw)
                .map_err(|_| Error::UnsupportedTask)?
                .task_id
        };
        publish_startup_root(&self.jobs, raw, id, owner).await
    }
    pub(crate) async fn write(
        &self,
        message: &RecoveryMessage,
        report: &crate::normal_recording_recovery::Report,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        let children: Vec<serde_json::Value> = report
            .children
            .iter()
            .map(|key| serde_json::json!([[key, null], null]))
            .collect();
        let bytes=serde_json::to_vec(&serde_json::json!({"status":"SUCCESS","result":report.result,"traceback":null,"children":children,"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":message.task_id.to_string()})).map_err(|_|Error::Protocol)?;
        self.jobs
            .write_metadata(message.task_id, bytes, owner)
            .await
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.jobs.shutdown(timeout).await
    }
}
