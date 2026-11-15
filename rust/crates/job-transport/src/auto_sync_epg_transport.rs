//! Exact typed EPG child publication over shared Jobs; no new client/pool.
use super::*;
impl Jobs {
    pub(crate) async fn publish_auto_sync_epg(
        &self,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        let (message, parent) =
            crate::tvg_programme_protocol::parse_normal(raw).map_err(|_| Error::Protocol)?;
        if parent.is_none() {
            return Err(Error::Protocol);
        }
        let maximum = self.inner.config.max_queued_jobs.to_string();
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                PUBLISH,
                &["celery".into(), "_kombu.binding.celery".into()],
                &[raw, maximum.as_bytes()],
                self.lease(owner)?,
            )
            .await
        {
            Ok((n, _)) if n > 0 => Ok(Published {
                task_id: message.task_id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(message.task_id)),
            Err(error) => Err(map(error)),
        }
    }
}
