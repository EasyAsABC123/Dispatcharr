//! Actual bounded publication through Main's already connected broker transport.
use super::*;
impl Jobs {
    pub(crate) async fn publish_core_settings(
        &self,
        publication: dispatcharr_store::CoreTaskPublication,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        // The same sealed Store producer also owns Source's new STD-account
        // refresh_m3u_groups.delay publication; normal queue dispatch already
        // delegates this exact root envelope to its existing STD implementation.
        let task_id = if let Ok(message) = crate::normal_core_tasks::parse(publication.bytes()) {
            message.id
        } else {
            crate::normal_xc_live::parse(publication.bytes())
                .map_err(|_| Error::Protocol)?
                .id
        };
        if task_id != publication.task_id() {
            return Err(Error::Protocol);
        }
        let maximum = self.inner.config.max_queued_jobs.to_string();
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                PUBLISH,
                &["celery".into(), "_kombu.binding.celery".into()],
                &[publication.bytes(), maximum.as_bytes()],
                self.lease(owner)?,
            )
            .await
        {
            Ok((n, _)) if n > 0 => Ok(()),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(task_id)),
            Err(error) => Err(map(error)),
        }
    }
}
