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
        let (task_id, programme_root) = if let Ok(message) =
            crate::normal_core_tasks::parse(publication.bytes())
        {
            (message.id, false)
        } else if let Ok(message) = crate::normal_xc_live::parse(publication.bytes()) {
            (message.id, false)
        } else if let Ok(message) = crate::normal_xc_accounts::parse(publication.bytes()) {
            // Direct HTTP Groups publishes an actual unparented Profiles
            // root. It is not a synthetic Groups child or Info request.
            if message.kind != crate::normal_xc_accounts::Kind::Profiles || message.parent.is_some()
            {
                return Err(Error::Protocol);
            }
            (message.id, false)
        } else {
            let message =
                crate::xml_tvg_protocol::parse(publication.bytes()).map_err(|_| Error::Protocol)?;
            if message.common.parent.is_some()
                || message.force
                || message.defer != 0
                || message.eta.is_some()
            {
                return Err(Error::Protocol);
            }
            (message.common.id, true)
        };
        if task_id != publication.task_id() {
            return Err(Error::Protocol);
        }
        let maximum = self.inner.config.max_queued_jobs.to_string();
        let guard = self.lease(owner)?;
        let keys = ["celery".into(), "_kombu.binding.celery".into()];
        let args = [publication.bytes(), maximum.as_bytes()];
        let outcome = if programme_root {
            // Source programme roots use the already qualified bounded socket
            // readiness operation. Only PING may repeat; never replay PUBLISH.
            self.inner
                .broker
                .command_publication_classified_with_diagnostic::<i64, _>(
                    PUBLISH, &keys, &args, guard, None,
                )
                .await
        } else {
            self.inner
                .broker
                .command_classified::<i64, _>(PUBLISH, &keys, &args, guard)
                .await
        };
        match outcome {
            Ok((n, _)) if n > 0 => Ok(()),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(task_id)),
            Err(error) => Err(map(error)),
        }
    }
}
