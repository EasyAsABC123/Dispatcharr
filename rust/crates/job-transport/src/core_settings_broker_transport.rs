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
        match self
            .publish_core_frame(publication, owner, task_id, PUBLISH, programme_root)
            .await?
        {
            n if n > 0 => Ok(()),
            -2 => Err(Error::Capacity),
            _ => Err(Error::Corrupt),
        }
    }

    pub(crate) async fn publish_provider_rehash(
        &self,
        publication: dispatcharr_store::CoreTaskPublication,
        owner: Arc<dyn Send + Sync>,
        task_id: Uuid,
    ) -> Result<dispatcharr_store::ProviderRehashResult, Error> {
        use dispatcharr_store::ProviderRehashResult;
        match self
            .publish_core_frame(publication, owner, task_id, PUBLISH_PROVIDER_REHASH, false)
            .await?
        {
            n if n > 0 => Ok(ProviderRehashResult::Acknowledged),
            -4 => Ok(ProviderRehashResult::Refused),
            -2 => Err(Error::Capacity),
            _ => Err(Error::Corrupt),
        }
    }

    async fn publish_core_frame(
        &self,
        publication: dispatcharr_store::CoreTaskPublication,
        owner: Arc<dyn Send + Sync>,
        task_id: Uuid,
        script: &'static str,
        programme_root: bool,
    ) -> Result<i64, Error> {
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
                    script, &keys, &args, guard, None,
                )
                .await
        } else {
            self.inner
                .broker
                .command_classified::<i64, _>(script, &keys, &args, guard)
                .await
        };
        match outcome {
            Ok((n, _)) => Ok(n),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(task_id)),
            Err(error) => Err(map(error)),
        }
    }
}
