use super::*;
impl Jobs {
    pub(crate) async fn peek_xml_tvg_raw(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let lease = self.lease(owner)?;
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &["celery".into()], &[], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => Ok(Some(reply.1)),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn publish_xml_tvg(
        &self,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        let message = crate::xml_tvg_protocol::parse(raw).map_err(|_| Error::Protocol)?;
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
                task_id: message.common.id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(message.common.id)),
            Err(e) => Err(map(e)),
        }
    }
}
