//! Known Source periodic Refresh/Series publication; no generic task authority.
//! Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
use super::{map, ClassifiedError, Error, Jobs, Lease, Published, PUBLISH};
use crate::{periodic_headers, protocol, refresh_protocol, series_protocol};
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;
fn marked(bytes: &[u8], name: &str) -> Result<Vec<u8>, Error> {
    let mut envelope: Value = serde_json::from_slice(bytes).map_err(|_| Error::Protocol)?;
    periodic_headers::put(&mut envelope, name).map_err(|_| Error::Protocol)?;
    let bytes = serde_json::to_vec(&envelope).map_err(|_| Error::Protocol)?;
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(Error::Capacity);
    }
    Ok(bytes)
}
impl Jobs {
    pub async fn publish_periodic_refresh(
        &self,
        message: &refresh_protocol::RefreshMessage,
        name: &str,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        if !self.allows_refresh() {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller)?;
        if message.parent.is_some() || message.eta.is_some() {
            return Err(Error::Protocol);
        }
        let base = refresh_protocol::encode(
            message,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "rust@periodic-refresh",
        )
        .map_err(|_| Error::Protocol)?;
        let bytes = marked(&base, name)?;
        if refresh_protocol::parse(&bytes).map_err(|_| Error::Protocol)? != *message {
            return Err(Error::Protocol);
        }
        self.publish_periodic_checked(message.task_id, bytes, lease)
            .await
    }
    pub async fn publish_periodic_series(
        &self,
        message: &series_protocol::SeriesMessage,
        name: &str,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        if !self.allows_series() {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller)?;
        if message.parent.is_some() {
            return Err(Error::Protocol);
        }
        let base = protocol::encode(
            &protocol::Task::CreateBackup,
            message.task_id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "rust@periodic-series",
        )
        .map_err(|_| Error::Protocol)?;
        let mut frame: Value = serde_json::from_slice(&base).map_err(|_| Error::Protocol)?;
        frame["headers"]["task"] =
            Value::String("apps.channels.tasks.evaluate_series_rules".to_owned());
        let base = serde_json::to_vec(&frame).map_err(|_| Error::Protocol)?;
        let bytes = marked(&base, name)?;
        if series_protocol::parse(&bytes).map_err(|_| Error::Protocol)? != *message {
            return Err(Error::Protocol);
        }
        self.publish_periodic_checked(message.task_id, bytes, lease)
            .await
    }
    async fn publish_periodic_checked(
        &self,
        id: Uuid,
        bytes: Vec<u8>,
        lease: Lease,
    ) -> Result<Published, Error> {
        let limit = self.inner.config.max_queued_jobs.to_string();
        let keys = vec!["celery".to_owned(), "_kombu.binding.celery".to_owned()];
        match self
            .inner
            .broker
            .command_classified::<i64, _>(PUBLISH, &keys, &[&bytes, limit.as_bytes()], lease)
            .await
        {
            Ok((n, _held)) if n > 0 => Ok(Published {
                task_id: id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(id)),
            Err(error) => Err(map(error)),
        }
    }
}
