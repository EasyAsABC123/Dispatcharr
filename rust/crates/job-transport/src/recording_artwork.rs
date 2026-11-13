//! Explicit typed artwork publication only; no child consumption or default
//! graph activation. Original Dispatcharr/Celery/Kombu authors retain credit.
use crate::{
    configured_recording_service::{ArtworkError, ArtworkSink},
    protocol::{self, ProtocolError},
    transport::{Config, Jobs},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dispatcharr_contracts::BoxFuture;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;
#[derive(Clone, Copy, Debug)]
pub struct DirectTask {
    id: Uuid,
}
impl DirectTask {
    pub(crate) fn series(message: &crate::series_protocol::SeriesMessage) -> Option<Self> {
        if message.parent.is_some()
            || message.root != message.task_id
            || (message.task_id.get_version_num() != 4
                || message.task_id.get_variant() != uuid::Variant::RFC4122)
        {
            return None;
        }
        Some(Self {
            id: message.task_id,
        })
    }
    pub(crate) fn recurring(message: &crate::default_protocol::Message) -> Option<Self> {
        if message.kind != crate::default_protocol::Kind::RecurringMaintenance
            || message.expires.is_some()
            || (message.task_id.get_version_num() != 4
                || message.task_id.get_variant() != uuid::Variant::RFC4122)
        {
            return None;
        }
        Some(Self {
            id: message.task_id,
        })
    }
    pub fn id(self) -> Uuid {
        self.id
    }
}
/// Raw spelling is checked separately because legacy protocol UUID parsing
/// intentionally normalizes inputs; this new configured scope must not do so.
pub(crate) fn canonical_frame(raw: &[u8], task: DirectTask) -> bool {
    if raw.len() > protocol::MAX_ENVELOPE {
        return false;
    }
    let Ok(frame) = serde_json::from_slice::<serde_json::Value>(raw) else {
        return false;
    };
    let canonical = task.id.to_string();
    frame
        .get("headers")
        .and_then(|h| h.get("id"))
        .and_then(serde_json::Value::as_str)
        == Some(canonical.as_str())
        && frame
            .get("headers")
            .and_then(|h| h.get("root_id"))
            .and_then(serde_json::Value::as_str)
            == Some(canonical.as_str())
        && frame.get("headers").and_then(|h| h.get("parent_id")) == Some(&serde_json::Value::Null)
}
fn python_utc(t: DateTime<Utc>) -> String {
    let base = t.format("%Y-%m-%dT%H:%M:%S").to_string();
    if t.timestamp_subsec_micros() == 0 {
        format!("{base}+00:00")
    } else {
        format!("{base}.{:06}+00:00", t.timestamp_subsec_micros())
    }
}
pub(crate) fn encode(
    recording: i64,
    task: DirectTask,
    id: Uuid,
    reply: Uuid,
    delivery: Uuid,
    now: DateTime<Utc>,
) -> Result<Vec<u8>, ProtocolError> {
    if !(1..=i64::from(i32::MAX)).contains(&recording) {
        return Err(ProtocolError::InvalidArgument);
    }
    // Reuse the supported common protocol2 envelope, then set ONLY the captured
    // family, one integer argument, direct lineage and one-second countdown ETA.
    let mut frame: serde_json::Value = serde_json::from_slice(&protocol::encode(
        &protocol::Task::CreateBackup,
        id,
        reply,
        delivery,
        "rust@configured-recordings",
    )?)
    .map_err(|_| ProtocolError::InvalidArgument)?;
    frame["headers"]["task"] = json!("apps.channels.tasks.prefetch_recording_artwork");
    frame["headers"]["root_id"] = json!(task.id.to_string());
    frame["headers"]["parent_id"] = json!(task.id.to_string());
    frame["headers"]["argsrepr"] = json!(format!("[{recording}]"));
    frame["headers"]["eta"] = json!(python_utc(now + ChronoDuration::seconds(1)));
    frame["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([[recording],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .map_err(|_| ProtocolError::InvalidArgument)?
    ));
    let bytes = serde_json::to_vec(&frame).map_err(|_| ProtocolError::InvalidArgument)?;
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    Ok(bytes)
}
/// Opening this typed publisher starts no intake. Its private Series transport
/// is never exposed as generic default/index/backup publication authority.
pub struct Publisher {
    jobs: Jobs,
    reply: Uuid,
}
impl Publisher {
    pub async fn connect(
        broker: &str,
        backend: &str,
        config: Config,
    ) -> Result<Self, crate::transport::Error> {
        let jobs = Jobs::connect_series(broker, backend, config).await?;
        Ok(Self {
            jobs,
            reply: Uuid::new_v4(),
        })
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), crate::transport::Error> {
        self.jobs.shutdown(timeout).await
    }
}
impl ArtworkSink for Publisher {
    fn publish<'a>(
        &'a self,
        recording: i64,
        task: DirectTask,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<Uuid, ArtworkError>> {
        Box::pin(async move {
            // Python's clock is microsecond precision; trim actual Rust wallclock
            // precision explicitly, never reuse a captured Source timestamp.
            let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
                .ok_or(ArtworkError::Uncertain)?;
            let id = Uuid::new_v4();
            let bytes = encode(recording, task, id, self.reply, Uuid::new_v4(), now)
                .map_err(|_| ArtworkError::Uncertain)?;
            self.jobs
                .publish_configured_artwork(&bytes, id, owner)
                .await
                .map_err(|_| ArtworkError::Uncertain)?;
            Ok(id)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn captured_artwork_protocol_has_direct_parent_and_countdown_one() {
        let id = Uuid::new_v4();
        let parent = Uuid::new_v4();
        let task = DirectTask::series(&crate::series_protocol::SeriesMessage {
            task_id: parent,
            root: parent,
            parent: None,
        })
        .unwrap();
        let now: DateTime<Utc> = "2026-11-12T23:34:49.548943+00:00".parse().unwrap();
        let raw = encode(1, task, id, Uuid::new_v4(), Uuid::new_v4(), now).unwrap();
        let frame: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        let body: serde_json::Value =
            serde_json::from_slice(&STANDARD.decode(frame["body"].as_str().unwrap()).unwrap())
                .unwrap();
        assert_eq!(
            body,
            json!([[1],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        );
        assert_eq!(
            frame["headers"]["task"],
            "apps.channels.tasks.prefetch_recording_artwork"
        );
        assert_eq!(frame["headers"]["root_id"], parent.to_string());
        assert_eq!(frame["headers"]["parent_id"], parent.to_string());
        assert_eq!(frame["headers"]["eta"], "2026-11-12T23:34:50.548943+00:00");
        assert_eq!(frame["headers"]["ignore_result"], false);
        assert_eq!(
            frame["properties"]["delivery_info"],
            json!({"exchange":"","routing_key":"celery"})
        );
        assert!(frame["headers"]["expires"].is_null());
    }
    #[test]
    fn configured_parented_or_foreign_root_is_not_admitted() {
        let id = Uuid::new_v4();
        let other = Uuid::new_v4();
        assert!(DirectTask::series(&crate::series_protocol::SeriesMessage {
            task_id: id,
            root: other,
            parent: Some(other)
        })
        .is_none());
        assert!(DirectTask::series(&crate::series_protocol::SeriesMessage {
            task_id: id,
            root: other,
            parent: None
        })
        .is_none());
        assert!(DirectTask::series(&crate::series_protocol::SeriesMessage {
            task_id: Uuid::nil(),
            root: Uuid::nil(),
            parent: None
        })
        .is_none());
        assert!(DirectTask::recurring(&crate::default_protocol::Message {
            task_id: id,
            kind: crate::default_protocol::Kind::RedisBackendCleanup,
            expires: None
        })
        .is_none());
    }
    #[test]
    fn publication_refuses_non_recording_identifiers() {
        let id = Uuid::new_v4();
        let task = DirectTask::series(&crate::series_protocol::SeriesMessage {
            task_id: id,
            root: id,
            parent: None,
        })
        .unwrap();
        for recording in [0, -1, i64::from(i32::MAX) + 1] {
            assert!(encode(
                recording,
                task,
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                Utc::now()
            )
            .is_err());
        }
    }
}

#[cfg(test)]
mod canonical_controls {
    use super::*;
    #[test]
    fn raw_uppercase_or_missing_explicit_null_cannot_gain_configured_authority() {
        let id = Uuid::new_v4();
        let task = DirectTask::series(&crate::series_protocol::SeriesMessage {
            task_id: id,
            root: id,
            parent: None,
        })
        .unwrap();
        let raw = protocol::encode(
            &protocol::Task::CreateBackup,
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "owned",
        )
        .unwrap();
        assert!(canonical_frame(&raw, task));
        let mut frame: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        frame["headers"]["id"] = json!(id.to_string().to_uppercase());
        assert!(!canonical_frame(&serde_json::to_vec(&frame).unwrap(), task));
        frame["headers"]["id"] = json!(id.to_string());
        frame["headers"]
            .as_object_mut()
            .unwrap()
            .remove("parent_id");
        assert!(!canonical_frame(&serde_json::to_vec(&frame).unwrap(), task));
    }
    #[test]
    fn version_four_with_wrong_variant_is_not_captured_rfc4122_domain() {
        let mut bytes = *Uuid::new_v4().as_bytes();
        bytes[8] &= 0x3f;
        let id = Uuid::from_bytes(bytes);
        assert_eq!(id.get_version_num(), 4);
        assert!(DirectTask::series(&crate::series_protocol::SeriesMessage {
            task_id: id,
            root: id,
            parent: None
        })
        .is_none());
        assert!(DirectTask::recurring(&crate::default_protocol::Message {
            task_id: id,
            kind: crate::default_protocol::Kind::RecurringMaintenance,
            expires: None
        })
        .is_none());
    }
}
