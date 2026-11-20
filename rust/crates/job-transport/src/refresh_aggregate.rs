//! Selected ordinary XMLTV aggregate. Original Dispatcharr and Celery authors retain credit.
use crate::{protocol::ProtocolError, refresh_protocol::RefreshMessage, worker};
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

pub(crate) const NAME: &str = "apps.epg.tasks.refresh_all_epg_data";
pub(crate) const SUCCESS: &str = "EPG data refreshed.";
pub(crate) const MAX_SOURCES: i64 = 32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RefreshWork {
    Individual(Box<RefreshMessage>),
    Aggregate,
}

/// The wire identity belongs to the reserved message, not an inline source invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScheduledMessage {
    pub task_id: Uuid,
    pub root: Uuid,
    pub parent: Option<Uuid>,
    pub eta: Option<DateTime<Utc>>,
    pub work: RefreshWork,
}

impl ScheduledMessage {
    pub(crate) fn prepare(mut frame: Value) -> Result<Self, ProtocolError> {
        let invalid = ProtocolError::InvalidArgument;
        if frame["headers"]["task"] != NAME {
            let wire = serde_json::to_vec(&frame).map_err(|_| invalid)?;
            let message = crate::refresh_protocol::parse(&wire)?;
            return Ok(Self {
                task_id: message.task_id,
                root: message.root,
                parent: message.parent,
                eta: message.eta,
                work: RefreshWork::Individual(Box::new(message)),
            });
        }
        // Only the witnessed custom Beat shape is admitted.
        crate::periodic_headers::take(&mut frame)?.ok_or(invalid)?;
        let task_id = worker::no_argument_root_identity(
            &frame,
            worker::ImmediateNoArgumentTask::XmltvAggregate,
        )
        .map_err(|_| invalid)?;
        Ok(Self {
            task_id,
            root: task_id,
            parent: None,
            eta: None,
            work: RefreshWork::Aggregate,
        })
    }

    pub(crate) fn aggregate(&self) -> bool {
        matches!(self.work, RefreshWork::Aggregate)
    }

    pub(crate) fn individual(&self) -> &RefreshMessage {
        match &self.work {
            RefreshWork::Individual(work) => work,
            RefreshWork::Aggregate => unreachable!("aggregate belongs only to selected intake"),
        }
    }
}

pub(crate) enum Admission {
    Ordinary(XmltvSources),
    Unsupported,
}

/// IDs from one completed Original-equivalent selection query.
pub struct XmltvSources(Vec<i64>);
impl XmltvSources {
    pub(crate) async fn read(store: &dispatcharr_store::Store) -> Result<Admission, sqlx::Error> {
        let rows = sqlx::query_as::<_, (i64, String, bool)>(
            "SELECT id::bigint, source_type, NOT (
                EXISTS (SELECT FROM plugins_pluginconfig WHERE enabled)
                OR EXISTS (SELECT FROM dispatcharr_connect_eventsubscription s
                           JOIN dispatcharr_connect_integration i ON i.id = s.integration_id
                           WHERE s.enabled AND i.enabled)
            )
            FROM epg_epgsource WHERE is_active AND source_type <> 'dummy' LIMIT $1",
        )
        .bind(MAX_SOURCES + 1)
        .fetch_all(store.pool())
        .await?;
        if rows.len() > MAX_SOURCES as usize
            || rows
                .iter()
                .any(|(id, source_type, plain)| *id <= 0 || source_type != "xmltv" || !plain)
        {
            return Ok(Admission::Unsupported);
        }
        Ok(Admission::Ordinary(Self(
            rows.into_iter().map(|(id, _, _)| id).collect(),
        )))
    }

    pub(crate) fn into_ids(self) -> Vec<i64> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{encode, Task};
    use serde_json::json;

    #[test]
    fn only_exact_ordinary_beat_shape_is_an_aggregate() {
        let id = Uuid::new_v4();
        let common = encode(
            &Task::CreateBackup,
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "owned",
        )
        .unwrap();
        let mut frame: Value = serde_json::from_slice(&common).unwrap();
        frame["headers"]["task"] = json!(NAME);
        assert!(ScheduledMessage::prepare(frame.clone()).is_err());
        frame["headers"]["periodic_task_name"] = json!("ordinary custom row");
        assert_eq!(
            ScheduledMessage::prepare(frame.clone()).unwrap(),
            ScheduledMessage {
                task_id: id,
                root: id,
                parent: None,
                eta: None,
                work: RefreshWork::Aggregate,
            }
        );
        frame["headers"]["timelimit"] = json!([14400, null]);
        assert!(ScheduledMessage::prepare(frame).is_err());
    }
}
