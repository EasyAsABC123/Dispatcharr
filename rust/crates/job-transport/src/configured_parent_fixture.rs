//! Test-only OWNED parent-route control. Source11 parents use this queue while
//! real setup/artwork descendants stay UNTOUCHED on celery. No production route
//! or default-FIFO/full-workflow authority. Original authors retain credit.
use super::*;
pub(crate) const QUEUE: &str = "rust-owned-configured-recordings";
fn supported(
    queue: &str,
    database: Option<&str>,
    actual: Option<&str>,
    disposable: Option<&str>,
) -> bool {
    queue == QUEUE
        && database
            .is_some_and(|db| db.starts_with("rust_probe_backup_jobs_") && Some(db) == actual)
        && disposable == Some("playback-fixture-only")
}
#[derive(Clone)]
pub(crate) enum ParentMessage {
    Series(crate::series_protocol::SeriesMessage),
    Recurring(crate::default_protocol::Message),
}
impl ParentMessage {
    pub(crate) fn id(&self) -> Uuid {
        match self {
            Self::Series(v) => v.task_id,
            Self::Recurring(v) => v.task_id,
        }
    }
}
pub(crate) struct ParentCandidate {
    pub(crate) message: ParentMessage,
    bytes: Vec<u8>,
    origin: Arc<Inner>,
    held: Arc<Lease>,
}
impl ParentCandidate {
    pub(crate) fn canonical(&self, task: crate::recording_artwork::DirectTask) -> bool {
        crate::recording_artwork::canonical_frame(&self.bytes, task)
    }
}
// Only the copied-DB fixture route is projected for legacy parser validation.
// The original bytes remain the candidate/CAS authority and canonical input.
fn parse_parent(bytes: &[u8], series: bool) -> Result<ParentMessage, Error> {
    if bytes.len() > crate::protocol::MAX_ENVELOPE {
        return Err(Error::UnsupportedTask);
    }
    let mut validation: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| Error::UnsupportedTask)?;
    if validation["properties"]["delivery_info"]
        != serde_json::json!({"exchange":"","routing_key":QUEUE})
    {
        return Err(Error::UnsupportedTask);
    }
    validation["properties"]["delivery_info"]["routing_key"] = serde_json::json!("celery");
    let projected = serde_json::to_vec(&validation).map_err(|_| Error::UnsupportedTask)?;
    let (message, task) = if series {
        let value =
            crate::series_protocol::parse(&projected).map_err(|_| Error::UnsupportedTask)?;
        let task =
            crate::recording_artwork::DirectTask::series(&value).ok_or(Error::UnsupportedTask)?;
        (ParentMessage::Series(value), task)
    } else {
        let value =
            crate::default_protocol::parse(&projected).map_err(|_| Error::UnsupportedTask)?;
        let task = crate::recording_artwork::DirectTask::recurring(&value)
            .ok_or(Error::UnsupportedTask)?;
        (ParentMessage::Recurring(value), task)
    };
    if !crate::recording_artwork::canonical_frame(bytes, task) {
        return Err(Error::UnsupportedTask);
    }
    Ok(message)
}
impl Jobs {
    pub(crate) fn require_configured_parent_fixture(&self, queue: &str) -> Result<(), Error> {
        if supported(
            queue,
            self.copied_database(),
            std::env::var("POSTGRES_DB").ok().as_deref(),
            std::env::var("JOB_DISPOSABLE_FIXTURE").ok().as_deref(),
        ) {
            Ok(())
        } else {
            Err(Error::Configuration)
        }
    }
    pub(crate) async fn peek_configured_parent_fixture(
        &self,
        queue: &str,
        series: bool,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<ParentCandidate>, Error> {
        self.require_configured_parent_fixture(queue)?;
        let held = Arc::new(self.lease(caller)?);
        let (reply, _physical) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &[QUEUE.into()],
                &[],
                held.clone(),
            )
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => {
                let message = parse_parent(&reply.1, series)?;
                Ok(Some(ParentCandidate {
                    message,
                    bytes: reply.1,
                    origin: self.inner.clone(),
                    held,
                }))
            }
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn claim_configured_parent_fixture(
        &self,
        candidate: ParentCandidate,
    ) -> Result<(), Error> {
        self.require_configured_parent_fixture(QUEUE)?;
        if !Arc::ptr_eq(&self.inner, &candidate.origin) {
            return Err(Error::Configuration);
        }
        let id = candidate.message.id();
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &[QUEUE.into()],
                &[&candidate.bytes],
                candidate.held,
            )
            .await
        {
            Ok((1, _)) => Ok(()),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(id)),
            Err(e) => Err(map(e)),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_fixed_copied_parent_route_is_admitted() {
        let db = "rust_probe_backup_jobs_owned";
        assert!(supported(
            QUEUE,
            Some(db),
            Some(db),
            Some("playback-fixture-only")
        ));
        for queue in [
            "celery",
            "unknown",
            "rust-owned-configured-recordings-other",
        ] {
            assert!(!supported(
                queue,
                Some(db),
                Some(db),
                Some("playback-fixture-only")
            ));
        }
        assert!(!supported(
            QUEUE,
            None,
            Some(db),
            Some("playback-fixture-only")
        ));
        assert!(!supported(
            QUEUE,
            Some("real_database"),
            Some("real_database"),
            Some("playback-fixture-only")
        ));
        assert!(!supported(
            QUEUE,
            Some(db),
            Some("other"),
            Some("playback-fixture-only")
        ));
        assert!(!supported(QUEUE, Some(db), Some(db), None));
    }
    fn owned_series_frame() -> serde_json::Value {
        let source: serde_json::Value =
            serde_json::from_str(include_str!("../datasets/source-series-rules-wire.json"))
                .unwrap();
        let mut frame = source["cases"][0]["envelope"].clone();
        let id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        frame["headers"]["id"] = serde_json::json!(id);
        frame["headers"]["root_id"] = serde_json::json!(id);
        frame["properties"]["correlation_id"] = serde_json::json!(id);
        frame["properties"]["delivery_info"]["routing_key"] = serde_json::json!(QUEUE);
        frame
    }
    #[test]
    fn only_owned_validation_clone_bridges_legacy_route() {
        let frame = owned_series_frame();
        let raw = serde_json::to_vec(&frame).unwrap();
        assert!(crate::series_protocol::parse(&raw).is_err());
        assert!(matches!(
            parse_parent(&raw, true),
            Ok(ParentMessage::Series(_))
        ));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&raw).unwrap(),
            frame
        );
        for (key, value) in [
            ("exchange", "foreign"),
            ("routing_key", "celery"),
            ("routing_key", "unknown"),
        ] {
            let mut wrong = frame.clone();
            wrong["properties"]["delivery_info"][key] = serde_json::json!(value);
            assert!(parse_parent(&serde_json::to_vec(&wrong).unwrap(), true).is_err());
        }
    }
    #[test]
    fn owned_route_still_refuses_noncanonical_ids_and_parented_frames() {
        let frame = owned_series_frame();
        for key in ["id", "root_id"] {
            let mut wrong = frame.clone();
            wrong["headers"][key] = serde_json::json!("AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA");
            assert!(parse_parent(&serde_json::to_vec(&wrong).unwrap(), true).is_err());
        }
        let mut parented = frame.clone();
        parented["headers"]["parent_id"] =
            serde_json::json!("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb");
        assert!(parse_parent(&serde_json::to_vec(&parented).unwrap(), true).is_err());
        let mut other = frame;
        other["headers"]["task"] =
            serde_json::json!("apps.channels.tasks.prefetch_recording_artwork");
        assert!(parse_parent(&serde_json::to_vec(&other).unwrap(), true).is_err());
    }
    #[test]
    fn actual_artwork_descendant_cannot_be_a_parent() {
        let id = Uuid::new_v4();
        let parent = Uuid::new_v4();
        let task =
            crate::recording_artwork::DirectTask::series(&crate::series_protocol::SeriesMessage {
                task_id: parent,
                root: parent,
                parent: None,
            })
            .unwrap();
        let bytes = crate::recording_artwork::encode(
            1,
            task,
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            chrono::Utc::now(),
        )
        .unwrap();
        assert!(crate::series_protocol::parse(&bytes).is_err());
        assert!(crate::default_protocol::parse(&bytes).is_err());
    }
}
