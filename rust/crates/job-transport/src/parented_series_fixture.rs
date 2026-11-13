//! Only the exact copied-DB parented fixture route; production routing unchanged.
use super::*;
pub(crate) const SERIES_QUEUE: &str = "rust-owned-parented-series";
pub(crate) const ARTWORK_QUEUE: &str = "rust-owned-parented-artwork";
pub(crate) struct Candidate {
    pub(crate) message: crate::series_protocol::SeriesMessage,
    bytes: Vec<u8>,
    origin: Arc<Inner>,
    held: Arc<Lease>,
}
impl Candidate {
    pub(crate) fn canonical(&self, task: crate::recording_artwork::DirectTask) -> bool {
        crate::recording_artwork::canonical_frame(&self.bytes, task)
    }
}
pub(crate) fn parse_frame(bytes: &[u8]) -> Result<crate::series_protocol::SeriesMessage, Error> {
    if bytes.len() > crate::protocol::MAX_ENVELOPE {
        return Err(Error::Protocol);
    }
    let mut validation: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| Error::Protocol)?;
    if validation["properties"]["delivery_info"]
        != serde_json::json!({"exchange":"","routing_key":SERIES_QUEUE})
    {
        return Err(Error::UnsupportedTask);
    }
    validation["properties"]["delivery_info"]["routing_key"] = serde_json::json!("celery");
    crate::series_protocol::parse(&serde_json::to_vec(&validation).map_err(|_| Error::Protocol)?)
        .map_err(|_| Error::Protocol)
}
fn supported(
    queue: &str,
    database: Option<&str>,
    actual: Option<&str>,
    guard: Option<&str>,
) -> bool {
    [SERIES_QUEUE, ARTWORK_QUEUE].contains(&queue)
        && database
            .is_some_and(|db| db.starts_with("rust_probe_backup_jobs_") && Some(db) == actual)
        && guard == Some("playback-fixture-only")
}
impl Jobs {
    pub(crate) fn require_parented_fixture(&self, queue: &str) -> Result<(), Error> {
        if !supported(
            queue,
            self.copied_database(),
            std::env::var("POSTGRES_DB").ok().as_deref(),
            std::env::var("JOB_DISPOSABLE_FIXTURE").ok().as_deref(),
        ) {
            return Err(Error::Configuration);
        }
        Ok(())
    }
    pub(crate) async fn peek_parented_series_fixture(
        &self,
        cap: crate::recording_artwork::RefreshSeriesChild,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Candidate>, Error> {
        self.require_parented_fixture(SERIES_QUEUE)?;
        let held = Arc::new(self.lease(owner)?);
        let (reply, _physical) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &[SERIES_QUEUE.into()],
                &[],
                held.clone(),
            )
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => {
                let message = parse_frame(&reply.1)?;
                let task = cap.admit(&message).ok_or(Error::UnsupportedTask)?;
                if !crate::recording_artwork::canonical_frame(&reply.1, task) {
                    return Err(Error::UnsupportedTask);
                }
                Ok(Some(Candidate {
                    message,
                    bytes: reply.1,
                    origin: self.inner.clone(),
                    held,
                }))
            }
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn claim_parented_series_fixture(
        &self,
        candidate: Candidate,
    ) -> Result<(), Error> {
        self.require_parented_fixture(SERIES_QUEUE)?;
        if !Arc::ptr_eq(&self.inner, &candidate.origin) {
            return Err(Error::Configuration);
        }
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &[SERIES_QUEUE.into()],
                &[&candidate.bytes],
                candidate.held,
            )
            .await
        {
            Ok((1, _)) => Ok(()),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(candidate.message.task_id)),
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn publish_parented_artwork_fixture(
        &self,
        bytes: &[u8],
        id: Uuid,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        self.require_parented_fixture(ARTWORK_QUEUE)?;
        if bytes.len() > crate::protocol::MAX_ENVELOPE {
            return Err(Error::Protocol);
        }
        let frame: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| Error::Protocol)?;
        if frame["headers"]["task"] != "apps.channels.tasks.prefetch_recording_artwork"
            || frame["headers"]["id"] != id.to_string()
            || frame["properties"]["delivery_info"]
                != serde_json::json!({"exchange":"","routing_key":ARTWORK_QUEUE})
        {
            return Err(Error::Protocol);
        }
        let held = self.lease(owner)?;
        let limit = self.inner.config.max_queued_jobs.to_string();
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                PUBLISH_OWNED_ARTWORK,
                &[
                    ARTWORK_QUEUE.into(),
                    "_kombu.binding.rust-owned-parented-artwork".into(),
                ],
                &[bytes, limit.as_bytes()],
                held,
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
            Err(e) => Err(map(e)),
        }
    }
}
const PUBLISH_OWNED_ARTWORK: &str = r#"
local q=redis.call('TYPE',KEYS[1]).ok;local b=redis.call('TYPE',KEYS[2]).ok
if(q~='none'and q~='list')or(b~='none'and b~='set')then return -3 end
if #ARGV[1]>65536 then return -1 end
if redis.call('LLEN',KEYS[1])>=tonumber(ARGV[2])then return -2 end
redis.call('SADD',KEYS[2],'rust-owned-parented-artwork'..string.char(6,22)..string.char(6,22)..'rust-owned-parented-artwork')
return redis.call('LPUSH',KEYS[1],ARGV[1])
"#;

#[cfg(test)]
mod controls {
    use super::*;
    #[test]
    fn only_exact_owned_routes_database_and_disposable_guard() {
        let db = "rust_probe_backup_jobs_owned";
        for q in [SERIES_QUEUE, ARTWORK_QUEUE] {
            assert!(supported(
                q,
                Some(db),
                Some(db),
                Some("playback-fixture-only")
            ));
        }
        for q in ["celery", "rust-owned-configured-recordings", "unknown"] {
            assert!(!supported(
                q,
                Some(db),
                Some(db),
                Some("playback-fixture-only")
            ));
        }
        for (expected, actual, guard) in [
            (None, Some(db), Some("playback-fixture-only")),
            (
                Some("dispatcharr"),
                Some("dispatcharr"),
                Some("playback-fixture-only"),
            ),
            (
                Some(db),
                Some("rust_probe_backup_jobs_other"),
                Some("playback-fixture-only"),
            ),
            (Some(db), Some(db), None),
        ] {
            assert!(!supported(SERIES_QUEUE, expected, actual, guard));
        }
    }
    #[test]
    fn validation_route_clone_does_not_modify_original_cas_bytes() {
        let id = Uuid::parse_str("9f7c6077-8205-4e75-883e-098c181747c9").unwrap();
        let root = "cb78e036-04f3-40d0-a07c-591ed05ef09c";
        let mut frame: serde_json::Value = serde_json::from_slice(
            &crate::protocol::encode(
                &crate::protocol::Task::CreateBackup,
                id,
                Uuid::new_v4(),
                Uuid::new_v4(),
                "owned",
            )
            .unwrap(),
        )
        .unwrap();
        frame["headers"]["task"] = serde_json::json!("apps.channels.tasks.evaluate_series_rules");
        frame["headers"]["root_id"] = serde_json::json!(root);
        frame["headers"]["parent_id"] = serde_json::json!(root);
        frame["properties"]["delivery_info"]["routing_key"] = serde_json::json!(SERIES_QUEUE);
        let original = serde_json::to_vec(&frame).unwrap();
        let preserved = original.clone();
        let parsed = parse_frame(&original).unwrap();
        assert_eq!(parsed.task_id, id);
        assert_eq!(original, preserved);
        for info in [
            serde_json::json!({"exchange":"celery","routing_key":SERIES_QUEUE}),
            serde_json::json!({"exchange":"","routing_key":"celery"}),
            serde_json::json!({"exchange":"","routing_key":"unknown"}),
        ] {
            let mut wrong = frame.clone();
            wrong["properties"]["delivery_info"] = info;
            assert!(parse_frame(&serde_json::to_vec(&wrong).unwrap()).is_err());
        }
        frame["headers"]["task"] =
            serde_json::json!("apps.channels.tasks.prefetch_recording_artwork");
        assert!(parse_frame(&serde_json::to_vec(&frame).unwrap()).is_err());
    }
}
