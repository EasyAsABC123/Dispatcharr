//! Narrow consumer methods, inaccessible through any legacy public constructor.
use super::*;
use crate::recording_artwork_protocol::Message;
pub(crate) struct Candidate {
    pub message: Message,
    bytes: Vec<u8>,
    origin: Arc<Inner>,
}
impl Jobs {
    fn local_artwork_authorized(&self) -> bool {
        #[cfg(test)]
        if self.inner.authority == Authority::LocalArtworkFixture {
            // Legacy copied_database() intentionally recognizes only Fixture.
            // This dedicated cfgtest authority keeps the same stored copiedDB,
            // but must validate it directly rather than reject its own tag.
            return artwork_fixture_authorized(
                self.inner.authority,
                &self.inner.database,
                std::env::var("POSTGRES_DB").ok().as_deref(),
                std::env::var("JOB_DISPOSABLE_FIXTURE").ok().as_deref(),
            );
        }
        self.inner.authority == Authority::LocalArtwork
    }
    fn local_artwork_queue(&self) -> &'static str {
        #[cfg(test)]
        if self.inner.authority == Authority::LocalArtworkFixture {
            return "rust-owned-parented-artwork";
        }
        "celery"
    }
    #[cfg(test)]
    pub(crate) async fn connect_local_artwork_fixture(
        broker: &str,
        backend: &str,
        database: &str,
        config: Config,
    ) -> Result<Self, Error> {
        let mut jobs = Self::connect_fixture(broker, backend, database, config).await?;
        jobs.require_parented_fixture("rust-owned-parented-artwork")?;
        Arc::get_mut(&mut jobs.inner)
            .ok_or(Error::Configuration)?
            .authority = Authority::LocalArtworkFixture;
        Ok(jobs)
    }

    pub(crate) async fn connect_local_artwork(
        broker: &str,
        backend: &str,
        config: Config,
    ) -> Result<Self, Error> {
        Self::connect_owned(
            broker,
            backend,
            config,
            String::new(),
            Authority::LocalArtwork,
        )
        .await
    }
    pub(crate) async fn peek_local_artwork(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<Candidate>, Error> {
        if !self.local_artwork_authorized() {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller)?;
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &[self.local_artwork_queue().into()],
                &[],
                lease,
            )
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => {
                let validation = local_validation(&reply.1, self.local_artwork_queue())?;
                let message = crate::recording_artwork_protocol::parse(&validation)
                    .ok_or(Error::UnsupportedTask)?;
                Ok(Some(Candidate {
                    message,
                    bytes: reply.1,
                    origin: self.inner.clone(),
                }))
            }
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn claim_local_artwork(
        &self,
        candidate: Candidate,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if !self.local_artwork_authorized() || !Arc::ptr_eq(&self.inner, &candidate.origin) {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller)?;
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &[self.local_artwork_queue().into()],
                &[&candidate.bytes],
                lease,
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
    pub(crate) async fn write_local_artwork(
        &self,
        message: &Message,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.local_artwork_authorized() {
            return Err(Error::UnsupportedTask);
        }
        self.write_result(
            message.task_id,
            serde_json::json!("ok"),
            Some(message.parent),
            vec![],
            caller,
        )
        .await
    }
}

fn local_validation(raw: &[u8], queue: &str) -> Result<Vec<u8>, Error> {
    if raw.len() > crate::protocol::MAX_ENVELOPE {
        return Err(Error::Protocol);
    }
    if queue == "celery" {
        return Ok(raw.to_vec());
    }
    #[cfg(test)]
    if queue == "rust-owned-parented-artwork" {
        let mut frame: serde_json::Value =
            serde_json::from_slice(raw).map_err(|_| Error::Protocol)?;
        if frame["properties"]["delivery_info"]
            != serde_json::json!({"exchange":"","routing_key":queue})
        {
            return Err(Error::UnsupportedTask);
        }
        frame["properties"]["delivery_info"]["routing_key"] = serde_json::json!("celery");
        return serde_json::to_vec(&frame).map_err(|_| Error::Protocol);
    }
    Err(Error::UnsupportedTask)
}
#[cfg(test)]
pub(crate) fn parse_fixture(raw: &[u8]) -> Result<Message, Error> {
    let validation = local_validation(raw, "rust-owned-parented-artwork")?;
    crate::recording_artwork_protocol::parse(&validation).ok_or(Error::UnsupportedTask)
}

#[cfg(test)]
fn artwork_fixture_authorized(
    authority: Authority,
    database: &str,
    actual: Option<&str>,
    guard: Option<&str>,
) -> bool {
    authority == Authority::LocalArtworkFixture
        && database.starts_with("rust_probe_backup_jobs_")
        && database.len() <= 63
        && database
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && actual == Some(database)
        && guard == Some("playback-fixture-only")
}
#[cfg(test)]
mod route_controls {
    use super::*;
    #[test]
    fn dedicated_fixture_authority_retains_copied_db_without_widening_legacy_fixture() {
        let db = "rust_probe_backup_jobs_artwork";
        assert!(artwork_fixture_authorized(
            Authority::LocalArtworkFixture,
            db,
            Some(db),
            Some("playback-fixture-only")
        ));
        for authority in [
            Authority::Fixture,
            Authority::LocalArtwork,
            Authority::Series,
            Authority::Refresh,
            Authority::Index,
            Authority::DefaultServices,
            Authority::Backup,
        ] {
            assert!(!artwork_fixture_authorized(
                authority,
                db,
                Some(db),
                Some("playback-fixture-only")
            ));
        }
        for (database, actual, guard) in [
            (
                "dispatcharr",
                Some("dispatcharr"),
                Some("playback-fixture-only"),
            ),
            (db, None, Some("playback-fixture-only")),
            (
                db,
                Some("rust_probe_backup_jobs_other"),
                Some("playback-fixture-only"),
            ),
            (db, Some(db), None),
            (
                "rust_probe_backup_jobs_BAD",
                Some("rust_probe_backup_jobs_BAD"),
                Some("playback-fixture-only"),
            ),
        ] {
            assert!(!artwork_fixture_authorized(
                Authority::LocalArtworkFixture,
                database,
                actual,
                guard
            ));
        }
    }
    #[test]
    fn only_exact_owned_route_projects_validation_and_raw_cas_bytes_remain_separate() {
        use base64::Engine;
        let source: serde_json::Value = serde_json::from_str(include_str!(
            "../datasets/source-local-recording-artwork.json"
        ))
        .unwrap();
        let raw = base64::engine::general_purpose::STANDARD
            .decode(source["frames_raw_base64"][0].as_str().unwrap())
            .unwrap();
        let original = raw.clone();
        let validation = local_validation(&raw, "rust-owned-parented-artwork").unwrap();
        assert_eq!(raw, original);
        assert_ne!(raw, validation);
        assert!(crate::recording_artwork_protocol::parse(&validation).is_some());
        assert!(crate::recording_artwork_protocol::parse(&raw).is_none());
        assert!(local_validation(&raw, "unknown").is_err());
        let mut changed: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        changed["properties"]["delivery_info"]["exchange"] = serde_json::json!("unknown");
        assert!(local_validation(
            &serde_json::to_vec(&changed).unwrap(),
            "rust-owned-parented-artwork"
        )
        .is_err());
    }
}
