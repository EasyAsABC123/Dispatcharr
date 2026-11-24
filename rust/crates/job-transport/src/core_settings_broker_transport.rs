//! Actual bounded publication through Main's already connected broker transport.
use super::*;
use crate::core_settings_broker::CoreSettingsPublication;
use crate::decode::{Metadata, State};
use dispatcharr_store::{CoreOffsetPublication, ProviderRehashResult};
use sha2::{Digest, Sha256};
use std::{future::Future, sync::Mutex};

#[derive(PartialEq, Eq)]
struct ClaimIdentity {
    task_id: Uuid,
    frame_sha256: [u8; 32],
}

impl ClaimIdentity {
    fn new(task_id: Uuid, frame: &[u8]) -> Self {
        Self {
            task_id,
            frame_sha256: Sha256::digest(frame).into(),
        }
    }
}

// At most one active observation per Jobs. The provider's existing admission
// lease serializes its publications. This slot never controls queue admission.
#[derive(Default)]
pub(super) struct ProviderRehashClaim {
    active: Mutex<Option<(ClaimIdentity, bool)>>,
}

struct ClaimObservation<'a>(&'a ProviderRehashClaim);

impl ProviderRehashClaim {
    fn observe(&self, task_id: Uuid, frame: &[u8]) -> Option<ClaimObservation<'_>> {
        let identity = ClaimIdentity::new(task_id, frame);
        let mut slot = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if slot.is_some() {
            return None;
        }
        *slot = Some((identity, false));
        Some(ClaimObservation(self))
    }

    pub(super) fn claimed(&self, task_id: Uuid, frame: &[u8]) {
        let identity = ClaimIdentity::new(task_id, frame);
        let mut slot = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some((expected, claimed)) = slot.as_mut() {
            if *expected == identity {
                *claimed = true;
            }
        }
    }
}

impl ClaimObservation<'_> {
    fn confirmed(&self) -> bool {
        self.0
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .is_some_and(|(_, claimed)| *claimed)
    }
}

impl Drop for ClaimObservation<'_> {
    fn drop(&mut self) {
        // Only this guard can vacate the slot. observe() cannot replace it.
        *self
            .0
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = None;
    }
}

async fn provider_rehash_outcome<F>(
    task_id: Uuid,
    publication: Result<i64, Error>,
    claimed: impl Fn() -> bool,
    lookup: impl FnOnce(Uuid) -> F,
) -> Result<ProviderRehashResult, Error>
where
    F: Future<Output = Result<Option<Metadata>, Error>>,
{
    match publication {
        Ok(n) if n > 0 => Ok(ProviderRehashResult::Acknowledged),
        Ok(-4 | -5) => Ok(ProviderRehashResult::Refused),
        Ok(-2) => Err(Error::Capacity),
        Ok(_) => Err(Error::Corrupt),
        Err(error @ Error::AmbiguousPublication(id)) if id == task_id => {
            if claimed() {
                return Ok(ProviderRehashResult::Acknowledged);
            }
            // The result backend is trusted and identity-bound. A worker result
            // proves admission even when the task ran unsuccessfully. Revocation
            // alone is inconclusive because control can write it without a claim.
            if lookup(id).await.is_ok_and(|result| {
                result.is_some_and(|metadata| {
                    metadata.task_id == id
                        && matches!(metadata.state, State::Success | State::Failure)
                        && metadata.date_done.as_deref().is_some_and(|date_done| {
                            chrono::DateTime::parse_from_rfc3339(date_done).is_ok()
                        })
                })
            }) || claimed()
            {
                Ok(ProviderRehashResult::Acknowledged)
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

fn offset_outcome(result: Result<i64, Error>) -> Result<CoreOffsetPublication, Error> {
    match result {
        Ok(n) if n > 0 => Ok(CoreOffsetPublication::Acknowledged),
        // Capacity returns before admission or before SADD/LPUSH. The
        // reviewed explicit LPUSH rejection returns -4 with no queue effect.
        Ok(-2 | -4) | Err(Error::Capacity) => Ok(CoreOffsetPublication::NotEnqueued),
        Ok(_) => Err(Error::Corrupt),
        Err(error) => Err(error),
    }
}

fn core_settings_outcome(
    result: Result<i64, Error>,
    global_rehash: bool,
) -> Result<CoreSettingsPublication, Error> {
    match result? {
        n if n > 0 => Ok(CoreSettingsPublication::Acknowledged),
        -4 if global_rehash => Ok(CoreSettingsPublication::GlobalRehashRefused),
        -5 if global_rehash => Ok(CoreSettingsPublication::GlobalRehashDefaultAclRefused),
        -2 => Err(Error::Capacity),
        _ => Err(Error::Corrupt),
    }
}

impl Jobs {
    pub(crate) async fn publish_dvr_offsets(
        &self,
        publication: dispatcharr_store::CoreTaskPublication,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<CoreOffsetPublication, Error> {
        let message =
            crate::normal_core_tasks::parse(publication.bytes()).map_err(|_| Error::Protocol)?;
        if !matches!(message.kind, crate::normal_core_tasks::Kind::Offsets) {
            return Err(Error::Protocol);
        }
        offset_outcome(
            self.publish_core_frame(
                publication,
                owner,
                message.id,
                PUBLISH_PROVIDER_REHASH,
                false,
            )
            .await,
        )
    }

    pub(crate) async fn publish_core_settings(
        &self,
        publication: dispatcharr_store::CoreTaskPublication,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<CoreSettingsPublication, Error> {
        // The same sealed Store producer also owns Source's new STD-account
        // refresh_m3u_groups.delay publication; normal queue dispatch already
        // delegates this exact root envelope to its existing STD implementation.
        let (task_id, programme_root, global_rehash) = if let Ok(message) =
            crate::normal_core_tasks::parse(publication.bytes())
        {
            // The settings update owns a global lock handoff. An explicit
            // LPUSH refusal reaches Original's unhandled HTTP 500; its
            // finally block releases the locks after the already saved row.
            let global_rehash = matches!(
                message.kind,
                crate::normal_core_tasks::Kind::Rehash(
                    dispatcharr_store::core_task_rehash::Request {
                        account: None,
                        preacquired: true,
                        token: Some(_),
                        ..
                    }
                )
            );
            (message.id, false, global_rehash)
        } else if let Ok(message) = crate::normal_xc_live::parse(publication.bytes()) {
            (message.id, false, false)
        } else if let Ok(message) = crate::normal_xc_accounts::parse(publication.bytes()) {
            // Direct HTTP Groups publishes an actual unparented Profiles
            // root. It is not a synthetic Groups child or Info request.
            if message.kind != crate::normal_xc_accounts::Kind::Profiles || message.parent.is_some()
            {
                return Err(Error::Protocol);
            }
            (message.id, false, false)
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
            (message.common.id, true, false)
        };
        let script = if global_rehash {
            PUBLISH_PROVIDER_REHASH
        } else {
            PUBLISH
        };
        core_settings_outcome(
            self.publish_core_frame(publication, owner, task_id, script, programme_root)
                .await,
            global_rehash,
        )
    }

    pub(crate) async fn publish_provider_rehash(
        &self,
        publication: dispatcharr_store::CoreTaskPublication,
        owner: Arc<dyn Send + Sync>,
        task_id: Uuid,
    ) -> Result<ProviderRehashResult, Error> {
        let observation = self
            .inner
            .provider_rehash_claim
            .observe(task_id, publication.bytes());
        let published = self
            .publish_core_frame(
                publication,
                owner.clone(),
                task_id,
                PUBLISH_PROVIDER_REHASH,
                false,
            )
            .await;
        provider_rehash_outcome(
            task_id,
            published,
            || {
                observation
                    .as_ref()
                    .is_some_and(ClaimObservation::confirmed)
            },
            |id| self.read(id, owner),
        )
        .await
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::ready;

    #[test]
    fn only_explicit_lpush_refusal_classifies_as_global_rehash_refused() {
        let ambiguous = Error::AmbiguousPublication(Uuid::new_v4());
        assert_eq!(
            core_settings_outcome(Ok(-4), true),
            Ok(CoreSettingsPublication::GlobalRehashRefused)
        );
        assert_eq!(core_settings_outcome(Ok(-4), false), Err(Error::Corrupt));
        assert_eq!(
            core_settings_outcome(Ok(-5), true),
            Ok(CoreSettingsPublication::GlobalRehashDefaultAclRefused)
        );
        assert_eq!(core_settings_outcome(Ok(-5), false), Err(Error::Corrupt));
        assert_eq!(core_settings_outcome(Ok(-2), true), Err(Error::Capacity));
        assert_eq!(core_settings_outcome(Err(ambiguous), true), Err(ambiguous));
        assert_eq!(
            core_settings_outcome(Ok(1), true),
            Ok(CoreSettingsPublication::Acknowledged)
        );
    }

    #[test]
    fn dvr_offset_fallback_requires_proven_non_enqueue() {
        let id = Uuid::new_v4();
        for result in [Ok(-4), Ok(-2), Err(Error::Capacity)] {
            assert_eq!(
                offset_outcome(result),
                Ok(CoreOffsetPublication::NotEnqueued)
            );
        }
        assert_eq!(
            offset_outcome(Ok(1)),
            Ok(CoreOffsetPublication::Acknowledged)
        );
        for error in [
            Error::AmbiguousPublication(id),
            Error::Closed,
            Error::Protocol,
            Error::Corrupt,
            Error::Backend,
        ] {
            assert_eq!(offset_outcome(Err(error)), Err(error));
        }
        assert_eq!(offset_outcome(Ok(-3)), Err(Error::Corrupt));
    }

    fn metadata(task_id: Uuid, state: State) -> Metadata {
        Metadata {
            task_id,
            state,
            result: serde_json::Value::Null,
            date_done: Some("2026-10-15T08:30:00+00:00".into()),
            traceback: None,
            exception: None,
            tagged_values: Vec::new(),
        }
    }

    #[tokio::test]
    async fn provider_rehash_only_matching_ambiguous_publication_looks_up_a_result() {
        let id = Uuid::new_v4();
        let other = Uuid::new_v4();
        let cases = [
            (Ok(1), Ok(ProviderRehashResult::Acknowledged)),
            (Ok(-4), Ok(ProviderRehashResult::Refused)),
            (Ok(-2), Err(Error::Capacity)),
            (Ok(0), Err(Error::Corrupt)),
            (Ok(-3), Err(Error::Corrupt)),
            (Err(Error::Backend), Err(Error::Backend)),
            (Err(Error::Closed), Err(Error::Closed)),
            (Err(Error::Capacity), Err(Error::Capacity)),
            (Err(Error::Protocol), Err(Error::Protocol)),
            (Err(Error::Configuration), Err(Error::Configuration)),
            (Err(Error::UnsupportedTask), Err(Error::UnsupportedTask)),
            (
                Err(Error::AmbiguousPublication(other)),
                Err(Error::AmbiguousPublication(other)),
            ),
        ];
        for (publication, expected) in cases {
            let actual = provider_rehash_outcome(
                id,
                publication,
                || panic!("claim read without matching ambiguous publication"),
                |_| -> std::future::Ready<Result<Option<Metadata>, Error>> {
                    panic!("result lookup without matching ambiguous publication")
                },
            )
            .await;
            assert_eq!(actual, expected);
        }
    }

    #[tokio::test]
    async fn provider_rehash_matching_execution_terminal_confirms_only_acceptance() {
        let id = Uuid::new_v4();
        for state in [State::Success, State::Failure] {
            let mut lookups = 0;
            let actual = provider_rehash_outcome(
                id,
                Err(Error::AmbiguousPublication(id)),
                || false,
                |key| {
                    assert_eq!(key, id);
                    lookups += 1;
                    ready(Ok(Some(metadata(id, state))))
                },
            )
            .await;
            assert_eq!(lookups, 1);
            assert_eq!(actual, Ok(ProviderRehashResult::Acknowledged));
        }
    }

    #[tokio::test]
    async fn provider_rehash_unproven_result_preserves_ambiguous_outcome() {
        let id = Uuid::new_v4();
        let wrong = Uuid::new_v4();
        let mut undated = metadata(id, State::Success);
        undated.date_done = None;
        let mut malformed_date = metadata(id, State::Success);
        malformed_date.date_done = Some("unknown".into());
        let cases = [
            Ok(None),
            Ok(Some(metadata(wrong, State::Success))),
            Ok(Some(metadata(id, State::Pending))),
            Ok(Some(metadata(id, State::Started))),
            Ok(Some(metadata(id, State::Retry))),
            Ok(Some(metadata(id, State::Revoked))),
            Ok(Some(undated)),
            Ok(Some(malformed_date)),
            Err(Error::Backend),
            Err(Error::Closed),
            Err(Error::Capacity),
            Err(Error::Decode(crate::decode::DecodeError::Identity)),
            Err(Error::Decode(crate::decode::DecodeError::Malformed)),
        ];
        for result in cases {
            let mut lookups = 0;
            let actual = provider_rehash_outcome(
                id,
                Err(Error::AmbiguousPublication(id)),
                || false,
                |key| {
                    assert_eq!(key, id);
                    lookups += 1;
                    ready(result)
                },
            )
            .await;
            assert_eq!(lookups, 1);
            assert_eq!(actual, Err(Error::AmbiguousPublication(id)));
        }
    }

    #[test]
    fn provider_rehash_claim_matches_current_exact_frame_in_same_jobs_only() {
        let jobs = ProviderRehashClaim::default();
        let other_jobs = ProviderRehashClaim::default();
        let task = Uuid::new_v4();
        let other_task = Uuid::new_v4();
        let observation = jobs.observe(task, b"owned frame").unwrap();
        assert!(jobs.observe(other_task, b"next frame").is_none());
        other_jobs.claimed(task, b"owned frame");
        jobs.claimed(other_task, b"owned frame");
        jobs.claimed(task, b"other frame");
        assert!(!observation.confirmed());
        jobs.claimed(task, b"owned frame");
        assert!(observation.confirmed());
        drop(observation);
        jobs.claimed(task, b"owned frame");
        assert!(!jobs.observe(other_task, b"next frame").unwrap().confirmed());
    }

    #[tokio::test]
    async fn provider_rehash_cancelled_observer_retires_slot() {
        let jobs = Arc::new(ProviderRehashClaim::default());
        let task = Uuid::new_v4();
        let (started, registered) = tokio::sync::oneshot::channel();
        let owned = jobs.clone();
        let waiter = tokio::spawn(async move {
            let observation = owned.observe(task, b"owned frame").unwrap();
            started.send(()).unwrap();
            std::future::pending::<()>().await;
            drop(observation);
        });
        registered.await.unwrap();
        jobs.claimed(task, b"owned frame");
        assert!(jobs.observe(task, b"owned frame").is_none());
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(!jobs.observe(task, b"owned frame").unwrap().confirmed());
    }

    #[tokio::test]
    async fn provider_rehash_local_claim_can_confirm_before_worker_result() {
        let jobs = ProviderRehashClaim::default();
        let task = Uuid::new_v4();
        let observation = jobs.observe(task, b"owned frame").unwrap();
        jobs.claimed(task, b"owned frame");
        let verdict = provider_rehash_outcome(
            task,
            Err(Error::AmbiguousPublication(task)),
            || observation.confirmed(),
            |_| -> std::future::Ready<Result<Option<Metadata>, Error>> {
                panic!("confirmed claim must not issue a result lookup")
            },
        )
        .await;
        assert_eq!(verdict, Ok(ProviderRehashResult::Acknowledged));
    }

    #[tokio::test]
    async fn provider_rehash_claim_during_result_lookup_can_confirm() {
        let jobs = ProviderRehashClaim::default();
        let task = Uuid::new_v4();
        let observation = jobs.observe(task, b"owned frame").unwrap();
        let verdict = provider_rehash_outcome(
            task,
            Err(Error::AmbiguousPublication(task)),
            || observation.confirmed(),
            |_| {
                jobs.claimed(task, b"owned frame");
                ready(Ok(None))
            },
        )
        .await;
        assert_eq!(verdict, Ok(ProviderRehashResult::Acknowledged));
    }
}
