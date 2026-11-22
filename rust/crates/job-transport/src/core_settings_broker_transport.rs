//! Actual bounded publication through Main's already connected broker transport.
use super::*;
use crate::decode::{Metadata, State};
use dispatcharr_store::ProviderRehashResult;
use std::future::Future;

async fn provider_rehash_outcome<F>(
    task_id: Uuid,
    publication: Result<i64, Error>,
    lookup: impl FnOnce(Uuid) -> F,
) -> Result<ProviderRehashResult, Error>
where
    F: Future<Output = Result<Option<Metadata>, Error>>,
{
    match publication {
        Ok(n) if n > 0 => Ok(ProviderRehashResult::Acknowledged),
        Ok(-4) => Ok(ProviderRehashResult::Refused),
        Ok(-2) => Err(Error::Capacity),
        Ok(_) => Err(Error::Corrupt),
        Err(error @ Error::AmbiguousPublication(id)) if id == task_id => {
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
            }) {
                Ok(ProviderRehashResult::Acknowledged)
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

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
    ) -> Result<ProviderRehashResult, Error> {
        let published = self
            .publish_core_frame(
                publication,
                owner.clone(),
                task_id,
                PUBLISH_PROVIDER_REHASH,
                false,
            )
            .await;
        provider_rehash_outcome(task_id, published, |id| self.read(id, owner)).await
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
            let actual = provider_rehash_outcome(id, Err(Error::AmbiguousPublication(id)), |key| {
                assert_eq!(key, id);
                lookups += 1;
                ready(Ok(Some(metadata(id, state))))
            })
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
            let actual = provider_rehash_outcome(id, Err(Error::AmbiguousPublication(id)), |key| {
                assert_eq!(key, id);
                lookups += 1;
                ready(result)
            })
            .await;
            assert_eq!(lookups, 1);
            assert_eq!(actual, Err(Error::AmbiguousPublication(id)));
        }
    }
}
