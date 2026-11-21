//! Scheduled age-selected XC series refresh over the existing fair VOD actor.
//! Original Dispatcharr and dependency authors retain application credit.

use super::{
    vod_api_owned_read::{ReadError, ReadRuntime, ReadWork},
    vod_api_provider_refresh_actor::{self as refresh, Clock},
    vod_api_provider_refresh_sql as sql,
};
use crate::AuthService;
use chrono::{DateTime, TimeDelta, Utc};
use std::{future::Future, sync::Arc};

const MAX_BATCH: usize = 64;

#[derive(Clone)]
pub struct ScheduledVod {
    runtime: ReadRuntime,
    auth: AuthService,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScheduledVodError {
    Capacity,
    Unsupported,
    Unsettled,
}

impl From<ReadError> for ScheduledVodError {
    fn from(error: ReadError) -> Self {
        match error {
            ReadError::Capacity => Self::Capacity,
            ReadError::Unsupported(_) => Self::Unsupported,
            _ => Self::Unsettled,
        }
    }
}

impl ScheduledVod {
    pub(super) fn new(auth: AuthService, runtime: ReadRuntime) -> Self {
        Self { auth, runtime }
    }

    /// Reserve the same fair SQL/refresh work budget as an ordinary VOD reader.
    /// The caller's claim is invoked only after the business selection and
    /// physical refresh admission. It must return true only for a confirmed
    /// job to execute. A false return has no provider or mutation effects.
    pub async fn run_series<F, Fut>(
        &self,
        account: i64,
        owner: Arc<dyn Send + Sync>,
        claim: F,
    ) -> Result<Option<u8>, ScheduledVodError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        if account <= 0 {
            return Err(ScheduledVodError::Unsupported);
        }
        let auth = self.auth.clone();
        let result = self
            .runtime
            .run(std::mem::size_of::<i64>(), move |work| async move {
                let _owner = owner;
                let cutoff = Utc::now() - TimeDelta::days(1);
                let selection = eligible(&work, &auth, account, cutoff).await?;
                let count = u8::try_from(selection.len()).map_err(|_| ReadError::Capacity)?;
                let selection = if selection.is_empty() {
                    selection
                } else {
                    let mut document = work.snapshot_document();
                    serde_json::to_writer(&mut document, &selection)
                        .map_err(|_| ReadError::Capacity)?;
                    drop(selection);
                    let snapshot = work.freeze_snapshot(document.finish())?;
                    let snapshot = work.admit_refresh(snapshot).await?;
                    serde_json::from_slice(&work.consume_snapshot(snapshot)?)
                        .map_err(|_| ReadError::WorkerFailed)?
                };
                work.check()?;
                if !claim().await {
                    return Ok(Vec::new());
                }
                for relation in selection {
                    let mut connection = work.connection(auth.store.pool()).await?;
                    let snapshot = sql::series_view(&work, &mut connection, relation).await;
                    connection.finish().await?;
                    let snapshot = snapshot?.ok_or(ReadError::Unsupported(
                        "Scheduled series relation changed before refresh",
                    ))?;
                    if snapshot["relation"]["m3u_account_id"].as_i64() != Some(account)
                        || snapshot["account"]["id"].as_i64() != Some(account)
                        || snapshot["account"]["account_type"].as_str() != Some("XC")
                        || snapshot["account"]["is_active"].as_bool() != Some(true)
                    {
                        return Err(ReadError::Unsupported(
                            "Scheduled series account changed before refresh",
                        ));
                    }
                    // The actor folds Source-caught in-line errors as the
                    // existing Source helper does. Count the attempted relation.
                    refresh::series(&work, &auth, &snapshot, Clock { fixed: None }).await?;
                }
                Ok(vec![count])
            })
            .await?;
        match result.as_ref() {
            [] => Ok(None),
            [count] => Ok(Some(*count)),
            _ => Err(ScheduledVodError::Unsettled),
        }
    }
    /// Admit the same refresh budget and owned task before running the Movie
    /// actor. A confirmed decline or revoked claim goes through no actor.
    pub async fn run_movie<F, Fut>(
        &self,
        relation: i64,
        owner: Arc<dyn Send + Sync>,
        claim: F,
    ) -> Result<Option<&'static str>, ScheduledVodError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        if relation <= 0 {
            return Err(ScheduledVodError::Unsupported);
        }
        let auth = self.auth.clone();
        let result = self
            .runtime
            .run(std::mem::size_of::<i64>(), move |work| async move {
                let _owner = owner;
                // Reserve fair refresh capacity before claiming logical ownership.
                let input = work.freeze_snapshot(relation.to_be_bytes().to_vec())?;
                let admitted = work.admit_refresh(input).await?;
                let operand: [u8; 8] = work
                    .consume_snapshot(admitted)?
                    .try_into()
                    .map_err(|_| ReadError::WorkerFailed)?;
                work.check()?;
                if !claim().await {
                    return Ok(Vec::new());
                }
                let result = refresh::movie_result(
                    &work,
                    &auth,
                    i64::from_be_bytes(operand),
                    false,
                    Clock { fixed: None },
                )
                .await?
                .ok_or(ReadError::Unsupported(
                    "Unqualified Stock Movie error result",
                ))?;
                Ok(result.celery_result().as_bytes().to_vec())
            })
            .await?;
        match result.as_ref() {
            b"" => Ok(None),
            b"Advanced data recently fetched, skipping." => {
                Ok(Some(refresh::MovieResult::Recent.celery_result()))
            }
            b"Advanced data refreshed." => {
                Ok(Some(refresh::MovieResult::Refreshed.celery_result()))
            }
            _ => Err(ScheduledVodError::Unsettled),
        }
    }
}

struct AgeRow {
    id: i64,
    account: i64,
    last_episode_refresh: Option<DateTime<Utc>>,
}
impl AgeRow {
    fn due_for(&self, account: i64, cutoff: DateTime<Utc>) -> bool {
        self.id > 0
            && self.account == account
            && self.last_episode_refresh.is_some_and(|last| last < cutoff)
    }
}

async fn eligible(
    work: &ReadWork,
    auth: &AuthService,
    account: i64,
    cutoff: DateTime<Utc>,
) -> Result<Vec<i64>, ReadError> {
    let mut connection = work.connection(auth.store.pool()).await?;
    let rows = async {
        let active: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM m3u_m3uaccount WHERE id=$1 AND is_active AND account_type='XC')",
        )
        .bind(account)
        .persistent(false)
        .fetch_one(connection.raw())
        .await?;
        if !active {
            return Err(ReadError::Unsupported("Scheduled series account is outside active XC scope"));
        }
        let found = sqlx::query_as::<_, (i64, i64, Option<DateTime<Utc>>)>(
            "SELECT id::bigint AS id, m3u_account_id::bigint AS account, last_episode_refresh
             FROM vod_m3useriesrelation
             WHERE m3u_account_id=$1 AND last_episode_refresh < $2
             ORDER BY id LIMIT $3",
        )
        .bind(account)
        .bind(cutoff)
        .bind((MAX_BATCH + 1) as i64)
        .persistent(false)
        .fetch_all(connection.raw())
        .await?;
        if found.len() > MAX_BATCH {
            return Err(ReadError::Unsupported("Scheduled series batch exceeds its owned bound"));
        }
        found
            .into_iter()
            .map(|(id, account_id, last_episode_refresh)| {
                let row = AgeRow { id, account: account_id, last_episode_refresh };
                if row.due_for(account, cutoff) {
                    Ok(row.id)
                } else {
                    Err(ReadError::WorkerFailed)
                }
            })
            .collect()
    }
    .await;
    connection.finish().await?;
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn age_selection_is_strict_fenced_and_excludes_never_opened() {
        let cutoff = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let older = cutoff - TimeDelta::seconds(1);
        let rows = [
            (10, Some(older), true),
            (10, Some(cutoff), false),
            (10, Some(cutoff + TimeDelta::seconds(1)), false),
            (10, None, false),
            (11, Some(older), false),
        ];
        for (account, last_episode_refresh, expected) in rows {
            let row = AgeRow {
                id: 1,
                account,
                last_episode_refresh,
            };
            assert_eq!(row.due_for(10, cutoff), expected);
        }
    }
}
