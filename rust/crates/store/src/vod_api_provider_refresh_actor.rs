//! Isolated/unwired Source refresh mutation actor. Shares existing ReadWork and
//! OwnedConnection. No encompassing transaction and no background mutation replay.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::{
    vod_api_owned_read::{OwnedConnection, ReadError, ReadWork},
    vod_api_projection::truthy,
    vod_api_provider_refresh_http as http, vod_api_provider_refresh_movie as movie,
    vod_api_provider_refresh_sql as sql, vod_api_provider_refresh_value as values,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value};
#[derive(Clone, Copy)]
pub(super) struct Clock {
    pub fixed: Option<DateTime<Utc>>,
}
impl Clock {
    fn now(self) -> DateTime<Utc> {
        self.fixed.unwrap_or_else(Utc::now)
    }
    pub(super) fn stamp(self) -> String {
        self.now().to_rfc3339_opts(SecondsFormat::Micros, false)
    }
}
#[derive(Debug)]
enum Step {
    Source,
    Control(ReadError),
    Capacity,
    Unsupported(&'static str),
}
impl From<ReadError> for Step {
    fn from(error: ReadError) -> Self {
        match error {
            ReadError::Capacity => Self::Capacity,
            error @ (ReadError::Deadline | ReadError::Cancelled | ReadError::Shutdown) => {
                Self::Control(error)
            }
            ReadError::WorkerFailed => Self::Unsupported("Native refresh invariant failure"),
            ReadError::Source(crate::StoreError::Database(sqlx::Error::Database(ref error)))
                if error.code().is_some_and(|code| code == "57014")
                    && error.message().contains("statement timeout") =>
            {
                Self::Control(ReadError::Deadline)
            }
            ReadError::Unsupported(message) => Self::Unsupported(message),
            _ => Self::Source,
        }
    }
}
impl From<values::Fault> for Step {
    fn from(error: values::Fault) -> Self {
        match error {
            values::Fault::Type => Self::Source,
            values::Fault::Capacity => Self::Capacity,
        }
    }
}
fn id(value: &Value) -> Result<i64, Step> {
    value.as_i64().ok_or(Step::Source)
}
fn object_or_empty(value: &Value) -> Result<Map<String, Value>, Step> {
    if truthy(value) {
        value.as_object().cloned().ok_or(Step::Source)
    } else {
        Ok(Map::new())
    }
}
fn recent(relation: &Value, force: bool, now: DateTime<Utc>) -> Result<bool, Step> {
    let props = object_or_empty(&relation["custom_properties"])?;
    if force || !props.get("detailed_fetched").is_some_and(truthy) {
        return Ok(false);
    }
    let Some(last) = relation["last_advanced_refresh"].as_str() else {
        return Ok(false);
    };
    let last = DateTime::parse_from_rfc3339(last)
        .map_err(|_| Step::Source)?
        .with_timezone(&Utc);
    Ok(now
        .signed_duration_since(last)
        .num_microseconds()
        .is_some_and(|age| age < 86_400_000_000))
}
async fn save_with_source_retry(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    model: &mut Value,
    clock: Clock,
) -> Result<(), Step> {
    if let Err(error) = sql::save_movie(work, connection, model, &clock.stamp()).await {
        match error {
            ReadError::Capacity => return Err(Step::Capacity),
            error @ (ReadError::Deadline
            | ReadError::Cancelled
            | ReadError::Shutdown
            | ReadError::WorkerFailed) => return Err(error.into()),
            ReadError::Unsupported(message) => return Err(Step::Unsupported(message)),
            _ => {}
        }
        if let ReadError::Source(crate::StoreError::Database(sqlx::Error::Database(ref database))) =
            &error
        {
            if database.code().is_some_and(|code| code == "57014")
                && database.message().contains("statement timeout")
            {
                return Err(Step::Control(ReadError::Deadline));
            }
        }
        let text = format!("{error:?}");
        for key in ["tmdb_id", "imdb_id"] {
            if text.contains(key) && truthy(&model[key]) {
                model[key] = Value::Null;
            }
        }
        // Source retries save even when no ID was cleared; preserve that second
        // physical attempt, without replaying any provider HTTP request.
        sql::save_movie(work, connection, model, &clock.stamp()).await?;
    }
    Ok(())
}
async fn identity(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    phase: &mut movie::ParentPhase,
    clock: Clock,
) -> Result<(), Step> {
    let mut found: Vec<Option<Value>> = Vec::new();
    for (key, wanted) in [("tmdb_id", &phase.tmdb), ("imdb_id", &phase.imdb)] {
        found.push(if let Some(wanted) = wanted {
            Some(
                sql::movie_identity(
                    work,
                    connection,
                    key,
                    &values::text(wanted)?,
                    phase.movie["language"].as_str().ok_or(Step::Source)?,
                )
                .await?,
            )
            .flatten()
        } else {
            None
        });
    }
    let chosen_index = if found[0].is_some() {
        0
    } else if found[1].is_some() {
        1
    } else {
        return Ok(());
    };
    let clear_ids = found
        .iter()
        .map(|value| value.as_ref().map(|value| id(&value["id"])).transpose())
        .collect::<Result<Vec<_>, _>>()?;
    let mut donor = found[chosen_index].take().ok_or(Step::Source)?;
    let source_id = id(&donor["id"])?;
    let target_id = id(&phase.movie["id"])?;
    if source_id == target_id {
        return Ok(());
    }
    // The two Source lookups are separate instances, even when both IDs refer
    // to one row. Clear the chosen instance only for its own lookup's key.
    for (index, key) in ["tmdb_id", "imdb_id"].into_iter().enumerate() {
        if let Some(found) = clear_ids[index] {
            sql::clear_identity(connection, key, found).await?;
            if index == chosen_index {
                donor[key] = Value::Null;
            }
        }
    }
    drop(found);
    let mut changed = false;
    for key in ["description", "year", "rating", "genre", "duration_secs"] {
        if !truthy(&phase.movie[key]) && truthy(&donor[key]) {
            phase.movie[key] = donor[key].clone();
            changed = true;
        }
    }
    if phase.movie["logo_id"].is_null() && !donor["logo_id"].is_null() {
        phase.movie["logo_id"] = donor["logo_id"].clone();
        changed = true;
    }
    for (key, wanted) in [("tmdb_id", &phase.tmdb), ("imdb_id", &phase.imdb)] {
        if !truthy(&phase.movie[key]) {
            if let Some(wanted) = wanted.as_ref().filter(|v| truthy(v)) {
                phase.movie[key] = values::text(wanted)?.into();
                changed = true;
            } else if truthy(&donor[key]) {
                phase.movie[key] = donor[key].clone();
                changed = true;
            }
        }
    }
    let mut props = object_or_empty(&phase.movie["custom_properties"])?;
    for (key, value) in object_or_empty(&donor["custom_properties"])? {
        if truthy(&value) && !props.get(&key).is_some_and(truthy) {
            props.insert(key, value);
            changed = true;
        }
    }
    if changed {
        phase.movie["custom_properties"] = props.into();
        sql::save_movie(work, connection, &phase.movie, &clock.stamp()).await?;
    }
    sql::transfer_and_delete(connection, source_id, target_id).await?;
    Ok(())
}
async fn finish_phase<T>(connection: OwnedConnection, result: Result<T, Step>) -> Result<T, Step> {
    connection.finish().await?;
    result
}
async fn movie_steps(
    work: &ReadWork,
    auth: &crate::AuthService,
    relation_id: i64,
    force: bool,
    clock: Clock,
) -> Result<(), Step> {
    let mut connection = work.connection(auth.store.pool()).await?;
    let prepared = async {
        let snapshot = sql::movie_task(work, &mut connection, relation_id)
            .await?
            .ok_or(Step::Source)?;
        let attempt = clock.now();
        if recent(&snapshot["relation"], force, attempt)? {
            return Ok(None);
        }
        work.promote_provider_refresh()?;
        let agent = match snapshot["agent"].as_str().filter(|value| !value.is_empty()) {
            Some(value) => value.to_owned(),
            None => auth
                .clone()
                .with_retained_scope(work.io_guard()?.erased())
                .source_default_user_agent()
                .await
                .map_err(|_| Step::Source)?,
        };

        Ok(Some((snapshot, attempt, agent)))
    }
    .await;
    let Some((snapshot, attempt, agent)) = finish_phase(connection, prepared).await? else {
        return Ok(());
    };
    // No SQL/row/scoped Auth survives the transport's header loan boundary.
    let response = match http::fetch_admitted(
        work,
        &snapshot["account"],
        &agent,
        snapshot["relation"]["stream_id"]
            .as_str()
            .ok_or(Step::Source)?,
        false,
    )
    .await
    {
        Ok(value) => value,
        Err(http::Fault::Control(error)) => return Err(Step::Control(error)),
        Err(http::Fault::Remote | http::Fault::BodyDecode) => return Err(Step::Source),
        Err(http::Fault::Capacity) => return Err(Step::Capacity),
        Err(http::Fault::UncapturedText) => {
            return Err(Step::Unsupported(
                "Uncaptured provider response character encoding",
            ))
        }
        Err(http::Fault::UncapturedNumeric) => {
            return Err(Step::Unsupported(
                "Nonfinite provider JSON phases need Source authority",
            ))
        }
    };
    let Some(details) = movie::Details::from_owned_response(response) else {
        return Ok(());
    };
    let mut connection = work.connection(auth.store.pool()).await?;
    let result = async {
        let mut phase = movie::before_identity(&snapshot["movie"], &details)?;
        if phase.tmdb.is_some() || phase.imdb.is_some() {
            identity(work, &mut connection, &mut phase, clock).await?;
        }
        movie::after_identity(&mut phase, &details)?;
        if phase.updated {
            save_with_source_retry(work, &mut connection, &mut phase.movie, clock).await?;
        }
        drop(phase);
        let relation = movie::relation(
            &snapshot["relation"],
            &details,
            &attempt.to_rfc3339_opts(SecondsFormat::Micros, false),
        )?;
        drop(details);
        sql::save_movie_relation(work, &mut connection, &relation).await?;
        Ok(())
    }
    .await;
    finish_phase(connection, result).await
}
/// Source task catches its own provider/ORM/conversion failures. Caller MUST
/// re-read parent/relation afterwards even when this returns Ok after failure.
/// Native capacity/known-unported cases remain explicit, never cached fake200.
pub(super) async fn movie(
    work: &ReadWork,
    auth: &crate::AuthService,
    relation_id: i64,
    force: bool,
    clock: Clock,
) -> Result<(), ReadError> {
    work.check()?;
    match movie_steps(work, auth, relation_id, force, clock).await {
        Ok(()) | Err(Step::Source) => Ok(()),
        Err(Step::Capacity) => Err(ReadError::Capacity),
        Err(Step::Control(error)) => Err(error),
        Err(Step::Unsupported(message)) => Err(ReadError::Unsupported(message)),
    }
}
async fn series_steps(
    work: &ReadWork,
    auth: &crate::AuthService,
    snapshot: &Value,
    clock: Clock,
) -> Result<(), Step> {
    use super::{
        vod_api_provider_refresh_episode_sql as episode_sql,
        vod_api_provider_refresh_episodes as episodes,
        vod_api_provider_refresh_series_value as series_values,
    };
    work.promote_provider_refresh()?;
    let selected_agent = if snapshot.get("agent").is_some() {
        snapshot["agent"].clone()
    } else if let Some(id) = snapshot["account"]["user_agent_id"].as_i64() {
        let mut connection = work.connection(auth.store.pool()).await?;
        let result = sql::agent_by_id(work, &mut connection, id)
            .await
            .map_err(Step::from);
        finish_phase(connection, result)
            .await?
            .ok_or(Step::Source)?["user_agent"]
            .clone()
    } else {
        Value::Null
    };
    let agent = match selected_agent.as_str().filter(|v| !v.is_empty()) {
        Some(value) => value.to_owned(),
        None => auth
            .clone()
            .with_retained_scope(work.io_guard()?.erased())
            .source_default_user_agent()
            .await
            .map_err(|_| Step::Source)?,
    };
    let mut response = match http::fetch_admitted(
        work,
        &snapshot["account"],
        &agent,
        snapshot["relation"]["external_series_id"]
            .as_str()
            .ok_or(Step::Source)?,
        true,
    )
    .await
    {
        Ok(value) => value,
        Err(http::Fault::Control(error)) => return Err(Step::Control(error)),
        Err(http::Fault::Remote | http::Fault::BodyDecode) => return Err(Step::Source),
        Err(http::Fault::Capacity) => return Err(Step::Capacity),
        Err(http::Fault::UncapturedText) => {
            return Err(Step::Unsupported(
                "Uncaptured provider response character encoding",
            ))
        }
        Err(http::Fault::UncapturedNumeric) => {
            return Err(Step::Unsupported(
                "Nonfinite provider JSON phases need Source authority",
            ))
        }
    };
    let mut connection = work.connection(auth.store.pool()).await?;
    let result = async {
        let has_details = response.is_object();
        let mut series = snapshot["series"].clone();
        let series_id = id(&series["id"])?;
        let account_id = id(&snapshot["account"]["id"])?;
        if has_details {
            let info = movie::owned_details(response["info"].take());
            let mut updated = false;
            for (source_key, key) in [("plot", "description"), ("genre", "genre")] {
                if values::should_update(&series[key], &info[source_key])? {
                    series[key] = values::string(&info[source_key])?
                        .map(Value::String)
                        .unwrap_or(Value::Null);
                    updated = true;
                }
            }
            if let Some(rating) = values::rating(&info["rating"])? {
                if !truthy(&series["rating"])
                    || values::text(&series["rating"])?
                        .trim_matches(|c: char| {
                            c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}')
                        })
                        .is_empty()
                {
                    series["rating"] = rating.into();
                    updated = true;
                }
            }
            if !truthy(&series["year"]) {
                if let Some(year) = series_values::year(&info) {
                    series["year"] = year.into();
                    updated = true;
                }
            }
            // This full parent save commits before episode preparation/transaction.
            if updated {
                sql::save_series(work, &mut connection, &series, &clock.stamp()).await?;
            }
        }
        // The full-save instance is no longer needed; release it before episode
        // prefetch/maps so the same fixed work ledger covers those allocations.
        drop(series);
        let mut relation = sql::series_task_relation(
            work,
            &mut connection,
            account_id,
            snapshot["relation"]["external_series_id"]
                .as_str()
                .ok_or(Step::Source)?,
        )
        .await?;
        let empty = Value::Object(Map::new());
        let raw = if has_details && truthy(&response["episodes"]) {
            &response["episodes"]
        } else {
            &empty
        };
        let flat = episodes::flatten(work, raw)?;
        let count = flat.entries.len();
        if count > 0 {
            let old_episodes = episode_sql::episodes(work, &mut connection, series_id).await?;
            let old_relations =
                episode_sql::relations(work, &mut connection, account_id, &flat.ids).await?;
            let relation_id = relation.as_ref().map(|r| id(&r["id"])).transpose()?;
            let mut batch = episodes::prepare(
                work,
                flat,
                episodes::Context {
                    series_id,
                    account_id,
                    series_relation: relation_id,
                    clock,
                },
                old_episodes,
                old_relations,
            )?;
            episode_sql::apply(
                work,
                &mut connection,
                &mut batch,
                series_id,
                account_id,
                relation_id,
                clock,
            )
            .await?;
        }
        if let Some(relation) = relation.as_mut() {
            let mut props = object_or_empty(&relation["custom_properties"])?;
            props.insert("detailed_fetched".into(), has_details.into());
            let fetched = count > 0
                || sql::stored_series_episodes(&mut connection, id(&relation["id"])?).await?;
            props.insert("episodes_fetched".into(), fetched.into());
            relation["custom_properties"] = props.into();
            // Source stamps after the episode transaction, rather than HTTP start.
            relation["last_episode_refresh"] = clock.stamp().into();
            sql::save_series_relation(work, &mut connection, relation, &clock.stamp()).await?;
        }
        Ok(())
    }
    .await;
    finish_phase(connection, result).await
}
/// Original view's owned snapshot is passed through; task errors are swallowed
/// only for Source failures, never capacity, cancellation or known missing work.
pub(super) async fn series(
    work: &ReadWork,
    auth: &crate::AuthService,
    snapshot: &Value,
    clock: Clock,
) -> Result<(), ReadError> {
    work.check()?;
    match series_steps(work, auth, snapshot, clock).await {
        Ok(()) | Err(Step::Source) => Ok(()),
        Err(Step::Capacity) => Err(ReadError::Capacity),
        Err(Step::Control(error)) => Err(error),
        Err(Step::Unsupported(message)) => Err(ReadError::Unsupported(message)),
    }
}
