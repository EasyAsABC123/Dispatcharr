//! Bounded plain Movie catalog initial/repeat workflow from Original apps/vod/tasks.py.
//! Original Dispatcharr/Django/Celery authors retain all algorithms and credit.
use crate::{xc_live_provider::Client, AuthService, StoreError};
use dispatcharr_contracts::UpdateEventSink;
use serde_json::{json, Value};
use std::sync::Arc;
type Owner = Arc<dyn Send + Sync>;
#[path = "vod_ingestion_language_sql.rs"]
mod language_sql;
#[path = "vod_ingestion_series_sql.rs"]
mod series_sql;
#[path = "vod_ingestion_series_values.rs"]
mod series_values;
#[path = "vod_ingestion_sql.rs"]
mod sql;
#[path = "vod_ingestion_values.rs"]
mod values;
fn unsupported() -> StoreError {
    StoreError::Unsupported("Initial VOD list-ingestion capability unavailable")
}
// Credential-bearing snapshots intentionally have no Debug implementation.
#[derive(PartialEq)]
struct Account {
    id: i64,
    name: String,
    base: String,
    username: String,
    password: String,
    agent: Option<String>,
    message: Option<String>,
    movie_enabled: bool,
    series_enabled: bool,
}
pub struct Prepared {
    account: Account,
}
pub trait Cancellation: Send + Sync {
    fn is_draining(&self) -> bool;
}
pub enum AdmissionError {
    Unsupported,
    Operational(StoreError),
}
impl From<StoreError> for AdmissionError {
    fn from(error: StoreError) -> Self {
        Self::Operational(error)
    }
}
pub async fn preflight(
    auth: &AuthService,
    id: i64,
    owner: Owner,
) -> Result<Prepared, AdmissionError> {
    crate::m3u_refresh_all::require_supported_domain_checked(auth, owner.clone())
        .await
        .map_err(|error| match error {
            crate::m3u_refresh_all::AdmissionError::Unsupported => AdmissionError::Unsupported,
            crate::m3u_refresh_all::AdmissionError::Operational(error) => {
                AdmissionError::Operational(error)
            }
            crate::m3u_refresh_all::AdmissionError::Draining => {
                AdmissionError::Operational(unsupported())
            }
        })?;
    if !sql::supported_catalog(auth, owner.clone()).await? {
        return Err(AdmissionError::Unsupported);
    }
    Ok(Prepared {
        account: sql::account(auth, id, owner)
            .await?
            .ok_or(AdmissionError::Unsupported)?,
    })
}

enum Step {
    Control(StoreError),
    // Rejected without an outstanding SQL/cache effect. A previous movie
    // category write may already have committed before the series rejection.
    Settled(StoreError),
    Caught(String),
}
impl From<StoreError> for Step {
    fn from(error: StoreError) -> Self {
        Self::Control(error)
    }
}
fn draining(cancellation: &dyn Cancellation) -> Result<(), Step> {
    if cancellation.is_draining() {
        Err(Step::Settled(unsupported()))
    } else {
        Ok(())
    }
}
async fn update(
    events: &dyn UpdateEventSink,
    account: &Account,
    progress: i64,
    status: &str,
    message: Option<&str>,
    owner: Owner,
) -> Result<(), StoreError> {
    let mut data = json!({"progress":progress,"type":"m3u_refresh","account":account.id,"action":"vod_refresh","status":status});
    if let Some(message) = message
        .or(account.message.as_deref())
        .filter(|message| !message.is_empty())
    {
        data["message"] = json!(message);
    }
    events
        .send_retained("updates", json!({"type":"update","data":data}), owner)
        .await
        .map_err(|_| unsupported())
}
async fn action(client: &mut Client, action: &str) -> Result<Value, Step> {
    client.vod_action(action).await?.map_err(Step::Caught)
}
/// Source fetches each complete list once, then processes raw chunks of 1000.
/// Take owned rows without cloning the full provider document; filter disabled
/// categories only inside each chunk so its atomic failure boundary stays exact.
fn source_batches(document: Value) -> Result<impl Iterator<Item = Value>, StoreError> {
    let Value::Array(rows) = document else {
        return Err(unsupported());
    };
    let mut rows = rows.into_iter();
    Ok(std::iter::from_fn(move || {
        let batch: Vec<_> = rows.by_ref().take(1000).collect();
        (!batch.is_empty()).then_some(Value::Array(batch))
    }))
}
/// Provider-ID maps returned by Original refresh_categories. No Uncategorized
/// row or content rows are added by this phase.
struct CategoryMaps {
    movies: std::collections::HashMap<String, i64>,
    series: std::collections::HashMap<String, i64>,
}

async fn category_maps(
    auth: &AuthService,
    account: &Account,
    client: &mut Client,
    cancellation: &dyn Cancellation,
    owner: Owner,
) -> Result<Option<CategoryMaps>, Step> {
    draining(cancellation)?;
    let movie_categories =
        values::categories(action(client, "get_vod_categories").await?).map_err(Step::Settled)?;
    if movie_categories.is_empty()
        && sql::has_selected_categories(auth, owner.clone(), account.id, "movie").await?
    {
        return Ok(None);
    }
    let movie_map = sql::categories(
        auth,
        owner.clone(),
        account.id,
        "movie",
        &movie_categories,
        account.movie_enabled,
    )
    .await?;
    draining(cancellation)?;
    let series_categories = values::categories(action(client, "get_series_categories").await?)
        .map_err(Step::Settled)?;
    if series_categories.is_empty()
        && sql::has_selected_categories(auth, owner.clone(), account.id, "series").await?
    {
        return Ok(None);
    }
    let series_map = sql::categories(
        auth,
        owner.clone(),
        account.id,
        "series",
        &series_categories,
        account.series_enabled,
    )
    .await?;
    Ok(Some(CategoryMaps {
        movies: movie_map,
        series: series_map,
    }))
}

/// Physical settlement is separate from the endpoint's business outcome.
pub enum CreateCategoriesError {
    Settled(StoreError),
    Unsettled(StoreError),
}

/// Direct Source create-view helper, after Groups has released its lock and
/// closed its SQL actor. Do not emit full-ingestion task notifications here.
pub async fn create_categories(
    auth: &AuthService,
    id: i64,
    cancellation: &dyn Cancellation,
    owner: Owner,
) -> Result<(), CreateCategoriesError> {
    use CreateCategoriesError::{Settled, Unsettled};

    let prepared = preflight(auth, id, owner.clone())
        .await
        .map_err(|error| match error {
            AdmissionError::Unsupported => Settled(unsupported()),
            AdmissionError::Operational(error) => Unsettled(error),
        })?;
    if cancellation.is_draining() {
        return Err(Settled(unsupported()));
    }
    let mut client = category_client(auth, &prepared.account, owner.clone())
        .await
        .map_err(Unsettled)?;
    match category_maps(auth, &prepared.account, &mut client, cancellation, owner).await {
        Ok(_) => Ok(()),
        Err(Step::Settled(error)) => Err(Settled(error)),
        Err(Step::Caught(_)) => Err(Settled(StoreError::UnhandledRestFault)),
        Err(Step::Control(error)) => Err(Unsettled(error)),
    }
}

async fn category_client(
    auth: &AuthService,
    account: &Account,
    owner: Owner,
) -> Result<Client, StoreError> {
    let agent = match &account.agent {
        Some(agent) if !agent.is_empty() => agent.clone(),
        _ => {
            let writer = auth.task_sql_writer()?;
            let scoped = auth
                .clone()
                .with_request_writer_permit(writer)?
                .with_retained_scope(owner);
            crate::core_settings_special_effects::default_user_agent(&scoped).await?
        }
    };
    Client::new(&account.base, &account.username, &account.password, agent)?.ok_or_else(unsupported)
}

/// Only actual claimed tasks call execute. Source's direct decorated helpers
/// remain ordinary in-process phases: no fake Celery children are synthesized.
pub async fn execute(
    auth: AuthService,
    events: Arc<dyn UpdateEventSink>,
    prepared: Prepared,
    cancellation: Arc<dyn Cancellation>,
    owner: Owner,
) -> Result<Value, StoreError> {
    // Revalidate before first application effect, without freezing API-time rows.
    let account = sql::account(&auth, prepared.account.id, owner.clone())
        .await?
        .ok_or_else(unsupported)?;
    if account != prepared.account {
        return Err(unsupported());
    }
    if !sql::supported_catalog(&auth, owner.clone()).await? {
        return Err(unsupported());
    }
    crate::m3u_refresh_all::require_supported_domain(&auth, owner.clone()).await?;
    let scan = chrono::Utc::now();
    if cancellation.is_draining() {
        return Err(unsupported());
    }
    update(
        events.as_ref(),
        &account,
        0,
        "processing",
        None,
        owner.clone(),
    )
    .await?;
    let outcome = async {
        // Source resolves cached default UA after its start notification. No SQL
        // session/writer remains borrowed across provider network/CPU phases.
        let mut client = category_client(&auth, &account, owner.clone()).await?;
        let Some(CategoryMaps { movies: mut movie_map, series: mut series_map }) =
            category_maps(&auth, &account, &mut client, cancellation.as_ref(), owner.clone()).await?
        else {
            return Err(Step::Caught(format!("Provider returned no VOD categories for account {}; aborting VOD refresh to preserve existing category selections", account.name)));
        };
        // Source retains one relation-property snapshot across both list kinds,
        // updating only each freshly fetched Uncategorized relation.
        let mut languages = sql::category_languages(&auth, owner.clone(), account.id).await?;
        let (uncategorized, language, uncategorized_enabled) = sql::uncategorized(
            &auth,
            owner.clone(),
            account.id,
            "movie",
            account.movie_enabled,
        )
        .await?;
        languages.insert(uncategorized, language);
        movie_map.insert("__uncategorized__".into(), uncategorized);
        let mut enabled = sql::enabled(&auth, owner.clone(), account.id).await?;
        enabled.insert(uncategorized,uncategorized_enabled);
        draining(cancellation.as_ref())?;
        let document = action(&mut client, "get_vod_streams").await?;
        for document in source_batches(document)? {
            draining(cancellation.as_ref())?;
            let retained = owner.clone();
            let draining_cpu = cancellation.clone();
            let rows = tokio::task::spawn_blocking(move || {
                let _owner = retained;
                values::movies(document, &|| draining_cpu.is_draining())
            })
            .await
            .map_err(|_| unsupported())??;
            let mut imported = Vec::new();
            for movie in rows {
                let category = movie
                    .provider
                    .as_ref()
                    .and_then(|provider| movie_map.get(provider))
                    .copied()
                    .unwrap_or(uncategorized);
                if enabled.get(&category).copied().unwrap_or(true) {
                    imported.push((movie, category));
                }
            }
            sql::movies(&auth, owner.clone(), account.id, imported, scan, &languages).await?;
        }
        let (uncategorized_series, language, uncategorized_series_enabled) = sql::uncategorized(
            &auth,
            owner.clone(),
            account.id,
            "series",
            account.series_enabled,
        )
        .await?;
        draining(cancellation.as_ref())?;
        languages.insert(uncategorized_series, language);
        // Source overwrites the shared relation snapshot with actual get/create
        // Uncategorized state for this kind; defaultfalse is not missing=>true.
        enabled.insert(uncategorized_series,uncategorized_series_enabled);
        series_map.insert("__uncategorized__".into(), uncategorized_series);
        // Source refresh_series calls the decorated batch helper inline. It
        // creates no Celery child and does not fetch episodes/details here.
        let document = action(&mut client, "get_series").await?;
        for document in source_batches(document)? {
            draining(cancellation.as_ref())?;
            let retained = owner.clone();
            let draining_cpu = cancellation.clone();
            let rows = tokio::task::spawn_blocking(move || {
                let _owner = retained;
                series_values::series(document, &|| draining_cpu.is_draining())
            }).await.map_err(|_| unsupported())??;
            let mut imported = Vec::new();
            for series in rows {
                let category = series.provider.as_ref()
                    .and_then(|provider| series_map.get(provider)).copied()
                    .unwrap_or(uncategorized_series);
                if enabled.get(&category).copied().unwrap_or(true) {
                    imported.push((series, category));
                }
            }
            series_sql::apply(&auth, owner.clone(), account.id, imported, scan, &languages).await?;
        }
        // Source leaves its XtreamCodesClient context BEFORE reconcile/cleanup.
        drop(client);
        // Source reconciles once after both lists and before duration/cleanup.
        let movie_language = language_sql::reconcile_movies(&auth, owner.clone(), account.id, scan).await?;
        let series_language = language_sql::reconcile_series(&auth, owner.clone(), account.id, scan).await?;
        let skip_note = language_sql::language_skip_note(movie_language.failed, series_language.failed);
        let end = chrono::Utc::now();
        let micros = end
            .signed_duration_since(scan)
            .num_microseconds()
            .ok_or_else(unsupported)?;
        let seconds = micros as f64 / 1_000_000.0;
        let result = format!(
            "Batch VOD refresh completed for account {} in {seconds:.2} seconds{skip_note}",
            account.name
        );
        sql::cleanup(&auth, owner.clone(), account.id, scan).await?;
        update(
            events.as_ref(),
            &account,
            100,
            "success",
            Some(&format!("VOD refresh completed in {seconds:.2} seconds{skip_note}")),
            owner.clone(),
        )
        .await?;
        Ok::<_, Step>(Value::String(result))
    }
    .await;
    match outcome {
        Ok(value) => Ok(value),
        Err(Step::Caught(message)) => {
            let message = format!("VOD refresh failed: {message}");
            update(
                events.as_ref(),
                &account,
                100,
                "error",
                Some(&message),
                owner,
            )
            .await?;
            Ok(Value::String(message))
        }
        Err(Step::Control(error) | Step::Settled(error)) => Err(error),
    }
}

pub async fn api_target(auth: &AuthService, id: i64, owner: Owner) -> Result<String, StoreError> {
    sql::api_target(auth, id, owner).await
}

#[cfg(test)]
pub(crate) fn schema_statements() -> Vec<(&'static str, String)> {
    let mut rows = series_sql::schema_statements();
    rows.extend(sql::schema_statements());
    rows.extend(language_sql::schema_statements());
    rows
}

#[cfg(test)]
pub(crate) fn language_schema_parameters(name: &str) -> Option<Vec<sqlx::postgres::PgTypeInfo>> {
    language_sql::schema_parameters(name)
}
#[cfg(test)]
pub(crate) fn language_schema_columns(name: &str) -> Option<Vec<(&'static str, &'static str)>> {
    language_sql::schema_columns(name)
}
