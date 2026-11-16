//! Initial nonempty VOD catalog workflow from Original apps/vod/tasks.py.
//! Original Dispatcharr/Django/Celery authors retain all algorithms and credit.
use crate::{xc_live_provider::Client, AuthService, StoreError};
use dispatcharr_contracts::UpdateEventSink;
use serde_json::{json, Value};
use std::sync::Arc;
type Owner = Arc<dyn Send + Sync>;
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
    if !sql::fresh(auth, owner.clone()).await? {
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
    Caught(String),
}
impl From<StoreError> for Step {
    fn from(error: StoreError) -> Self {
        Self::Control(error)
    }
}
fn draining(cancellation: &dyn Cancellation) -> Result<(), Step> {
    if cancellation.is_draining() {
        Err(Step::Control(unsupported()))
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
    if !sql::fresh(&auth, owner.clone()).await? {
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
        let agent = match &account.agent {
            Some(agent) if !agent.is_empty() => agent.clone(),
            _ => {
                let writer = auth.task_sql_writer()?;
                let scoped = auth
                    .clone()
                    .with_request_writer_permit(writer)?
                    .with_retained_scope(owner.clone());
                crate::core_settings_special_effects::default_user_agent(&scoped).await?
            }
        };
        let mut client = Client::new(&account.base, &account.username, &account.password, agent)?
            .ok_or_else(unsupported)?;
        draining(cancellation.as_ref())?;
        let movie_categories =
            values::categories(action(&mut client, "get_vod_categories").await?)?;
        let mut movie_map = sql::categories(
            &auth,
            owner.clone(),
            account.id,
            "movie",
            &movie_categories,
            account.movie_enabled,
        )
        .await?;
        draining(cancellation.as_ref())?;
        let series_categories =
            values::categories(action(&mut client, "get_series_categories").await?)?;
        let _series_map = sql::categories(
            &auth,
            owner.clone(),
            account.id,
            "series",
            &series_categories,
            account.series_enabled,
        )
        .await?;
        let uncategorized = sql::uncategorized(
            &auth,
            owner.clone(),
            account.id,
            "movie",
            account.movie_enabled,
        )
        .await?;
        movie_map.insert("__uncategorized__".into(), uncategorized);
        let enabled = sql::enabled(&auth, owner.clone(), account.id).await?;
        draining(cancellation.as_ref())?;
        let document = action(&mut client, "get_vod_streams").await?;
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
        sql::movies(&auth, owner.clone(), account.id, imported, scan).await?;
        let _uncategorized_series = sql::uncategorized(
            &auth,
            owner.clone(),
            account.id,
            "series",
            account.series_enabled,
        )
        .await?;
        draining(cancellation.as_ref())?;
        let series = action(&mut client, "get_series").await?;
        if !series.as_array().is_some_and(Vec::is_empty) {
            return Err(Step::Control(unsupported()));
        }
        // Source leaves its XtreamCodesClient context BEFORE reconcile/cleanup.
        drop(client);
        // None of the newly created identities is language tagged, so Source's
        // reconcile needs no split/merge/update. Verify this in actual SQL.
        sql::verify_untagged(&auth, owner.clone()).await?;
        let end = chrono::Utc::now();
        let micros = end
            .signed_duration_since(scan)
            .num_microseconds()
            .ok_or_else(unsupported)?;
        let seconds = micros as f64 / 1_000_000.0;
        let result = format!(
            "Batch VOD refresh completed for account {} in {seconds:.2} seconds",
            account.name
        );
        sql::cleanup(&auth, owner.clone(), account.id, scan).await?;
        update(
            events.as_ref(),
            &account,
            100,
            "success",
            Some(&format!("VOD refresh completed in {seconds:.2} seconds")),
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
        Err(Step::Control(error)) => Err(error),
    }
}

pub async fn api_target(auth: &AuthService, id: i64, owner: Owner) -> Result<String, StoreError> {
    sql::api_target(auth, id, owner).await
}
