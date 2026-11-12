//! Shared-runtime authenticated provider/action reads with physical ownership.
//! Actual Source/native component gates passed; app/browser acceptance is
//! separate. Original Dispatcharr authors retain application/algorithm credit.
use super::{
    vod_api_actions_account as account, vod_api_actions_content as content,
    vod_api_actions_dto::{self, RelationKind},
    vod_api_actions_sql as sql,
    vod_api_owned_read::{OwnedConnection, ReadError, ReadRuntime, ReadWork},
    vod_api_projection::visibility,
    vod_api_query::{self, QueryError},
    vod_api_read_component::HttpRead,
    vod_api_sql::Resource,
};
use axum::http::{HeaderMap, Method};
use bytes::Bytes;
use serde_json::{json, Value};
use sqlx::Row;
use std::{collections::HashMap, io::Write, net::SocketAddr, sync::Arc};
#[derive(Clone, Copy)]
pub(super) enum Action {
    MovieProviders,
    SeriesProviders,
    SeriesEpisodes,
    MovieInfo,
    SeriesInfo,
}
impl Action {
    fn resource(self) -> Resource {
        if matches!(self, Self::MovieProviders | Self::MovieInfo) {
            Resource::Movie
        } else {
            Resource::Series
        }
    }
    fn relation(self) -> RelationKind {
        match self {
            Self::MovieProviders | Self::MovieInfo => RelationKind::Movie,
            Self::SeriesProviders | Self::SeriesInfo => RelationKind::Series,
            Self::SeriesEpisodes => RelationKind::Episode,
        }
    }
    fn options(self) -> Value {
        let (name, description) = match self {
            Self::MovieInfo => (
                "Provider info",
                "Get detailed movie information from the original provider, throttled to 24h.",
            ),
            Self::SeriesInfo => (
                "Series info",
                "Get detailed series information, refreshing from provider if needed",
            ),
            Self::MovieProviders => (
                "Get providers",
                "Get all providers (M3U accounts) that have this movie",
            ),
            Self::SeriesProviders => (
                "Get providers",
                "Get all providers (M3U accounts) that have this series",
            ),
            Self::SeriesEpisodes => (
                "Get episodes",
                "Get episodes for this series with provider information",
            ),
        };
        json!({"name":name,"description":description,"renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]})
    }
}
#[derive(Clone)]
pub(super) struct ActionReads {
    auth: crate::AuthService,
    runtime: ReadRuntime,
}
fn missing(resource: Resource) -> ReadError {
    crate::StoreError::StatusBody(404,json!({"detail":format!("No {} matches the given query.",if resource==Resource::Movie{"Movie"}else{"Series"})})).into()
}
fn output(_: impl std::fmt::Display) -> ReadError {
    ReadError::Capacity
}
fn query_error(error: QueryError) -> ReadError {
    match error {
        QueryError::Source400(body) => crate::StoreError::StatusBody(400, body).into(),
        QueryError::Capacity => ReadError::Capacity,
        QueryError::Unsupported(message) => ReadError::Unsupported(message),
    }
}
impl ActionReads {
    /// Constructor takes the existing ordinary reader's runtime. It does not
    /// create a new budget, connection pool, queue or shutdown service.
    pub(super) fn shared(auth: crate::AuthService, runtime: ReadRuntime) -> Self {
        Self { auth, runtime }
    }
    pub(super) async fn dispatch(
        &self,
        action: Action,
        peer: SocketAddr,
        headers: HeaderMap,
        http: HttpRead,
    ) -> Result<Bytes, ReadError> {
        let input = headers.iter().try_fold(
            http.uri
                .to_string()
                .len()
                .checked_add(1024)
                .ok_or(ReadError::Capacity)?,
            |total, (key, value)| {
                total
                    .checked_add(key.as_str().len())
                    .and_then(|n| n.checked_add(value.as_bytes().len()))
                    .ok_or(ReadError::Capacity)
            },
        )?;
        let state = self.clone();
        let deadline = http.deadline;
        self.runtime
            .run_scoped(
                input,
                Some(deadline),
                http.scope.clone(),
                move |work| async move {
                    work.check()?;
                    super::vod_api_http::negotiate(&headers, &http.uri)?;
                    let indent = super::vod_api_http::json_indent(&headers);
                    let guard = work.io_guard()?.erased();
                    let guard: Arc<dyn Send + Sync> = if let Some(scope) = http.scope.clone() {
                        Arc::new((guard, scope))
                    } else {
                        guard
                    };
                    let origin = crate::logos::origin(&state.auth, peer.ip(), &headers);
                    let user = state
                        .auth
                        .clone()
                        .with_retained_scope(guard)
                        .account_user(peer, headers, false)
                        .await?;
                    work.check()?;
                    if !matches!(http.method, Method::GET | Method::HEAD) {
                        if user.user_level < 10 {
                            return Err(crate::StoreError::PermissionDenied.into());
                        }
                        if http.method == Method::OPTIONS {
                            let mut document = work.document();
                            serde_json::to_writer(&mut document, &action.options())
                                .map_err(output)?;
                            return Ok(document.finish());
                        }
                        return Err(crate::StoreError::StatusBody(
                            405,
                            json!({"detail":format!("Method \"{}\" not allowed.",http.method)}),
                        )
                        .into());
                    }
                    // Authenticated actions deliberately have no Standard-level check.
                    let visible =
                        visibility(&user.custom_properties, user.user_level).map_err(|_| {
                            ReadError::SourceUnhandledFault("VOD user properties failed")
                        })?;
                    let query: Vec<(String, String)> =
                        url::form_urlencoded::parse(http.uri.query().unwrap_or("").as_bytes())
                            .map(|(k, v)| (k.into_owned(), v.into_owned()))
                            .collect();
                    let raw = http.raw_detail.as_deref().ok_or(ReadError::WorkerFailed)?;
                    let parsed = dispatcharr_ingest_core::source_integer_literal(raw);
                    let id = parsed
                        .as_ref()
                        .and_then(|number| number.parse::<i64>().ok());
                    let selection =
                        vod_api_query::ordinary(action.resource(), Some(id.unwrap_or(0)), &query)
                            .map_err(query_error)?;
                    let Some(id) = id else {
                        return Err(if parsed.is_none() {
                            crate::StoreError::NotFound.into()
                        } else {
                            missing(action.resource())
                        });
                    };
                    if !action.resource().enabled(&visible) {
                        return Err(missing(action.resource()));
                    }
                    let mut connection = work.connection(state.auth.store.pool()).await?;
                    if matches!(action, Action::MovieInfo | Action::SeriesInfo) {
                        let prepared = async {
                            let statement = selection.rows(&visible, work.row_bytes(), None)?;
                            connection.configure(&work).await?;
                            let rows = statement.query().fetch_all(connection.raw()).await?;
                            if rows.is_empty() {
                                return Err(missing(action.resource()));
                            }
                            if rows.len() != 1 {
                                return Err(ReadError::WorkerFailed);
                            }
                            if rows[0].try_get::<bool, _>("oversized")? {
                                return Err(ReadError::Capacity);
                            }
                            let result = super::vod_api_provider_info_read::prepare(
                                &work,
                                &mut connection,
                                matches!(action, Action::SeriesInfo),
                                id,
                                &rows[0],
                                &query,
                                &origin,
                            )
                            .await;
                            drop(rows);
                            result
                        }
                        .await;
                        // Sealing the descriptor already promoted its deadline.
                        // Finish/drop row/connection/identity state before lending.
                        let cleanup = connection.finish().await;
                        drop(user);
                        drop(selection);
                        drop(query);
                        drop(origin);
                        let prepared = prepared?;
                        cleanup?;
                        let value = match prepared {
                            super::vod_api_provider_info_read::Preparation::Ready(value) => value,
                            super::vod_api_provider_info_read::Preparation::Refresh(snapshot) => {
                                let snapshot = work.admit_refresh(snapshot).await?;
                                super::vod_api_provider_info_read::execute(
                                    &work,
                                    &state.auth,
                                    snapshot,
                                )
                                .await?
                            }
                        };
                        return provider_body(&work, value, indent);
                    }
                    let result = async {
                        let statement = selection.rows(&visible, work.row_bytes(), None)?;
                        connection.configure(&work).await?;
                        let rows = statement.query().fetch_all(connection.raw()).await?;
                        if rows.is_empty() {
                            return Err(missing(action.resource()));
                        }
                        if rows.len() != 1 {
                            return Err(ReadError::WorkerFailed);
                        }
                        if rows[0].try_get::<bool, _>("oversized")? {
                            return Err(ReadError::Capacity);
                        }
                        drop(rows);
                        let mut document = work.document();
                        let mut context = account::SerializerContext::default();
                        document.write_all(b"[").map_err(output)?;
                        if matches!(action, Action::SeriesEpisodes) {
                            let episodes =
                                sql::episode_snapshots(&work, &mut connection, id).await?;
                            let mut retained = Vec::new();
                            let mut ids = Vec::new();
                            let mut total = 0usize;
                            for text in episodes.into_rows() {
                                let value = sql::decode(&work, &text)?;
                                let episode =
                                    value["id"].as_i64().ok_or(ReadError::WorkerFailed)?;
                                total = total.checked_add(256).ok_or(ReadError::Capacity)?;
                                if total > work.work_bytes() / 64 {
                                    return Err(ReadError::Capacity);
                                }
                                retained.push(text);
                                ids.push(episode);
                            }
                            let providers =
                                sql::prefetched_episodes(&work, &mut connection, &ids).await?;
                            drop(ids);
                            let mut grouped: HashMap<i64, Vec<String>> = HashMap::new();
                            for text in providers.into_rows() {
                                let value = sql::decode(&work, &text)?;
                                let episode = value["relation"]["episode_id"]
                                    .as_i64()
                                    .ok_or(ReadError::WorkerFailed)?;
                                grouped.entry(episode).or_default().push(text);
                            }
                            for (index, text) in retained.into_iter().enumerate() {
                                work.check()?;
                                let raw = sql::decode(&work, &text)?;
                                let episode = raw["id"].as_i64().ok_or(ReadError::WorkerFailed)?;
                                let cached =
                                    content::episode_context(&work, &mut connection, &raw).await?;
                                let body = content::project_cached_episode(
                                    &work,
                                    &mut connection,
                                    &raw,
                                    &cached,
                                )
                                .await?;
                                let mut nested = Vec::new();
                                let mut heap = 0usize;
                                for text in grouped.remove(&episode).unwrap_or_default() {
                                    let value = relation(
                                        &work,
                                        &mut connection,
                                        &mut context,
                                        RelationKind::Episode,
                                        &text,
                                        Some((&raw, &cached)),
                                    )
                                    .await?;
                                    heap = heap
                                        .checked_add(account::heap(&value)?)
                                        .and_then(|n| n.checked_add(256))
                                        .ok_or(ReadError::Capacity)?;
                                    if heap > work.work_bytes() / 32 {
                                        return Err(ReadError::Capacity);
                                    }
                                    nested.push(value);
                                }
                                let value = vod_api_actions_dto::episode_with_providers(
                                    body,
                                    nested.into(),
                                );
                                if index > 0 {
                                    document.write_all(b",").map_err(output)?;
                                }
                                serde_json::to_writer(&mut document, &value).map_err(output)?;
                            }
                        } else {
                            let rows = sql::relation_snapshots(
                                &work,
                                &mut connection,
                                action.relation(),
                                id,
                            )
                            .await?;
                            for (index, text) in rows.into_rows().enumerate() {
                                work.check()?;
                                let value = relation(
                                    &work,
                                    &mut connection,
                                    &mut context,
                                    action.relation(),
                                    &text,
                                    None,
                                )
                                .await?;
                                if index > 0 {
                                    document.write_all(b",").map_err(output)?;
                                }
                                serde_json::to_writer(&mut document, &value).map_err(output)?;
                            }
                        }
                        document.write_all(b"]").map_err(output)?;
                        Ok(document.finish())
                    }
                    .await;
                    let cleanup = connection.finish().await;
                    match result {
                        Err(error) => Err(error),
                        Ok(body) => {
                            cleanup?;
                            match indent {
                                None => Ok(body),
                                Some(indent) => super::vod_api_http::pretty_document(
                                    &body,
                                    indent,
                                    super::vod_api_owned_read::Document::bounded(
                                        work.work_bytes().min(work.response_bytes()),
                                    ),
                                ),
                            }
                        }
                    }
                },
            )
            .await
    }
}
fn provider_body(
    work: &ReadWork,
    value: Value,
    indent: Option<usize>,
) -> Result<Vec<u8>, ReadError> {
    let mut document = work.document();
    serde_json::to_writer(&mut document, &value).map_err(output)?;
    drop(value);
    let body = document.finish();
    match indent {
        None => Ok(body),
        Some(indent) => super::vod_api_http::pretty_document(
            &body,
            indent,
            super::vod_api_owned_read::Document::bounded(
                work.work_bytes().min(work.response_bytes()),
            ),
        ),
    }
}

async fn relation(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    context: &mut account::SerializerContext,
    kind: RelationKind,
    text: &str,
    cached: Option<(&Value, &content::EpisodeContext)>,
) -> Result<Value, ReadError> {
    let snapshot = sql::decode(work, text)?;
    let resource = match kind {
        RelationKind::Movie => Resource::Movie,
        RelationKind::Series => Resource::Series,
        RelationKind::Episode => Resource::Episode,
    };
    let key = match kind {
        RelationKind::Movie => "movie_id",
        RelationKind::Series => "series_id",
        RelationKind::Episode => "episode_id",
    };
    let id = snapshot["relation"][key]
        .as_i64()
        .ok_or(ReadError::WorkerFailed)?;
    let body = if let Some((raw, cached)) = cached {
        if raw["id"].as_i64() != Some(id) {
            return Err(ReadError::WorkerFailed);
        }
        content::project_cached_episode(work, connection, raw, cached).await?
    } else {
        let raw = content::content_raw(work, connection, resource, id).await?;
        content::project(work, connection, resource, &raw).await?
    };
    let category = if matches!(kind, RelationKind::Episode) {
        Value::Null
    } else {
        account::category(work, connection, &snapshot["category"]).await?
    };
    let account = account::account(work, connection, context, &snapshot["account"]).await?;
    let value = vod_api_actions_dto::relation(kind, &snapshot["relation"], body, category, account)
        .map_err(|_| ReadError::SourceUnhandledFault("Provider quality representation failed"))?;
    if account::heap(&value)? > work.work_bytes() / 8 {
        return Err(ReadError::Capacity);
    }
    Ok(value)
}
