//! Source-derived VOD image/cache component and ownership controls.
//! Actual Original/native240 and physical/mixed host gates passed;
//! canonical app, unchanged React and broader image contracts are separate.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::{
    vod_api_images_resolve as resolve, vod_api_images_sql as sql,
    vod_api_owned_read::{ReadError, ReadRuntime},
    vod_api_projection::visibility,
    vod_api_query::{self, QueryError},
    vod_api_read_component::HttpRead,
    vod_api_sql::Resource,
};
use axum::http::{HeaderMap, Method};
use bytes::Bytes;
use serde_json::{json, Value};
use sqlx::Row;
use std::{io::Write, net::SocketAddr, sync::Arc};
#[derive(Clone, Copy, Debug)]
pub(super) enum Image {
    Movie,
    Series,
    Episode,
    Logo,
}
impl Image {
    pub(super) fn resource(self) -> Option<Resource> {
        match self {
            Self::Movie => Some(Resource::Movie),
            Self::Series => Some(Resource::Series),
            Self::Episode => Some(Resource::Episode),
            Self::Logo => None,
        }
    }
    pub(super) fn missing(self) -> ReadError {
        crate::StoreError::StatusBody(404,json!({"detail":format!("No {} matches the given query.",match self{Self::Movie=>"Movie",Self::Series=>"Series",Self::Episode=>"Episode",Self::Logo=>"VODLogo"})})).into()
    }
    fn options(self) -> Value {
        let (name, description) = match self {
            Self::Movie => (
                "Image",
                "Proxy a stored movie image (backdrop, movie_image, poster_path).",
            ),
            Self::Series => (
                "Image",
                "Proxy a stored series image (backdrop, movie_image, poster_path).",
            ),
            Self::Episode => (
                "Image",
                "Proxy a stored episode image (movie_image, backdrop, poster_path).",
            ),
            Self::Logo => (
                "Cache",
                "Streams the VOD logo file, whether it's local or remote.",
            ),
        };
        json!({"name":name,"description":description,"renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]})
    }
}
#[derive(Clone)]
pub(super) struct Images {
    auth: crate::AuthService,
    runtime: ReadRuntime,
}
impl Images {
    pub(super) fn shared(auth: crate::AuthService, runtime: ReadRuntime) -> Self {
        Self { auth, runtime }
    }
    pub(super) async fn dispatch(
        &self,
        image: Image,
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
            .run_until(input, Some(deadline), move |work| async move {
                work.check()?;
                let raw_image = matches!(http.method, Method::GET | Method::HEAD);
                if !raw_image {
                    super::vod_api_http::negotiate(&headers, &http.uri)?;
                }
                let physical = work.physical_guard();
                let guard: Arc<dyn Send + Sync> = match http.scope {
                    Some(scope) => Arc::new((physical, scope)),
                    None => physical,
                };
                let auth = state.auth.with_retained_scope(guard);
                let user = if raw_image {
                    if ["authorization", "x-api-key"].iter().any(|name| {
                        headers
                            .get(*name)
                            .is_some_and(|value| value.to_str().is_err())
                    }) {
                        return Err(crate::StoreError::Unauthorized(
                            "Invalid authentication header",
                        )
                        .into());
                    }
                    auth.authenticate(&crate::auth::request(peer, headers))
                        .await?
                } else {
                    Some(auth.account_user(peer, headers, true).await?)
                };
                work.check()?;
                let mut document = work.document();
                if !raw_image {
                    if http.method == Method::OPTIONS {
                        serde_json::to_writer(&mut document, &image.options())
                            .map_err(|_| ReadError::Capacity)?;
                        return Ok(document.finish());
                    }
                    return Err(crate::StoreError::StatusBody(
                        405,
                        json!({"detail":format!("Method \"{}\" not allowed.",http.method)}),
                    )
                    .into());
                }
                let query: Vec<(String, String)> =
                    url::form_urlencoded::parse(http.uri.query().unwrap_or("").as_bytes())
                        .into_owned()
                        .collect();
                let last = |key| {
                    query
                        .iter()
                        .rev()
                        .find(|(name, _)| name == key)
                        .map(|(_, value)| value.as_str())
                };
                let raw = http.raw_detail.as_deref().ok_or(ReadError::WorkerFailed)?;
                let parsed = dispatcharr_ingest_core::source_integer_literal(raw);
                let id = parsed
                    .as_deref()
                    .and_then(|value| value.parse::<i64>().ok());
                // Filter/form construction precedes typed lookup failure in Source get_object.
                let visible = if matches!(image, Image::Logo) {
                    visibility(&Value::Null, 10)
                } else {
                    user.as_ref()
                        .map(|user| visibility(&user.custom_properties, user.user_level))
                        .unwrap_or_else(|| visibility(&Value::Null, 10))
                }
                .map_err(|_| ReadError::SourceUnhandledFault("Image user properties failed"))?;
                let selection = image
                    .resource()
                    .map(|resource| {
                        vod_api_query::ordinary(resource, Some(id.unwrap_or(0)), &query)
                    })
                    .transpose()
                    .map_err(|error| match error {
                        QueryError::Source400(value) => {
                            ReadError::Source(crate::StoreError::StatusBody(400, value))
                        }
                        QueryError::Capacity => ReadError::Capacity,
                        QueryError::Unsupported(message) => ReadError::Unsupported(message),
                    })?;
                let Some(id) = id else {
                    return Err(if parsed.is_none() {
                        crate::StoreError::NotFound.into()
                    } else {
                        image.missing()
                    });
                };
                let mut connection = work.connection(auth.store.pool()).await?;
                let result = async {
                    let selected = if let Some(resource) = image.resource() {
                        if !resource.enabled(&visible) {
                            return Err(image.missing());
                        }
                        let statement = selection.as_ref().ok_or(ReadError::WorkerFailed)?.rows(
                            &visible,
                            work.row_bytes(),
                            None,
                        )?;
                        connection.configure(&work).await?;
                        let rows = statement.query().fetch_all(connection.raw()).await?;
                        if rows.is_empty() {
                            return Err(image.missing());
                        }
                        if rows.len() != 1 {
                            return Err(ReadError::WorkerFailed);
                        }
                        if rows[0].try_get::<bool, _>("oversized")? {
                            return Err(ReadError::Capacity);
                        }
                        let props = rows[0]
                            .try_get::<Option<Value>, _>("content_custom_properties")?
                            .unwrap_or(Value::Null);
                        let kind = last("kind").unwrap_or("backdrop");
                        let index = last("index").unwrap_or("0");
                        if !["backdrop", "movie_image", "poster_path"].contains(&kind) {
                            return Err(crate::StoreError::StatusBody(
                                404,
                                json!({"detail":"Image not found"}),
                            )
                            .into());
                        }
                        let relation = sql::relation(
                            &work,
                            &mut connection,
                            resource,
                            id,
                            resolve::account(last("m3u_account_id")),
                        )
                        .await?;
                        resolve::resolve(
                            &props,
                            relation.as_ref().map(|row| &row["custom_properties"]),
                            kind,
                            index,
                        )
                        .map_err(|_| {
                            ReadError::SourceUnhandledFault("Image object properties failed")
                        })?
                        .ok_or_else(|| {
                            crate::StoreError::StatusBody(404, json!({"detail":"Image not found"}))
                        })?
                    } else {
                        sql::logo(&work, &mut connection, id, &query).await?
                    };
                    document
                        .write_all(selected.as_bytes())
                        .map_err(|_| ReadError::Capacity)?;
                    Ok(document.finish())
                }
                .await;
                let cleanup = connection.finish().await;
                match result {
                    Err(error) => Err(error),
                    Ok(value) => {
                        cleanup?;
                        Ok(value)
                    }
                }
            })
            .await
    }
}
