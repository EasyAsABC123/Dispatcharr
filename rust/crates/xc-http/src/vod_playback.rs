//! Source XC Movie/Episode public routes and the trusted VOD engine handoff.
//! Original Dispatcharr authors and contributors retain application credit.
use super::{identity, reader, xc_entry_error, Worker, XcGateway};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{header, Method, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    Router,
};
use dispatcharr_ingest_core::source_integer_literal;
use dispatcharr_store::{StoreError, XcUserReadLimits};
use dispatcharr_vod_http::{VodEngine, VodRequest};
use percent_encoding::percent_decode_str;
use serde_json::json;
use std::{net::SocketAddr, sync::atomic::Ordering, sync::Arc};

const MAX_HTTP_BYTES: usize = 64 * 1024;

struct Entry {
    episode: bool,
    username: String,
    password: String,
    stream: String,
}

fn entry(path: &str) -> Option<Entry> {
    let (episode, rest) = path
        .strip_prefix("/movie/")
        .map(|tail| (false, tail))
        .or_else(|| path.strip_prefix("/series/").map(|tail| (true, tail)))?;
    let mut parts = rest.splitn(3, '/');
    let decode = |part: &str| {
        let text = percent_decode_str(part).decode_utf8().ok()?;
        (!text.is_empty() && !text.contains('/')).then(|| text.into_owned())
    };
    let username = decode(parts.next()?)?;
    let password = decode(parts.next()?)?;
    let video = decode(parts.next()?)?;
    // The Source's nonempty str converter is greedy before the last dot.
    let (stream, _) = video
        .rsplit_once('.')
        .filter(|(stream, extension)| !stream.is_empty() && !extension.is_empty())?;
    Some(Entry {
        episode,
        username,
        password,
        stream: stream.into(),
    })
}

impl XcGateway {
    pub fn with_vod_playback(&self, router: Router, vod: VodEngine) -> Router {
        // Partial-segment Django converters cannot be expressed as Axum route
        // parameters. Leave unmatched requests to Main's existing routing.
        router.layer(middleware::from_fn_with_state(
            (self.clone(), vod),
            dispatch,
        ))
    }

    async fn resolve_vod(
        &self,
        request: Request<Body>,
        entry: Entry,
        vod: VodEngine,
    ) -> Result<Response, StoreError> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(reader::capacity());
        }
        let permit = self
            .inner
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| reader::capacity())?;
        let gateway = self.clone();
        self.inner.workers.fetch_add(1, Ordering::AcqRel);
        let worker = Worker(self.inner.clone());
        tokio::spawn(async move {
            let _worker = worker;
            let config = &gateway.inner.config;
            let owner = {
                let memory = tokio::time::timeout(
                    config.request_deadline,
                    gateway
                        .inner
                        .memory
                        .clone()
                        .acquire_many_owned((config.max_metadata_bytes + MAX_HTTP_BYTES * 4) as u32),
                )
                .await
                .map_err(|_| reader::capacity())?
                .map_err(|_| reader::capacity())?;
                Arc::new((permit, memory))
            };
            let head = request.method() == Method::HEAD;
            let auth = gateway.inner.auth.clone().with_retained_scope(owner.clone());
            let input = identity(&request);
            let path = request.uri().path().to_owned();
            let (request, user) = match tokio::time::timeout(config.request_deadline, async {
                if input
                    .parameter("format")
                    .is_some_and(|format| !matches!(format, "" | "json" | "api"))
                {
                    return Ok(Err(drf(404, json!({"detail":"Not found."}))));
                }
                // The outer DRF AllowAny wrapper still authenticates before
                // its method check or the credentials in the XC route.
                auth.authenticate(&input).await?;
                if request.method() == Method::OPTIONS {
                    let name = if entry.episode { "Stream Xc Episode" } else { "Stream Xc Movie" };
                    return Ok(Err(drf(200, json!({"name":name,"description":"",
                        "renders":["application/json","text/html"],
                        "parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]}))));
                }
                if request.method() != Method::GET {
                    return Ok(Err(drf(405, json!({"detail":format!("Method \"{}\" not allowed.", request.method())}))));
                }
                let user = auth.authorize_xc_bounded(
                    &input,
                    &entry.username,
                    &entry.password,
                    "XC_API",
                    XcUserReadLimits {
                        max_raw_bytes: config.max_metadata_bytes.min(64 * 1024),
                        max_nodes: 4096,
                        max_depth: 64,
                        work_bytes: config.max_metadata_bytes,
                    },
                ).await?.user;
                if !crate::vod::enabled(&user.custom_properties, entry.episode)? {
                    return Ok(Err(json_error(403, r#"{"error": "Forbidden"}"#)));
                }
                let integer = source_integer_literal(&entry.stream)
                    .ok_or(StoreError::UnhandledRestFault)?;
                let content = match integer.parse::<i64>() {
                    Ok(id) => gateway.xc_content(entry.episode, id).await?,
                    Err(_) => None,
                };
                let Some(content_id) = content else {
                    return Ok(Err(if entry.episode {
                        json_error(404, r#"{"error": "Episode not found"}"#)
                    } else {
                        json_error(404, r#"{"error": "Movie not found"}"#)
                    }));
                };
                let session_id = input.parameter("session_id").filter(|value| !value.is_empty()).map(str::to_owned);
                let profile_id = input.parameter("profile_id")
                    .and_then(source_integer_literal)
                    .and_then(|value| value.parse().ok());
                let kind = if entry.episode { "episode" } else { "movie" };
                Ok::<_, StoreError>(Ok((VodRequest {
                    resolved_client_ip: None,
                    playback: input,
                    kind: kind.into(),
                    content_id,
                    session_id,
                    profile_id,
                }, user)))
            }).await.map_err(|_| reader::capacity())?? {
                Ok(authorized) => authorized,
                Err(mut response) => {
                    if head {
                        *response.body_mut() = Body::empty();
                    }
                    return Ok(response);
                }
            };

            // Main owns this bridge through response construction. Playback
            // then transfers resources to the existing VOD body/session owner.
            let response = vod.respond_xc(request, user, &path).await;
            drop(owner);
            Ok(response)
        })
        .await
        .map_err(|_| reader::capacity())?
    }

    async fn xc_content(&self, episode: bool, id: i64) -> Result<Option<String>, StoreError> {
        let statement = if episode {
            "SELECT e.uuid::text FROM vod_m3uepisoderelation r JOIN vod_episode e ON e.id=r.episode_id JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.episode_id=$1 AND a.is_active ORDER BY a.priority DESC,r.id LIMIT 1"
        } else {
            "SELECT m.uuid::text FROM vod_m3umovierelation r JOIN vod_movie m ON m.id=r.movie_id JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.movie_id=$1 AND a.is_active ORDER BY a.priority DESC,r.id LIMIT 1"
        };
        Ok(sqlx::query_scalar(statement)
            .bind(id)
            .fetch_optional(self.inner.store.pool())
            .await?)
    }
}

fn json_error(status: u16, body: &'static str) -> Response {
    (
        StatusCode::from_u16(status).unwrap(),
        [("content-type", "application/json")],
        body,
    )
        .into_response()
}

fn drf(status: u16, value: serde_json::Value) -> Response {
    (StatusCode::from_u16(status).unwrap(), axum::Json(value)).into_response()
}

async fn dispatch(
    State((gateway, vod)): State<(XcGateway, VodEngine)>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if !path.starts_with("/movie/") && !path.starts_with("/series/") {
        return next.run(request).await;
    }
    let header_bytes = request.headers().iter().try_fold(
        path.len() + request.uri().query().map_or(0, str::len),
        |size, (name, value)| size.checked_add(name.as_str().len() + value.as_bytes().len() + 4),
    );
    if header_bytes.is_none_or(|size| size > MAX_HTTP_BYTES) {
        return reader::capacity().into_response();
    }
    let Some(entry) = entry(path) else {
        return next.run(request).await;
    };
    if request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .is_none()
    {
        return reader::capacity().into_response();
    }
    let head = request.method() == Method::HEAD;
    let mut response = match gateway.resolve_vod(request, entry, vod).await {
        Ok(response) => response,
        Err(error) => xc_entry_error(error),
    };
    response
        .headers_mut()
        .insert(header::VARY, "Accept, origin".parse().unwrap());
    response
        .headers_mut()
        .insert(header::ALLOW, "GET, OPTIONS".parse().unwrap());
    if let Some(location) = response.headers_mut().get_mut(header::LOCATION) {
        // Both the same-path XC redirect and a provider redirect can carry
        // credentials. The protocol needs the value, general telemetry does not.
        location.set_sensitive(true);
    }
    if head {
        *response.body_mut() = Body::empty();
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_xc_converter_and_fallthrough() {
        let movie = entry("/movie/u%2B1/p%26w/123.mp4").unwrap();
        assert!(!movie.episode);
        assert_eq!(
            (
                movie.username.as_str(),
                movie.password.as_str(),
                movie.stream.as_str()
            ),
            ("u+1", "p&w", "123")
        );
        let series = entry("/series/u/p/1.2.mp4").unwrap();
        assert!(series.episode);
        assert_eq!(series.stream, "1.2");
        for unmatched in [
            "/movie/u/p/123",
            "/movie/u/p/123.",
            "/movie/u/p/.mp4",
            "/movie/u/p/123.mp4/",
            "/movie/u%2Fp/p/123.mp4",
            "/series/u//123.mp4",
        ] {
            assert!(entry(unmatched).is_none());
        }
    }
}
