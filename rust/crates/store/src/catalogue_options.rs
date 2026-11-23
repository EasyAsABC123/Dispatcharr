//! Bounded JSON metadata measured on Original Dispatcharr's catalogue routes.
//! Original Dispatcharr and Django REST Framework authors retain full credit.
use crate::{
    channels::{
        channel_options_headers, channel_options_indentation, options_json_response,
        serialize_options_json,
    },
    AuthService, StoreError,
};
use axum::{
    extract::{ConnectInfo, OriginalUri, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde_json::{Map, Value};
use std::net::SocketAddr;

pub(super) const COLLECTION_ALLOW: &str = "GET, POST, HEAD, OPTIONS";
pub(super) const DETAIL_ALLOW: &str = "GET, PUT, PATCH, DELETE, HEAD, OPTIONS";
pub(super) const READ_ONLY_ALLOW: &str = "GET, HEAD, OPTIONS";
const COLLECTIONS: &str = include_str!("../datasets/source-catalogue-options-collections.json");

#[derive(Clone, Copy)]
pub(super) enum Kind {
    OutputProfile,
    ChannelProfile,
    ServerGroup,
    Programme,
    EpgData,
}

impl Kind {
    fn definition(self) -> (&'static str, &'static str, Option<&'static str>) {
        match self {
            Self::OutputProfile => (
                "output_profile",
                "/api/core/outputprofiles/",
                Some("SELECT EXISTS(SELECT 1 FROM core_outputprofile WHERE id=$1)"),
            ),
            Self::ChannelProfile => (
                "channel_profile",
                "/api/channels/profiles/",
                Some(
                    "SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channelprofile WHERE id=$1)",
                ),
            ),
            Self::ServerGroup => (
                "server_group",
                "/api/m3u/server-groups/",
                Some("SELECT EXISTS(SELECT 1 FROM m3u_servergroup WHERE id=$1)"),
            ),
            Self::Programme => (
                "programme",
                "/api/epg/programs/",
                Some("SELECT EXISTS(SELECT 1 FROM epg_programdata WHERE id=$1)"),
            ),
            Self::EpgData => ("epg_data", "/api/epg/epgdata/", None),
        }
    }

    fn allow(self, detail: bool) -> &'static str {
        match (self, detail) {
            (Self::EpgData, _) => READ_ONLY_ALLOW,
            (_, false) => COLLECTION_ALLOW,
            (_, true) => DETAIL_ALLOW,
        }
    }

    fn previous_unhandled(self, detail: bool) -> Response {
        let allow = match (self, detail) {
            (Self::EpgData, _) | (Self::Programme, false) => "GET,HEAD",
            (Self::OutputProfile, true) => "GET,HEAD,PUT,PATCH,DELETE",
            (_, true) => "GET,HEAD,PATCH,PUT,DELETE",
            (_, false) => "GET,HEAD,POST",
        };
        (StatusCode::METHOD_NOT_ALLOWED, [(header::ALLOW, allow)]).into_response()
    }

    fn metadata(self, detail: bool, actions: bool) -> Result<Map<String, Value>, StoreError> {
        let mut datasets: Map<String, Value> =
            serde_json::from_str(COLLECTIONS).map_err(|_| StoreError::UnhandledRestFault)?;
        let Some(Value::Object(mut metadata)) = datasets.shift_remove(self.definition().0) else {
            return Err(StoreError::UnhandledRestFault);
        };
        if detail {
            let name = metadata
                .get_mut("name")
                .ok_or(StoreError::UnhandledRestFault)?;
            let label = name
                .as_str()
                .and_then(|value| value.strip_suffix(" List"))
                .ok_or(StoreError::UnhandledRestFault)?;
            *name = format!("{label} Instance").into();
            if !actions {
                metadata.shift_remove("actions");
            } else if let Some(Value::Object(methods)) = metadata.get_mut("actions") {
                let fields = methods
                    .shift_remove("POST")
                    .ok_or(StoreError::UnhandledRestFault)?;
                methods.insert("PUT".into(), fields);
            }
        }
        Ok(metadata)
    }

    fn entity(
        self,
        detail: bool,
        actions: bool,
        indentation: usize,
    ) -> Result<Response, StoreError> {
        let body = serialize_options_json(&self.metadata(detail, actions)?, indentation)
            .ok_or(StoreError::UnhandledRestFault)?;
        options_json_response(body)
    }
}

fn detail_id(uri: &Uri, root: &str) -> Option<Option<i64>> {
    let suffix = uri.path().strip_prefix(root)?;
    if suffix.is_empty() {
        return Some(None);
    }
    let key = suffix.strip_suffix('/')?;
    if key.is_empty() || !key.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    // Keep very large decimal keys as missing under the same detail metadata.
    Some(Some(key.parse().unwrap_or(-1)))
}

pub(super) async fn respond(
    auth: &AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    uri: Uri,
    kind: Kind,
) -> Response {
    let (_, root, query) = kind.definition();
    let Some(id) = detail_id(&uri, root) else {
        return kind.previous_unhandled(true);
    };
    let detail = id.is_some();
    let Ok(indentation) = channel_options_indentation(&uri, &headers) else {
        return kind.previous_unhandled(detail);
    };
    let result = async {
        // DRF metadata uses the signed administrator permission and only its
        // PUT branch looks up the instance. The read-only serializer has no actions.
        auth.account_user(peer, headers, true).await?;
        let actions = match (id, query) {
            (Some(id), Some(query)) if id != -1 => {
                sqlx::query_scalar::<_, bool>(query)
                    .bind(id)
                    .fetch_one(auth.store.pool())
                    .await?
            }
            (None, _) => true,
            (Some(_), _) => false,
        };
        kind.entity(detail, actions, indentation)
    }
    .await;
    channel_options_headers(
        result.unwrap_or_else(IntoResponse::into_response),
        HeaderValue::from_static(kind.allow(detail)),
    )
}

macro_rules! options {
    ($name:ident, $kind:ident) => {
        pub(super) async fn $name(
            State(auth): State<AuthService>,
            ConnectInfo(peer): ConnectInfo<SocketAddr>,
            headers: HeaderMap,
            OriginalUri(uri): OriginalUri,
        ) -> Response {
            respond(&auth, peer, headers, uri, Kind::$kind).await
        }
    };
}
options!(output_profile, OutputProfile);
options!(channel_profile, ChannelProfile);
options!(server_group, ServerGroup);
options!(epg_data, EpgData);

pub(super) async fn typed_read_headers(
    State(allow): State<&'static str>,
    request: Request,
    next: Next,
) -> Response {
    let reads = matches!(*request.method(), Method::GET | Method::HEAD);
    let response = next.run(request).await;
    if reads
        && response.status() == StatusCode::OK
        && response
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|value| value == "application/json")
    {
        channel_options_headers(response, HeaderValue::from_static(allow))
    } else {
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
        middleware::from_fn_with_state,
        routing::get,
        Extension, Router,
    };
    use sha2::{Digest, Sha256};
    use tower::ServiceExt;

    #[tokio::test]
    async fn ten_catalogue_metadata_responses_match_physical_source_bytes() {
        for (kind, detail, length, digest) in [
            (
                Kind::OutputProfile,
                false,
                1102,
                "a9afdc89f2f4281d1229840761b2d772b4b942637162088c5add229766a9c382",
            ),
            (
                Kind::OutputProfile,
                true,
                1105,
                "fe8d8b8bdadcd1ae9d941aff61f19b559bd181e60c7a91a0fe9715acac19916e",
            ),
            (
                Kind::ChannelProfile,
                false,
                533,
                "7c6cb64b16bea11419253ef27b790a763df304584d6104ddda2816e07f2571f5",
            ),
            (
                Kind::ChannelProfile,
                true,
                536,
                "93da2b270da90d046a17f52d4bf6d3703bcdf4f6e22ca0f67d788a07895a8752",
            ),
            (
                Kind::ServerGroup,
                false,
                449,
                "5d1b3abac1df74e6a70fbf8a52a02798dea846a14a39f2254c972c5af525555a",
            ),
            (
                Kind::ServerGroup,
                true,
                452,
                "bbe5c72f5fb0654fa9f37fd3917e842fda6e96364b3dd04d5600f0e4a5b564fe",
            ),
            (
                Kind::Programme,
                false,
                838,
                "87ebfa1010089607b236872e80d34be90765fd0fccb78fd088f65f62407d5f24",
            ),
            (
                Kind::Programme,
                true,
                841,
                "398a9bd8a86aa582f08e71977e62703b5cdc0a6eabfe04a02d4d53621edbf00f",
            ),
            (
                Kind::EpgData,
                false,
                226,
                "1be93d604f1bc1436ca656e37d3fc3d905ca67cf7da0419683b5041f5cd1a501",
            ),
            (
                Kind::EpgData,
                true,
                230,
                "ac0e44cfa55143ce1a0a051d19931c0ab5eca3c541531d6a5e06ca999e139430",
            ),
        ] {
            let response = channel_options_headers(
                kind.entity(detail, true, 0).unwrap(),
                HeaderValue::from_static(kind.allow(detail)),
            );
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::ALLOW], kind.allow(detail));
            assert_eq!(response.headers()[header::VARY], "Accept");
            assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
            assert_eq!(
                response.headers()[header::CONTENT_LENGTH],
                length.to_string()
            );
            let body = to_bytes(response.into_body(), length).await.unwrap();
            assert_eq!(format!("{:x}", Sha256::digest(body)), digest);
            assert!(!kind.metadata(true, false).unwrap().contains_key("actions"));
        }
    }

    #[tokio::test]
    async fn catalogue_options_metadata_uses_the_protected_canonical_routers() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy_with(sqlx::postgres::PgConnectOptions::new());
        let auth = AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("catalogue-options-test-fixture".into()).unwrap(),
        );
        let programmes = crate::EpgProgramReads::new(auth.clone(), Default::default()).unwrap();
        let peer: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let router = programmes
            .router()
            .merge(crate::core_reads::router(auth.clone()))
            .merge(crate::channel_profiles::router(auth.clone()))
            .merge(crate::provider_reads::router(auth))
            .layer(Extension(ConnectInfo(peer)));
        for (kind, key) in [
            (Kind::OutputProfile, "3"),
            (Kind::ChannelProfile, "1"),
            (Kind::ServerGroup, "1"),
            (Kind::Programme, "1"),
            (Kind::EpgData, "1"),
        ] {
            let (_, root, _) = kind.definition();
            for (detail, path) in [(false, root.to_owned()), (true, format!("{root}{key}/"))] {
                for (accept, expected) in [
                    ("application/json", StatusCode::UNAUTHORIZED),
                    ("text/html", StatusCode::METHOD_NOT_ALLOWED),
                ] {
                    let request = Request::builder()
                        .method(Method::OPTIONS)
                        .uri(path.as_str())
                        .header(header::ACCEPT, accept)
                        .body(Body::empty())
                        .unwrap();
                    let response = router.clone().oneshot(request).await.unwrap();
                    assert_eq!(response.status(), expected, "{path} {accept}");
                    if expected == StatusCode::UNAUTHORIZED {
                        assert_eq!(response.headers()[header::ALLOW], kind.allow(detail));
                        assert_eq!(response.headers()[header::VARY], "Accept");
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn catalogue_read_headers_only_decorate_successful_typed_get_and_head() {
        let router = Router::new()
            .route("/ready", get(|| async { axum::Json([1, 2]) }))
            .route(
                "/missing",
                get(|| async { (StatusCode::NOT_FOUND, axum::Json([1, 2])) }),
            )
            .layer(from_fn_with_state(READ_ONLY_ALLOW, typed_read_headers));
        for (method, uri, status, metadata) in [
            (Method::GET, "/ready", StatusCode::OK, true),
            (Method::HEAD, "/ready", StatusCode::OK, true),
            (Method::GET, "/missing", StatusCode::NOT_FOUND, false),
            (
                Method::OPTIONS,
                "/ready",
                StatusCode::METHOD_NOT_ALLOWED,
                false,
            ),
        ] {
            let request = Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .unwrap();
            let response = router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), status);
            assert_eq!(response.headers().get(header::VARY).is_some(), metadata);
            if metadata {
                assert_eq!(response.headers()[header::ALLOW], READ_ONLY_ALLOW);
            }
        }
    }
}
