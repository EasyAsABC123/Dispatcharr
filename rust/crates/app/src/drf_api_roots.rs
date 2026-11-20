//! The pinned Original router's ordinary JSON API directories.
//! Original Dispatcharr and Django REST Framework authors retain full credit.
use axum::{
    extract::{ConnectInfo, OriginalUri, State},
    http::{header, HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use dispatcharr_contracts::{source_headers, PlaybackRequest};
use dispatcharr_store::{AuthService, StoreError};
use serde_json::{Map, Value};
use std::net::SocketAddr;

struct Directory {
    prefix: &'static str,
    bare: bool,
    resources: &'static [&'static str],
}

const DIRECTORIES: &[Directory] = &[
    Directory {
        prefix: "/api/accounts/",
        bare: true,
        resources: &["users", "groups", "api-keys"],
    },
    Directory {
        prefix: "/api/channels/",
        bare: true,
        resources: &[
            "streams",
            "groups",
            "channels",
            "logos",
            "profiles",
            "recordings",
            "recurring-rules",
        ],
    },
    Directory {
        prefix: "/api/epg/",
        bare: true,
        resources: &["sources", "programs", "epgdata"],
    },
    Directory {
        prefix: "/api/m3u/",
        bare: true,
        resources: &["accounts", "server-groups"],
    },
    Directory {
        prefix: "/api/core/",
        bare: true,
        resources: &[
            "useragents",
            "streamprofiles",
            "outputprofiles",
            "settings",
            "notifications",
        ],
    },
    Directory {
        prefix: "/api/vod/",
        bare: true,
        resources: &[
            "movies",
            "episodes",
            "series",
            "categories",
            "all",
            "vodlogos",
        ],
    },
    Directory {
        prefix: "/api/connect/",
        bare: true,
        resources: &["integrations", "subscriptions", "logs"],
    },
    Directory {
        prefix: "/api/hdhr/",
        bare: false,
        resources: &["devices"],
    },
    Directory {
        prefix: "/hdhr/",
        bare: false,
        resources: &["devices"],
    },
];

pub(super) fn router(auth: AuthService) -> Router {
    let mut router = Router::new();
    for directory in DIRECTORIES {
        if directory.bare {
            router = router.route(directory.prefix, get(directory_response));
        }
        router = router.route(
            &format!("{}.json", directory.prefix),
            get(directory_response),
        );
    }
    router.with_state(auth)
}

fn directory_links(directory: &Directory, origin: &str, suffix: bool) -> Map<String, Value> {
    let extension = if suffix { ".json" } else { "/" };
    directory
        .resources
        .iter()
        .map(|resource| {
            (
                (*resource).to_owned(),
                Value::String(format!("{origin}{}{resource}{extension}", directory.prefix)),
            )
        })
        .collect()
}

async fn directory_response(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    let authorized = auth.account_user(peer, headers.clone(), true).await;
    let mut response = match authorized {
        Ok(_) => {
            let (prefix, suffix) = uri
                .path()
                .strip_suffix(".json")
                .map_or((uri.path(), false), |path| (path, true));
            let Some(directory) = DIRECTORIES
                .iter()
                .find(|directory| directory.prefix == prefix)
            else {
                return StoreError::NotFound.into_response();
            };
            let request = PlaybackRequest {
                channel_id: String::new(),
                query: Vec::new(),
                peer_ip: peer.ip(),
                headers: source_headers(
                    headers
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.as_bytes())),
                ),
            };
            let origin = dispatcharr_output_http::source_output_origin(&auth, &request);
            Json(directory_links(directory, &origin, suffix)).into_response()
        }
        Err(error) => error.into_response(),
    };
    response.headers_mut().insert(
        header::ALLOW,
        HeaderValue::from_static("GET, HEAD, OPTIONS"),
    );
    response
        .headers_mut()
        .append(header::VARY, HeaderValue::from_static("Accept"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directories_keep_original_link_order_absolute_paths_and_narrow_hdhr_scope() {
        let accounts = &DIRECTORIES[0];
        let value = directory_links(accounts, "http://127.0.0.1:5656", true);
        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            r#"{"users":"http://127.0.0.1:5656/api/accounts/users.json","groups":"http://127.0.0.1:5656/api/accounts/groups.json","api-keys":"http://127.0.0.1:5656/api/accounts/api-keys.json"}"#
        );
        assert_eq!(DIRECTORIES.len(), 9);
        assert_eq!(
            DIRECTORIES
                .iter()
                .filter(|directory| directory.bare)
                .count(),
            7
        );
        assert!(DIRECTORIES
            .iter()
            .filter(|directory| !directory.bare)
            .all(|directory| {
                directory.prefix.ends_with("/hdhr/") && directory.resources == ["devices"]
            }));
    }
}
