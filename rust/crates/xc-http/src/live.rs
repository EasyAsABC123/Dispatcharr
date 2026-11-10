//! XC live entry port from original apps/proxy/live_proxy/views.py stream_xc.
//! Original Dispatcharr authors retain credit. Component/media gates pending.
use super::*;
use dispatcharr_contracts::{BoxFuture, OutputFormat, PlaybackError, PlaybackSource};
pub type LiveDispatcher = Arc<
    dyn Fn(PlaybackSource) -> BoxFuture<'static, Result<Response, PlaybackError>> + Send + Sync,
>;
enum Authorized {
    Response(Response),
    Source(Box<PlaybackSource>),
}
impl XcGateway {
    pub fn with_live(self, dispatcher: LiveDispatcher) -> Result<Self, StoreError> {
        self.inner
            .live
            .set(dispatcher)
            .map_err(|_| StoreError::Unsupported("XC live dispatcher already configured"))?;
        Ok(self)
    }
    /// Explicit opt-in: caller must preserve static-file precedence for the
    /// original short three-component credential alias when merging routers.
    pub fn live_router(&self) -> Router {
        Router::new()
            .route("/live/{username}/{password}/{channel}", any(live_dispatch))
            .route("/{username}/{password}/{channel}", any(live_dispatch))
            .with_state(self.clone())
    }
    async fn authorize_live(&self, request: Request<Body>) -> Result<Authorized, StoreError> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(reader::capacity());
        }
        let permit = self
            .inner
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| reader::capacity())?;
        self.inner.workers.fetch_add(1, Ordering::AcqRel);
        let worker = Worker(self.inner.clone());
        let gateway = self.clone();
        tokio::spawn(async move {
            let _worker = worker;
            let _permit = permit;
            tokio::time::timeout(
                gateway.inner.config.request_deadline,
                gateway.resolve_live(request),
            )
            .await
            .map_err(|_| reader::capacity())?
        })
        .await
        .map_err(|_| reader::capacity())?
    }
    async fn resolve_live(&self, request: Request<Body>) -> Result<Authorized, StoreError> {
        let _memory = self
            .inner
            .memory
            .clone()
            .acquire_many_owned((self.inner.config.max_metadata_bytes * 2) as u32)
            .await
            .map_err(|_| reader::capacity())?;
        let bytes = request
            .headers()
            .iter()
            .try_fold(request.uri().to_string().len(), |size, (key, value)| {
                size.checked_add(key.as_str().len() + value.as_bytes().len() + 4)
            })
            .ok_or_else(reader::capacity)?;
        if bytes > 64 * 1024 {
            return Err(reader::capacity());
        }
        for key in ["authorization", "x-api-key"] {
            let values = request.headers().get_all(key);
            if values.iter().count() > 1 || values.iter().any(|value| value.to_str().is_err()) {
                return Err(StoreError::Unauthorized(
                    "Invalid authentication credentials.",
                ));
            }
        }
        let identity = identity(&request);
        // DRF performs content negotiation/authentication before method dispatch.
        if identity
            .parameter("format")
            .is_some_and(|value| !value.is_empty() && value != "json")
        {
            return Ok(Authorized::Response(drf(
                404,
                json!({"detail":"Not found."}),
                false,
            )));
        }
        self.inner.auth.authenticate(&identity).await?;
        let head = request.method() == Method::HEAD;
        if request.method() == Method::OPTIONS {
            return Ok(Authorized::Response(drf(
                200,
                json!({"name":"Stream Xc","description":"","renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]}),
                false,
            )));
        }
        if request.method() != Method::GET {
            return Ok(Authorized::Response(drf(
                405,
                json!({"detail":format!("Method \"{}\" not allowed.",request.method())}),
                head,
            )));
        }
        let path = request
            .uri()
            .path()
            .strip_prefix("/live/")
            .unwrap_or_else(|| request.uri().path().trim_start_matches('/'));
        let parts: Vec<_> = path.split('/').collect();
        if parts.len() != 3 {
            return Err(StoreError::NotFound);
        }
        let decode = |value: &str| {
            url::form_urlencoded::parse(format!("v={}", value.replace('+', "%2B")).as_bytes())
                .next()
                .map(|(_, value)| value.into_owned())
                .unwrap_or_default()
        };
        let username = decode(parts[0]);
        let password = decode(parts[1]);
        let raw = decode(parts[2]);
        let user = self
            .inner
            .auth
            .authorize_xc(&identity, &username, &password, "XC_API")
            .await?;
        // pathlib.Path suffix treats a leading dot as part of the stem; Python
        //3.13 treats a trailing lone dot as no suffix. Multiple dots retain all
        //but the last suffix in the stem. Actual vectors gate unusual spellings.
        let (channel, suffix) = raw
            .rsplit_once('.')
            .filter(|(stem, suffix)| !stem.is_empty() && !suffix.is_empty())
            .unwrap_or((&raw, ""));
        let forced = match suffix.to_ascii_lowercase().as_str() {
            "ts" => Some(OutputFormat::MpegTs),
            "mp4" => Some(OutputFormat::Fmp4),
            "m3u8" => Some(OutputFormat::Hls),
            _ => None,
        };
        match self
            .inner
            .auth
            .resolve_xc_authorized(identity, user, channel, forced)
            .await
        {
            Ok(source) => Ok(Authorized::Source(Box::new(source))),
            Err(error) => {
                if error.status == 404
                    && error.body.get("error").and_then(serde_json::Value::as_str)
                        == Some("Not found")
                {
                    let mut response = Response::new(Body::from(Bytes::from_static(
                        b"{\"error\": \"Not found\"}",
                    )));
                    *response.status_mut() = StatusCode::NOT_FOUND;
                    response
                        .headers_mut()
                        .insert("content-type", "application/json".parse().unwrap());
                    Ok(Authorized::Response(response))
                } else {
                    Ok(Authorized::Response(drf(error.status, error.body, false)))
                }
            }
        }
    }
}
fn drf(status: u16, value: serde_json::Value, head: bool) -> Response {
    let mut response = if head {
        Response::new(Body::empty())
    } else {
        axum::Json(value).into_response()
    };
    *response.status_mut() =
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    response
        .headers_mut()
        .insert("content-type", "application/json".parse().unwrap());
    response
}
async fn live_dispatch(State(gateway): State<XcGateway>, request: Request<Body>) -> Response {
    let head = request.method() == Method::HEAD;
    let mut response = match gateway.authorize_live(request).await {
        Ok(Authorized::Response(response)) => response,
        Ok(Authorized::Source(source)) => match gateway.inner.live.get() {
            Some(dispatcher) => match dispatcher(*source).await {
                Ok(response) => response,
                Err(error) => drf(error.status, error.body, false),
            },
            None => drf(503, json!({"error":"XC live output unavailable"}), false),
        },
        Err(StoreError::XcDenied {
            status,
            retry_after,
        }) => {
            let body = match status {
                403 => "Forbidden",
                429 => "Too many attempts",
                _ => "Unauthorized",
            };
            let mut response = Response::new(if head {
                Body::empty()
            } else {
                Body::from(format!("{{\"error\": \"{body}\"}}"))
            });
            *response.status_mut() = StatusCode::from_u16(status).unwrap();
            response
                .headers_mut()
                .insert("content-type", "application/json".parse().unwrap());
            if let Some(wait) = retry_after {
                response.headers_mut().insert("retry-after", wait.into());
            }
            response
        }
        Err(error) => {
            let mut response = error.into_response();
            if head {
                *response.body_mut() = Body::empty();
            }
            response
        }
    };
    response
        .headers_mut()
        .insert("allow", "OPTIONS, GET".parse().unwrap());
    response
}
