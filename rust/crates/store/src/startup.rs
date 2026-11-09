//! Original React startup helper APIs. Contracts derive from Dispatcharr's
//! accounts/core views; original authors retain all credit (ATTRIBUTION.md).
use crate::{auth::request, cidrs_allow, AuthService, StoreError, TokenPair, User};
use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, Method},
    routing::{any, get, post},
    Json, Router,
};
use serde_json::{json, Value};
use std::net::{IpAddr, SocketAddr};

const LOCAL: &str =
    "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,::1/128,fc00::/7,fe80::/10";
const SETUP_ENV: &str = "DISPATCHARR_SETUP_ALLOWED_IP";
fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.into())
}
fn flag(name: &str, default: &str) -> bool {
    env(name, default).to_lowercase() == "true"
}
fn normalized(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or(IpAddr::V6(ip), IpAddr::V4),
        ip => ip,
    }
}
fn setup_allowed(ip: IpAddr, override_ip: &str) -> bool {
    if override_ip.trim().is_empty() {
        return cidrs_allow(LOCAL, ip);
    }
    override_ip
        .trim()
        .parse::<IpAddr>()
        .is_ok_and(|value| normalized(value) == ip)
}
async fn setup(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    method: Method,
) -> Result<Json<Value>, StoreError> {
    // This is a plain Django view, not DRF. It ignores presented credentials,
    // body and method once any admin-level account exists (even inactive).
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM accounts_user WHERE user_level>=10)")
            .fetch_one(auth.store.pool())
            .await?;
    if exists {
        return Ok(Json(json!({"superuser_exists":true})));
    }
    let ip = auth.client_ip(&request(peer, headers));
    let allowed = setup_allowed(ip, &env(SETUP_ENV, ""));
    if method == Method::POST {
        if !allowed {
            return Err(StoreError::StatusBody(
                403,
                json!({"error":format!("Web setup is limited to local networks by default. Set {SETUP_ENV} to your IP to allow setup from this network, or create the account with: python manage.py createsuperuser"),"client_ip":ip.to_string(),"setup_allowed":false}),
            ));
        }
        return Err(StoreError::Unsupported(
            "First-install bootstrap requires the original migrations and has not been ported.",
        ));
    }
    Ok(Json(
        json!({"superuser_exists":false,"client_ip":ip.to_string(),"setup_allowed":allowed}),
    ))
}

async fn version(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    // AllowAny still invokes DRF authentication when credentials are presented.
    auth.authenticate(&request(peer, headers)).await?;
    let timestamp = std::env::var("DISPATCHARR_VERSION_TIMESTAMP")
        .ok()
        .map(Value::String)
        .unwrap_or(Value::Null);
    Ok(Json(
        json!({"version":env("DISPATCHARR_VERSION","0.33.0"),"timestamp":timestamp}),
    ))
}

fn valid_header(header: &str) -> bool {
    !header.is_empty()
        && header.len() <= 64
        && header.as_bytes()[0].is_ascii_alphanumeric()
        && header
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || value == b'-')
}
impl AuthService {
    async fn proxy_user(&self, identity: &str) -> Result<Option<User>, StoreError> {
        if let Some(user) = self.store.user_by_username(identity).await? {
            return Ok(Some(user));
        }
        let ids: Vec<i64> = sqlx::query_scalar(
            "SELECT id::bigint FROM accounts_user WHERE UPPER(username)=UPPER($1) LIMIT 2",
        )
        .bind(identity)
        .fetch_all(self.store.pool())
        .await?;
        if !ids.is_empty() {
            return if ids.len() == 1 {
                self.store.user_by_id(ids[0]).await
            } else {
                Ok(None)
            };
        }
        if identity.contains('@') {
            let ids: Vec<i64> = sqlx::query_scalar(
                "SELECT id::bigint FROM accounts_user WHERE UPPER(email)=UPPER($1) LIMIT 2",
            )
            .bind(identity)
            .fetch_all(self.store.pool())
            .await?;
            if ids.len() == 1 {
                return self.store.user_by_id(ids[0]).await;
            }
        }
        Ok(None)
    }
    pub(crate) fn explicit_proxy_peer(&self, peer: IpAddr) -> bool {
        self.explicit_trusted_proxies()
            .is_some_and(|cidrs| cidrs_allow(cidrs, normalized(peer)))
    }
}
async fn proxy_login(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<TokenPair>, StoreError> {
    let request = request(peer, headers);
    auth.authenticate(&request).await?;
    let ip = auth.client_ip(&request);
    if !auth.store.network_allowed(ip, "UI", None).await? {
        return Err(StoreError::Forbidden);
    }
    let settings = auth.store.setting("reverse_proxy_auth").await?;
    let header = settings
        .get("header")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    let unavailable = || StoreError::Unauthorized("Reverse proxy authentication unavailable.");
    if !crate::channels::truthy(settings.get("enabled"))
        || !valid_header(header)
        || !auth.explicit_proxy_peer(peer.ip())
    {
        return Err(unavailable());
    }
    let header = header.to_ascii_lowercase();
    let identity = request
        .header(&header)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(unavailable)?;
    auth.throttle(ip, "login", true)?;
    let user = auth
        .proxy_user(identity)
        .await?
        .filter(|user| user.is_active)
        .ok_or(StoreError::Unauthorized(
            "No account matches the authenticated identity.",
        ))?;
    sqlx::query("UPDATE accounts_user SET last_login=NOW() WHERE id=$1")
        .bind(user.id)
        .execute(auth.store.pool())
        .await?;
    Ok(Json(TokenPair {
        access: auth.mint(json!(user.id.to_string()), "access")?,
        refresh: auth.mint(json!(user.id.to_string()), "refresh")?,
    }))
}

fn tls_status() -> (Value, Value) {
    let postgres = flag("POSTGRES_SSL", "false");
    let postgres = json!({"enabled":postgres,"ssl_mode":if postgres {Some(env("POSTGRES_SSL_MODE","verify-full"))}else{None},"mtls":!env("POSTGRES_SSL_CERT","").is_empty()&&!env("POSTGRES_SSL_KEY","").is_empty()});
    let address = env("REDIS_URL", "");
    let redis = if !address.is_empty() {
        match url::Url::parse(&address)
            .ok()
            .filter(|url| url.scheme() == "rediss")
        {
            None => json!({"enabled":false,"verify":false,"mtls":false}),
            Some(url) => {
                let pairs = url
                    .query_pairs()
                    .filter(|(_, value)| !value.is_empty())
                    .collect::<std::collections::HashMap<_, _>>();
                let reqs = pairs
                    .get("ssl_cert_reqs")
                    .map(|value| value.trim().to_lowercase())
                    .unwrap_or_else(|| "required".into());
                json!({"enabled":true,"verify":!matches!(reqs.as_str(),"none"|"cert_none"),"mtls":pairs.contains_key("ssl_certfile")&&pairs.contains_key("ssl_keyfile")})
            }
        }
    } else if flag("REDIS_SSL", "false") {
        json!({"enabled":true,"verify":flag("REDIS_SSL_VERIFY","true"),"mtls":!env("REDIS_SSL_CERT","").is_empty()&&!env("REDIS_SSL_KEY","").is_empty()})
    } else {
        json!({"enabled":false,"verify":false,"mtls":false})
    };
    (redis, postgres)
}
async fn environment(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, false).await?;
    let system = auth.system_settings().await?;
    let disabled = !flag("DISPATCHARR_ENABLE_IP_LOOKUP", "true");
    let enabled = if disabled {
        Value::Bool(false)
    } else {
        system
            .get("enable_ip_lookup")
            .cloned()
            .unwrap_or(Value::Bool(true))
    };
    let mut result = auth.ip_environment(enabled).await?;
    let (redis, postgres) = tls_status();
    result["authenticated"] = json!(true);
    result["ip_lookup_env_disabled"] = json!(disabled);
    result["env_mode"] = json!(env("DISPATCHARR_ENV", "aio"));
    result["redis_tls"] = redis;
    result["postgres_tls"] = postgres;
    Ok(Json(result))
}
pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route("/api/accounts/initialize-superuser/", any(setup))
        .route("/api/accounts/auth/proxy-login/", post(proxy_login))
        .route("/api/core/version/", get(version))
        .route("/api/core/settings/env/", get(environment))
        .with_state(auth)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bootstrap_exact_ip_and_local_defaults_follow_source() {
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "172.31.2.3",
            "192.168.2.3",
            "::1",
            "fd00::1",
            "fe80::1",
        ] {
            assert!(setup_allowed(ip.parse().unwrap(), ""));
        }
        let ip = "203.0.113.3".parse().unwrap();
        assert!(!setup_allowed(ip, ""));
        assert!(setup_allowed(ip, " ::ffff:203.0.113.3 "));
        for value in ["invalid", "203.0.113.0/24", "203.0.113.4"] {
            assert!(!setup_allowed(ip, value));
        }
    }
    #[test]
    fn proxy_identity_header_has_source_ascii_length_rules() {
        for header in ["X-Forwarded-User", "Remote-User", "a", "0"] {
            assert!(valid_header(header));
        }
        for header in ["", "-Bad", "X_User", "é", "x user", "x:user"] {
            assert!(!valid_header(header));
        }
        assert!(!valid_header(&"x".repeat(65)));
    }
    #[tokio::test]
    async fn identity_requires_explicit_peer_trust_even_when_private_default_trusts_forwarding() {
        let make = |trusted: Option<&str>| {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgresql://unused:unused@localhost/unused")
                .unwrap();
            let mut config = crate::AuthConfig::new("fixture-only-key".into()).unwrap();
            config.trusted_proxies = trusted.map(str::to_owned);
            AuthService::new(crate::Store::from_pool(pool), config)
        };
        let peer = "192.168.2.3".parse().unwrap();
        assert!(!make(None).explicit_proxy_peer(peer));
        assert!(!make(Some("none")).explicit_proxy_peer(peer));
        assert!(!make(Some("")).explicit_proxy_peer(peer));
        assert!(make(Some("192.168.2.3")).explicit_proxy_peer(peer));
        assert!(make(Some("192.168.2.0/24")).explicit_proxy_peer(peer));
        assert!(!make(Some("192.168.3.0/24")).explicit_proxy_peer(peer));
        assert!(
            make(Some("192.168.2.3")).explicit_proxy_peer("::ffff:192.168.2.3".parse().unwrap())
        );
    }
}
