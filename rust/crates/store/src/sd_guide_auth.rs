//! Source Schedules Direct authentication on the SAME retained task SQL/cache.
//! Original Dispatcharr/Requests/Django authors retain all algorithm credit.
use super::sd_guide_http::{self as http, Owner, Reply};
use crate::{AuthService, StoreError, TaskSqlSession};
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use sha1::{Digest, Sha1};
use sha2::Sha256;
use sqlx::Row;
use std::time::Duration;
const LOCK_KEYS: [&str; 5] = [
    "sd_auth_lockout",
    "sd_auth_lockout_code",
    "sd_auth_lockout_reason",
    "sd_auth_lockout_until",
    "sd_auth_lockout_credential_fp",
];
const RETRY:&str="Not retrying Schedules Direct authentication until the username or password is updated, or the cooldown expires.";
const DEBUG_REJECTED:&str="Schedules Direct rejected the debug routing header (code 2055). Extra Schedules Direct Debugging has been turned off. Retry without it unless Schedules Direct support asked you to enable it.";
#[derive(Debug)]
pub(super) enum Error {
    Store(StoreError),
    Http(http::Error),
    Capacity,
    Unsupported,
}
impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}
impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Self::Store(StoreError::Database(e))
    }
}
impl From<http::Error> for Error {
    fn from(e: http::Error) -> Self {
        Self::Http(e)
    }
}
/// Owns no connection/client pool: caller supplies its admitted SQL/cache owner.
pub(super) struct Source {
    pub id: i64,
    pub name: String,
    pub username: String,
    pub password: String,
    pub properties: Map<String, Value>,
    pub session: TaskSqlSession,
    pub(super) auth: AuthService,
    pub(super) owner: Owner,
}
pub(super) struct AuthResult {
    pub token: Option<String>,
    pub message: String,
    pub soft: bool,
}
impl AuthResult {
    fn failure(message: String, code: Option<i64>, _lockout: bool) -> Self {
        Self {
            token: None,
            message,
            soft: code.is_some_and(|n| matches!(n, 3000 | 3001)),
        }
    }
    fn success(token: String, _expires: Option<f64>) -> Self {
        Self {
            token: Some(token),
            message: String::new(),
            soft: false,
        }
    }
}
fn now() -> f64 {
    Utc::now().timestamp_micros() as f64 / 1_000_000.0
}
fn strip(s: &str) -> &str {
    s.trim_matches(|c: char| c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}'))
}
fn number(v: &Value) -> Option<f64> {
    match v {
        Value::Bool(v) => Some(f64::from(u8::from(*v))),
        Value::Number(n) => n.as_f64().filter(|v| v.is_finite()),
        _ => None,
    }
}
fn fingerprint(user: &str, password: &str) -> String {
    format!(
        "{:x}",
        Sha256::digest(format!("{}\0{}", strip(user), strip(password)).as_bytes())
    )
}
fn lock_code(code: i64) -> bool {
    matches!(
        code,
        3000 | 3001 | 4001 | 4002 | 4003 | 4004 | 4005 | 4007 | 4008 | 4009 | 4010
    )
}
fn cooldown(code: i64) -> i64 {
    match code {
        4004 => 900,
        3000 | 3001 => 3600,
        _ => 86400,
    }
}
fn message(code: i64, provider: Option<&str>) -> String {
    match code{
 3000=>"Schedules Direct is offline for maintenance. Do not retry for at least 1 hour.".into(),
 3001=>"Schedules Direct is busy. Stop requesting and retry later.".into(),
 4001=>"Schedules Direct: account has expired. Please renew your subscription at schedulesdirect.org.".into(),
 4002=>"Schedules Direct: invalid password hash. Check that credentials are stored correctly.".into(),
 4003=>"Schedules Direct: invalid username or password.".into(),
 4004=>"Schedules Direct: account locked due to too many failed login attempts. Try again in 15 minutes.".into(),
 4005=>"Schedules Direct: JSON API access is disabled for this account. Contact Schedules Direct support.".into(),
 4007=>"Schedules Direct: this application is not authorized. Please contact the Dispatcharr maintainers.".into(),
 4008=>"Schedules Direct: account is inactive. Please log in to schedulesdirect.org to reactivate.".into(),
 4009=>"Schedules Direct: too many login attempts in 24 hours. Token is valid for 24 hours. Check for misconfiguration.".into(),
 4010=>"Schedules Direct: too many unique IP addresses in 24 hours. Avoid VPNs or multiple locations, or contact SD support to raise the limit.".into(),
 _=>match provider.filter(|s|!s.is_empty()){Some(s)=>format!("Schedules Direct authentication failed (code {code}): {s}"),None=>format!("Schedules Direct authentication failed (code {code}).")}}
}
pub(super) enum Read {
    Missing,
    Other,
    Source(Box<Source>),
}
impl Source {
    pub async fn read(
        id: i64,
        session: TaskSqlSession,
        auth: AuthService,
        owner: Owner,
    ) -> Result<Read, Error> {
        let row=session.run(move|c|Box::pin(async move{sqlx::query("SELECT source_type,name,CASE WHEN COALESCE(octet_length(username),0)<=65536 THEN username END AS username,CASE WHEN COALESCE(octet_length(password),0)<=65536 THEN password END AS password,CASE WHEN octet_length(custom_properties::text)<=65536 THEN custom_properties END AS custom_properties,COALESCE(octet_length(username),0)<=65536 AND COALESCE(octet_length(password),0)<=65536 AND COALESCE(octet_length(custom_properties::text),0)<=65536 AS fits FROM epg_epgsource WHERE id=$1").bind(id).fetch_optional(c).await})).await??;
        let Some(row) = row else {
            return Ok(Read::Missing);
        };
        if row.try_get::<String, _>("source_type")? != "schedules_direct" {
            return Ok(Read::Other);
        }
        if !row.try_get::<bool, _>("fits")? {
            return Err(Error::Capacity);
        }
        let properties = match row.try_get::<Option<Value>, _>("custom_properties")? {
            None | Some(Value::Null) => Map::new(),
            Some(Value::Object(v)) => v,
            _ => return Err(Error::Unsupported),
        };
        Ok(Read::Source(Box::new(Self {
            id,
            name: row.try_get("name")?,
            username: strip(
                &row.try_get::<Option<String>, _>("username")?
                    .unwrap_or_default(),
            )
            .to_owned(),
            password: strip(
                &row.try_get::<Option<String>, _>("password")?
                    .unwrap_or_default(),
            )
            .to_owned(),
            properties,
            session,
            auth: auth.with_retained_scope(owner.clone()),
            owner,
        })))
    }
    fn debug(&self) -> bool {
        self.properties
            .get("sd_extra_debugging")
            .is_some_and(crate::xc_live_provider::truthy)
    }
    async fn save_properties(&self) -> Result<(), Error> {
        let id = self.id;
        let cp = Value::Object(self.properties.clone());
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query("UPDATE epg_epgsource SET custom_properties=$1 WHERE id=$2")
                        .bind(cp)
                        .bind(id)
                        .execute(c)
                        .await
                        .map(|_| ())
                        .map_err(StoreError::from)
                })
            })
            .await??;
        Ok(())
    }
    fn key(&self) -> String {
        format!(":1:sd:token:{}", self.id)
    }
    async fn clear_token(&self) {
        if let Some(cache) = &self.auth.notification_cache {
            let mut c = redis::cmd("DEL");
            c.arg(self.key());
            let _ = cache.command::<i64>(c).await;
        }
    }
    async fn cached(&self) -> Option<String> {
        let cache = self.auth.notification_cache.as_ref()?;
        let raw = cache.get(&self.key()).await.ok()??;
        let value = crate::django_pickle::decode(&raw)?;
        let obj = value.as_object()?;
        let token = obj.get("token")?.as_str()?.to_owned();
        if token.is_empty() || token.len() > 65536 {
            return None;
        }
        let expires = number(obj.get("expires")?)?;
        if obj.get("fp")?.as_str()? != fingerprint(&self.username, &self.password) {
            self.clear_token().await;
            return None;
        }
        (now() < expires - 60.0).then_some(token)
    }
    async fn clear_lock(&mut self) -> Result<(), Error> {
        let changed = LOCK_KEYS.iter().fold(false, |found, key| {
            self.properties.remove(*key).is_some() || found
        });
        if changed {
            self.save_properties().await?;
        }
        Ok(())
    }
    async fn disable_debug(&mut self, reply: &Reply) -> Result<bool, Error> {
        let rejected = reply
            .value
            .as_ref()
            .and_then(|v| v.get("code"))
            .and_then(number)
            == Some(2055.0);
        if !rejected {
            return Ok(false);
        }
        if self.debug() {
            self.properties
                .insert("sd_extra_debugging".into(), json!(false));
            self.save_properties().await?;
        }
        Ok(true)
    }
    pub async fn obtain(&mut self) -> Result<AuthResult, Error> {
        if self.username.is_empty() || self.password.is_empty() {
            return Ok(AuthResult::failure(
                "Username and password are required.".into(),
                None,
                false,
            ));
        }
        if self
            .properties
            .get("sd_auth_lockout")
            .is_some_and(crate::xc_live_provider::truthy)
        {
            let until = self
                .properties
                .get("sd_auth_lockout_until")
                .and_then(Value::as_str)
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok());
            let same = self
                .properties
                .get("sd_auth_lockout_credential_fp")
                .and_then(Value::as_str)
                == Some(fingerprint(&self.username, &self.password).as_str());
            if until.is_some_and(|until| until.with_timezone(&Utc) > Utc::now()) && same {
                let code = self
                    .properties
                    .get("sd_auth_lockout_code")
                    .map(http::integer_code)
                    .transpose()?
                    .flatten();
                let reason = match self.properties.get("sd_auth_lockout_reason") {
                    Some(Value::String(s)) if !s.is_empty() => s.as_str(),
                    None | Some(Value::Null) | Some(Value::String(_)) => {
                        "Schedules Direct authentication is temporarily blocked."
                    }
                    _ => return Err(Error::Unsupported),
                };
                return Ok(AuthResult::failure(format!("{reason} {RETRY}"), code, true));
            }
            self.clear_lock().await?;
        }
        if let Some(token) = self.cached().await {
            return Ok(AuthResult::success(token, None));
        }
        let payload = json!({"username":self.username,"password":format!("{:x}",Sha1::digest(self.password.as_bytes()))});
        let mut reply = http::request(
            reqwest::Method::POST,
            "/token",
            None,
            self.debug(),
            Some(&payload),
            Duration::from_secs(30),
            self.owner.clone(),
        )
        .await?;
        if self.disable_debug(&reply).await? {
            reply = http::request(
                reqwest::Method::POST,
                "/token",
                None,
                self.debug(),
                Some(&payload),
                Duration::from_secs(30),
                self.owner.clone(),
            )
            .await?;
            if self.disable_debug(&reply).await? {
                return Ok(AuthResult::failure(
                    DEBUG_REJECTED.into(),
                    Some(2055),
                    false,
                ));
            }
        }
        let code = reply
            .value
            .as_ref()
            .and_then(|v| v.get("code"))
            .map(http::integer_code)
            .transpose()?
            .flatten()
            .unwrap_or(0);
        if code != 0 {
            let provider = match reply.value.as_ref().and_then(|v| v.get("message")) {
                None => Some("Unknown error"),
                Some(Value::String(v)) => Some(v.as_str()),
                Some(Value::Null) => None,
                _ => return Err(Error::Unsupported),
            };
            let mut reason = message(code, provider);
            let locked = lock_code(code);
            if locked {
                self.properties
                    .insert("sd_auth_lockout".into(), json!(true));
                self.properties
                    .insert("sd_auth_lockout_code".into(), json!(code));
                self.properties
                    .insert("sd_auth_lockout_reason".into(), json!(reason));
                self.properties.insert(
                    "sd_auth_lockout_until".into(),
                    json!((Utc::now() + chrono::Duration::seconds(cooldown(code)))
                        .to_rfc3339_opts(chrono::SecondsFormat::Micros, false)),
                );
                self.properties.insert(
                    "sd_auth_lockout_credential_fp".into(),
                    json!(fingerprint(&self.username, &self.password)),
                );
                self.save_properties().await?;
                self.clear_token().await;
                reason = format!("{reason} {RETRY}");
            }
            return Ok(AuthResult::failure(reason, Some(code), locked));
        }
        if let Some(error) = reply.request_error() {
            return Ok(AuthResult::failure(
                format!("Network error authenticating with Schedules Direct: {error}"),
                None,
                false,
            ));
        }
        let token = match reply.value.as_ref().and_then(|v| v.get("token")) {
            None | Some(Value::Null) => None,
            Some(Value::String(v)) if v.is_empty() => None,
            Some(Value::String(v)) if v.len() <= 65536 => Some(v.clone()),
            Some(Value::String(_)) => return Err(Error::Capacity),
            _ => return Err(Error::Unsupported),
        };
        let Some(token) = token else {
            return Ok(AuthResult::failure(
                "Schedules Direct returned no token.".into(),
                None,
                false,
            ));
        };
        self.clear_lock().await?;
        let expires = match reply.value.as_ref().and_then(|v| v.get("tokenExpires")) {
            Some(Value::Bool(value)) => f64::from(u8::from(*value)),
            Some(Value::Number(value)) => value
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or(Error::Unsupported)?,
            _ => now() + 86400.0,
        };
        let ttl = (expires - now() - 60.0).trunc();
        if ttl >= 1.0 && ttl <= u64::MAX as f64 {
            if let Some(cache) = &self.auth.notification_cache {
                let _=cache.set_for(&self.key(),&json!({"token":token,"expires":expires,"fp":fingerprint(&self.username,&self.password)}),ttl as u64).await;
            }
        }
        Ok(AuthResult::success(token, Some(expires)))
    }
    pub async fn authorized(
        &mut self,
        method: reqwest::Method,
        path: &str,
        token: &mut String,
        payload: Option<&Value>,
        timeout: Duration,
    ) -> Result<Reply, Error> {
        let mut reply = http::request(
            method.clone(),
            path,
            Some(token),
            self.debug(),
            payload,
            timeout,
            self.owner.clone(),
        )
        .await?;
        if self.disable_debug(&reply).await? {
            reply = http::request(
                method.clone(),
                path,
                Some(token),
                self.debug(),
                payload,
                timeout,
                self.owner.clone(),
            )
            .await?;
        }
        if !matches!(reply.status, 401 | 403) {
            return Ok(reply);
        }
        self.clear_token().await;
        let fresh = self.obtain().await?;
        let Some(fresh) = fresh.token else {
            return Ok(reply);
        };
        *token = fresh;
        reply = http::request(
            method.clone(),
            path,
            Some(token),
            self.debug(),
            payload,
            timeout,
            self.owner.clone(),
        )
        .await?;
        if self.disable_debug(&reply).await? {
            reply = http::request(
                method,
                path,
                Some(token),
                self.debug(),
                payload,
                timeout,
                self.owner.clone(),
            )
            .await?;
        }
        Ok(reply)
    }
}
#[cfg(test)]
mod controls {
    use super::*;
    #[test]
    fn credentials_hash_strip_and_cooldown_follow_pinned_source() {
        assert_eq!(strip("\u{1c} user\u{1f}"), "user");
        assert_eq!(fingerprint(" user ", " pass "), fingerprint("user", "pass"));
        assert_ne!(fingerprint("ab", "c"), fingerprint("a", "bc"));
        assert_eq!(cooldown(4004), 900);
        assert_eq!(cooldown(3001), 3600);
        assert_eq!(cooldown(4003), 86400);
        assert!(!lock_code(4006));
        assert_eq!(number(&json!(true)), Some(1.0));
        assert_eq!(number(&json!("1")), None);
    }
}
