//! Shared XC authorization ported from original Dispatcharr helpers.
//! All original application credit remains with Dispatcharr contributors.
//! Native HTTP, actual-schema and original cache/event goldens are required.
use crate::{
    channels::{scope, truthy},
    django_pickle,
    notifications::decode_cache,
    AuthService, ChannelIdentity, NotificationCache, Store, StoreError, User,
};
use dispatcharr_contracts::PlaybackRequest;
use ipnet::IpNet;
use serde_json::{json, Map, Value};
use sqlx::{QueryBuilder, Row};
use std::{
    net::{IpAddr, Ipv4Addr},
    time::{SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;
fn failed() -> StoreError {
    StoreError::StatusBody(500, json!({"error":"XC authentication unavailable"}))
}
fn denied(status: u16, retry_after: Option<u64>) -> StoreError {
    StoreError::XcDenied {
        status,
        retry_after,
    }
}
pub(crate) fn failure_body(status: u16) -> Value {
    json!({"error":match status{403=>"Forbidden",429=>"Too many attempts",_=>"Unauthorized"}})
}
fn now() -> Result<f64, StoreError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs_f64())
        .map_err(|_| failed())
}
fn mapping(value: &Value) -> Result<Option<&Map<String, Value>>, StoreError> {
    if !truthy(Some(value)) {
        Ok(None)
    } else {
        value.as_object().map(Some).ok_or_else(failed)
    }
}
fn network(raw: &str, strict: bool) -> Result<IpNet, StoreError> {
    if raw.trim() != raw || raw.is_empty() {
        return Err(failed());
    }
    let (address, prefix) = raw
        .split_once('/')
        .map_or((raw, None), |(address, prefix)| (address, Some(prefix)));
    let address: IpAddr = address.parse().map_err(|_| failed())?;
    let prefix = if let Some(prefix) = prefix {
        if address.is_ipv4() && prefix.contains('.') {
            let mask: u32 = prefix
                .parse::<Ipv4Addr>()
                .map(u32::from)
                .map_err(|_| failed())?;
            if mask.leading_ones() + mask.trailing_zeros() == 32 {
                mask.leading_ones() as u8
            } else {
                let inverse = !mask;
                if inverse.leading_ones() + inverse.trailing_zeros() != 32 {
                    return Err(failed());
                }
                inverse.leading_ones() as u8
            }
        } else {
            if prefix.is_empty() || !prefix.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(failed());
            }
            prefix.parse::<u8>().map_err(|_| failed())?
        }
    } else if address.is_ipv4() {
        32
    } else {
        128
    };
    let network = IpNet::new(address, prefix).map_err(|_| failed())?;
    if strict && network.network() != address {
        return Err(failed());
    }
    Ok(network)
}
pub(crate) fn system_allowed(policy: &Value, key: &str, ip: IpAddr) -> Result<bool, StoreError> {
    const LOCAL: &str =
        "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,::1/128,fc00::/7,fe80::/10";
    let raw = match policy.get(key) {
        Some(value) => value.as_str().ok_or_else(failed)?,
        None if key == "M3U_EPG" => LOCAL,
        None => "0.0.0.0/0,::/0",
    };
    // Global source policy intentionally does not trim or ignore bad CIDRs.
    for part in raw.split(',') {
        if network(part, true)?.contains(&ip) {
            return Ok(true);
        }
    }
    Ok(false)
}
pub(crate) fn user_allowed(user: &User, key: &str, ip: IpAddr) -> Result<bool, StoreError> {
    let props = mapping(&user.custom_properties)?;
    let allowed = props.and_then(|props| props.get("allowed_networks"));
    let Some(allowed) = allowed else {
        return Ok(true);
    };
    let allowed = allowed.as_object().ok_or_else(failed)?;
    let raw = allowed.get(key);
    if !truthy(raw) {
        return Ok(true);
    }
    let raw = raw.and_then(Value::as_str).ok_or_else(failed)?;
    for part in raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if network(part, false).is_ok_and(|network| network.contains(&ip)) {
            return Ok(true);
        }
    }
    Ok(false)
}
fn password_matches(user: &User, password: &str) -> Result<bool, StoreError> {
    let Some(expected) = mapping(&user.custom_properties)?
        .and_then(|props| props.get("xc_password"))
        .filter(|value| truthy(Some(value)))
    else {
        return Ok(false);
    };
    let expected = match expected {
        Value::String(value) if value.len() <= 1024 * 1024 => value.clone(),
        Value::String(_) => return Err(failed()),
        value => crate::python_action::collection_text(value, 1024 * 1024).ok_or_else(failed)?,
    };
    Ok(bool::from(expected.as_bytes().ct_eq(password.as_bytes())))
}
fn history(raw: Option<&[u8]>, at: f64) -> Result<Option<Vec<f64>>, StoreError> {
    let Some(raw) = raw else {
        return Ok(Some(vec![]));
    };
    let value = decode_cache(raw).ok_or_else(failed)?;
    if value.is_null() {
        return Ok(None);
    }
    let values = value.as_array().ok_or_else(failed)?;
    if values.len() > 1024 {
        return Err(failed());
    }
    let mut history = values
        .iter()
        .map(|value| match value {
            Value::Bool(value) => Ok(if *value { 1.0 } else { 0.0 }),
            Value::Number(value) => value
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(failed),
            _ => Err(failed()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    while history.last().is_some_and(|last| *last <= at - 60.0) {
        history.pop();
    }
    Ok(Some(history))
}
async fn limited(cache: &NotificationCache, key: &str) -> Result<Option<Option<u64>>, StoreError> {
    let raw = cache.get(key).await.map_err(|_| failed())?;
    let at = now()?;
    let Some(history) = history(raw.as_deref(), at)? else {
        return Ok(None);
    };
    if history.len() < 10 {
        return Ok(None);
    }
    let wait = retry_after(&history, at);
    Ok(Some(wait))
}
fn retry_after(history: &[f64], at: f64) -> Option<u64> {
    if history.len() == 10 {
        Some((60.0 - (at - history[9])).floor().max(1.0) as u64)
    } else {
        None
    }
}

async fn record_failure(cache: &NotificationCache, key: &str) -> Result<bool, StoreError> {
    for _ in 0..16 {
        let raw = cache.get(key).await.map_err(|_| failed())?;
        let at = now()?;
        let Some(mut history) = history(raw.as_deref(), at)? else {
            return Ok(false);
        };
        let was_limited = history.len() >= 10;
        history.insert(0, at);
        let crossed = !was_limited && history.len() >= 10;
        let encoded = django_pickle::encode(&json!(history)).ok_or_else(failed)?;
        // Fences Rust failure writers while retaining Django's exact cache key
        // and primitive-list format. Legacy Django read/modify/write is not CAS.
        let mut cmd = redis::cmd("EVAL");
        cmd.arg("local old=redis.call('GET',KEYS[1]); if (ARGV[1]=='0' and old) or (ARGV[1]=='1' and old~=ARGV[2]) then return 0 end redis.call('SET',KEYS[1],ARGV[3],'EX',60); return 1").arg(1).arg(key).arg(if raw.is_some(){"1"}else{"0"}).arg(raw.as_deref().unwrap_or(&[])).arg(encoded);
        let changed: i64 = cache.command(cmd).await.map_err(|_| failed())?;
        if changed == 1 {
            return Ok(crossed);
        }
    }
    Err(failed())
}
async fn settings(
    auth: &AuthService,
    cache: &NotificationCache,
    key: &str,
) -> Result<Value, StoreError> {
    cache
        .settings_group(key, || auth.store.source_group_value(key))
        .await
        .map_err(|_| failed())
}
impl AuthService {
    /// Exact XC credentials, independent of native active-JWT authentication.
    /// No Django-password fallback, enable_xc flag, or active-user restriction.
    pub async fn authorize_xc(
        &self,
        request: &PlaybackRequest,
        username: &str,
        password: &str,
        network_key: &str,
    ) -> Result<User, StoreError> {
        let cache = self
            .notification_cache
            .as_ref()
            .ok_or(StoreError::Unsupported(
                "XC authentication requires the original shared Redis cache.",
            ))?;
        let ip = self.client_ip(request);
        let policy = settings(self, cache, "network_access").await?;
        if !system_allowed(&policy, network_key, ip)? {
            return Err(denied(403, None));
        }
        let key = format!(":1:throttle_xc_auth_{ip}");
        if let Some(wait) = limited(cache, &key).await? {
            return Err(denied(429, wait));
        }
        let user = if username.is_empty() || password.is_empty() {
            None
        } else {
            self.store.user_by_username(username).await?
        };
        if let Some(user) = user {
            // Source repeats the system check after lookup before user/password.
            let policy = settings(self, cache, "network_access").await?;
            if !system_allowed(&policy, network_key, ip)? || !user_allowed(&user, network_key, ip)?
            {
                return Err(denied(403, None));
            }
            if password_matches(&user, password)? {
                return Ok(user);
            }
        }
        if record_failure(cache, &key).await? {
            self.xc_lockout_event(request, username, cache).await;
            self.xc_lockout_notice(cache).await;
        }
        Err(denied(401, None))
    }
    async fn xc_lockout_event(
        &self,
        request: &PlaybackRequest,
        username: &str,
        cache: &NotificationCache,
    ) {
        let details = json!({"user":if username.is_empty(){"unknown"}else{username},"client_ip":self.client_ip(request).to_string(),"user_agent":request.header("user-agent").unwrap_or("unknown"),"reason":"XC auth rate limited"});
        if sqlx::query("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details) VALUES('login_failed',now(),NULL,NULL,$1)").bind(details).execute(self.store.pool()).await.is_err(){tracing::warn!("XC lockout event storage failed");return}
        // Connect/plugin dispatch is an independently tracked M8 bridge.
        let setting = settings(self, cache, "system_settings")
            .await
            .unwrap_or(Value::Null);
        let value = setting
            .get("max_system_events")
            .filter(|value| truthy(Some(value)))
            .cloned()
            .unwrap_or(json!(100));
        let max = match value {
            Value::Bool(value) => Some(i64::from(value)),
            Value::Number(value) if value.is_f64() => value
                .as_f64()
                .filter(|v| v.is_finite() && *v >= i64::MIN as f64 && *v < i64::MAX as f64)
                .map(|v| v.trunc() as i64),
            value => crate::channel_memberships::primary_key(&value).unwrap_or(Some(100)),
        };
        if let Some(max) = max.filter(|max| *max >= 0) {
            let result=async{
                let count:i64=sqlx::query_scalar("SELECT count(*) FROM core_systemevent").fetch_one(self.store.pool()).await?;
                if count>max{let cutoff:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemevent ORDER BY timestamp DESC LIMIT 1 OFFSET $1").bind(max).fetch_optional(self.store.pool()).await?;if let Some(cutoff)=cutoff{sqlx::query("DELETE FROM core_systemevent WHERE id<$1").bind(cutoff).execute(self.store.pool()).await?;}}
                Ok::<_,sqlx::Error>(())
            }.await;
            if result.is_err() {
                tracing::warn!("XC lockout retention failed");
            }
        }
    }
    async fn xc_lockout_notice(&self, cache: &NotificationCache) {
        let mut claim = redis::cmd("SET");
        claim
            .arg(":1:xc_auth_lockout_notice_cooldown")
            .arg([0x80u8, 4, 0x88, b'.'].as_slice())
            .arg("NX")
            .arg("EX")
            .arg(3600);
        let claimed: Result<Option<String>, _> = cache.command(claim).await;
        if !matches!(claimed, Ok(Some(_))) {
            return;
        }
        let result=async{
            // Source commits the collector deletion before attempting creation.
            let mut tx=self.store.pool().begin().await?;
            sqlx::query("DELETE FROM core_notificationdismissal WHERE notification_id IN(SELECT id FROM core_systemnotification WHERE notification_key='xc-auth-lockout')").execute(&mut *tx).await?;
            sqlx::query("DELETE FROM core_systemnotification WHERE notification_key='xc-auth-lockout'").execute(&mut *tx).await?;tx.commit().await?;
            let row=sqlx::query("INSERT INTO core_systemnotification(notification_key,notification_type,priority,source,title,message,action_data,is_active,admin_only,expires_at,created_at,updated_at) VALUES('xc-auth-lockout','warning','high','system','XC authentication rate limit triggered','One or more clients were temporarily locked out after too many failed XC credential attempts. Check System Events on the Stats page for IP addresses and details.',$1,true,true,NULL,now(),now()) RETURNING id::bigint AS id,created_at")
                .bind(json!({"action_url":"/stats","action_text":"Open Stats"})).fetch_one(self.store.pool()).await?;
            Ok::<_,sqlx::Error>(row)
        }.await;
        match result {
            Ok(row) => {
                let created: chrono::DateTime<chrono::Utc> = match row.try_get("created_at") {
                    Ok(value) => value,
                    Err(_) => return,
                };
                let id: i64 = match row.try_get("id") {
                    Ok(value) => value,
                    Err(_) => return,
                };
                self.xc_publish_notice(id, created).await;
            }
            Err(_) => {
                let mut cmd = redis::cmd("DEL");
                cmd.arg(":1:xc_auth_lockout_notice_cooldown");
                let _: Result<i64, _> = cache.command(cmd).await;
                tracing::warn!("XC lockout inbox storage failed");
            }
        }
    }
    async fn xc_publish_notice(&self, id: i64, created: chrono::DateTime<chrono::Utc>) {
        let Some(sink) = self.event_sink.as_ref() else {
            tracing::warn!("XC inbox created; M7 event sink is not configured");
            return;
        };
        let created = created.to_rfc3339_opts(
            if created.timestamp_subsec_micros() == 0 {
                chrono::SecondsFormat::Secs
            } else {
                chrono::SecondsFormat::Micros
            },
            false,
        );
        let message = json!({"type":"update","data":{"type":"system_notification","notification":{"id":id,"notification_key":"xc-auth-lockout","notification_type":"warning","priority":"high","title":"XC authentication rate limit triggered","message":"One or more clients were temporarily locked out after too many failed XC credential attempts. Check System Events on the Stats page for IP addresses and details.","action_data":{"action_url":"/stats","action_text":"Open Stats"},"is_active":true,"admin_only":true,"created_at":created}}});
        if sink.send("updates", message).await.is_err() {
            tracing::warn!("XC lockout websocket publication failed");
        }
    }
}
impl Store {
    /// Source get_channel_for_user(user,id=...) for already-XC-authorized users.
    /// Inactive users remain eligible under the original XC contract.
    pub async fn channel_for_xc_user_by_id(
        &self,
        user: &User,
        pk: i64,
    ) -> Result<Option<ChannelIdentity>, StoreError> {
        let mut query=QueryBuilder::new("SELECT c.id::bigint AS id,c.uuid,c.name,COALESCE(o.name,c.name) AS effective_name FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE c.id=");
        query.push_bind(pk);
        scope(&mut query, user);
        query
            .build()
            .fetch_optional(self.pool())
            .await?
            .map(|row| {
                Ok(ChannelIdentity {
                    id: row.try_get("id")?,
                    uuid: row.try_get("uuid")?,
                    name: row.try_get("name")?,
                    effective_name: row.try_get("effective_name")?,
                })
            })
            .transpose()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_global_networks_are_strict_but_user_lists_tighten_leniently() {
        let ip = "192.168.1.4".parse().unwrap();
        assert!(
            system_allowed(&json!({"XC_API":"192.168.1.0/255.255.255.0"}), "XC_API", ip).unwrap()
        );
        assert!(system_allowed(&json!({"XC_API":"192.168.1.1/24"}), "XC_API", ip).is_err());
        assert!(system_allowed(&json!({"XC_API":" 0.0.0.0/0"}), "XC_API", ip).is_err());
        assert!(network("192.168.1.2/0.0.0.255", false)
            .unwrap()
            .contains(&ip));
    }
    #[test]
    fn failure_history_null_expiry_and_failed_only_trip_shape() {
        assert_eq!(history(None, 100.0).unwrap(), Some(vec![]));
        assert_eq!(history(Some(&[0x80, 4, b'N', b'.']), 100.0).unwrap(), None);
        let raw = django_pickle::encode(&json!([90.5, 80, 40])).unwrap();
        assert_eq!(history(Some(&raw), 100.0).unwrap(), Some(vec![90.5, 80.0]));
    }
    #[test]
    fn retry_headers_floor_and_clamp_original_wait() {
        assert_eq!(retry_after(&[100.0; 10], 147.7), Some(12));
        assert_eq!(retry_after(&[100.0; 10], 159.9), Some(1));
        assert_eq!(retry_after(&[100.0; 11], 147.7), None);
    }
}
