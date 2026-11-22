//! Original environment IP cache/background worker, ported from core/api_views.py.
//! Original Dispatcharr authors retain full application credit (ATTRIBUTION.md).
use crate::{
    channels::truthy, notifications::decode_cache, AuthService, NotificationCache, StoreError,
};
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_upstream_http::{body, Attempt};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::{
    net::{IpAddr, SocketAddr, UdpSocket},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
const CACHE: &str = ":1:dispatcharr:ip_lookup_result";
const LOCK: &str = ":1:dispatcharr:ip_lookup_lock";
const MAX_JSON: usize = 64 * 1024;
#[derive(Clone, Debug)]
pub struct IpLookupConfig {
    pub ipify_url: String,
    pub geo_url_template: String,
    pub fallback_geo_url: String,
    pub local_probe: SocketAddr,
}
impl Default for IpLookupConfig {
    fn default() -> Self {
        Self {
            ipify_url: "https://api64.ipify.org?format=json".into(),
            geo_url_template: "https://ipapi.co/{ip}/json/".into(),
            fallback_geo_url: "http://ip-api.com/json/".into(),
            local_probe: "203.0.113.1:80".parse().expect("source local probe"),
        }
    }
}
#[derive(Clone)]
pub(crate) struct IpLookup {
    config: Arc<IpLookupConfig>,
    running: Arc<AtomicBool>,
}
impl IpLookup {
    pub(crate) fn new(config: IpLookupConfig) -> Self {
        Self {
            config: Arc::new(config),
            running: Arc::new(AtomicBool::new(false)),
        }
    }
}
struct Running(Arc<AtomicBool>);
impl Drop for Running {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release)
    }
}
fn failed() -> StoreError {
    StoreError::StatusBody(500, json!({"error":"IP lookup cache unavailable"}))
}
fn now() -> Result<f64, StoreError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_secs_f64())
        .map_err(|_| failed())
}
fn numeric(value: &Value) -> Option<f64> {
    match value {
        Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        Value::Number(value) => value.as_f64().filter(|value| value.is_finite()),
        _ => None,
    }
}
fn valid_ip(value: &Value) -> bool {
    match value {
        Value::String(value) => value.parse::<IpAddr>().is_ok(),
        Value::Bool(_) => true,
        Value::Number(value) if !value.is_f64() => value.to_string().parse::<u128>().is_ok(),
        _ => false,
    }
}
fn ip_text(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Bool(value) => Some(if *value { "True" } else { "False" }.into()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}
// Each requests.get uses a separate Session/cookie jar, retaining cookies only
// across that one request's redirect chain. JSON bytes and wall time are bounded.
async fn get(url: &str) -> Result<(u16, Vec<u8>), ()> {
    let attempt = Attempt::new(Duration::from_secs(5), Duration::from_secs(5)).map_err(|_| ())?;
    let response = attempt
        .request(url, "", &[])
        .map_err(|_| ())?
        .send()
        .await
        .map_err(|_| ())?;
    let status = response.status().as_u16();
    let mut stream = body(response).map_err(|_| ())?;
    let mut bytes = Vec::new();
    while let Some(frame) = stream.next().await {
        let frame = frame.map_err(|_| ())?;
        if bytes.len().saturating_add(frame.len()) > MAX_JSON {
            return Err(());
        }
        bytes.extend_from_slice(&frame);
    }
    Ok((status, bytes))
}
async fn lookup(config: &IpLookupConfig) -> Result<Value, ()> {
    let mut public = Value::Null;
    if let Ok((status, bytes)) = get(&config.ipify_url).await {
        if status < 400 {
            if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                let object = value.as_object().ok_or(())?;
                public = object.get("ip").cloned().unwrap_or(Value::Null);
            }
        }
    }
    let local = UdpSocket::bind("0.0.0.0:0")
        .and_then(|socket| {
            socket.connect(config.local_probe)?;
            socket.local_addr().map(|address| address.ip().to_string())
        })
        .ok();
    if !valid_ip(&public) {
        public = Value::Null;
    }
    let mut result = json!({"public_ip":public,"local_ip":local,"country_code":null,"country_name":null,"city":null});
    if truthy(result.get("public_ip")) {
        let target = config
            .geo_url_template
            .replace("{ip}", &ip_text(&result["public_ip"]).ok_or(())?);
        // Source falls back only on a non-200 response, never a primary
        // transport/JSON exception. Both errors leave the remaining fields null.
        if let Ok((status, geo)) = get(&target).await {
            let geo = if status == 200 {
                serde_json::from_slice::<Value>(&geo)
                    .ok()
                    .map(|value| (value, false))
            } else {
                get(&config.fallback_geo_url)
                    .await
                    .ok()
                    .filter(|(status, _)| *status == 200)
                    .and_then(|(_, value)| serde_json::from_slice::<Value>(&value).ok())
                    .map(|value| (value, true))
            };
            if let Some((geo, fallback)) = geo {
                if let Some(geo) = geo.as_object() {
                    for (field, key) in [
                        (
                            "country_code",
                            if fallback {
                                "countryCode"
                            } else {
                                "country_code"
                            },
                        ),
                        (
                            "country_name",
                            if fallback { "country" } else { "country_name" },
                        ),
                        ("city", "city"),
                    ] {
                        result[field] = geo.get(key).cloned().unwrap_or(Value::Null);
                    }
                }
            }
        }
    }
    Ok(result)
}
impl AuthService {
    pub fn with_ip_lookup_config(mut self, config: IpLookupConfig) -> Self {
        self.ip_lookup = IpLookup::new(config);
        self
    }
    pub(crate) async fn ip_environment(&self, enabled: Value) -> Result<Value, StoreError> {
        self.ip_lookup
            .environment(
                enabled,
                self.notification_cache.as_ref(),
                None,
                self.event_sink.as_ref(),
            )
            .await
    }
}

impl IpLookup {
    pub(crate) async fn environment(
        &self,
        enabled: Value,
        cache: Option<&NotificationCache>,
        request_cache: Option<&NotificationCache>,
        event_sink: Option<&Arc<dyn UpdateEventSink>>,
    ) -> Result<Value, StoreError> {
        let mut result = json!({"public_ip":null,"local_ip":null,"country_code":null,"country_name":null,"city":null,"ip_lookup_enabled":enabled,"ip_lookup_pending":false});
        if !truthy(Some(&enabled)) {
            return Ok(result);
        }
        let cache = cache.ok_or(StoreError::Unsupported(
            "Enabled IP lookup requires the original shared Redis cache.",
        ))?;
        // Copied SQLite retains its request permit through the foreground
        // cache operations. Only the process-wide cache and event owner
        // enter a separately coalesced background lookup.
        let request_cache = request_cache.unwrap_or(cache);
        let cached = request_cache
            .get(CACHE)
            .await
            .map_err(|_| failed())?
            .map(|raw| decode_cache(&raw).ok_or_else(failed))
            .transpose()?
            .filter(|value| !value.is_null());
        let stale = if let Some(cached) = cached {
            let cached = cached.as_object().ok_or_else(failed)?;
            for key in [
                "public_ip",
                "local_ip",
                "country_code",
                "country_name",
                "city",
            ] {
                result[key] = cached.get(key).cloned().unwrap_or(Value::Null);
            }
            now()? - numeric(cached.get("verified_at").unwrap_or(&json!(0))).ok_or_else(failed)?
                > 60.0
        } else {
            result["ip_lookup_pending"] = json!(true);
            true
        };
        if stale {
            let mut cmd = redis::cmd("SET");
            cmd.arg(LOCK)
                .arg([0x80u8, 4, 0x88, b'.'].as_slice())
                .arg("NX")
                .arg("EX")
                .arg(30);
            let claimed: Option<String> = request_cache.command(cmd).await.map_err(|_| failed())?;
            if claimed.is_some()
                && self
                    .running
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                let ip_lookup = self.clone();
                let running = Running(self.running.clone());
                let cache = cache.clone();
                let event_sink = event_sink.cloned();
                tokio::spawn(async move {
                    let _running = running;
                    if !matches!(
                        tokio::time::timeout(
                            Duration::from_secs(30),
                            ip_lookup.refresh_ip(&cache, event_sink.as_deref())
                        )
                        .await,
                        Ok(Ok(()))
                    ) {
                        tracing::warn!("Background IP lookup did not complete; original cache lock will expire");
                    }
                });
            }
        }
        Ok(result)
    }
    async fn refresh_ip(
        &self,
        cache: &NotificationCache,
        event_sink: Option<&dyn UpdateEventSink>,
    ) -> Result<(), StoreError> {
        let result = lookup(&self.config).await.map_err(|_| failed())?;
        let previous = cache
            .get(CACHE)
            .await
            .map_err(|_| failed())?
            .map(|raw| decode_cache(&raw).ok_or_else(failed))
            .transpose()?
            .filter(|value| !value.is_null());
        let store = !result["public_ip"].is_null() || previous.is_none();
        let mut payload = if store {
            result.clone()
        } else {
            previous.expect("existing cached value")
        };
        let object = payload.as_object_mut().ok_or_else(failed)?;
        object.insert("verified_at".into(), json!(now()?));
        cache
            .set_for(CACHE, &payload, 3600)
            .await
            .map_err(|_| failed())?;
        let mut cmd = redis::cmd("DEL");
        cmd.arg(LOCK);
        let _: i64 = cache.command(cmd).await.map_err(|_| failed())?;
        if store {
            publish_ip_update(event_sink, &result).await;
        }
        Ok(())
    }
}

async fn publish_ip_update(sink: Option<&dyn UpdateEventSink>, result: &Value) {
    if let Some(sink) = sink {
        let mut data = result.as_object().expect("lookup result object").clone();
        data.insert("type".into(), json!("ip_lookup_complete"));
        if sink
            .send("updates", json!({"type":"update","data":data}))
            .await
            .is_err()
        {
            tracing::warn!("IP lookup update delivery failed");
        }
    } else {
        tracing::warn!("IP lookup cached; M7 event sink is not configured");
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn disabled_lookup_uses_no_cache_or_network() {
        let owner = IpLookup::new(IpLookupConfig::default());
        assert_eq!(
            owner
                .environment(json!(false), None, None, None)
                .await
                .unwrap(),
            json!({"public_ip":null,"local_ip":null,"country_code":null,"country_name":null,
                   "city":null,"ip_lookup_enabled":false,"ip_lookup_pending":false})
        );
        assert!(matches!(
            owner.environment(json!(true), None, None, None).await,
            Err(StoreError::Unsupported(_))
        ));
        assert!(!owner.running.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn ip_completion_sends_only_the_original_message_to_the_configured_sink() {
        use dispatcharr_contracts::{BoxFuture, UpdateEventError};
        use std::sync::Mutex;

        #[derive(Default)]
        struct OwnedSink(Mutex<Vec<(String, Value)>>);
        impl UpdateEventSink for OwnedSink {
            fn send<'a>(
                &'a self,
                group: &'a str,
                message: Value,
            ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
                self.0.lock().unwrap().push((group.into(), message));
                Box::pin(async { Ok(()) })
            }
        }

        let owned = OwnedSink::default();
        let result = json!({"public_ip":"192.0.2.4","local_ip":"127.0.0.1",
                            "country_code":null,"country_name":null,"city":null});
        publish_ip_update(Some(&owned), &result).await;
        assert_eq!(
            owned.0.into_inner().unwrap(),
            vec![(
                "updates".into(),
                json!({"type":"update","data":{
                    "type":"ip_lookup_complete","public_ip":"192.0.2.4","local_ip":"127.0.0.1",
                    "country_code":null,"country_name":null,"city":null
                }})
            )]
        );
        assert!(result.get("type").is_none());
        assert!(result.get("verified_at").is_none());
    }
    #[test]
    fn source_public_ip_validation_retains_original_value_types() {
        for value in [
            json!("2001:db8::1"),
            json!("192.0.2.1"),
            json!(0),
            json!(true),
        ] {
            assert!(valid_ip(&value));
        }
        for value in [
            json!("1"),
            json!("192.168.001.1"),
            json!(-1),
            json!(1.0),
            json!([]),
            Value::Null,
        ] {
            assert!(!valid_ip(&value));
        }
    }
}
