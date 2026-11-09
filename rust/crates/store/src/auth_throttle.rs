//! Shared original Django authentication throttle histories.
//! Original application credit remains with Dispatcharr contributors.
//! Actual-source host contracts are recorded separately; packaged parity pending.
use crate::{django_pickle, notifications::decode_cache, NotificationCache, StoreError};
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
pub(crate) enum Scope {
    Login,
    ApiKey,
}
impl Scope {
    fn key(self, ip: &str) -> String {
        format!(
            ":1:throttle_{}_{ip}",
            match self {
                Self::Login => "login",
                Self::ApiKey => "api_key_auth",
            }
        )
    }
    fn limit(self) -> usize {
        match self {
            Self::Login => 3,
            Self::ApiKey => 10,
        }
    }
}
#[derive(Debug, PartialEq)]
pub(crate) enum Decision {
    Allowed { crossed: bool },
    Limited { wait: Option<f64> },
}
fn failed() -> StoreError {
    StoreError::StatusBody(500, json!({"error":"Authentication cache unavailable"}))
}
fn history(raw: Option<&[u8]>, at: f64, scope: Scope) -> Result<Option<Vec<f64>>, StoreError> {
    let Some(raw) = raw else {
        return Ok(Some(vec![]));
    };
    let value = decode_cache(raw).ok_or_else(failed)?;
    if value.is_null() && matches!(scope, Scope::ApiKey) {
        return Ok(None);
    }
    let values = value
        .as_array()
        .filter(|v| v.len() <= 1024)
        .ok_or_else(failed)?;
    let mut result = values
        .iter()
        .map(|v| match v {
            Value::Bool(v) => Ok(if *v { 1.0 } else { 0.0 }),
            Value::Number(v) => v.as_f64().filter(|v| v.is_finite()).ok_or_else(failed),
            _ => Err(failed()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    while result.last().is_some_and(|last| *last <= at - 60.0) {
        result.pop();
    }
    Ok(Some(result))
}
fn wait(history: &[f64], at: f64, limit: usize) -> Option<f64> {
    if history.len() > limit {
        return None;
    }
    history
        .last()
        .map(|oldest| (60.0 - (at - oldest)) / (limit - history.len() + 1) as f64)
}
/// `record=false` is the failure-only API-key preflight. Login always records
/// allowed attempts; rejected attempts do not extend the original TTL.
pub(crate) async fn check(
    cache: &NotificationCache,
    ip: &str,
    scope: Scope,
    record: bool,
) -> Result<Decision, StoreError> {
    let key = scope.key(ip);
    for _ in 0..16 {
        let raw = cache.get(&key).await.map_err(|_| failed())?;
        let at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| failed())?
            .as_secs_f64();
        let Some(mut entries) = history(raw.as_deref(), at, scope)? else {
            return Ok(Decision::Allowed { crossed: false });
        };
        if entries.len() >= scope.limit() {
            return Ok(Decision::Limited {
                wait: wait(&entries, at, scope.limit()),
            });
        }
        if !record {
            return Ok(Decision::Allowed { crossed: false });
        }
        entries.insert(0, at);
        let encoded = django_pickle::encode(&json!(entries)).ok_or_else(failed)?;
        let mut cmd = redis::cmd("EVAL");
        cmd.arg("local old=redis.call('GET',KEYS[1]); if (ARGV[1]=='0' and old) or (ARGV[1]=='1' and old~=ARGV[2]) then return 0 end redis.call('SET',KEYS[1],ARGV[3],'EX',60); return 1").arg(1).arg(&key).arg(if raw.is_some(){"1"}else{"0"}).arg(raw.as_deref().unwrap_or(&[])).arg(encoded);
        let changed: i64 = cache.command(cmd).await.map_err(|_| failed())?;
        if changed == 1 {
            return Ok(Decision::Allowed {
                crossed: entries.len() == scope.limit(),
            });
        }
    }
    Err(failed())
}

impl crate::AuthService {
    /// Redis-backed compatibility when configured; legacy standalone users of
    /// the constructor retain the existing bounded local budget.
    pub(crate) async fn shared_auth_budget(
        &self,
        ip: dispatcharr_contracts::SourceIpAddr,
        scope: Scope,
        record: bool,
        proxy: bool,
    ) -> Result<bool, StoreError> {
        let Some(cache) = self.notification_cache.as_ref() else {
            self.throttle(
                ip.address(),
                match scope {
                    Scope::Login => "login",
                    Scope::ApiKey => "api-key",
                },
                record,
            )?;
            return Ok(false);
        };
        match check(cache, &ip.to_string(), scope, record).await? {
            Decision::Allowed { crossed } => Ok(crossed),
            Decision::Limited { wait } => {
                let retry_after = wait.map(|wait| {
                    if proxy {
                        wait.floor().max(1.0) as u64
                    } else {
                        wait.ceil().max(0.0) as u64
                    }
                });
                let prefix = match scope {
                    Scope::Login if proxy => "Too many login attempts. Try again later.",
                    Scope::Login => "Request was throttled.",
                    Scope::ApiKey => "Too many API key attempts. Try again later.",
                };
                let detail = if proxy {
                    prefix.to_owned()
                } else if let Some(wait) = wait {
                    let seconds = wait.ceil().max(0.0) as u64;
                    format!(
                        "{prefix} Expected available in {seconds} {}.",
                        if seconds == 1 { "second" } else { "seconds" }
                    )
                } else {
                    prefix.to_owned()
                };
                Err(StoreError::Throttled {
                    body: json!({"detail":detail}),
                    retry_after,
                    wait,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn django_wait_divides_available_slots_and_expiry_is_inclusive() {
        assert_eq!(wait(&[100.0; 3], 147.2, 3), Some(12.800000000000011));
        assert_eq!(wait(&[100.0; 2], 140.0, 3), Some(10.0));
        assert_eq!(wait(&[100.0; 4], 140.0, 3), None);
        let raw = django_pickle::encode(&json!([99.0, 40.0])).unwrap();
        assert_eq!(
            history(Some(&raw), 100.0, Scope::Login).unwrap(),
            Some(vec![99.0])
        );
    }
    #[test]
    fn failed_auth_null_disables_budget_but_login_null_is_source_error() {
        let raw = django_pickle::encode(&Value::Null).unwrap();
        assert!(history(Some(&raw), 100.0, Scope::ApiKey).unwrap().is_none());
        assert!(history(Some(&raw), 100.0, Scope::Login).is_err());
    }
}
