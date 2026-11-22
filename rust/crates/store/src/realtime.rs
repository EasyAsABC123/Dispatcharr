//! Unwired realtime policy/auth foundation, derived from dispatcharr/consumers.py
//! and jwt_ws_auth.py. Original authors retain full credit; see ATTRIBUTION.md.
//! No WebSocket/worker transport parity is established by these helpers alone.
#[cfg(feature = "sqlite-copied-data-runtime")]
use crate::{auth::decode_token_for_config, AuthConfig};
use crate::{auth::Claims, AuthService, NotificationCache, StoreError, User};
use dispatcharr_contracts::BoxFuture;
use serde_json::{json, Value};

fn query_token(query: &[(String, String)]) -> Option<&str> {
    // urllib.parse.parse_qs drops blank values and selects the first retained
    // token. The caller must supply decoded query pairs in their original order.
    query
        .iter()
        .find(|(key, value)| key == "token" && !value.is_empty())
        .map(|(_, value)| value.as_str())
}

/// The shared WebSocket gateway needs only a live role and optional IP replay.
pub trait WebsocketIdentity: Send + Sync {
    fn role<'a>(
        &'a self,
        query: &'a [(String, String)],
    ) -> BoxFuture<'a, Result<Option<i32>, StoreError>>;
    fn cached_ip(&self) -> BoxFuture<'_, Result<Option<Value>, StoreError>>;
}

fn access_user_id(
    query: &[(String, String)],
    decode_access: impl FnOnce(&str) -> Result<Claims, StoreError>,
) -> Option<i64> {
    decode_access(query_token(query)?).ok()?.id()
}

async fn cached_ip(cache: &NotificationCache) -> Result<Option<Value>, StoreError> {
    let Some(raw) = cache.get(":1:dispatcharr:ip_lookup_result").await? else {
        return Ok(None);
    };
    let value = crate::notifications::decode_cache(&raw)
        .ok_or_else(|| StoreError::StatusBody(503, json!({"detail":"IP cache unavailable."})))?;
    let Some(mut payload) = value.as_object().filter(|value| !value.is_empty()).cloned() else {
        return Ok(None);
    };
    payload.remove("verified_at");
    Ok(Some(Value::Object(payload)))
}

impl AuthService {
    /// Optional initial IP replay. Transport errors are handled by the gateway.
    pub async fn websocket_cached_ip(&self) -> Result<Option<Value>, StoreError> {
        match &self.notification_cache {
            Some(cache) => cached_ip(cache).await,
            None => Ok(None),
        }
    }

    /// Source middleware accepts a query access JWT and reads the live user.
    pub async fn websocket_user(
        &self,
        query: &[(String, String)],
    ) -> Result<Option<User>, StoreError> {
        let Some(id) = access_user_id(query, |token| self.decode_token(token, "access")) else {
            return Ok(None);
        };
        Ok(self
            .store
            .user_by_id(id)
            .await?
            .filter(|user| user.is_active))
    }
}

impl WebsocketIdentity for AuthService {
    fn role<'a>(
        &'a self,
        query: &'a [(String, String)],
    ) -> BoxFuture<'a, Result<Option<i32>, StoreError>> {
        Box::pin(async move {
            Ok(self
                .websocket_user(query)
                .await?
                .map(|user| user.user_level))
        })
    }

    fn cached_ip(&self) -> BoxFuture<'_, Result<Option<Value>, StoreError>> {
        Box::pin(self.websocket_cached_ip())
    }
}

#[cfg(feature = "sqlite-copied-data-runtime")]
pub struct CopiedWebsocketIdentity {
    pub(crate) owner: std::sync::Weak<crate::sqlite_identity::SqliteIdentity>,
    pub(crate) config: AuthConfig,
    pub(crate) cache: NotificationCache,
}

#[cfg(feature = "sqlite-copied-data-runtime")]
async fn sqlite_active_role(
    pool: &sqlx::SqlitePool,
    user_id: i64,
) -> Result<Option<i32>, StoreError> {
    Ok(
        sqlx::query_scalar("SELECT user_level FROM accounts_user WHERE id=?1 AND is_active")
            .bind(user_id)
            .fetch_optional(pool)
            .await?,
    )
}

#[cfg(feature = "sqlite-copied-data-runtime")]
impl WebsocketIdentity for CopiedWebsocketIdentity {
    fn role<'a>(
        &'a self,
        query: &'a [(String, String)],
    ) -> BoxFuture<'a, Result<Option<i32>, StoreError>> {
        Box::pin(async move {
            let Some(id) = access_user_id(query, |token| {
                decode_token_for_config(&self.config, token, "access")
            }) else {
                return Ok(None);
            };
            let Some(owner) = self.owner.upgrade() else {
                return Ok(None);
            };
            sqlite_active_role(owner.pool(), id).await
        })
    }

    fn cached_ip(&self) -> BoxFuture<'_, Result<Option<Value>, StoreError>> {
        Box::pin(cached_ip(&self.cache))
    }
}

/// Exact initial consumer message, sent after accepting/joining the group.
pub fn connection_established() -> Value {
    json!({"type":"connection_established","data":{"success":true,"message":"WebSocket connection established successfully"}})
}

/// Source visibility policy for valid update objects. The level belongs to the
/// authenticated connection principal captured at handshake. None represents
/// an unauthenticated principal; it must never be accepted as a live socket.
/// Malformed non-object updates are refused rather than forwarded unexamined.
pub fn may_receive_update(user_level: Option<i32>, data: &Value) -> bool {
    if !data.is_null() && !data.is_object() {
        return false;
    }
    let admin = user_level.is_some_and(|level| level >= 10);
    match data.get("type").and_then(Value::as_str) {
        Some("channel_stats" | "vod_stats" | "timeshift_stats" | "vod_started" | "vod_stopped") => {
            admin
        }
        Some("system_notification")
            if crate::channels::truthy(
                data.get("notification")
                    .and_then(|value| value.get("admin_only")),
            ) =>
        {
            admin
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_selection_matches_parse_qs_first_nonblank_value() {
        let pairs = vec![
            ("token".into(), "".into()),
            ("other".into(), "ignored".into()),
            ("token".into(), "first".into()),
            ("token".into(), "second".into()),
        ];
        assert_eq!(query_token(&pairs), Some("first"));
        assert_eq!(query_token(&[]), None);
        assert_eq!(query_token(&[("Token".into(), "wrongcase".into())]), None);
        assert_eq!(query_token(&[("token".into(), " ".into())]), Some(" "));
    }
    #[cfg(feature = "sqlite-copied-data-runtime")]
    #[tokio::test]
    async fn copied_websocket_selects_query_access_and_only_active_sqlite_roles() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE TABLE accounts_user(id INTEGER PRIMARY KEY,user_level INTEGER,is_active BOOL);
            INSERT INTO accounts_user VALUES (1,1,true),(2,10,true),(3,1,false);",
        )
        .execute(&pool)
        .await
        .unwrap();
        let config = AuthConfig::new("owned-copied-websocket-fixture-key".into()).unwrap();
        let access = crate::auth::mint_token(&config, json!(1), "access").unwrap();
        let refresh = crate::auth::mint_token(&config, json!(2), "refresh").unwrap();
        let user_id = |query: &[(String, String)]| {
            access_user_id(query, |token| {
                decode_token_for_config(&config, token, "access")
            })
        };
        let valid = [("token".to_owned(), access)];
        assert_eq!(user_id(&valid), Some(1));
        assert_eq!(
            sqlite_active_role(&pool, user_id(&valid).unwrap())
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(sqlite_active_role(&pool, 2).await.unwrap(), Some(10));
        assert_eq!(sqlite_active_role(&pool, 3).await.unwrap(), None);
        assert_eq!(sqlite_active_role(&pool, 91).await.unwrap(), None);
        assert_eq!(user_id(&[("token".to_owned(), refresh)]), None);
        assert_eq!(user_id(&[]), None);
        assert!(may_receive_update(Some(1), &json!({"type":"epg_refresh"})));
        assert!(!may_receive_update(
            Some(1),
            &json!({"type":"channel_stats"})
        ));
    }

    #[test]
    fn connection_envelope_and_per_event_restrictions_match_source() {
        assert_eq!(
            connection_established(),
            json!({"type":"connection_established","data":{"success":true,"message":"WebSocket connection established successfully"}})
        );
        for kind in [
            "channel_stats",
            "vod_stats",
            "timeshift_stats",
            "vod_started",
            "vod_stopped",
        ] {
            let data = json!({"type":kind});
            for level in [None, Some(0), Some(1), Some(9)] {
                assert!(!may_receive_update(level, &data));
            }
            for level in [10, 11] {
                assert!(may_receive_update(Some(level), &data));
            }
        }
        for flag in [json!(true), json!(1), json!("false"), json!([false])] {
            let data = json!({"type":"system_notification","notification":{"admin_only":flag}});
            assert!(!may_receive_update(Some(1), &data));
            assert!(may_receive_update(Some(10), &data));
        }
        for flag in [
            Value::Null,
            json!(false),
            json!(0),
            json!(""),
            json!([]),
            json!({}),
        ] {
            assert!(may_receive_update(
                Some(1),
                &json!({"type":"system_notification","notification":{"admin_only":flag}})
            ));
        }
        for data in [
            Value::Null,
            json!({}),
            json!({"type":"epg_refresh"}),
            json!({"type":"future_event"}),
        ] {
            assert!(may_receive_update(Some(1), &data));
        }
        assert!(!may_receive_update(Some(10), &json!(["malformed"])));
    }
}
