//! Unwired realtime policy/auth foundation, derived from dispatcharr/consumers.py
//! and jwt_ws_auth.py. Original authors retain full credit; see ATTRIBUTION.md.
//! No WebSocket/worker transport parity is established by these helpers alone.
use crate::{AuthService, StoreError, User};
use serde_json::{json, Value};

fn query_token(query: &[(String, String)]) -> Option<&str> {
    // urllib.parse.parse_qs drops blank values and selects the first retained
    // token. The caller must supply decoded query pairs in their original order.
    query
        .iter()
        .find(|(key, value)| key == "token" && !value.is_empty())
        .map(|(_, value)| value.as_str())
}

impl AuthService {
    /// Optional readonly initial IP replay payload; adapter adds its data/type
    /// envelope, then merges this payload as the original consumer does.
    /// Cache errors must be caught by the adapter without closing the socket.
    pub async fn websocket_cached_ip(&self) -> Result<Option<Value>, StoreError> {
        let Some(cache) = self.notification_cache.as_ref() else {
            return Ok(None);
        };
        let Some(raw) = cache.get(":1:dispatcharr:ip_lookup_result").await? else {
            return Ok(None);
        };
        let value = crate::notifications::decode_cache(&raw).ok_or_else(|| {
            StoreError::StatusBody(503, json!({"detail":"IP cache unavailable."}))
        })?;
        let Some(mut payload) = value.as_object().filter(|value| !value.is_empty()).cloned() else {
            return Ok(None);
        };
        payload.remove("verified_at");
        Ok(Some(Value::Object(payload)))
    }
    /// WebSocket middleware authentication. Deliberately separate from REST:
    /// query access JWT only, no API-key fallback and no added UI network gate.
    /// An adapter must reject the upgrade for None or an operational error.
    pub async fn websocket_user(
        &self,
        query: &[(String, String)],
    ) -> Result<Option<User>, StoreError> {
        let Some(token) = query_token(query) else {
            return Ok(None);
        };
        let Ok(claims) = self.decode_token(token, "access") else {
            return Ok(None);
        };
        let Some(id) = claims.id() else {
            return Ok(None);
        };
        Ok(self
            .store
            .user_by_id(id)
            .await?
            .filter(|user| user.is_active))
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
