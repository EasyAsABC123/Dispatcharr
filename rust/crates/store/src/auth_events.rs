//! Original authentication audit and cooldown inbox side effects.
//! Original Dispatcharr authors and contributors retain application credit.
use crate::{channels::truthy, AuthService};
use dispatcharr_contracts::PlaybackRequest;
use serde_json::{json, Value};
use sqlx::Row;

pub(crate) struct LockoutNotice {
    pub key: &'static str,
    pub cooldown: &'static str,
    pub title: &'static str,
    pub message: &'static str,
}
pub(crate) const LOGIN_NOTICE:LockoutNotice=LockoutNotice{key:"login-rate-limit",cooldown:"login_rate_limit_notice_cooldown",title:"Login rate limit triggered",message:"One or more clients were temporarily locked out after too many login attempts (JWT, reverse-proxy headers, or Django admin). Check System Events on the Stats page for IP addresses and details."};
pub(crate) const API_NOTICE:LockoutNotice=LockoutNotice{key:"api-key-auth-lockout",cooldown:"api_key_auth_lockout_notice_cooldown",title:"API key authentication rate limit triggered",message:"One or more clients were temporarily locked out after too many failed API key attempts. Check System Events on the Stats page for IP addresses and details."};
impl AuthService {
    pub(crate) async fn auth_event(
        &self,
        request: &PlaybackRequest,
        event_type: &str,
        user: Value,
        reason: Option<&str>,
    ) {
        let mut details = json!({"user":user,"client_ip":self.client_ip_text(request),"user_agent":request.header("user-agent").unwrap_or("unknown")});
        if let Some(reason) = reason {
            details["reason"] = json!(reason)
        }
        if sqlx::query("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details) VALUES($1,now(),NULL,NULL,$2)").bind(event_type).bind(details).execute(self.store.pool()).await.is_err(){tracing::warn!("Authentication audit storage failed");return}
        // Connect/plugin dispatch remains an independently tracked M8 bridge.
        let setting = self.system_settings().await.unwrap_or(Value::Null);
        let value = setting
            .get("max_system_events")
            .filter(|v| truthy(Some(v)))
            .cloned()
            .unwrap_or(json!(100));
        let max = match value {
            Value::Bool(v) => Some(i64::from(v)),
            Value::Number(v) if v.is_f64() => v
                .as_f64()
                .filter(|v| v.is_finite() && *v >= i64::MIN as f64 && *v < i64::MAX as f64)
                .map(|v| v.trunc() as i64),
            v => crate::channel_memberships::primary_key(&v).unwrap_or(Some(100)),
        };
        if let Some(max) = max.filter(|v| *v >= 0) {
            let result=async{
            let count:i64=sqlx::query_scalar("SELECT count(*) FROM core_systemevent").fetch_one(self.store.pool()).await?;
            if count>max {let cutoff:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemevent ORDER BY timestamp DESC LIMIT 1 OFFSET $1").bind(max).fetch_optional(self.store.pool()).await?;if let Some(cutoff)=cutoff{sqlx::query("DELETE FROM core_systemevent WHERE id<$1").bind(cutoff).execute(self.store.pool()).await?;}}
            Ok::<_,sqlx::Error>(())
        }.await;
            if result.is_err() {
                tracing::warn!("Authentication audit retention failed")
            }
        }
    }
    pub(crate) async fn auth_lockout_notice(&self, notice: &LockoutNotice) {
        let Some(cache) = self.notification_cache.as_ref() else {
            return;
        };
        let cooldown = format!(":1:{}", notice.cooldown);
        let mut claim = redis::cmd("SET");
        claim
            .arg(&cooldown)
            .arg([0x80u8, 4, 0x88, b'.'].as_slice())
            .arg("NX")
            .arg("EX")
            .arg(3600);
        let claimed: Result<Option<String>, _> = cache.command(claim).await;
        if !matches!(claimed, Ok(Some(_))) {
            return;
        }
        let result=async {
            let mut tx=self.store.pool().begin().await?;
            sqlx::query("DELETE FROM core_notificationdismissal WHERE notification_id IN(SELECT id FROM core_systemnotification WHERE notification_key=$1)").bind(notice.key).execute(&mut *tx).await?;
            sqlx::query("DELETE FROM core_systemnotification WHERE notification_key=$1").bind(notice.key).execute(&mut *tx).await?;
            tx.commit().await?;
            sqlx::query("INSERT INTO core_systemnotification(notification_key,notification_type,priority,source,title,message,action_data,is_active,admin_only,expires_at,created_at,updated_at) VALUES($1,'warning','high','system',$2,$3,$4,true,true,NULL,now(),now()) RETURNING id::bigint AS id,created_at").bind(notice.key).bind(notice.title).bind(notice.message).bind(json!({"action_url":"/stats","action_text":"Open Stats"})).fetch_one(self.store.pool()).await
        }.await;
        let row = match result {
            Ok(row) => row,
            Err(_) => {
                let mut cmd = redis::cmd("DEL");
                cmd.arg(cooldown);
                let _: Result<i64, _> = cache.command(cmd).await;
                tracing::warn!("Authentication inbox storage failed");
                return;
            }
        };
        let Some(sink) = self.event_sink.as_ref() else {
            tracing::warn!("Authentication inbox created without M7 event sink");
            return;
        };
        let Ok(id) = row.try_get::<i64, _>("id") else {
            return;
        };
        let Ok(created) = row.try_get::<chrono::DateTime<chrono::Utc>, _>("created_at") else {
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
        let message = json!({"type":"update","data":{"type":"system_notification","notification":{"id":id,"notification_key":notice.key,"notification_type":"warning","priority":"high","title":notice.title,"message":notice.message,"action_data":{"action_url":"/stats","action_text":"Open Stats"},"is_active":true,"admin_only":true,"created_at":created}}});
        if sink.send("updates", message).await.is_err() {
            tracing::warn!("Authentication inbox publication failed")
        }
    }
    pub(crate) async fn login_lockout(
        &self,
        request: &PlaybackRequest,
        username: Value,
        wait: Option<f64>,
    ) {
        let Some(cache) = self.notification_cache.as_ref() else {
            return;
        };
        let mut cmd = redis::cmd("SET");
        cmd.arg(format!(
            ":1:login_rate_limit_lockout_emitted:{}",
            self.client_ip_text(request)
        ))
        .arg([0x80u8, 4, 0x88, b'.'].as_slice())
        .arg("NX")
        .arg("EX")
        .arg(wait.unwrap_or(60.0).ceil().max(1.0) as u64);
        let claimed: Result<Option<String>, _> = cache.command(cmd).await;
        if matches!(claimed, Ok(Some(_))) {
            self.auth_event(
                request,
                "login_failed",
                if truthy(Some(&username)) {
                    username
                } else {
                    json!("unknown")
                },
                Some("Login rate limited"),
            )
            .await;
            self.auth_lockout_notice(&LOGIN_NOTICE).await;
        }
    }
}
pub(crate) fn audit_username(body: &Value) -> Value {
    body.get("username")
        .filter(|value| truthy(Some(value)))
        .cloned()
        .unwrap_or(json!("unknown"))
}
pub(crate) fn validation_exception(body: &Value) -> String {
    let mut fields = Vec::new();
    for key in ["username", "password"] {
        let Some(errors) = body.get(key).and_then(Value::as_array) else {
            continue;
        };
        let values = errors
            .iter()
            .filter_map(Value::as_str)
            .map(|message| {
                let code = match message {
                    "This field is required." => "required",
                    "This field may not be null." => "null",
                    "This field may not be blank." => "blank",
                    "Null characters are not allowed." => "null_characters_not_allowed",
                    _ => "invalid",
                };
                // All serializer messages here are fixed English messages. Reuse
                // the original Python string representation for quoting/escaping.
                let message = crate::python_action::collection_text(&json!([message]), 4096)
                    .unwrap_or_else(|| "['Invalid input.']".into());
                format!(
                    "ErrorDetail(string={}, code='{code}')",
                    &message[1..message.len() - 1]
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        fields.push(format!("'{key}': [{values}]"));
    }
    format!("{{{}}}", fields.join(", "))
}
