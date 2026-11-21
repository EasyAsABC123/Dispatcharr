//! Original Dispatcharr's ordinary live-client audit and bounded Main Connect webhook.
//! Original Dispatcharr, Django, and Requests authors retain application and algorithm credit.
use crate::{
    epg_connect::{self, LiveClientEvent, TemplateResult, Templates},
    AuthService, StoreError, TaskSqlSession,
};
use dispatcharr_contracts::{
    BoxFuture, LiveClientConnectEvent, LiveClientConnectObserver, LiveClientDisconnectEvent,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use uuid::Uuid;

type Owner = Arc<dyn Send + Sync>;
const ACTIVE_SCOPE: &str = "SELECT EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription s \
    JOIN dispatcharr_connect_integration i ON i.id=s.integration_id \
    WHERE s.event=$1 AND s.enabled AND i.enabled) \
    AND NOT EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription s \
    JOIN dispatcharr_connect_integration i ON i.id=s.integration_id \
    WHERE s.event=$1 AND s.enabled AND i.enabled \
    AND (i.type<>'webhook' OR coalesce(s.payload_template,'')<>'')) \
    AND NOT EXISTS(SELECT 1 FROM plugins_pluginconfig WHERE enabled)";

struct Unsettled {
    _session: TaskSqlSession,
    _owner: Owner,
}

#[derive(Clone)]
pub struct LiveClientConnect {
    auth: AuthService,
    used: Arc<AtomicBool>,
    unsettled: Arc<Mutex<Vec<Unsettled>>>,
}

struct Plain;
impl Templates for Plain {
    fn begin_event(
        &self,
        _event: String,
        _payload: Value,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }

    fn render(&self, _template: String) -> BoxFuture<'_, Result<TemplateResult, StoreError>> {
        Box::pin(async { Err(StoreError::Unsupported("Live Connect template scope")) })
    }
}

fn event_details(event: &LiveClientConnectEvent, end: Option<&LiveClientDisconnectEvent>) -> Value {
    let user_agent: String = event.viewer.user_agent.chars().take(100).collect();
    let mut details = json!({
        "client_ip":event.viewer.ip_address.to_string(),
        "client_id":event.client_id,
        "user_agent":(!user_agent.is_empty()).then_some(user_agent),
        "username":event.viewer.username,
    });
    if let Some(end) = end {
        details["duration"] = json!((end.elapsed.as_secs_f64() * 100.0).round_ties_even() / 100.0);
        details["bytes_sent"] = json!(end.bytes_sent);
    }
    details
}

impl LiveClientConnect {
    pub fn new(auth: AuthService) -> Self {
        Self {
            auth,
            used: Arc::new(AtomicBool::new(false)),
            unsettled: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn was_used(&self) -> bool {
        self.used.load(Ordering::Acquire)
    }

    async fn record_audit(
        &self,
        kind: LiveClientEvent,
        event: &LiveClientConnectEvent,
        channel: Uuid,
        details: &Value,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details) \
             VALUES($1,$2,$3,$4,$5)",
        )
        .bind(kind.name())
        .bind(chrono::Utc::now())
        .bind(channel)
        .bind(&event.channel_name)
        .bind(details)
        .execute(self.auth.store.pool())
        .await?;
        Ok(())
    }

    async fn deliver(
        &self,
        kind: LiveClientEvent,
        session: &TaskSqlSession,
        owner: Owner,
        channel: Uuid,
        channel_name: String,
        details: Value,
    ) -> Result<(), epg_connect::Error> {
        let payload = crate::recording_capture::enrichment::enrich(
            &self.auth,
            session,
            Some(channel),
            Some(channel_name),
            details,
            owner.clone(),
        )
        .await?;
        epg_connect::dispatch_live_client(kind, payload, session.clone(), Arc::new(Plain), owner)
            .await?;
        Ok(())
    }

    async fn deliver_if_eligible(
        &self,
        kind: LiveClientEvent,
        owner: Owner,
        channel: Uuid,
        channel_name: String,
        details: Value,
        admission: Option<Owner>,
    ) -> Result<(), epg_connect::Error> {
        let eligible = sqlx::query_scalar::<_, bool>(ACTIVE_SCOPE)
            .bind(kind.name())
            .fetch_one(self.auth.store.pool())
            .await
            .map_err(StoreError::from)?;
        if !eligible {
            return Ok(());
        }
        let session = self.auth.borrow_task_sql_session(owner.clone()).await?;
        drop(admission);
        let outcome = self
            .deliver(
                kind,
                &session,
                owner.clone(),
                channel,
                channel_name,
                details,
            )
            .await;
        if session.close().await.is_err() {
            // Retain the actual writer and worker if physical close is uncertain.
            self.unsettled
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(Unsettled {
                    _session: session,
                    _owner: owner,
                });
        }
        outcome
    }

    async fn retain_audit_history(&self) -> Result<(), StoreError> {
        let settings = self.auth.system_settings().await?;
        let maximum = crate::sd_guide::max_events(settings.get("max_system_events"));
        if maximum >= 0 {
            sqlx::query(
                "DELETE FROM core_systemevent WHERE id < (SELECT id FROM core_systemevent \
                 ORDER BY timestamp DESC LIMIT 1 OFFSET $1)",
            )
            .bind(maximum)
            .execute(self.auth.store.pool())
            .await?;
        }
        Ok(())
    }
}

impl LiveClientConnectObserver for LiveClientConnect {
    fn before_media(&self, event: LiveClientConnectEvent, owner: Owner) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let Ok(channel) = Uuid::parse_str(&event.channel_id) else {
                return;
            };
            self.used.store(true, Ordering::Release);
            let details = event_details(&event, None);
            // Stock writes the SystemEvent after playback is ready and before
            // useful media even if no delivery will be sent.
            if self
                .record_audit(LiveClientEvent::Connect, &event, channel, &details)
                .await
                .is_err()
            {
                tracing::warn!("Live client audit storage failed");
                return;
            }

            // Recipient settings can change after startup. Read them in the
            // detached worker, using an owned SQL writer only for delivery.
            let runtime = self.clone();
            tokio::spawn(async move {
                let outcome = runtime
                    .deliver_if_eligible(
                        LiveClientEvent::Connect,
                        owner.clone(),
                        channel,
                        event.channel_name,
                        details,
                        None,
                    )
                    .await;
                if outcome.is_err() {
                    tracing::warn!("Live Connect delivery did not settle successfully");
                }
                if runtime.retain_audit_history().await.is_err() {
                    tracing::warn!("Live client audit retention failed");
                }
                drop(owner);
            });
        })
    }

    fn after_natural_retirement(
        &self,
        event: LiveClientDisconnectEvent,
        owner: Owner,
        admission: Owner,
    ) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let Ok(channel) = Uuid::parse_str(&event.connected.channel_id) else {
                return;
            };
            self.used.store(true, Ordering::Release);
            let details = event_details(&event.connected, Some(&event));
            if self
                .record_audit(
                    LiveClientEvent::Disconnect,
                    &event.connected,
                    channel,
                    &details,
                )
                .await
                .is_err()
            {
                tracing::warn!("Live client disconnect audit storage failed");
                return;
            }
            if self.retain_audit_history().await.is_err() {
                tracing::warn!("Live client audit retention failed");
            }
            // Retain the existing bounded client slot through the audit and
            // recipient lookup. Webhook I/O owns only the existing SQL writer.
            let channel_name = event.connected.channel_name;
            if self
                .deliver_if_eligible(
                    LiveClientEvent::Disconnect,
                    owner,
                    channel,
                    channel_name,
                    details,
                    Some(admission),
                )
                .await
                .is_err()
            {
                tracing::warn!("Live Connect disconnect delivery did not settle successfully");
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn viewer_connect_and_disconnect_retain_same_identity_and_typed_counters() {
        let event = LiveClientConnectEvent {
            channel_id: "11ca0bac-7532-414a-9d5b-9790cb6784a1".into(),
            channel_name: "Owned".into(),
            client_id: "owned-connection".into(),
            viewer: dispatcharr_contracts::PlaybackViewer {
                user_id: Some(17),
                username: Some("owner".into()),
                ip_address: "127.0.0.1".parse().unwrap(),
                user_agent: "ř".repeat(103),
                stream_limit: 0,
                limit_policy: Default::default(),
            },
        };
        let fields = event_details(&event, None);
        assert_eq!(
            fields
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["client_ip", "client_id", "user_agent", "username"]
        );
        assert_eq!(fields["user_agent"], "ř".repeat(100));
        assert_eq!(fields["client_id"], "owned-connection");
        let end = LiveClientDisconnectEvent {
            connected: event.clone(),
            elapsed: std::time::Duration::from_millis(1249),
            bytes_sent: 75_200,
        };
        let mut disconnected = event_details(&event, Some(&end));
        assert_eq!(
            disconnected.as_object_mut().unwrap().remove("duration"),
            Some(json!(1.25))
        );
        assert_eq!(
            disconnected.as_object_mut().unwrap().remove("bytes_sent"),
            Some(json!(75200))
        );
        assert_eq!(disconnected, fields);
        assert_eq!(LiveClientEvent::Disconnect.name(), "client_disconnect");
    }
}
