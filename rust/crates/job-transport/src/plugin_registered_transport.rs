//! Typed actual-plugin frame publication over Main's SAME broker and lease.
//! Source/Kombu creates raw bytes; this port never reserializes a fake envelope.
//! Original Dispatcharr/Celery/Kombu authors retain full protocol credit.
use super::{Authority, ClassifiedError, Jobs, PUBLISH};
use dispatcharr_store::PluginTaskCapability;
use serde_json::Value;
use std::sync::Arc;
#[derive(Clone, Debug)]
pub enum PluginError {
    Unsupported,
    Closed,
    Capacity,
    Corrupt,
    Backend,
    AmbiguousPublication { task_id: String },
}
#[derive(Clone, Debug)]
pub struct PluginPublished {
    pub task_id: String,
    pub queue_length: u64,
}
fn transport(error: super::Error) -> PluginError {
    match error {
        super::Error::Closed => PluginError::Closed,
        super::Error::Capacity => PluginError::Capacity,
        _ => PluginError::Backend,
    }
}
struct Frame {
    id: String,
}
fn frame(capability: &PluginTaskCapability, raw: &[u8]) -> Result<Frame, PluginError> {
    if raw.is_empty() || raw.len() > 65536 || !capability.live() {
        return Err(PluginError::Unsupported);
    }
    // Keep unsupported custom IDs/codecs/body shapes BEFORE publication until
    // the generic selected runtime supports them; never poison an accepted FIFO.
    crate::registered_plugin_protocol::parse(raw, capability)
        .map_err(|_| PluginError::Unsupported)?;
    let value: Value = serde_json::from_slice(raw).map_err(|_| PluginError::Corrupt)?;
    let h = value["headers"].as_object().ok_or(PluginError::Corrupt)?;
    let text = |key: &str| {
        h.get(key)
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty() && v.len() <= 512 && !v.contains('\0'))
            .ok_or(PluginError::Corrupt)
    };
    if text("task")? != capability.task_name() {
        return Err(PluginError::Unsupported);
    }
    let id = text("id")?.to_owned();
    let props = value["properties"]
        .as_object()
        .ok_or(PluginError::Corrupt)?;
    if props.get("correlation_id").and_then(Value::as_str) != Some(id.as_str())
        || value["content-type"] != "application/json"
        || value["content-encoding"] != "utf-8"
        || props.get("body_encoding").and_then(Value::as_str) != Some("base64")
        || !value["body"].is_string()
    {
        return Err(PluginError::Unsupported);
    }
    let info = props
        .get("delivery_info")
        .and_then(Value::as_object)
        .ok_or(PluginError::Corrupt)?;
    if info.get("routing_key").and_then(Value::as_str) != Some("celery")
        || !matches!(
            info.get("exchange").and_then(Value::as_str),
            Some("celery" | "")
        )
    {
        return Err(PluginError::Unsupported);
    }
    if props
        .get("priority")
        .is_some_and(|v| !v.is_null() && v.as_i64() != Some(0))
    {
        return Err(PluginError::Unsupported);
    }
    // The extended Main registry now owns plain/ETA/expiry through the SAME
    // durable reservation budget. This module is joined with that actual
    // selected handler; the earlier compiler-only slice keeps its refusal.
    Ok(Frame { id })
}
impl Jobs {
    pub fn plugin_queue_capacity(&self) -> Result<usize, PluginError> {
        if !matches!(self.inner.authority, Authority::DefaultServices) {
            return Err(PluginError::Unsupported);
        }
        Ok(self.inner.config.max_queued_jobs)
    }
    pub fn validate_plugin_broker(&self, params: &Value) -> Result<(), PluginError> {
        if !matches!(self.inner.authority, Authority::DefaultServices) {
            return Err(PluginError::Unsupported);
        }
        let target = self.inner.broker.configured_endpoint();
        let same = match &target.addr {
            redis::ConnectionAddr::Tcp(host, port) => {
                params["host"] == *host
                    && params["port"] == u64::from(*port)
                    && matches!(
                        params.get("ssl"),
                        None | Some(Value::Null) | Some(Value::Bool(false))
                    )
            }
            _ => false,
        };
        let database = params["virtual_host"]
            .as_str()
            .and_then(|s| s.trim_start_matches('/').parse::<i64>().ok());
        if !same
            || database != Some(target.redis.db)
            || params["username"] != serde_json::json!(target.redis.username)
            || params["password"] != serde_json::json!(target.redis.password)
        {
            return Err(PluginError::Unsupported);
        }
        Ok(())
    }

    /// Only normal Main + actual loaded-callable capability grants this port.
    /// Every older fixture/index/refresh/series/DVR authority remains fenced.
    pub async fn publish_registered_plugin(
        &self,
        capability: PluginTaskCapability,
        raw: Vec<u8>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<PluginPublished, PluginError> {
        if !matches!(self.inner.authority, Authority::DefaultServices) {
            return Err(PluginError::Unsupported);
        }
        let candidate = frame(&capability, &raw)?;
        let lease = self.lease(owner).map_err(transport)?;
        let limit = self.inner.config.max_queued_jobs.to_string();
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                PUBLISH,
                &["celery".into(), "_kombu.binding.celery".into()],
                &[&raw, limit.as_bytes()],
                (lease, capability),
            )
            .await
        {
            Ok((n, _)) if n > 0 => Ok(PluginPublished {
                task_id: candidate.id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(PluginError::Capacity),
            Ok(_) => Err(PluginError::Corrupt),
            Err(ClassifiedError::Backend) => Err(PluginError::AmbiguousPublication {
                task_id: candidate.id,
            }),
            Err(ClassifiedError::Capacity) => Err(PluginError::Capacity),
            Err(_) => Err(PluginError::Backend),
        }
    }
    /// Exact existing native family only; Source/Kombu supplies original bytes.
    /// Parented/expiry/other unsupported shapes fail the real family codec
    /// BEFORE shared broker publication and cannot leave an unconsumable head.
    pub async fn publish_builtin_plugin_refresh(
        &self,
        raw: Vec<u8>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<PluginPublished, PluginError> {
        if !matches!(self.inner.authority, Authority::DefaultServices) {
            return Err(PluginError::Unsupported);
        }
        let frame: Value = serde_json::from_slice(&raw).map_err(|_| PluginError::Corrupt)?;
        let candidate = if frame["headers"]["eta"].is_null() {
            crate::normal_plugin_repos::parse(&raw)
        } else {
            crate::normal_plugin_repos::parse_eta(&raw)
        }
        .map_err(|_| PluginError::Unsupported)?;
        let lease = self.lease(owner).map_err(transport)?;
        let maximum = self.inner.config.max_queued_jobs.to_string();
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                PUBLISH,
                &["celery".into(), "_kombu.binding.celery".into()],
                &[&raw, maximum.as_bytes(), b"celery\x06\x16\x06\x16celery"],
                lease,
            )
            .await
        {
            Ok((n, _)) if n > 0 => Ok(PluginPublished {
                task_id: candidate.id.to_string(),
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(PluginError::Capacity),
            Ok(_) => Err(PluginError::Corrupt),
            Err(ClassifiedError::Backend) => Err(PluginError::AmbiguousPublication {
                task_id: candidate.id.to_string(),
            }),
            Err(ClassifiedError::Capacity) => Err(PluginError::Capacity),
            Err(_) => Err(PluginError::Backend),
        }
    }
    pub async fn registered_plugin_queue_length(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<u64, PluginError> {
        if !matches!(self.inner.authority, Authority::DefaultServices) {
            return Err(PluginError::Unsupported);
        }
        let lease = self.lease(owner).map_err(transport)?;
        self.inner
            .broker
            .command_classified::<u64, _>(
                "return redis.call('LLEN',KEYS[1])",
                &["celery".into()],
                &[],
                lease,
            )
            .await
            .map(|(n, _)| n)
            .map_err(|e| {
                if e == ClassifiedError::Capacity {
                    PluginError::Capacity
                } else {
                    PluginError::Backend
                }
            })
    }
    pub async fn bind_registered_plugin_queue(
        &self,
        exchange: &str,
        routing_key: &str,
        pattern: Option<&str>,
        queue: &str,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), PluginError> {
        if !matches!(self.inner.authority, Authority::DefaultServices)
            || exchange != "celery"
            || routing_key != "celery"
            || pattern.is_some_and(|p| !p.is_empty())
            || queue != "celery"
        {
            return Err(PluginError::Unsupported);
        }
        let lease = self.lease(owner).map_err(transport)?;
        self.inner.broker.command_classified::<i64,_>("return redis.call('SADD',KEYS[1],'celery'..string.char(6,22)..string.char(6,22)..'celery')",&["_kombu.binding.celery".into()],&[],lease).await.map(|_|()).map_err(|e|if e==ClassifiedError::Capacity{PluginError::Capacity}else{PluginError::Backend})
    }
}
