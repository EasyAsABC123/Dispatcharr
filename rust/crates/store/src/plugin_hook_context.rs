//! Native-owned task/event compatibility contracts; original Dispatcharr/Celery
//! authors retain all algorithm credit. Metadata is never business authority.
use crate::StoreError;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::Value;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeTaskTrailState {
    Open,
    Closed,
    Unsettled,
}
pub struct NativeTaskTrailEntry {
    pub ordinal: u64,
    pub source_result_tuple: Value,
}
pub struct NativeTaskTrailDelta {
    pub next_cursor: u64,
    pub entries: Vec<NativeTaskTrailEntry>,
    pub state: NativeTaskTrailState,
}
/// Implement ONLY on actual native settled ChildPublishers. JSON supplied by a
/// Python caller is not an implementation and cannot grant child authority.
pub trait NativeTaskTrail: Send + Sync {
    fn claimed_task_id(&self) -> &str;
    fn settled_after(&self, cursor: u64) -> Result<NativeTaskTrailDelta, StoreError>;
}
#[derive(Clone)]
pub struct PluginHookTaskContext {
    pub(crate) task_id: String,
    pub(crate) task_name: String,
    pub(crate) frame: Value,
}
fn unsupported() -> StoreError {
    StoreError::Unsupported("Claimed native plugin hook context unavailable")
}
impl PluginHookTaskContext {
    /// Caller supplies the ACTUAL already-claimed native task envelope. This is
    /// not an HTTP/IPC method and grants no queue/SQL/recording capability.
    pub fn from_native_claim(raw: &[u8]) -> Result<Self, StoreError> {
        if raw.is_empty() || raw.len() > 65536 {
            return Err(unsupported());
        }
        let frame: Value = serde_json::from_slice(raw).map_err(|_| unsupported())?;
        let name = frame["headers"]["task"].as_str().ok_or_else(unsupported)?;
        if !matches!(
            name,
            "apps.epg.tasks.refresh_all_epg_data"
                | "apps.epg.tasks.refresh_epg_data"
                | "apps.epg.tasks.parse_programs_for_tvg_id"
                | "apps.epg.sd_tasks.fetch_sd_mapped_guide_batch"
                | "apps.epg.sd_tasks.fetch_sd_guide_for_epg"
        ) {
            return Err(unsupported());
        }
        let id = frame["headers"]["id"]
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 255 && !s.chars().any(char::is_control))
            .ok_or_else(unsupported)?;
        if frame["content-type"] != "application/json"
            || frame["content-encoding"] != "utf-8"
            || frame["properties"]["body_encoding"] != "base64"
        {
            return Err(unsupported());
        }
        let decoded = STANDARD
            .decode(frame["body"].as_str().ok_or_else(unsupported)?)
            .map_err(|_| unsupported())?;
        let body: Value = serde_json::from_slice(&decoded).map_err(|_| unsupported())?;
        if !body.as_array().is_some_and(|v| {
            v.len() == 3 && v[0].is_array() && v[1].is_object() && v[2].is_object()
        }) {
            return Err(unsupported());
        }
        Ok(Self {
            task_id: id.to_owned(),
            task_name: name.to_owned(),
            frame,
        })
    }
    pub fn task_id(&self) -> &str {
        &self.task_id
    }
    pub fn task_name(&self) -> &str {
        &self.task_name
    }
}
/// Actual registered-plugin PUBLISH observed inside an owned hook invocation.
/// Private fields prohibit constructing a receipt from Python-reported trails.
/// This never converts to DVR/Refresh/Index authority.
#[derive(Clone)]
pub struct PluginHookChildReceipt {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) publisher: String,
    pub(crate) raw_sha256: String,
}
impl PluginHookChildReceipt {
    pub fn task_id(&self) -> &str {
        &self.id
    }
    pub fn task_name(&self) -> &str {
        &self.name
    }
    pub fn publisher_context_task_id(&self) -> &str {
        &self.publisher
    }
    pub fn raw_sha256(&self) -> &str {
        &self.raw_sha256
    }
}
pub struct PluginHookReport {
    pub actual_published_children: Vec<PluginHookChildReceipt>,
    /// Exact pinned Context trail metadata; NOT an authority/claim grant. Group
    /// IDs, add_to_parent=False and user mutation differ from physical receipts.
    pub observed_request_children: Value,
    pub attempted: u64,
    pub completed: u64,
    pub caught: u64,
}

#[derive(Debug)]
pub enum PluginTemplateRender {
    Rendered(String),
    Caught {
        actual_class: String,
        message: String,
    },
}

pub struct PluginTemplateOutcome {
    pub render: PluginTemplateRender,
    pub current_payload: Value,
    pub source_json_loads_succeeded: Option<bool>,
}
