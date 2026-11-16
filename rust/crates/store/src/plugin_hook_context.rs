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
                | "apps.epg.tasks.fetch_sd_mapped_guide_batch"
                | "apps.epg.tasks.fetch_sd_guide_for_epg"
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

#[cfg(test)]
mod actual_sd_publication_tests {
    use super::*;

    #[test]
    fn source_observed_sd_frames_keep_identity_body_and_reject_unqualified_names() {
        let dataset: Value = serde_json::from_str(include_str!(
            "../datasets/source-sd-hook-context-publications.json"
        ))
        .unwrap();
        let vectors = dataset["vectors"].as_array().unwrap();
        assert_eq!(vectors.len(), 2);
        for vector in vectors {
            let raw = vector["raw_frame"].as_str().unwrap();
            let frame: Value = serde_json::from_str(raw).unwrap();
            let context = PluginHookTaskContext::from_native_claim(raw.as_bytes()).unwrap();
            assert_eq!(context.task_id(), frame["headers"]["id"].as_str().unwrap());
            assert_eq!(context.task_name(), vector["task_name"].as_str().unwrap());
            assert_eq!(context.frame, frame);
            let decoded = STANDARD.decode(frame["body"].as_str().unwrap()).unwrap();
            let actual_body: Value = serde_json::from_slice(&decoded).unwrap();
            assert_eq!(actual_body[0], serde_json::json!([3]));
            assert_eq!(actual_body.as_array().unwrap().len(), 3);

            let mut invalid = frame.clone();
            invalid["headers"]["task"] = Value::String(
                context
                    .task_name()
                    .replace("apps.epg.tasks.", "apps.epg.sd_tasks."),
            );
            assert!(PluginHookTaskContext::from_native_claim(
                &serde_json::to_vec(&invalid).unwrap()
            )
            .is_err());
            for (key, value) in [
                ("content-type", "text/plain"),
                ("content-encoding", "utf-16"),
                ("body", "!not-base64!"),
            ] {
                let mut invalid = frame.clone();
                invalid[key] = Value::String(value.into());
                assert!(PluginHookTaskContext::from_native_claim(
                    &serde_json::to_vec(&invalid).unwrap()
                )
                .is_err());
            }
            let mut invalid = frame.clone();
            invalid["properties"]["body_encoding"] = Value::String("raw".into());
            assert!(PluginHookTaskContext::from_native_claim(
                &serde_json::to_vec(&invalid).unwrap()
            )
            .is_err());
            let mut invalid = frame.clone();
            invalid["body"] = Value::String(STANDARD.encode(b"{}"));
            assert!(PluginHookTaskContext::from_native_claim(
                &serde_json::to_vec(&invalid).unwrap()
            )
            .is_err());
            for id in [String::new(), "x".repeat(256), "unsafe\nidentity".into()] {
                let mut invalid = frame.clone();
                invalid["headers"]["id"] = Value::String(id);
                assert!(PluginHookTaskContext::from_native_claim(
                    &serde_json::to_vec(&invalid).unwrap()
                )
                .is_err());
            }
        }
        assert!(PluginHookTaskContext::from_native_claim(&[]).is_err());
        assert!(PluginHookTaskContext::from_native_claim(&vec![b' '; 65537]).is_err());
    }
}
