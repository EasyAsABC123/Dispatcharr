//! Bounded observed local-VODLogo child protocol, not a generic parent option.
//! Original Dispatcharr/Celery/Kombu authors retain protocol/application credit.
use crate::{
    decode, protocol, recording_artwork::RefreshSeriesChild, series_protocol::SeriesMessage,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use uuid::Uuid;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub task_id: Uuid,
    pub root: Uuid,
    pub parent: Uuid,
    pub recording: i64,
    pub eta: DateTime<Utc>,
}
fn canonical(value: &Value) -> Option<Uuid> {
    let s = value.as_str()?;
    let id = Uuid::parse_str(s).ok()?;
    (id.to_string() == s && id.get_version_num() == 4 && id.get_variant() == uuid::Variant::RFC4122)
        .then_some(id)
}
pub fn parse(raw: &[u8]) -> Option<Message> {
    if raw.len() > protocol::MAX_ENVELOPE {
        return None;
    }
    let mut frame: Value = serde_json::from_slice(raw).ok()?;
    let h = &frame["headers"];
    if h["task"] != "apps.channels.tasks.prefetch_recording_artwork" {
        return None;
    }
    let task_id = canonical(&h["id"])?;
    let root = canonical(&h["root_id"])?;
    let parent = canonical(&h["parent_id"])?;
    if task_id == root || task_id == parent || parent == root {
        return None;
    }
    let eta_text = h["eta"].as_str()?;
    let eta: DateTime<Utc> = eta_text.parse().ok()?;
    if eta.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string() != eta_text {
        return None;
    }
    let body: Value =
        serde_json::from_slice(&STANDARD.decode(frame["body"].as_str()?).ok()?).ok()?;
    let recording = body.get(0)?.get(0)?.as_i64()?;
    if !(1..=i64::from(i32::MAX)).contains(&recording)
        || body
            != json!([[recording],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        || h["argsrepr"] != format!("[{recording}]")
    {
        return None;
    }
    // Validate every shared envelope/option with the unchanged strict identity
    // parser; only the captured task, lineage, argument and ETA are projected.
    frame["headers"]["task"] = json!("apps.backups.tasks.create_backup_task");
    frame["headers"]["root_id"] = json!(task_id.to_string());
    frame["headers"]["parent_id"] = Value::Null;
    frame["headers"]["eta"] = Value::Null;
    frame["headers"]["argsrepr"] = json!("()");
    frame["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([[],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .ok()?
    ));
    crate::worker::create_identity(&serde_json::to_vec(&frame).ok()?).ok()?;
    Some(Message {
        task_id,
        root,
        parent,
        recording,
        eta,
    })
}
/// Minted only from a previously proven Refresh→Series capability, its parsed
/// parent and actual bounded completed Series metadata. Trusted backend/frame
/// inputs are not a cryptographic proof. Fields and constructor are not generic.
#[derive(Clone, Debug)]
pub struct SeriesArtworkChild {
    message: Message,
}
impl SeriesArtworkChild {
    pub fn from_completed_series(
        refresh_child: RefreshSeriesChild,
        parent: &SeriesMessage,
        backend: &[u8],
        child: &Message,
    ) -> Option<Self> {
        refresh_child.admit(parent)?;
        if child.root != parent.root || child.parent != parent.task_id {
            return None;
        }
        let record = decode::decode(backend, parent.task_id).ok()?;
        if record.state != decode::State::Success
            || record.traceback.is_some()
            || record.date_done.is_none()
            || !record.tagged_values.is_empty()
            || record.result
                != json!({"scheduled":1,"details":[{"tvg_id":"rust.owned.parented.artwork","title":"Owned Parented Artwork","status":"ok","created":1}]})
        {
            return None;
        }
        let raw: Value = serde_json::from_slice(backend).ok()?;
        let fields = raw.as_object()?;
        let allowed = [
            "status",
            "result",
            "traceback",
            "children",
            "date_done",
            "task_id",
            "parent_id",
        ];
        if fields.len() != allowed.len()
            || fields.keys().any(|k| !allowed.contains(&k.as_str()))
            || raw["task_id"] != parent.task_id.to_string()
            || raw["parent_id"] != parent.root.to_string()
        {
            return None;
        }
        let children = raw["children"].as_array()?;
        if children.len() != 2 {
            return None;
        }
        let mut ids = Vec::with_capacity(2);
        for tuple in children {
            let id = canonical(tuple.get(0)?.get(0)?)?;
            if *tuple != json!([[id.to_string(), null], null])
                || id == parent.task_id
                || id == parent.root
                || ids.contains(&id)
            {
                return None;
            }
            ids.push(id);
        }
        if !ids.contains(&child.task_id) {
            return None;
        }
        Some(Self {
            message: child.clone(),
        })
    }
    pub(crate) fn admits(&self, message: &Message) -> bool {
        self.message == *message
    }
}

#[cfg(test)]
mod controls {
    use super::*;
    fn source() -> Value {
        serde_json::from_str(include_str!(
            "../datasets/source-local-recording-artwork.json"
        ))
        .unwrap()
    }
    fn frame(source: &Value, index: usize) -> Value {
        let raw = STANDARD
            .decode(source["frames_raw_base64"][index].as_str().unwrap())
            .unwrap();
        let mut frame: Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(
            frame["properties"]["delivery_info"],
            json!({"exchange":"","routing_key":"rust-owned-parented-artwork"})
        );
        frame["properties"]["delivery_info"]["routing_key"] = json!("celery");
        frame
    }
    fn lineage() -> (RefreshSeriesChild, SeriesMessage) {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-parented-recordings.json"))
                .unwrap();
        let raw = STANDARD
            .decode(source["refresh_frames_raw_base64"][0].as_str().unwrap())
            .unwrap();
        let mut frame: Value = serde_json::from_slice(&raw).unwrap();
        frame["properties"]["delivery_info"]["routing_key"] = json!("celery");
        let refresh = crate::refresh_protocol::parse(&serde_json::to_vec(&frame).unwrap()).unwrap();
        let parent = SeriesMessage {
            task_id: Uuid::parse_str(
                source["observations"][1]["tasks"][0]["id"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap(),
            root: refresh.task_id,
            parent: Some(refresh.task_id),
        };
        let backend =
            serde_json::to_vec(&source["observations"][0]["tasks"][0]["backend"]).unwrap();
        (
            RefreshSeriesChild::from_completed_refresh(&refresh, &backend, &parent).unwrap(),
            parent,
        )
    }
    #[test]
    fn both_actual_children_require_completed_series_and_inherited_refresh_root() {
        let source = source();
        let (refresh, parent) = lineage();
        for index in 0..2 {
            let frame = frame(&source, index);
            let message = parse(&serde_json::to_vec(&frame).unwrap()).unwrap();
            assert_eq!(message.recording, 1);
            assert_ne!(message.root, message.parent);
            let metadata = serde_json::to_vec(&source["Series_backend"]).unwrap();
            let cap =
                SeriesArtworkChild::from_completed_series(refresh, &parent, &metadata, &message)
                    .unwrap();
            assert!(cap.admits(&message));
            let mut wrong = message.clone();
            wrong.task_id = Uuid::new_v4();
            assert!(!cap.admits(&wrong));
        }
    }
    #[test]
    fn raw_options_arguments_lineage_spelling_and_eta_cannot_be_normalized_into_authority() {
        let source = source();
        let original = frame(&source, 0);
        for key in ["id", "parent_id", "root_id"] {
            let mut changed = original.clone();
            changed["headers"][key] =
                json!(original["headers"][key].as_str().unwrap().to_uppercase());
            assert!(parse(&serde_json::to_vec(&changed).unwrap()).is_none());
        }
        for (key, value) in [
            ("retries", json!(1)),
            ("ignore_result", json!(true)),
            ("expires", json!("2026-11-13T00:00:00+00:00")),
            ("kwargsrepr", json!("{'force':True}")),
            ("argsrepr", json!("(1,)")),
            ("eta", Value::Null),
            ("eta", json!("2026-11-13T07:06:40Z")),
        ] {
            let mut changed = original.clone();
            changed["headers"][key] = value;
            assert!(
                parse(&serde_json::to_vec(&changed).unwrap()).is_none(),
                "{key}"
            );
        }
        for recording in [
            json!(0),
            json!(-1),
            json!(true),
            json!(2147483648_i64),
            json!("1"),
        ] {
            let mut changed = original.clone();
            changed["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([[recording],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).unwrap()));
            assert!(parse(&serde_json::to_vec(&changed).unwrap()).is_none());
        }
        let mut changed = original.clone();
        changed["properties"]["delivery_info"]["routing_key"] = json!("unknown");
        assert!(parse(&serde_json::to_vec(&changed).unwrap()).is_none());
        assert!(parse(&vec![0; protocol::MAX_ENVELOPE + 1]).is_none());
    }
    #[test]
    fn missing_duplicate_wrong_parent_failed_or_uncaptured_result_cannot_mint_child() {
        let source = source();
        let (refresh, parent) = lineage();
        let child = parse(&serde_json::to_vec(&frame(&source, 0)).unwrap()).unwrap();
        for case in 0..8 {
            let mut metadata = source["Series_backend"].clone();
            match case {
                0 => metadata["children"] = json!([]),
                1 => metadata["children"][1] = metadata["children"][0].clone(),
                2 => metadata["parent_id"] = json!(Uuid::new_v4().to_string()),
                3 => metadata["status"] = json!("PENDING"),
                4 => metadata["result"]["scheduled"] = json!(0),
                5 => metadata["extra"] = json!(true),
                6 => metadata["children"][0][0][0] = json!(Uuid::nil().to_string()),
                _ => metadata["date_done"] = Value::Null,
            }
            assert!(
                SeriesArtworkChild::from_completed_series(
                    refresh,
                    &parent,
                    &serde_json::to_vec(&metadata).unwrap(),
                    &child
                )
                .is_none(),
                "case {case}"
            );
        }
        let mut wrong = parent.clone();
        wrong.parent = None;
        assert!(SeriesArtworkChild::from_completed_series(
            refresh,
            &wrong,
            &serde_json::to_vec(&source["Series_backend"]).unwrap(),
            &child
        )
        .is_none());
    }
}
