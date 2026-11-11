//! Captured Original expiry wire/results and strict rejection controls.
use crate::{
    cleanup_expiry::CleanupExpiration,
    decode::{self, State},
    default_protocol::{self, Kind, Message},
};
use serde_json::{json, Value};
use uuid::Uuid;
fn corpus() -> Value {
    serde_json::from_str(include_str!("../datasets/source-cleanup-expiry-3.json")).unwrap()
}
#[test]
fn actual_source_expiry_envelopes_and_metadata() {
    for case in corpus()["cases"].as_array().unwrap() {
        let meta = &case["metadata"];
        let id = Uuid::parse_str(meta["task_id"].as_str().unwrap()).unwrap();
        let decoded = decode::decode(&serde_json::to_vec(meta).unwrap(), id).unwrap();
        if meta["status"] == "REVOKED" {
            assert_eq!(decoded.state, State::Revoked);
            assert_eq!(decoded.exception.unwrap().class, "TaskRevokedError");
        } else {
            assert_eq!(decoded.state, State::Success);
        }
        if let Some(envelope) = case.get("envelope") {
            let message = default_protocol::parse(&serde_json::to_vec(envelope).unwrap()).unwrap();
            assert_eq!(message.kind, Kind::RedisBackendCleanup);
            assert!(message.expires.is_some());
            assert_eq!(
                default_protocol::parse(&default_protocol::encode(&message).unwrap()).unwrap(),
                message
            );
        }
    }
}
#[test]
fn expiry_authority_and_typed_metadata_rejections() {
    let data = corpus();
    let good = data["cases"][0]["envelope"].clone();
    for (task, expiration, deadline) in [
        (
            "apps.channels.tasks.maintain_recurring_recordings",
            json!("43200000"),
            json!("2026-11-12T04:00:00+00:00"),
        ),
        (
            "apps.m3u.tasks.check_account_expirations",
            json!("43200000"),
            json!("2026-11-12T04:00:00+00:00"),
        ),
        (
            "celery.backend_cleanup",
            json!("43200001"),
            json!("2026-11-12T04:00:00+00:00"),
        ),
        (
            "celery.backend_cleanup",
            json!("1000"),
            json!("2026-11-12T04:00:00-05:00"),
        ),
        (
            "celery.backend_cleanup",
            json!("1000"),
            json!("2026-11-12T04:00:00.000000001+00:00"),
        ),
        ("celery.backend_cleanup", json!("01000"), Value::Null),
    ] {
        let mut value = good.clone();
        value["headers"]["task"] = json!(task);
        value["properties"]["expiration"] = expiration;
        value["headers"]["expires"] = deadline;
        assert!(default_protocol::parse(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    let raw = &data["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["metadata"]["status"] == "REVOKED")
        .unwrap()["metadata"];
    let id = Uuid::parse_str(raw["task_id"].as_str().unwrap()).unwrap();
    for (path, value) in [
        ("/result/exc_module", json!("builtins")),
        ("/result/exc_type", json!("RuntimeError")),
        ("/result/exc_message", json!([true])),
        ("/result/exc_message", json!(["other"])),
        ("/children", json!(["unknown"])),
        ("/traceback", json!("fabricated frame")),
        ("/date_done", Value::Null),
        ("/status", json!("FAILURE")),
    ] {
        let mut bad = raw.clone();
        *bad.pointer_mut(path).unwrap() = value;
        assert!(decode::decode(&serde_json::to_vec(&bad).unwrap(), id).is_err());
    }
    let at = chrono::DateTime::parse_from_rfc3339("2026-11-12T04:00:00+00:00")
        .unwrap()
        .to_utc();
    let message = Message {
        task_id: Uuid::new_v4(),
        kind: Kind::InactiveAccountNotifications,
        expires: Some(CleanupExpiration::new(at, 1000).unwrap()),
    };
    assert!(default_protocol::encode(&message).is_err());
}

#[test]
fn captured_backend_seed_null_children_does_not_widen_result_domain() {
    let data: Value =
        serde_json::from_str(include_str!("../datasets/source-cleanup-expiry-2.json")).unwrap();
    let meta = &data["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "expired-oldSUCCESS")
        .unwrap()["metadata"];
    assert!(meta["children"].is_null());
    let id = Uuid::parse_str(meta["task_id"].as_str().unwrap()).unwrap();
    assert_eq!(
        decode::decode(&serde_json::to_vec(meta).unwrap(), id),
        Err(decode::DecodeError::Malformed)
    );
}

#[test]
fn actual_source_warm_restored_cleanup_flags_and_identity() {
    let data: Value =
        serde_json::from_str(include_str!("../datasets/source-cleanup-ack-3.json")).unwrap();
    let case = &data["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "backend-SETEX-denied")
        .unwrap();
    let restored = &case["after_warm_stop"]["queue"][0];
    assert_eq!(restored["headers"]["redelivered"], true);
    assert_eq!(restored["properties"]["delivery_info"]["redelivered"], true);
    let message = default_protocol::parse(&serde_json::to_vec(restored).unwrap()).unwrap();
    assert_eq!(message.kind, Kind::RedisBackendCleanup);
    assert_eq!(message.task_id.to_string(), restored["headers"]["id"]);
    for path in [
        "/headers/redelivered",
        "/properties/delivery_info/redelivered",
    ] {
        let mut bad = restored.clone();
        *bad.pointer_mut(path).unwrap() = json!(false);
        assert!(default_protocol::parse(&serde_json::to_vec(&bad).unwrap()).is_err());
    }
}
