//! Actual Redis differential of captured original hop/cap policy effects.
//! Original Dispatcharr authors/contributors retain all application credit.
use dispatcharr_clients::timeshift::{self, Admission, AdmissionMode};
use redis::{AsyncCommands, Script};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and TIMESHIFT_ADMISSION_ORACLE original-source capture"]
async fn original_source_hop_limit_order_and_existing_connection_effects_match() {
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("TIMESHIFT_ADMISSION_ORACLE").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(fixture["capture_complete"], true);
    assert_eq!(fixture["events"].as_array().unwrap().len(), 6);
    let mut redis = redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    for event in fixture["events"].as_array().unwrap() {
        let input = &event["input"];
        let mut owned = BTreeSet::new();
        for record in input["seed"].as_array().unwrap() {
            let key = record["key"].as_str().unwrap();
            owned.insert(key.to_owned());
            let _: () = redis.del(key).await.unwrap();
            if let Some(fields) = record["fields"].as_object() {
                let fields: Vec<_> = fields
                    .iter()
                    .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
                    .collect();
                let _: () = redis.hset_multiple(key, &fields).await.unwrap();
            } else if let Some(members) = record["members"].as_array() {
                let members: Vec<_> = members
                    .iter()
                    .map(|value| value.as_str().unwrap())
                    .collect();
                let _: () = redis.sadd(key, &members).await.unwrap();
            } else if let Some(value) = record["string"].as_str() {
                let _: () = redis.set_ex(key, value, 60).await.unwrap();
            }
        }
        for key in input["stops"].as_object().unwrap().values() {
            let key = key.as_str().unwrap();
            owned.insert(key.into());
            let _: () = redis.del(key).await.unwrap();
        }
        let user = input["user_id"].as_i64().unwrap();
        let sid = input["session_id"].as_str().unwrap();
        let channel = input["channel_id"].as_i64().unwrap();
        let media = input["programme_media_id"].as_str().unwrap();
        let policy = &input["policy"];
        let policy = dispatcharr_contracts::UserLimitPolicy {
            terminate_on_limit_exceeded: policy["terminate_on_limit_exceeded"].as_bool().unwrap(),
            prioritize_single_client_channels: policy["prioritize_single_client_channels"]
                .as_bool()
                .unwrap(),
            ignore_same_channel_connections: policy["ignore_same_channel_connections"]
                .as_bool()
                .unwrap(),
            terminate_oldest: policy["terminate_oldest"].as_bool().unwrap(),
        };
        let fields = BTreeMap::from([
            ("user_id".into(), user.to_string()),
            ("username".into(), "fixture".into()),
            ("programme_vid".into(), format!("{media}_provider")),
        ]);
        let prepared = timeshift::prepare(Admission {
            mode: if input["mode"] == "check" {
                AdmissionMode::Check
            } else {
                AdmissionMode::Create
            },
            session_id: sid,
            channel_id: channel,
            programme_media_id: media,
            fields: &fields,
            stream_limit: 1,
            policy: &policy,
            owner_token: input["owner_token"].as_str().unwrap(),
            now: "2",
            ttl: Duration::from_secs(60),
            scan_limit: 50000,
        })
        .unwrap();
        for key in &prepared.keys {
            owned.insert(key.clone());
        }
        let script = Script::new(timeshift::SCRIPT);
        let mut invocation = script.prepare_invoke();
        for key in &prepared.keys {
            invocation.key(key);
        }
        for arg in &prepared.args {
            invocation.arg(arg);
        }
        let result: Vec<String> = invocation.invoke_async(&mut redis).await.unwrap();
        let allowed = match timeshift::decode(result) {
            Ok(value) => value,
            Err(dispatcharr_clients::Error::UserLimit) => false,
            Err(error) => panic!("unexpected admission result {error:?}"),
        };
        assert_eq!(json!(allowed), event["allowed"], "{}", event["case"]);
        for (label, key) in input["observe"].as_object().unwrap() {
            let exists: bool = redis.exists(key.as_str().unwrap()).await.unwrap();
            assert_eq!(
                json!(exists),
                event["state"]["exists"][label],
                "{} {label}",
                event["case"]
            );
        }
        for (label, key) in input["stops"].as_object().unwrap() {
            let value: Option<String> = redis.get(key.as_str().unwrap()).await.unwrap();
            assert_eq!(
                json!(value),
                event["state"]["stops"][label],
                "{} {label}",
                event["case"]
            );
            let ttl: i64 = redis.ttl(key.as_str().unwrap()).await.unwrap();
            if value.is_some() {
                let maximum = if key.as_str().unwrap().starts_with("live:") {
                    30
                } else {
                    60
                };
                assert!((0..=maximum).contains(&ttl));
            } else {
                assert_eq!(ttl, -2);
            }
        }
        // A successful create publishes one pending reservation atomically.
        // Original policy helpers only check/stop existing clients; media later
        // registers theirs. Do not mistake this intentional pending record for
        // an original existing-connection effect or full statistics parity.
        if input["mode"] == "create" && allowed {
            assert!(redis.exists::<_, bool>(&prepared.keys[0]).await.unwrap());
        }
        let keys: Vec<_> = owned.into_iter().collect();
        if !keys.is_empty() {
            let _: () = redis.del(keys).await.unwrap();
        }
    }
}
