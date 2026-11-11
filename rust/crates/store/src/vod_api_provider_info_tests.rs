//! Source-derived cached provider-info extension and host controls.
//! Actual Source90 TCP and owned-resource gates passed; canonical app,
//! provider refresh, image serving and full React modal gates are separate.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::vod_api_provider_info_dto as dto;
use serde_json::{json, Value};
#[test]
fn source_cached_movie_projection() {
    let source: Value = serde_json::from_str(include_str!(
        "../datasets/vod-cached-provider-info-source.json"
    ))
    .unwrap();
    let mut count = 0;
    for record in source["records"].as_array().unwrap() {
        if record["endpoint"] != "movies"
            || record["method"] != "GET"
            || record["expected"]["status"] != 200
        {
            continue;
        }
        let metadata = &record["private_pre_request_metadata"];
        let expected = &record["expected"]["json"];
        let account = &expected["m3u_account"];
        let account_id = account["id"].as_i64().unwrap();
        let selected_id = record["query"]
            .as_str()
            .unwrap()
            .split('&')
            .filter_map(|part| part.strip_prefix("relation_id="))
            .next_back()
            .and_then(|value| value.parse::<i64>().ok());
        let relation = metadata["movie_relations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| {
                row["m3u_account_id"] == account_id && selected_id.is_none_or(|id| row["id"] == id)
            })
            .unwrap();
        let logo = json!({"id":metadata["movie"]["logo_id"],"url":"https://fixture.invalid/logo.jpg","name":"rust-probe-vod-rest-shared-logo"});
        let actual = dto::movie(
            &metadata["movie"],
            relation,
            account,
            Some(&logo),
            "http://localhost",
        )
        .unwrap();
        assert_eq!(
            actual, *expected,
            "{} query={} role={}",
            record["label"], record["query"], record["role"]
        );
        count += 1;
    }
    assert!(count >= 10, "only {count} source successes");
}

#[test]
fn source_cached_series_header_projection() {
    let source: Value = serde_json::from_str(include_str!(
        "../datasets/vod-cached-provider-info-source.json"
    ))
    .unwrap();
    let mut count = 0;
    for record in source["records"].as_array().unwrap() {
        if record["endpoint"] != "series"
            || record["method"] != "GET"
            || record["expected"]["status"] != 200
        {
            continue;
        }
        let metadata = &record["private_pre_request_metadata"];
        let mut expected = record["expected"]["json"].clone();
        expected.as_object_mut().unwrap().remove("episodes");
        let account = &expected["m3u_account"];
        let logo = json!({"id":metadata["series"]["logo_id"],"url":"https://fixture.invalid/logo.jpg","name":"rust-probe-vod-rest-shared-logo"});
        // Category/account values are captured serializer-context inputs here;
        // selection, SQL visibility and cache eligibility are separate actor gates.
        let category = json!({"id":expected["category_id"],"name":expected["category_name"]});
        let actual = dto::series_header(
            &metadata["series"],
            &metadata["series_relation"],
            account,
            Some(&category),
            Some(&logo),
            "http://localhost",
        )
        .unwrap();
        assert_eq!(
            actual, expected,
            "{} query={} role={}",
            record["label"], record["query"], record["role"]
        );
        count += 1;
    }
    assert!(count >= 10, "only {count} source successes");
}

#[test]
fn source_cached_info_eligibility_and_series_errors() {
    use super::vod_api_provider_info_policy::{self as policy, Eligibility, Fault};
    let source: Value = serde_json::from_str(include_str!(
        "../datasets/vod-cached-provider-info-source.json"
    ))
    .unwrap();
    let mut cached = 0;
    let mut errors = 0;
    for record in source["records"].as_array().unwrap() {
        if record["method"] != "GET" {
            continue;
        }
        let metadata = &record["private_pre_request_metadata"];
        let query: Vec<(String, String)> = url::form_urlencoded::parse(
            record["query"]
                .as_str()
                .unwrap()
                .trim_start_matches('?')
                .as_bytes(),
        )
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
        let series = record["endpoint"] == "series";
        let relation = if series {
            &metadata["series_relation"]
        } else {
            &metadata["movie_relations"][0]
        };
        let key = if series {
            "last_episode_refresh"
        } else {
            "last_advanced_refresh"
        };
        let now = chrono::DateTime::parse_from_rfc3339(relation[key].as_str().unwrap())
            .unwrap()
            .with_timezone(&chrono::Utc)
            + chrono::Duration::seconds(1);
        let observed = if series {
            policy::series(relation, &query, now)
        } else {
            policy::movie(relation, &query, now)
        };
        if record["expected"]["status"] == 200 {
            assert_eq!(observed, Ok(Eligibility::Cached), "{record}");
            cached += 1;
        }
        if series && record["expected"]["status"] == 500 {
            let Err(Fault::Series(message)) = observed else {
                panic!("missing Source series exception: {observed:?}")
            };
            assert_eq!(
                format!("Failed to fetch series information: {message}"),
                record["expected"]["json"]["error"].as_str().unwrap()
            );
            errors += 1;
        }
    }
    assert_eq!(errors, 2);
    assert!(cached >= 25);
}
