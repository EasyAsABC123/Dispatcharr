//! Actual Source inline renderer statements, not a second reference implementation.
use super::*;
#[test]
fn actual_source_programme_xml_and_rejected_metadata() {
    let source: Value =
        serde_json::from_str(include_str!("../datasets/source-render.json")).unwrap();
    let cases = source["real_programme_cases"].as_array().unwrap();
    assert_eq!(cases.len(), 378);
    for (index, case) in cases.iter().enumerate() {
        let data = &case["programme"];
        let row = XmltvProgramme {
            epg_id: data["epg_id"].as_i64().unwrap(),
            start_time: data["start_time"].as_str().unwrap().parse().unwrap(),
            end_time: data["end_time"].as_str().unwrap().parse().unwrap(),
            title: data["title"].as_str().unwrap().into(),
            sub_title: data["sub_title"].as_str().map(str::to_owned),
            description: data["description"].as_str().map(str::to_owned),
            tvg_id: String::new(),
            custom_properties: Some(data["custom_properties"].clone()),
        };
        let result = programme(
            Programme {
                id: data["id"].as_i64().unwrap(),
                channel: case["channel_id"].as_str().unwrap(),
                origin: case["origin"].as_str().unwrap(),
                row: &row,
            },
            1024 * 1024,
        );
        if case.get("error").is_some() {
            assert!(
                result.is_err(),
                "Source case {index} rejects {}",
                data["custom_properties"]
            );
        } else {
            assert_eq!(
                result.unwrap_or_else(|error| panic!(
                    "Source case {index} expectedXML, got{error:?}: {}",
                    data["custom_properties"]
                )),
                case["xml"].as_str().unwrap(),
                "Source case {index}"
            );
        }
    }
}
#[test]
fn metadata_budget_checks_precede_recursive_render_scratch() {
    let base = || XmltvProgramme {
        epg_id: 1,
        start_time: "2026-01-02T03:04:05+00:00".parse().unwrap(),
        end_time: "2026-01-02T04:04:05+00:00".parse().unwrap(),
        title: "fixture".into(),
        sub_title: None,
        description: None,
        tvg_id: String::new(),
        custom_properties: None,
    };
    let mut row = base();
    let mut deep = Value::String("leaf".into());
    for _ in 0..40 {
        deep = serde_json::json!({"episode":deep});
    }
    row.custom_properties = Some(deep);
    assert_eq!(
        programme(
            Programme {
                id: 1,
                channel: "1",
                origin: "http://fixture",
                row: &row
            },
            1024 * 1024
        ),
        Err(RenderError::Capacity)
    );
    row = base();
    row.custom_properties = Some(serde_json::json!({"episode":vec!["";10001]}));
    assert_eq!(
        programme(
            Programme {
                id: 1,
                channel: "1",
                origin: "http://fixture",
                row: &row
            },
            1024 * 1024
        ),
        Err(RenderError::Capacity)
    );
    row = base();
    row.description = Some("<&>".repeat(1024));
    assert_eq!(
        programme(
            Programme {
                id: 1,
                channel: "1",
                origin: "http://fixture",
                row: &row
            },
            1024
        ),
        Err(RenderError::Capacity)
    );
}
