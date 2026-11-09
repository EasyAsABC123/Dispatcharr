use dispatcharr_ingest_core::{extract_xmltv_properties, XmltvElement, XmltvMetadataError};
use serde_json::json;

fn node(tag: &str, text: &str) -> XmltvElement {
    XmltvElement {
        tag: tag.into(),
        text: Some(text.into()),
        ..Default::default()
    }
}

#[test]
fn source_integer_range_and_episode_increment_are_not_limited_to_i64() {
    let mut episode = node("episode-num", "9223372036854775808.-2.");
    episode.attrs.insert("system".into(), "xmltv_ns".into());
    let programme = XmltvElement {
        children: vec![episode],
        ..node("programme", "")
    };
    let result = extract_xmltv_properties(&programme).unwrap();
    assert_eq!(result["season"].to_string(), "9223372036854775809");
    assert_eq!(result["episode"], json!(-1));
}

#[test]
fn source_first_empty_rating_suppresses_later_rating() {
    let programme = XmltvElement {
        children: vec![
            node("rating", ""),
            XmltvElement {
                children: vec![node("value", "PG")],
                ..node("rating", "")
            },
        ],
        ..node("programme", "")
    };
    assert!(!extract_xmltv_properties(&programme)
        .unwrap()
        .contains_key("rating"));
}

#[test]
fn nesting_and_total_bytes_are_rejected_before_projection() {
    let mut nested = node("category", "leaf");
    for _ in 0..34 {
        nested = XmltvElement {
            children: vec![nested],
            ..node("programme", "")
        };
    }
    assert_eq!(
        extract_xmltv_properties(&nested),
        Err(XmltvMetadataError::Capacity)
    );
    let large = node("programme", &"x".repeat(4 * 1024 * 1024 + 1));
    assert_eq!(
        extract_xmltv_properties(&large),
        Err(XmltvMetadataError::Capacity)
    );
}

#[test]
fn original_air_date_fills_only_absent_start_and_preserves_channel() {
    let mut episode = node("episode-num", " 2026-06-24 ");
    episode
        .attrs
        .insert("system".into(), "original-air-date".into());
    let mut prior = node("previously-shown", "");
    prior
        .attrs
        .insert("channel".into(), "channel-one.tv".into());
    let programme = XmltvElement {
        children: vec![episode, prior],
        ..node("programme", "")
    };
    let result = extract_xmltv_properties(&programme).unwrap();
    assert_eq!(
        result["previously_shown_details"],
        json!({"channel":"channel-one.tv","start":"2026-06-24"})
    );
}
