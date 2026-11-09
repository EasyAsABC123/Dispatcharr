use dispatcharr_ingest_core::{project_xmltv_programme, XmltvElement};
use serde_json::json;

fn programme(children: Vec<XmltvElement>) -> XmltvElement {
    let mut node = XmltvElement {
        tag: "programme".into(),
        children,
        ..Default::default()
    };
    for (key, value) in [
        ("channel", "one"),
        ("start", "20261109000000 +0000"),
        ("stop", "20261108000000 +0000"),
    ] {
        node.attrs.insert(key.into(), value.into());
    }
    node
}
fn child(tag: &str, text: Option<&str>) -> XmltvElement {
    XmltvElement {
        tag: tag.into(),
        text: text.map(String::from),
        ..Default::default()
    }
}

#[test]
fn original_last_title_wins_and_missing_description_remains_null() {
    let row = project_xmltv_programme(
        &programme(vec![child("title", Some("first")), child("title", None)]),
        11,
        255,
    )
    .unwrap();
    assert_eq!(row.title, "No Title");
    assert_eq!(row.description, None);
    assert_eq!(row.sub_title, None);
    assert!(
        row.end_time < row.start_time,
        "original projection does not validate interval order"
    );
}
#[test]
fn description_fills_only_missing_episode_and_keeps_existing_season() {
    let mut episode = child("episode-num", Some("3.."));
    episode.attrs.insert("system".into(), "xmltv_ns".into());
    let row = project_xmltv_programme(
        &programme(vec![
            episode,
            child("desc", Some("ſeaſon 8 Epİſode 5: News")),
        ]),
        11,
        255,
    )
    .unwrap();
    let properties = row.custom_properties.unwrap();
    assert_eq!(properties["season"], json!(4));
    assert_eq!(properties["episode"], json!(5));
    assert_eq!(properties["season_episode_source"], "description");
    assert_eq!(row.description.as_deref(), Some("News"));
}
#[test]
fn title_limit_counts_unicode_characters() {
    let text = "東京😀".repeat(200);
    let row =
        project_xmltv_programme(&programme(vec![child("title", Some(&text))]), 11, 255).unwrap();
    assert_eq!(row.title.chars().count(), 255);
}
