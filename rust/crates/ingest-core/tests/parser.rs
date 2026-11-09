use dispatcharr_ingest_core::{Error, Limits, Parser};

#[test]
fn streams_one_pending_entry_and_preserves_explicit_group_and_url_quirks() {
    let mut parser = Parser::new(Limits::default()).unwrap();
    assert!(parser
        .feed("#EXTINF:-1 GROUP-TITLE='' tvg-name='epg-key',Title")
        .unwrap()
        .is_none());
    parser.feed("#EXTGRP: fallback ").unwrap();
    parser.feed("#KODIPROP: ignored").unwrap();
    parser.feed("#EXTVLCOPT: http-referrer=test  ").unwrap();
    let entry = parser.feed(" udp://@239.1.2.3:1234 ").unwrap().unwrap();
    assert_eq!(entry.name, "Title");
    assert_eq!(entry.attributes["group-title"], "");
    assert_eq!(entry.vlc_opts.unwrap(), [" http-referrer=test"]);
    assert_eq!(entry.url.unwrap(), "udp://239.1.2.3:1234");
    assert!(parser.feed("http://orphan").unwrap().is_none());
    parser.feed("#EXTINF:-1,discarded").unwrap();
    parser.feed("#EXTINF invalid").unwrap();
    assert!(parser
        .feed("http://must-not-attach-to-old-entry")
        .unwrap()
        .is_none());
}

#[test]
fn capacity_failure_discards_pending_and_never_emits_the_rejected_entry() {
    let mut parser = Parser::new(Limits {
        max_vlc_options: 1,
        ..Default::default()
    })
    .unwrap();
    parser.feed("#EXTINF:-1,Title").unwrap();
    parser.feed("#EXTVLCOPT:first").unwrap();
    assert_eq!(parser.feed("#EXTVLCOPT:second"), Err(Error::OptionCapacity));
    assert!(parser.feed("http://must-not-import").is_err());
    let mut parser = Parser::new(Limits {
        max_line_bytes: 4,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(parser.feed("longer"), Err(Error::LineCapacity));
}
