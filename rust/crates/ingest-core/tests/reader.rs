use dispatcharr_ingest_core::{Entries, Error, Limits, ReadError};
use std::io::{BufReader, Cursor};

#[test]
fn small_input_buffers_preserve_unicode_invalid_utf8_and_universal_newlines() {
    let input=b"#EXTM3U\r\n#EXTINF:-1,Caf\xc3\xa9\xff\rhttp://fixture/one\n#EXTINF:-1,Two\r\nudp://@239.1.2.3:1234\r#EXTINF:-1,Discard";
    for capacity in 1..=17 {
        let reader = BufReader::with_capacity(capacity, Cursor::new(input));
        let rows = Entries::new(reader, Limits::default())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "Café");
        assert_eq!(rows[1].url.as_deref(), Some("udp://239.1.2.3:1234"));
    }
}

#[test]
fn overlong_line_errors_before_reading_the_rest_and_iterator_does_not_resume() {
    let reader =
        BufReader::with_capacity(3, Cursor::new(b"#EXTINF:-1,Title\nhttp://must-not-import"));
    let mut entries = Entries::new(
        reader,
        Limits {
            max_line_bytes: 5,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        entries.next(),
        Some(Err(ReadError::Parser(Error::LineCapacity)))
    ));
    assert!(entries.next().is_none());
}
