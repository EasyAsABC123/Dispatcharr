use dispatcharr_xmltv_parser::{parse_xmltv_reader, XmltvError};
use std::{
    io::{self, Read},
    sync::atomic::{AtomicBool, Ordering},
};

#[test]
fn failed_sink_stops_before_later_records() {
    let mut count = 0;
    let result = parse_xmltv_reader(
        &b"<tv><programme/><programme/></tv>"[..],
        &AtomicBool::new(false),
        |_| {
            count += 1;
            Err("database failure")
        },
    );
    assert!(matches!(result, Err(XmltvError::Sink("database failure"))));
    assert_eq!(count, 1);
}
#[test]
fn cancellation_from_sink_cannot_complete_job() {
    let flag = AtomicBool::new(false);
    let mut count = 0;
    let result = parse_xmltv_reader(&b"<tv><programme/><programme/></tv>"[..], &flag, |_| {
        count += 1;
        flag.store(true, Ordering::Release);
        Ok::<_, ()>(())
    });
    assert!(matches!(result, Err(XmltvError::Cancelled)));
    assert_eq!(count, 1);
}
#[test]
fn sink_panic_is_contained_at_foreign_boundary() {
    let result = parse_xmltv_reader(
        &b"<tv><programme/></tv>"[..],
        &AtomicBool::new(false),
        |_| -> Result<(), ()> { panic!("synthetic sink panic") },
    );
    assert!(matches!(result, Err(XmltvError::SinkPanicked)));
    assert!(parse_xmltv_reader(
        &b"<tv><programme/></tv>"[..],
        &AtomicBool::new(false),
        |_| Ok::<_, ()>(())
    )
    .is_ok());
}
struct Broken;
impl Read for Broken {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("synthetic read failure"))
    }
}
#[test]
fn io_failure_and_early_cancellation_never_call_sink() {
    let result = parse_xmltv_reader(Broken, &AtomicBool::new(false), |_| -> Result<(), ()> {
        panic!("not called")
    });
    assert!(matches!(result, Err(XmltvError::Io(_))));
    let result = parse_xmltv_reader(Broken, &AtomicBool::new(true), |_| -> Result<(), ()> {
        panic!("not called")
    });
    assert!(matches!(result, Err(XmltvError::Cancelled)));
}
#[test]
fn actual_record_budget_refuses_oversized_body() {
    let data = format!(
        "<tv><programme><title>{}</title></programme></tv>",
        "x".repeat(5 * 1024 * 1024)
    );
    let mut count = 0;
    let result = parse_xmltv_reader(data.as_bytes(), &AtomicBool::new(false), |_| {
        count += 1;
        Ok::<_, ()>(())
    });
    assert!(matches!(result, Err(XmltvError::Capacity)));
    assert_eq!(count, 0);
}
#[test]
fn repeated_entity_markup_is_bounded_before_any_sink_commit() {
    let data = format!(
        "<!DOCTYPE tv [<!ENTITY block '{}'>]><tv><programme>{}</programme></tv>",
        "<b/>".repeat(500),
        "&block;".repeat(1000)
    );
    let mut count = 0;
    let result = parse_xmltv_reader(data.as_bytes(), &AtomicBool::new(false), |_| {
        count += 1;
        Ok::<_, ()>(())
    });
    assert!(matches!(result, Err(XmltvError::Capacity)));
    assert_eq!(count, 0);
}
#[test]
fn independent_contexts_do_not_share_failure_budget() {
    let threads: Vec<_> = (0..8)
        .map(|_| {
            std::thread::spawn(|| {
                for _ in 0..100 {
                    let result = parse_xmltv_reader(
                        &b"<tv><programme><title>news</title></programme></tv>"[..],
                        &AtomicBool::new(false),
                        |node| {
                            assert_eq!(node.children[0].text.as_deref(), Some("news"));
                            Ok::<_, ()>(())
                        },
                    )
                    .unwrap();
                    assert_eq!(result.programmes, 1);
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}

struct Overreported;
impl Read for Overreported {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        Ok(output.len() + 1)
    }
}
#[test]
fn incorrect_safe_reader_cannot_expose_foreign_out_of_bounds_memory() {
    let result = parse_xmltv_reader(Overreported, &AtomicBool::new(false), |_| Ok::<_, ()>(()));
    assert!(
        matches!(result,Err(XmltvError::Io(error)) if error.kind()==io::ErrorKind::InvalidData)
    );
}
struct Interrupted<R> {
    source: R,
    first: bool,
}
impl<R: Read> Read for Interrupted<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.first {
            self.first = false;
            Err(io::Error::from(io::ErrorKind::Interrupted))
        } else {
            self.source.read(output)
        }
    }
}
#[test]
fn interrupted_read_retries_without_discarding_parser_state() {
    let result = parse_xmltv_reader(
        Interrupted {
            source: &b"<tv><programme/></tv>"[..],
            first: true,
        },
        &AtomicBool::new(false),
        |_| Ok::<_, ()>(()),
    )
    .unwrap();
    assert_eq!(result.programmes, 1);
}
