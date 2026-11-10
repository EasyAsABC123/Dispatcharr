//! Post24 strict helper; Source/resource gates remain required before acceptance.
//! Original Dispatcharr authors retain full application/algorithm credit.
use super::{dxml_failed, Extraction, Native};
use dispatcharr_ingest_core::{XmltvElement, HTML_ENTITY_DOCTYPE};
use std::{
    ffi::{c_char, c_int, c_void},
    io::{Cursor, Read},
    ptr::NonNull,
    sync::atomic::{AtomicBool, Ordering},
};
extern "C" {
    fn dxml_strict_new(
        emit: extern "C" fn(*mut c_void, *mut c_void) -> c_int,
        opaque: *mut c_void,
        heap_limit: usize,
    ) -> *mut c_void;
    fn dxml_strict_feed(
        parser: *mut c_void,
        bytes: *const c_char,
        len: c_int,
        final_chunk: c_int,
    ) -> c_int;
}
#[derive(Clone, Copy, Debug)]
pub struct StrictElementLimits {
    pub max_input_bytes: usize,
    pub native_heap_bytes: usize,
}
impl Default for StrictElementLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 4 * 1024 * 1024,
            native_heap_bytes: 16 * 1024 * 1024,
        }
    }
}
#[derive(Debug, PartialEq, Eq)]
pub enum StrictElementError {
    Syntax,
    Capacity,
    Cancelled,
    Allocation,
    InvalidConfiguration,
    ExtractionPanicked,
}
struct State<'a> {
    cancelled: &'a AtomicBool,
    native: *mut c_void,
    result: Option<XmltvElement>,
    error: Option<StrictElementError>,
}
extern "C" fn emit(opaque: *mut c_void, node: *mut c_void) -> c_int {
    // The Box and native document remain alive through this synchronous callback.
    let state = unsafe { &mut *opaque.cast::<State<'_>>() };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if state.cancelled.load(Ordering::Acquire) {
            return Err(StrictElementError::Cancelled);
        }
        // Root is borrowed only during active native-arena ownership; copy fields.
        let value = unsafe { Extraction { bytes: 0, nodes: 0 }.tree(node, 0) }
            .map_err(|_| StrictElementError::Capacity)?;
        if unsafe { dxml_failed(state.native) } != 0 {
            return Err(StrictElementError::Capacity);
        }
        if state.cancelled.load(Ordering::Acquire) {
            return Err(StrictElementError::Cancelled);
        }
        state.result = Some(value);
        Ok(())
    }));
    match result {
        Ok(Ok(())) => 0,
        Ok(Err(error)) => {
            state.error = Some(error);
            1
        }
        Err(_) => {
            state.error = Some(StrictElementError::ExtractionPanicked);
            1
        }
    }
}
/// Source's strict fromstring helper with its original HTML4 prefix. One root is
/// returned after complete validation; nested programmes are ordinary children.
/// Caller must retain worker/memory guards until this synchronous call returns.
pub fn parse_xmltv_element(
    bytes: &[u8],
    cancelled: &AtomicBool,
) -> Result<XmltvElement, StrictElementError> {
    parse_xmltv_element_with_limits(bytes, cancelled, StrictElementLimits::default())
}
pub fn parse_xmltv_element_with_limits(
    bytes: &[u8],
    cancelled: &AtomicBool,
    limits: StrictElementLimits,
) -> Result<XmltvElement, StrictElementError> {
    if cancelled.load(Ordering::Acquire) {
        return Err(StrictElementError::Cancelled);
    }
    if !(1..=4 * 1024 * 1024).contains(&limits.max_input_bytes)
        || !(65536..=16 * 1024 * 1024).contains(&limits.native_heap_bytes)
    {
        return Err(StrictElementError::InvalidConfiguration);
    }
    if bytes.len() > limits.max_input_bytes {
        return Err(StrictElementError::Capacity);
    }
    let input = Cursor::new(HTML_ENTITY_DOCTYPE).chain(Cursor::new(bytes));
    parse_prepared(input, cancelled, limits)
}
fn parse_prepared<R: Read>(
    mut input: R,
    cancelled: &AtomicBool,
    limits: StrictElementLimits,
) -> Result<XmltvElement, StrictElementError> {
    if cancelled.load(Ordering::Acquire) {
        return Err(StrictElementError::Cancelled);
    }
    let mut state = Box::new(State {
        cancelled,
        native: std::ptr::null_mut(),
        result: None,
        error: None,
    });
    // Fresh strict parser shares the existing common-layout destructor and arena.
    let pointer = unsafe {
        dxml_strict_new(
            emit,
            (&mut *state as *mut State<'_>).cast(),
            limits.native_heap_bytes,
        )
    };
    let native = Native(NonNull::new(pointer).ok_or(StrictElementError::Allocation)?);
    state.native = pointer;
    let mut buffer = [0u8; 32768];
    let mut prepared_bytes = 0usize;
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Err(StrictElementError::Cancelled);
        }
        let count = input
            .read(&mut buffer)
            .map_err(|_| StrictElementError::Syntax)?;
        if count > buffer.len() {
            return Err(StrictElementError::Capacity);
        }
        prepared_bytes = prepared_bytes
            .checked_add(count)
            .ok_or(StrictElementError::Capacity)?;
        if prepared_bytes > HTML_ENTITY_DOCTYPE.len() + limits.max_input_bytes {
            return Err(StrictElementError::Capacity);
        }
        if cancelled.load(Ordering::Acquire) {
            return Err(StrictElementError::Cancelled);
        }
        // Reader input is bounded by this slice; no borrowed pointer survives
        // the feed call. The root callback only occurs on successful final EOF.
        let status = unsafe {
            dxml_strict_feed(
                native.0.as_ptr(),
                buffer.as_ptr().cast(),
                count as c_int,
                c_int::from(count == 0),
            )
        };
        if let Some(error) = state.error.take() {
            return Err(error);
        }
        match status {
            0 => {}
            2 => return Err(StrictElementError::Capacity),
            _ => return Err(StrictElementError::Syntax),
        }
        if count == 0 {
            break;
        }
    }
    if cancelled.load(Ordering::Acquire) {
        return Err(StrictElementError::Cancelled);
    }
    state.result.take().ok_or(StrictElementError::Syntax)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{mpsc, Arc};
    #[test]
    fn one_strict_root_whitespace_and_rejected_partial_documents() {
        let value=parse_xmltv_element(b"<programme> \n<programme><title>Inner</title></programme><title>  Outer  </title></programme>",&AtomicBool::new(false)).unwrap();
        assert_eq!(value.tag, "programme");
        assert_eq!(value.text.as_deref(), Some(" \n"));
        assert_eq!(value.children.len(), 2);
        assert_eq!(value.children[1].text.as_deref(), Some("  Outer  "));
        for input in [
            &b"<programme/><extra/>"[..],
            &b"<programme><title>unfinished"[..],
            &b"<programme channel='a' channel='b'/>"[..],
            &b"<programme>&unknown;</programme>"[..],
        ] {
            assert_eq!(
                parse_xmltv_element(input, &AtomicBool::new(false)).unwrap_err(),
                StrictElementError::Syntax
            );
        }
    }
    #[test]
    fn input_nodes_and_small_native_heap_fail_closed() {
        let valid = b"<programme><title>source</title></programme>";
        assert_eq!(
            parse_xmltv_element_with_limits(
                valid,
                &AtomicBool::new(false),
                StrictElementLimits {
                    max_input_bytes: 8,
                    ..StrictElementLimits::default()
                }
            )
            .unwrap_err(),
            StrictElementError::Capacity
        );
        let mut nodes = b"<programme>".to_vec();
        for _ in 0..10000 {
            nodes.extend_from_slice(b"<!--x-->");
        }
        nodes.extend_from_slice(b"</programme>");
        assert_eq!(
            parse_xmltv_element(&nodes, &AtomicBool::new(false)).unwrap_err(),
            StrictElementError::Capacity
        );
        let mut text = b"<programme><title>".to_vec();
        text.resize(1024 * 1024, b'x');
        text.extend_from_slice(b"</title></programme>");
        let error = parse_xmltv_element_with_limits(
            &text,
            &AtomicBool::new(false),
            StrictElementLimits {
                native_heap_bytes: 65536,
                ..StrictElementLimits::default()
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            StrictElementError::Capacity | StrictElementError::Allocation
        ));
        assert_eq!(
            parse_xmltv_element(valid, &AtomicBool::new(true)).unwrap_err(),
            StrictElementError::Cancelled
        );
        assert_eq!(
            parse_xmltv_element(valid, &AtomicBool::new(false))
                .unwrap()
                .children[0]
                .text
                .as_deref(),
            Some("source")
        );
    }
    struct Blocked {
        data: Cursor<Vec<u8>>,
        reads: usize,
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
        dropped: Arc<AtomicBool>,
    }
    impl Read for Blocked {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            if self.reads == 1 {
                self.entered.send(()).unwrap();
                self.release.recv().unwrap();
            }
            self.reads += 1;
            self.data.read(out)
        }
    }
    impl Drop for Blocked {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }
    #[test]
    fn cancelled_partial_native_context_stays_owned_until_reader_returns() {
        let mut input = HTML_ENTITY_DOCTYPE.to_vec();
        input.extend_from_slice(b"<programme><title>");
        input.resize(2 * 1024 * 1024, b'x');
        input.extend_from_slice(b"</title></programme>");
        let (entered, wait) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = Arc::new(AtomicBool::new(false));
        let cancelled = flag.clone();
        let guard = dropped.clone();
        let thread = std::thread::spawn(move || {
            parse_prepared(
                Blocked {
                    data: Cursor::new(input),
                    reads: 0,
                    entered,
                    release: blocked,
                    dropped: guard,
                },
                &cancelled,
                StrictElementLimits::default(),
            )
        });
        wait.recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        flag.store(true, Ordering::Release);
        assert!(!dropped.load(Ordering::Acquire));
        release.send(()).unwrap();
        assert_eq!(
            thread.join().unwrap().unwrap_err(),
            StrictElementError::Cancelled
        );
        assert!(dropped.load(Ordering::Acquire));
    }
    #[test]
    fn concurrent_strict_failures_do_not_poison_existing_recover_arenas() {
        let workers = (0..4)
            .map(|_| {
                std::thread::spawn(|| {
                    for _ in 0..32 {
                        assert_eq!(
                            parse_xmltv_element(
                                b"<programme><broken></programme>",
                                &AtomicBool::new(false)
                            )
                            .unwrap_err(),
                            StrictElementError::Syntax
                        );
                        assert_eq!(
                            parse_xmltv_element(
                                b"<programme><title>&eacute;</title></programme>",
                                &AtomicBool::new(false)
                            )
                            .unwrap()
                            .children[0]
                                .text
                                .as_deref(),
                            Some("é")
                        );
                        let stats = crate::parse_xmltv_reader(
                            Cursor::new(b"<tv><programme><title>legacy</title></programme></tv>"),
                            &AtomicBool::new(false),
                            |_| Ok::<_, ()>(()),
                        )
                        .unwrap();
                        assert_eq!(stats.programmes, 1);
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
    }
}

#[cfg(test)]
mod owned_resources {
    use super::*;
    #[test]
    fn only_owned_external_file_and_loopback_references_are_probed() {
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}/owned.dtd", listener.local_addr().unwrap());
        let body = format!("<!DOCTYPE programme SYSTEM '{url}'><programme>&owned;</programme>");
        assert_eq!(
            parse_xmltv_element(body.as_bytes(), &AtomicBool::new(false)).unwrap_err(),
            StrictElementError::Syntax
        );
        assert!(
            matches!(listener.accept(),Err(error)if error.kind()==std::io::ErrorKind::WouldBlock)
        );
        let directory = std::env::temp_dir().join(format!(
            "dispatcharr-strict-native-owned-{}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        struct Guard(std::path::PathBuf);
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _guard = Guard(directory.clone());
        let sentinel = directory.join("entity.txt");
        std::fs::write(&sentinel, b"owned-local-marker").unwrap();
        let body = format!(
            "<!DOCTYPE programme [<!ENTITY ext SYSTEM 'file://{}'>]><programme>&ext;</programme>",
            sentinel.display()
        );
        assert_eq!(
            parse_xmltv_element(body.as_bytes(), &AtomicBool::new(false)).unwrap_err(),
            StrictElementError::Syntax
        );
    }
}
