//! Bounded native XMLTV streaming; original application credit: Dispatcharr authors.
use dispatcharr_ingest_core::{XmltvElement, XmltvReader};
use std::{
    collections::BTreeMap,
    ffi::{c_char, c_int, c_void, CStr},
    io::{self, Read},
    path::Path,
    ptr::NonNull,
    sync::atomic::{AtomicBool, Ordering},
};

extern "C" {
    fn dxml_new(
        emit: extern "C" fn(*mut c_void, *mut c_void) -> c_int,
        opaque: *mut c_void,
    ) -> *mut c_void;
    fn dxml_feed(
        parser: *mut c_void,
        bytes: *const c_char,
        len: c_int,
        final_chunk: c_int,
    ) -> c_int;
    fn dxml_drop(parser: *mut c_void);
    fn dxml_failed(parser: *mut c_void) -> c_int;
    fn dxml_name(node: *mut c_void) -> *const c_char;
    fn dxml_uri(node: *mut c_void) -> *const c_char;
    fn dxml_child(node: *mut c_void) -> *mut c_void;
    fn dxml_next(node: *mut c_void) -> *mut c_void;
    fn dxml_attr(node: *mut c_void) -> *mut c_void;
    fn dxml_attr_next(attr: *mut c_void) -> *mut c_void;
    fn dxml_attr_name(attr: *mut c_void) -> *const c_char;
    fn dxml_attr_uri(attr: *mut c_void) -> *const c_char;
    fn dxml_attr_value(node: *mut c_void, attr: *mut c_void) -> *mut c_char;
    fn dxml_text(node: *mut c_void) -> *mut c_char;
    fn dxml_free(value: *mut c_void);
}

#[derive(Debug)]
pub enum XmltvError<E> {
    Io(io::Error),
    Capacity,
    Cancelled,
    Sink(E),
    SinkPanicked,
    Allocation,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct XmltvStats {
    /// Includes the injected doctype, when the original file lacks one.
    pub prepared_input_bytes: u64,
    pub channels: u64,
    pub programmes: u64,
}
struct Native(NonNull<c_void>);
impl Drop for Native {
    fn drop(&mut self) {
        // This context is created once, never shared or moved to another thread,
        // and freed before the callback state is dropped.
        unsafe { dxml_drop(self.0.as_ptr()) }
    }
}
struct ForeignString(*mut c_char);
impl Drop for ForeignString {
    fn drop(&mut self) {
        // Null is accepted; non-null values come from the bridge's allocator.
        unsafe { dxml_free(self.0.cast()) }
    }
}
struct Extraction {
    bytes: usize,
    nodes: usize,
}
impl Extraction {
    fn charge(&mut self, bytes: usize) -> Result<(), ()> {
        self.bytes = self.bytes.checked_add(bytes).ok_or(())?;
        if self.bytes > 4 * 1024 * 1024 {
            return Err(());
        }
        Ok(())
    }
    // Every pointer below is borrowed from the synchronous native callback.
    // Its document remains alive until the callback returns; no borrowed
    // pointer escapes this extraction. Strings are copied after admission.
    unsafe fn string(&mut self, pointer: *const c_char) -> Result<Option<String>, ()> {
        if pointer.is_null() {
            return Ok(None);
        }
        let value = CStr::from_ptr(pointer).to_str().map_err(|_| ())?;
        self.charge(value.len())?;
        Ok(Some(value.to_owned()))
    }
    unsafe fn qualified(&mut self, name: *const c_char, uri: *const c_char) -> Result<String, ()> {
        let name = self.string(name)?.ok_or(())?;
        if let Some(uri) = self.string(uri)? {
            self.charge(2)?;
            Ok(format!("{{{uri}}}{name}"))
        } else {
            Ok(name)
        }
    }
    unsafe fn tree(&mut self, node: *mut c_void, depth: usize) -> Result<XmltvElement, ()> {
        self.nodes += 1;
        if depth > 256 || self.nodes > 4096 {
            return Err(());
        }
        let tag = self.qualified(dxml_name(node), dxml_uri(node))?;
        let foreign = ForeignString(dxml_text(node));
        let text = self.string(foreign.0)?;
        let mut attrs = BTreeMap::new();
        let mut attr = dxml_attr(node);
        while !attr.is_null() {
            let key = self.qualified(dxml_attr_name(attr), dxml_attr_uri(attr))?;
            let value = ForeignString(dxml_attr_value(node, attr));
            // Original lxml's duplicate-attribute recovery exposes the first.
            let value = self.string(value.0)?.unwrap_or_default();
            attrs.entry(key).or_insert(value);
            attr = dxml_attr_next(attr);
        }
        let mut children = Vec::new();
        let mut child = dxml_child(node);
        while !child.is_null() {
            children.push(self.tree(child, depth + 1)?);
            child = dxml_next(child);
        }
        Ok(XmltvElement {
            tag,
            text,
            attrs,
            children,
        })
    }
}
struct State<'a, F, E> {
    cancelled: &'a AtomicBool,
    sink: F,
    error: Option<XmltvError<E>>,
    native: *mut c_void,
    stats: XmltvStats,
}
extern "C" fn emit<F, E>(opaque: *mut c_void, node: *mut c_void) -> c_int
where
    F: FnMut(XmltvElement) -> Result<(), E>,
{
    // The boxed State address is stable until after the native context drops.
    let state = unsafe { &mut *opaque.cast::<State<'_, F, E>>() };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if state.cancelled.load(Ordering::Acquire) {
            return Err(XmltvError::Cancelled);
        }
        // The bridge calls only for a non-null, complete element. Extraction
        // takes owned copies while the native document still owns the node.
        let value = unsafe { Extraction { bytes: 0, nodes: 0 }.tree(node, 0) }
            .map_err(|_| XmltvError::Capacity)?;
        // A failed native allocation must not look like a missing XML field
        // and reach a database sink as a successfully decoded record.
        if unsafe { dxml_failed(state.native) } != 0 {
            return Err(XmltvError::Capacity);
        }
        match value.tag.as_str() {
            "channel" => state.stats.channels += 1,
            "programme" => state.stats.programmes += 1,
            _ => {}
        }
        (state.sink)(value).map_err(XmltvError::Sink)
    }));
    match result {
        Ok(Ok(())) => 0,
        Ok(Err(error)) => {
            state.error = Some(error);
            1
        }
        Err(_) => {
            state.error = Some(XmltvError::SinkPanicked);
            1
        }
    }
}

/// Parse with the original doctype injection and recover-mode element cleanup.
/// The synchronous sink owns one bounded record. It must separately bound any
/// retained records/queues. Errors/cancellation forbid successful job completion.
/// This function does not assert the original XML suffix completeness heuristic.
pub fn parse_xmltv_file<F, E>(
    path: &Path,
    cancelled: &AtomicBool,
    sink: F,
) -> Result<XmltvStats, XmltvError<E>>
where
    F: FnMut(XmltvElement) -> Result<(), E>,
{
    if cancelled.load(Ordering::Acquire) {
        return Err(XmltvError::Cancelled);
    }
    let reader = XmltvReader::open(path).map_err(XmltvError::Io)?;
    parse_xmltv_reader(reader, cancelled, sink)
}

/// The reader must already contain any required doctype prefix. Working input
/// is32KiB and total prepared input is capped at4GiB. Polling the cancellation
/// flag never frees work still executing in the native parser or sink.
pub fn parse_xmltv_reader<R, F, E>(
    mut reader: R,
    cancelled: &AtomicBool,
    sink: F,
) -> Result<XmltvStats, XmltvError<E>>
where
    R: Read,
    F: FnMut(XmltvElement) -> Result<(), E>,
{
    if cancelled.load(Ordering::Acquire) {
        return Err(XmltvError::Cancelled);
    }
    let mut state = Box::new(State {
        cancelled,
        sink,
        error: None,
        native: std::ptr::null_mut(),
        stats: XmltvStats::default(),
    });
    // Native creation does not parse bytes or invoke the element callback.
    let pointer = unsafe { dxml_new(emit::<F, E>, (&mut *state as *mut State<'_, F, E>).cast()) };
    let native = Native(NonNull::new(pointer).ok_or(XmltvError::Allocation)?);
    state.native = pointer;
    let mut buffer = [0_u8; 32768];
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Err(XmltvError::Cancelled);
        }
        let len = match reader.read(&mut buffer) {
            Ok(len) => len,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(XmltvError::Io(error)),
        };
        // Read is a safe trait. Never let an incorrect implementation expose
        // memory beyond the input buffer to the foreign parser.
        if len > buffer.len() {
            return Err(XmltvError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "reader exceeded input buffer",
            )));
        }
        state.stats.prepared_input_bytes += len as u64;
        if state.stats.prepared_input_bytes > 4 * 1024 * 1024 * 1024 {
            return Err(XmltvError::Capacity);
        }
        if cancelled.load(Ordering::Acquire) {
            return Err(XmltvError::Cancelled);
        }
        // Input is borrowed only for this call. The boxed State and native
        // document remain alive for all synchronous callbacks within it.
        let status = unsafe {
            dxml_feed(
                native.0.as_ptr(),
                buffer.as_ptr().cast(),
                len as c_int,
                c_int::from(len == 0),
            )
        };
        if let Some(error) = state.error.take() {
            return Err(error);
        }
        if status != 0 {
            return Err(XmltvError::Capacity);
        }
        if len == 0 {
            break;
        }
    }
    if cancelled.load(Ordering::Acquire) {
        return Err(XmltvError::Cancelled);
    }
    Ok(state.stats)
}

mod strict;
pub use strict::{
    parse_xmltv_element, parse_xmltv_element_with_limits, StrictElementError, StrictElementLimits,
};
