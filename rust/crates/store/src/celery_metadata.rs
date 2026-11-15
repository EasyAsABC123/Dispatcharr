//! Bounded primitive metadata matching pinned Celery/Kombu semantics.
//! Original Dispatcharr authors retain application credit. Celery/Kombu authors
//! retain their library credit; see datasets/celery-metadata-authority.json.
use crate::StoreError;
use md5::{Digest, Md5};
use serde_json::Value;
use std::sync::OnceLock;
use uuid::Uuid;

const MAX_REPR: isize = 1024;
const MAX_OUTPUT: usize = 16384;
const OID_NAMESPACE: [u8; 16] = [
    0x6b, 0xa7, 0xb8, 0x12, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
];
fn unavailable() -> StoreError {
    StoreError::Unsupported("Producer metadata outside supported capacity")
}

pub(crate) struct Identity {
    pub reply_to: String,
    pub origin: String,
}
fn oid(node: u128, process: u32, thread: usize, instance: usize) -> Uuid {
    let name = format!("{node:x}-{process:x}-{thread:x}-{instance:x}");
    let mut digest = Md5::new();
    digest.update(OID_NAMESPACE);
    digest.update(name.as_bytes());
    let mut bytes: [u8; 16] = digest.finalize().into();
    bytes[6] = (bytes[6] & 0x0f) | 0x30;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}
#[cfg(unix)]
pub(crate) fn identity(instance: usize) -> Result<Identity, StoreError> {
    static NODE: OnceLock<u128> = OnceLock::new();
    let node = *NODE.get_or_init(|| Uuid::new_v4().as_u128());
    let process = std::process::id();
    // CPython's Unix get_ident uses the current pthread identity. It is opaque
    // epoch metadata, never a fixture counter or a wire-visible pointer.
    let thread = unsafe { libc::pthread_self() } as usize;
    let mut hostname = [0u8; 256];
    // This fixed, writable buffer bounds the libc write; the following NUL and
    // UTF-8 checks reject unsupported hostnames instead of fabricating identity.
    if unsafe { libc::gethostname(hostname.as_mut_ptr().cast(), hostname.len()) } != 0 {
        return Err(unavailable());
    }
    let length = hostname
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(unavailable)?;
    let hostname = std::str::from_utf8(&hostname[..length]).map_err(|_| unavailable())?;
    if hostname.is_empty() || hostname.len() > 128 || hostname.chars().any(char::is_control) {
        return Err(unavailable());
    }
    let origin = format!("gen{process}@{hostname}");
    if origin.len() > 160 {
        return Err(unavailable());
    }
    // Deterministic for one actual app instance/thread, matching thread_oid's
    // cached value without an unbounded per-thread/per-app identity map.
    Ok(Identity {
        reply_to: oid(node, process, thread, instance).to_string(),
        origin,
    })
}
#[cfg(not(unix))]
pub(crate) fn identity(_instance: usize) -> Result<Identity, StoreError> {
    Err(unavailable())
}

// These iterators retain O(depth) descriptors rather than quoting/cloning the
// full argument tree. JSON has no cycles; Celery's maximum nesting is three.
enum Frame<'a> {
    Value(&'a Value, usize),
    Tuple(&'a [Value], usize),
    Sequence(std::slice::Iter<'a, Value>, usize),
    Mapping(serde_json::map::Iter<'a>, usize),
    Key(&'a str),
    Quoted(&'a str),
    Text(String),
    Literal(&'static str, bool),
}
fn truncate(value: &str, maximum: isize) -> String {
    if maximum > 0 && value.chars().count() >= maximum as usize {
        let prefix: String = value.chars().take(maximum as usize).collect();
        let text = prefix
            .rsplit_once(' ')
            .map_or(prefix.as_str(), |(head, _)| head);
        format!("{text}...")
    } else {
        value.to_owned()
    }
}
fn scalar(value: &Value) -> Result<String, StoreError> {
    Ok(match value {
        Value::Null => "None".into(),
        Value::Bool(true) => "True".into(),
        Value::Bool(false) => "False".into(),
        Value::Number(number) => {
            let raw = number.to_string();
            if raw.contains(['.', 'e', 'E']) {
                crate::json_compat::float_string(
                    number
                        .as_f64()
                        .filter(|v| v.is_finite())
                        .ok_or_else(unavailable)?,
                )
            } else {
                raw
            }
        }
        _ => return Err(unavailable()),
    })
}
fn render<'a>(initial: Frame<'a>, mut remaining: isize) -> Result<String, StoreError> {
    let mut stack = vec![initial];
    let mut output = String::new();
    while let Some(frame) = stack.pop() {
        let token = match frame {
            Frame::Value(Value::Array(values), depth) => {
                if depth >= 3 {
                    Frame::Text("[...]".into())
                } else {
                    stack.push(Frame::Literal("]", true));
                    stack.push(Frame::Sequence(values.iter(), depth + 1));
                    Frame::Literal("[", true)
                }
            }
            Frame::Value(Value::Object(values), depth) => {
                if depth >= 3 {
                    Frame::Text("{...}".into())
                } else {
                    stack.push(Frame::Literal("}", true));
                    stack.push(Frame::Mapping(values.iter(), depth + 1));
                    Frame::Literal("{", true)
                }
            }
            Frame::Tuple(values, depth) => {
                let end = if values.len() == 1 { ",)" } else { ")" };
                if depth >= 3 {
                    Frame::Text(format!("(...{end}"))
                } else {
                    stack.push(Frame::Literal(end, true));
                    stack.push(Frame::Sequence(values.iter(), depth + 1));
                    Frame::Literal("(", true)
                }
            }
            Frame::Sequence(mut values, depth) => {
                if let Some(value) = values.next() {
                    if values.len() > 0 {
                        stack.push(Frame::Sequence(values, depth));
                        stack.push(Frame::Literal(", ", false));
                    }
                    stack.push(Frame::Value(value, depth));
                }
                continue;
            }
            Frame::Mapping(mut values, depth) => {
                if let Some((key, value)) = values.next() {
                    if values.len() > 0 {
                        stack.push(Frame::Mapping(values, depth));
                        stack.push(Frame::Literal(", ", false));
                    }
                    stack.push(Frame::Value(value, depth));
                    stack.push(Frame::Literal(": ", false));
                    stack.push(Frame::Key(key));
                }
                continue;
            }
            Frame::Value(Value::String(value), _) => Frame::Quoted(value),
            Frame::Value(value, _) => Frame::Text(scalar(value)?),
            other => other,
        };
        if remaining <= 0 {
            output.push_str(", ...");
            // As in Celery, the current yielded token is consumed. Only pending
            // literal delimiters are kept; unvisited values are never expanded.
            while let Some(rest) = stack.pop() {
                if let Frame::Literal(value, true) = rest {
                    output.push_str(value);
                }
            }
            break;
        }
        let value = match token {
            Frame::Literal(value, _) => value.to_owned(),
            Frame::Quoted(value) => {
                format!("'{}'", truncate(value, remaining).replace('\'', "\\'"))
            }
            Frame::Text(value) => truncate(&value, remaining),
            Frame::Key(value) => render(Frame::Quoted(value), remaining)?,
            _ => return Err(unavailable()),
        };
        remaining -= value.chars().count() as isize;
        output.push_str(&value);
        if output.len() > MAX_OUTPUT {
            return Err(unavailable());
        }
    }
    if output.len() > MAX_OUTPUT {
        return Err(unavailable());
    }
    Ok(output)
}
pub(crate) fn arguments(args: &Value) -> Result<String, StoreError> {
    render(
        Frame::Tuple(args.as_array().ok_or_else(unavailable)?, 0),
        MAX_REPR,
    )
}
pub(crate) fn periodic_arguments(args: &Value) -> Result<String, StoreError> {
    if args.as_array().ok_or_else(unavailable)?.is_empty() {
        arguments(args)
    } else {
        render(Frame::Value(args, 0), MAX_REPR)
    }
}
pub(crate) fn keywords(kwargs: &Value) -> Result<String, StoreError> {
    kwargs.as_object().ok_or_else(unavailable)?;
    render(Frame::Value(kwargs, 0), MAX_REPR)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pinned_celery_primitive_repr_and_oid_vectors() {
        let vectors: Value =
            serde_json::from_str(include_str!("../datasets/celery-metadata-vectors.json")).unwrap();
        for case in vectors["repr"].as_array().unwrap() {
            let value = &case["input"];
            let frame = if case["kind"] == "arguments" {
                Frame::Tuple(value.as_array().unwrap(), 0)
            } else {
                Frame::Value(value, 0)
            };
            assert_eq!(
                render(frame, case["limit"].as_i64().unwrap() as isize).unwrap(),
                case["expected"].as_str().unwrap(),
                "case{}",
                case["id"]
            );
        }
        for case in vectors["identity"].as_array().unwrap() {
            let node = u128::from_str_radix(case["node_hex"].as_str().unwrap(), 16).unwrap();
            let process = case["process"].as_u64().unwrap() as u32;
            let thread = usize::from_str_radix(case["thread_hex"].as_str().unwrap(), 16).unwrap();
            let instance =
                usize::from_str_radix(case["instance_hex"].as_str().unwrap(), 16).unwrap();
            assert_eq!(
                oid(node, process, thread, instance).to_string(),
                case["expected"].as_str().unwrap()
            );
        }
    }
    #[cfg(unix)]
    #[test]
    fn actual_runtime_identity_is_stable_for_the_same_app_thread() {
        let app = Box::new(0u8);
        let pointer = (&*app as *const u8) as usize;
        let first = identity(pointer).unwrap();
        let second = identity(pointer).unwrap();
        assert_eq!(first.reply_to, second.reply_to);
        assert_eq!(first.origin, second.origin);
        assert!(first
            .origin
            .starts_with(&format!("gen{}@", std::process::id())));
        assert_eq!(
            Uuid::parse_str(&first.reply_to).unwrap().get_version_num(),
            3
        );
    }
}
