//! Source VOD manager process identity and Django-compatible response value.
//! Original Dispatcharr authors retain application/algorithm credit and licenses.
use axum::http::HeaderValue;
use std::{ffi::OsString, sync::OnceLock};

const MAX_HOSTNAME_BYTES: usize = 255;
const MAX_HEADER_BYTES: usize = 512;
static PROCESS: OnceLock<Result<HeaderValue, InvalidIdentity>> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct InvalidIdentity;

/// The Source manager mints its worker identity once, not once per response.
/// A failed hostname syscall uses Source's four-digit fallback, not HOSTNAME.
fn cached() -> &'static Result<HeaderValue, InvalidIdentity> {
    cached_with(
        &PROCESS,
        || nix::unistd::gethostname().map_err(|_| ()),
        std::process::id,
        || 1000 + (uuid::Uuid::new_v4().as_u128() % 9000) as u16,
    )
}
fn cached_with(
    cache: &OnceLock<Result<HeaderValue, InvalidIdentity>>,
    hostname: impl FnOnce() -> Result<OsString, ()>,
    pid: impl FnOnce() -> u32,
    fallback: impl FnOnce() -> u16,
) -> &Result<HeaderValue, InvalidIdentity> {
    cache.get_or_init(|| build(hostname(), pid(), fallback))
}
pub(super) fn initialize() {
    let _ = cached();
}
pub(super) fn current() -> Result<HeaderValue, InvalidIdentity> {
    cached().clone()
}
fn build(
    hostname: Result<OsString, ()>,
    pid: u32,
    fallback: impl FnOnce() -> u16,
) -> Result<HeaderValue, InvalidIdentity> {
    let value = match hostname {
        Ok(hostname) => {
            // Linux gethostname is bounded by nix's 256-byte syscall buffer.
            // Invalid Unicode is a response conversion failure, not a reason
            // to silently remint a random identity after a successful syscall.
            let hostname = hostname.into_string().map_err(|_| InvalidIdentity)?;
            if hostname.len() > MAX_HOSTNAME_BYTES {
                return Err(InvalidIdentity);
            }
            format!("{hostname}-{pid}")
        }
        Err(()) => {
            let number = fallback();
            if !(1000..=9999).contains(&number) {
                return Err(InvalidIdentity);
            }
            format!("worker-{number}")
        }
    };
    response_value(&value)
}

/// Same Latin-1 / UTF-8 encoded-word choice as the existing Source image
/// response conversion. CR/LF and invalid HTTP control values stay errors.
fn response_value(value: &str) -> Result<HeaderValue, InvalidIdentity> {
    if value.contains(['\r', '\n']) {
        return Err(InvalidIdentity);
    }
    let latin = value
        .chars()
        .map(|c| u8::try_from(c as u32))
        .collect::<Result<Vec<_>, _>>();
    let bytes = match latin {
        Ok(bytes) => bytes,
        Err(_) => {
            let q = value
                .as_bytes()
                .iter()
                .map(|byte| match byte {
                    b' ' => "_".into(),
                    b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'!' | b'*' | b'+' | b'-' | b'/' => {
                        char::from(*byte).to_string()
                    }
                    v => format!("={v:02X}"),
                })
                .collect::<String>();
            let b = base64(value.as_bytes());
            if b.len() < q.len() {
                format!("=?utf-8?b?{b}?=").into_bytes()
            } else {
                format!("=?utf-8?q?{q}?=").into_bytes()
            }
        }
    };
    if bytes.len() > MAX_HEADER_BYTES {
        return Err(InvalidIdentity);
    }
    HeaderValue::from_bytes(&bytes).map_err(|_| InvalidIdentity)
}
// A bounded encoded-word primitive avoids adding another direct package edge.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        result.push(char::from(ALPHABET[(a >> 2) as usize]));
        result.push(char::from(ALPHABET[(((a & 3) << 4) | (b >> 4)) as usize]));
        result.push(if chunk.len() > 1 {
            char::from(ALPHABET[(((b & 15) << 2) | (c >> 6)) as usize])
        } else {
            '='
        });
        result.push(if chunk.len() > 2 {
            char::from(ALPHABET[(c & 63) as usize])
        } else {
            '='
        });
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        os::unix::ffi::OsStringExt,
        process::Command,
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn source_hostname_pid_and_fallback_bounds() {
        assert_eq!(
            build(Ok("source-host".into()), 42, || panic!(
                "normal path uses no fallback"
            ))
            .unwrap(),
            "source-host-42"
        );
        for value in [1000, 9999] {
            assert_eq!(
                build(Err(()), 42, || value).unwrap(),
                format!("worker-{value}")
            );
        }
        assert!(build(Err(()), 42, || 999).is_err());
        assert!(build(Err(()), 42, || 10000).is_err());
    }
    #[test]
    fn manager_identity_and_fallback_are_cached_without_rereading() {
        for failure in [false, true] {
            let cache = OnceLock::new();
            let calls = AtomicUsize::new(0);
            let first = cached_with(
                &cache,
                || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    if failure {
                        Err(())
                    } else {
                        Ok("first".into())
                    }
                },
                || 7,
                || 2345,
            )
            .clone();
            let later = cached_with(
                &cache,
                || panic!("cached hostname"),
                || panic!("cached PID"),
                || panic!("cached fallback"),
            )
            .clone();
            assert_eq!(first, later);
            assert_eq!(calls.load(Ordering::Relaxed), 1);
            assert_eq!(
                first.unwrap(),
                if failure { "worker-2345" } else { "first-7" }
            );
        }
    }
    #[test]
    fn header_failure_never_silently_remints_identity() {
        for name in ["bad\rname", "bad\nname", "bad\0name"] {
            assert!(build(Ok(name.into()), 7, || panic!(
                "conversion is not syscall failure"
            ))
            .is_err());
        }
        assert!(build(Ok(OsString::from_vec(vec![0xff])), 7, || panic!(
            "invalid Unicode is not syscall failure"
        ))
        .is_err());
        assert!(build(Ok("x".repeat(256).into()), 7, || panic!(
            "capacity is not syscall failure"
        ))
        .is_err());
    }
    #[test]
    fn base64_is_exact_for_every_tail_and_byte() {
        for (input, expected) in [
            (b"".as_slice(), ""),
            (b"f", "Zg=="),
            (b"fo", "Zm8="),
            (b"foo", "Zm9v"),
            (b"foob", "Zm9vYg=="),
            (b"fooba", "Zm9vYmE="),
            (b"foobar", "Zm9vYmFy"),
            (&[0, 127, 128, 255], "AH+A/w=="),
        ] {
            assert_eq!(base64(input), expected);
        }
    }
    #[test]
    fn process_cache_is_shared_by_threads_and_header_clones() {
        let expected = current().unwrap();
        for join in (0..8)
            .map(|_| std::thread::spawn(current))
            .collect::<Vec<_>>()
        {
            assert_eq!(join.join().unwrap().unwrap(), expected.clone());
        }
    }
    #[test]
    fn real_hostname_pid_ignore_override_and_are_per_process() {
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "worker_identity::tests::process_probe",
                "--ignored",
                "--nocapture",
            ])
            .env("HOSTNAME", "untrusted-header-spoof")
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
        let text = String::from_utf8(child.stdout).unwrap();
        let row: serde_json::Value = serde_json::from_str(
            text.lines()
                .find_map(|line| line.strip_prefix("WORKER_PROBE:"))
                .expect("actual child identity"),
        )
        .unwrap();
        assert_ne!(row["pid"].as_u64().unwrap(), u64::from(std::process::id()));
        assert_eq!(
            row["hostname"].as_str().unwrap(),
            nix::unistd::gethostname().unwrap().to_str().unwrap()
        );
        assert_ne!(row["hostname"], "untrusted-header-spoof");
        assert_eq!(
            row["header"],
            format!(
                "{}-{}",
                row["hostname"].as_str().unwrap(),
                row["pid"].as_u64().unwrap()
            )
        );
    }
    #[test]
    #[ignore = "only launched by the exact process-identity control"]
    fn process_probe() {
        initialize();
        let value = current().unwrap();
        assert_eq!(value, current().unwrap());
        println!(
            "WORKER_PROBE:{}",
            serde_json::json!({"hostname":nix::unistd::gethostname().unwrap().to_str().unwrap(),"pid":std::process::id(),"header":value.to_str().unwrap()})
        );
    }
    #[test]
    fn pinned_django_header_primitives_match() {
        let vectors: &[(&str, Option<&[u8]>)] = &[
            ("host-42", Some(&[104, 111, 115, 116, 45, 52, 50])),
            ("é-42", Some(&[233, 45, 52, 50])),
            (
                "☃-42",
                Some(&[
                    61, 63, 117, 116, 102, 45, 56, 63, 98, 63, 52, 112, 105, 68, 76, 84, 81, 121,
                    63, 61,
                ]),
            ),
            (
                "long-ascii-hostname-with-snowman-☃-42",
                Some(&[
                    61, 63, 117, 116, 102, 45, 56, 63, 113, 63, 108, 111, 110, 103, 45, 97, 115,
                    99, 105, 105, 45, 104, 111, 115, 116, 110, 97, 109, 101, 45, 119, 105, 116,
                    104, 45, 115, 110, 111, 119, 109, 97, 110, 45, 61, 69, 50, 61, 57, 56, 61, 56,
                    51, 45, 52, 50, 63, 61,
                ]),
            ),
            (
                "😀-42",
                Some(&[
                    61, 63, 117, 116, 102, 45, 56, 63, 98, 63, 56, 74, 43, 89, 103, 67, 48, 48, 77,
                    103, 61, 61, 63, 61,
                ]),
            ),
            (
                "space host-42",
                Some(&[115, 112, 97, 99, 101, 32, 104, 111, 115, 116, 45, 52, 50]),
            ),
            ("bad\rhost-42", None),
            ("bad\nhost-42", None),
        ];
        for (value, expected) in vectors {
            match expected {
                Some(bytes) => assert_eq!(response_value(value).unwrap().as_bytes(), *bytes),
                None => assert!(response_value(value).is_err()),
            }
        }
    }
}
