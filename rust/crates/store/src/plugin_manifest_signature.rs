//! Source-derived manifest signing contract. Original Dispatcharr authors retain
//! full application/algorithm/license credit. Pending full Compose acceptance.
use super::{Failure, RAW_LIMIT};
use serde_json::Value;
use std::{
    io,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
const OFFICIAL_KEY: &str = include_str!("../datasets/source-plugin-official-public-key.asc");
const PIPE_BYTES: usize = 65536;

struct Home(PathBuf);
impl Home {
    fn new() -> io::Result<Self> {
        use std::os::unix::fs::DirBuilderExt;
        let path = std::env::temp_dir().join(format!("dispatcharr-gpg-{}", uuid::Uuid::new_v4()));
        std::fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }
}
impl Drop for Home {
    fn drop(&mut self) {
        let removed = std::fs::remove_dir_all(&self.0);
        tracing::debug!(target: "dispatcharr::plugin_gpg", event = "home_drop",
            home_id = ?self.0.file_name(), remove_ok = removed.is_ok(),
            home_absent = !self.0.exists(), error_kind = ?removed.err().map(|error| error.kind()));
    }
}

/// Python json.dumps(..., separators=(',', ':')) preserves insertion order,
/// escapes non-ASCII including surrogate pairs, and the caller appends LF.
/// Floats are guarded until Python's number rendering is characterized.
fn canonical(value: &Value) -> Result<Vec<u8>, Failure> {
    fn check(value: &Value, nodes: &mut usize, depth: usize) -> Result<(), Failure> {
        *nodes += 1;
        if *nodes > 2048 || depth > 32 {
            return Err(Failure::Unqualified("Signature JSON capacity unavailable"));
        }
        match value {
            Value::Number(n) if n.to_string().contains(['.', 'e', 'E']) => {
                return Err(Failure::Unqualified(
                    "Python floating-point signing bytes remain unqualified",
                ))
            }
            Value::Array(values) => {
                for v in values {
                    check(v, nodes, depth + 1)?;
                }
            }
            Value::Object(values) => {
                for v in values.values() {
                    check(v, nodes, depth + 1)?;
                }
            }
            _ => (),
        }
        Ok(())
    }
    check(value, &mut 0, 0)?;
    let mut normalized = value.clone();
    fn integers(v: &mut Value) {
        match v {
            Value::Number(n) if n.to_string() == "-0" => *n = 0.into(),
            Value::Array(a) => {
                for v in a {
                    integers(v);
                }
            }
            Value::Object(o) => {
                for v in o.values_mut() {
                    integers(v);
                }
            }
            _ => (),
        }
    }
    integers(&mut normalized);
    let json = serde_json::to_string(&normalized).map_err(|_| Failure::Json)?;
    if json.len() > RAW_LIMIT {
        return Err(Failure::Unqualified("Signature JSON capacity unavailable"));
    }
    let mut ascii = Vec::new();
    for ch in json.chars() {
        if ch <= '\u{7e}' {
            ascii.push(ch as u8);
        } else {
            for unit in ch.encode_utf16(&mut [0; 2]) {
                const HEX: &[u8] = b"0123456789abcdef";
                ascii.extend_from_slice(b"\\u");
                for shift in [12, 8, 4, 0] {
                    ascii.push(HEX[(((*unit as u32) >> shift) & 15) as usize]);
                }
            }
        }
    }
    ascii.push(b'\n');
    Ok(ascii)
}
fn normalize(key: &str) -> String {
    if key.trim().is_empty() {
        return key.to_owned();
    }
    let mut key = key.trim().to_owned();
    if !key.contains("-----BEGIN PGP PUBLIC KEY BLOCK-----") {
        key = format!("-----BEGIN PGP PUBLIC KEY BLOCK-----\n\n{key}");
    }
    if !key.contains("-----END PGP PUBLIC KEY BLOCK-----") {
        key.push_str("\n-----END PGP PUBLIC KEY BLOCK-----");
    }
    key
}
async fn bounded<R: AsyncRead + Unpin>(mut pipe: R) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut chunk = [0; 8192];
    loop {
        let n = pipe.read(&mut chunk).await?;
        if n == 0 {
            return Ok(bytes);
        }
        if bytes.len().saturating_add(n) > PIPE_BYTES {
            return Err(io::Error::other("GPG output capacity"));
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
}
async fn run(
    home: &Path,
    args: &[&std::ffi::OsStr],
    input: &[u8],
) -> io::Result<(Option<i32>, Vec<u8>)> {
    // Public-key verification needs no agent. Inhibit implicit daemon startup so
    // every launched child is directly owned; API equivalence needs batch proof.
    let mut command = tokio::process::Command::new("gpg");
    command
        .args([
            "--batch",
            "--no-tty",
            "--no-autostart",
            "--status-fd",
            "2",
            "--homedir",
        ])
        .arg(home)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().inspect_err(|error| {
        tracing::debug!(target: "dispatcharr::plugin_gpg", event = "spawn_error", error_kind = ?error.kind());
    })?;
    let pid = child.id().unwrap_or(0);
    let trace_enabled = tracing::enabled!(target: "dispatcharr::plugin_gpg", tracing::Level::DEBUG);
    let generation = trace_enabled
        .then(|| {
            std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .ok()
                .and_then(|stat| stat.rsplit_once(')').map(|(_, tail)| tail.to_owned()))
                .and_then(|tail| tail.split_whitespace().nth(19).map(str::to_owned))
        })
        .flatten();
    let namespace = trace_enabled
        .then(|| std::fs::read_link("/proc/self/ns/pid").ok())
        .flatten();
    let role = if args
        .first()
        .is_some_and(|arg| *arg == std::ffi::OsStr::new("--import"))
    {
        "import"
    } else {
        "verify"
    };
    tracing::debug!(target: "dispatcharr::plugin_gpg", event = "spawn", pid,
        start_generation = ?generation, pid_namespace = ?namespace, role,
        home_id = ?home.file_name(), stdin_bytes = input.len());
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("missing GPG stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("missing GPG stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("missing GPG stderr"))?;
    let operation = async {
        let write = async {
            stdin.write_all(input).await?;
            stdin.shutdown().await?;
            drop(stdin);
            Ok::<_, io::Error>(())
        };
        let (sent, out, err, waited) =
            tokio::join!(write, bounded(stdout), bounded(stderr), child.wait());
        use std::os::unix::process::ExitStatusExt;
        tracing::debug!(target: "dispatcharr::plugin_gpg", event = "joined", pid,
            start_generation = ?generation, role, home_id = ?home.file_name(),
            stdin_complete = sent.is_ok(), stdout_eof = out.is_ok(), stderr_eof = err.is_ok(),
            stdout_bytes = ?out.as_ref().ok().map(Vec::len), stderr_bytes = ?err.as_ref().ok().map(Vec::len),
            wait_observed = waited.is_ok(), wait_exit_code = ?waited.as_ref().ok().and_then(|status| status.code()),
            wait_signal = ?waited.as_ref().ok().and_then(|status| status.signal()));
        sent?;
        out?;
        Ok::<_, io::Error>((waited?.code(), err?))
    };
    match tokio::time::timeout(Duration::from_secs(30), operation).await {
        Ok(result) => result,
        Err(_) => {
            let killed = child.kill().await;
            let waited = child.wait().await;
            use std::os::unix::process::ExitStatusExt;
            tracing::debug!(target: "dispatcharr::plugin_gpg", event = "timeout_settled", pid,
                start_generation = ?generation, role, home_id = ?home.file_name(),
                kill_ok = killed.is_ok(), wait_observed = waited.is_ok(),
                wait_exit_code = ?waited.as_ref().ok().and_then(|status| status.code()),
                wait_signal = ?waited.as_ref().ok().and_then(|status| status.signal()),
                pipe_eof_observed = false);
            Err(io::Error::new(io::ErrorKind::TimedOut, "GPG deadline"))
        }
    }
}

fn capacity_error(error: &io::Error) -> Option<Failure> {
    if error.kind() == io::ErrorKind::TimedOut {
        Some(Failure::Unqualified(
            "Signature process lifetime capacity unavailable",
        ))
    } else if error.kind() == io::ErrorKind::Other && error.to_string() == "GPG output capacity" {
        Some(Failure::Unqualified(
            "Signature output capacity unavailable",
        ))
    } else {
        None
    }
}

/// Source distinguishes missing/unimportable key (null), an unsuccessful verify
/// (false), and an actual successful verify with VALIDSIG (true).
pub(super) async fn verify(
    manifest: &Value,
    signature: Option<&Value>,
    public_key: Option<&str>,
) -> Result<Option<bool>, Failure> {
    let Some(signature) = signature.filter(|v| crate::channels::truthy(Some(v))) else {
        return Ok(None);
    };
    let Some(signature) = signature.as_str() else {
        return Err(Failure::Unqualified(
            "Non-string signatures remain unqualified",
        ));
    };
    let key = public_key
        .filter(|v| !v.is_empty())
        .map(normalize)
        .unwrap_or_else(|| OFFICIAL_KEY.into());
    if key.is_empty() {
        return Ok(None);
    }
    if key.len() > RAW_LIMIT || signature.len() > RAW_LIMIT {
        return Err(Failure::Unqualified("Signature/key capacity unavailable"));
    }
    let home = match Home::new() {
        Ok(v) => v,
        Err(_) => return Ok(Some(false)),
    };
    let imported = match run(&home.0, &[std::ffi::OsStr::new("--import")], key.as_bytes()).await {
        Ok(v) => v,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return match capacity_error(&error) {
                Some(refusal) => Err(refusal),
                None => Ok(Some(false)),
            }
        }
    };
    if imported.0 != Some(0) {
        return Ok(None);
    }
    let bytes = canonical(manifest)?;
    let sig = home.0.join("manifest.sig");
    if std::fs::write(&sig, signature.as_bytes()).is_err() {
        return Ok(Some(false));
    }
    match run(
        &home.0,
        &[
            std::ffi::OsStr::new("--verify"),
            sig.as_os_str(),
            std::ffi::OsStr::new("-"),
        ],
        &bytes,
    )
    .await
    {
        Ok((status, stderr)) => Ok(Some(
            status == Some(0) && stderr.windows(8).any(|v| v == b"VALIDSIG"),
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => match capacity_error(&error) {
            Some(refusal) => Err(refusal),
            None => Ok(Some(false)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn capacity_refusals_are_not_invalid_signatures() {
        assert!(matches!(
            capacity_error(&io::Error::other("GPG output capacity")),
            Some(Failure::Unqualified(_))
        ));
        assert!(matches!(
            capacity_error(&io::Error::new(io::ErrorKind::TimedOut, "GPG deadline")),
            Some(Failure::Unqualified(_))
        ));
        assert!(capacity_error(&io::Error::new(io::ErrorKind::NotFound, "gpg")).is_none());
    }
    #[test]
    fn source_unicode_and_order_bytes() {
        let value: Value =
            serde_json::from_str(r#"{"registry_name":"Owned café 💡 fixture","plugins":[]}"#)
                .unwrap();
        assert_eq!(
            canonical(&value).unwrap(),
            b"{\"registry_name\":\"Owned caf\\u00e9 \\ud83d\\udca1 fixture\",\"plugins\":[]}\n"
        );
    }
    #[test]
    fn source_ascii_bytes_and_del() {
        assert_eq!(
            canonical(&serde_json::json!({"x":"\u{7f}"})).unwrap(),
            b"{\"x\":\"\\u007f\"}\n"
        );
    }
}
