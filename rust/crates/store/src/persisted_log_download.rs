//! Original Dispatcharr's persisted log attachment. Original authors retain credit.
use super::{family, missing_log, LogDirectory};
use crate::StoreError;
use axum::{
    body::{Body, Bytes},
    http::header,
    response::Response,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use std::{fmt::Write, io::Read, path::Path, sync::Arc};
use tokio::sync::{mpsc, OwnedSemaphorePermit};

const CHUNK: usize = 32 * 1024;

fn disposition(name: &str) -> String {
    let value = format!("attachment; filename=\"{name}\"");
    if name.is_ascii() {
        return value;
    }
    // The pinned view overwrites FileResponse's generated disposition.
    // Django ResponseHeaders then uses email.header.Header for values that
    // cannot be encoded as Latin-1: UTF-8 shortest header encoding, ties Q.
    fn q_literal(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || b" !*+-/".contains(&byte)
    }
    let q_len: usize = value
        .bytes()
        .map(|byte| if q_literal(byte) { 1 } else { 3 })
        .sum();
    if value.len().div_ceil(3) * 4 < q_len {
        return format!("=?utf-8?b?{}?=", STANDARD.encode(value));
    }
    let mut encoded = String::with_capacity(q_len + 12);
    encoded.push_str("=?utf-8?q?");
    for byte in value.bytes() {
        if byte == b' ' {
            encoded.push('_');
        } else if q_literal(byte) {
            encoded.push(char::from(byte));
        } else {
            write!(&mut encoded, "={byte:02X}").expect("writing a String cannot fail");
        }
    }
    encoded.push_str("?=");
    encoded
}

pub(super) fn serve(
    permit: OwnedSemaphorePermit,
    root: &Path,
    name: &str,
) -> Result<Response, StoreError> {
    if !family(name) {
        return Err(missing_log());
    }
    let file = LogDirectory::open(root)
        .and_then(|dir| dir.open_regular(name))
        .ok_or_else(missing_log)?;
    let size = file
        .metadata()
        .map_err(|_| StoreError::UnhandledRestFault)?
        .len();
    let permit = Arc::new(permit);
    let worker_permit = permit.clone();
    let (sender, mut receiver) = mpsc::channel(1);
    // The blocking reader retains the file and permit even if the response is
    // lost during a physical read. A dropped receiver releases backpressure.
    tokio::task::spawn_blocking(move || {
        let _owner = worker_permit;
        let mut input = file.take(size);
        loop {
            let mut raw = vec![0; CHUNK];
            match input.read(&mut raw) {
                Ok(0) => break,
                Ok(len) => {
                    raw.truncate(len);
                    if sender.blocking_send(Ok(Bytes::from(raw))).is_err() {
                        break;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    let _ = sender.blocking_send(Err(error));
                    break;
                }
            }
        }
    });
    let stream = async_stream::stream! {
        let _owner = permit;
        while let Some(next) = receiver.recv().await {
            yield next;
        }
    };
    Response::builder()
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(header::CONTENT_DISPOSITION, disposition(name))
        .header(header::CONTENT_LENGTH, size)
        .body(Body::from_stream(stream))
        .map_err(|_| StoreError::UnhandledRestFault)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_source_quoted_and_shortest_mime_disposition() {
        assert_eq!(
            disposition("dispatcharr.log.1"),
            "attachment; filename=\"dispatcharr.log.1\""
        );
        assert_eq!(
            disposition("dispatcharr.log.١"),
            "=?utf-8?b?YXR0YWNobWVudDsgZmlsZW5hbWU9ImRpc3BhdGNoYXJyLmxvZy7ZoSI=?="
        );
        assert_eq!(
            disposition("dispatcharr.log-web0123456789abc.١"),
            "=?utf-8?q?attachment=3B_filename=3D=22dispatcharr=2Elog-web0123456789abc=2E=D9=A1=22?="
        );
    }
}
