//! Original Requests HTTPStreamReader semantics with bounded streaming state.
//! Full Dispatcharr author/contributor attribution remains in ATTRIBUTION.md.
use bytes::Bytes;
use flate2::{Decompress, FlushDecompress, Status};
use futures_util::{Stream, StreamExt};
use reqwest::{
    cookie::{CookieStore, Jar},
    header::{self, HeaderValue},
    Client, RequestBuilder, Response, Url,
};
use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
const FRAME: usize = 8192;
const PREFIX: usize = 64 * 1024;
const HEADER_BUDGET: usize = 64 * 1024;
const COOKIE_COUNT: usize = 512;
const CHAIN: usize = 5; // Pinned urllib3 MultiDecoder.max_decode_links.
const DEFAULT_AGENT: &str = "python-requests/2.34.2";
pub type ByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;

#[derive(Default)]
struct AttemptCookies {
    jar: Jar,
    bytes: AtomicUsize,
    count: AtomicUsize,
    exceeded: AtomicBool,
}
impl CookieStore for AttemptCookies {
    fn set_cookies(&self, headers: &mut dyn Iterator<Item = &HeaderValue>, url: &Url) {
        for header in headers {
            let bytes = self
                .bytes
                .fetch_add(header.as_bytes().len(), Ordering::Relaxed);
            let count = self.count.fetch_add(1, Ordering::Relaxed);
            if bytes.saturating_add(header.as_bytes().len()) > HEADER_BUDGET
                || count >= COOKIE_COUNT
            {
                self.exceeded.store(true, Ordering::Relaxed);
                continue;
            }
            // Requests receives HTTP header values as Latin-1 strings. Jar's
            // parser expects UTF-8; translate both boundaries without changing
            // the actual cookie bytes sent on the wire.
            let value: String = header
                .as_bytes()
                .iter()
                .map(|byte| char::from(*byte))
                .collect();
            if let Ok(value) = HeaderValue::from_bytes(value.as_bytes()) {
                self.jar.set_cookies(&mut std::iter::once(&value), url);
            }
        }
    }
    fn cookies(&self, url: &Url) -> Option<HeaderValue> {
        let value = self.jar.cookies(url)?;
        let text = std::str::from_utf8(value.as_bytes()).ok()?;
        latin1(text).ok()
    }
}

pub struct Attempt {
    client: Client,
    cookies: Arc<AttemptCookies>,
}
impl Attempt {
    pub fn new(connect: Duration, read: Duration) -> Result<Self, reqwest::Error> {
        Self::new_with_redirects(connect, read, true)
    }
    /// Opt out only for a caller that supervises intermediate response bodies.
    /// Existing callers keep the legacy thirty-hop automatic redirect policy.
    pub fn new_with_redirects(
        connect: Duration,
        read: Duration,
        follow: bool,
    ) -> Result<Self, reqwest::Error> {
        Self::configured(connect, read, follow, true)
    }
    /// A Source Requests-compatible session may supply its own cookie URL context
    /// for URL-embedded credentials; it still uses this bounded attempt jar.
    pub fn new_manual_session(connect: Duration, read: Duration) -> Result<Self, reqwest::Error> {
        Self::configured(connect, read, false, false)
    }
    fn configured(
        connect: Duration,
        read: Duration,
        follow: bool,
        automatic_cookies: bool,
    ) -> Result<Self, reqwest::Error> {
        let cookies = Arc::new(AttemptCookies::default());
        let builder = Client::builder()
            .connect_timeout(connect)
            .read_timeout(read)
            .referer(false)
            .http1_only()
            .http1_title_case_headers()
            .pool_max_idle_per_host(1)
            .retry(reqwest::retry::never())
            .no_gzip()
            .no_deflate()
            .no_brotli()
            .no_zstd()
            .redirect(if follow {
                reqwest::redirect::Policy::limited(30)
            } else {
                reqwest::redirect::Policy::none()
            });
        let builder = if automatic_cookies {
            builder.cookie_provider(cookies.clone())
        } else {
            builder
        };
        let client = builder.build()?;
        Ok(Self { client, cookies })
    }
    pub fn request(
        &self,
        url: &str,
        agent: &str,
        headers: &[(String, String)],
    ) -> io::Result<RequestBuilder> {
        let agent = if agent.is_empty() {
            DEFAULT_AGENT
        } else {
            agent
        };
        let mut request = self
            .client
            .get(url)
            .header(header::USER_AGENT, latin1(agent)?)
            .header(header::ACCEPT, "*/*")
            .header(header::ACCEPT_ENCODING, "gzip, deflate")
            .header(header::CONNECTION, "keep-alive");
        let mut size = agent.len();
        for (name, value) in headers {
            size = size.saturating_add(name.len()).saturating_add(value.len());
            if size > HEADER_BUDGET {
                return Err(invalid("upstream request headers exceed budget"));
            }
            request = request.header(name, latin1(value)?);
        }
        Ok(request)
    }
    pub fn session_cookie_header(&self, url: &Url) -> Option<String> {
        self.cookies.cookies(url).map(|value| {
            value
                .as_bytes()
                .iter()
                .map(|byte| char::from(*byte))
                .collect()
        })
    }
    pub fn store_session_cookies(&self, headers: &header::HeaderMap, url: &Url) {
        self.cookies
            .set_cookies(&mut headers.get_all(header::SET_COOKIE).iter(), url);
    }
    pub fn cookie_budget_exceeded(&self) -> bool {
        self.cookies.exceeded.load(Ordering::Relaxed)
    }
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn latin1(value: &str) -> io::Result<HeaderValue> {
    if value.chars().next().is_some_and(char::is_whitespace)
        || value.contains(['\r', '\n'])
        || value.len() > HEADER_BUDGET
    {
        return Err(invalid("invalid upstream header value"));
    }
    let bytes: Result<Vec<_>, _> = value
        .chars()
        .map(|ch| u8::try_from(u32::from(ch)))
        .collect();
    HeaderValue::from_bytes(&bytes.map_err(|_| invalid("upstream header is not Latin-1"))?)
        .map_err(|_| invalid("invalid upstream header bytes"))
}
#[derive(Clone, Copy, PartialEq)]
enum Coding {
    Gzip,
    Deflate,
}
fn known(value: &str) -> bool {
    matches!(value, "gzip" | "x-gzip" | "deflate")
}
/// Actual compressed response bytes exceeded the caller's admitted wire budget.
#[derive(Debug)]
pub struct WireLimitExceeded;
impl std::fmt::Display for WireLimitExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("upstream compressed wire byte budget exceeded")
    }
}
impl std::error::Error for WireLimitExceeded {}
/// Native bounded-storage limits, distinct from malformed provider encodings.
#[derive(Debug)]
pub enum ResourceLimitExceeded {
    HeaderBytes,
    DeflatePrefix,
}
impl std::fmt::Display for ResourceLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::HeaderBytes => "upstream response headers exceed budget",
            Self::DeflatePrefix => "deflate first-output prefix exceeds budget",
        })
    }
}
impl std::error::Error for ResourceLimitExceeded {}
pub fn body(response: Response) -> io::Result<ByteStream> {
    body_with_wire_limit(response, None)
}
/// Preserve the existing streaming decoder while charging actual received bytes
/// before any decoder prefix/output buffers allocate from each incoming frame.
pub fn body_limited(response: Response, max_wire_bytes: usize) -> io::Result<ByteStream> {
    body_with_wire_limit(response, Some(max_wire_bytes))
}
fn body_with_wire_limit(response: Response, maximum: Option<usize>) -> io::Result<ByteStream> {
    if response
        .headers()
        .iter()
        .map(|(name, value)| name.as_str().len() + value.as_bytes().len())
        .sum::<usize>()
        > HEADER_BUDGET
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            ResourceLimitExceeded::HeaderBytes,
        ));
    }
    let encoding = response
        .headers()
        .get_all(header::CONTENT_ENCODING)
        .iter()
        .map(|value| {
            value
                .as_bytes()
                .iter()
                .map(|byte| char::from(*byte))
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join(",")
        .to_ascii_lowercase();
    let modes: Vec<_> = encoding.split(',').map(str::trim).collect();
    let decode =
        known(&encoding) || (encoding.contains(',') && modes.iter().any(|mode| known(mode)));
    let mut stream: ByteStream = Box::pin({
        let mut incoming = response.bytes_stream();
        async_stream::try_stream! {
            let mut received = 0usize;
            while let Some(frame) = incoming.next().await {
                let frame = frame.map_err(io::Error::other)?;
                if let Some(maximum) = maximum {
                    received = received.checked_add(frame.len())
                        .filter(|total| *total <= maximum)
                        .ok_or_else(|| io::Error::other(WireLimitExceeded))?;
                }
                yield frame;
            }
        }
    });
    if decode {
        if modes.len() > CHAIN {
            return Err(invalid("too many upstream content encodings"));
        }
        for mode in modes.into_iter().rev() {
            let coding = if matches!(mode, "gzip" | "x-gzip") {
                Coding::Gzip
            } else {
                Coding::Deflate
            };
            stream = decode_layer(stream, coding);
        }
    }
    Ok(stream)
}
fn decode_layer(mut source: ByteStream, coding: Coding) -> ByteStream {
    Box::pin(async_stream::try_stream! {
        let mut decoder=if coding==Coding::Gzip{Decompress::new_gzip(15)}else{Decompress::new(true)};
        let mut probe=if coding==Coding::Deflate{Some(Vec::new())}else{None};
        let mut finished=false;let mut other_member=false;let mut swallow=false;
        while let Some(frame)=source.next().await {
            let frame=frame?;
            if swallow || finished {tokio::task::yield_now().await;continue;}
            for part in frame.chunks(FRAME) {
                if swallow || finished {break;}
                if let Some(prefix)=probe.as_mut() {
                    if prefix.len()+part.len()>PREFIX {Err(io::Error::new(io::ErrorKind::InvalidData,ResourceLimitExceeded::DeflatePrefix))?;}
                    prefix.extend_from_slice(part);
                }
                let mut pending=Bytes::copy_from_slice(part);let mut offset=0;
                loop {
                    let mut output=[0u8;FRAME];let before_in=decoder.total_in();let before_out=decoder.total_out();
                    let status=decoder.decompress(&pending[offset..],&mut output,FlushDecompress::None);
                    let status=match status {
                        Ok(status)=>status,
                        Err(_) if probe.is_some()=>{
                            pending=Bytes::from(probe.take().unwrap());offset=0;decoder=Decompress::new(false);continue;
                        }
                        Err(_) if coding==Coding::Gzip && other_member=>{swallow=true;break;}
                        Err(_)=>Err(invalid("invalid upstream compressed data"))?,
                    };
                    let consumed=(decoder.total_in()-before_in) as usize;
                    let produced=(decoder.total_out()-before_out) as usize;
                    offset+=consumed;
                    if produced>0 {probe=None;yield Bytes::copy_from_slice(&output[..produced]);}
                    if status==Status::StreamEnd {
                        if coding==Coding::Gzip {other_member=true;decoder=Decompress::new_gzip(15);if offset==pending.len(){break;}continue;}
                        finished=true;break;
                    }
                    if offset==pending.len() && produced<FRAME {break;}
                    if consumed==0 && produced==0 {Err(invalid("upstream decompressor made no progress"))?;}
                }
            }
        }
        // Python zlib.flush() permits a truncated final stream; do not invent
        // an EOF/trailer requirement. Preserve errors from actual compressed data.
        if !swallow && !finished {
            loop {
                let mut output=[0u8;FRAME];let before=decoder.total_out();
                match decoder.decompress(&[],&mut output,FlushDecompress::Finish) {
                    Ok(_)=>{},Err(_) if coding==Coding::Gzip && other_member=>break,
                    Err(_)=>Err(invalid("invalid upstream compression flush"))?,
                }
                let produced=(decoder.total_out()-before) as usize;
                if produced==0 {break;}
                yield Bytes::copy_from_slice(&output[..produced]);
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{
        write::{DeflateEncoder, GzEncoder, ZlibEncoder},
        Compression,
    };
    use std::io::Write;
    fn encode(data: &[u8], kind: &str) -> Vec<u8> {
        match kind {
            "gzip" => {
                let mut writer = GzEncoder::new(Vec::new(), Compression::default());
                writer.write_all(data).unwrap();
                writer.finish().unwrap()
            }
            "zlib" => {
                let mut writer = ZlibEncoder::new(Vec::new(), Compression::default());
                writer.write_all(data).unwrap();
                writer.finish().unwrap()
            }
            _ => {
                let mut writer = DeflateEncoder::new(Vec::new(), Compression::default());
                writer.write_all(data).unwrap();
                writer.finish().unwrap()
            }
        }
    }
    async fn decode(data: Vec<u8>, coding: Coding, split: usize) -> io::Result<Vec<u8>> {
        let frames: Vec<_> = data
            .chunks(split)
            .map(|bytes| Ok(Bytes::copy_from_slice(bytes)))
            .collect();
        let mut stream = decode_layer(Box::pin(futures_util::stream::iter(frames)), coding);
        let mut result = Vec::new();
        while let Some(frame) = stream.next().await {
            let frame = frame?;
            assert!(frame.len() <= FRAME);
            result.extend(frame);
        }
        Ok(result)
    }
    #[tokio::test]
    async fn incremental_gzip_zlib_and_raw_deflate_match_plain_bytes_with_bounded_frames() {
        let data: Vec<_> = (0..300_000).map(|index| (index % 251) as u8).collect();
        for (kind, coding) in [
            ("gzip", Coding::Gzip),
            ("zlib", Coding::Deflate),
            ("raw", Coding::Deflate),
        ] {
            for split in [1, 7, 8192, usize::MAX] {
                assert_eq!(
                    decode(encode(&data, kind), coding, split).await.unwrap(),
                    data,
                    "{kind}/{split}"
                );
            }
        }
    }
    #[tokio::test]
    async fn concatenated_gzip_and_trailing_junk_follow_original_other_member_policy() {
        let mut data = encode(b"first", "gzip");
        data.extend(encode(b"second", "gzip"));
        data.extend(b"junk after valid gzip members");
        assert_eq!(decode(data, Coding::Gzip, 3).await.unwrap(), b"firstsecond");
        assert!(decode(b"not gzip".to_vec(), Coding::Gzip, 2).await.is_err());
    }
    #[tokio::test]
    async fn stacked_decoders_preserve_bytes_and_truncated_eof_is_not_new_failure() {
        let data = vec![0x47; 188 * 4096];
        let outer = encode(&encode(&data, "gzip"), "zlib");
        let source: ByteStream = Box::pin(futures_util::stream::iter(vec![Ok(Bytes::from(outer))]));
        let mut stream = decode_layer(decode_layer(source, Coding::Deflate), Coding::Gzip);
        let mut plain = Vec::new();
        while let Some(frame) = stream.next().await {
            plain.extend(frame.unwrap());
        }
        assert_eq!(plain, data);
        let mut truncated = encode(&data, "gzip");
        truncated.truncate(truncated.len() - 8);
        assert_eq!(decode(truncated, Coding::Gzip, 17).await.unwrap(), data);
    }
    #[tokio::test]
    async fn deflate_no_output_probe_has_an_explicit_bounded_prefix() {
        let mut encoded = vec![0x78, 0x01];
        for _ in 0..(PREFIX / 5 + 2) {
            encoded.extend_from_slice(&[0, 0, 0, 255, 255]);
        }
        let error = decode(encoded, Coding::Deflate, 8192).await.unwrap_err();
        assert!(error.to_string().contains("prefix exceeds budget"));
    }
    #[tokio::test]
    async fn actual_chunked_wire_limit_precedes_stacked_decoding() {
        use std::io::{Read, Write};
        let plain = vec![b'x'; 128 * 1024];
        let payload = encode(&encode(&plain, "gzip"), "zlib");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut request = Vec::new();
                let mut byte = [0u8; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    socket.read_exact(&mut byte).unwrap();
                    request.push(byte[0]);
                }
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip, deflate\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").unwrap();
                for chunk in payload.chunks(7) {
                    if write!(socket, "{:x}\r\n", chunk.len())
                        .and_then(|_| socket.write_all(chunk))
                        .and_then(|_| socket.write_all(b"\r\n"))
                        .is_err()
                    {
                        break;
                    }
                }
                let _ = socket.write_all(b"0\r\n\r\n");
            }
        });
        let client = Client::builder().no_proxy().build().unwrap();
        let length = encode(&encode(&plain, "gzip"), "zlib").len();
        for maximum in [length, length - 1] {
            let response = client
                .get(format!("http://{address}/fixture"))
                .send()
                .await
                .unwrap();
            let mut stream = body_limited(response, maximum).unwrap();
            let mut received = Vec::new();
            let mut failure = None;
            while let Some(frame) = stream.next().await {
                match frame {
                    Ok(bytes) => received.extend_from_slice(&bytes),
                    Err(error) => {
                        failure = Some(error);
                        break;
                    }
                }
            }
            if maximum == length {
                assert!(failure.is_none());
                assert_eq!(received, plain);
            } else {
                assert!(failure
                    .unwrap()
                    .get_ref()
                    .unwrap()
                    .is::<WireLimitExceeded>());
            }
        }
        server.join().unwrap();
    }
    #[tokio::test]
    async fn explicit_referer_survives_without_automatic_redirect_referer() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut observed = Vec::new();
            for index in 0..2 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut byte = [0u8; 1];
                while !bytes.ends_with(b"\r\n\r\n") {
                    socket.read_exact(&mut byte).unwrap();
                    bytes.push(byte[0]);
                }
                observed.push(String::from_utf8(bytes).unwrap());
                if index == 0 {
                    socket.write_all(b"HTTP/1.1 302 Found\r\nLocation: /terminal\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                } else {
                    socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                        )
                        .unwrap();
                }
            }
            observed
        });
        let attempt = Attempt::new(Duration::from_secs(5), Duration::from_secs(5)).unwrap();
        let response = attempt
            .request(
                &format!("http://{address}/initial"),
                "fixture-agent",
                &[("Referer".into(), "http://explicit.test/kept".into())],
            )
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        drop(response);
        for request in server.join().unwrap() {
            assert!(request
                .to_ascii_lowercase()
                .contains("referer: http://explicit.test/kept\r\n"));
        }
    }
    #[test]
    fn latin1_matches_requests_and_rejects_invalid_header_values() {
        assert_eq!(latin1("é").unwrap().as_bytes(), b"\xe9");
        assert_eq!(latin1("Agent\tvalue").unwrap().as_bytes(), b"Agent\tvalue");
        for value in [" ", " Agent", "\tAgent", "Agent\r\nInjected:x", "emoji😀"] {
            assert!(latin1(value).is_err(), "{value:?}");
        }
    }
    #[test]
    fn cookie_jar_is_attempt_scoped_and_has_cumulative_capacity() {
        let url = Url::parse("http://example.test/path").unwrap();
        let first = AttemptCookies::default();
        let second = AttemptCookies::default();
        first.set_cookies(
            &mut std::iter::once(&HeaderValue::from_static("a=fixed; Path=/")),
            &url,
        );
        assert_eq!(first.cookies(&url).unwrap(), "a=fixed");
        assert!(second.cookies(&url).is_none());
        assert!(first
            .cookies(&Url::parse("http://other.test/").unwrap())
            .is_none());
        for _ in 0..COOKIE_COUNT {
            first.set_cookies(
                &mut std::iter::once(&HeaderValue::from_static("b=fixed")),
                &url,
            );
        }
        assert!(first.exceeded.load(Ordering::Relaxed));
    }
}
