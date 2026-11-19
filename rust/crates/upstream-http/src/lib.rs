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

// Source Requests gives a nonempty REQUESTS_CA_BUNDLE precedence over CURL's
// fallback. An invalid selected override is an error, never default-root trust.
// Arguments keep tests independent of process-global environment mutation.
fn requests_ca_roots(
    requests: Option<std::ffi::OsString>,
    curl: Option<std::ffi::OsString>,
) -> io::Result<Option<Vec<reqwest::Certificate>>> {
    use std::io::Read;
    let path = requests
        .filter(|value| !value.is_empty())
        .or_else(|| curl.filter(|value| !value.is_empty()));
    let Some(path) = path else { return Ok(None) };
    let file = std::fs::File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid(
            "Requests CA directories are outside this bounded factory",
        ));
    }
    let mut bytes = Vec::new();
    file.take(4 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(invalid("Requests CA bundle capacity"));
    }
    let certificates = reqwest::Certificate::from_pem_bundle(&bytes).map_err(io::Error::other)?;
    if certificates.is_empty() || certificates.len() > 128 {
        return Err(invalid("Requests CA certificate count"));
    }
    Ok(Some(certificates))
}

fn requests_ca_roots_from_env() -> io::Result<Option<Vec<reqwest::Certificate>>> {
    requests_ca_roots(
        std::env::var_os("REQUESTS_CA_BUNDLE"),
        std::env::var_os("CURL_CA_BUNDLE"),
    )
}

fn playback_ca_roots() -> Option<Vec<reqwest::Certificate>> {
    // Requests checks its CA path only for HTTPS. An invalid selected bundle
    // therefore leaves HTTP usable but must reject every TLS peer, including
    // an HTTP-to-HTTPS redirect. An empty store keeps verification enabled.
    requests_ca_roots_from_env().unwrap_or_else(|_| Some(Vec::new()))
}

/// Apply the bounded Requests trust policy while retaining the playback
/// client's own redirects, pooling, and body handling.
pub fn with_playback_requests_trust(builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    with_ca_roots(builder, playback_ca_roots())
}

fn with_ca_roots(
    builder: reqwest::ClientBuilder,
    roots: Option<Vec<reqwest::Certificate>>,
) -> reqwest::ClientBuilder {
    match roots {
        Some(roots) => roots.into_iter().fold(
            builder.tls_built_in_root_certs(false),
            |builder, certificate| builder.add_root_certificate(certificate),
        ),
        None => builder,
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
    /// Preserve the refresh caller's manual redirect/cookie policy while
    /// honoring Source Requests' environment trust bundle.
    pub fn new_manual_requests_session(connect: Duration, read: Duration) -> io::Result<Self> {
        Self::manual_requests_with_paths(
            connect,
            read,
            std::env::var_os("REQUESTS_CA_BUNDLE"),
            std::env::var_os("CURL_CA_BUNDLE"),
        )
    }
    fn manual_requests_with_paths(
        connect: Duration,
        read: Duration,
        requests: Option<std::ffi::OsString>,
        curl: Option<std::ffi::OsString>,
    ) -> io::Result<Self> {
        let roots = requests_ca_roots(requests, curl)?;
        Self::configured_with_roots(connect, read, false, false, roots).map_err(io::Error::other)
    }
    /// Source Requests environment trust override for ordinary independent
    /// provider calls. Reuse this factory and its physical client; do not bypass
    /// certificate verification to reach an owned acceptance Origin.
    pub fn new_requests(connect: Duration, read: Duration) -> io::Result<Self> {
        let roots = requests_ca_roots_from_env()?;
        Self::configured_with_roots(connect, read, true, true, roots).map_err(io::Error::other)
    }
    /// Native playback keeps plain HTTP usable with an invalid CA override.
    pub fn new_playback_requests(
        connect: Duration,
        read: Duration,
    ) -> Result<Self, reqwest::Error> {
        Self::configured_with_roots(connect, read, true, true, playback_ca_roots())
    }
    fn configured(
        connect: Duration,
        read: Duration,
        follow: bool,
        automatic_cookies: bool,
    ) -> Result<Self, reqwest::Error> {
        Self::configured_with_roots(connect, read, follow, automatic_cookies, None)
    }
    fn configured_with_roots(
        connect: Duration,
        read: Duration,
        follow: bool,
        automatic_cookies: bool,
        roots: Option<Vec<reqwest::Certificate>>,
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
        let builder = with_ca_roots(builder, roots);
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
        self.request_method(reqwest::Method::GET, url, agent, headers)
    }
    /// Source Requests HEAD/GET validation uses the same bounded factory and
    /// redirect/TLS/header policy, with no separate client or admission pool.
    pub fn request_method(
        &self,
        method: reqwest::Method,
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
            .request(method, url)
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
    body_with_wire_limit(response, None, None)
}
/// Preserve the existing streaming decoder while charging actual received bytes
/// before any decoder prefix/output buffers allocate from each incoming frame.
pub fn body_limited(response: Response, max_wire_bytes: usize) -> io::Result<ByteStream> {
    body_with_wire_limit(response, Some(max_wire_bytes), None)
}
/// Actual received wire bytes before existing content decoding, for Source
/// iter_content/raw.tell progress. The observer does not change stream ownership.
pub fn body_measured(
    response: Response,
    max_wire_bytes: usize,
    received: std::sync::Arc<std::sync::atomic::AtomicU64>,
) -> io::Result<ByteStream> {
    body_with_wire_limit(response, Some(max_wire_bytes), Some(received))
}
fn body_with_wire_limit(
    response: Response,
    maximum: Option<usize>,
    wire: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
) -> io::Result<ByteStream> {
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
                if let Some(wire)=wire.as_ref(){wire.fetch_add(frame.len() as u64,std::sync::atomic::Ordering::AcqRel);}
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
    #[tokio::test]
    async fn source_head_validation_reuses_get_headers_and_range_without_network() {
        let attempt = Attempt::new(Duration::from_secs(4), Duration::from_secs(4)).unwrap();
        let get = attempt
            .request("https://owned.invalid/poster", "", &[])
            .unwrap()
            .build()
            .unwrap();
        let head = attempt
            .request_method(
                reqwest::Method::HEAD,
                "https://owned.invalid/poster",
                "",
                &[],
            )
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(get.method(), reqwest::Method::GET);
        assert_eq!(head.method(), reqwest::Method::HEAD);
        assert_eq!(get.url(), head.url());
        assert_eq!(get.headers(), head.headers());
        let ranged = attempt
            .request(
                "https://owned.invalid/poster",
                "",
                &[("Range".into(), "bytes=0-0".into())],
            )
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(ranged.headers()["range"], "bytes=0-0");
    }

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

#[cfg(test)]
mod measured_wire_control {
    use super::*;
    #[tokio::test]
    async fn actual_gzip_response_counts_encoded_wire_before_decoding() {
        use std::io::{Read, Write};
        let payload = vec![b'a'; 100_000];
        let mut compressor =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        compressor.write_all(&payload).unwrap();
        let encoded = compressor.finish().unwrap();
        let actual_wire = encoded.len();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut byte = [0];
            while !request.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
                assert!(request.len() <= 8192);
            }
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Encoding: gzip\r\nConnection: close\r\n\r\n",encoded.len()).as_bytes()).unwrap();
            socket.write_all(&encoded).unwrap();
            socket.flush().unwrap();
            drop(socket);
        });
        let attempt = Attempt::new(Duration::from_secs(5), Duration::from_secs(5)).unwrap();
        let response = attempt
            .request(
                &format!("http://{address}/guide"),
                "owned-Requests-agent",
                &[],
            )
            .unwrap()
            .send()
            .await
            .unwrap();
        let count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut body = body_measured(response, actual_wire, count.clone()).unwrap();
        let mut decoded = Vec::new();
        while let Some(bytes) = body.next().await {
            decoded.extend_from_slice(&bytes.unwrap());
        }
        server.join().unwrap();
        assert_eq!(decoded, payload);
        assert_eq!(
            count.load(std::sync::atomic::Ordering::Acquire),
            actual_wire as u64
        );
        assert_ne!(actual_wire, decoded.len());
    }
}

#[cfg(test)]
mod requests_ca_override_control {
    use super::*;
    use std::{
        ffi::OsString,
        path::{Path, PathBuf},
    };
    const CA: &[u8] =
        include_bytes!("../../../vendor/sqlx-core-0.8.6/src/net/tls/fixtures/trusted-ca.pem");
    struct OwnedDirectory(PathBuf);
    impl OwnedDirectory {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "dispatcharr-ca-{}-{time}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn write(&self, name: &str, bytes: &[u8]) -> PathBuf {
            let path = self.0.join(name);
            std::fs::write(&path, bytes).unwrap();
            path
        }
    }
    impl Drop for OwnedDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn arg(path: &Path) -> Option<OsString> {
        Some(path.as_os_str().to_owned())
    }
    #[test]
    fn requests_bundle_precedence_and_empty_fallback_do_not_change_process_environment() {
        let directory = OwnedDirectory::new();
        let valid = directory.write("owned-public-ca.pem", CA);
        let missing = directory.0.join("missing.pem");
        assert!(requests_ca_roots(None, None).unwrap().is_none());
        assert_eq!(
            requests_ca_roots(arg(&valid), arg(&missing))
                .unwrap()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            requests_ca_roots(Some(OsString::new()), arg(&valid))
                .unwrap()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            requests_ca_roots(arg(&missing), arg(&valid))
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::NotFound
        );
    }
    #[test]
    fn manual_requests_factory_rejects_missing_invalid_and_oversized_selected_roots() {
        let directory = OwnedDirectory::new();
        let valid = directory.write("valid.pem", CA);
        let invalid = directory.write("invalid.pem", b"not a certificate");
        let large = directory.write("oversized.pem", &vec![b'x'; 4 * 1024 * 1024 + 1]);
        for path in [
            &directory.0.join("missing.pem"),
            &invalid,
            &large,
            &directory.0,
        ] {
            assert!(Attempt::manual_requests_with_paths(
                Duration::from_secs(2),
                Duration::from_secs(2),
                arg(path),
                arg(&valid)
            )
            .is_err());
        }
        let too_many = directory.write("many.pem", &CA.repeat(129));
        assert!(requests_ca_roots(arg(&too_many), None).is_err());
    }
    #[tokio::test]
    async fn manual_requests_override_keeps_redirect_and_cookie_ownership_with_caller() {
        use std::io::{Read, Write};
        let directory = OwnedDirectory::new();
        let valid = directory.write("owned-public-ca.pem", CA);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            for index in 0..3 {
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            assert!(
                                std::time::Instant::now() < deadline,
                                "owned request deadline"
                            );
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("owned accept: {error}"),
                    }
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut bytes = Vec::new();
                while !bytes.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    socket.read_exact(&mut byte).unwrap();
                    bytes.push(byte[0]);
                    assert!(bytes.len() <= 8192);
                }
                requests.push(String::from_utf8(bytes).unwrap());
                let reply = if index == 2 {
                    format!("HTTP/1.1 302 Found\r\nLocation: http://{address}/must-not-follow\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                } else {
                    "HTTP/1.1 200 OK\r\nSet-Cookie: owned=1; Path=/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned()
                };
                socket.write_all(reply.as_bytes()).unwrap();
            }
            requests
        });
        let attempt = Attempt::manual_requests_with_paths(
            Duration::from_secs(3),
            Duration::from_secs(3),
            arg(&valid),
            None,
        )
        .unwrap();
        for (index, expected) in [200, 200, 302].into_iter().enumerate() {
            let response = attempt
                .request(
                    &format!("http://{address}/{index}"),
                    "owned-manual-agent",
                    &[],
                )
                .unwrap()
                .send()
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), expected);
        }
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests
            .iter()
            .all(|request| !request.to_ascii_lowercase().contains("\r\ncookie:")));
    }
}
