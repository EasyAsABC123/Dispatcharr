//! Original Dispatcharr authors retain application/algorithm/license credit.
//! Candidate hostname/HTTPS transport for the measured Source16 domain.
//! DNS driver closure, complete trust-store parity, and production integration
//! require separate physical acceptance.
use super::{address_allowed, Failure, TransportUse, INACTIVITY, RAW_LIMIT};
use dispatcharr_upstream_http::{body_limited, ResourceLimitExceeded, WireLimitExceeded};
use futures_util::StreamExt;
use reqwest::{header, redirect::Policy, Client, ClientBuilder, Response, Url};
use std::{net::IpAddr, path::Path, time::Duration};

const MAX_DNS_ADDRESSES: usize = 64;
const MAX_CA_BYTES: usize = 1024 * 1024;
const MAX_TARGET_BYTES: usize = 8192;
const MAX_HEADER_BYTES: usize = 65536;
const MANIFEST_BODY_LIMIT: &str = "Manifest body capacity unavailable";
pub(super) const UNQUALIFIED_DNS: &str = "Platform DNS error rendering remains unqualified";

/// Source validates all DNS answers first, then requests resolves independently.
/// Do not pin validation answers into the HTTP connector: Source16 case8
/// actually accepts the separately-resolved owned loopback rebind.
pub(super) fn request_domain(url: &Url) -> Result<(), Failure> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(Failure::Policy(format!(
            "URL scheme '{}' is not allowed; only http and https are permitted.",
            url.scheme()
        )));
    }
    if url.host_str().is_none() {
        return Err(Failure::Policy("URL has no hostname.".into()));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Failure::Unqualified("URL credentials remain unqualified"));
    }
    Ok(())
}
fn validate_address(ip: IpAddr) -> Result<(), Failure> {
    if address_allowed(ip) {
        Ok(())
    } else {
        Err(Failure::Policy(format!(
            "URL resolves to a non-routable address ({ip}) and cannot be fetched."
        )))
    }
}

async fn validate(url: &Url, purpose: TransportUse) -> Result<(), Failure> {
    request_domain(url)?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(Failure::Policy(format!(
            "URL scheme '{}' is not allowed; only http and https are permitted.",
            url.scheme()
        )));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Failure::Unqualified("URL credentials remain unqualified"));
    }
    let host = url
        .host_str()
        .ok_or_else(|| Failure::Policy("URL has no hostname.".into()))?
        .trim_matches(['[', ']']);
    if host.len() > 253 || url.as_str().len() > MAX_TARGET_BYTES {
        return Err(Failure::Unqualified("Manifest URL capacity unavailable"));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return validate_address(ip);
    }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    if matches!(purpose, TransportUse::RefreshTask) {
        return task_resolution::validate(host).await;
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    let _ = purpose;
    let answers = tokio::time::timeout(INACTIVITY, tokio::net::lookup_host((host, 0)))
        .await
        .map_err(|_| Failure::Timeout)?
        .map_err(|_| Failure::Unqualified(UNQUALIFIED_DNS))?;
    let mut count = 0;
    for answer in answers {
        count += 1;
        if count > MAX_DNS_ADDRESSES {
            return Err(Failure::Unqualified(
                "Manifest DNS answer capacity unavailable",
            ));
        }
        validate_address(answer.ip())?;
    }
    if count == 0 {
        return Err(Failure::Policy(format!(
            "Could not resolve hostname '{host}'."
        )));
    }
    Ok(())
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod task_resolution {
    use super::{validate_address, Failure, INACTIVITY, MAX_DNS_ADDRESSES, UNQUALIFIED_DNS};
    use std::{
        ffi::CString,
        mem::size_of,
        net::{IpAddr, Ipv4Addr, Ipv6Addr},
        ptr::{self, NonNull},
    };

    struct ResolverAnswers(NonNull<libc::addrinfo>);

    impl Drop for ResolverAnswers {
        fn drop(&mut self) {
            // The list was obtained from one successful getaddrinfo call.
            unsafe { libc::freeaddrinfo(self.0.as_ptr()) };
        }
    }

    fn resolution_failure(status: libc::c_int) -> Failure {
        match status {
            libc::EAI_AGAIN => Failure::PreConnectTemporaryResolution,
            _ => Failure::Unqualified(UNQUALIFIED_DNS),
        }
    }

    fn answer_address(answer: &libc::addrinfo) -> Option<IpAddr> {
        let len = answer.ai_addrlen as usize;
        if answer.ai_addr.is_null() || len < size_of::<libc::sa_family_t>() {
            return None;
        }
        // The resolver owns the sockaddr until the list is freed. Read only
        // the family and address fields, checking their layout and length.
        unsafe {
            let family = ptr::addr_of!((*answer.ai_addr).sa_family).read_unaligned();
            match libc::c_int::from(family) {
                libc::AF_INET if len >= size_of::<libc::sockaddr_in>() => {
                    let address = answer.ai_addr.cast::<libc::sockaddr_in>();
                    let bytes = ptr::addr_of!((*address).sin_addr.s_addr).read_unaligned();
                    Some(Ipv4Addr::from(bytes.to_ne_bytes()).into())
                }
                libc::AF_INET6 if len >= size_of::<libc::sockaddr_in6>() => {
                    let address = answer.ai_addr.cast::<libc::sockaddr_in6>();
                    let bytes = ptr::addr_of!((*address).sin6_addr.s6_addr).read_unaligned();
                    Some(Ipv6Addr::from(bytes).into())
                }
                _ => None,
            }
        }
    }

    fn validate_platform_answers(host: &str) -> Result<(), Failure> {
        let name = CString::new(host).map_err(|_| Failure::Unqualified(UNQUALIFIED_DNS))?;
        let hints = libc::addrinfo {
            ai_family: libc::AF_UNSPEC,
            ai_socktype: libc::SOCK_STREAM,
            ai_flags: 0,
            ai_protocol: 0,
            ai_addrlen: 0,
            ai_addr: ptr::null_mut(),
            ai_canonname: ptr::null_mut(),
            ai_next: ptr::null_mut(),
        };
        let mut first = ptr::null_mut();
        // The C string, hints, and out-pointer are valid for this call. Only
        // its success initializes a list that must be freed by this caller.
        let code = unsafe { libc::getaddrinfo(name.as_ptr(), ptr::null(), &hints, &mut first) };
        if code != 0 {
            return Err(resolution_failure(code));
        }
        let answers = NonNull::new(first).map(ResolverAnswers);
        let mut next = first;
        let mut count = 0;
        // Keep the list owned while traversing it. Like Rust's LookupHost,
        // count only entries convertible to an IP address.
        while let Some(answer) = unsafe { next.as_ref() } {
            next = answer.ai_next;
            let Some(address) = answer_address(answer) else {
                continue;
            };
            count += 1;
            if count > MAX_DNS_ADDRESSES {
                return Err(Failure::Unqualified(
                    "Manifest DNS answer capacity unavailable",
                ));
            }
            validate_address(address)?;
        }
        drop(answers);
        if count == 0 {
            return Err(Failure::Policy(format!(
                "Could not resolve hostname '{host}'."
            )));
        }
        Ok(())
    }

    pub(super) async fn validate(host: &str) -> Result<(), Failure> {
        let host = host.to_owned();
        tokio::time::timeout(
            INACTIVITY,
            tokio::task::spawn_blocking(move || validate_platform_answers(&host)),
        )
        .await
        .map_err(|_| Failure::Timeout)?
        .map_err(|_| Failure::Unqualified(UNQUALIFIED_DNS))?
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn only_temporary_libc_resolution_matches_the_task_class() {
            assert!(matches!(
                resolution_failure(libc::EAI_AGAIN),
                Failure::PreConnectTemporaryResolution
            ));
            for code in [
                libc::EAI_NONAME,
                libc::EAI_FAIL,
                libc::EAI_SYSTEM,
                libc::EAI_MEMORY,
                libc::EAI_FAMILY,
                libc::EAI_BADFLAGS,
            ] {
                assert!(matches!(
                    resolution_failure(code),
                    Failure::Unqualified(UNQUALIFIED_DNS)
                ));
            }
        }

        #[test]
        fn typed_resolver_passes_ipv4_and_ipv6_to_the_existing_address_policy() {
            // Numeric lookups exercise the platform sockaddr conversion
            // without forwarding a DNS request.
            for address in ["127.0.0.1", "::1"] {
                assert!(matches!(
                    validate_platform_answers(address),
                    Err(Failure::Policy(_))
                ));
            }
        }
    }
}

fn client(ca_bundle: Option<&Path>) -> Result<Client, Failure> {
    // Count coded entity bytes before content decoding, even if a different
    // workspace crate ever enables reqwest's automatic decoders.
    let builder = Client::builder()
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .no_deflate();
    client_with_timeout(builder, ca_bundle, INACTIVITY)
}
fn client_with_timeout(
    builder: ClientBuilder,
    ca_bundle: Option<&Path>,
    timeout: Duration,
) -> Result<Client, Failure> {
    let mut builder = builder
        .no_proxy()
        .redirect(Policy::none())
        .pool_max_idle_per_host(0)
        .http1_only()
        .connect_timeout(timeout)
        .read_timeout(timeout)
        .user_agent("python-requests/2.34.2");
    {
        let path =
            ca_bundle.unwrap_or_else(|| Path::new("/opt/dispatcharr-crypto/trust/cacert.pem"));
        let file = std::fs::File::open(path).map_err(|_| Failure::Connect)?;
        use std::io::Read;
        let mut bytes = Vec::new();
        file.take(MAX_CA_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Failure::Connect)?;
        if bytes.len() > MAX_CA_BYTES {
            return Err(Failure::Unqualified(
                "Manifest trust bundle capacity unavailable",
            ));
        }
        let certs = reqwest::Certificate::from_pem_bundle(&bytes).map_err(|_| Failure::Connect)?;
        if certs.is_empty() {
            return Err(Failure::Connect);
        }
        // requests' explicit CA bundle replaces its default trust roots.
        builder = builder.tls_built_in_root_certs(false);
        for cert in certs {
            builder = builder.add_root_certificate(cert);
        }
    }
    builder.build().map_err(|_| Failure::Connect)
}

/// Returns one response, leaving redirect policy/order to the existing fetcher.
/// A new client per hop avoids retaining a connection pool across actor owners.
/// Literal-IP plain HTTP continues to use the already qualified explicit-FD path.
pub(super) async fn one_get(
    url: &Url,
    ca_bundle: Option<&Path>,
    proxy_port: Option<u16>,
    purpose: TransportUse,
) -> Result<(u16, Option<String>, Vec<u8>), Failure> {
    validate(url, purpose).await?;
    super::proxy_environment::enforce(url, proxy_port)?;
    let client = client(ca_bundle)?;
    let request = client
        .get(url.clone())
        .header(header::ACCEPT, "*/*")
        .header(header::ACCEPT_ENCODING, "gzip, deflate")
        .header(header::CONNECTION, "keep-alive")
        .send();
    let response = request.await.map_err(|e| {
        if e.is_timeout() {
            Failure::Timeout
        } else {
            Failure::Connect
        }
    })?;
    let status = response.status().as_u16();
    let header_bytes = response
        .headers()
        .iter()
        .try_fold(0usize, |sum, (key, value)| {
            sum.checked_add(key.as_str().len())?
                .checked_add(value.as_bytes().len())?
                .checked_add(4)
        })
        .ok_or(Failure::Unqualified("Manifest header capacity unavailable"))?;
    if header_bytes > MAX_HEADER_BYTES {
        return Err(Failure::Unqualified("Manifest header capacity unavailable"));
    }
    let location = response
        .headers()
        .get(header::LOCATION)
        .map(|v| v.to_str().map(str::to_owned))
        .transpose()
        .map_err(|_| Failure::Unqualified("Non-UTF8 redirect location remains unqualified"))?;
    if [301, 302, 303, 307, 308].contains(&status) || status >= 400 {
        drop(response);
        drop(client);
        return Ok((status, location, Vec::new()));
    }
    let body = manifest_body(response).await?;
    drop(client);
    Ok((status, location, body))
}

fn manifest_framing(headers: &header::HeaderMap) -> Result<(), Failure> {
    let mut transfer = headers.get_all(header::TRANSFER_ENCODING).iter();
    if let Some(value) = transfer.next() {
        // Hyper removes chunk framing from body frames. Admit only that
        // single transfer coding with no competing Content-Length.
        if !value.as_bytes().eq_ignore_ascii_case(b"chunked")
            || transfer.next().is_some()
            || headers.contains_key(header::CONTENT_LENGTH)
        {
            return Err(Failure::Unqualified(
                "Manifest transfer coding remains unqualified",
            ));
        }
    }
    let mut content = headers.get_all(header::CONTENT_ENCODING).iter();
    if let Some(value) = content.next() {
        let known = [b"identity".as_slice(), b"gzip", b"x-gzip", b"deflate"]
            .into_iter()
            .any(|coding| value.as_bytes().eq_ignore_ascii_case(coding));
        if !known || content.next().is_some() {
            return Err(Failure::Unqualified(
                "Manifest content coding remains unqualified",
            ));
        }
    }
    Ok(())
}

fn manifest_body_failure(error: std::io::Error) -> Failure {
    match error.get_ref() {
        Some(cause) if cause.is::<WireLimitExceeded>() => Failure::Unqualified(MANIFEST_BODY_LIMIT),
        Some(cause) if cause.is::<ResourceLimitExceeded>() => {
            Failure::Unqualified("Manifest content decoding capacity unavailable")
        }
        // Keep the prior stream error class. The task layer does not persist a
        // guessed Python error class after a partial/invalid download.
        _ => Failure::Connect,
    }
}

async fn manifest_body(response: Response) -> Result<Vec<u8>, Failure> {
    manifest_framing(response.headers())?;
    if response
        .content_length()
        .is_some_and(|n| n > RAW_LIMIT as u64)
    {
        return Err(Failure::Unqualified(MANIFEST_BODY_LIMIT));
    }
    let mut stream = body_limited(response, RAW_LIMIT).map_err(manifest_body_failure)?;
    let collect = async {
        let mut body = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(manifest_body_failure)?;
            if body.len().saturating_add(chunk.len()) > RAW_LIMIT {
                return Err(Failure::Unqualified(MANIFEST_BODY_LIMIT));
            }
            crate::image_proxy::append_bounded(&mut body, &chunk, RAW_LIMIT);
        }
        Ok(body)
    };
    // Explicit finite capacity guard; Source timeout is per inactivity interval.
    tokio::time::timeout(Duration::from_secs(120), collect)
        .await
        .map_err(|_| Failure::Unqualified("Manifest actor lifetime capacity unavailable"))?
}

/// Stream a managed ZIP using the SAME validated HTTPS/DNS/NO_PROXY port and
/// already-admitted plugin owner. No response body Vec or detached file writer.
pub(super) async fn download_one(
    url: &Url,
    ca_bundle: Option<&Path>,
    proxy_port: Option<u16>,
    file: &mut tokio::fs::File,
    hash: bool,
) -> Result<(u16, Option<String>, Option<String>), Failure> {
    use sha2::{Digest, Sha256};
    use tokio::io::AsyncWriteExt;
    validate(url, TransportUse::Request).await?;
    super::proxy_environment::enforce(url, proxy_port)?;
    let client = client_with_timeout(Client::builder(), ca_bundle, Duration::from_secs(60))?;
    let response = client
        .get(url.clone())
        .header(header::ACCEPT, "*/*")
        .header(header::ACCEPT_ENCODING, "gzip, deflate")
        .header(header::CONNECTION, "keep-alive")
        .send()
        .await
        .map_err(|e| {
            if e.is_timeout() {
                Failure::Timeout
            } else {
                Failure::Connect
            }
        })?;
    let status = response.status().as_u16();
    let bytes = response
        .headers()
        .iter()
        .try_fold(0usize, |sum, (k, v)| {
            sum.checked_add(k.as_str().len())?
                .checked_add(v.as_bytes().len())?
                .checked_add(4)
        })
        .ok_or(Failure::Unqualified("Release header capacity unavailable"))?;
    if bytes > MAX_HEADER_BYTES {
        return Err(Failure::Unqualified("Release header capacity unavailable"));
    }
    let location = response
        .headers()
        .get(header::LOCATION)
        .map(|v| v.to_str().map(str::to_owned))
        .transpose()
        .map_err(|_| Failure::Unqualified("Non-UTF8 redirect location remains unqualified"))?;
    if [301, 302, 303, 307, 308].contains(&status) || status >= 400 {
        drop(response);
        drop(client);
        return Ok((status, location, None));
    }
    const LIMIT: usize = 200 * 1024 * 1024;
    let mut stream =
        dispatcharr_upstream_http::body_limited(response, LIMIT + 65536).map_err(|_| {
            Failure::Unqualified("Release content decoding outside bounded Requests domain")
        })?;
    let mut total = 0usize;
    let mut digest = hash.then(Sha256::new);
    let result = async {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| Failure::Connect)?;
            total = total
                .checked_add(chunk.len())
                .ok_or(Failure::ReleaseTooLarge)?;
            if total > LIMIT {
                return Err(Failure::ReleaseTooLarge);
            }
            if let Some(digest) = &mut digest {
                digest.update(&chunk);
            }
            file.write_all(&chunk).await.map_err(|_| Failure::File)?;
        }
        Ok((
            status,
            location,
            digest.map(|d| format!("{:x}", d.finalize())),
        ))
    }
    .await;
    drop(stream);
    drop(client);
    result
}

#[cfg(test)]
mod manifest_body_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const PLAIN: &[u8] = br#"{"plugins":[]}"#;
    const GZIP: &[u8] = &[
        31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 171, 86, 42, 200, 41, 77, 207, 204, 43, 86, 178, 138,
        142, 173, 5, 0, 68, 80, 108, 13, 14, 0, 0, 0,
    ];
    const ZLIB: &[u8] = &[
        120, 156, 171, 86, 42, 200, 41, 77, 207, 204, 43, 86, 178, 138, 142, 173, 5, 0, 39, 109, 5,
        49,
    ];
    const RAW_DEFLATE: &[u8] = &[
        171, 86, 42, 200, 41, 77, 207, 204, 43, 86, 178, 138, 142, 173, 5, 0,
    ];
    const GZIP_OVERSIZED: &[u8] = &[
        31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 237, 193, 129, 0, 0, 0, 0, 128, 32, 149, 253, 41, 23,
        169, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 170, 1, 1, 180, 186, 30, 1, 0, 1, 0,
    ];

    fn chunked(encoding: &str, body: &[u8]) -> Vec<u8> {
        let mut wire = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Encoding: {encoding}\r\nConnection: close\r\n\r\n"
        )
        .into_bytes();
        for chunk in body.chunks(8192) {
            wire.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            wire.extend_from_slice(chunk);
            wire.extend_from_slice(b"\r\n");
        }
        wire.extend_from_slice(b"0\r\n\r\n");
        wire
    }

    async fn read_owned_response(wire: Vec<u8>) -> Result<Vec<u8>, Failure> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut part = [0; 1024];
            while !request.ends_with(b"\r\n\r\n") {
                let read = socket.read(&mut part).await.unwrap();
                assert!(read > 0 && request.len() + read <= 4096);
                request.extend_from_slice(&part[..read]);
            }
            socket.write_all(&wire).await.unwrap();
        });
        // Exercise the real HTTP decoder with an owned response. Production
        // URL validation and TLS remain covered by their independent cases.
        let client = Client::builder()
            .no_proxy()
            .http1_only()
            .no_gzip()
            .no_deflate()
            .read_timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let response = client
            .get(format!("http://{address}/manifest.json"))
            .send()
            .await
            .unwrap();
        let result = manifest_body(response).await;
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        result
    }

    #[test]
    fn manifest_accepts_one_supported_coding_per_transport_layer() {
        use header::{CONTENT_ENCODING, CONTENT_LENGTH, TRANSFER_ENCODING};

        let mut headers = header::HeaderMap::new();
        headers.insert(TRANSFER_ENCODING, "Chunked".parse().unwrap());
        headers.insert(CONTENT_ENCODING, "GZip".parse().unwrap());
        assert!(manifest_framing(&headers).is_ok());

        headers.insert(CONTENT_LENGTH, "12".parse().unwrap());
        assert!(matches!(
            manifest_framing(&headers),
            Err(Failure::Unqualified(_))
        ));
        headers.remove(CONTENT_LENGTH);
        headers.insert(TRANSFER_ENCODING, "gzip, chunked".parse().unwrap());
        assert!(matches!(
            manifest_framing(&headers),
            Err(Failure::Unqualified(_))
        ));
        headers.remove(TRANSFER_ENCODING);
        headers.append(CONTENT_ENCODING, "deflate".parse().unwrap());
        assert!(matches!(
            manifest_framing(&headers),
            Err(Failure::Unqualified(_))
        ));
        headers.insert(CONTENT_ENCODING, "br".parse().unwrap());
        assert!(matches!(
            manifest_framing(&headers),
            Err(Failure::Unqualified(_))
        ));
    }

    #[tokio::test]
    async fn manifest_decodes_owned_chunked_identity_gzip_and_requests_deflate_variants() {
        for (encoding, bytes) in [
            ("identity", PLAIN),
            ("gZip", GZIP),
            ("x-gzip", GZIP),
            ("deflate", ZLIB),
            ("deflate", RAW_DEFLATE),
        ] {
            assert_eq!(
                read_owned_response(chunked(encoding, bytes)).await.unwrap(),
                PLAIN
            );
        }
        let mut wire = format!(
            "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            GZIP.len()
        )
        .into_bytes();
        wire.extend_from_slice(GZIP);
        assert_eq!(read_owned_response(wire).await.unwrap(), PLAIN);
    }

    #[tokio::test]
    async fn manifest_rejects_coded_and_decoded_overflow_and_partial_transport() {
        assert!(matches!(
            read_owned_response(chunked("identity", &vec![b' '; RAW_LIMIT + 1])).await,
            Err(Failure::Unqualified(MANIFEST_BODY_LIMIT))
        ));
        assert!(matches!(
            read_owned_response(chunked("gzip", GZIP_OVERSIZED)).await,
            Err(Failure::Unqualified(MANIFEST_BODY_LIMIT))
        ));
        let mut truncated = chunked("identity", PLAIN);
        truncated.truncate(truncated.len() - 5);
        assert!(matches!(
            read_owned_response(truncated).await,
            Err(Failure::Connect)
        ));
        assert!(matches!(
            read_owned_response(chunked("gzip", b"invalid compressed body")).await,
            Err(Failure::Connect)
        ));
    }
}
