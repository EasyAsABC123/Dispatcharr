//! Original Dispatcharr authors retain application/algorithm/license credit.
//! Candidate hostname/HTTPS transport for the measured Source16 domain.
//! Not registered or accepted: DNS driver cancellation/physical closure, complete
//! trust-store parity, codecs and production integration still need batch tests.
use super::{address_allowed, Failure, INACTIVITY, RAW_LIMIT};
use futures_util::StreamExt;
use reqwest::{header, redirect::Policy, Client, Url};
use std::{net::IpAddr, path::Path, time::Duration};

const MAX_DNS_ADDRESSES: usize = 64;
const MAX_CA_BYTES: usize = 1024 * 1024;
const MAX_TARGET_BYTES: usize = 8192;
const MAX_HEADER_BYTES: usize = 65536;

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
async fn validate(url: &Url) -> Result<(), Failure> {
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
    let reject = |ip: IpAddr| {
        if address_allowed(ip) {
            Ok(())
        } else {
            Err(Failure::Policy(format!(
                "URL resolves to a non-routable address ({ip}) and cannot be fetched."
            )))
        }
    };
    if let Ok(ip) = host.parse::<IpAddr>() {
        return reject(ip);
    }
    let answers = tokio::time::timeout(INACTIVITY, tokio::net::lookup_host((host, 0)))
        .await
        .map_err(|_| Failure::Timeout)?
        .map_err(|_| Failure::Unqualified("Platform DNS error rendering remains unqualified"))?;
    let mut count = 0;
    for answer in answers {
        count += 1;
        if count > MAX_DNS_ADDRESSES {
            return Err(Failure::Unqualified(
                "Manifest DNS answer capacity unavailable",
            ));
        }
        reject(answer.ip())?;
    }
    if count == 0 {
        return Err(Failure::Policy(format!(
            "Could not resolve hostname '{host}'."
        )));
    }
    Ok(())
}

fn client(ca_bundle: Option<&Path>) -> Result<Client, Failure> {
    client_with_timeout(ca_bundle, INACTIVITY)
}
fn client_with_timeout(ca_bundle: Option<&Path>, timeout: Duration) -> Result<Client, Failure> {
    let mut builder = Client::builder()
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
) -> Result<(u16, Option<String>, Vec<u8>), Failure> {
    validate(url).await?;
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
    if response.headers().contains_key(header::TRANSFER_ENCODING)
        || response
            .headers()
            .get(header::CONTENT_ENCODING)
            .is_some_and(|v| v.as_bytes() != b"identity")
    {
        return Err(Failure::Unqualified(
            "Chunked/compressed manifest transport remains unqualified",
        ));
    }
    if response
        .content_length()
        .is_some_and(|n| n > RAW_LIMIT as u64)
    {
        return Err(Failure::Unqualified("Manifest body capacity unavailable"));
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    let collect = async {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| Failure::Connect)?;
            if body.len().saturating_add(chunk.len()) > RAW_LIMIT {
                return Err(Failure::Unqualified("Manifest body capacity unavailable"));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(())
    };
    // Explicit finite capacity guard; Source timeout is per inactivity interval.
    tokio::time::timeout(Duration::from_secs(120), collect)
        .await
        .map_err(|_| Failure::Unqualified("Manifest actor lifetime capacity unavailable"))??;
    drop(stream);
    drop(client);
    Ok((status, location, body))
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
    validate(url).await?;
    super::proxy_environment::enforce(url, proxy_port)?;
    let client = client_with_timeout(ca_bundle, Duration::from_secs(60))?;
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
