//! Opt-in finite unsigned manifest adapter. Original Dispatcharr authors retain
//! application/algorithm credit. Main/default metadata remain separate.
//! The measured transport domain is literal-IP plain HTTP, uncompressed,
//! non-chunked UTF-8 JSON. Other transports/signatures are explicitly guarded.
use super::*;
use md5::{Digest, Md5};
use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    sync::OnceLock,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub(super) const DOMAIN: &str = "Opt-in measured Source25: literal-IP plain HTTP unsigned UTF8 JSON; headers/body<=65536, JSON nodes<=2048/depth<=32/work<=4MiB, existing four original request owners. HTTPS/hostname DNS/URL credentials/chunked/compressed/signed manifests and cache-failure rendering remain unqualified. No Main/default router activation or whole-request physical qualification until actual held-wire tests.";
const INACTIVITY: Duration = Duration::from_secs(15);
const HEADERS: usize = 65536;
#[derive(Clone)]
pub(super) enum Action {
    Create,
    Preview,
    Refresh(String),
}
#[derive(Debug)]
enum Failure {
    Policy(String),
    Json,
    MissingPlugins,
    Http(u16),
    Timeout,
    Connect,
    Unexpected,
    Unqualified(&'static str),
}
fn unqualified(message: &'static str) -> StoreError {
    StoreError::Unsupported(message)
}
fn source_private(address: IpAddr) -> bool {
    let address = match address {
        IpAddr::V6(v) => v.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(address),
        _ => address,
    };
    static DATA: OnceLock<Value> = OnceLock::new();
    let data = DATA.get_or_init(|| {
        serde_json::from_str(include_str!(
            "../datasets/source-plugin-public-ip-policy.json"
        ))
        .expect("pinned IP policy data")
    });
    let family = if address.is_ipv4() {
        "_IPv4Constants"
    } else {
        "_IPv6Constants"
    };
    let contains = |suffix: &str| {
        data[format!("{family}_{suffix}")]
            .as_array()
            .expect("CIDR list")
            .iter()
            .any(|v| {
                v.as_str()
                    .expect("CIDR")
                    .parse::<ipnet::IpNet>()
                    .expect("CIDR")
                    .contains(&address)
            })
    };
    contains("_private_networks") && !contains("_private_networks_exceptions")
}
fn address_allowed(address: IpAddr) -> bool {
    let address = match address {
        IpAddr::V6(v) => v.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(address),
        _ => address,
    };
    crate::image_proxy::address_allowed(address) && !source_private(address)
}
fn domain(url: &reqwest::Url) -> Result<IpAddr, Failure> {
    if url.scheme() != "http" {
        return Err(Failure::Unqualified(
            "Unsigned manifests: HTTPS and other transports remain unqualified",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Failure::Unqualified(
            "Unsigned manifests: URL credentials remain unqualified",
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| Failure::Policy("URL has no hostname.".into()))?
        .trim_matches(['[', ']']);
    host.parse::<IpAddr>().map_err(|_| {
        Failure::Unqualified("Unsigned manifests: hostname/DNS ownership remains unqualified")
    })
}
fn policy(url: &reqwest::Url) -> Result<IpAddr, Failure> {
    let ip = domain(url)?;
    if !address_allowed(ip) {
        return Err(Failure::Policy(format!(
            "URL resolves to a non-routable address ({ip}) and cannot be fetched."
        )));
    }
    Ok(ip)
}
fn unsigned(data: Value) -> Result<Value, Failure> {
    if let Some(inner) = data.get("manifest") {
        if inner.get("plugins").is_some() {
            if crate::channels::truthy(data.get("signature")) {
                return Err(Failure::Unqualified(
                    "Signed manifests/GPG remain unqualified",
                ));
            }
            return Ok(data);
        }
    }
    if data.get("plugins").is_some() {
        return Ok(json!({"manifest":data}));
    }
    Err(Failure::MissingPlugins)
}
fn decoded_manifest(raw: &[u8]) -> Result<Value, Failure> {
    let text = std::str::from_utf8(raw).map_err(|_| Failure::Json)?;
    let (value, _quote) = crate::xc_bounded_user::checked_properties(
        text,
        0,
        crate::XcUserReadLimits {
            max_raw_bytes: RAW_LIMIT,
            max_nodes: 2048,
            max_depth: 32,
            work_bytes: 4 * 1024 * 1024,
        },
    )
    .map_err(|error| match error {
        StoreError::StatusBody(503, _) => {
            Failure::Unqualified("Manifest JSON decode capacity unavailable")
        }
        _ => Failure::Json,
    })?;
    unsigned(value)
}
struct Head {
    status: u16,
    length: Option<usize>,
    location: Option<String>,
}
fn parse_headers(bytes: &[u8]) -> Result<Head, Failure> {
    if bytes.len() > HEADERS {
        return Err(Failure::Unqualified(
            "Manifest headers exceed qualified capacity",
        ));
    }
    let header = std::str::from_utf8(bytes)
        .map_err(|_| Failure::Unqualified("Non-UTF8 manifest headers remain unqualified"))?;
    if !header.is_ascii() || !header.ends_with("\r\n\r\n") {
        return Err(Failure::Unqualified(
            "Malformed manifest headers remain unqualified",
        ));
    }
    let mut lines = header.split("\r\n");
    let first = lines.next().ok_or(Failure::Connect)?;
    let mut statusline = first.splitn(3, ' ');
    let protocol = statusline.next().unwrap_or("");
    let code = statusline.next().unwrap_or("");
    if !matches!(protocol, "HTTP/1.0" | "HTTP/1.1")
        || code.len() != 3
        || !code.bytes().all(|c| c.is_ascii_digit())
    {
        return Err(Failure::Unqualified(
            "Malformed manifest status line remains unqualified",
        ));
    }
    let status = code.parse::<u16>().map_err(|_| Failure::Connect)?;
    if !(200..=599).contains(&status) {
        return Err(Failure::Unqualified(
            "Informational/invalid manifest status remains unqualified",
        ));
    }
    let mut length = None;
    let mut location = None;
    let mut count = 0;
    for line in lines.filter(|line| !line.is_empty()) {
        count += 1;
        if count > 128 {
            return Err(Failure::Unqualified(
                "Manifest header count capacity unavailable",
            ));
        }
        let (key, value) = line.split_once(':').ok_or(Failure::Unqualified(
            "Malformed manifest header remains unqualified",
        ))?;
        if key.is_empty()
            || !key
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&c))
            || value.bytes().any(|c| c < 32 && c != b'\t' || c == 127)
        {
            return Err(Failure::Unqualified(
                "Malformed manifest header remains unqualified",
            ));
        }
        let value = value.trim();
        if key.eq_ignore_ascii_case("content-length") {
            if length.is_some() || value.is_empty() || !value.bytes().all(|c| c.is_ascii_digit()) {
                return Err(Failure::Unqualified(
                    "Duplicate/malformed Content-Length remains unqualified",
                ));
            }
            length = Some(
                value
                    .parse::<usize>()
                    .map_err(|_| Failure::Unqualified("Manifest length capacity unavailable"))?,
            );
        }
        if key.eq_ignore_ascii_case("location") {
            if location.is_some() {
                return Err(Failure::Unqualified(
                    "Duplicate Location remains unqualified",
                ));
            }
            location = Some(value.to_owned());
        }
        if key.eq_ignore_ascii_case("transfer-encoding")
            || key.eq_ignore_ascii_case("content-encoding")
                && !value.eq_ignore_ascii_case("identity")
        {
            return Err(Failure::Unqualified(
                "Chunked/compressed manifest transport remains unqualified",
            ));
        }
    }
    Ok(Head {
        status,
        length,
        location,
    })
}
// There is no detached HTTP connection driver or client pool. The supervised
// request actor owns this stream through every read and explicit shutdown/drop.
// Actual FD/late-reply settlement still requires the separate physical fixture.
async fn read_bytes(
    stream: &mut tokio::net::TcpStream,
    bytes: &mut [u8],
    body: bool,
) -> Result<usize, Failure> {
    tokio::time::timeout(INACTIVITY, stream.read(bytes))
        .await
        .map_err(|_| {
            if body {
                Failure::Connect
            } else {
                Failure::Timeout
            }
        })?
        .map_err(|_| Failure::Connect)
}
async fn one_get(url: &reqwest::Url) -> Result<(u16, Option<String>, Vec<u8>), Failure> {
    let ip = policy(url)?;
    let socket = SocketAddr::new(ip, url.port_or_known_default().unwrap_or(80));
    let mut stream = tokio::time::timeout(INACTIVITY, tokio::net::TcpStream::connect(socket))
        .await
        .map_err(|_| Failure::Timeout)?
        .map_err(|_| Failure::Connect)?;
    let result=async {
        let host=url.host_str().ok_or(Failure::Unexpected)?.trim_matches(['[',']']);let host=if ip.is_ipv6(){format!("[{host}]")}else{host.to_owned()};let host=if let Some(port)=url.port(){format!("{host}:{port}")}else{host};
        let mut path=url.path().to_owned();if let Some(query)=url.query(){path.push('?');path.push_str(query);}
        if path.len()>8192{return Err(Failure::Unqualified("Manifest request target exceeds qualified capacity"));}
        let request=format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: python-requests/2.34.2\r\nAccept-Encoding: gzip, deflate\r\nAccept: */*\r\nConnection: keep-alive\r\n\r\n");
        tokio::time::timeout(INACTIVITY,stream.write_all(request.as_bytes())).await.map_err(|_|Failure::Timeout)?.map_err(|_|Failure::Connect)?;
        let mut bytes=Vec::new();let end=loop {
            if let Some(pos)=bytes.windows(4).position(|v|v==b"\r\n\r\n"){break pos+4;}
            if bytes.len()>=HEADERS{return Err(Failure::Unqualified("Manifest headers exceed qualified capacity"));}
            let mut chunk=[0;8192];let n=read_bytes(&mut stream,&mut chunk,false).await?;if n==0{return Err(Failure::Connect);}
            if bytes.len().saturating_add(n)>HEADERS+RAW_LIMIT{return Err(Failure::Unqualified("Manifest header/body capacity unavailable"));}
            bytes.extend_from_slice(&chunk[..n]);
        };
        if end>HEADERS{return Err(Failure::Unqualified("Manifest headers exceed qualified capacity"));}
        let Head{status,length,location}=parse_headers(&bytes[..end])?;
        // Source closes intermediate/error responses before reading their body.
        if [301,302,303,307,308].contains(&status)||status>=400{return Ok((status,location,Vec::new()));}
        if length.is_some_and(|n|n>RAW_LIMIT){return Err(Failure::Unqualified("Manifest body capacity unavailable"));}
        let mut body=bytes[end..].to_vec();if body.len()>RAW_LIMIT{return Err(Failure::Unqualified("Manifest body capacity unavailable"));}
        loop {
            if let Some(length)=length {if body.len()>=length {body.truncate(length);break;}}
            let mut chunk=[0;8192];let n=read_bytes(&mut stream,&mut chunk,true).await?;
            if n==0 {if length.is_some_and(|n|body.len()!=n){return Err(Failure::Unexpected);}break;}
            if body.len().saturating_add(n)>RAW_LIMIT{return Err(Failure::Unqualified("Manifest body capacity unavailable"));}
            crate::image_proxy::append_bounded(&mut body,&chunk[..n],RAW_LIMIT);
        }
        Ok((status,location,body))
    }.await;
    let shutdown = tokio::time::timeout(INACTIVITY, stream.shutdown()).await;
    drop(stream);
    if shutdown.is_err() && result.is_ok() {
        return Err(Failure::Connect);
    }
    result
}
async fn fetch(raw: &str) -> Result<Value, Failure> {
    if [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ]
    .iter()
    .any(|key| std::env::var(key).is_ok_and(|value| !value.is_empty()))
    {
        return Err(Failure::Unqualified(
            "Proxy-configured manifest transports remain unqualified",
        ));
    }
    let mut current =
        reqwest::Url::parse(raw).map_err(|_| Failure::Policy("URL has no hostname.".into()))?;
    for _ in 0..=5 {
        let (status, location, body) = one_get(&current).await?;
        if [301, 302, 303, 307, 308].contains(&status) {
            let location = location.filter(|v| !v.is_empty()).ok_or_else(|| {
                Failure::Policy("Redirect response missing Location header.".into())
            })?;
            current = current.join(&location).map_err(|_| Failure::Json)?;
            continue;
        }
        if status >= 400 {
            return Err(Failure::Http(status));
        }
        return decoded_manifest(&body);
    }
    Err(Failure::Policy("Exceeded maximum of 5 redirects.".into()))
}
fn friendly(error: &Failure) -> String {
    match error {
    Failure::Policy(value)=>value.clone(),Failure::MissingPlugins=>"Manifest JSON missing 'manifest.plugins' list".into(),Failure::Json=>"The URL did not return valid JSON. Make sure it points directly to a manifest .json file.".into(),Failure::Timeout=>"The request timed out. Check the URL and try again.".into(),Failure::Connect=>"Could not connect to the server. Check the URL and your network connection.".into(),Failure::Http(404)=>"Manifest not found (404). Check that the URL points to a valid manifest file.".into(),Failure::Http(403)=>"Access denied (403). The server refused the request.".into(),Failure::Http(code)=>format!("The server returned an error ({code}). Check the URL and try again."),_=>"An unexpected error occurred. Check the URL and try again.".into()}
}
fn inner(data: &Value) -> &Value {
    data.get("manifest").unwrap_or(data)
}
fn raw_text(value: Option<&Value>) -> Result<String, StoreError> {
    match value {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(v)) => Ok(v.clone()),
        _ => Err(unqualified(
            "Non-string manifest URL values remain unqualified",
        )),
    }
}
fn text(value: Option<&Value>) -> Result<String, StoreError> {
    match value {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(v)) => Ok(v.trim().to_owned()),
        _ => Err(unqualified(
            "Non-string manifest names/URLs remain unqualified",
        )),
    }
}
fn name_error(data: &Value) -> Result<Option<String>, StoreError> {
    let name = text(inner(data).get("registry_name"))?;
    Ok(if name.is_empty() {
        Some(
            "Manifest is missing a 'registry_name'. The repo maintainer must set this field."
                .into(),
        )
    } else if official(&name) {
        Some(format!("The registry name '{name}' is not allowed because it may be confused with an official repo."))
    } else {
        None
    })
}
fn official(name: &str) -> bool {
    let value = name.trim().to_lowercase();
    [
        "official",
        "official repo",
        "dispatcharr plugins",
        "dispatcharr repo",
        "dispatcharr official",
    ]
    .iter()
    .any(|part| value.contains(part))
}
fn ordered_repo(row: &Value) -> Result<Value, StoreError> {
    let source = serialized(row)?;
    let mut out = Map::new();
    for name in [
        "id",
        "name",
        "url",
        "is_official",
        "enabled",
        "public_key",
        "signature_verified",
        "registry_url",
        "plugin_count",
        "last_fetched",
        "last_fetch_status",
        "created_at",
        "updated_at",
    ] {
        out.insert(
            name.into(),
            source.get(name).ok_or_else(framework_shape)?.clone(),
        );
    }
    Ok(Value::Object(out))
}
const INSERT:&str="INSERT INTO plugins_pluginrepo(name,url,is_official,enabled,cached_manifest,public_key,signature_verified,last_fetched,last_fetch_status,created_at,updated_at) VALUES('',$1,false,$2,'{}'::jsonb,$3,NULL,NULL,'',$4,$5) RETURNING id";
const SAVE:&str="UPDATE plugins_pluginrepo AS r SET name=$2,cached_manifest=$3,signature_verified=NULL,last_fetched=$4,last_fetch_status='200',updated_at=$5 WHERE id=$1 AND octet_length((to_jsonb(r)||jsonb_build_object('name',$2,'cached_manifest',$3,'signature_verified',NULL,'last_fetched',$4,'last_fetch_status','200','updated_at',$5))::text)<=65536 RETURNING octet_length(to_jsonb(r)::text) AS bytes,CASE WHEN octet_length(to_jsonb(r)::text)<=65536 THEN to_jsonb(r)::text END AS raw";
async fn save(
    connection: &mut sqlx::PgConnection,
    id: i64,
    data: &Value,
) -> Result<Value, StoreError> {
    let name = text(inner(data).get("registry_name"))?;
    let fetched = Utc::now();
    let encoded = serde_json::to_string(data).map_err(|_| capacity())?;
    if encoded.len() > RAW_LIMIT {
        return Err(capacity());
    }
    let updated = Utc::now();
    payload(
        sqlx::query(SAVE)
            .bind(id)
            .bind(name)
            .bind(sqlx::types::Json(data))
            .bind(fetched)
            .bind(updated)
            .fetch_optional(&mut *connection)
            .await?
            .ok_or_else(capacity)?,
    )
}
async fn refresh_children(
    auth: &AuthService,
    connection: &mut sqlx::PgConnection,
    id: i64,
    data: &Value,
) -> Result<(), StoreError> {
    let manifest = inner(data);
    let plugins = manifest
        .get("plugins")
        .and_then(Value::as_array)
        .ok_or_else(|| unqualified("Non-list plugin collections remain unqualified"))?;
    if plugins.len() > 256 {
        return Err(capacity());
    }
    let mut entries = BTreeMap::new();
    for plugin in plugins {
        if let Some(slug) = plugin
            .get("slug")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
        {
            entries.insert(
                slug.to_owned(),
                crate::channels::truthy(plugin.get("deprecated")),
            );
        }
    }
    let slugs = entries.keys().cloned().collect::<Vec<_>>();
    sqlx::query("UPDATE plugins_pluginconfig SET source_repo_id=NULL,slug='',deprecated=false WHERE source_repo_id=$1 AND NOT(slug=ANY($2))").bind(id).bind(&slugs).execute(&mut *connection).await?;
    let rows=sqlx::query("SELECT id,slug,deprecated FROM plugins_pluginconfig WHERE source_repo_id=$1 AND slug=ANY($2) ORDER BY id LIMIT 257").bind(id).bind(&slugs).fetch_all(&mut *connection).await?;
    if rows.len() > 256 {
        return Err(capacity());
    }
    for row in rows {
        let slug: String = row.try_get("slug")?;
        let old: bool = row.try_get("deprecated")?;
        let new = *entries.get(&slug).ok_or_else(framework_shape)?;
        if old != new {
            sqlx::query("UPDATE plugins_pluginconfig SET deprecated=$2,updated_at=$3 WHERE id=$1")
                .bind(row.try_get::<i64, _>("id")?)
                .bind(new)
                .bind(Utc::now())
                .execute(&mut *connection)
                .await?;
        }
    }
    let root = raw_text(manifest.get("root_url"))?
        .trim_end_matches('/')
        .to_owned();
    let metadata = raw_text(manifest.get("metadata_base_url"))?
        .trim_end_matches('/')
        .to_owned();
    let base = if metadata.is_empty() { root } else { metadata };
    let mut keys = Vec::new();
    for plugin in plugins {
        let url = raw_text(plugin.get("manifest_url"))?;
        if url.is_empty() {
            continue;
        }
        let url = if !base.is_empty() && !url.starts_with("http://") && !url.starts_with("https://")
        {
            format!("{base}/{url}")
        } else {
            url
        };
        keys.push(format!(
            ":1:plugin_detail:{id}:{:x}",
            Md5::digest(url.as_bytes())
        ));
    }
    if !keys.is_empty() {
        if let Some(cache) = auth.notification_cache.as_ref() {
            let mut cmd = redis::cmd("DEL");
            cmd.arg(keys);
            let _: i64 = cache.command(cmd).await?;
        }
    }
    Ok(())
}
pub(super) async fn execute(
    auth: &AuthService,
    connection: &mut sqlx::PgConnection,
    request: Request,
    action: Action,
) -> Result<Response, StoreError> {
    if auth.notification_cache.is_none() {
        return Err(unqualified(
            "Unsigned manifest domain requires the configured cache adapter",
        ));
    }
    let target = if let Action::Refresh(raw) = &action {
        let id = raw.parse::<i64>().map_err(|_| missing())?;
        let row = payload(
            sqlx::query(TARGET_SQL)
                .bind(id)
                .fetch_optional(&mut *connection)
                .await?
                .ok_or_else(missing)?,
        )?;
        Some((id, row))
    } else {
        None
    };
    let fields = if matches!(action, Action::Refresh(_)) {
        Map::new()
    } else {
        let content_type = request
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !content_type.starts_with("application/json") {
            return Err(StoreError::StatusBody(
                415,
                json!({"detail":format!("Unsupported media type \"{content_type}\" in request.")}),
            ));
        }
        let raw = to_bytes(request.into_body(), RAW_LIMIT)
            .await
            .map_err(|_| capacity())?;
        parsed(&raw)?.as_object().cloned().ok_or_else(|| {
            unqualified("Non-object unsigned manifest requests remain unqualified")
        })?
    };
    let url = if let Some((_, row)) = &target {
        row.get("url")
            .and_then(Value::as_str)
            .ok_or_else(framework_shape)?
            .to_owned()
    } else if matches!(action, Action::Preview) {
        let value = text(fields.get("url"))?;
        if value.is_empty() {
            return Ok((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"url is required"})),
            )
                .into_response());
        }
        value
    } else {
        character(
            "url",
            fields
                .get("url")
                .ok_or_else(|| field("url", "This field is required."))?,
            Some(200),
            false,
        )?
    };
    // Refuse explicitly unqualified transport domains BEFORE creating a row.
    let parsed_url = reqwest::Url::parse(&url).map_err(|_| field("url", "Enter a valid URL."))?;
    domain(&parsed_url).map_err(|e| match e {
        Failure::Unqualified(v) => unqualified(v),
        _ => field("url", "Enter a valid URL."),
    })?;
    let mut created = None;
    if matches!(action, Action::Create) {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM plugins_pluginrepo WHERE url=$1)")
                .bind(&url)
                .fetch_one(&mut *connection)
                .await?;
        if exists {
            return Err(field("url", "plugin repo with this url already exists."));
        }
        let enabled = fields
            .get("enabled")
            .map(|v| boolean("enabled", v))
            .transpose()?
            .unwrap_or(true);
        let key = fields
            .get("public_key")
            .map(|v| character("public_key", v, None, true))
            .transpose()?
            .unwrap_or_default();
        created = Some(
            sqlx::query_scalar::<_, i64>(INSERT)
                .bind(&url)
                .bind(enabled)
                .bind(key)
                .bind(Utc::now())
                .bind(Utc::now())
                .fetch_one(&mut *connection)
                .await?,
        );
    }
    let data = match fetch(&url).await {
        Ok(value) => value,
        Err(error) => {
            if let Some(id) = created {
                sqlx::query("DELETE FROM plugins_pluginrepo WHERE id=$1")
                    .bind(id)
                    .execute(&mut *connection)
                    .await?;
            }
            if let Failure::Unqualified(message) = error {
                return Err(unqualified(message));
            }
            if matches!(action, Action::Preview) {
                return Ok(Json(json!({"valid":false,"errors":[friendly(&error)]})).into_response());
            }
            let status = if created.is_some() {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::BAD_GATEWAY
            };
            return Ok((
                status,
                Json(json!({"error":"Failed to fetch manifest. Check the URL and try again."})),
            )
                .into_response());
        }
    };
    if matches!(action, Action::Preview) {
        let name = text(inner(&data).get("registry_name"))?;
        let registry = text(inner(&data).get("registry_url"))?;
        let count = inner(&data)
            .get("plugins")
            .and_then(Value::as_array)
            .ok_or_else(|| unqualified("Non-list plugin collections remain unqualified"))?
            .len();
        let mut errors = Vec::new();
        if name.is_empty() {
            errors.push("Manifest is missing a 'registry_name'.".to_owned());
        } else if official(&name) {
            errors.push(format!("The registry name '{name}' is not allowed because it may be confused with an official repo."));
        }
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM plugins_pluginrepo WHERE url=$1)")
                .bind(&url)
                .fetch_one(&mut *connection)
                .await?;
        if exists {
            errors.push("This manifest URL has already been added.".to_owned());
        }
        return Ok(Json(json!({"valid":errors.is_empty(),"registry_name":name,"registry_url":registry,"signature_verified":Value::Null,"plugin_count":count,"errors":errors})).into_response());
    }
    if let Some(error) = name_error(&data)? {
        if let Some(id) = created {
            sqlx::query("DELETE FROM plugins_pluginrepo WHERE id=$1")
                .bind(id)
                .execute(&mut *connection)
                .await?;
        }
        return Ok((StatusCode::BAD_REQUEST, Json(json!({"error":error}))).into_response());
    }
    let id = created
        .or_else(|| target.as_ref().map(|v| v.0))
        .ok_or_else(framework_shape)?;
    let saved = save(connection, id, &data).await?;
    if created.is_none() {
        refresh_children(auth, connection, id, &data).await?;
    }
    Ok((
        if created.is_some() {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(ordered_repo(&saved)?),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn python_private_policy_is_not_is_global() {
        for ip in ["11.253.250.2", "100.64.0.1", "192.0.0.9", "192.0.0.10"] {
            assert!(address_allowed(ip.parse().unwrap()), "{ip}");
        }
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.169.254",
            "192.0.2.1",
            "224.0.0.1",
            "240.0.0.1",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
        ] {
            assert!(!address_allowed(ip.parse().unwrap()), "{ip}");
        }
    }
    #[test]
    fn private_target_refuses_before_connection() {
        assert!(matches!(
            policy(&reqwest::Url::parse("http://127.0.0.1:18080/secret").unwrap()),
            Err(Failure::Policy(_))
        ));
    }
    #[test]
    fn unqualified_transport_domains_are_explicit() {
        for url in [
            "https://11.253.250.2/a",
            "http://example.org/a",
            "http://user:pass@11.253.250.2/a",
        ] {
            assert!(matches!(
                domain(&reqwest::Url::parse(url).unwrap()),
                Err(Failure::Unqualified(_))
            ));
        }
    }
    #[test]
    fn bare_manifest_is_wrapped_without_reordering_payload() {
        let value = decoded_manifest(br#"{"registry_name":"Owned Bare","plugins":[]}"#).unwrap();
        assert_eq!(
            value,
            json!({"manifest":{"registry_name":"Owned Bare","plugins":[]}})
        );
    }
    #[test]
    fn invalid_json_and_missing_plugins_are_distinct() {
        assert!(matches!(decoded_manifest(b"not JSON"), Err(Failure::Json)));
        assert!(matches!(
            decoded_manifest(br#"{"registry_name":"Owned"}"#),
            Err(Failure::MissingPlugins)
        ));
    }
    #[test]
    fn signatures_never_invoke_or_fabricate_gpg() {
        assert!(matches!(
            unsigned(json!({"manifest":{"plugins":[]},"signature":"signature"})),
            Err(Failure::Unqualified(_))
        ));
        assert!(unsigned(json!({"manifest":{"plugins":[]},"signature":null})).is_ok());
    }
    #[test]
    fn source_official_name_substring_includes_unofficial() {
        assert!(official("Dispatcharr Official Untrusted"));
        assert!(official("unofficial"));
        assert!(!official("Owned Refreshed"));
    }
    #[test]
    fn missing_registry_error_is_save_stage_not_fetch_shape_error() {
        let data = decoded_manifest(br#"{"manifest":{"plugins":[]}}"#).unwrap();
        assert!(name_error(&data)
            .unwrap()
            .unwrap()
            .contains("repo maintainer"));
    }
    #[test]
    fn source_string_false_deprecated_is_truthy() {
        assert!(crate::channels::truthy(Some(&json!("false"))));
        assert!(!crate::channels::truthy(Some(&json!(false))));
    }
    #[test]
    fn metadata_url_whitespace_is_not_silently_normalized() {
        assert_eq!(
            raw_text(Some(&json!(" metadata/ "))).unwrap(),
            " metadata/ "
        );
        assert_eq!(text(Some(&json!(" Owned "))).unwrap(), "Owned");
    }

    #[test]
    fn actual_http10_header_shape_parses() {
        let h = parse_headers(b"HTTP/1.0 200 OK\r\nServer: BaseHTTP\r\nContent-Length: 27\r\n\r\n")
            .unwrap();
        assert_eq!(h.status, 200);
        assert_eq!(h.length, Some(27));
    }
    #[test]
    fn malformed_status_is_not_interpreted_as_success() {
        for wire in [
            b"GARBAGE 200 OK\r\n\r\n".as_slice(),
            b"HTTP/2 200 OK\r\n\r\n",
            b"HTTP/1.1 +200 OK\r\n\r\n",
        ] {
            assert!(parse_headers(wire).is_err());
        }
    }
    #[test]
    fn duplicate_and_ambiguous_lengths_are_guarded() {
        for wire in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\n".as_slice(),
            b"HTTP/1.1 200 OK\r\nContent-Length: +2\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 2, 2\r\n\r\n",
        ] {
            assert!(parse_headers(wire).is_err());
        }
    }
    #[test]
    fn transfer_codecs_guard_before_json_interpretation() {
        for wire in [
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".as_slice(),
            b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n\r\n",
        ] {
            assert!(parse_headers(wire).is_err());
        }
    }
    #[test]
    fn oversized_and_folded_header_are_guarded() {
        assert!(parse_headers(&vec![b'x'; HEADERS + 1]).is_err());
        assert!(parse_headers(b"HTTP/1.1 200 OK\r\n bad: value\r\n\r\n").is_err());
    }
    #[test]
    fn decoded_body_capacity_is_distinct_from_bad_json() {
        assert!(matches!(
            decoded_manifest(&vec![b' '; RAW_LIMIT + 1]),
            Err(Failure::Unqualified(_))
        ));
    }
}

#[cfg(test)]
#[path = "plugin_repository_fetch_peers.rs"]
mod peers;
