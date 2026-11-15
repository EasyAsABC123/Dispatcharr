//! Pinned Original recording poster fallback over the existing Requests factory.
//! Original Dispatcharr/Requests/IANA authors retain all algorithm/data credit.
//! No ad-hoc reqwest client, additional PG pool or writer semaphore.
use super::*;
use dispatcharr_upstream_http::Attempt;
use futures_util::StreamExt;
use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio::sync::OwnedSemaphorePermit;
const BODY: usize = 1_048_576;
const URL: usize = 65_536;
pub(super) struct Runtime {
    auth: crate::AuthService,
    tmdb: Option<String>,
    omdb: Option<String>,
    validation: Mutex<BTreeMap<String, (bool, Instant)>>,
}
pub(super) struct Plan {
    pub runtime: Arc<Runtime>,
    pub properties: Value,
    pub program: Value,
    pub matched_program: bool,
    pub epg_props: Value,
    pub title: Option<String>,
    pub channel_name: String,
    pub channel_logo: Option<i64>,
}
impl Runtime {
    pub fn new(auth: crate::AuthService) -> Result<Self, Error> {
        let key = |name| -> Result<Option<String>, Error> {
            let value = std::env::var(name).ok().filter(|v| !v.is_empty());
            if value.as_ref().is_some_and(|v| v.len() > 4096) {
                return Err(Error::Unsupported("artwork API key capacity"));
            }
            Ok(value)
        };
        Ok(Self {
            auth,
            tmdb: key("TMDB_API_KEY")?,
            omdb: key("OMDB_API_KEY")?,
            validation: Mutex::new(BTreeMap::new()),
        })
    }
    pub async fn admit(&self) -> Result<Arc<OwnedSemaphorePermit>, Error> {
        // Reserve the SAME original writer before a single pool checkout. The
        // queue shell bounds waiting task futures; no extra admission pool.
        self.auth
            .user_agent_workers
            .clone()
            .acquire_owned()
            .await
            .map(Arc::new)
            .map_err(|_| Error::Unsupported("artwork writer admission stopped"))
    }
    async fn json(&self, url: String) -> Result<Option<Value>, Error> {
        if url.len() > URL {
            return Err(Error::Unsupported("artwork request URL capacity"));
        }
        let attempt = Attempt::new_requests(Duration::from_secs(5), Duration::from_secs(5))
            .map_err(|_| Error::Unsupported("artwork HTTP factory"))?;
        let request = match attempt.request(&url, "", &[]) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        let response = match request.send().await {
            Ok(v) => v,
            Err(_) => {
                if attempt.cookie_budget_exceeded() {
                    return Err(Error::Unsupported("artwork redirect cookie capacity"));
                }
                return Ok(None);
            }
        };
        let status = response.status().as_u16();
        if response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                v.to_ascii_lowercase().split(';').skip(1).any(|p| {
                    p.trim()
                        .strip_prefix("charset=")
                        .is_some_and(|c| !matches!(c.trim_matches('"'), "utf-8" | "utf8"))
                })
            })
        {
            return Err(Error::Unsupported("artwork provider JSON charset"));
        }
        if attempt.cookie_budget_exceeded() {
            return Err(Error::Unsupported("artwork redirect cookie capacity"));
        }
        if response
            .headers()
            .get_all(reqwest::header::CONTENT_ENCODING)
            .iter()
            .any(|header| {
                header.to_str().ok().is_none_or(|text| {
                    text.split(',').any(|coding| {
                        !matches!(
                            coding.trim().to_ascii_lowercase().as_str(),
                            "" | "identity" | "gzip" | "x-gzip" | "deflate"
                        )
                    })
                })
            })
        {
            return Err(Error::Unsupported("artwork response content coding"));
        }

        let mut stream = match dispatcharr_upstream_http::body_limited(response, BODY) {
            Ok(v) => v,
            Err(error) => {
                return if response_capacity(&error) {
                    Err(Error::Unsupported("artwork provider response capacity"))
                } else {
                    Ok(None)
                }
            }
        };
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(v) => v,
                Err(error) => {
                    if response_capacity(&error) {
                        return Err(Error::Unsupported("artwork provider wire capacity"));
                    }
                    return Ok(None);
                }
            };
            if bytes.len().saturating_add(chunk.len()) > BODY {
                return Err(Error::Unsupported("artwork provider JSON capacity"));
            }
            bytes.extend_from_slice(&chunk);
        }
        if status >= 400 {
            return Ok(None);
        }
        decoded_json(&bytes)
    }
    async fn valid(&self, url: &str) -> Result<bool, Error> {
        if url.is_empty() {
            return Ok(false);
        }
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Ok(true);
        }
        if url.len() > URL {
            return Err(Error::Unsupported("artwork validation URL capacity"));
        }
        let now = Instant::now();
        {
            let cache = self.validation.lock().map_err(|_| Error::Supervisor)?;
            if let Some((value, at)) = cache.get(url) {
                if now.duration_since(*at) < Duration::from_secs(300) {
                    return Ok(*value);
                }
            }
        }
        {
            let mut cache = self.validation.lock().map_err(|_| Error::Supervisor)?;
            cache.retain(|_, (_, at)| now.duration_since(*at) < Duration::from_secs(300));
            if cache.len() >= 512 && !cache.contains_key(url) {
                return Err(Error::Unsupported("artwork validation cache capacity"));
            }
        }
        let attempt = Attempt::new_requests(Duration::from_secs(4), Duration::from_secs(4))
            .map_err(|_| Error::Unsupported("artwork validation HTTP factory"))?;
        let request = attempt.request_method(reqwest::Method::HEAD, url, "", &[]);
        let response = match request {
            Ok(v) => v.send().await.ok(),
            Err(_) => None,
        };
        let result = if let Some(response) = response {
            if !header_budget(&response) {
                return Err(Error::Unsupported("artwork HEAD header capacity"));
            }
            if response.status().as_u16() == 405 {
                drop(response);
                // requests.get(stream=True) immediately closes the response;
                // use an independent Source-equivalent Attempt, not HEAD cookies.
                let fallback =
                    Attempt::new_requests(Duration::from_secs(4), Duration::from_secs(4))
                        .map_err(|_| Error::Unsupported("artwork ranged GET factory"))?;
                match fallback.request(url, "", &[("Range".into(), "bytes=0-0".into())]) {
                    Ok(request) => match request.send().await {
                        Ok(response) => {
                            if !header_budget(&response) {
                                return Err(Error::Unsupported(
                                    "artwork ranged GET header capacity",
                                ));
                            }
                            if fallback.cookie_budget_exceeded() {
                                return Err(Error::Unsupported(
                                    "artwork ranged GET cookie capacity",
                                ));
                            }
                            let ok = response.status().as_u16() < 400;
                            drop(response);
                            ok
                        }
                        Err(_) => {
                            if fallback.cookie_budget_exceeded() {
                                return Err(Error::Unsupported(
                                    "artwork ranged GET cookie capacity",
                                ));
                            }
                            false
                        }
                    },
                    Err(_) => false,
                }
            } else {
                response.status().as_u16() < 400
            }
        } else {
            false
        };
        if attempt.cookie_budget_exceeded() {
            return Err(Error::Unsupported("artwork validation cookie capacity"));
        }
        let mut cache = self.validation.lock().map_err(|_| Error::Supervisor)?;
        insert_validation(&mut cache, url, result, now)?;
        Ok(result)
    }
    async fn external(&self, title: &str, epg: &Value) -> Result<Option<String>, Error> {
        if epg
            .get("date")
            .filter(|v| truth(v))
            .is_some_and(|v| matches!(v, Value::Array(_) | Value::Object(_)))
        {
            return Err(Error::Unsupported("artwork EPG year type"));
        }
        let year = epg
            .get("date")
            .filter(|v| truth(v))
            .map(python_text)
            .filter(|v| v.chars().count() >= 4)
            .map(|v| v.chars().take(4).collect::<String>());
        let imdb = epg.get("imdb.com_id").filter(|v| truth(v));
        let imdb = match imdb {
            Some(Value::String(v)) => Some(v.as_str()),
            Some(_) => return Err(Error::Unsupported("artwork IMDb argument type")),
            None => None,
        };
        if let Some(key) = &self.tmdb {
            if let Some(imdb) = imdb {
                if let Some(data)=self.json(format!("https://api.themoviedb.org/3/find/{}?api_key={key}&external_source=imdb_id",quote(imdb))).await?{
                    for name in ["movie_results","tv_results","tv_episode_results","tv_season_results"]{
                        for item in array(&data,name)?{if let Some(path)=selected_text(item.get("poster_path"))?{return Ok(Some(format!("https://image.tmdb.org/t/p/w780{path}")))}}
                    }
                }
            }
            let suffix = year
                .as_ref()
                .map(|v| format!("&year={v}"))
                .unwrap_or_default();
            if let Some(data) = self
                .json(format!(
                    "https://api.themoviedb.org/3/search/multi?api_key={key}&query={}{}",
                    quote(title),
                    suffix
                ))
                .await?
            {
                let values = array(&data, "results")?;
                let mut ranked = Vec::new();
                let mut invalid = false;
                for value in values {
                    let popularity = value
                        .get("popularity")
                        .filter(|v| truth(v))
                        .map(number)
                        .unwrap_or(Some(0.0));
                    match popularity {
                        Some(v) if v.is_finite() => ranked.push((v, value)),
                        Some(_) => return Err(Error::Unsupported("artwork popularity nonfinite")),
                        None => {
                            invalid = true;
                            break;
                        }
                    }
                }
                if !invalid {
                    ranked
                        .sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                    for (_, value) in ranked {
                        if let Some(path) = selected_text(value.get("poster_path"))? {
                            return Ok(Some(format!("https://image.tmdb.org/t/p/w780{path}")));
                        }
                    }
                }
            }
        }
        if let Some(key) = &self.omdb {
            let args = if let Some(imdb) = imdb {
                format!("i={}", quote(imdb))
            } else {
                format!(
                    "t={}{}",
                    quote(title),
                    year.as_ref().map(|v| format!("&y={v}")).unwrap_or_default()
                )
            };
            if let Some(data) = self
                .json(format!("https://www.omdbapi.com/?apikey={key}&{args}"))
                .await?
            {
                if let Some(p) = selected_text(data.get("Poster"))? {
                    if p != "N/A" {
                        return Ok(Some(p));
                    }
                }
            }
        }
        if let Some(data) = self
            .json(format!(
                "https://api.tvmaze.com/singlesearch/shows?q={}",
                quote(title)
            ))
            .await?
        {
            let image = data.get("image").filter(|v| truth(v));
            if let Some(image) = image {
                if !image.is_object() {
                    return Err(Error::Unsupported("artwork TVMaze image shape"));
                }
                if let Some(p) = selected_text(
                    image
                        .get("original")
                        .filter(|v| truth(v))
                        .or_else(|| image.get("medium")),
                )? {
                    return Ok(Some(p));
                }
            }
        }
        for media in ["movie", "tvShow"] {
            if let Some(data) = self
                .json(format!(
                    "https://itunes.apple.com/search?term={}&media={media}&limit=1",
                    quote(title)
                ))
                .await?
            {
                if let Some(first) = array(&data, "results")?.first() {
                    if let Some(url) = selected_text(first.get("artworkUrl100"))? {
                        return Ok(Some(url.replace("100x100", "600x600")));
                    }
                }
            }
        }
        Ok(None)
    }
}
fn header_budget(response: &reqwest::Response) -> bool {
    response
        .headers()
        .iter()
        .map(|(name, value)| name.as_str().len() + value.as_bytes().len())
        .sum::<usize>()
        <= 65536
}
fn response_capacity(error: &std::io::Error) -> bool {
    error.get_ref().is_some_and(|cause| {
        cause.is::<dispatcharr_upstream_http::WireLimitExceeded>()
            || cause.is::<dispatcharr_upstream_http::ResourceLimitExceeded>()
    })
}

fn insert_validation(
    cache: &mut BTreeMap<String, (bool, Instant)>,
    url: &str,
    result: bool,
    at: Instant,
) -> Result<(), Error> {
    let now = Instant::now();
    cache.retain(|_, (_, at)| now.duration_since(*at) < Duration::from_secs(300));
    if cache.len() >= 512 && !cache.contains_key(url) {
        return Err(Error::Unsupported("artwork validation cache capacity"));
    }
    cache.insert(url.into(), (result, at));
    Ok(())
}

fn quote(value: &str) -> String {
    let mut out = String::new();
    for b in value.as_bytes() {
        if b.is_ascii_alphanumeric() || b"_.-~/".contains(b) {
            out.push(char::from(*b))
        } else {
            out.push_str(&format!("%{b:02X}"))
        }
    }
    out
}
fn python_text(v: &Value) -> String {
    match v {
        Value::String(v) => v.clone(),
        Value::Bool(true) => "True".into(),
        Value::Bool(false) => "False".into(),
        Value::Null => "None".into(),
        _ => v.to_string(),
    }
}
fn number(v: &Value) -> Option<f64> {
    match v {
        Value::String(v) => v.trim().parse().ok(),
        Value::Bool(v) => Some(u8::from(*v).into()),
        Value::Number(v) => v.as_f64(),
        _ => None,
    }
}
fn selected_text(value: Option<&Value>) -> Result<Option<String>, Error> {
    match value.filter(|v| truth(v)) {
        None => Ok(None),
        Some(Value::String(v)) if v.len() <= URL => Ok(Some(v.clone())),
        Some(_) => Err(Error::Unsupported(
            "artwork provider selected URL type/capacity",
        )),
    }
}
fn array<'a>(data: &'a Value, name: &str) -> Result<&'a [Value], Error> {
    if data.is_null() || !truth(data) {
        return Ok(&[]);
    }
    if !data.is_object() {
        return Err(Error::Unsupported("artwork provider document shape"));
    }
    match data.get(name).filter(|v| truth(v)) {
        None => Ok(&[]),
        Some(Value::Array(v)) if v.len() <= 16384 => Ok(v),
        Some(_) => Err(Error::Unsupported(
            "artwork provider result array shape/capacity",
        )),
    }
}
fn best_image(epg: &Value) -> Result<Option<String>, Error> {
    let Some(images) = epg.get("images").filter(|v| truth(v)) else {
        return Ok(None);
    };
    let Some(images) = images.as_array() else {
        return Ok(None);
    };
    let mut best: Option<((u8, u8), String)> = None;
    for image in images {
        let Some(object) = image.as_object() else {
            continue;
        };
        let Some(url) = selected_text(object.get("url"))? else {
            continue;
        };
        let text = |key| -> Result<String, Error> {
            match object.get(key).filter(|v| truth(v)) {
                None => Ok(String::new()),
                Some(Value::String(v)) => Ok(v.to_lowercase()),
                _ => Err(Error::Unsupported("EPG artwork image score type")),
            }
        };
        let kind = text("type")?;
        let size = text("size")?;
        let score = (
            if matches!(kind.as_str(), "poster" | "cover") {
                2
            } else {
                1
            },
            match size.as_str() {
                "xxl" => 6,
                "xl" => 5,
                "l" => 4,
                "m" => 3,
                "s" => 2,
                "xs" => 1,
                _ => 0,
            },
        );
        if best.as_ref().is_none_or(|(old, _)| score > *old) {
            best = Some((score, url))
        }
    }
    Ok(best.map(|(_, v)| v))
}
async fn known<T>(
    connection: &mut sqlx::PgConnection,
    result: Result<T, sqlx::Error>,
) -> Result<Option<T>, Error> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.as_database_error().is_some() => {
            connection.ping().await.map_err(sql)?;
            Ok(None)
        }
        Err(error) => Err(sql(error)),
    }
}
async fn resolve(
    connection: &mut sqlx::PgConnection,
    plan: &Plan,
) -> Result<(Option<i64>, Option<String>), Error> {
    let runtime = &plan.runtime;
    let external = plan
        .title
        .as_ref()
        .is_some_and(|title| normalized(title) != normalized(&plan.channel_name));
    let mut url = best_image(&plan.epg_props)?;
    if let Some(value) = &url {
        if !runtime.valid(value).await? {
            url = None
        }
    }
    if url.is_none() {
        if let Some(icon) = plan
            .epg_props
            .get("icon")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
        {
            if runtime.valid(icon).await? {
                url = Some(icon.into())
            }
        }
    }
    let mut logo = None;
    if url.is_none() && external {
        for query in ["SELECT l.id::bigint FROM vod_movie m LEFT JOIN vod_vodlogo l ON l.id=m.logo_id WHERE UPPER(m.name::text)=UPPER($1) ORDER BY m.name LIMIT 1","SELECT l.id::bigint FROM vod_series m LEFT JOIN vod_vodlogo l ON l.id=m.logo_id WHERE UPPER(m.name::text)=UPPER($1) ORDER BY m.name LIMIT 1"]{
            let result=sqlx::query_scalar::<_,Option<i64>>(query).bind(plan.title.as_deref()).fetch_optional(&mut *connection).await;
            logo=known(connection,result).await?.flatten().flatten();if logo.is_some(){break}
        }
    }
    if logo.is_none() && url.is_none() && external {
        url = runtime
            .external(
                plan.title
                    .as_deref()
                    .ok_or(Error::Unsupported("provider title"))?,
                &plan.epg_props,
            )
            .await?;
    }
    if logo.is_none() && url.is_none() {
        for name in ["poster", "cover", "cover_big", "image", "icon"] {
            let value = plan.program.get(name);
            let candidate = match value {
                Some(Value::Object(v)) => selected_text(v.get("url"))?,
                Some(Value::String(v)) if !v.is_empty() => Some(v.clone()),
                _ => None,
            };
            if let Some(candidate) = candidate {
                if runtime.valid(&candidate).await? {
                    url = Some(candidate);
                    break;
                }
            }
        }
    }
    let clamped = plan
        .title
        .as_deref()
        .unwrap_or(&plan.channel_name)
        .chars()
        .take(255)
        .collect::<String>();
    if logo.is_none() && url.is_none() && external {
        let result=sqlx::query_as::<_,(i64,Option<String>)>("SELECT id::bigint,CASE WHEN octet_length(url)<=65536 THEN url END FROM dispatcharr_channels_logo WHERE UPPER(name)=UPPER($1) ORDER BY id LIMIT 1").bind(&clamped).fetch_optional(&mut *connection).await;
        if let Some(Some((id, value))) = known(connection, result).await? {
            logo = Some(id);
            url = Some(value.ok_or(Error::Unsupported("existing Logo URL capacity"))?);
        }
    }
    if logo.is_none() && url.as_ref().is_some_and(|v| v.chars().count() <= 1000) {
        let selected = url
            .as_ref()
            .ok_or(Error::Unsupported("selected logo URL"))?;
        let result = sqlx::query_scalar::<_, i64>(
            "SELECT id::bigint FROM dispatcharr_channels_logo WHERE url=$1 LIMIT 1",
        )
        .bind(selected)
        .fetch_optional(&mut *connection)
        .await;
        if let Some(Some(existing)) = known(connection, result).await? {
            return Ok((Some(existing), url));
        }

        let result=sqlx::query_scalar::<_,i64>("INSERT INTO dispatcharr_channels_logo(name,url) VALUES($1,$2) ON CONFLICT(url) DO NOTHING RETURNING id::bigint").bind(&clamped).bind(selected).fetch_optional(&mut *connection).await;
        if let Some(found) = known(connection, result).await? {
            logo = found;
            if logo.is_none() {
                let result = sqlx::query_scalar::<_, i64>(
                    "SELECT id::bigint FROM dispatcharr_channels_logo WHERE url=$1 LIMIT 1",
                )
                .bind(selected)
                .fetch_optional(&mut *connection)
                .await;
                logo = known(connection, result).await?.flatten();
            }
        }
    }
    if logo.is_none() && url.is_none() {
        logo = plan.channel_logo;
    }
    Ok((logo, url))
}
pub(super) async fn execute(
    store: Store,
    mut prepared: Prepared,
    owner: Arc<dyn Send + Sync>,
) -> Result<Completed, Error> {
    if !Arc::ptr_eq(&store.origin, &prepared.origin) {
        return Err(Error::Unsupported("foreign provider preparation"));
    }
    let plan = prepared
        .provider
        .take()
        .ok_or(Error::Unsupported("provider plan missing"))?;
    let owned = store.clone();
    let retained = owner.clone();
    let writer = prepared.writer.clone();
    let (resolved, changed) = tokio::spawn(async move {
        let _owner = retained;
        let _writer = writer;
        let mut connection = owned.pool.acquire().await.map_err(sql)?;
        let observed = async {
            let (logo, url) = resolve(&mut connection, &plan).await?;
            let (mut resolved, mut changed) = normal::resolved_properties(
                plan.properties,
                plan.program,
                plan.matched_program,
                logo,
                &plan.epg_props,
            )?;
            if let Some(url) = url.filter(|v| !v.is_empty()) {
                let object = resolved
                    .as_object_mut()
                    .ok_or(Error::Unsupported("provider recording object"))?;
                if object
                    .get("poster_url")
                    .is_none_or(|old| !python_equal(old, &json!(url)))
                {
                    object.insert("poster_url".into(), json!(url));
                    changed = true;
                }
            }
            Ok::<_, Error>((resolved, changed))
        }
        .await;
        let ready = connection.ping().await.err().map(Box::new);
        let close = connection.close().await.err().map(Box::new);
        match observed {
            Err(Error::Sql { primary, .. }) => Err(Error::Sql {
                primary,
                ready,
                close,
            }),
            other => {
                if ready.is_some() || close.is_some() {
                    Err(Error::Sql {
                        primary: None,
                        ready,
                        close,
                    })
                } else {
                    other
                }
            }
        }
    })
    .await
    .map_err(|_| Error::Supervisor)??;
    prepared.resolved = resolved;
    prepared.changed = changed;
    store.execute_mode(prepared, owner, true).await
}

// Two passes bound retained JSON nodes/strings before allocating Value trees.
// Same accounting policy as the existing provider-refresh JSON admission, without
// making the private/unwired VOD draft a dependency of production recordings.
struct Admission<'a> {
    remaining: &'a mut usize,
    depth: usize,
    exhausted: &'a mut bool,
}
impl Admission<'_> {
    fn charge<E: serde::de::Error>(&mut self, n: usize) -> Result<(), E> {
        if let Some(left) = self.remaining.checked_sub(n) {
            *self.remaining = left;
            Ok(())
        } else {
            *self.exhausted = true;
            Err(E::custom("artwork JSON capacity"))
        }
    }
    fn child(&mut self) -> Admission<'_> {
        Admission {
            remaining: self.remaining,
            depth: self.depth + 1,
            exhausted: self.exhausted,
        }
    }
}
impl<'de> serde::de::DeserializeSeed<'de> for Admission<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(mut self, d: D) -> Result<(), D::Error> {
        if self.depth > 64 {
            *self.exhausted = true;
            return Err(serde::de::Error::custom("artwork JSON depth"));
        }
        self.charge::<D::Error>(256)?;
        d.deserialize_any(self)
    }
}
impl<'de> serde::de::Visitor<'de> for Admission<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("bounded artwork JSON")
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E: serde::de::Error>(mut self, v: &str) -> Result<(), E> {
        self.charge(v.len().saturating_mul(2))
    }
    fn visit_string<E: serde::de::Error>(mut self, v: String) -> Result<(), E> {
        self.charge(v.capacity().saturating_mul(2))
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(mut self, mut a: A) -> Result<(), A::Error> {
        while a.next_element_seed(self.child())?.is_some() {}
        Ok(())
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(mut self, mut a: A) -> Result<(), A::Error> {
        while a.next_key_seed(self.child())?.is_some() {
            a.next_value_seed(self.child())?;
        }
        Ok(())
    }
}
fn decoded_json(bytes: &[u8]) -> Result<Option<Value>, Error> {
    use serde::de::DeserializeSeed;
    if bytes.starts_with(&[239, 187, 191]) || std::str::from_utf8(bytes).is_err() {
        return Err(Error::Unsupported("artwork nonUTF8 JSON encoding"));
    }
    let mut quoted = false;
    let mut escaped = false;
    for (i, b) in bytes.iter().enumerate() {
        if quoted {
            if escaped {
                escaped = false
            } else if *b == b'\\' {
                escaped = true
            } else if *b == b'"' {
                quoted = false
            }
        } else if *b == b'"' {
            quoted = true
        } else if bytes[i..].starts_with(b"NaN")
            || bytes[i..].starts_with(b"Infinity")
            || bytes[i..].starts_with(b"-Infinity")
        {
            return Err(Error::Unsupported("artwork nonfinite provider JSON"));
        }
    }
    let mut remaining = 8 * 1024 * 1024;
    let mut exhausted = false;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    if (Admission {
        remaining: &mut remaining,
        depth: 0,
        exhausted: &mut exhausted,
    })
    .deserialize(&mut decoder)
    .and_then(|()| decoder.end())
    .is_err()
    {
        return if exhausted {
            Err(Error::Unsupported("artwork decoded JSON capacity"))
        } else {
            Ok(None)
        };
    }
    let mut value: Value =
        serde_json::from_slice(bytes).map_err(|_| Error::Unsupported("artwork admitted JSON"))?;
    crate::json_compat::normalize_input_numbers(&mut value)
        .map_err(|_| Error::Unsupported("artwork JSON numeric domain"))?;
    Ok(Some(value))
}
#[cfg(test)]
mod controls {
    use super::*;
    #[test]
    fn response_budgets_remain_explicit_instead_of_becoming_provider_absence() {
        assert!(response_capacity(&std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            dispatcharr_upstream_http::WireLimitExceeded
        )));
        assert!(response_capacity(&std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            dispatcharr_upstream_http::ResourceLimitExceeded::HeaderBytes
        )));
        assert!(!response_capacity(&std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "actual transport EOF"
        )));
    }
    #[test]
    fn validation_cache_checks_capacity_at_final_insertion_under_competing_writers() {
        let cache = Arc::new(Mutex::new(BTreeMap::new()));
        for i in 0..511 {
            insert_validation(
                &mut cache.lock().unwrap(),
                &format!("owned-{i}"),
                true,
                Instant::now(),
            )
            .unwrap();
        }
        let threads = (0..4)
            .map(|i| {
                let cache = cache.clone();
                std::thread::spawn(move || {
                    insert_validation(
                        &mut cache.lock().unwrap(),
                        &format!("new-{i}"),
                        true,
                        Instant::now(),
                    )
                    .is_ok()
                })
            })
            .collect::<Vec<_>>();
        let completed = threads
            .into_iter()
            .filter_map(|t| t.join().ok())
            .filter(|ok| *ok)
            .count();
        assert_eq!(completed, 1);
        assert_eq!(cache.lock().unwrap().len(), 512);
    }
    #[test]
    fn source_epg_image_precedence_and_ties_are_stable_and_do_not_fabricate_url() {
        let images = json!({"images":[{"type":"icon","size":"xxl","url":"icon"},{"type":"poster","size":"m","url":"first"},{"type":"cover","size":"m","url":"tie"}]});
        assert_eq!(best_image(&images).unwrap().as_deref(), Some("first"));
        assert!(best_image(&json!({"images":"not a list"}))
            .unwrap()
            .is_none());
        assert!(matches!(
            best_image(&json!({"images":[{"type":3,"url":"candidate"}]})),
            Err(Error::Unsupported(_))
        ));
    }
    #[test]
    fn provider_url_encoding_matches_source_quote_and_itunes_upscale_literal() {
        assert_eq!(quote("A /日本 &?"), "A%20/%E6%97%A5%E6%9C%AC%20%26%3F");
        assert_eq!(
            "https://owned/100x100bb.jpg".replace("100x100", "600x600"),
            "https://owned/600x600bb.jpg"
        );
        assert!(selected_text(Some(&json!({"url":"not a string"}))).is_err());
    }
    #[test]
    fn real_malformed_json_is_caught_but_capacity_encoding_and_nonfinite_are_not_none() {
        assert!(decoded_json(b"{broken").unwrap().is_none());
        assert_eq!(
            decoded_json(br#"{"image":null,"results":[]}"#).unwrap(),
            Some(json!({"image":null,"results":[]}))
        );
        assert!(matches!(decoded_json(b"[NaN]"), Err(Error::Unsupported(_))));
        assert!(matches!(
            decoded_json(&[255, 254]),
            Err(Error::Unsupported(_))
        ));
        let many = format!("[{}]", vec!["null"; 40000].join(","));
        assert!(matches!(
            decoded_json(many.as_bytes()),
            Err(Error::Unsupported(_))
        ));
    }
}
