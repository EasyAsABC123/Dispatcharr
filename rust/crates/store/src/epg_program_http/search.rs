//! Ordinary programme search from the pinned Original Dispatcharr view.
//! Original authors retain full credit.
use super::{
    json_negotiation, origin, projected_fields, series_preview::SearchText, unavailable,
    BoundedJson, EpgProgramReads, OwnedJson,
};
use crate::{
    channels::{
        channel_options_headers, channel_options_indentation, options_json_response, scope,
        serialize_options_json,
    },
    core_reads::negotiate_literal_json_alias,
    vod_api_draft::vod_api_pagination::{last_query, ordinary_with_limits, page_url},
    StoreError, User,
};
use axum::{
    body::Body,
    extract::{ConnectInfo, OriginalUri, State},
    http::{header, HeaderMap, HeaderValue, Method, Uri},
    response::{IntoResponse, Response},
    routing::{get, MethodRouter},
};
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
};
use tokio::sync::{oneshot, OwnedSemaphorePermit};

pub(super) const PATH: &str = "/api/epg/programs/search/";
const ALLOW: &str = "GET, HEAD, OPTIONS";
const TITLE_AND_DESCRIPTION: [(&str, &str); 2] =
    [("title", "p.title"), ("description", "p.description")];
const DATE_FILTERS: [(&str, &str); 4] = [
    ("start_after", "p.start_time>="),
    ("start_before", "p.start_time<="),
    ("end_after", "p.end_time>="),
    ("end_before", "p.end_time<="),
];

type Pairs = Vec<(String, String)>;
struct Search {
    pairs: Pairs,
    fields: Option<BTreeSet<String>>,
    text: Vec<(&'static str, SearchText)>,
    times: Vec<(&'static str, DateTime<Utc>)>,
    source: Option<Option<i64>>,
    tvg_id: Option<String>,
    channel: Option<String>,
    channel_id: Option<Option<i64>>,
    stream: Option<String>,
    group: Option<String>,
}

impl Search {
    fn new(pairs: Pairs) -> Result<Self, StoreError> {
        let get = |name| last_query(&pairs, name).filter(|value| !value.is_empty());
        let mut text = Vec::new();
        for (key, column) in TITLE_AND_DESCRIPTION {
            let Some(value) = get(key) else { continue };
            let enabled = |suffix: &str| {
                last_query(&pairs, &format!("{key}_{suffix}")).is_some_and(|flag| {
                    matches!(flag.to_ascii_lowercase().as_str(), "true" | "1" | "yes")
                })
            };
            if let Some(predicate) =
                SearchText::parse(value, enabled("regex"), enabled("whole_words"))?
            {
                text.push((column, predicate));
            }
        }
        let mut times = Vec::new();
        for (name, condition) in DATE_FILTERS {
            if let Some(raw) = get(name) {
                times.push((condition, instant(name, raw)?));
            }
        }
        if let Some(raw) = get("airing_at") {
            let at = instant("airing_at", raw)?;
            times.extend([("p.start_time<=", at), ("p.end_time>", at)]);
        }
        let integer_filter = |key| {
            get(key)
                .and_then(dispatcharr_ingest_core::source_integer_literal)
                .map(|value| value.parse().ok())
        };
        let source = integer_filter("epg_source");
        let channel_id = integer_filter("channel_id");
        let channel = get("channel").map(str::to_owned);
        let stream = get("stream").map(str::to_owned);
        let group = get("group").map(str::to_owned);
        let tvg_id = get("tvg_id").map(str::to_owned);
        let fields =
            get("fields").map(|value| value.split(',').map(|k| k.trim().to_owned()).collect());
        Ok(Self {
            pairs,
            fields,
            text,
            times,
            source,
            tvg_id,
            channel,
            channel_id,
            stream,
            group,
        })
    }
    fn includes(&self, name: &str) -> bool {
        self.fields
            .as_ref()
            .is_none_or(|fields| fields.contains(name))
    }
    fn restrict<'a>(&self, query: &mut QueryBuilder<'a, Postgres>, user: &User) {
        query.push(" FROM epg_programdata p JOIN epg_epgdata e ON e.id=p.epg_id LEFT JOIN epg_epgsource g ON g.id=e.epg_source_id WHERE TRUE");
        if user.user_level < 10 {
            effective_guide_exists(query, |query| scope(query, user));
        }
        for (column, value) in &self.text {
            value.append(query, column);
        }
        for (condition, at) in &self.times {
            query.push(" AND ").push(*condition).push_bind(*at);
        }
        if let Some(tvg_id) = &self.tvg_id {
            query.push(" AND e.tvg_id=").push_bind(tvg_id.clone());
        }
        self.restrict_relations(query);
        match self.source {
            Some(Some(id)) => {
                query.push(" AND e.epg_source_id=").push_bind(id);
            }
            Some(None) => {
                query.push(" AND FALSE");
            }
            None => {}
        }
    }

    fn restrict_relations(&self, query: &mut QueryBuilder<'_, Postgres>) {
        // Source intersects separately resolved guide sets. Two channel
        // filters may match different channels that share an effective guide.
        if let Some(name) = &self.channel {
            effective_guide_exists(query, |query| {
                query.push(" AND ");
                contains(query, "COALESCE(o.name,c.name)", name);
            });
        }
        match self.channel_id {
            Some(Some(id)) => {
                effective_guide_exists(query, |query| {
                    query.push(" AND c.id=").push_bind(id);
                });
            }
            Some(None) => {
                query.push(" AND FALSE");
            }
            None => {}
        }
        if let Some(name) = &self.stream {
            effective_guide_exists(query, |query| {
                query.push(
                    " AND EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs
                       JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id
                       WHERE cs.channel_id=c.id AND ",
                );
                contains(query, "s.name", name);
                query.push(")");
            });
        }
        if let Some(name) = &self.group {
            effective_guide_exists(query, |query| {
                query.push(
                    " AND (EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroup cg
                       WHERE cg.id=COALESCE(o.channel_group_id,c.channel_group_id) AND ",
                );
                contains(query, "cg.name", name);
                query.push(
                    ") OR EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs
                       JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id
                       JOIN dispatcharr_channels_channelgroup sg ON sg.id=s.channel_group_id
                       WHERE cs.channel_id=c.id AND ",
                );
                contains(query, "sg.name", name);
                query.push("))");
            });
        }
    }
}

fn effective_guide_exists(
    query: &mut QueryBuilder<'_, Postgres>,
    condition: impl FnOnce(&mut QueryBuilder<'_, Postgres>),
) {
    query.push(
        " AND EXISTS(SELECT 1 FROM dispatcharr_channels_channel c
           LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id
           WHERE COALESCE(o.epg_data_id,c.epg_data_id)=p.epg_id",
    );
    condition(query);
    query.push(")");
}

fn contains(query: &mut QueryBuilder<'_, Postgres>, column: &'static str, term: &str) {
    query
        .push("strpos(upper(")
        .push(column)
        .push("), upper(")
        .push_bind(term.to_owned())
        .push(")) > 0");
}

fn instant(name: &str, raw: &str) -> Result<DateTime<Utc>, StoreError> {
    crate::grid_parse_datetime(raw,name).ok().and_then(|parsed|
        parsed.local.checked_sub_signed(TimeDelta::microseconds(parsed.offset_micros)))
        .map(|value|value.and_utc()).ok_or_else(||{
            let example=if name=="start_after" {" (e.g., 2026-02-14T18:00:00Z)"} else {""};
            let end=if example.is_empty() {"."} else {""};
            StoreError::StatusBody(400,json!({"error":format!("Invalid datetime format for {name}: {raw}. Use ISO 8601 format{example}{end}")}))
        })
}

fn stamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(
        if value.timestamp_subsec_micros() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        },
        true,
    )
}

#[derive(Default)]
struct Mapping {
    channels: Vec<Value>,
    streams: Vec<Value>,
    stream_ids: BTreeSet<i64>,
}

fn charge(used: &mut usize, value: &Value, limit: usize) -> Result<(), StoreError> {
    let mut measured = BoundedJson::new(limit.saturating_sub(*used));
    serde_json::to_writer(&mut measured, value).map_err(|_| unavailable())?;
    *used += measured.into_data().len();
    Ok(())
}

async fn mappings(
    tx: &mut PgConnection,
    user: &User,
    search: &Search,
    ids: Vec<i64>,
    limit: usize,
    row_limit: usize,
) -> Result<BTreeMap<i64, Mapping>, StoreError> {
    let mut map = BTreeMap::<i64, Mapping>::new();
    if ids.is_empty() || !search.includes("channels") && !search.includes("streams") {
        return Ok(map);
    }
    let mut used = 0usize;
    let mut query=QueryBuilder::new("SELECT c.id::bigint AS channel_id,COALESCE(o.epg_data_id,c.epg_data_id)::bigint AS epg_id,COALESCE(o.name,c.name) AS name,COALESCE(o.channel_number,c.channel_number) AS number,COALESCE(o.tvg_id,c.tvg_id) AS tvg_id,CASE WHEN octet_length(cg.name)<=");
    query.push_bind(row_limit as i64).push(" THEN cg.name END AS group_name,COALESCE(octet_length(cg.name)>")
        .push_bind(row_limit as i64).push(",false) AS oversized FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id LEFT JOIN dispatcharr_channels_channelgroup cg ON cg.id=COALESCE(o.channel_group_id,c.channel_group_id) WHERE COALESCE(o.epg_data_id,c.epg_data_id)::bigint=ANY(").push_bind(ids.clone()).push(")");
    scope(&mut query, user);
    query.push(" ORDER BY c.id");
    if search.includes("channels") {
        let mut rows = query.build().fetch(&mut *tx);
        while let Some(row) = rows.try_next().await? {
            if row.try_get::<bool, _>("oversized")? {
                return Err(unavailable());
            }
            let value = json!({"id":row.try_get::<i64,_>("channel_id")?,"name":row.try_get::<String,_>("name")?,
                "channel_number":row.try_get::<Option<f64>,_>("number")?,"channel_group":row.try_get::<Option<String>,_>("group_name")?,
                "tvg_id":row.try_get::<Option<String>,_>("tvg_id")?});
            charge(&mut used, &value, limit)?;
            map.entry(row.try_get("epg_id")?)
                .or_default()
                .channels
                .push(value);
        }
    }
    if search.includes("streams") {
        let mut query=QueryBuilder::new("SELECT COALESCE(o.epg_data_id,c.epg_data_id)::bigint AS epg_id,s.id::bigint AS stream_id,s.name,s.tvg_id,a.name AS provider,CASE WHEN octet_length(g.name)<=");
        query.push_bind(row_limit as i64).push(" THEN g.name END AS group_name,COALESCE(octet_length(g.name)>")
            .push_bind(row_limit as i64).push(",false) AS oversized FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id JOIN dispatcharr_channels_channelstream cs ON cs.channel_id=c.id JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id LEFT JOIN dispatcharr_channels_channelgroup g ON g.id=s.channel_group_id LEFT JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id WHERE COALESCE(o.epg_data_id,c.epg_data_id)::bigint=ANY(").push_bind(ids).push(")");
        scope(&mut query, user);
        query.push(" ORDER BY c.id,s.updated_at DESC");
        let mut rows = query.build().fetch(&mut *tx);
        while let Some(row) = rows.try_next().await? {
            if row.try_get::<bool, _>("oversized")? {
                return Err(unavailable());
            }
            let result = map.entry(row.try_get("epg_id")?).or_default();
            let id = row.try_get::<i64, _>("stream_id")?;
            if result.stream_ids.insert(id) {
                let value = json!({"id":id,"name":row.try_get::<String,_>("name")?,"channel_group":row.try_get::<Option<String>,_>("group_name")?,
                    "tvg_id":row.try_get::<Option<String>,_>("tvg_id")?,"m3u_account":row.try_get::<Option<String>,_>("provider")?});
                charge(&mut used, &value, limit)?;
                result.streams.push(value);
            }
        }
    }
    Ok(map)
}

async fn read(
    state: &EpgProgramReads,
    user: &User,
    search: &Search,
    base: &str,
    lease: Arc<OwnedSemaphorePermit>,
) -> Result<Bytes, StoreError> {
    use sqlx::Acquire;
    let limits = state.limits;
    let mut conn = state.auth.store.pool().acquire().await?;
    let mut tx = conn.begin().await?;
    let result=async{
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY").execute(&mut *tx).await?;
        sqlx::query("SELECT set_config('statement_timeout',$1,true)")
            .bind(format!("{}ms",limits.deadline.as_millis())).execute(&mut *tx).await?;
        let mut count_query=QueryBuilder::new("SELECT COUNT(*) AS total");
        search.restrict(&mut count_query,user);
        let count=count_query.build().fetch_one(&mut *tx).await?.try_get::<i64,_>("total")? as u64;
        let page=ordinary_with_limits(&search.pairs,count,50,500).map_err(|_|StoreError::StatusBody(404,json!({"detail":"Invalid page."})))?;
        let mut query=QueryBuilder::new(format!("SELECT {},p.epg_id::bigint AS epg_id,e.name AS epg_name,e.icon_url AS epg_icon_url,g.name AS epg_source",
            projected_fields(&limits.row_bytes.to_string())));
        search.restrict(&mut query,user);
        query.push(" ORDER BY p.start_time LIMIT ").push_bind(page.size as i64).push(" OFFSET ").push_bind(page.offset as i64);
        let mut programmes=Vec::new();
        let mut epg_ids=BTreeSet::new();
        let mut used=0usize;
        {
            let mut rows=query.build().fetch(&mut *tx);
            while let Some(row)=rows.try_next().await?{
                if row.try_get::<bool,_>("oversized")? {return Err(unavailable())}
                let epg_id=row.try_get::<i64,_>("epg_id")?;
                epg_ids.insert(epg_id);
                let mut value=json!({
                   "id":row.try_get::<i64,_>("id")?,"title":row.try_get::<String,_>("title")?,
                   "sub_title":row.try_get::<Option<String>,_>("sub_title")?,
                   "description":row.try_get::<Option<String>,_>("description")?,
                   "start_time":stamp(row.try_get("start_time")?),"end_time":stamp(row.try_get("end_time")?),
                   "tvg_id":row.try_get::<Option<String>,_>("tvg_id")?,
                   "custom_properties":row.try_get::<Option<Value>,_>("custom_properties")?,
                   "epg_source":row.try_get::<Option<String>,_>("epg_source")?,
                   "epg_name":row.try_get::<String,_>("epg_name")?,
                   "epg_icon_url":row.try_get::<Option<String>,_>("epg_icon_url")?,
                });
                value.as_object_mut().unwrap().retain(|key,_|search.includes(key));
                charge(&mut used,&value,limits.response_bytes)?;
                programmes.push((epg_id,value));
            }
        }
        let mapped=mappings(&mut tx,user,search,epg_ids.into_iter().collect(),limits.response_bytes.saturating_sub(used),limits.row_bytes).await?;
        let link=|number|page_url(base,PATH,&search.pairs,number);
        let previous=page.has_previous().then(||link((page.number>2).then_some(page.number-1)));
        let next=page.has_next().then(||link(Some(page.number+1)));
        let mut out=BoundedJson::new(limits.response_bytes);
        use std::io::Write;
        let topics=json!({"count":count,"next":next,"previous":previous});
        let top=serde_json::to_vec(&topics).map_err(|_|unavailable())?;
        out.write_all(&top[..top.len()-1]).map_err(|_|unavailable())?;
        out.write_all(b",\"results\":[").map_err(|_|unavailable())?;
        for (index,(epg_id,mut value)) in programmes.into_iter().enumerate() {
            if index>0 {out.write_all(b",").map_err(|_|unavailable())?;}
            if search.includes("channels") {value["channels"]=json!(mapped.get(&epg_id).map(|m|m.channels.as_slice()).unwrap_or(&[]));}
            if search.includes("streams") {value["streams"]=json!(mapped.get(&epg_id).map(|m|m.streams.as_slice()).unwrap_or(&[]));}
            serde_json::to_writer(&mut out,&value).map_err(|_|unavailable())?;
        }
        out.write_all(b"]}").map_err(|_|unavailable())?;
        Ok(Bytes::from_owner(OwnedJson{data:out.into_data(),_lease:lease}))
    }.await;
    tx.rollback().await?;
    result
}

pub(super) fn routes() -> MethodRouter<EpgProgramReads> {
    get(handle).fallback(handle)
}

fn search_options(indentation: usize) -> Result<Response, StoreError> {
    let metadata = json!({
        "name": "Search",
        "description": "",
        "renders": ["application/json", "text/html"],
        "parses": ["application/json", "application/x-www-form-urlencoded", "multipart/form-data"],
    });
    let body = serialize_options_json(metadata.as_object().unwrap(), indentation)
        .ok_or(StoreError::UnhandledRestFault)?;
    options_json_response(body)
}

async fn handle(
    State(state): State<EpgProgramReads>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    method: Method,
) -> Response {
    let result = async {
        let json_alias = uri.path() != PATH;
        if json_alias {
            negotiate_literal_json_alias(&headers)?;
        } else {
            json_negotiation(&headers, &uri)?;
        }

        let reads = matches!(method, Method::GET | Method::HEAD);
        let base = reads.then(|| origin(&state.auth, peer, &headers));
        let options_indentation = (method == Method::OPTIONS)
            .then(|| channel_options_indentation(&Uri::from_static("/"), &headers).unwrap_or(0));
        // DRF uses the declared Standard permission for search, and its
        // fail-closed administrator permission for metadata and other methods.
        let user = state.auth.account_user(peer, headers, !reads).await?;
        let Some(base) = base else {
            return if let Some(indentation) = options_indentation {
                search_options(indentation)
            } else {
                Err(StoreError::StatusBody(
                    405,
                    json!({"detail": format!("Method \"{method}\" not allowed.")}),
                ))
            };
        };
        if user.user_level < 1 {
            return Err(StoreError::PermissionDenied);
        }
        if json_alias {
            // Original's formatted action calls search with a format
            // argument that its handler signature does not accept.
            return Err(StoreError::UnhandledRestFault);
        }

        let raw = uri.query().unwrap_or_default();
        if raw.len() > 16384 {
            return Err(unavailable());
        }
        let search = Search::new(
            url::form_urlencoded::parse(raw.as_bytes())
                .into_owned()
                .collect(),
        )?;
        let lease = Arc::new(
            state
                .workers
                .clone()
                .try_acquire_owned()
                .map_err(|_| unavailable())?,
        );
        let deadline = state.limits.deadline;
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = read(&state, &user, &search, &base, lease).await;
            let _ = sender.send(result);
        });
        let data = tokio::time::timeout(deadline, receiver)
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())??;
        Ok((
            [(header::CONTENT_TYPE, "application/json")],
            Body::from(data),
        )
            .into_response())
    }
    .await;
    channel_options_headers(
        result.unwrap_or_else(IntoResponse::into_response),
        HeaderValue::from_static(ALLOW),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn programme_search_routing_checks_negotiation_before_permissions() {
        use axum::{body::to_bytes, extract::Request, Extension};
        use tower::ServiceExt;

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy_with(sqlx::postgres::PgConnectOptions::new());
        let auth = crate::AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("programme-method-test-only".into()).unwrap(),
        );
        let state =
            EpgProgramReads::new(auth, super::super::EpgProgramReadLimits::default()).unwrap();
        let router = state
            .router()
            .layer(Extension(ConnectInfo(SocketAddr::from((
                [127, 0, 0, 1],
                1234,
            )))));
        for path in [
            PATH,
            "/api/epg/programs/search.json",
            "/api/epg/programs/search.json/",
        ] {
            for method in [Method::GET, Method::HEAD, Method::OPTIONS, Method::POST] {
                for (accept, status) in [("application/json", 401), ("text/plain", 406)] {
                    let response = router
                        .clone()
                        .oneshot(
                            Request::builder()
                                .uri(path)
                                .method(method.clone())
                                .header(header::ACCEPT, accept)
                                .body(Body::empty())
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(response.status().as_u16(), status, "{method} {path}");
                    assert_eq!(response.headers()[header::ALLOW], ALLOW);
                    assert_eq!(response.headers()[header::VARY], "Accept");
                    if method == Method::HEAD {
                        assert!(to_bytes(response.into_body(), 1024)
                            .await
                            .unwrap()
                            .is_empty());
                    }
                }
            }
        }
        let body = to_bytes(search_options(0).unwrap().into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(
            body.as_ref(),
            br#"{"name":"Search","description":"","renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]}"#
        );
        let pretty = to_bytes(search_options(4).unwrap().into_body(), 1024)
            .await
            .unwrap();
        assert!(pretty.starts_with(b"{\n    \"name\": \"Search\",\n    \"description\":"));
    }

    #[tokio::test]
    #[ignore = "requires a freshly owned Unix-socket PostgreSQL fixture"]
    async fn programme_search_postgres_role_override_pagination() {
        use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
        let socket = std::env::var("DISPATCHARR_OWNED_GUIDE_SEARCH_SOCKET").unwrap();
        assert!(
            socket.starts_with("/tmp/dispatcharr-guide-search-") && socket.ends_with("/socket")
        );
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(
                PgConnectOptions::new()
                    .socket(&socket)
                    .port(54941)
                    .username("fixture_owner")
                    .database("postgres"),
            )
            .await
            .unwrap();
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT inet_server_addr() IS NULL AND current_setting('listen_addresses')=''"
        )
        .fetch_one(&pool)
        .await
        .unwrap());
        sqlx::raw_sql(r#"
            CREATE TABLE epg_epgsource(id bigint PRIMARY KEY,name text);
            CREATE TABLE epg_epgdata(id bigint PRIMARY KEY,name text NOT NULL,tvg_id text,icon_url text,epg_source_id bigint);
            CREATE TABLE epg_programdata(id bigint PRIMARY KEY,epg_id bigint NOT NULL,title text NOT NULL,sub_title text,description text,
                tvg_id text,custom_properties jsonb,start_time timestamptz NOT NULL,end_time timestamptz NOT NULL);
            CREATE TABLE dispatcharr_channels_channel(id bigint PRIMARY KEY,name text NOT NULL,channel_number double precision,
                tvg_id text,epg_data_id bigint,channel_group_id bigint,user_level integer NOT NULL,is_adult boolean NOT NULL);
            CREATE TABLE dispatcharr_channels_channeloverride(channel_id bigint PRIMARY KEY,name text,channel_number double precision,
                tvg_id text,epg_data_id bigint,channel_group_id bigint);
            CREATE TABLE dispatcharr_channels_channelgroup(id bigint PRIMARY KEY,name text);
            CREATE TABLE dispatcharr_channels_channelstream(channel_id bigint,stream_id bigint);
            CREATE TABLE dispatcharr_channels_stream(id bigint PRIMARY KEY,name text NOT NULL,tvg_id text,channel_group_id bigint,
                m3u_account_id bigint,updated_at timestamptz NOT NULL);
            CREATE TABLE m3u_m3uaccount(id bigint PRIMARY KEY,name text);
            CREATE TABLE accounts_user_channel_profiles(user_id bigint,channelprofile_id bigint);
            CREATE TABLE dispatcharr_channels_channelprofilemembership(channel_id bigint,channel_profile_id bigint,enabled boolean);
            INSERT INTO epg_epgsource VALUES (1,'Owned Guide');
            INSERT INTO epg_epgdata VALUES (1,'Mapped','mapped',NULL,1),(2,'Old','old',NULL,1),
                                          (3,'Excluded','excluded',NULL,1),(4,'Unmapped','unmapped',NULL,1);
            INSERT INTO epg_programdata VALUES
                (1,1,'Owned News',NULL,'Owned synopsis',NULL,'{"owned":true}','2026-02-14 18:00Z','2026-02-14 18:30Z'),
                (2,1,'Owned Sports',NULL,'Owned synopsis',NULL,'{"owned":true}','2026-02-14 18:30Z','2026-02-14 19:00Z'),
                (3,2,'Provider old mapping',NULL,'Owned synopsis',NULL,'{"owned":true}','2026-02-14 18:04Z','2026-02-14 18:34Z'),
                (4,3,'Outside scope',NULL,'Owned synopsis',NULL,'{"owned":true}','2026-02-14 18:08Z','2026-02-14 18:38Z'),
                (5,4,'Unmapped admin',NULL,'Owned synopsis',NULL,'{"owned":true}','2026-02-14 18:12Z','2026-02-14 18:42Z');
            INSERT INTO dispatcharr_channels_channel VALUES
                (1,'Provider Channel',5,NULL,2,1,0,false),(2,'Other role',5,NULL,1,1,0,false),
                (3,'Outside',5,NULL,3,1,0,false),(4,'Adult',5,NULL,3,1,0,true),(5,'Admin high',5,NULL,3,1,5,false);
            INSERT INTO dispatcharr_channels_channeloverride VALUES (1,'Owned Override',5.5,'override',1,NULL);
            INSERT INTO dispatcharr_channels_channelgroup VALUES(1,'Owned Group');
            INSERT INTO dispatcharr_channels_stream VALUES (1,'Private feed label','input',1,1,'2026-02-14 18:00Z');
            INSERT INTO dispatcharr_channels_channelstream VALUES(1,1);
            INSERT INTO m3u_m3uaccount VALUES(1,'Owned Feed');
            INSERT INTO accounts_user_channel_profiles VALUES(1,1);
            INSERT INTO dispatcharr_channels_channelprofilemembership VALUES
                (1,1,true),(2,1,false),(3,1,false),(4,1,true),(5,1,true);
        "#).execute(&pool).await.unwrap();
        let state = EpgProgramReads::new(
            crate::AuthService::new(
                crate::Store::from_pool(pool.clone()),
                crate::AuthConfig::new("owned-search-component".into()).unwrap(),
            ),
            Default::default(),
        )
        .unwrap();
        let standard = User {
            id: 1,
            username: "owned".into(),
            password: String::new(),
            is_active: true,
            user_level: 1,
            stream_limit: 0,
            custom_properties: json!({"hide_adult_content":true}),
        };
        let mut admin = standard.clone();
        admin.user_level = 10;
        let read = |user: &User, query: &str| {
            let state = state.clone();
            let user = user.clone();
            let search = Search::new(
                url::form_urlencoded::parse(query.as_bytes())
                    .into_owned()
                    .collect(),
            )
            .unwrap();
            async move {
                let lease = Arc::new(state.workers.clone().try_acquire_owned().unwrap());
                super::read(&state, &user, &search, "http://testserver", lease).await
            }
        };
        let data = read(&standard, "").await.unwrap();
        let value: Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(value["count"], 2);
        for row in value["results"].as_array().unwrap() {
            assert_eq!(
                row["channels"],
                json!([{"id":1,"name":"Owned Override","channel_number":5.5,"channel_group":"Owned Group","tvg_id":"override"}])
            );
            assert_eq!(
                row["streams"],
                json!([{"id":1,"name":"Private feed label","channel_group":"Owned Group","tvg_id":"input","m3u_account":"Owned Feed"}])
            );
        }
        drop(data);
        let data = read(&standard, "page_size=1&page=last").await.unwrap();
        let value: Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(
            value["previous"],
            "http://testserver/api/epg/programs/search/?page_size=1"
        );
        assert_eq!(value["next"], Value::Null);
        assert_eq!(value["results"][0]["id"], 2);
        drop(data);
        let data = read(
            &standard,
            "airing_at=2026-02-14T19%3A30%3A00%2B01%3A00&fields=title,id,start_time",
        )
        .await
        .unwrap();
        let value: Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(
            value["results"],
            json!([{"id":2,"title":"Owned Sports","start_time":"2026-02-14T18:30:00Z"}])
        );
        drop(data);
        for (query, expected) in [
            ("title=Owned+AND+Sports&fields=id", json!([{"id": 2}])),
            (
                "title=Owned+OR+Absent&fields=id",
                json!([{"id": 1}, {"id": 2}]),
            ),
            (
                "title=Sports&title_whole_words=1&fields=id",
                json!([{"id": 2}]),
            ),
            (
                "title=%5EOwned+Sports%24&title_regex=yes&fields=id",
                json!([{"id": 2}]),
            ),
            (
                "title=%28Absent+OR+Sports%29+AND+Owned&fields=id",
                json!([{"id": 2}]),
            ),
        ] {
            let data = read(&standard, query).await.unwrap();
            let value: Value = serde_json::from_slice(&data).unwrap();
            assert_eq!(value["results"], expected, "{query}");
        }
        sqlx::raw_sql(
            r#"
            INSERT INTO dispatcharr_channels_channelgroup VALUES
                (2,'Override Group'),(3,'Hidden Base Group'),(4,'Stream Group');
            UPDATE dispatcharr_channels_channel SET channel_group_id=3 WHERE id=1;
            UPDATE dispatcharr_channels_channeloverride SET channel_group_id=2 WHERE channel_id=1;
            INSERT INTO dispatcharr_channels_channel VALUES
                (6,'Other Visible',6,NULL,1,NULL,0,false);
            INSERT INTO dispatcharr_channels_channelprofilemembership VALUES (6,1,true);
            INSERT INTO dispatcharr_channels_stream VALUES
                (2,'Secondary _100% Feed','second',4,1,'2026-02-14 18:00Z');
            INSERT INTO dispatcharr_channels_channelstream VALUES (6,2),(6,1);
        "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let both = json!([{"id":1},{"id":2}]);
        for (query, expected) in [
            ("channel=Owned+Override&fields=id", both.clone()),
            ("channel=Provider+Channel&fields=id", json!([])),
            ("channel_id=%2B006&fields=id", both.clone()),
            ("channel_id=foo&fields=id", both.clone()),
            ("channel_id=0&fields=id", json!([])),
            ("channel_id=-1&fields=id", json!([])),
            (
                "channel_id=999999999999999999999999999&fields=id",
                json!([]),
            ),
            ("group=Override+Group&fields=id", both.clone()),
            ("group=Hidden+Base+Group&fields=id", json!([])),
            ("group=Stream+Group&fields=id", both.clone()),
            ("stream=_100%25&fields=id", both.clone()),
            (
                "channel=OwnED+Override&channel_id=6&stream=_100%25&group=Override+Group&fields=id",
                both,
            ),
            ("channel=Outside&fields=id", json!([])),
        ] {
            let data = read(&standard, query).await.unwrap();
            let value: Value = serde_json::from_slice(&data).unwrap();
            assert_eq!(value["results"], expected, "{query}");
        }
        let data = read(&admin, "fields=id").await.unwrap();
        let value: Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(
            value["results"],
            json!([{"id":1},{"id":3},{"id":4},{"id":5},{"id":2}])
        );
        drop(data);
        assert!(matches!(
            read(&standard, "page_size=1&page=3").await,
            Err(StoreError::StatusBody(404, _))
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM epg_programdata")
                .fetch_one(&pool)
                .await
                .unwrap(),
            5
        );
        pool.close().await;
    }

    #[test]
    fn programme_search_literal_dates_filter_scope_and_projection() {
        let pairs=url::form_urlencoded::parse(b"title=Own&title=News+Evening&airing_at=2026-02-14T19%3A30%3A00%2B01%3A00&fields=title%2Cid%2Cstart_time").into_owned().collect();
        let search = Search::new(pairs).unwrap();
        assert_eq!(search.text.len(), 1);
        let mut query = QueryBuilder::<Postgres>::new("WHERE TRUE");
        search.text[0].1.append(&mut query, search.text[0].0);
        assert_eq!(
            query.sql(),
            "WHERE TRUE AND strpos(upper(p.title), upper($1)) > 0"
        );
        assert_eq!(
            search.times,
            [
                ("p.start_time<=", "2026-02-14T18:30:00Z".parse().unwrap()),
                ("p.end_time>", "2026-02-14T18:30:00Z".parse().unwrap())
            ]
        );
        assert!(search.includes("start_time") && !search.includes("streams"));
        for (pairs, fragment) in [
            (vec![("title".into(), "This OR That".into())], " OR "),
            (
                vec![
                    ("description".into(), "^owned".into()),
                    ("description_regex".into(), "true".into()),
                ],
                "p.description ~* ",
            ),
            (
                vec![
                    ("title".into(), "News".into()),
                    ("title_whole_words".into(), "yes".into()),
                ],
                "p.title ~* ",
            ),
        ] {
            let filter = Search::new(pairs).unwrap();
            let mut query = QueryBuilder::<Postgres>::new("WHERE TRUE");
            filter.text[0].1.append(&mut query, filter.text[0].0);
            assert!(query.sql().contains(fragment));
        }
    }
}
