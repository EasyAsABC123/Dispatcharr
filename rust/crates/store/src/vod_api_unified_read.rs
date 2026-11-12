//! Source-validated bounded unified raw-page/projection/fresh-count reader.
//! Actual SQL ordering/error/physical ownership gates passed; canonical app,
//! unchanged React and remaining protocol contracts are separate.
//! Original Dispatcharr authors retain application credit and AGPL licensing.
use super::{
    vod_api_dto::{self, CommonRow, LogoContext, LogoRow, UnifiedError},
    vod_api_owned_read::{ReadError, ReadWork},
    vod_api_projection::Visibility,
    vod_api_unified_query::{QueryFault, UnifiedPage, UnifiedQuery},
};
use chrono::{DateTime, Utc};
use dispatcharr_contracts::BoxFuture;
use futures_util::TryStreamExt;
use num_bigint::BigInt;
use serde_json::Value;
use sqlx::{postgres::PgRow, PgPool, Row};
use std::{io::Write, mem::size_of, sync::Arc};

#[derive(Debug)]
pub(super) enum UnifiedFault {
    Read(ReadError),
    View(String),
    /// A successful view can fail only later in JSONRenderer (NaN/Infinity).
    /// Do not convert it into the view's {error:...} JSON response.
    Renderer,
}
impl From<ReadError> for UnifiedFault {
    fn from(value: ReadError) -> Self {
        Self::Read(value)
    }
}
impl From<sqlx::Error> for UnifiedFault {
    fn from(value: sqlx::Error) -> Self {
        match &value {
            sqlx::Error::Database(error) => Self::View(error.message().to_owned()),
            _ => Self::Read(value.into()),
        }
    }
}
impl From<QueryFault> for UnifiedFault {
    fn from(value: QueryFault) -> Self {
        match value {
            QueryFault::View(message) => Self::View(message),
            QueryFault::Capacity => Self::Read(ReadError::Capacity),
            QueryFault::Uncaptured(message) => Self::Read(ReadError::Unsupported(message)),
        }
    }
}
fn capacity() -> UnifiedFault {
    ReadError::Capacity.into()
}

/// Own fields rather than retaining SQLx rows backed by shared protocol
/// buffers. There is at most one guarded PgRow outside this charged page.
struct RawRow {
    common: CommonRow,
    properties_json: Option<String>,
    logo: Option<LogoRow>,
    duration: Option<i64>,
    kind: String,
}
impl RawRow {
    fn decode(row: PgRow) -> Result<Self, UnifiedFault> {
        if row.try_get::<bool, _>("oversized")? {
            return Err(capacity());
        }
        let common = CommonRow {
            id: row.try_get("id")?,
            uuid: row.try_get("uuid")?,
            name: row.try_get("name")?,
            description: row.try_get("description")?,
            year: row.try_get("year")?,
            rating: row.try_get("rating")?,
            genre: row.try_get("genre")?,
            tmdb_id: None,
            imdb_id: None,
            language: String::new(),
            custom_properties: Value::Null,
            created_at: row.try_get::<DateTime<Utc>, _>("created_at")?,
            updated_at: row.try_get::<DateTime<Utc>, _>("updated_at")?,
        };
        let logo = row
            .try_get::<Option<i64>, _>("logo_id")?
            .filter(|id| *id != 0)
            .map(|id| {
                Ok::<_, sqlx::Error>(LogoRow {
                    id,
                    name: row.try_get("logo_name")?,
                    url: row.try_get("logo_url")?,
                    movie_count: 0,
                    series_count: 0,
                    is_used: true,
                    movie_names: Vec::new(),
                    series_names: Vec::new(),
                })
            })
            .transpose()?;
        Ok(Self {
            common,
            properties_json: row.try_get("properties_json")?,
            logo,
            duration: row.try_get("duration")?,
            kind: row.try_get("content_type")?,
        })
    }
    fn heap_bytes(&self) -> Result<usize, UnifiedFault> {
        let mut count = 0usize;
        let fields = [
            Some(&self.common.uuid),
            Some(&self.common.name),
            self.common.description.as_ref(),
            self.common.rating.as_ref(),
            self.common.genre.as_ref(),
            self.properties_json.as_ref(),
            Some(&self.kind),
            self.logo.as_ref().map(|v| &v.name),
            self.logo.as_ref().map(|v| &v.url),
        ];
        for value in fields.into_iter().flatten() {
            count = count.checked_add(value.capacity()).ok_or_else(capacity)?;
        }
        Ok(count)
    }
    fn project(mut self, context: &LogoContext<'_>) -> Result<(Value, bool), UnifiedFault> {
        // Django's raw cursor deliberately bypasses JSONField conversion:
        // psycopg returns PostgreSQL JSONB text, which the unified serializer
        // retains as a JSON string (ordinary ORM DTOs instead expose objects).
        // Actual Source/native1 body mismatches demonstrate this distinction.
        self.common.custom_properties = self
            .properties_json
            .take()
            .map(Value::String)
            .unwrap_or(Value::Null);
        let nonfinite = match self.common.rating.as_deref().filter(|v| !v.is_empty()) {
            Some(raw) => match dispatcharr_ingest_core::source_float_literal(raw) {
                Some(value) => !value.is_finite(),
                None => {
                    let quoted =
                        crate::python_action::collection_text(&Value::String(raw.into()), 65536)
                            .ok_or_else(capacity)?;
                    return Err(UnifiedFault::View(format!(
                        "could not convert string to float: {quoted}"
                    )));
                }
            },
            None => false,
        };
        // Retain the fault separately until all later row projections, count
        // and links complete. This placeholder is never returned on success.
        if nonfinite {
            self.common.rating = Some("0".into());
        }
        let value = vod_api_dto::unified(
            &self.common,
            self.logo.as_ref(),
            self.duration,
            &self.kind,
            context,
        )
        .map_err(|error| match error {
            UnifiedError::InvalidRating => UnifiedFault::Read(ReadError::WorkerFailed),
            UnifiedError::NonFiniteRating => UnifiedFault::Renderer,
        })?;
        Ok((value, nonfinite))
    }
}

type PhaseHook = Arc<dyn Fn() -> BoxFuture<'static, Result<(), UnifiedFault>> + Send + Sync>;
#[derive(Default)]
pub(super) struct PhaseHooks {
    pub after_page: Option<PhaseHook>,
    pub before_count: Option<PhaseHook>,
}

/// Same admitted work lease as ordinary/category reads. Source fetches ALL
/// page rows before projecting, then counts in a fresh autocommit statement.
/// Raw page capacity is actual owned strings + vector capacity + row overhead,
/// never a 100/124-row clamp. Over-budget requests fail explicitly.
pub(super) async fn read(
    pool: &PgPool,
    work: &ReadWork,
    visible: &Visibility,
    pairs: &[(String, String)],
    origin: &str,
    path: &str,
) -> Result<Vec<u8>, UnifiedFault> {
    read_with_hooks(
        pool,
        work,
        visible,
        pairs,
        origin,
        path,
        &PhaseHooks::default(),
    )
    .await
}
async fn read_with_hooks(
    pool: &PgPool,
    work: &ReadWork,
    visible: &Visibility,
    pairs: &[(String, String)],
    origin: &str,
    path: &str,
    hooks: &PhaseHooks,
) -> Result<Vec<u8>, UnifiedFault> {
    let Some(selection) = UnifiedQuery::parse(pairs, visible)? else {
        return Ok(b"{\"count\":0,\"next\":null,\"previous\":null,\"results\":[]}".to_vec());
    };
    let mut connection = work.connection(pool).await?;
    let result = async {
        let statement = selection.page(visible, work.row_bytes())?;
        // 256x one-row parser/DTO allowance and 4MiB fixed work. Results Vec
        // growth can temporarily retain its previous allocation too. These
        // credits partition, rather than add to, the shared work reservation.
        let reserved = work
            .row_bytes()
            .checked_mul(256)
            .and_then(|v| v.checked_add(4 * 1024 * 1024))
            .and_then(|v| v.checked_add(work.response_bytes() / 2))
            .ok_or_else(capacity)?;
        let raw_budget = work
            .work_bytes()
            .checked_sub(reserved)
            .ok_or_else(capacity)?;
        let mut rows: Vec<RawRow> = Vec::new();
        let mut heap = 0usize;
        {
            let mut cursor = statement.query().fetch(connection.raw());
            while let Some(row) = cursor.try_next().await? {
                work.check()?;
                let row = RawRow::decode(row)?;
                let row_heap = row.heap_bytes()?.checked_add(256).ok_or_else(capacity)?;
                let next_heap = heap.checked_add(row_heap).ok_or_else(capacity)?;
                if rows.len() == rows.capacity() {
                    let old = rows.capacity();
                    let next = (old.max(8)).checked_mul(2).ok_or_else(capacity)?;
                    let metadata = old
                        .checked_add(next)
                        .and_then(|v| v.checked_mul(size_of::<RawRow>()))
                        .and_then(|v| v.checked_add(next_heap))
                        .ok_or_else(capacity)?;
                    if metadata > raw_budget {
                        return Err(capacity());
                    }
                    rows.try_reserve_exact(next - rows.len())
                        .map_err(|_| capacity())?;
                }
                let retained = rows
                    .capacity()
                    .checked_mul(size_of::<RawRow>())
                    .and_then(|v| v.checked_add(next_heap))
                    .ok_or_else(capacity)?;
                if retained > raw_budget {
                    return Err(capacity());
                }
                heap = next_heap;
                rows.push(row);
            }
        }
        if let Some(hook) = &hooks.after_page {
            hook().await?;
        }
        let mut results = work.document();
        let mut renderer_fault = false;
        results.write_all(b"[").map_err(|_| capacity())?;
        for (ordinal, row) in rows.into_iter().enumerate() {
            work.check()?;
            let (value, nonfinite) = row.project(&LogoContext {
                absolute_origin: Some(origin),
            })?;
            renderer_fault |= nonfinite;
            if ordinal > 0 {
                results.write_all(b",").map_err(|_| capacity())?;
            }
            let bytes = crate::json_compat::compact_json_bytes(&value).map_err(|_| capacity())?;
            results.write_all(&bytes).map_err(|_| capacity())?;
        }
        results.write_all(b"]").map_err(|_| capacity())?;
        work.check()?;
        connection.configure(work).await?;
        if let Some(hook) = &hooks.before_count {
            hook().await?;
        }
        let count = selection
            .count(visible)?
            .query()
            .fetch_one(connection.raw())
            .await?
            .try_get::<i64, _>("count")?;
        let (next, previous) = links(origin, path, pairs, &selection.page, count)?;
        // Serializer/renderer follows the count/link phase. A later PG or
        // projection failure therefore overrides an earlier nonfinite rating.
        if renderer_fault {
            return Err(UnifiedFault::Renderer);
        }
        let mut prefix = super::vod_api_owned_read::Document::bounded(4 * 1024 * 1024);
        prefix.write_all(b"{\"count\":").map_err(|_| capacity())?;
        serde_json::to_writer(&mut prefix, &count).map_err(|_| capacity())?;
        prefix.write_all(b",\"next\":").map_err(|_| capacity())?;
        serde_json::to_writer(&mut prefix, &next).map_err(|_| capacity())?;
        prefix
            .write_all(b",\"previous\":")
            .map_err(|_| capacity())?;
        serde_json::to_writer(&mut prefix, &previous).map_err(|_| capacity())?;
        prefix.write_all(b",\"results\":").map_err(|_| capacity())?;
        let prefix = prefix.finish();
        let mut bytes = results.finish();
        let old_len = bytes.len();
        let final_len = old_len
            .checked_add(prefix.len())
            .and_then(|v| v.checked_add(1))
            .ok_or_else(capacity)?;
        if final_len > work.response_bytes() {
            return Err(capacity());
        }
        bytes
            .try_reserve_exact(final_len - old_len)
            .map_err(|_| capacity())?;
        if bytes.capacity() > work.response_bytes() {
            return Err(capacity());
        }
        bytes.resize(final_len, 0);
        bytes.copy_within(..old_len, prefix.len());
        bytes[..prefix.len()].copy_from_slice(&prefix);
        bytes[final_len - 1] = b'}';
        Ok(bytes)
    }
    .await;
    let cleanup = connection.finish().await;
    match (result, cleanup) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
        (Ok(value), Ok(())) => Ok(value),
    }
}

fn links(
    origin: &str,
    path: &str,
    pairs: &[(String, String)],
    page: &UnifiedPage,
    count: i64,
) -> Result<(Option<String>, Option<String>), QueryFault> {
    let next = if &page.offset + &page.size < BigInt::from(count) {
        Some(page_link(origin, path, pairs, Some(&(&page.number + 1)))?)
    } else {
        None
    };
    let previous = if page.number > BigInt::from(1) {
        let prior = &page.number - 1;
        Some(page_link(
            origin,
            path,
            pairs,
            (prior != BigInt::from(1)).then_some(&prior),
        )?)
    } else {
        None
    };
    Ok((next, previous))
}
fn page_link(
    origin: &str,
    path: &str,
    pairs: &[(String, String)],
    page: Option<&BigInt>,
) -> Result<String, QueryFault> {
    let page = page
        .map(super::vod_api_unified_query::decimal_text)
        .transpose()?;
    Ok(super::vod_api_pagination::page_url_text(
        origin,
        path,
        pairs,
        page.as_deref(),
    ))
}

#[cfg(test)]
mod tests {
    use super::super::{
        vod_api_owned_read::{ReadLimits, ReadRuntime},
        vod_api_projection::visibility,
    };
    use super::*;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use sqlx::postgres::PgPoolOptions;
    use std::time::Duration;

    pub(super) async fn actual_owned_rows(pool: &PgPool, source: &Value) -> Value {
        let mut result = serde_json::Map::new();
        for (table, expected) in source["fingerprints_after"].as_object().unwrap() {
            assert!(table
                .bytes()
                .all(|v| v.is_ascii_lowercase() || v.is_ascii_digit() || v == b'_'));
            assert!(expected["rows"].as_u64().unwrap() <= 10000);
            let query = format!("SELECT count(*)::bigint AS count,md5(COALESCE(jsonb_agg(to_jsonb(t) ORDER BY id)::text,'')) AS digest FROM {table} t");
            let row = sqlx::query(&query)
                .persistent(false)
                .fetch_one(pool)
                .await
                .unwrap();
            let count: i64 = row.try_get("count").unwrap();
            assert_eq!(Some(count as u64), expected["rows"].as_u64());
            result.insert(
                table.clone(),
                json!({"rows":count,"sql_jsonb_md5":row.try_get::<String,_>("digest").unwrap()}),
            );
        }
        result.into()
    }

    #[tokio::test]
    #[ignore = "Requires uniquely owned actual Source SQL fixture and captured unified corpus"]
    async fn actual_source_unified_page_projection_then_count() {
        assert_eq!(
            std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
        );
        let url = std::env::var("DATABASE_URL").unwrap();
        assert!(url.contains("playback-fixture-only"));
        let source: Value = serde_json::from_slice(
            &std::fs::read(std::env::var("VOD_UNIFIED_SOURCE").unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(source["passed"], true);
        assert_eq!(source["observation"], "factory");
        assert_eq!(source["exact_owned_rows_restored"], true);
        assert_eq!(source["provider_trap_attempts"], json!([]));
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .unwrap();
        let movie = sqlx::query("SELECT id::bigint AS id,rating,custom_properties FROM vod_movie WHERE name LIKE 'rust-probe-vod-rest-%' ORDER BY id LIMIT 1")
            .persistent(false).fetch_one(&pool).await.unwrap();
        let movie_id: i64 = movie.try_get("id").unwrap();
        let saved_rating: Option<String> = movie.try_get("rating").unwrap();
        let saved_properties: Value = movie.try_get("custom_properties").unwrap();
        let series = sqlx::query("SELECT id::bigint AS id,name FROM vod_series WHERE name LIKE 'rust-probe-vod-rest-%' ORDER BY id LIMIT 1")
            .persistent(false).fetch_one(&pool).await.unwrap();
        let series_id: i64 = series.try_get("id").unwrap();
        let series_name: String = series.try_get("name").unwrap();
        let movie_name: String = sqlx::query_scalar("SELECT name FROM vod_movie WHERE id=$1")
            .bind(movie_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let actual_before = actual_owned_rows(&pool, &source).await;
        let runtime = ReadRuntime::new(ReadLimits::default()).unwrap();
        let mut records = Vec::new();
        let mut differences = Vec::new();
        let mut excluded = Vec::new();
        for (index, case) in source["records"].as_array().unwrap().iter().enumerate() {
            let path = case["path"].as_str().unwrap();
            let base = path.split('?').next().unwrap();
            let role = case["role"].as_str().unwrap_or("");
            let accept = case["headers"]["Accept"].as_str().unwrap_or("");
            let label = &case["label"];
            if case["method"] != "GET"
                || base != "/api/vod/all/"
                || matches!(role, "" | "basic" | "inactive")
                || label.get("presented_auth_scheme").is_some()
                || !matches!(accept, "application/json" | "application/json;q=0" | "*/*")
                || path.contains("format=bad")
                || path.contains("format=api")
            {
                excluded.push(json!({"source_index":index,"reason":"Outside SQL projection component: method/auth/route/negotiation"}));
                continue;
            }
            let properties = match role {
                "movies_off" | "admin_movies_off" => json!({"vod_movies_enabled":false}),
                "series_off" => json!({"vod_series_enabled":false}),
                "both_off" => json!({"vod_movies_enabled":false,"vod_series_enabled":false}),
                "zero_flags" => json!({"vod_movies_enabled":0,"vod_series_enabled":0}),
                "adult_hidden" => json!({"hide_adult_content":true}),
                _ => json!({}),
            };
            let visible =
                visibility(&properties, if role.starts_with("admin") { 10 } else { 1 }).unwrap();
            if let Some(rating) = label.get("rating") {
                let rating = rating.as_str();
                sqlx::query("UPDATE vod_movie SET rating=$1 WHERE id=$2")
                    .bind(rating)
                    .bind(movie_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            if let Some(properties) = label.get("properties") {
                sqlx::query("UPDATE vod_movie SET custom_properties=$1 WHERE id=$2")
                    .bind(if properties.is_null() {
                        None
                    } else {
                        Some(properties)
                    })
                    .bind(movie_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            if label == "cross-kind LOWER(name),id tie" {
                sqlx::query("UPDATE vod_series SET name=$1 WHERE id=$2")
                    .bind(&movie_name)
                    .bind(series_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            // Predeclared six tied-source cases ONLY: actual Original HTTP
            // immediately before the query; never selected from mismatches.
            let (expected, fresh_binding) = if label == "cross-kind LOWER(name),id tie" {
                let (expected, binding) =
                    super::phase_tests::fresh_original_tie(&pool, &source, case).await;
                (expected, Some(binding))
            } else {
                (case["expected"].clone(), None)
            };
            let no_write_before = if fresh_binding.is_some() {
                Some(actual_owned_rows(&pool, &source).await)
            } else {
                None
            };
            let pool_copy = pool.clone();
            let pairs =
                url::form_urlencoded::parse(path.split_once('?').map_or("", |v| v.1).as_bytes())
                    .map(|(a, b)| (a.into_owned(), b.into_owned()))
                    .collect::<Vec<_>>();
            let actual = runtime
                .run(1024, move |work| async move {
                    match read(
                        &pool_copy,
                        &work,
                        &visible,
                        &pairs,
                        "http://localhost",
                        "/api/vod/all/",
                    )
                    .await
                    {
                        Ok(bytes) => Ok(bytes),
                        Err(UnifiedFault::Read(error)) => Err(error),
                        Err(UnifiedFault::View(message)) => {
                            Err(crate::StoreError::StatusBody(500, json!({"error":message})).into())
                        }
                        Err(UnifiedFault::Renderer) => Err(ReadError::SourceUnhandledFault(
                            "unified nonfinite renderer",
                        )),
                    }
                })
                .await;
            let no_write_after = if fresh_binding.is_some() {
                Some(actual_owned_rows(&pool, &source).await)
            } else {
                None
            };
            assert_eq!(no_write_before, no_write_after);
            // Restore before any assertion, including failed comparisons.
            sqlx::query("UPDATE vod_movie SET rating=$1,custom_properties=$2 WHERE id=$3")
                .bind(&saved_rating)
                .bind(&saved_properties)
                .bind(movie_id)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("UPDATE vod_series SET name=$1 WHERE id=$2")
                .bind(&series_name)
                .bind(series_id)
                .execute(&pool)
                .await
                .unwrap();
            let record = match actual {
                Ok(bytes) => {
                    json!({"status":200,"body_sha256":format!("{:x}",Sha256::digest(&bytes)),"json":serde_json::from_slice::<Value>(&bytes).ok()})
                }
                Err(ReadError::Source(crate::StoreError::StatusBody(status, body))) => {
                    let bytes = crate::json_compat::compact_json_bytes(&body).unwrap();
                    json!({"status":status,"body_sha256":format!("{:x}",Sha256::digest(&bytes)),"json":body})
                }
                Err(ReadError::SourceUnhandledFault("unified nonfinite renderer")) => {
                    json!({"renderer_unhandled":"ValueError"})
                }
                Err(error) => json!({"unexpected_error":format!("{error:?}")}),
            };
            let same = if expected["renderer_unhandled"].is_string() {
                record["renderer_unhandled"] == expected["renderer_unhandled"]
            } else {
                record["status"] == expected["status"]
                    && record["body_sha256"] == expected["body_sha256"]
            };
            if !same {
                differences.push(json!({"source_index":index,"path":path,"label":label,"source":expected,"native":record}));
            }
            records.push(json!({"source_index":index,"same":same,"native":record,"fresh_Source_tie_HTTP":fresh_binding,"native_query_no_intervening_writes":no_write_before.as_ref().map(|_|no_write_before==no_write_after)}));
        }
        let drained = runtime.shutdown(Duration::from_secs(5)).await;
        let actual_after = actual_owned_rows(&pool, &source).await;
        let passed = differences.is_empty()
            && records.len() > 90
            && drained
            && actual_before == actual_after;
        let evidence = std::path::PathBuf::from(std::env::var("VOD_NATIVE_EVIDENCE").unwrap());
        std::fs::write(evidence.join("unified-sql.json"),serde_json::to_vec_pretty(&json!({
            "passed":passed,"scope":"Actual Source Factory projection/query corpus vs actual native PG SQL component; permissions/HTTP/planner/resource gates separate",
            "compared":records.len(),"records":records,"differences":differences,"outside_component_not_counted_pass":excluded,
            "physical_workers_drained":drained,"retained_memory_after":runtime.retained_memory_bytes(),
            "source_restoration_reference":source["fingerprints_after"],"actual_native_owned_rows_before":actual_before,"actual_native_owned_rows_after":actual_after,"actual_native_owned_rows_restored":actual_before==actual_after,"performance_verified":false
        })).unwrap()).unwrap();
        pool.close().await;
        assert!(
            passed,
            "Unified SQL differences retained in unified-sql.json"
        );
    }
}

#[cfg(test)]
#[path = "vod_api_unified_phase_tests.rs"]
pub(super) mod phase_tests;
