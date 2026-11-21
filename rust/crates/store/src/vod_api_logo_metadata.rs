//! Read-only Original VOD logo library through the shared VOD runtime.
//! Original Dispatcharr authors and contributors retain full credit.
use super::{
    vod_api_dto::{LogoContext, LogoRow},
    vod_api_http::{json_indent, negotiate, pretty_document},
    vod_api_owned_read::{OwnedConnection, ReadError, ReadRuntime, ReadWork},
    vod_api_pagination::{last_query, ordinary_with_limits, page_url, Page},
    vod_api_read_component::HttpRead,
};
use crate::{AuthService, StoreError};
use axum::http::HeaderMap;
use bytes::Bytes;
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{postgres::PgRow, Postgres, QueryBuilder, Row};
use std::{io::Write, net::SocketAddr, sync::Arc};

const MOVIE_LINK: &str = "EXISTS(SELECT 1 FROM vod_movie m WHERE m.logo_id=l.id)";
const SERIES_LINK: &str = "EXISTS(SELECT 1 FROM vod_series s WHERE s.logo_id=l.id)";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Usage {
    Any,
    Used,
    Unused,
    Movies,
    Series,
}

struct Selection {
    detail: Option<i64>,
    ids: Option<Vec<i64>>,
    name: Option<String>,
    terms: Vec<String>,
    usage: Usage,
    order: Vec<(&'static str, bool)>,
    unpaginated: bool,
}

fn literal_contains(value: &str) -> String {
    format!(
        "%{}%",
        value
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    )
}

fn missing() -> ReadError {
    StoreError::StatusBody(404, json!({"detail":"No VODLogo matches the given query."})).into()
}

impl Selection {
    fn parse(raw_detail: Option<&str>, query: &[(String, String)]) -> Result<Self, ReadError> {
        let detail = raw_detail
            .map(|raw| {
                dispatcharr_ingest_core::source_integer_literal(raw)
                    .ok_or_else(|| ReadError::from(StoreError::NotFound))?
                    .parse::<i64>()
                    .map_err(|_| missing())
            })
            .transpose()?;
        let mut ids = None;
        for (_, value) in query.iter().filter(|(name, _)| name == "ids") {
            if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
                let bucket = ids.get_or_insert_with(Vec::new);
                if let Ok(id) = value.parse() {
                    bucket.push(id);
                }
            }
        }
        let name = last_query(query, "name")
            .filter(|value| !value.is_empty())
            .map(literal_contains);
        let usage = match last_query(query, "used") {
            Some("true") => Usage::Used,
            Some("false") => Usage::Unused,
            Some("movies") => Usage::Movies,
            Some("series") => Usage::Series,
            _ => Usage::Any,
        };
        let search = last_query(query, "search").unwrap_or_default();
        if search
            .chars()
            .any(|c| matches!(c, '\'' | '"' | '\\' | '\0'))
        {
            return Err(ReadError::Unsupported(
                "Quoted/control SearchFilter grammar",
            ));
        }
        let terms: Vec<_> = search
            .split(|c: char| c == ',' || c.is_whitespace())
            .filter(|term| !term.is_empty())
            .map(literal_contains)
            .collect();
        if terms.len() > 32 {
            return Err(ReadError::Capacity);
        }
        let mut order = Vec::new();
        for raw in last_query(query, "ordering").unwrap_or_default().split(',') {
            let raw = raw.trim();
            let (field, descending) = raw
                .strip_prefix('-')
                .map_or((raw, false), |field| (field, true));
            let field = match field {
                "name" => "l.name",
                "id" => "l.id",
                _ => continue,
            };
            order.push((field, descending));
        }
        if order.is_empty() {
            order.push(("l.name", false));
        }
        let unpaginated = last_query(query, "no_pagination")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        Ok(Self {
            detail,
            ids,
            name,
            terms,
            usage,
            order,
            unpaginated,
        })
    }

    fn filters(&self, query: &mut QueryBuilder<'_, Postgres>) {
        query.push(" FROM vod_vodlogo l WHERE TRUE");
        if let Some(id) = self.detail {
            query.push(" AND l.id=").push_bind(id);
        }
        if let Some(ids) = &self.ids {
            query
                .push(" AND l.id::bigint=ANY(")
                .push_bind(ids.clone())
                .push(")");
        }
        if let Some(name) = &self.name {
            query
                .push(" AND UPPER(l.name) LIKE UPPER(")
                .push_bind(name.clone())
                .push(")");
        }
        match self.usage {
            Usage::Any => {}
            Usage::Used => {
                query
                    .push(" AND (")
                    .push(MOVIE_LINK)
                    .push(" OR ")
                    .push(SERIES_LINK)
                    .push(")");
            }
            Usage::Unused => {
                query
                    .push(" AND NOT ")
                    .push(MOVIE_LINK)
                    .push(" AND NOT ")
                    .push(SERIES_LINK);
            }
            Usage::Movies => {
                query.push(" AND ").push(MOVIE_LINK);
            }
            Usage::Series => {
                query.push(" AND ").push(SERIES_LINK);
            }
        }
        for term in &self.terms {
            query
                .push(" AND (UPPER(l.name) LIKE UPPER(")
                .push_bind(term.clone())
                .push(") OR UPPER(l.url) LIKE UPPER(")
                .push_bind(term.clone())
                .push("))");
        }
    }

    fn order(&self, query: &mut QueryBuilder<'_, Postgres>) {
        query.push(" ORDER BY ");
        let mut keys = query.separated(", ");
        for (field, descending) in &self.order {
            keys.push(*field);
            if *descending {
                keys.push_unseparated(" DESC");
            }
        }
    }

    fn rows(&self, row_bytes: i64, page: Option<&Page>) -> QueryBuilder<'static, Postgres> {
        let mut query =
            QueryBuilder::new("WITH selected AS MATERIALIZED (SELECT l.id,l.name,l.url");
        self.filters(&mut query);
        self.order(&mut query);
        if let Some(page) = page {
            query
                .push(" LIMIT ")
                .push_bind(page.size as i64)
                .push(" OFFSET ")
                .push_bind(page.offset as i64);
        }
        query.push(
            r#") SELECT l.id::bigint AS id,
                CASE WHEN budget.fits THEN l.name END AS name,
                CASE WHEN budget.fits THEN l.url END AS url,
                movies.total AS movie_count, series.total AS series_count,
                CASE WHEN budget.fits THEN movies.names END AS movie_names,
                CASE WHEN budget.fits THEN series.names END AS series_names
            FROM selected l
            CROSS JOIN LATERAL (
                SELECT (SELECT COUNT(*) FROM vod_movie m WHERE m.logo_id=l.id) AS total,
                       ARRAY(SELECT m.name::text FROM vod_movie m WHERE m.logo_id=l.id ORDER BY m.name LIMIT 10) AS names
            ) movies
            CROSS JOIN LATERAL (
                SELECT (SELECT COUNT(*) FROM vod_series s WHERE s.logo_id=l.id) AS total,
                       ARRAY(SELECT s.name::text FROM vod_series s WHERE s.logo_id=l.id ORDER BY s.name LIMIT 10) AS names
            ) series
            CROSS JOIN LATERAL (
                SELECT octet_length(l.name)::bigint + octet_length(l.url)::bigint
                    + pg_column_size(movies.names) + pg_column_size(series.names) + 256 <= "#,
        ).push_bind(row_bytes).push(" AS fits) budget");
        self.order(&mut query);
        query
    }
}

fn metadata(row: PgRow, context: &LogoContext<'_>) -> Result<Value, ReadError> {
    let Some(url) = row.try_get::<Option<String>, _>("url")? else {
        return Err(ReadError::Capacity);
    };
    let movie_count = row.try_get("movie_count")?;
    let series_count = row.try_get("series_count")?;
    Ok(LogoRow {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        url,
        movie_count,
        series_count,
        is_used: movie_count != 0 || series_count != 0,
        movie_names: row.try_get("movie_names")?,
        series_names: row.try_get("series_names")?,
    }
    .metadata(context))
}

fn capacity(_: impl std::fmt::Display) -> ReadError {
    ReadError::Capacity
}

async fn render(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    selection: &Selection,
    origin: &str,
    path: &str,
    query: &[(String, String)],
) -> Result<Vec<u8>, ReadError> {
    let mut document = work.document();
    let context = LogoContext {
        absolute_origin: Some(origin),
    };
    let result_page = if selection.detail.is_none() && !selection.unpaginated {
        work.check()?;
        connection.configure(work).await?;
        let mut count = QueryBuilder::new("SELECT COUNT(*)");
        selection.filters(&mut count);
        let count = count
            .build_query_scalar::<i64>()
            .persistent(false)
            .fetch_one(connection.raw())
            .await?;
        let count = u64::try_from(count).map_err(capacity)?;
        let page = ordinary_with_limits(query, count, 100, 1000)
            .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Invalid page."})))?;
        let next = page
            .has_next()
            .then(|| page_url(origin, path, query, Some(page.number + 1)));
        let previous = page.has_previous().then(|| {
            page_url(
                origin,
                path,
                query,
                (page.number > 2).then_some(page.number - 1),
            )
        });
        write!(&mut document, "{{\"count\":{count},\"next\":").map_err(capacity)?;
        serde_json::to_writer(&mut document, &next).map_err(capacity)?;
        document.write_all(b",\"previous\":").map_err(capacity)?;
        serde_json::to_writer(&mut document, &previous).map_err(capacity)?;
        document.write_all(b",\"results\":").map_err(capacity)?;
        Some(page)
    } else {
        None
    };
    connection.configure(work).await?;
    let mut statement = selection.rows(
        i64::try_from(work.row_bytes()).map_err(capacity)?,
        result_page.as_ref(),
    );
    if selection.detail.is_some() {
        let row = statement
            .build()
            .persistent(false)
            .fetch_optional(connection.raw())
            .await?
            .ok_or_else(missing)?;
        serde_json::to_writer(&mut document, &metadata(row, &context)?).map_err(capacity)?;
    } else {
        document.write_all(b"[").map_err(capacity)?;
        let mut rows = statement.build().persistent(false).fetch(connection.raw());
        let mut first = true;
        while let Some(row) = rows.try_next().await? {
            work.check()?;
            if !first {
                document.write_all(b",").map_err(capacity)?;
            }
            first = false;
            serde_json::to_writer(&mut document, &metadata(row, &context)?).map_err(capacity)?;
        }
        document
            .write_all(if result_page.is_some() { b"]}" } else { b"]" })
            .map_err(capacity)?;
    }
    Ok(document.finish())
}

pub(super) async fn dispatch(
    runtime: ReadRuntime,
    auth: AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    http: HttpRead,
    origin: String,
) -> Result<Bytes, ReadError> {
    let fixed = http
        .uri
        .path_and_query()
        .map_or(0, |path| path.as_str().len())
        .checked_add(origin.len())
        .and_then(|total| total.checked_add(1024))
        .ok_or(ReadError::Capacity)?;
    let input = headers.iter().try_fold(fixed, |total, (key, value)| {
        total
            .checked_add(key.as_str().len())
            .and_then(|total| total.checked_add(value.as_bytes().len()))
            .ok_or(ReadError::Capacity)
    })?;
    runtime
        .run_scoped(
            input,
            Some(http.deadline),
            http.scope.clone(),
            move |work| async move {
                negotiate(&headers, &http.uri)?;
                let indent = json_indent(&headers);
                let guard = work.io_guard()?.erased();
                let guard: Arc<dyn Send + Sync> = match http.scope {
                    Some(scope) => Arc::new((guard, scope)),
                    None => guard,
                };
                let user = auth
                    .clone()
                    .with_retained_scope(guard)
                    .account_user(peer, headers, false)
                    .await?;
                if user.user_level < 1 {
                    return Err(StoreError::PermissionDenied.into());
                }
                let query: Vec<_> =
                    url::form_urlencoded::parse(http.uri.query().unwrap_or_default().as_bytes())
                        .map(|(key, value)| (key.into_owned(), value.into_owned()))
                        .collect();
                let selection = Selection::parse(http.raw_detail.as_deref(), &query)?;
                let mut connection = work.connection(auth.store.pool()).await?;
                let data = render(
                    &work,
                    &mut connection,
                    &selection,
                    &origin,
                    http.uri.path(),
                    &query,
                )
                .await;
                let released = connection.finish().await;
                let data = data?;
                released?;
                let data = match indent {
                    Some(indent) => pretty_document(&data, indent, work.document())?,
                    None => data,
                };
                Ok(data)
            },
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(value: &str) -> Vec<(String, String)> {
        url::form_urlencoded::parse(value.as_bytes())
            .into_owned()
            .collect()
    }

    #[test]
    fn normal_logo_filters_and_page_use_limited_ordered_sql() {
        let query = query("ids=4&ids=7&name=50%25_&used=movies&ordering=-id&page=2&page_size=25");
        let selected = Selection::parse(None, &query).unwrap();
        assert_eq!(selected.ids, Some(vec![4, 7]));
        assert_eq!(selected.name.as_deref(), Some(r"%50\%\_%"));
        assert_eq!(selected.usage, Usage::Movies);
        let page = ordinary_with_limits(&query, 80, 100, 1000).unwrap();
        assert_eq!((page.size, page.offset), (25, 25));
        let statement = selected.rows(128_000, Some(&page));
        assert!(statement.sql().contains("l.id::bigint=ANY($1)"));
        assert!(statement.sql().contains(MOVIE_LINK));
        assert!(statement
            .sql()
            .contains("ORDER BY l.id DESC LIMIT $3 OFFSET $4"));
        assert!(statement.sql().ends_with("ORDER BY l.id DESC"));
    }

    #[test]
    fn stock_usage_controls_and_versioned_link_use_original_shapes() {
        for (raw, expected) in [
            ("used=true", Usage::Used),
            ("used=false", Usage::Unused),
            ("used=series", Usage::Series),
            ("used=movies", Usage::Movies),
        ] {
            assert_eq!(Selection::parse(None, &query(raw)).unwrap().usage, expected);
        }
        assert!(
            Selection::parse(None, &query("no_pagination=TRUE"))
                .unwrap()
                .unpaginated
        );
        assert_eq!(
            Selection::parse(Some("41"), &query("ids=41"))
                .unwrap()
                .detail,
            Some(41)
        );
        let logo = LogoRow {
            id: 41,
            name: "Local poster".into(),
            url: "abc".into(),
            movie_count: 1,
            series_count: 1,
            is_used: true,
            movie_names: vec!["A film".into()],
            series_names: vec!["A show".into()],
        }
        .metadata(&LogoContext {
            absolute_origin: Some("https://owned.example"),
        });
        assert_eq!(
            logo["cache_url"],
            "https://owned.example/api/vod/vodlogos/41/cache/?v=90015098"
        );
        assert_eq!(
            logo["item_names"],
            json!(["Movie: A film", "Series: A show"])
        );
    }
}
