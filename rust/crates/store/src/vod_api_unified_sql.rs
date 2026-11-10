//! Original Source-shaped UNION/page/count SQL with bounded raw-row guards.
//! Actual Source/currentSQL/raw-inner/guarded EXPLAIN and body gates passed.
//! No general planner, canonical app or fullVOD parity claim follows.
//! Original Dispatcharr authors/contributors retain application credit/AGPL.
use super::{
    vod_api_projection::Visibility,
    vod_api_unified_query::{QueryFault, UnifiedQuery},
};
use sqlx::{postgres::PgArguments, query::Query, Postgres};

#[derive(Clone)]
pub(super) enum Bind {
    Text(String),
    Integer(i64),
}
#[derive(Clone)]
pub(super) struct Statement {
    pub sql: String,
    pub binds: Vec<Bind>,
}
impl Statement {
    pub fn query(&self) -> Query<'_, Postgres, PgArguments> {
        let mut query = sqlx::query(&self.sql).persistent(false);
        for bind in &self.binds {
            query = match bind {
                Bind::Text(value) => query.bind(value.clone()),
                Bind::Integer(value) => query.bind(*value),
            };
        }
        query
    }
    fn bind(&mut self, bind: Bind) -> String {
        self.binds.push(bind);
        format!("${}", self.binds.len())
    }
    fn predicate(&mut self, sql: &str, arguments: Vec<String>) -> Result<String, QueryFault> {
        let mut parts = sql.split('?');
        let mut result = parts.next().unwrap_or("").to_owned();
        for value in arguments {
            // Psycopg rejects text NUL before submitting it. Only retained
            // branch arguments matter; Source clears an excluded branch.
            if value.contains('\0') {
                return Err(QueryFault::View(
                    "PostgreSQL text fields cannot contain NUL (0x00) bytes".into(),
                ));
            }
            result.push_str(&self.bind(Bind::Text(value)));
            result.push_str(parts.next().ok_or(QueryFault::Uncaptured(
                "Invalid internal unified SQL bind shape",
            ))?);
        }
        if parts.next().is_some() {
            return Err(QueryFault::Uncaptured(
                "Invalid internal unified SQL argument count",
            ));
        }
        Ok(result)
    }
    fn limit(&mut self, value: &num_bigint::BigInt) -> String {
        let raw = value.to_string();
        match raw.parse::<i64>() {
            Ok(value) => format!("{}::bigint", self.bind(Bind::Integer(value))),
            // Source Psycopg sends values outside i64 as numeric, then PG's
            // LIMIT coercion raises the captured `bigint out of range`.
            Err(_) => format!("{}::numeric::bigint", self.bind(Bind::Text(raw))),
        }
    }
}
impl UnifiedQuery {
    fn start(&self, visible: &Visibility) -> Result<(Statement, String, String), QueryFault> {
        let mut statement = Statement {
            sql: String::new(),
            binds: Vec::new(),
        };
        let (m, mp, s, sp) = self.predicates(visible);
        let movie = statement.predicate(&m, mp)?;
        let series = statement.predicate(&s, sp)?;
        Ok((statement, movie, series))
    }
    /// Original page SQL inside a materialized boundary; no outer reorder,
    /// join/window or invented kind tie-breaker. Must compare actual plans/IDs.
    pub fn page(&self, visible: &Visibility, row_bytes: usize) -> Result<Statement, QueryFault> {
        let (mut statement, movie, series) = self.start(visible)?;
        let limit = statement.limit(&self.page.size);
        let offset = statement.limit(&self.page.offset);
        let maximum = statement.bind(Bind::Integer(
            i64::try_from(row_bytes).map_err(|_| QueryFault::Capacity)?,
        ));
        let raw = format!(
            r#"
            WITH unified_content AS (
                SELECT movies.id,movies.uuid,movies.name,movies.description,
                    movies.year,movies.rating,movies.genre,
                    movies.duration_secs as duration,movies.created_at,
                    movies.updated_at,movies.custom_properties,movies.logo_id,
                    logo.name as logo_name,logo.url as logo_url,'movie' as content_type
                FROM vod_movie movies
                LEFT JOIN vod_vodlogo logo ON movies.logo_id = logo.id
                WHERE {movie}
                UNION ALL
                SELECT series.id,series.uuid,series.name,series.description,
                    series.year,series.rating,series.genre,NULL as duration,
                    series.created_at,series.updated_at,series.custom_properties,
                    series.logo_id,logo.name as logo_name,logo.url as logo_url,
                    'series' as content_type
                FROM vod_series series
                LEFT JOIN vod_vodlogo logo ON series.logo_id = logo.id
                WHERE {series}
            )
            SELECT * FROM unified_content
            ORDER BY LOWER(name), id
            LIMIT {limit} OFFSET {offset}
        "#
        );
        // Guard expanded text/json bytes, not compressed TOAST storage size.
        // PG executor allocations are not claimed bounded by client credits.
        let measured="512::bigint+COALESCE(octet_length(p.name),0)+COALESCE(octet_length(p.description),0)+COALESCE(octet_length(p.rating),0)+COALESCE(octet_length(p.genre),0)+COALESCE(octet_length(p.custom_properties::text),0)+COALESCE(octet_length(p.logo_name),0)+COALESCE(octet_length(p.logo_url),0)";
        let safe = format!("({measured})<={maximum}");
        let guarded =
            |field: &str| format!("CASE WHEN {safe} THEN p.{field} ELSE NULL END AS {field}");
        let fields = [
            "name",
            "description",
            "rating",
            "genre",
            "logo_name",
            "logo_url",
        ]
        .map(guarded)
        .join(",");
        statement.sql=format!("WITH source_page AS MATERIALIZED ({raw}) SELECT p.id::bigint AS id,p.uuid::text AS uuid,{fields},p.year::bigint AS year,p.duration::bigint AS duration,p.created_at,p.updated_at,p.logo_id::bigint AS logo_id,p.content_type,CASE WHEN {safe} THEN p.custom_properties::text ELSE NULL END AS properties_json,({measured})::bigint AS retained_wire_bytes,NOT({safe}) AS oversized FROM source_page p");
        Ok(statement)
    }
    pub fn count(&self, visible: &Visibility) -> Result<Statement, QueryFault> {
        let (mut statement, movie, series) = self.start(visible)?;
        statement.sql=format!("SELECT COUNT(*)::bigint AS count FROM (SELECT 1 FROM vod_movie movies WHERE {movie} UNION ALL SELECT 1 FROM vod_series series WHERE {series}) as total_count");
        Ok(statement)
    }
}
