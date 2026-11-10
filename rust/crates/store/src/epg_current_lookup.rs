//! Ordered current-programme/file lookup component. No application attachment.
//! Original Dispatcharr authors retain all application and algorithm credit.
use crate::{
    epg_current::{capacity, encode_programme, Reply},
    CurrentFileLookup, CurrentFileOutcome, CurrentFileRequest, Store, StoreError,
};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use serde_json::json;
use sqlx::{postgres::PgRow, Connection, Postgres, QueryBuilder, Row};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

const PROGRAMME_FIELDS: &[&str] = &[
    "id",
    "epg_id",
    "start_time",
    "end_time",
    "title",
    "sub_title",
    "description",
    "tvg_id",
    "program_id",
    "custom_properties",
];
const EPG_FIELDS: &[&str] = &["id", "tvg_id", "name", "icon_url", "epg_source_id"];
const SOURCE_FIELDS: &[&str] = &[
    "id",
    "name",
    "source_type",
    "url",
    "username",
    "password",
    "is_active",
    "file_path",
    "extracted_file_path",
    "refresh_interval",
    "refresh_task_id",
    "custom_properties",
    "priority",
    "status",
    "last_message",
    "created_at",
    "updated_at",
];

fn columns(table: &str, prefix: &str, fields: &[&str]) -> String {
    fields
        .iter()
        .map(|field| format!("{table}.{field} AS {prefix}{field}"))
        .collect::<Vec<_>>()
        .join(",")
}
fn ids_clause<'a>(query: &mut QueryBuilder<'a, Postgres>, ids: &[i64]) {
    let mut values = query.separated(",");
    for id in ids {
        values.push_bind(*id);
    }
    values.push_unseparated(")");
}
fn programme_filter<'a>(query: &mut QueryBuilder<'a, Postgres>, ids: &[i64], now: DateTime<Utc>) {
    query
        .push(" WHERE p.end_time>")
        .push_bind(now)
        .push(" AND p.epg_id IN (SELECT u.id FROM epg_epgdata u WHERE u.id IN (");
    ids_clause(query, ids);
    query.push(") AND p.start_time<=").push_bind(now);
}
fn metadata_filter<'a>(query: &mut QueryBuilder<'a, Postgres>, ids: &[i64]) {
    query.push(" WHERE d.id IN (");
    ids_clause(query, ids);
}
fn integer(row: &PgRow, name: &str) -> Result<Option<i64>, StoreError> {
    row.try_get::<Option<i64>, _>(name)
        .or_else(|_| {
            row.try_get::<Option<i32>, _>(name)
                .map(|v| v.map(i64::from))
        })
        .map_err(Into::into)
}
async fn begin<'a>(
    conn: &'a mut sqlx::PgConnection,
    tables: &'static str,
) -> Result<sqlx::Transaction<'a, Postgres>, StoreError> {
    let mut tx = conn.begin().await?;
    let configured = async {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        // SELECT set_config would pin a repeatable-read snapshot before a
        // relation-lock wait, unlike the original single SELECT statement.
        sqlx::query("SET LOCAL statement_timeout='2000ms'")
            .execute(&mut *tx)
            .await?;
        // Acquire the same phase relations before the first MVCC snapshot.
        // The subsequent bounded preflight and full query share that snapshot.
        sqlx::query(&format!("LOCK TABLE {tables} IN ACCESS SHARE MODE"))
            .execute(&mut *tx)
            .await?;
        Ok::<_, StoreError>(())
    }
    .await;
    if let Err(error) = configured {
        tx.rollback().await?;
        return Err(error);
    }
    Ok(tx)
}
// The bound is checked inside the same snapshot as its full-width SELECT.
// JSON text is measured on PostgreSQL before a potentially large row reaches
// the client. Fourfold aggregate charge reserves wire/DTO bookkeeping room.
async fn guard(
    query: &mut QueryBuilder<'_, Postgres>,
    tx: &mut sqlx::PgConnection,
    bytes: usize,
) -> Result<(), StoreError> {
    let row = query.build().persistent(false).fetch_one(tx).await?;
    let count: i64 = row.try_get("rows")?;
    let total: i64 = row.try_get("bytes")?;
    let largest: i64 = row.try_get("largest")?;
    if count > 50000 || largest > 1024 * 1024 || total < 0 || total as u64 > (bytes / 4) as u64 {
        return Err(capacity());
    }
    Ok(())
}
enum Selected {
    Stored(i64, PgRow),
    Lookup(CurrentFileRequest),
}

impl Store {
    /// Component entrypoint for validated EPG IDs. It preserves the observed
    /// programme-first/metadata-second schedule, then releases PostgreSQL before
    /// any file callback. A supervised task retains caller ownership on abort.
    /// `cache_root` is the original MEDIA_ROOT/cached_epg directory.
    pub async fn current_epg_with_file_lookup(
        &self,
        ids: Vec<i64>,
        now: DateTime<Utc>,
        max_bytes: usize,
        cache_root: PathBuf,
        lookup: Arc<dyn CurrentFileLookup>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Vec<u8>, StoreError> {
        if ids.len() > 50
            || !(2..=128 * 1024 * 1024).contains(&max_bytes)
            || !cache_root.is_absolute()
            || cache_root.as_os_str().len() > 4096
        {
            return Err(capacity());
        }
        let store = self.clone();
        tokio::spawn(async move {
            let mut seen = HashSet::new();
            let ids = ids
                .into_iter()
                .filter(|id| seen.insert(*id))
                .collect::<Vec<_>>();
            let selected = store
                .current_lookup_snapshot(&ids, now, max_bytes, &cache_root)
                .await?;
            let mut reply = Reply::new(50, max_bytes)?;
            for entry in selected {
                match entry {
                    Selected::Stored(id, row) => {
                        let value = encode_programme(row, max_bytes)?;
                        reply.append(&value, "epg_data_id", &json!(id))?
                    }
                    Selected::Lookup(request) => {
                        let id = request.epg_entry_id;
                        match lookup.lookup(request, caller.clone()).await? {
                            CurrentFileOutcome::Found(value) => {
                                reply.append(&value, "epg_data_id", &json!(id))?
                            }
                            CurrentFileOutcome::Missing => {}
                            CurrentFileOutcome::Parsing => {
                                let base = format!("{{\"epg_data_id\":{id}}}");
                                reply.append(base.as_bytes(), "parsing", &json!(true))?
                            }
                        }
                    }
                }
            }
            reply.finish()
        })
        .await
        .map_err(|_| capacity())?
    }
    async fn current_lookup_snapshot(
        &self,
        ids: &[i64],
        now: DateTime<Utc>,
        max_bytes: usize,
        cache_root: &std::path::Path,
    ) -> Result<Vec<Selected>, StoreError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self.pool().acquire().await?;
        let mut programmes = HashMap::new();
        // Source evaluates the ProgramData queryset before its lazy metadata
        // queryset. Separate snapshots allow refreshes between these phases.
        let mut tx = begin(&mut connection, "epg_programdata, epg_epgdata").await?;
        let programme_result = async {
            let from = " FROM epg_programdata p JOIN epg_epgdata e ON e.id=p.epg_id";
            let charge = "coalesce(octet_length(row_to_json(p)::text),0)::bigint+coalesce(octet_length(row_to_json(e)::text),0)::bigint+256";
            let mut preflight = QueryBuilder::<Postgres>::new(format!("SELECT count(*)::bigint AS rows,coalesce(sum({charge}),0)::bigint AS bytes,coalesce(max({charge}),0)::bigint AS largest{from}"));
            programme_filter(&mut preflight, ids, now); guard(&mut preflight, &mut tx, max_bytes).await?;
            let projection = format!("{},{}", columns("p", "", PROGRAMME_FIELDS), columns("e", "e_", EPG_FIELDS));
            let mut query = QueryBuilder::<Postgres>::new(format!("SELECT {projection}{from}"));
            programme_filter(&mut query, ids, now);
            let mut rows = query.build().persistent(false).fetch(&mut *tx);
            while let Some(row) = rows.try_next().await? {
                let id = integer(&row, "epg_id")?.ok_or_else(capacity)?;
                if let std::collections::hash_map::Entry::Vacant(entry) = programmes.entry(id) {
                    // Source retains its first model without running the
                    // serializer yet. Metadata deletion and earlier fallback
                    // side effects must precede later serializer failures.
                    entry.insert(row);
                }
            }
            Ok::<_, StoreError>(())
        }.await;
        tx.rollback().await?;
        programme_result?;
        let mut tx = begin(&mut connection, "epg_epgdata, epg_epgsource").await?;
        let metadata_result = async {
            let from = " FROM epg_epgdata d LEFT JOIN epg_epgsource s ON s.id=d.epg_source_id";
            let charge = "coalesce(octet_length(row_to_json(d)::text),0)::bigint+coalesce(octet_length(row_to_json(s)::text),0)::bigint+256";
            let mut preflight = QueryBuilder::<Postgres>::new(format!("SELECT count(*)::bigint AS rows,coalesce(sum({charge}),0)::bigint AS bytes,coalesce(max({charge}),0)::bigint AS largest{from}"));
            metadata_filter(&mut preflight, ids); guard(&mut preflight, &mut tx, max_bytes).await?;
            let mut query = QueryBuilder::<Postgres>::new(format!("SELECT {},{}{from}", columns("d", "d_", EPG_FIELDS), columns("s", "s_", SOURCE_FIELDS)));
            metadata_filter(&mut query, ids);
            let mut rows = query.build().persistent(false).fetch(&mut *tx);
            let mut selected = Vec::new();
            while let Some(row) = rows.try_next().await? {
                if selected.len() >= 50 { return Err(capacity()); }
                let id = integer(&row, "d_id")?.ok_or_else(capacity)?;
                if let Some(value) = programmes.remove(&id) { selected.push(Selected::Stored(id, value)); continue; }
                let Some(source_id) = integer(&row, "s_id")? else { continue; };
                let source_type = row.try_get::<String, _>("s_source_type")?;
                if matches!(source_type.as_str(), "dummy" | "schedules_direct") { continue; }
                let tvg_id = row.try_get::<Option<String>, _>("d_tvg_id")?.unwrap_or_default();
                if tvg_id.is_empty() { continue; }
                let file_path = row.try_get::<Option<String>, _>("s_file_path")?.map(PathBuf::from);
                let extracted_file_path = row.try_get::<Option<String>, _>("s_extracted_file_path")?.map(PathBuf::from);
                let request = CurrentFileRequest { epg_entry_id: id, source_id, source_type, tvg_id, file_path, extracted_file_path, cache_file_path: cache_root.join(format!("{source_id}.tmp")) };
                selected.push(Selected::Lookup(request));
            }
            Ok::<_, StoreError>(selected)
        }.await;
        tx.rollback().await?;
        // Explicitly release the pool connection before the callback can ask
        // the same pool for a fresh index, including a one-connection pool.
        drop(connection);
        metadata_result
    }
}
