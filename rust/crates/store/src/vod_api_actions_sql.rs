//! Bounded Source-shaped provider/action SQL and owned raw snapshots.
//! Actual Source/native component gates passed; app/browser acceptance is
//! separate. Original Dispatcharr authors retain application/algorithm credit.
use super::vod_api_actions_dto::RelationKind;
use super::vod_api_owned_read::{OwnedConnection, ReadError, ReadWork};
use futures_util::TryStreamExt;
use serde_json::Value;
use sqlx::Row;

/// Owned strings break SQLx shared protocol-buffer retention. During cursor
/// consumption one additional guarded PgRow is allowed; no decoded JSON tree
/// is retained for the entire unpaginated list.
pub(super) struct RawRows {
    rows: Vec<String>,
}
impl RawRows {
    pub(super) fn into_rows(self) -> impl Iterator<Item = String> {
        self.rows.into_iter()
    }
}
/// JSON payload strings are transfer-bounded before decode; sum actual String
/// capacities + vector allocation + fixed per-row overhead before retaining.
/// Exceeding capacity fails explicitly; it never truncates a successful array.
enum Key<'a> {
    One(i64),
    Many(&'a [i64]),
}
pub(super) async fn snapshot(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    source: &str,
    id: i64,
    maximum: usize,
) -> Result<RawRows, ReadError> {
    snapshot_key(work, connection, source, Key::One(id), maximum).await
}
async fn snapshot_key(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    source: &str,
    key: Key<'_>,
    maximum: usize,
) -> Result<RawRows, ReadError> {
    work.check()?;
    connection.configure(work).await?;
    let limit = i64::try_from(work.row_bytes()).map_err(|_| ReadError::Capacity)?;
    // Native server-side JSON temporary allocations remain in PostgreSQL's
    // service scope. Guard prevents oversized payload transfer to this process.
    // The materialized source plan must pass Source order/EXPLAIN schedule
    // gates; no acceptance can be inferred from query construction alone.
    let sql=format!("WITH source AS MATERIALIZED ({source}) SELECT octet_length(payload::text)>$2 AS oversized,CASE WHEN octet_length(payload::text)<=$2 THEN payload::text ELSE NULL END AS payload FROM source");
    let mut retained = 0usize;
    let mut rows: Vec<String> = Vec::new();
    {
        let query = sqlx::query(&sql).persistent(false);
        let query = match key {
            Key::One(id) => query.bind(id),
            Key::Many(ids) => query.bind(ids),
        };
        let mut cursor = query.bind(limit).fetch(connection.raw());
        while let Some(row) = cursor.try_next().await? {
            work.check()?;
            if row.try_get::<bool, _>("oversized")? {
                return Err(ReadError::Capacity);
            }
            let text = row.try_get::<String, _>("payload")?;
            if text.len() > work.row_bytes() {
                return Err(ReadError::Capacity);
            }
            let bytes = text
                .capacity()
                .checked_add(256)
                .ok_or(ReadError::Capacity)?;
            let next = retained.checked_add(bytes).ok_or(ReadError::Capacity)?;
            if rows.len() == rows.capacity() {
                let count = rows
                    .capacity()
                    .max(8)
                    .checked_mul(2)
                    .ok_or(ReadError::Capacity)?;
                let allocation = count
                    .checked_mul(std::mem::size_of::<String>())
                    .ok_or(ReadError::Capacity)?;
                if next.checked_add(allocation).ok_or(ReadError::Capacity)? > maximum {
                    return Err(ReadError::Capacity);
                }
                rows.try_reserve_exact(count - rows.len())
                    .map_err(|_| ReadError::Capacity)?;
            }
            let allocation = rows
                .capacity()
                .checked_mul(std::mem::size_of::<String>())
                .ok_or(ReadError::Capacity)?;
            if next.checked_add(allocation).ok_or(ReadError::Capacity)? > maximum {
                return Err(ReadError::Capacity);
            }
            rows.push(text);
            retained = next;
        }
    }
    #[cfg(test)]
    super::vod_api_actions_schedule::observe(source).await?;
    Ok(RawRows { rows })
}
pub(super) async fn relation_snapshots(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    kind: RelationKind,
    content_id: i64,
) -> Result<RawRows, ReadError> {
    let source = match kind {
        RelationKind::Movie => MOVIE_SOURCE,
        RelationKind::Series => SERIES_SOURCE,
        RelationKind::Episode => EPISODE_SOURCE,
    };
    snapshot(work, connection, source, content_id, work.work_bytes() / 16).await
}
pub(super) async fn episode_snapshots(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    series_id: i64,
) -> Result<RawRows, ReadError> {
    snapshot(work,connection,"SELECT to_jsonb(e) AS payload FROM vod_episode e WHERE e.series_id=$1 ORDER BY e.season_number,e.episode_number",series_id,work.work_bytes()/16).await
}
#[derive(Clone, Copy)]
pub(super) enum AccountRows {
    Profiles,
    Filters,
    Groups,
    StreamCounts,
}
pub(super) async fn account_snapshots(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    kind: AccountRows,
    account_id: i64,
) -> Result<RawRows, ReadError> {
    let source=match kind {
        AccountRows::Profiles=>"SELECT to_jsonb(p) AS payload FROM m3u_m3uaccountprofile p WHERE p.m3u_account_id=$1",
        // Source Python stable-sorts filter rows AFTER model retrieval; this
        // query therefore preserves raw scan order. Sort decoded order field
        // stably later, rather than adding an SQL tie-breaker.
        AccountRows::Filters=>"SELECT to_jsonb(f) AS payload FROM m3u_m3ufilter f WHERE f.m3u_account_id=$1",
        AccountRows::Groups=>"SELECT to_jsonb(g) AS payload FROM dispatcharr_channels_channelgroupm3uaccount g WHERE g.m3u_account_id=$1",
        AccountRows::StreamCounts=>"SELECT jsonb_build_object('group',s.channel_group_id,'count',COUNT(*)) AS payload FROM dispatcharr_channels_stream s WHERE s.m3u_account_id=$1 GROUP BY s.channel_group_id",
    };
    snapshot(work, connection, source, account_id, work.work_bytes() / 16).await
}
pub(super) fn decode(work: &ReadWork, text: &str) -> Result<Value, ReadError> {
    let value: Value = serde_json::from_str(text).map_err(|_| ReadError::WorkerFailed)?;
    if super::vod_api_actions_account::heap(&value)? > work.work_bytes() / 16 {
        return Err(ReadError::Capacity);
    }
    Ok(value)
}

/// Source prefetches active provider relations for ALL selected episodes before
/// serializing the first episode. Preserve that snapshot phase; per-episode
/// later SELECTs would expose updates Source never sees in this response.
pub(super) async fn prefetched_episodes(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    ids: &[i64],
) -> Result<RawRows, ReadError> {
    let source = EPISODE_PREFETCH_SOURCE;
    snapshot_key(
        work,
        connection,
        source,
        Key::Many(ids),
        work.work_bytes() / 16,
    )
    .await
}

// Source SELECT width and join projection are retained BEFORE JSON encoding.
// Constants derive from actual Factory SQL, with aliases only to disambiguate
// duplicate field names. No invented ORDER BY or identifier tie-breaker.
const MOVIE_SOURCE: &str = r#"WITH action_source AS MATERIALIZED (SELECT "vod_m3umovierelation"."id" AS "x0_id", "vod_m3umovierelation"."m3u_account_id" AS "x0_m3u_account_id", "vod_m3umovierelation"."movie_id" AS "x0_movie_id", "vod_m3umovierelation"."category_id" AS "x0_category_id", "vod_m3umovierelation"."stream_id" AS "x0_stream_id", "vod_m3umovierelation"."container_extension" AS "x0_container_extension", "vod_m3umovierelation"."custom_properties" AS "x0_custom_properties", "vod_m3umovierelation"."created_at" AS "x0_created_at", "vod_m3umovierelation"."updated_at" AS "x0_updated_at", "vod_m3umovierelation"."last_advanced_refresh" AS "x0_last_advanced_refresh", "vod_m3umovierelation"."last_seen" AS "x0_last_seen", "m3u_m3uaccount"."id" AS "x1_id", "m3u_m3uaccount"."name" AS "x1_name", "m3u_m3uaccount"."server_url" AS "x1_server_url", "m3u_m3uaccount"."file_path" AS "x1_file_path", "m3u_m3uaccount"."server_group_id" AS "x1_server_group_id", "m3u_m3uaccount"."max_streams" AS "x1_max_streams", "m3u_m3uaccount"."is_active" AS "x1_is_active", "m3u_m3uaccount"."created_at" AS "x1_created_at", "m3u_m3uaccount"."updated_at" AS "x1_updated_at", "m3u_m3uaccount"."status" AS "x1_status", "m3u_m3uaccount"."last_message" AS "x1_last_message", "m3u_m3uaccount"."user_agent_id" AS "x1_user_agent_id", "m3u_m3uaccount"."locked" AS "x1_locked", "m3u_m3uaccount"."stream_profile_id" AS "x1_stream_profile_id", "m3u_m3uaccount"."account_type" AS "x1_account_type", "m3u_m3uaccount"."username" AS "x1_username", "m3u_m3uaccount"."password" AS "x1_password", "m3u_m3uaccount"."custom_properties" AS "x1_custom_properties", "m3u_m3uaccount"."refresh_interval" AS "x1_refresh_interval", "m3u_m3uaccount"."refresh_task_id" AS "x1_refresh_task_id", "m3u_m3uaccount"."stale_stream_days" AS "x1_stale_stream_days", "m3u_m3uaccount"."priority" AS "x1_priority", "vod_vodcategory"."id" AS "x2_id", "vod_vodcategory"."name" AS "x2_name", "vod_vodcategory"."category_type" AS "x2_category_type", "vod_vodcategory"."created_at" AS "x2_created_at", "vod_vodcategory"."updated_at" AS "x2_updated_at" FROM "vod_m3umovierelation" INNER JOIN "m3u_m3uaccount" ON ("vod_m3umovierelation"."m3u_account_id" = "m3u_m3uaccount"."id") LEFT OUTER JOIN "vod_vodcategory" ON ("vod_m3umovierelation"."category_id" = "vod_vodcategory"."id") WHERE ("m3u_m3uaccount"."is_active" AND "vod_m3umovierelation"."movie_id" = $1)) SELECT jsonb_build_object('relation',jsonb_build_object('id', "x0_id", 'm3u_account_id', "x0_m3u_account_id", 'movie_id', "x0_movie_id", 'category_id', "x0_category_id", 'stream_id', "x0_stream_id", 'container_extension', "x0_container_extension", 'custom_properties', "x0_custom_properties", 'created_at', "x0_created_at", 'updated_at', "x0_updated_at", 'last_advanced_refresh', "x0_last_advanced_refresh", 'last_seen', "x0_last_seen"),'account',jsonb_build_object('id', "x1_id", 'name', "x1_name", 'server_url', "x1_server_url", 'file_path', "x1_file_path", 'server_group_id', "x1_server_group_id", 'max_streams', "x1_max_streams", 'is_active', "x1_is_active", 'created_at', "x1_created_at", 'updated_at', "x1_updated_at", 'status', "x1_status", 'last_message', "x1_last_message", 'user_agent_id', "x1_user_agent_id", 'locked', "x1_locked", 'stream_profile_id', "x1_stream_profile_id", 'account_type', "x1_account_type", 'username', "x1_username", 'password', "x1_password", 'custom_properties', "x1_custom_properties", 'refresh_interval', "x1_refresh_interval", 'refresh_task_id', "x1_refresh_task_id", 'stale_stream_days', "x1_stale_stream_days", 'priority', "x1_priority"),'category',CASE WHEN "x2_id" IS NULL THEN NULL ELSE jsonb_build_object('id', "x2_id", 'name', "x2_name", 'category_type', "x2_category_type", 'created_at', "x2_created_at", 'updated_at', "x2_updated_at") END) AS payload FROM action_source"#;
const SERIES_SOURCE: &str = r#"WITH action_source AS MATERIALIZED (SELECT "vod_m3useriesrelation"."id" AS "x0_id", "vod_m3useriesrelation"."m3u_account_id" AS "x0_m3u_account_id", "vod_m3useriesrelation"."series_id" AS "x0_series_id", "vod_m3useriesrelation"."category_id" AS "x0_category_id", "vod_m3useriesrelation"."external_series_id" AS "x0_external_series_id", "vod_m3useriesrelation"."custom_properties" AS "x0_custom_properties", "vod_m3useriesrelation"."created_at" AS "x0_created_at", "vod_m3useriesrelation"."updated_at" AS "x0_updated_at", "vod_m3useriesrelation"."last_episode_refresh" AS "x0_last_episode_refresh", "vod_m3useriesrelation"."last_seen" AS "x0_last_seen", "m3u_m3uaccount"."id" AS "x1_id", "m3u_m3uaccount"."name" AS "x1_name", "m3u_m3uaccount"."server_url" AS "x1_server_url", "m3u_m3uaccount"."file_path" AS "x1_file_path", "m3u_m3uaccount"."server_group_id" AS "x1_server_group_id", "m3u_m3uaccount"."max_streams" AS "x1_max_streams", "m3u_m3uaccount"."is_active" AS "x1_is_active", "m3u_m3uaccount"."created_at" AS "x1_created_at", "m3u_m3uaccount"."updated_at" AS "x1_updated_at", "m3u_m3uaccount"."status" AS "x1_status", "m3u_m3uaccount"."last_message" AS "x1_last_message", "m3u_m3uaccount"."user_agent_id" AS "x1_user_agent_id", "m3u_m3uaccount"."locked" AS "x1_locked", "m3u_m3uaccount"."stream_profile_id" AS "x1_stream_profile_id", "m3u_m3uaccount"."account_type" AS "x1_account_type", "m3u_m3uaccount"."username" AS "x1_username", "m3u_m3uaccount"."password" AS "x1_password", "m3u_m3uaccount"."custom_properties" AS "x1_custom_properties", "m3u_m3uaccount"."refresh_interval" AS "x1_refresh_interval", "m3u_m3uaccount"."refresh_task_id" AS "x1_refresh_task_id", "m3u_m3uaccount"."stale_stream_days" AS "x1_stale_stream_days", "m3u_m3uaccount"."priority" AS "x1_priority", "vod_vodcategory"."id" AS "x2_id", "vod_vodcategory"."name" AS "x2_name", "vod_vodcategory"."category_type" AS "x2_category_type", "vod_vodcategory"."created_at" AS "x2_created_at", "vod_vodcategory"."updated_at" AS "x2_updated_at" FROM "vod_m3useriesrelation" INNER JOIN "m3u_m3uaccount" ON ("vod_m3useriesrelation"."m3u_account_id" = "m3u_m3uaccount"."id") LEFT OUTER JOIN "vod_vodcategory" ON ("vod_m3useriesrelation"."category_id" = "vod_vodcategory"."id") WHERE ("m3u_m3uaccount"."is_active" AND "vod_m3useriesrelation"."series_id" = $1)) SELECT jsonb_build_object('relation',jsonb_build_object('id', "x0_id", 'm3u_account_id', "x0_m3u_account_id", 'series_id', "x0_series_id", 'category_id', "x0_category_id", 'external_series_id', "x0_external_series_id", 'custom_properties', "x0_custom_properties", 'created_at', "x0_created_at", 'updated_at', "x0_updated_at", 'last_episode_refresh', "x0_last_episode_refresh", 'last_seen', "x0_last_seen"),'account',jsonb_build_object('id', "x1_id", 'name', "x1_name", 'server_url', "x1_server_url", 'file_path', "x1_file_path", 'server_group_id', "x1_server_group_id", 'max_streams', "x1_max_streams", 'is_active', "x1_is_active", 'created_at', "x1_created_at", 'updated_at', "x1_updated_at", 'status', "x1_status", 'last_message', "x1_last_message", 'user_agent_id', "x1_user_agent_id", 'locked', "x1_locked", 'stream_profile_id', "x1_stream_profile_id", 'account_type', "x1_account_type", 'username', "x1_username", 'password', "x1_password", 'custom_properties', "x1_custom_properties", 'refresh_interval', "x1_refresh_interval", 'refresh_task_id', "x1_refresh_task_id", 'stale_stream_days', "x1_stale_stream_days", 'priority', "x1_priority"),'category',CASE WHEN "x2_id" IS NULL THEN NULL ELSE jsonb_build_object('id', "x2_id", 'name', "x2_name", 'category_type', "x2_category_type", 'created_at', "x2_created_at", 'updated_at', "x2_updated_at") END) AS payload FROM action_source"#;
const EPISODE_PREFETCH_SOURCE: &str = r#"WITH action_source AS MATERIALIZED (SELECT "vod_m3uepisoderelation"."id" AS "x0_id", "vod_m3uepisoderelation"."m3u_account_id" AS "x0_m3u_account_id", "vod_m3uepisoderelation"."episode_id" AS "x0_episode_id", "vod_m3uepisoderelation"."series_relation_id" AS "x0_series_relation_id", "vod_m3uepisoderelation"."stream_id" AS "x0_stream_id", "vod_m3uepisoderelation"."container_extension" AS "x0_container_extension", "vod_m3uepisoderelation"."custom_properties" AS "x0_custom_properties", "vod_m3uepisoderelation"."created_at" AS "x0_created_at", "vod_m3uepisoderelation"."updated_at" AS "x0_updated_at", "vod_m3uepisoderelation"."last_seen" AS "x0_last_seen", "m3u_m3uaccount"."id" AS "x1_id", "m3u_m3uaccount"."name" AS "x1_name", "m3u_m3uaccount"."server_url" AS "x1_server_url", "m3u_m3uaccount"."file_path" AS "x1_file_path", "m3u_m3uaccount"."server_group_id" AS "x1_server_group_id", "m3u_m3uaccount"."max_streams" AS "x1_max_streams", "m3u_m3uaccount"."is_active" AS "x1_is_active", "m3u_m3uaccount"."created_at" AS "x1_created_at", "m3u_m3uaccount"."updated_at" AS "x1_updated_at", "m3u_m3uaccount"."status" AS "x1_status", "m3u_m3uaccount"."last_message" AS "x1_last_message", "m3u_m3uaccount"."user_agent_id" AS "x1_user_agent_id", "m3u_m3uaccount"."locked" AS "x1_locked", "m3u_m3uaccount"."stream_profile_id" AS "x1_stream_profile_id", "m3u_m3uaccount"."account_type" AS "x1_account_type", "m3u_m3uaccount"."username" AS "x1_username", "m3u_m3uaccount"."password" AS "x1_password", "m3u_m3uaccount"."custom_properties" AS "x1_custom_properties", "m3u_m3uaccount"."refresh_interval" AS "x1_refresh_interval", "m3u_m3uaccount"."refresh_task_id" AS "x1_refresh_task_id", "m3u_m3uaccount"."stale_stream_days" AS "x1_stale_stream_days", "m3u_m3uaccount"."priority" AS "x1_priority" FROM "vod_m3uepisoderelation" INNER JOIN "m3u_m3uaccount" ON ("vod_m3uepisoderelation"."m3u_account_id" = "m3u_m3uaccount"."id") WHERE ("m3u_m3uaccount"."is_active" AND "vod_m3uepisoderelation"."episode_id" = ANY($1))) SELECT jsonb_build_object('relation',jsonb_build_object('id', "x0_id", 'm3u_account_id', "x0_m3u_account_id", 'episode_id', "x0_episode_id", 'series_relation_id', "x0_series_relation_id", 'stream_id', "x0_stream_id", 'container_extension', "x0_container_extension", 'custom_properties', "x0_custom_properties", 'created_at', "x0_created_at", 'updated_at', "x0_updated_at", 'last_seen', "x0_last_seen"),'account',jsonb_build_object('id', "x1_id", 'name', "x1_name", 'server_url', "x1_server_url", 'file_path', "x1_file_path", 'server_group_id', "x1_server_group_id", 'max_streams', "x1_max_streams", 'is_active', "x1_is_active", 'created_at', "x1_created_at", 'updated_at', "x1_updated_at", 'status', "x1_status", 'last_message', "x1_last_message", 'user_agent_id', "x1_user_agent_id", 'locked', "x1_locked", 'stream_profile_id', "x1_stream_profile_id", 'account_type', "x1_account_type", 'username', "x1_username", 'password', "x1_password", 'custom_properties', "x1_custom_properties", 'refresh_interval', "x1_refresh_interval", 'refresh_task_id', "x1_refresh_task_id", 'stale_stream_days', "x1_stale_stream_days", 'priority', "x1_priority"),'category',NULL) AS payload FROM action_source"#;
const EPISODE_SOURCE: &str = r#"WITH action_source AS MATERIALIZED (SELECT "vod_m3uepisoderelation"."id" AS "x0_id", "vod_m3uepisoderelation"."m3u_account_id" AS "x0_m3u_account_id", "vod_m3uepisoderelation"."episode_id" AS "x0_episode_id", "vod_m3uepisoderelation"."series_relation_id" AS "x0_series_relation_id", "vod_m3uepisoderelation"."stream_id" AS "x0_stream_id", "vod_m3uepisoderelation"."container_extension" AS "x0_container_extension", "vod_m3uepisoderelation"."custom_properties" AS "x0_custom_properties", "vod_m3uepisoderelation"."created_at" AS "x0_created_at", "vod_m3uepisoderelation"."updated_at" AS "x0_updated_at", "vod_m3uepisoderelation"."last_seen" AS "x0_last_seen", "m3u_m3uaccount"."id" AS "x1_id", "m3u_m3uaccount"."name" AS "x1_name", "m3u_m3uaccount"."server_url" AS "x1_server_url", "m3u_m3uaccount"."file_path" AS "x1_file_path", "m3u_m3uaccount"."server_group_id" AS "x1_server_group_id", "m3u_m3uaccount"."max_streams" AS "x1_max_streams", "m3u_m3uaccount"."is_active" AS "x1_is_active", "m3u_m3uaccount"."created_at" AS "x1_created_at", "m3u_m3uaccount"."updated_at" AS "x1_updated_at", "m3u_m3uaccount"."status" AS "x1_status", "m3u_m3uaccount"."last_message" AS "x1_last_message", "m3u_m3uaccount"."user_agent_id" AS "x1_user_agent_id", "m3u_m3uaccount"."locked" AS "x1_locked", "m3u_m3uaccount"."stream_profile_id" AS "x1_stream_profile_id", "m3u_m3uaccount"."account_type" AS "x1_account_type", "m3u_m3uaccount"."username" AS "x1_username", "m3u_m3uaccount"."password" AS "x1_password", "m3u_m3uaccount"."custom_properties" AS "x1_custom_properties", "m3u_m3uaccount"."refresh_interval" AS "x1_refresh_interval", "m3u_m3uaccount"."refresh_task_id" AS "x1_refresh_task_id", "m3u_m3uaccount"."stale_stream_days" AS "x1_stale_stream_days", "m3u_m3uaccount"."priority" AS "x1_priority" FROM "vod_m3uepisoderelation" INNER JOIN "m3u_m3uaccount" ON ("vod_m3uepisoderelation"."m3u_account_id" = "m3u_m3uaccount"."id") WHERE ("m3u_m3uaccount"."is_active" AND "vod_m3uepisoderelation"."episode_id" = $1)) SELECT jsonb_build_object('relation',jsonb_build_object('id', "x0_id", 'm3u_account_id', "x0_m3u_account_id", 'episode_id', "x0_episode_id", 'series_relation_id', "x0_series_relation_id", 'stream_id', "x0_stream_id", 'container_extension', "x0_container_extension", 'custom_properties', "x0_custom_properties", 'created_at', "x0_created_at", 'updated_at', "x0_updated_at", 'last_seen', "x0_last_seen"),'account',jsonb_build_object('id', "x1_id", 'name', "x1_name", 'server_url', "x1_server_url", 'file_path', "x1_file_path", 'server_group_id', "x1_server_group_id", 'max_streams', "x1_max_streams", 'is_active', "x1_is_active", 'created_at', "x1_created_at", 'updated_at', "x1_updated_at", 'status', "x1_status", 'last_message', "x1_last_message", 'user_agent_id', "x1_user_agent_id", 'locked', "x1_locked", 'stream_profile_id', "x1_stream_profile_id", 'account_type', "x1_account_type", 'username', "x1_username", 'password', "x1_password", 'custom_properties', "x1_custom_properties", 'refresh_interval', "x1_refresh_interval", 'refresh_task_id', "x1_refresh_task_id", 'stale_stream_days', "x1_stale_stream_days", 'priority', "x1_priority"),'category',NULL) AS payload FROM action_source"#;
