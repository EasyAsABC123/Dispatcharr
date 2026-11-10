//! Unverified source-derived XC live readers. Original Dispatcharr credit retained.
use dispatcharr_store::{Store, StoreError, User};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder, Row};

pub(crate) struct Channel {
    pub id: i64,
    pub number: Option<f64>,
    pub name: String,
    pub effective_group: Option<i64>,
    pub logo: Option<i64>,
    pub radio: bool,
    pub catchup: bool,
    pub catchup_days: i32,
    pub adult: bool,
    pub created_micros: i64,
}
pub(crate) struct ReadLimit {
    pub max_rows: usize,
    pub max_metadata: usize,
}
pub(crate) fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"XC read resource capacity is unavailable."}),
    )
}
fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(value)) => *value,
        Some(Value::String(value)) => !value.is_empty(),
        Some(Value::Array(value)) => !value.is_empty(),
        Some(Value::Object(value)) => !value.is_empty(),
        Some(Value::Number(value)) => value.as_f64() != Some(0.0),
    }
}
fn visibility(query: &mut QueryBuilder<'_, Postgres>, user: &User) {
    // Output views deliberately cap admins by level, unlike ordinary lookup.
    query.push(" AND c.user_level<=").push_bind(user.user_level);
    if user.user_level < 10 {
        if truthy(user.custom_properties.get("hide_adult_content")) {
            query.push(" AND NOT c.is_adult");
        }
        query.push(" AND (NOT EXISTS(SELECT 1 FROM accounts_user_channel_profiles up WHERE up.user_id=").push_bind(user.id).push(") OR EXISTS(SELECT 1 FROM accounts_user_channel_profiles up JOIN dispatcharr_channels_channelprofilemembership m ON m.channel_profile_id=up.channelprofile_id WHERE up.user_id=").push_bind(user.id).push(" AND m.channel_id=c.id AND m.enabled))");
    }
}
pub(crate) async fn channels(
    store: &Store,
    user: &User,
    category: Option<i64>,
    limit: &ReadLimit,
) -> Result<Vec<Channel>, StoreError> {
    let mut query=QueryBuilder::<Postgres>::new("SELECT c.id::bigint AS id,COALESCE(o.channel_number,c.channel_number) AS number,COALESCE(o.name,c.name) AS name,COALESCE(o.channel_group_id,c.channel_group_id)::bigint AS effective_group,COALESCE(o.logo_id,c.logo_id)::bigint AS logo,COALESCE(o.is_radio,c.is_radio) AS radio,c.is_catchup,c.catchup_days,c.is_adult,c.created_at FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE NOT c.hidden_from_output");
    visibility(&mut query, user);
    // Source applies category filtering before effective-group annotation.
    if let Some(category) = category {
        query.push(" AND c.channel_group_id=").push_bind(category);
    }
    query
        .push(" ORDER BY COALESCE(o.channel_number,c.channel_number) LIMIT ")
        .push_bind((limit.max_rows + 1) as i64);
    let built = query.build();
    let mut rows = built.fetch(store.pool());
    let mut result = Vec::new();
    let mut bytes = 0usize;
    while let Some(row) = rows.try_next().await? {
        if result.len() >= limit.max_rows {
            return Err(capacity());
        }
        let name: String = row.try_get("name")?;
        bytes = bytes
            .checked_add(name.capacity() + std::mem::size_of::<Channel>())
            .ok_or_else(capacity)?;
        if bytes > limit.max_metadata {
            return Err(capacity());
        }
        result.push(Channel {
            id: row.try_get("id")?,
            number: row.try_get("number")?,
            name,
            effective_group: row.try_get("effective_group")?,
            logo: row.try_get("logo")?,
            radio: row.try_get("radio")?,
            catchup: row.try_get("is_catchup")?,
            catchup_days: row.try_get("catchup_days")?,
            adult: row.try_get("is_adult")?,
            created_micros: row
                .try_get::<chrono::DateTime<chrono::Utc>, _>("created_at")?
                .timestamp_micros(),
        });
    }
    Ok(result)
}
pub(crate) async fn categories(
    store: &Store,
    user: &User,
    limit: &ReadLimit,
) -> Result<Vec<Value>, StoreError> {
    let mut query=QueryBuilder::<Postgres>::new("SELECT g.id::bigint AS id,CASE WHEN octet_length(g.name)<=65536 THEN g.name END AS name FROM dispatcharr_channels_channelgroup g JOIN dispatcharr_channels_channel c ON c.channel_group_id=g.id LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE NOT c.hidden_from_output");
    visibility(&mut query, user);
    query.push(" GROUP BY g.id,g.name ORDER BY MIN(COALESCE(o.channel_number,c.channel_number)) LIMIT ").push_bind((limit.max_rows+1) as i64);
    let built = query.build();
    let mut rows = built.fetch(store.pool());
    let mut result = Vec::new();
    let mut bytes = 0usize;
    while let Some(row) = rows.try_next().await? {
        let name: Option<String> = row.try_get("name")?;
        let name = name.ok_or_else(capacity)?;
        bytes = bytes
            .checked_add(name.capacity() + 256)
            .ok_or_else(capacity)?;
        if result.len() >= limit.max_rows || bytes > limit.max_metadata {
            return Err(capacity());
        }
        result.push(json!({"category_id":row.try_get::<i64,_>("id")?.to_string(),"category_name":name,"parent_id":0}));
    }
    Ok(result)
}
pub(crate) async fn default_group(store: &Store) -> Result<i64, StoreError> {
    // get_or_create_manual() always marks the custom-account relationship,
    // including when the group already exists. Do not hide that side effect.
    // Source get_or_create first reads: existing rows are neither updated nor
    // offered to INSERT (which would consume a sequence value on every GET).
    let existing: Option<i64> = sqlx::query_scalar(
        "SELECT id::bigint FROM dispatcharr_channels_channelgroup WHERE name='Default Group'",
    )
    .fetch_optional(store.pool())
    .await?;
    let group = if let Some(id) = existing {
        id
    } else {
        let created:Option<i64>=sqlx::query_scalar("INSERT INTO dispatcharr_channels_channelgroup(name) VALUES('Default Group') ON CONFLICT(name) DO NOTHING RETURNING id::bigint").fetch_optional(store.pool()).await?;
        if let Some(id) = created {
            id
        } else {
            sqlx::query_scalar("SELECT id::bigint FROM dispatcharr_channels_channelgroup WHERE name='Default Group'").fetch_one(store.pool()).await?
        }
    };
    let account: i64 =
        sqlx::query_scalar("SELECT id::bigint FROM m3u_m3uaccount WHERE name='custom' AND locked")
            .fetch_one(store.pool())
            .await?;
    sqlx::query("INSERT INTO dispatcharr_channels_channelgroupm3uaccount(channel_group_id,m3u_account_id,custom_properties,enabled,auto_channel_sync,auto_sync_channel_start,auto_sync_channel_end,last_seen,is_stale) SELECT $1,$2,'{}'::jsonb,true,false,NULL,NULL,now(),false WHERE NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount WHERE channel_group_id=$1 AND m3u_account_id=$2) ON CONFLICT(channel_group_id,m3u_account_id) DO NOTHING").bind(group).bind(account).execute(store.pool()).await?;
    Ok(group)
}
#[cfg(test)]
#[path = "reader_tests.rs"]
mod tests;
