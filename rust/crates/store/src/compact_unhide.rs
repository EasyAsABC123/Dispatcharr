//! Same-connection post_save compact hide/unhide from pinned Source signals.
//! Original Dispatcharr authors retain application/algorithm credit.
use crate::{
    auto_sync_config::Group,
    compact_numbering::{Failure, Owner},
    AuthService, StoreError,
};
use futures_util::TryStreamExt;
use serde_json::{Map, Value};
use sqlx::{Connection, PgConnection, Row};
fn group(row: sqlx::postgres::PgRow) -> Result<Group, StoreError> {
    if !row.try_get::<bool, _>("fits")? {
        return Err(Failure::Unsupported.store());
    }
    let properties = crate::auto_sync_config::bounded_properties(row.try_get("properties")?)
        .map_err(|_| Failure::Unsupported.store())?;
    Ok(Group {
        ordinal: 0,
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        start: row
            .try_get::<Option<f64>, _>("start")?
            .filter(|n| *n != 0.0)
            .unwrap_or(1.0),
        end: row.try_get("finish")?,
        properties,
    })
}
const PROJECTION:&str="r.channel_group_id::bigint AS id,g.name,r.auto_sync_channel_start AS start,r.auto_sync_channel_end AS finish,CASE WHEN COALESCE(octet_length(r.custom_properties::text),0)<=65536 THEN r.custom_properties::text END AS properties,COALESCE(octet_length(r.custom_properties::text),0)<=65536 AS fits";
pub(crate) async fn load_relation(
    c: &mut PgConnection,
    account: i64,
    id: i64,
) -> Result<Option<Group>, StoreError> {
    let row=sqlx::query(&format!("SELECT {PROJECTION} FROM dispatcharr_channels_channelgroupm3uaccount r JOIN dispatcharr_channels_channelgroup g ON g.id=r.channel_group_id WHERE r.m3u_account_id=$1 AND r.channel_group_id=$2")).bind(account).bind(id).fetch_optional(c).await?;
    row.map(group).transpose()
}
async fn for_channel(
    c: &mut PgConnection,
    account: i64,
    id: i64,
) -> Result<Option<Group>, StoreError> {
    if let Some(direct) = load_relation(c, account, id).await? {
        return Ok(Some(direct));
    }
    let target = id.to_string();
    let mut chosen = None;
    let sql = format!("SELECT {PROJECTION} FROM dispatcharr_channels_channelgroupm3uaccount r JOIN dispatcharr_channels_channelgroup g ON g.id=r.channel_group_id WHERE r.m3u_account_id=$1");
    let mut rows = sqlx::query(&sql).bind(account).fetch(c);
    // Preserve the Source queryset's actual first matching relation, without
    // imposing ID order or allocating every relation. Drain the physical query
    // before issuing later SQL on this SAME connection.
    while let Some(row) = rows.try_next().await? {
        if chosen.is_some() {
            continue;
        }
        let relation = group(row)?;
        let matches = match relation.properties.get("group_override") {
            Some(Value::String(s)) => s == &target,
            Some(Value::Number(n)) => {
                if n.is_f64() {
                    crate::json_compat::float_string(
                        n.as_f64().ok_or_else(|| Failure::Unsupported.store())?,
                    ) == target
                } else {
                    n.to_string() == target
                }
            }
            _ => false,
        };
        if matches {
            chosen = Some(relation)
        }
    }
    Ok(chosen)
}
pub(crate) async fn saved(
    auth: &AuthService,
    c: &mut PgConnection,
    id: i64,
    data: &Map<String, Value>,
    owner: Owner,
) -> Result<(), StoreError> {
    if data.get("auto_created") != Some(&Value::Bool(true)) {
        return Ok(());
    }
    let (Some(account), Some(group_id)) = (
        data.get("auto_created_by_id").and_then(Value::as_i64),
        data.get("channel_group_id").and_then(Value::as_i64),
    ) else {
        return Ok(());
    };
    // Source signals/get_group_relation use Python truthiness for these FKs;
    // schema-valid manually inserted ID0 is not an eligible model-hook owner.
    if account == 0 || group_id == 0 {
        return Ok(());
    }
    let hidden = data.get("hidden_from_output") == Some(&Value::Bool(true));
    let number = data.get("channel_number").filter(|n| !n.is_null());
    if (hidden && number.is_none()) || (!hidden && number.is_some()) {
        return Ok(());
    }
    let Some(relation) = for_channel(c, account, group_id).await? else {
        return Ok(());
    };
    if !relation
        .properties
        .get("compact_numbering")
        .is_some_and(crate::xc_live_provider::truthy)
    {
        return Ok(());
    }
    if hidden {
        // Source hide signal clears raw number even when an override is pinned;
        // it does NOT acquire the account refresh lock or touch updated_at.
        sqlx::query("UPDATE dispatcharr_channels_channel SET channel_number=NULL WHERE id=$1")
            .bind(id)
            .execute(c)
            .await?;
        return Ok(());
    }
    let pinned:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channeloverride WHERE channel_id=$1 AND channel_number IS NOT NULL)").bind(id).fetch_one(&mut*c).await?;
    if pinned {
        return Ok(());
    }
    let port = auth
        .compact_port()
        .ok_or_else(|| Failure::Unsupported.store())?;
    let lease = match port.acquire(account, owner).await {
        Ok(Some(lease)) => lease,
        // Source signal catches known failures and defers when refresh is held.
        Ok(None) | Err(Failure::Settled) | Err(Failure::Capacity) => return Ok(()),
        Err(error) => return Err(error.store()),
    };
    let work = assign(c, id, &relation).await;
    // Source's model signal catches a known SQL ErrorResponse AFTER its nested
    // atomic rollback. Prove that SAME connection is Ready before continuing.
    let ready = c.ping().await;
    let work = match (work, ready) {
        (Err(StoreError::Database(sqlx::Error::Database(_))), Ok(())) => {
            eprintln!("Compact unhide signal caught a settled SQL failure after rollback");
            Ok(())
        }
        (result, Ok(())) => result,
        (_, Err(error)) => Err(StoreError::Database(error)),
    };
    match lease.release().await {
        Ok(()) | Err(Failure::Settled) => work,
        Err(error) => Err(error.store()),
    }
}
async fn assign(c: &mut PgConnection, id: i64, relation: &Group) -> Result<(), StoreError> {
    let start = crate::auto_sync_compact::integer(relation.start)?;
    let end = relation
        .end
        .filter(|n| *n != 0.0)
        .map(crate::auto_sync_compact::integer)
        .transpose()?;
    // Inside ChannelSerializer.update this is a REAL nested savepoint, not an
    // independent connection/BEGIN that could accidentally commit its caller.
    let mut tx = c.begin().await?;
    let table = format!("native_compact_unhide_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!(
        "CREATE TEMP TABLE {table}(number double precision PRIMARY KEY) ON COMMIT DROP"
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query(&format!("INSERT INTO {table} SELECT channel_number FROM dispatcharr_channels_channeloverride WHERE channel_number IS NOT NULL AND channel_number>=$1 AND($2::double precision IS NULL OR channel_number<=$2) ON CONFLICT DO NOTHING")).bind(start).bind(end).execute(&mut*tx).await?;
    sqlx::query(&format!("INSERT INTO {table} SELECT channel_number FROM dispatcharr_channels_channel WHERE channel_number IS NOT NULL AND id<>$1 AND channel_number>=$2 AND($3::double precision IS NULL OR channel_number<=$3) ON CONFLICT DO NOTHING")).bind(id).bind(start).bind(end).execute(&mut*tx).await?;
    let picked=sqlx::query(&format!("WITH RECURSIVE candidate(n) AS(SELECT $1::double precision UNION ALL SELECT n+1 FROM candidate WHERE EXISTS(SELECT 1 FROM {table} WHERE number=n) AND($2::double precision IS NULL OR n<=$2) AND n+1<>n) SELECT n,EXISTS(SELECT 1 FROM {table} WHERE number=n) AS used FROM candidate WHERE NOT EXISTS(SELECT 1 FROM {table} WHERE number=n) OR n+1=n OR($2::double precision IS NOT NULL AND n>$2) LIMIT 1")).bind(start).bind(end).fetch_optional(&mut*tx).await?.ok_or_else(||Failure::Unsupported.store())?;
    let number: f64 = picked.try_get("n")?;
    if !end.is_some_and(|end| number > end) {
        if !number.is_finite() || picked.try_get::<bool, _>("used")? {
            return Err(Failure::Unsupported.store());
        }
        sqlx::query("UPDATE dispatcharr_channels_channel SET channel_number=$1 WHERE id=$2")
            .bind(number)
            .bind(id)
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query(&format!("DROP TABLE {table}"))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
