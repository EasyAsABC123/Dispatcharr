//! Same-connection post_save compact hide/unhide from pinned Source signals.
//! Original Dispatcharr authors retain application/algorithm credit.
use crate::{
    auto_sync_config::{Error as ConfigError, Group},
    compact_numbering::{Failure, Owner},
    AuthService, StoreError,
};
use futures_util::TryStreamExt;
use serde_json::{Map, Value};
use sqlx::{Connection, PgConnection, Row};
use std::collections::{BTreeSet, HashMap};
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
    if !is_compact(&relation) {
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
fn is_compact(relation: &Group) -> bool {
    relation
        .properties
        .get("compact_numbering")
        .is_some_and(crate::xc_live_provider::truthy)
}

type Pair = (i64, i64);

struct Assignment {
    account: i64,
    relation: Group,
    channels: Vec<i64>,
}

async fn bulk_assignments(
    c: &mut PgConnection,
    unhide: &[i64],
) -> Result<Vec<Assignment>, StoreError> {
    let mut pairs: Vec<(Pair, Vec<i64>)> = Vec::new();
    let mut pair_order = HashMap::new();
    let mut eligible = sqlx::query_as::<_, (i64, i64, Option<i64>, Option<f64>)>(
        "SELECT c.id::bigint,c.auto_created_by_id::bigint,c.channel_group_id::bigint,o.channel_number
         FROM dispatcharr_channels_channel c
         LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id
         WHERE c.id=ANY($1) AND c.auto_created AND c.auto_created_by_id IS NOT NULL
           AND NOT c.hidden_from_output AND c.channel_number IS NULL",
    )
    .bind(unhide)
    .fetch(&mut *c);
    // Source's unordered queryset determines first-encounter and channel order.
    while let Some((id, account, group, pin)) = eligible.try_next().await? {
        let (Some(group), None) = (group, pin) else {
            continue;
        };
        let pair = (account, group);
        let position = *pair_order.entry(pair).or_insert_with(|| {
            pairs.push((pair, Vec::new()));
            pairs.len() - 1
        });
        pairs[position].1.push(id);
    }
    drop(eligible);
    if pairs.is_empty() {
        return Ok(Vec::new());
    }

    let accounts: Vec<_> = pairs.iter().map(|((account, _), _)| *account).collect();
    let groups: Vec<_> = pairs.iter().map(|((_, group), _)| *group).collect();
    let direct_sql = format!(
        "SELECT r.id::bigint AS relation_id,r.m3u_account_id::bigint AS account_id,{PROJECTION}
         FROM dispatcharr_channels_channelgroupm3uaccount r
         JOIN dispatcharr_channels_channelgroup g ON g.id=r.channel_group_id
         WHERE r.m3u_account_id=ANY($1) AND r.channel_group_id=ANY($2)"
    );
    let mut direct = sqlx::query(&direct_sql)
        .bind(accounts)
        .bind(groups)
        .fetch(&mut *c);
    let mut resolved = HashMap::new();
    while let Some(row) = direct.try_next().await? {
        let account = row.try_get("account_id")?;
        let group_id = row.try_get("id")?;
        let pair = (account, group_id);
        if pair_order.contains_key(&pair) {
            resolved.insert(pair, (row.try_get::<i64, _>("relation_id")?, group(row)?));
        }
    }
    drop(direct);

    let unresolved: BTreeSet<_> = pairs
        .iter()
        .filter_map(|(pair, _)| (!resolved.contains_key(pair)).then_some(pair.0))
        .collect();
    if !unresolved.is_empty() {
        let fallback_sql = format!(
            "SELECT r.id::bigint AS relation_id,r.m3u_account_id::bigint AS account_id,{PROJECTION}
             FROM dispatcharr_channels_channelgroupm3uaccount r
             JOIN dispatcharr_channels_channelgroup g ON g.id=r.channel_group_id
             WHERE r.m3u_account_id=ANY($1)"
        );
        let mut fallback = sqlx::query(&fallback_sql)
            .bind(unresolved.into_iter().collect::<Vec<_>>())
            .fetch(&mut *c);
        let mut overrides = HashMap::new();
        while let Some(row) = fallback.try_next().await? {
            let account = row.try_get("account_id")?;
            let relation_id = row.try_get("relation_id")?;
            let relation = group(row)?;
            // Source bulk unhide uses Python int() and the last match. Single
            // hide deliberately uses the first string-equivalent match below.
            if let Some(target) = override_group(&relation)? {
                let pair = (account, target);
                if pair_order.contains_key(&pair) && !resolved.contains_key(&pair) {
                    overrides.insert(pair, (relation_id, relation));
                }
            }
        }
        resolved.extend(overrides);
    }

    let mut assignments = Vec::new();
    let mut relation_order = HashMap::new();
    for ((account, group), channels) in pairs {
        if let Some((relation_id, relation)) = resolved
            .remove(&(account, group))
            .filter(|(_, relation)| is_compact(relation))
        {
            let assignment = Assignment {
                account,
                relation,
                channels,
            };
            // Distinct target groups can resolve to one relation. Source uses
            // the last bucket for that relation at its first position.
            match relation_order.entry(relation_id) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(assignments.len());
                    assignments.push(assignment);
                }
                std::collections::hash_map::Entry::Occupied(entry) => {
                    assignments[*entry.get()] = assignment;
                }
            }
        }
    }
    Ok(assignments)
}

fn override_group(relation: &Group) -> Result<Option<i64>, StoreError> {
    match relation.properties.get("group_override") {
        // Source skips non-integer strings and collections. Reuse its decimal
        // literal normalization without rendering invalid JSON as error text.
        Some(Value::String(value)) => Ok(dispatcharr_ingest_core::source_integer_literal(value)
            .and_then(|text| text.parse().ok())),
        Some(Value::Bool(_) | Value::Number(_)) => match relation.optional_fk("group_override") {
            Ok(target) => Ok(target),
            Err(ConfigError::Caught(_)) => Ok(None),
            Err(ConfigError::Unsupported) => Err(Failure::Unsupported.store()),
            Err(ConfigError::Store(error)) => Err(error),
        },
        _ => Ok(None),
    }
}

fn group_accounts(assignments: Vec<Assignment>) -> Vec<(i64, Vec<Assignment>)> {
    let mut accounts: Vec<(i64, Vec<Assignment>)> = Vec::new();
    let mut positions = HashMap::new();
    for assignment in assignments {
        let account = assignment.account;
        let position = *positions.entry(account).or_insert_with(|| {
            accounts.push((account, Vec::new()));
            accounts.len() - 1
        });
        accounts[position].1.push(assignment);
    }
    accounts
}

async fn assign_account(
    c: &mut PgConnection,
    assignments: &[Assignment],
) -> Result<(), StoreError> {
    let mut tx = c.begin().await?;
    for assignment in assignments {
        if let Err(error) = allocate(&mut tx, &assignment.channels, &assignment.relation).await {
            tx.rollback().await?;
            return Err(error);
        }
    }
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn bulk_visibility(
    auth: &AuthService,
    c: &mut PgConnection,
    unhide: &[i64],
    hide: &[i64],
    owner: Owner,
) -> Result<(), StoreError> {
    if !unhide.is_empty() {
        let accounts = group_accounts(bulk_assignments(c, unhide).await?);
        if !accounts.is_empty() {
            let port = auth
                .compact_port()
                .ok_or_else(|| Failure::Unsupported.store())?;
            for (account, assignments) in accounts {
                let Some(lease) = port
                    .acquire(account, owner.clone())
                    .await
                    .map_err(Failure::store)?
                else {
                    // A real held refresh lock defers this account to sync.
                    continue;
                };
                let work = assign_account(c, &assignments).await;
                match lease.release().await {
                    Ok(()) | Err(Failure::Settled) => work?,
                    Err(error) => return Err(error.store()),
                }
            }
        }
    }
    if hide.is_empty() {
        return Ok(());
    }
    let candidates = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT id::bigint,auto_created_by_id::bigint,channel_group_id::bigint
         FROM dispatcharr_channels_channel
         WHERE id=ANY($1) AND auto_created_by_id<>0 AND channel_group_id<>0",
    )
    .bind(hide)
    .fetch_all(&mut *c)
    .await?;
    let mut compact = HashMap::new();
    let mut releasable = Vec::new();
    for (id, account, group) in candidates {
        let release = match compact.get(&(account, group)) {
            Some(release) => *release,
            None => {
                let release = for_channel(c, account, group)
                    .await?
                    .as_ref()
                    .is_some_and(is_compact);
                compact.insert((account, group), release);
                release
            }
        };
        if release {
            releasable.push(id);
        }
    }
    if !releasable.is_empty() {
        // No lease, clock, or callback. Existing override pins stay in place.
        sqlx::query("UPDATE dispatcharr_channels_channel SET channel_number=NULL WHERE id=ANY($1)")
            .bind(releasable)
            .execute(c)
            .await?;
    }
    Ok(())
}

async fn assign(c: &mut PgConnection, id: i64, relation: &Group) -> Result<(), StoreError> {
    // The single-save signal retains its own nested atomic unit.
    let mut tx = c.begin().await?;
    allocate(&mut tx, &[id], relation).await?;
    tx.commit().await?;
    Ok(())
}

async fn allocate(c: &mut PgConnection, ids: &[i64], relation: &Group) -> Result<(), StoreError> {
    let start = crate::auto_sync_compact::integer(relation.start)?;
    let end = relation
        .end
        .filter(|n| *n != 0.0)
        .map(crate::auto_sync_compact::integer)
        .transpose()?;
    let table = format!("native_compact_unhide_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!(
        "CREATE TEMP TABLE {table}(number double precision PRIMARY KEY) ON COMMIT DROP"
    ))
    .execute(&mut *c)
    .await?;
    sqlx::query(&format!(
        "INSERT INTO {table} SELECT channel_number FROM dispatcharr_channels_channeloverride
         WHERE channel_number IS NOT NULL AND channel_number>=$1
           AND ($2::double precision IS NULL OR channel_number<=$2) ON CONFLICT DO NOTHING"
    ))
    .bind(start)
    .bind(end)
    .execute(&mut *c)
    .await?;
    sqlx::query(&format!(
        "INSERT INTO {table} SELECT channel_number FROM dispatcharr_channels_channel
         WHERE channel_number IS NOT NULL AND NOT (id=ANY($1)) AND channel_number>=$2
           AND ($3::double precision IS NULL OR channel_number<=$3) ON CONFLICT DO NOTHING"
    ))
    .bind(ids)
    .bind(start)
    .bind(end)
    .execute(&mut *c)
    .await?;

    let pick = format!(
        "WITH RECURSIVE candidate(n) AS (
            SELECT $1::double precision
            UNION ALL SELECT n+1 FROM candidate WHERE EXISTS(SELECT 1 FROM {table} WHERE number=n)
              AND ($2::double precision IS NULL OR n<=$2) AND n+1<>n
         )
         SELECT n,EXISTS(SELECT 1 FROM {table} WHERE number=n) AS used FROM candidate
         WHERE NOT EXISTS(SELECT 1 FROM {table} WHERE number=n) OR n+1=n
           OR ($2::double precision IS NOT NULL AND n>$2) LIMIT 1"
    );
    let mut cursor = start;
    let mut numbers = Vec::new();
    for _ in ids {
        let picked = sqlx::query(&pick)
            .bind(cursor)
            .bind(end)
            .fetch_one(&mut *c)
            .await?;
        let number: f64 = picked.try_get("n")?;
        if end.is_some_and(|end| number > end) {
            break;
        }
        if !number.is_finite() || picked.try_get::<bool, _>("used")? {
            return Err(Failure::Unsupported.store());
        }
        numbers.push(number);
        if numbers.len() < ids.len() {
            // Advancing from the smallest free number is equivalent to adding
            // it to the reservation set and searching again from the start.
            cursor = number + 1.0;
            if cursor == number {
                return Err(Failure::Unsupported.store());
            }
        }
    }
    for (ids, numbers) in ids[..numbers.len()].chunks(100).zip(numbers.chunks(100)) {
        sqlx::query(
            "UPDATE dispatcharr_channels_channel c SET channel_number=p.number
             FROM unnest($1::bigint[],$2::double precision[]) AS p(id, number) WHERE c.id=p.id",
        )
        .bind(ids)
        .bind(numbers)
        .execute(&mut *c)
        .await?;
    }
    sqlx::query(&format!("DROP TABLE {table}"))
        .execute(c)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn relation(id: i64, override_value: Value) -> Group {
        Group {
            ordinal: 0,
            id,
            name: String::new(),
            start: 1.0,
            end: None,
            properties: Map::from_iter([("group_override".into(), override_value)]),
        }
    }

    #[test]
    fn bulk_override_group_uses_source_int_conversion() {
        for (value, expected) in [
            (json!("  +00_21 "), Some(21)),
            (json!(21.875), Some(21)),
            (json!(true), Some(1)),
            (json!(0), None),
            (json!("0"), Some(0)),
            (json!("21.875"), None),
            (json!([21]), None),
            (json!("18446744073709551616"), None),
        ] {
            assert_eq!(override_group(&relation(1, value)).unwrap(), expected);
        }
    }

    #[test]
    fn account_grouping_preserves_first_encounter_and_relation_order() {
        let assignments = [(2, 7), (1, 9), (2, 5)]
            .into_iter()
            .map(|(account, id)| Assignment {
                account,
                relation: relation(id, Value::Null),
                channels: vec![id],
            })
            .collect();
        let accounts = group_accounts(assignments);
        assert_eq!(
            accounts
                .iter()
                .map(|(account, group)| {
                    (
                        *account,
                        group
                            .iter()
                            .map(|item| item.relation.id)
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
            [(2, vec![7, 5]), (1, vec![9])]
        );
    }
}
