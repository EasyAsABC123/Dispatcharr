//! Bounded fresh account/category projections preserving Source query phases.
//! Actual Source/native component gates passed; app/browser acceptance is
//! separate. Original Dispatcharr authors retain application/algorithm credit.
use super::{
    vod_api_actions_dto::{self, AccountMetadata},
    vod_api_actions_sql::{account_snapshots, decode, snapshot, AccountRows},
    vod_api_owned_read::{OwnedConnection, ReadError, ReadWork},
};
use serde_json::{json, Value};
use std::collections::HashMap;
fn capacity() -> ReadError {
    ReadError::Capacity
}
fn id(raw: &Value, key: &str) -> Result<i64, ReadError> {
    raw[key].as_i64().ok_or(ReadError::WorkerFailed)
}
fn fields(raw: &Value, names: &[&str]) -> Value {
    Value::Object(
        names
            .iter()
            .map(|name| ((*name).into(), raw[*name].clone()))
            .collect(),
    )
}
fn date(raw: &Value) -> Result<Option<chrono::DateTime<chrono::Utc>>, ReadError> {
    if raw.is_null() {
        Ok(None)
    } else {
        chrono::DateTime::parse_from_rfc3339(raw.as_str().ok_or(ReadError::WorkerFailed)?)
            .map(|date| Some(date.with_timezone(&chrono::Utc)))
            .map_err(|_| ReadError::WorkerFailed)
    }
}
fn timestamp(raw: &Value, seconds: bool) -> Result<Value, ReadError> {
    Ok(match date(raw)? {
        None => Value::Null,
        Some(date) => {
            if seconds || date.timestamp_subsec_micros() == 0 {
                date.format("%Y-%m-%dT%H:%M:%SZ").to_string().into()
            } else {
                date.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string().into()
            }
        }
    })
}
/// Conservative retained heap charge, including spare vector storage and map
/// buckets. A single guarded row decode is an independent bounded allowance.
pub(super) fn heap(value: &Value) -> Result<usize, ReadError> {
    match value {
        Value::String(text) => Ok(text.capacity()),
        Value::Array(rows) => rows.iter().try_fold(
            rows.capacity()
                .checked_mul(std::mem::size_of::<Value>())
                .ok_or_else(capacity)?,
            |total, row| total.checked_add(heap(row)?).ok_or_else(capacity),
        ),
        Value::Object(fields) => {
            let buckets = fields
                .len()
                .max(1)
                .checked_next_power_of_two()
                .and_then(|n| n.checked_mul(256))
                .ok_or_else(capacity)?;
            fields.iter().try_fold(buckets, |total, (key, value)| {
                total
                    .checked_add(key.capacity())
                    .and_then(|n| n.checked_add(heap(value).ok()?))
                    .ok_or_else(capacity)
            })
        }
        _ => Ok(0),
    }
}
fn append(
    rows: &mut Vec<Value>,
    retained: &mut usize,
    value: Value,
    maximum: usize,
) -> Result<(), ReadError> {
    let next = retained.checked_add(heap(&value)?).ok_or_else(capacity)?;
    if rows.len() == rows.capacity() {
        let count = rows.capacity().max(8).checked_mul(2).ok_or_else(capacity)?;
        if count
            .checked_mul(std::mem::size_of::<Value>())
            .and_then(|n| n.checked_add(next))
            .ok_or_else(capacity)?
            > maximum
        {
            return Err(capacity());
        }
        rows.try_reserve_exact(count - rows.len())
            .map_err(|_| capacity())?;
    }
    if rows
        .capacity()
        .checked_mul(std::mem::size_of::<Value>())
        .and_then(|n| n.checked_add(next))
        .ok_or_else(capacity)?
        > maximum
    {
        return Err(capacity());
    }
    rows.push(value);
    *retained = next;
    Ok(())
}
#[derive(Default)]
pub(super) struct SerializerContext {
    counts: Option<(i64, HashMap<Option<i64>, i64>)>,
}
impl SerializerContext {
    async fn counts(
        &mut self,
        work: &ReadWork,
        connection: &mut OwnedConnection,
        account: i64,
    ) -> Result<(), ReadError> {
        if self.counts.is_some() {
            return Ok(());
        }
        let rows = account_snapshots(work, connection, AccountRows::StreamCounts, account).await?;
        let mut counts = HashMap::new();
        for raw in rows.into_rows() {
            let raw = decode(work, &raw)?;
            if counts
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_mul(128))
                .ok_or_else(capacity)?
                > work.work_bytes() / 64
            {
                return Err(capacity());
            }
            counts.insert(raw["group"].as_i64(), id(&raw, "count")?);
        }
        self.counts = Some((account, counts));
        Ok(())
    }
    fn count(&self, account: i64, group: i64) -> i64 {
        self.counts
            .as_ref()
            .filter(|(first, _)| *first == account)
            .and_then(|(_, counts)| counts.get(&Some(group)))
            .copied()
            .unwrap_or(0)
    }
}
#[derive(Clone, Copy)]
enum ProfilePass {
    Earliest,
    All,
    Default,
}
async fn expiration(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    account: i64,
    pass: ProfilePass,
) -> Result<Value, ReadError> {
    // No prefetch exists in the nested account serializer. Each declared
    // method and final default-expiry lookup gets its own fresh query.
    let rows = account_snapshots(work, connection, AccountRows::Profiles, account).await?;
    let mut profiles = Vec::new();
    let mut retained = 0;
    for raw in rows.into_rows() {
        let raw = decode(work, &raw)?;
        append(
            &mut profiles,
            &mut retained,
            fields(&raw, &["id", "name", "is_active", "is_default", "exp_date"]),
            work.work_bytes() / 32,
        )?;
    }
    if matches!(pass, ProfilePass::Default) {
        return profiles
            .iter()
            .find(|p| p["is_default"] == true)
            .map(|p| timestamp(&p["exp_date"], true))
            .transpose()
            .map(|p| p.unwrap_or(Value::Null));
    }
    let mut expiring = profiles
        .iter()
        .filter_map(|profile| match date(&profile["exp_date"]) {
            Ok(Some(date)) => Some(Ok((date, profile))),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<Vec<_>, ReadError>>()?;
    expiring.sort_by_key(|(date, _)| *date);
    if matches!(pass, ProfilePass::Earliest) {
        return expiring
            .iter()
            .find(|(_, p)| p["is_active"] == true)
            .map(|(_, p)| timestamp(&p["exp_date"], true))
            .transpose()
            .map(|p| p.unwrap_or(Value::Null));
    }
    Ok(Value::Array(expiring.iter().map(|(_,p)|Ok(json!({"profile_id":p["id"],"profile_name":p["name"],"exp_date":timestamp(&p["exp_date"],true)?,"is_active":p["is_active"]}))).collect::<Result<_,ReadError>>()?))
}
pub(super) async fn account(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    context: &mut SerializerContext,
    raw: &Value,
) -> Result<Value, ReadError> {
    let account = id(raw, "id")?;
    context.counts(work, connection, account).await?;
    let maximum = work.work_bytes() / 32;
    // Source super().to_representation evaluates filters before profiles,
    // then groups, then expiration methods, before custom preference fields.
    let filter_rows = account_snapshots(work, connection, AccountRows::Filters, account).await?;
    let mut filters = Vec::new();
    let mut retained = 0;
    for raw in filter_rows.into_rows() {
        let raw = decode(work, &raw)?;
        append(
            &mut filters,
            &mut retained,
            super::super::provider_reads::filter(&raw),
            maximum,
        )?;
    }
    filters.sort_by_key(|filter| filter["order"].as_u64().unwrap_or(0));
    let profile_rows = account_snapshots(work, connection, AccountRows::Profiles, account).await?;
    let mut profiles = Vec::new();
    retained = 0;
    for text in profile_rows.into_rows() {
        let row = decode(work, &text)?;
        let mut profile = fields(
            &row,
            &[
                "id",
                "name",
                "max_streams",
                "is_active",
                "is_default",
                "current_viewers",
                "search_pattern",
                "replace_pattern",
                "custom_properties",
                "exp_date",
            ],
        );
        profile["exp_date"] = timestamp(&row["exp_date"], false)?;
        profile["account"] = json!({"id":account,"name":raw["name"],"account_type":raw["account_type"],"is_xtream_codes":raw["account_type"]=="XC"});
        append(&mut profiles, &mut retained, profile, maximum)?;
    }
    let group_rows = account_snapshots(work, connection, AccountRows::Groups, account).await?;
    let mut groups = Vec::new();
    retained = 0;
    for row in group_rows.into_rows() {
        let row = decode(work, &row)?;
        let mut group = fields(
            &row,
            &[
                "channel_group_id",
                "enabled",
                "auto_channel_sync",
                "auto_sync_channel_start",
                "auto_sync_channel_end",
                "custom_properties",
                "is_stale",
                "last_seen",
            ],
        );
        let old = group.as_object_mut().ok_or(ReadError::WorkerFailed)?;
        let ordered = std::mem::take(old);
        for (key, value) in ordered {
            old.insert(
                if key == "channel_group_id" {
                    "channel_group".into()
                } else {
                    key
                },
                value,
            );
        }
        for key in ["auto_sync_channel_start", "auto_sync_channel_end"] {
            if !row[key].is_null() {
                let number = row[key]
                    .as_f64()
                    .and_then(serde_json::Number::from_f64)
                    .ok_or(ReadError::SourceUnhandledFault(
                        "Nonfinite group coordinate",
                    ))?;
                group[key] = number.into();
            }
        }
        group["custom_properties"] =
            super::super::provider_reads::profile_properties(&row["custom_properties"]);
        group["last_seen"] = timestamp(&row["last_seen"], false)?;
        group["stream_count"] = context.count(account, id(&row, "channel_group_id")?).into();
        append(&mut groups, &mut retained, group, maximum)?;
    }
    let earliest = expiration(work, connection, account, ProfilePass::Earliest).await?;
    let expirations = expiration(work, connection, account, ProfilePass::All).await?;
    // Source to_representation derives preferences before touching cron/FKs.
    // Preserve a nonmapping-properties fault before any later task lookup.
    if super::vod_api_projection::truthy(&raw["custom_properties"])
        && !raw["custom_properties"].is_object()
    {
        return Err(ReadError::SourceUnhandledFault(
            "Account representation failed",
        ));
    }
    let cron = if let Some(task) = raw["refresh_task_id"].as_i64() {
        connection.configure(work).await?;
        let crontab = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT crontab_id::bigint FROM django_celery_beat_periodictask WHERE id=$1",
        )
        .persistent(false)
        .bind(task)
        .fetch_optional(connection.raw())
        .await?
        .ok_or(ReadError::SourceUnhandledFault(
            "Related periodic task disappeared",
        ))?;
        if let Some(crontab) = crontab {
            connection.configure(work).await?;
            sqlx::query_scalar::<_,String>("SELECT concat_ws(' ',minute,hour,day_of_month,month_of_year,day_of_week) FROM django_celery_beat_crontabschedule WHERE id=$1").persistent(false).bind(crontab).fetch_optional(connection.raw()).await?.ok_or(ReadError::SourceUnhandledFault("Related crontab disappeared"))?
        } else {
            String::new()
        }
    } else {
        String::new()
    };
    let default_expiry = expiration(work, connection, account, ProfilePass::Default).await?;
    vod_api_actions_dto::account(
        raw,
        AccountMetadata {
            profiles: profiles.into(),
            filters: filters.into(),
            groups: groups.into(),
            cron: cron.into(),
            earliest,
            expirations,
            default_expiry,
        },
    )
    .map_err(|_| ReadError::SourceUnhandledFault("Account representation failed"))
}

pub(super) async fn category(
    work: &ReadWork,
    connection: &mut OwnedConnection,
    raw: &Value,
) -> Result<Value, ReadError> {
    if raw.is_null() {
        return Ok(Value::Null);
    }
    let category = id(raw, "id")?;
    let rows = snapshot(
        work,
        connection,
        CATEGORY_SOURCE,
        category,
        work.work_bytes() / 16,
    )
    .await?;
    let mut accounts = Vec::new();
    let mut retained = 0;
    for text in rows.into_rows() {
        let row = decode(work, &text)?;
        // Source IntegerField(source="m3u_account.id") dereferences the FK
        // on each materialized through-row. It is not a relation-table JOIN.
        let targets = snapshot(
            work,
            connection,
            "SELECT to_jsonb(a) AS payload FROM m3u_m3uaccount a WHERE a.id=$1",
            id(&row, "m3u_account_id")?,
            work.row_bytes()
                .checked_add(1024)
                .ok_or(ReadError::Capacity)?,
        )
        .await?;
        let target = targets
            .into_rows()
            .next()
            .ok_or(ReadError::SourceUnhandledFault(
                "Related category account disappeared",
            ))?;
        let target = decode(work, &target)?;
        if id(&target, "id")? != id(&row, "m3u_account_id")? {
            return Err(ReadError::WorkerFailed);
        }
        append(
            &mut accounts,
            &mut retained,
            json!({"category":row["category_id"],"m3u_account":row["m3u_account_id"],"enabled":row["enabled"],"custom_properties":row["custom_properties"]}),
            work.work_bytes() / 32,
        )?;
    }
    let kind = raw["category_type"]
        .as_str()
        .ok_or(ReadError::WorkerFailed)?;
    let display = match kind {
        "movie" => "Movie",
        "series" => "Series",
        other => other,
    };
    Ok(
        json!({"id":category,"name":raw["name"],"category_type":kind,"category_type_display":display,"m3u_accounts":accounts}),
    )
}

// Exact Source through-table projection; account dereferences remain fresh.
const CATEGORY_SOURCE: &str = r#"WITH category_source AS MATERIALIZED (SELECT "vod_m3uvodcategoryrelation"."id" AS "x_id","vod_m3uvodcategoryrelation"."m3u_account_id" AS "x_m3u_account_id","vod_m3uvodcategoryrelation"."category_id" AS "x_category_id","vod_m3uvodcategoryrelation"."enabled" AS "x_enabled","vod_m3uvodcategoryrelation"."custom_properties" AS "x_custom_properties","vod_m3uvodcategoryrelation"."created_at" AS "x_created_at","vod_m3uvodcategoryrelation"."updated_at" AS "x_updated_at" FROM "vod_m3uvodcategoryrelation" WHERE "vod_m3uvodcategoryrelation"."category_id" = $1) SELECT jsonb_build_object('id',"x_id",'m3u_account_id',"x_m3u_account_id",'category_id',"x_category_id",'enabled',"x_enabled",'custom_properties',"x_custom_properties",'created_at',"x_created_at",'updated_at',"x_updated_at") AS payload FROM category_source"#;
