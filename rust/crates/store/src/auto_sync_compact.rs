//! Pinned Source compact_numbering.repack_group on the actual owned SQL session.
//! Original Dispatcharr/Python authors retain all algorithm/application credit.
//! Rust owns SQL ordering, transaction, effects and physical admission/drain.
use crate::{auto_sync_config::Group, StoreError, TaskSqlSession};
use sqlx::{Connection, Row};
#[derive(Default)]
pub(crate) struct Report {
    pub assigned: u64,
    pub released: u64,
    pub failed: u64,
}
fn unsupported() -> StoreError {
    StoreError::Unsupported("Compact numbering Source numeric/sort domain is not qualified")
}
pub(crate) fn integer(value: f64) -> Result<f64, StoreError> {
    // Actual FloatFields enter Python int() before packing. Retain truncation,
    // including negative fractions. Beyond exact unit-step f64, Source's Python
    // integer progression needs a separate representation; never silently None.
    if !value.is_finite() || value.abs() >= 9_007_199_254_740_992.0 {
        return Err(unsupported());
    }
    Ok(value.trunc())
}
pub(crate) fn preflight(group: &Group) -> Result<(), StoreError> {
    integer(group.start)?;
    group.end.filter(|n| *n != 0.0).map(integer).transpose()?;
    Ok(())
}
pub(crate) async fn repack(
    session: &TaskSqlSession,
    account: i64,
    group: &Group,
) -> Result<Report, StoreError> {
    let start = integer(group.start)?;
    let end = group.end.filter(|n| *n != 0.0).map(integer).transpose()?;
    let group_id = group.id;
    // Source int(group_override) ignores invalid type/value, whereas normal
    // auto's earlier target resolution has already applied its own semantics.
    let override_id = match group.optional_fk("group_override") {
        Ok(id) => id,
        Err(crate::auto_sync_config::Error::Caught(_)) => None,
        Err(crate::auto_sync_config::Error::Store(error)) => return Err(error),
        Err(crate::auto_sync_config::Error::Unsupported) => return Err(unsupported()),
    };
    let sort = group
        .properties
        .get("channel_sort_order")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_owned();
    let reverse = group
        .properties
        .get("channel_sort_reverse")
        .is_some_and(crate::xc_live_provider::truthy);
    session.run(move|c|Box::pin(async move {
        if !c.protocol_transaction_is_idle().await? {return Err(unsupported())}
        let name=format!("native_compact_{}",uuid::Uuid::new_v4().simple());
        let reserved=format!("{name}_reserved");
        let ordered=format!("{name}_ordered");
        // A real transaction encloses model snapshot, both reservation reads,
        // visible writes, hidden release and pinned raw clearing. On error the
        // SQLx transaction queues rollback; TaskSqlSession retains physical close.
        let mut tx=c.begin().await?;
        sqlx::query(&format!("CREATE TEMP TABLE {name}(id bigint PRIMARY KEY,name text,tvg_id text,updated_at timestamptz,number double precision,hidden boolean,pinned boolean,natural_key text[] COLLATE \"C\") ON COMMIT DROP")).execute(&mut*tx).await?;
        sqlx::query(&format!("INSERT INTO {name}(id,name,tvg_id,updated_at,number,hidden,pinned) SELECT c.id::bigint,c.name,c.tvg_id,c.updated_at,c.channel_number,c.hidden_from_output,o.channel_number IS NOT NULL FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE c.auto_created AND c.auto_created_by_id=$1 AND(c.channel_group_id=$2 OR c.channel_group_id=$3) ORDER BY c.id")).bind(account).bind(group_id).bind(override_id).execute(&mut*tx).await?;
        if sort=="name" {
            let mut after=0i64;
            loop {
                let rows=sqlx::query(&format!("SELECT id,COALESCE(name,'') AS name FROM {name} WHERE NOT hidden AND NOT pinned AND id>$1 ORDER BY id LIMIT 100")).bind(after).fetch_all(&mut*tx).await?;
                if rows.is_empty(){break}
                let mut keys=Vec::with_capacity(rows.len());
                for row in &rows {
                    let id:i64=row.try_get("id")?;
                    keys.push((id,crate::auto_sync_natural::key(row.try_get::<&str,_>("name")?)?));
                }
                let mut update=sqlx::QueryBuilder::<sqlx::Postgres>::new(format!("UPDATE {name} p SET natural_key=v.key FROM("));
                update.push_values(keys,|mut values,(id,key)|{values.push_bind(id).push_bind(key);});
                update.push(") AS v(id,key) WHERE p.id=v.id");
                update.build().execute(&mut*tx).await?;
                after=rows.last().ok_or_else(unsupported)?.try_get("id")?;
            }
        }
        // Python's tuple comparison carries ID in the SAME reverse direction;
        // provider/default ignores reverse and retains Source's explicit ID ASC.
        let direction=if reverse{"DESC"}else{"ASC"};
        let order=match sort.as_str(){
            "name"=>format!("natural_key {direction},id {direction}"),
            "tvg_id"=>format!("COALESCE(tvg_id,'') COLLATE \"C\" {direction},id {direction}"),
            "updated_at"=>{
                let absent:bool=sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM {name} WHERE NOT hidden AND NOT pinned AND updated_at IS NULL)")).fetch_one(&mut*tx).await?;
                if absent{return Err(unsupported())}
                format!("updated_at {direction},id {direction}")
            },
            _=>"id ASC".into(),
        };
        sqlx::query(&format!("CREATE TEMP TABLE {ordered}(ordinal bigint PRIMARY KEY,id bigint,number double precision,natural_key text[] COLLATE \"C\") ON COMMIT DROP")).execute(&mut*tx).await?;
        sqlx::query(&format!("INSERT INTO {ordered} SELECT row_number() OVER(ORDER BY {order}),id,number,natural_key FROM {name} WHERE NOT hidden AND NOT pinned ORDER BY {order}")).execute(&mut*tx).await?;
        sqlx::query(&format!("CREATE TEMP TABLE {reserved}(number double precision PRIMARY KEY) ON COMMIT DROP")).execute(&mut*tx).await?;
        // Source reads overrides first, then other channels. ALL affected IDs,
        // including pinned and hidden, are excluded from raw-number reservation.
        sqlx::query(&format!("INSERT INTO {reserved} SELECT channel_number FROM dispatcharr_channels_channeloverride WHERE channel_number IS NOT NULL AND channel_number>=$1 AND($2::double precision IS NULL OR channel_number<=$2) ON CONFLICT DO NOTHING")).bind(start).bind(end).execute(&mut*tx).await?;
        sqlx::query(&format!("INSERT INTO {reserved} SELECT channel_number FROM dispatcharr_channels_channel WHERE channel_number IS NOT NULL AND id NOT IN(SELECT id FROM {name}) AND channel_number>=$1 AND($2::double precision IS NULL OR channel_number<=$2) ON CONFLICT DO NOTHING")).bind(start).bind(end).execute(&mut*tx).await?;
        let mut report=Report::default();
        let mut after=0i64;
        // Reservations only grow during this pack, so retain the smallest
        // still-possible integer instead of rescanning the occupied prefix.
        let mut cursor=start;
        let mut previous:Option<Vec<String>>=None;
        loop {
            let rows=sqlx::query(&format!("SELECT ordinal,id,number,natural_key FROM {ordered} WHERE ordinal>$1 ORDER BY ordinal LIMIT 100")).bind(after).fetch_all(&mut*tx).await?;
            if rows.is_empty(){break}
            let mut changes=Vec::with_capacity(rows.len());
            for row in rows {
                if sort=="name" {
                    let key:Vec<String>=row.try_get("natural_key")?;
                    if let Some(previous)=previous.as_ref(){crate::auto_sync_natural::compare(previous,&key)?;}
                    previous=Some(key);
                }
                after=row.try_get("ordinal")?;
                let id:i64=row.try_get("id")?;
                let current:Option<f64>=row.try_get("number")?;
                let picked=sqlx::query(&format!("WITH RECURSIVE candidate(n) AS(SELECT $1::double precision UNION ALL SELECT n+1 FROM candidate WHERE EXISTS(SELECT 1 FROM {reserved} WHERE number=n) AND($2::double precision IS NULL OR n<=$2) AND n+1<>n) SELECT n,EXISTS(SELECT 1 FROM {reserved} WHERE number=n) AS used FROM candidate WHERE NOT EXISTS(SELECT 1 FROM {reserved} WHERE number=n) OR n+1=n OR($2::double precision IS NOT NULL AND n>$2) LIMIT 1")).bind(cursor).bind(end).fetch_optional(&mut*tx).await?;
                let picked=picked.ok_or_else(unsupported)?;
                let n:f64=picked.try_get("n")?;
                let next=if end.is_some_and(|e|n>e){report.failed+=1;None}else{
                    if !n.is_finite()||picked.try_get::<bool,_>("used")?{return Err(unsupported())}
                    sqlx::query(&format!("INSERT INTO {reserved}(number) VALUES($1) ON CONFLICT DO NOTHING")).bind(n).execute(&mut*tx).await?;
                    report.assigned+=1;cursor=n+1.0;Some(n)
                };
                if current!=next{changes.push((id,next))}
            }
            if !changes.is_empty(){
                // Source bulk_update batch_size100 touches channel_number only;
                // no updated_at or model post_save signal is invented.
                let mut update=sqlx::QueryBuilder::<sqlx::Postgres>::new("UPDATE dispatcharr_channels_channel c SET channel_number=v.number FROM(");
                update.push_values(changes,|mut values,(id,n)|{values.push_bind(id).push_bind(n);});
                update.push(") AS v(id,number) WHERE c.id=v.id");
                update.build().execute(&mut*tx).await?;
            }
        }
        report.released=sqlx::query_scalar::<_,i64>(&format!("SELECT count(*) FROM {name} WHERE hidden AND NOT pinned AND number IS NOT NULL")).fetch_one(&mut*tx).await?.try_into().map_err(|_|unsupported())?;
        sqlx::query(&format!("UPDATE dispatcharr_channels_channel SET channel_number=NULL WHERE id IN(SELECT id FROM {name} WHERE(hidden OR pinned) AND number IS NOT NULL)")).execute(&mut*tx).await?;
        tx.commit().await?;
        Ok(report)
    })).await?
}
#[cfg(test)]
mod controls {
    use super::*;
    #[test]
    fn source_compact_int_truncation_differs_from_ordinary_fractional_cursor() {
        assert_eq!(integer(12.9).unwrap(), 12.0);
        assert_eq!(integer(-12.9).unwrap(), -12.0);
        assert_eq!(integer(-0.9).unwrap(), 0.0);
        assert!(integer(f64::INFINITY).is_err());
        assert!(integer(9_007_199_254_740_992.0).is_err());
    }
}
