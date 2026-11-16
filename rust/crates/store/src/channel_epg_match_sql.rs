//! Matching SQL snapshots and changed-only mutations; Original authors credited.
use super::{ChannelRow, EpgRow, Request, Snapshot};
use crate::{AuthService, StoreError, TaskSqlSession};
use serde_json::{json, Value};
use sqlx::{Connection, Row};
use std::sync::Arc;
type Owner = Arc<dyn Send + Sync>;

const CHANNELS:&str="SELECT id::bigint,name,tvg_id,tvc_guide_stationid,epg_data_id::bigint,auto_created,channel_group_id::bigint FROM dispatcharr_channels_channel WHERE ($1 AND epg_data_id IS NULL) OR (NOT $1 AND id=ANY($2)) LIMIT 65537";
const SINGLE_EXACT:&str="SELECT e.id::bigint FROM epg_epgdata e JOIN epg_epgsource s ON s.id=e.epg_source_id WHERE s.is_active AND UPPER(e.tvg_id::text)=UPPER($1) ORDER BY e.id ASC LIMIT 1";
const CATALOG:&str="SELECT e.id::bigint,e.name,e.tvg_id,e.epg_source_id::bigint,s.priority::bigint,s.source_type,EXISTS(SELECT 1 FROM epg_programdata p WHERE p.epg_id=e.id) AS has_programs FROM epg_epgdata e JOIN epg_epgsource s ON s.id=e.epg_source_id WHERE s.is_active AND ($1::bigint IS NULL OR e.id=$1) LIMIT 65537";
const SETTINGS:&str="SELECT key,CASE WHEN octet_length(value::text)<=65536 THEN value ELSE NULL END AS value,COALESCE(octet_length(value::text),0)<=65536 AS supported FROM core_coresettings WHERE key IN ('preferred-region','epg_settings')";
const BULK_CANDIDATES:&str="SELECT c.id::bigint,x.epg FROM dispatcharr_channels_channel c JOIN jsonb_to_recordset($1) x(id bigint,epg bigint) ON x.id=c.id JOIN epg_epgdata e ON e.id=x.epg WHERE c.epg_data_id IS DISTINCT FROM x.epg";
const BULK_UPDATE:&str="UPDATE dispatcharr_channels_channel c SET epg_data_id=x.epg_data_id FROM jsonb_to_recordset($1) x(channel_id bigint,epg_data_id bigint) WHERE c.id=x.channel_id";
const CHILD_EPGS:&str="SELECT e.id::bigint,e.name,e.tvg_id,e.epg_source_id::bigint,s.source_type,EXISTS(SELECT 1 FROM epg_programdata p WHERE p.epg_id=e.id) AS has_programs FROM epg_epgdata e LEFT JOIN epg_epgsource s ON s.id=e.epg_source_id WHERE e.id=ANY($1)";
const SINGLE_UPDATE: &str = "UPDATE dispatcharr_channels_channel SET epg_data_id=$2 WHERE id=$1";
const MARK_CUSTOM:&str="INSERT INTO dispatcharr_channels_channelgroupm3uaccount(channel_group_id,m3u_account_id,custom_properties,enabled,auto_channel_sync,auto_sync_channel_start,auto_sync_channel_end,last_seen,is_stale) SELECT $1,id,'{}'::jsonb,true,false,NULL,NULL,now(),false FROM m3u_m3uaccount WHERE name='custom' AND locked ON CONFLICT(channel_group_id,m3u_account_id) DO NOTHING";

pub(super) async fn snapshot(
    auth: &AuthService,
    request: &Request,
    ids: Option<Vec<i64>>,
    owner: Owner,
) -> Result<Snapshot, StoreError> {
    let session = auth.borrow_task_sql_session(owner.clone()).await?;
    let single = matches!(request, Request::Single(_));
    let all = matches!(request, Request::All);
    let result = session
        .run(move |c| {
            Box::pin(async move {
                // Bulk/queryset order is deliberately not replaced with priority/PK
                // ordering. Exact single lookup is separately ordered by its first PK.
                let rows = sqlx::query(CHANNELS)
                    .bind(all)
                    .bind(ids.unwrap_or_default())
                    .fetch_all(&mut *c)
                    .await?;
                if rows.len() > 65536 {
                    return Err(StoreError::Unsupported("Matching channel capacity"));
                }
                let mut channels = Vec::with_capacity(rows.len());
                let mut channel_bytes = 0usize;
                for row in rows {
                    let name: String = row.try_get("name")?;
                    let tvg: Option<String> = row.try_get("tvg_id")?;
                    let station: Option<String> = row.try_get("tvc_guide_stationid")?;
                    channel_bytes = channel_bytes.saturating_add(
                        name.len()
                            + tvg.as_ref().map_or(0, String::len)
                            + station.as_ref().map_or(0, String::len),
                    );
                    if name.len() > 8192
                        || tvg.as_ref().is_some_and(|v| v.len() > 4096)
                        || station.as_ref().is_some_and(|v| v.len() > 4096)
                        || channel_bytes > 16 * 1024 * 1024
                    {
                        return Err(StoreError::Unsupported("Matching channel field capacity"));
                    }
                    channels.push(ChannelRow {
                        id: row.try_get("id")?,
                        name,
                        tvg,
                        station,
                        current: row.try_get("epg_data_id")?,
                        auto_created: row.try_get("auto_created")?,
                        group: row.try_get("channel_group_id")?,
                    });
                }
                if single && channels.is_empty() {
                    return Ok(Snapshot {
                        channels,
                        epgs: Vec::new(),
                        region: None,
                        settings: crate::settings_groups::default_epg(),
                        single,
                        single_exact: None,
                    });
                }
                let mut single_exact = None;
                if single {
                    if let Some(channel) = channels.first() {
                        for (key, station) in [(&channel.tvg, false), (&channel.station, true)] {
                            let value = super::normalized(key.as_deref());
                            if value.is_empty() {
                                continue;
                            }
                            let found: Option<i64> = sqlx::query_scalar(SINGLE_EXACT)
                                .bind(value)
                                .fetch_optional(&mut *c)
                                .await?;
                            if let Some(id) = found {
                                single_exact = Some((id, station));
                                break;
                            }
                        }
                    }
                }
                let rows = sqlx::query(CATALOG)
                    .bind(if single {
                        single_exact.map(|v| v.0)
                    } else {
                        None
                    })
                    .fetch_all(&mut *c)
                    .await?;
                if rows.len() > 65536 {
                    return Err(StoreError::Unsupported("Matching EPG catalog capacity"));
                }
                let mut epgs = Vec::with_capacity(rows.len());
                let mut bytes = 0usize;
                for row in rows {
                    let name: String = row.try_get("name")?;
                    let tvg: Option<String> = row.try_get("tvg_id")?;
                    bytes = bytes.saturating_add(name.len() + tvg.as_ref().map_or(0, String::len));
                    if name.len() > 8192
                        || tvg.as_ref().is_some_and(|v| v.len() > 4096)
                        || bytes > 16 * 1024 * 1024
                    {
                        return Err(StoreError::Unsupported("Matching EPG field capacity"));
                    }
                    epgs.push(EpgRow {
                        id: row.try_get("id")?,
                        name,
                        tvg,
                        source: row.try_get("epg_source_id")?,
                        priority: row.try_get::<Option<i64>, _>("priority")?.unwrap_or(0),
                        source_type: row.try_get("source_type")?,
                        has_programs: row.try_get("has_programs")?,
                    });
                }
                let rows = sqlx::query(SETTINGS).fetch_all(&mut *c).await?;
                let mut region = None;
                let mut settings = crate::settings_groups::default_epg();
                for row in rows {
                    if !row.try_get::<bool, _>("supported")? {
                        return Err(StoreError::Unsupported("Matching CoreSettings capacity"));
                    }
                    let key: String = row.try_get("key")?;
                    let value: Value = row.try_get("value")?;
                    if key == "preferred-region" {
                        region = Some(value)
                    } else if value.as_object().is_some_and(|v| !v.is_empty()) {
                        settings = value
                    }
                }
                Ok::<_, StoreError>(Snapshot {
                    channels,
                    epgs,
                    region,
                    settings,
                    single,
                    single_exact,
                })
            })
        })
        .await;
    let closed = session.close().await;
    closed?;
    let mut snapshot = result??;
    // Read-only cached setting inspection: preclaim planning never fills Redis.
    // Actual execution uses the existing version-fenced group loader afterward.
    if let Some(cache) = &auth.notification_cache {
        if let Ok(Some(raw)) = cache
            .clone()
            .with_retained_scope(owner)
            .get(":1:coresettings:group:epg_settings")
            .await
        {
            if raw.len() > 65536 {
                return Err(StoreError::Unsupported("Matching settings cache capacity"));
            }
            let value = crate::notifications::decode_settings_cache(&raw).ok_or(
                StoreError::Unsupported("Matching settings cache serialization unsupported"),
            )?;
            if value.is_object() {
                snapshot.settings = value
            }
        }
    }
    Ok(snapshot)
}

pub(super) async fn apply_bulk(
    auth: &AuthService,
    wanted: Vec<(i64, i64)>,
    owner: Owner,
) -> Result<Vec<Value>, StoreError> {
    if wanted.is_empty() {
        return Ok(Vec::new());
    }
    let session = auth.borrow_task_sql_session(owner).await?;
    let result=session.run(move|c|Box::pin(async move{
        let mapping:Value=wanted.iter().map(|(id,epg)|json!({"id":id,"epg":epg})).collect();
        // Re-read actual channels/EPG presence, as Source apply does after CPU.
        // One autocommit bulk statement, no post_save/cache/group side effects.
        let rows=sqlx::query(BULK_CANDIDATES).bind(mapping).fetch_all(&mut*c).await?;
        let associations=rows.into_iter().map(|r|Ok(json!({"channel_id":r.try_get::<i64,_>("id")?,"epg_data_id":r.try_get::<i64,_>("epg")?}))).collect::<Result<Vec<_>,StoreError>>()?;
        if !associations.is_empty(){
            // Source bulk_update's entire mutation is atomic. Preserve that
            // fence even if a future bounded implementation pages statements.
            let mut tx=c.begin().await?;
            let update=sqlx::query(BULK_UPDATE).bind(Value::Array(associations.clone())).execute(&mut*tx).await;
            if let Err(error)=update {tx.rollback().await?;return Err(StoreError::from(error))}
            tx.commit().await?;
        }
        Ok::<_,StoreError>(associations)
    })).await;
    let closed = session.close().await;
    closed?;
    result?
}
pub(super) async fn child_epgs(
    auth: &AuthService,
    associations: &[Value],
    owner: Owner,
) -> Result<Vec<EpgRow>, StoreError> {
    let ids = associations
        .iter()
        .filter_map(|v| v["epg_data_id"].as_i64())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let session = auth.borrow_task_sql_session(owner).await?;
    let result = session
        .run(move |c| {
            Box::pin(async move {
                let rows = sqlx::query(CHILD_EPGS).bind(ids).fetch_all(c).await?;
                rows.into_iter()
                    .map(|r| {
                        Ok(EpgRow {
                            id: r.try_get("id")?,
                            name: r.try_get("name")?,
                            tvg: r.try_get("tvg_id")?,
                            source: r.try_get("epg_source_id")?,
                            priority: 0,
                            source_type: r.try_get("source_type")?,
                            has_programs: r.try_get("has_programs")?,
                        })
                    })
                    .collect::<Result<Vec<_>, StoreError>>()
            })
        })
        .await;
    session.close().await?;
    result?
}

pub(super) async fn update_single(
    session: &TaskSqlSession,
    channel: i64,
    epg: i64,
) -> Result<bool, StoreError> {
    session
        .run(move |c| {
            Box::pin(async move {
                // update_fields=[epg_data] does not update auto_now/updated_at.
                let result = sqlx::query(SINGLE_UPDATE)
                    .bind(channel)
                    .bind(epg)
                    .execute(c)
                    .await?;
                if result.rows_affected() == 0 {
                    return Err(StoreError::InvalidInput(
                        json!({"detail":"Save with update_fields did not affect any rows."}),
                    ));
                }
                Ok::<_, StoreError>(true)
            })
        })
        .await?
}
pub(super) async fn mark_custom(
    session: &TaskSqlSession,
    channel: &ChannelRow,
) -> Result<(), StoreError> {
    if channel.auto_created || channel.group.is_none() {
        return Ok(());
    }
    let group = channel.group;
    session
        .run(move |c| {
            Box::pin(async move {
                sqlx::query(MARK_CUSTOM).bind(group).execute(c).await?;
                Ok::<_, StoreError>(())
            })
        })
        .await?
}
pub(super) async fn serialize(
    session: &TaskSqlSession,
    channel: i64,
) -> Result<Option<Value>, StoreError> {
    session
        .run(move |c| {
            Box::pin(async move {
                let query = serialize_query();
                let row = sqlx::query(&query).bind(channel).fetch_optional(c).await?;
                row.map(|row| {
                    crate::channels::serialize(
                        row,
                        false,
                        true,
                        &url::Url::parse("http://localhost/").expect("constant valid URL"),
                    )
                })
                .transpose()
            })
        })
        .await?
}

fn serialize_query() -> String {
    format!("{} AND c.id=$1", crate::channels::SELECT)
}
#[cfg(test)]
pub(super) fn schema_statements() -> Vec<(&'static str, String)> {
    vec![
        ("channels", CHANNELS.to_owned()),
        ("single_exact", SINGLE_EXACT.to_owned()),
        ("catalog", CATALOG.to_owned()),
        ("settings", SETTINGS.to_owned()),
        ("bulk_candidates", BULK_CANDIDATES.to_owned()),
        ("bulk_update", BULK_UPDATE.to_owned()),
        ("child_epgs", CHILD_EPGS.to_owned()),
        ("single_update", SINGLE_UPDATE.to_owned()),
        ("mark_custom", MARK_CUSTOM.to_owned()),
        ("serialize", serialize_query()),
        ("plugins", super::PLUGIN_CONFIG_SQL.to_owned()),
    ]
}
