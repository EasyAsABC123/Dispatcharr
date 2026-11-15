//! SD post-refresh history/cache/logo effects on the SAME owned task session.
//! Original Dispatcharr/Django authors retain every application algorithm credit.
use super::{sd_guide_auth::Source, sd_guide_state::State};
use crate::{source_epg_history_policy, StoreError};
use serde_json::{json, Value};
use sqlx::Row;
#[derive(Debug)]
pub(super) enum Error {
    Store(StoreError),
    Unsupported,
}
impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}
impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Self::Store(StoreError::Database(e))
    }
}
async fn policy(source: &Source) -> Result<crate::EpgHistoryPolicy, Error> {
    let session = source.session.clone();
    let load = move || async move {
        session.run(|c|Box::pin(async move{
  let row=sqlx::query("SELECT octet_length(value::text)<=65536 AS fits,CASE WHEN octet_length(value::text)<=65536 THEN value END AS value FROM core_coresettings WHERE key='epg_settings'").fetch_optional(c).await?;
  match row{None=>Ok(crate::settings_groups::default_epg()),Some(row)=>{if !row.try_get::<bool,_>("fits")?{return Err(StoreError::Unsupported("SD history settings capacity"))}let value:Value=row.try_get("value")?;Ok(if value.as_object().is_some_and(|v|!v.is_empty()){value}else{crate::settings_groups::default_epg()})}}
 })).await?
    };
    let value = match &source.auth.notification_cache {
        Some(cache) => cache.settings_group("epg_settings", load).await?,
        None => load().await?,
    };
    source_epg_history_policy(&value).map_err(|_| Error::Unsupported)
}
fn known_sql(error: &StoreError) -> bool {
    matches!(error, StoreError::Database(sqlx::Error::Database(_)))
}
async fn logo_pages(source: &Source) -> Result<(), Error> {
    let source_id = source.id;
    let mut after = 0i64;
    loop {
        let rows:Vec<(i64,i64)>=source.session.run(move|c|Box::pin(async move{sqlx::query_as("SELECT c.id::bigint,e.id::bigint FROM dispatcharr_channels_channel c JOIN epg_epgdata e ON e.id=c.epg_data_id WHERE e.epg_source_id=$1 AND e.icon_url IS NOT NULL AND e.icon_url<>'' AND c.id>$2 ORDER BY c.id LIMIT 500").bind(source_id).bind(after).fetch_all(c).await.map_err(StoreError::from)})).await??;
        if rows.is_empty() {
            return Ok(());
        }
        after = rows.last().ok_or(Error::Unsupported)?.0;
        let ids = rows.into_iter().map(|(c, _)| c).collect::<Vec<_>>();
        source.session.run(move|c|Box::pin(async move{
   // Source batch snapshot, including existing logo, precedes bulk logo create.
   let rows=sqlx::query("SELECT c.id::bigint,c.logo_id::bigint,e.icon_url,e.name,e.tvg_id,l.url AS old_url FROM dispatcharr_channels_channel c JOIN epg_epgdata e ON e.id=c.epg_data_id LEFT JOIN dispatcharr_channels_logo l ON l.id=c.logo_id WHERE c.id=ANY($1)").bind(ids).fetch_all(&mut*c).await?;
   let mut work=Vec::new();let mut urls=std::collections::HashSet::new();let mut logos=Vec::new();
   for row in rows{let raw:Option<String>=row.try_get("icon_url")?;let url=raw.as_deref().unwrap_or("").trim_matches(|v:char|v.is_whitespace()||matches!(v,'\u{1c}'..='\u{1f}'));if url.is_empty()||row.try_get::<Option<String>,_>("old_url")?.as_deref()==Some(url){continue}let id:i64=row.try_get("id")?;work.push(json!({"id":id,"url":url}));if urls.insert(url.to_owned()){let name:String=row.try_get("name")?;let tvg:Option<String>=row.try_get("tvg_id")?;logos.push(json!({"url":url,"name":if name.is_empty(){format!("Logo for {}",tvg.as_deref().unwrap_or("None"))}else{name}}));}}
   if work.is_empty(){return Ok(())}
   // Each bulk statement commits separately under Source autocommit: logo
   // creation remains committed if a later Channel bulk update is rejected.
   sqlx::query("INSERT INTO dispatcharr_channels_logo(name,url) SELECT left(x.name,255),x.url FROM jsonb_to_recordset($1) x(name text,url text) WHERE NOT EXISTS(SELECT 1 FROM dispatcharr_channels_logo l WHERE l.url=x.url) ON CONFLICT DO NOTHING").bind(Value::Array(logos)).execute(&mut*c).await?;
   sqlx::query("UPDATE dispatcharr_channels_channel c SET logo_id=l.id FROM jsonb_to_recordset($1) x(id bigint,url text) CROSS JOIN LATERAL(SELECT id FROM dispatcharr_channels_logo WHERE url=x.url LIMIT 1) l WHERE c.id=x.id AND c.logo_id IS DISTINCT FROM l.id").bind(Value::Array(work)).execute(&mut*c).await?;
   Ok::<_,StoreError>(())
  })).await??;
    }
}
/// Every best-effort catch is limited to known SQL ErrorResponse followed by the
/// TaskSqlSession physical Ready fence. Unknown transport outcomes still fault.
pub(super) async fn run(source: &Source, state: &State) -> Result<(), Error> {
    if source
        .properties
        .get("auto_apply_epg_logos")
        .is_some_and(crate::xc_live_provider::truthy)
    {
        match logo_pages(source).await {
            Err(Error::Store(ref e)) if known_sql(e) => {}
            other => other?,
        }
    }
    let table = state.table.clone();
    let id = source.id;
    let orphan=source.session.run(move|c|Box::pin(async move{sqlx::query(&format!("DELETE FROM epg_programdata p USING epg_epgdata e WHERE p.epg_id=e.id AND e.epg_source_id=$1 AND e.id NOT IN(SELECT id FROM {table}_mapped)")).bind(id).execute(c).await.map(|_|()).map_err(StoreError::from)})).await?;
    if let Err(error) = orphan {
        if !known_sql(&error) {
            return Err(error.into());
        }
    }
    let policy = policy(source).await?;
    let now = chrono::Utc::now();
    let mut after = 0;
    loop {
        let stations = state.mapped_page(after).await.map_err(|e| match e {
            super::sd_guide_state::Error::Store(e) => Error::Store(e),
            _ => Error::Unsupported,
        })?;
        if stations.is_empty() {
            break;
        }
        after = stations.last().ok_or(Error::Unsupported)?.0;
        for (_, _, epg) in stations {
            let result = source
                .session
                .run(move |c| {
                    Box::pin(async move {
                        let days: i64 = sqlx::query_scalar(crate::tvg_programme::CATCHUP_SQL)
                            .bind(epg)
                            .fetch_one(&mut *c)
                            .await?;
                        let cutoff = now
                            - chrono::Duration::days(days.clamp(policy.min_days, policy.max_days));
                        sqlx::query("DELETE FROM epg_programdata WHERE epg_id=$1 AND end_time<$2")
                            .bind(epg)
                            .bind(cutoff)
                            .execute(c)
                            .await
                            .map(|_| ())
                            .map_err(StoreError::from)
                    })
                })
                .await?;
            if let Err(error) = result {
                if !known_sql(&error) {
                    return Err(error.into());
                }
            }
        }
    }
    let table = state.table.clone();
    let id = source.id;
    let result=source.session.run(move|c|Box::pin(async move{sqlx::query(&format!("DELETE FROM epg_sdprogrammd5 m WHERE m.epg_source_id=$1 AND NOT EXISTS(SELECT 1 FROM epg_programdata p WHERE p.epg_id IN(SELECT id FROM {table}_mapped) AND p.program_id=m.program_id)")).bind(id).execute(c).await.map(|_|()).map_err(StoreError::from)})).await?;
    if let Err(error) = result {
        if !known_sql(&error) {
            return Err(error.into());
        }
    }
    // Source increments the raw shared Redis generation, catching cache failures;
    // it never deletes/replaces a still-building or in-flight chunk list.
    if let Some(cache) = &source.auth.notification_cache {
        let mut command = redis::cmd("INCR");
        command.arg("epg_content_generation");
        let _ = cache.command::<i64>(command).await;
    }
    Ok(())
}
