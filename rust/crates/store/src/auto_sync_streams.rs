//! Same-session disk/SQL-backed current-stream and processed-ID working set.
//! Original Dispatcharr authors retain application/algorithm credit.
//! Draft provider-order component; natural/regex/compact orchestration pending.
use crate::{task_sql_session::TaskSqlSession, StoreError};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::Row;
#[derive(Clone, Copy)]
pub(crate) enum Sort {
    Provider,
    Name,
    Tvg,
    Updated,
}
pub(crate) struct Streams {
    session: TaskSqlSession,
    table: String,
}
pub(crate) struct Item {
    pub ordinal: i64,
    pub id: i64,
    pub name: String,
    pub tvg_id: Option<String>,
    pub logo_url: Option<String>,
    pub logo_rejected: bool,
    pub stream_chno: Option<f64>,
    pub is_radio: bool,
    pub station: Value,
    pub properties_valid: bool,
    pub properties_kind: String,
}
fn unsupported() -> StoreError {
    StoreError::Unsupported("Auto-sync stream field projection exceeds owned decode capacity")
}
impl Streams {
    pub(crate) async fn capture(
        session: TaskSqlSession,
        account: i64,
        group: i64,
        scan_start: DateTime<Utc>,
        sort: Sort,
        reverse: bool,
        name_filters: bool,
    ) -> Result<Self, StoreError> {
        let table = format!("native_auto_streams_{}", uuid::Uuid::new_v4().simple());
        let name = table.clone();
        session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("CREATE TEMP TABLE {name}(ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,id bigint UNIQUE,name text,tvg_id text,logo_url text,logo_rejected boolean,stream_chno double precision,is_radio boolean,updated_at timestamptz,station_json text,properties_valid boolean,properties_kind text,fits boolean,processed boolean NOT NULL DEFAULT false) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
            let direction=if reverse{"DESC"}else{"ASC"};
            let ordering=if name_filters||matches!(sort,Sort::Name){"updated_at DESC".to_owned()}else{
                format!("{} {direction}",match sort{Sort::Provider=>"id",Sort::Name|Sort::Updated=>"updated_at",Sort::Tvg=>"tvg_id"})
            };
            sqlx::query(&format!("INSERT INTO {name}(id,name,tvg_id,logo_url,logo_rejected,stream_chno,is_radio,updated_at,station_json,properties_valid,properties_kind,fits) SELECT id::bigint,name,tvg_id,CASE WHEN COALESCE(octet_length(logo_url),0)<=2000 THEN logo_url END,COALESCE(octet_length(logo_url)>2000,false),stream_chno,is_radio,updated_at,CASE WHEN jsonb_typeof(custom_properties)='object' AND COALESCE(octet_length((custom_properties->'tvc-guide-stationid')::text),0)<=65536 THEN (custom_properties->'tvc-guide-stationid')::text END,(custom_properties IS NULL OR jsonb_typeof(custom_properties)='object' OR custom_properties IN('null'::jsonb,'false'::jsonb,'0'::jsonb,'\"\"'::jsonb,'[]'::jsonb)),CASE jsonb_typeof(custom_properties) WHEN 'array' THEN 'list' WHEN 'string' THEN 'str' WHEN 'boolean' THEN 'bool' WHEN 'number' THEN CASE WHEN custom_properties::text~'[.eE]' THEN 'float' ELSE 'int' END ELSE 'NoneType' END,COALESCE(octet_length((custom_properties->'tvc-guide-stationid')::text),0)<=65536 FROM dispatcharr_channels_stream WHERE m3u_account_id=$1 AND channel_group_id=$2 AND last_seen>=$3 ORDER BY {ordering}")).bind(account).bind(group).bind(scan_start).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await??;
        Ok(Self { session, table })
    }
    /// Read the SAME captured per-group model snapshot on the renumber and
    /// create/update passes, as Source's cached QuerySet/list does. A concurrent
    /// Stream deletion must cause the later real FK outcome, never silently
    /// erase a selected stream through a second JOIN or reread its attributes.
    pub(crate) async fn next(&self, after: i64) -> Result<Vec<Item>, StoreError> {
        let table = self.table.clone();
        self.session.run(move|c|Box::pin(async move{
            let rows=sqlx::query(&format!("SELECT ordinal,id,name,tvg_id,logo_url,logo_rejected,stream_chno,is_radio,station_json,properties_valid,properties_kind,fits FROM {table} WHERE ordinal>$1 ORDER BY ordinal LIMIT 100")).bind(after).fetch_all(c).await?;
            rows.into_iter().map(|row|{
                if !row.try_get::<bool,_>("fits")?{return Err(unsupported())}
                let station=match row.try_get::<Option<String>,_>("station_json")?{
                    None=>Value::Null,
                    Some(text)=>{
                        crate::provider_json_admission::admit(text.as_bytes(),crate::provider_json_admission::Limits{work_bytes:128*1024,depth:64,string_bytes:65536,string_copies:2}).map_err(|_|unsupported())?;
                        {
                            let mut value:Value=serde_json::from_str(&text).map_err(|_|unsupported())?;
                            crate::json_compat::normalize_input_numbers(&mut value).map_err(|_|unsupported())?;
                            value
                        }
                    }
                };
                Ok(Item{ordinal:row.try_get("ordinal")?,id:row.try_get("id")?,name:row.try_get("name")?,tvg_id:row.try_get("tvg_id")?,logo_url:row.try_get("logo_url")?,logo_rejected:row.try_get("logo_rejected")?,stream_chno:row.try_get("stream_chno")?,is_radio:row.try_get("is_radio")?,station,properties_valid:row.try_get("properties_valid")?,properties_kind:row.try_get("properties_kind")?})
            }).collect::<Result<Vec<_>,StoreError>>()
        })).await?
    }
    pub(crate) async fn names(&self, after: i64) -> Result<Vec<(i64, i64, String)>, StoreError> {
        let table = self.table.clone();
        self.session.run(move|c|Box::pin(async move{
            let rows=sqlx::query(&format!("SELECT ordinal,id,name FROM {table} WHERE ordinal>$1 ORDER BY ordinal LIMIT 100")).bind(after).fetch_all(c).await?;
            rows.into_iter().map(|row|Ok((row.try_get("ordinal")?,row.try_get("id")?,row.try_get("name")?))).collect::<Result<Vec<_>,StoreError>>()
        })).await?
    }
    pub(crate) async fn processed(&self, ids: Vec<i64>) -> Result<(), StoreError> {
        if ids.len() > 100 {
            return Err(unsupported());
        }
        let table = self.table.clone();
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query(&format!(
                        "UPDATE {table} SET processed=true WHERE id=ANY($1)"
                    ))
                    .bind(ids)
                    .execute(c)
                    .await?;
                    Ok::<_, StoreError>(())
                })
            })
            .await?
    }
    /// Run AFTER actual include/exclude selection. QuerySet tvg ordering uses
    /// the database's collation/null semantics; Python list ordering after a
    /// regex filter instead uses Unicode lexicographic tvg_id-or-empty keys.
    pub(crate) async fn finish_order(
        &self,
        sort: Sort,
        reverse: bool,
        name_filters: bool,
    ) -> Result<(), StoreError> {
        if matches!(sort, Sort::Name) {
            return self.natural_sort(reverse).await;
        }
        let table = self.table.clone();
        self.session.run(move|c|Box::pin(async move{
            let ordered=format!("{table}_ordered");let direction=if reverse{"DESC"}else{"ASC"};
            let key=match sort{Sort::Provider=>"id",Sort::Tvg if name_filters=>"COALESCE(tvg_id,'') COLLATE \"C\"",Sort::Tvg=>"tvg_id",Sort::Updated=>"updated_at",Sort::Name=>return Err(unsupported())};
            sqlx::query(&format!("CREATE TEMP TABLE {ordered}(ordinal bigint PRIMARY KEY,id bigint UNIQUE,name text,tvg_id text,logo_url text,logo_rejected boolean,stream_chno double precision,is_radio boolean,updated_at timestamptz,station_json text,properties_valid boolean,properties_kind text,fits boolean,processed boolean) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
            sqlx::query(&format!("INSERT INTO {ordered} SELECT row_number() OVER(ORDER BY {key} {direction},ordinal ASC),id,name,tvg_id,logo_url,logo_rejected,stream_chno,is_radio,updated_at,station_json,properties_valid,properties_kind,fits,processed FROM {table} ORDER BY {key} {direction},ordinal ASC")).execute(&mut*c).await?;
            sqlx::query(&format!("DROP TABLE {table}")).execute(&mut*c).await?;
            sqlx::query(&format!("ALTER TABLE {ordered} RENAME TO {table}")).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await?
    }
    pub(crate) async fn exclude(&self, ids: Vec<i64>) -> Result<(), StoreError> {
        if ids.len() > 100 {
            return Err(unsupported());
        }
        let table = self.table.clone();
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query(&format!("DELETE FROM {table} WHERE id=ANY($1)"))
                        .bind(ids)
                        .execute(c)
                        .await?;
                    Ok::<_, StoreError>(())
                })
            })
            .await?
    }
    pub(crate) async fn natural_sort(&self, reverse: bool) -> Result<(), StoreError> {
        let table = self.table.clone();
        self.session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN natural_key text[] COLLATE \"C\"")).execute(&mut*c).await?;
            let mut after=0i64;
            loop{
                let rows=sqlx::query(&format!("SELECT ordinal,name FROM {table} WHERE ordinal>$1 ORDER BY ordinal LIMIT 100")).bind(after).fetch_all(&mut*c).await?;
                if rows.is_empty(){break}
                let mut keys=Vec::with_capacity(rows.len());
                for row in &rows{
                    let ordinal:i64=row.try_get("ordinal")?;
                    let key=super::auto_sync_natural::key(row.try_get::<&str,_>("name")?)?;
                    keys.push((ordinal,key));
                }
                let mut update=sqlx::QueryBuilder::<sqlx::Postgres>::new(format!("UPDATE {table} p SET natural_key=chosen.key FROM("));
                update.push_values(keys,|mut values,(ordinal,key)|{
                    values.push_bind(ordinal).push_bind(key);
                });
                update.push(") AS chosen(ordinal,key) WHERE p.ordinal=chosen.ordinal");
                update.build().execute(&mut*c).await?;
                after=rows.last().ok_or_else(unsupported)?.try_get("ordinal")?;
            }
            // Preserve Source list-sort stability. The caller must capture name
            // inputs in Source Meta -updated_at order before this stage.
            let ordered=format!("{table}_natural");let direction=if reverse{"DESC"}else{"ASC"};
            sqlx::query(&format!("CREATE TEMP TABLE {ordered}(ordinal bigint PRIMARY KEY,id bigint UNIQUE,name text,tvg_id text,logo_url text,logo_rejected boolean,stream_chno double precision,is_radio boolean,updated_at timestamptz,station_json text,properties_valid boolean,properties_kind text,fits boolean,processed boolean,natural_key text[] COLLATE \"C\") ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
            sqlx::query(&format!("INSERT INTO {ordered} SELECT row_number() OVER(ORDER BY natural_key {direction},ordinal ASC),id,name,tvg_id,logo_url,logo_rejected,stream_chno,is_radio,updated_at,station_json,properties_valid,properties_kind,fits,processed,natural_key FROM {table} ORDER BY natural_key {direction},ordinal ASC")).execute(&mut*c).await?;
            let mut last:Option<Vec<String>>=None;let mut after=0i64;
            loop{
                let page=sqlx::query(&format!("SELECT ordinal,natural_key FROM {ordered} WHERE ordinal>$1 ORDER BY ordinal LIMIT 100")).bind(after).fetch_all(&mut*c).await?;
                if page.is_empty(){break}
                for row in page{
                    let key:Vec<String>=row.try_get("natural_key")?;
                    if let Some(previous)=last.as_ref(){super::auto_sync_natural::compare(previous,&key)?;}
                    last=Some(key);after=row.try_get("ordinal")?;
                }
            }
            sqlx::query(&format!("DROP TABLE {table}")).execute(&mut*c).await?;
            sqlx::query(&format!("ALTER TABLE {ordered} RENAME TO {table}")).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await?
    }
    pub(crate) fn processed_relation(&self) -> &str {
        &self.table
    }
    pub(crate) async fn close(self) -> Result<(), StoreError> {
        let table = self.table;
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query(&format!("DROP TABLE {table}"))
                        .execute(c)
                        .await?;
                    Ok::<_, StoreError>(())
                })
            })
            .await?
    }
}
