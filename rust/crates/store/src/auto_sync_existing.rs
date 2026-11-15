//! Canonical per-group existing Channel objects/dirty fields, on owned SQL.
//! Original Dispatcharr authors retain all application/algorithm credit.
//! Draft component; normal orchestration and acceptance remain pending.
use crate::{task_sql_session::TaskSqlSession, StoreError};
use serde_json::{Map, Value};
use sqlx::Row;
const FIELDS: &[&str] = &[
    "name",
    "tvg_id",
    "tvc_guide_stationid",
    "channel_group_id",
    "logo_id",
    "epg_data_id",
    "stream_profile_id",
    "is_radio",
];
pub(crate) struct Existing {
    session: TaskSqlSession,
    rows: String,
    mapping: String,
}
fn unsupported() -> StoreError {
    StoreError::Unsupported("Auto-sync dirty Channel field is not a Source field")
}
impl Existing {
    pub(crate) async fn prepare(
        session: TaskSqlSession,
        account: i64,
        group: i64,
    ) -> Result<Self, StoreError> {
        let rows = format!("native_auto_existing_{}", uuid::Uuid::new_v4().simple());
        let mapping = format!("{rows}_streams");
        let work = rows.clone();
        let links = mapping.clone();
        session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("CREATE TEMP TABLE {work}(ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,id bigint UNIQUE,draft jsonb,dirty text[] NOT NULL DEFAULT '{{}}',epg_dirty boolean NOT NULL DEFAULT false,renumbered boolean NOT NULL DEFAULT false) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
            sqlx::query(&format!("CREATE TEMP TABLE {links}(ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,stream_id bigint,channel_id bigint,draft jsonb) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
            sqlx::query(&format!("INSERT INTO {links}(stream_id,channel_id,draft) SELECT cs.stream_id::bigint,cs.channel_id::bigint,to_jsonb(ch) FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_channel ch ON ch.id=cs.channel_id JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id WHERE ch.auto_created AND ch.auto_created_by_id=$1 AND s.m3u_account_id=$1 AND s.channel_group_id=$2 ORDER BY cs.\"order\"")).bind(account).bind(group).execute(&mut*c).await?;
            // Dict overwrite keeps the FIRST stream insertion position but
            // points to its LAST channel; canonical Channel data was captured
            // in the SAME SELECT as Source's select_related join, not reread.
            sqlx::query(&format!("WITH last_row AS(SELECT DISTINCT ON(stream_id) stream_id,channel_id,draft FROM {links} ORDER BY stream_id,ordinal DESC),first_row AS(SELECT stream_id,min(ordinal) AS ordinal FROM {links} GROUP BY stream_id) UPDATE {links} m SET channel_id=l.channel_id,draft=l.draft FROM last_row l,first_row f WHERE m.ordinal=f.ordinal AND f.stream_id=l.stream_id")).execute(&mut*c).await?;
            sqlx::query(&format!("DELETE FROM {links} WHERE ordinal NOT IN(SELECT min(ordinal) FROM {links} GROUP BY stream_id)")).execute(&mut*c).await?;
            sqlx::query(&format!("INSERT INTO {work}(id,draft) SELECT DISTINCT ON(channel_id) channel_id,draft FROM {links} ORDER BY channel_id,ordinal")).execute(&mut*c).await?;
            sqlx::query(&format!("CREATE INDEX ON {links}(stream_id,ordinal DESC)")).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await??;
        Ok(Self {
            session,
            rows,
            mapping,
        })
    }
    pub(crate) async fn for_stream(&self, stream: i64) -> Result<Option<(i64, Value)>, StoreError> {
        let rows = self.rows.clone();
        let mapping = self.mapping.clone();
        self.session.run(move|c|Box::pin(async move{
            let row=sqlx::query(&format!("SELECT c.id,c.draft FROM {rows} c JOIN {mapping} m ON m.channel_id=c.id WHERE m.stream_id=$1 ORDER BY m.ordinal DESC LIMIT 1")).bind(stream).fetch_optional(c).await?;
            row.map(|row|Ok((row.try_get("id")?,row.try_get("draft")?))).transpose()
        })).await?
    }
    /// Patches are already resolved Source properties. Rejected logos and an
    /// absent group stream-profile override are OMITTED, not silently cleared.
    pub(crate) async fn change(
        &self,
        id: i64,
        patch: Map<String, Value>,
    ) -> Result<(), StoreError> {
        if patch.keys().any(|key| !FIELDS.contains(&key.as_str())) {
            return Err(unsupported());
        }
        let rows = self.rows.clone();
        let patch = Value::Object(patch);
        self.session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("WITH delta AS(SELECT id,array_agg(p.key) FILTER(WHERE draft->p.key IS DISTINCT FROM p.value) AS fields FROM {rows} CROSS JOIN LATERAL jsonb_each($2) p WHERE id=$1 GROUP BY id) UPDATE {rows} r SET draft=r.draft||$2,dirty=ARRAY(SELECT DISTINCT unnest(r.dirty||COALESCE(d.fields,'{{}}'::text[]))),epg_dirty=r.epg_dirty OR('epg_data_id'=ANY(COALESCE(d.fields,'{{}}'::text[])) AND $2->'epg_data_id'<>'null'::jsonb) FROM delta d WHERE r.id=d.id")).bind(id).bind(patch).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await?
    }
    /// Stage canonical object number changes during Source's first pass. The
    /// actual Channel rows are written only by flush_numbers after that pass.
    pub(crate) async fn stage_numbers(&self, changes: Vec<(i64, f64)>) -> Result<(), StoreError> {
        if changes.len() > 500 || changes.iter().any(|(_, n)| !n.is_finite()) {
            return Err(unsupported());
        }
        let data = Value::Array(
            changes
                .into_iter()
                .map(|(id, number)| serde_json::json!({"id":id,"number":number}))
                .collect(),
        );
        let rows = self.rows.clone();
        self.session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("WITH chosen AS(SELECT DISTINCT ON((value->>'id')::bigint) (value->>'id')::bigint AS id,(value->>'number')::double precision AS number FROM jsonb_array_elements($1) WITH ORDINALITY input(value,position) ORDER BY (value->>'id')::bigint,position DESC) UPDATE {rows} r SET draft=jsonb_set(r.draft,'{{channel_number}}',to_jsonb(x.number)),renumbered=true FROM chosen x WHERE r.id=x.id")).bind(data).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await?
    }
    pub(crate) async fn flush_numbers(&self) -> Result<(), StoreError> {
        let rows = self.rows.clone();
        self.session.run(move|c|Box::pin(async move{
            sqlx::query("BEGIN").execute(&mut*c).await?;
            let body=async{
                let mut after=0i64;
                loop{
                    let high:Option<i64>=sqlx::query_scalar(&format!("SELECT max(ordinal) FROM(SELECT ordinal FROM {rows} WHERE ordinal>$1 AND renumbered ORDER BY ordinal LIMIT 500) p")).bind(after).fetch_one(&mut*c).await?;
                    let Some(high)=high else{break};
                    sqlx::query(&format!("UPDATE dispatcharr_channels_channel c SET channel_number=(r.draft->>'channel_number')::double precision FROM {rows} r WHERE c.id=r.id AND r.renumbered AND r.ordinal>$1 AND r.ordinal<=$2")).bind(after).bind(high).execute(&mut*c).await?;
                    after=high;
                }
                Ok::<_,StoreError>(())
            }.await;
            match body{Ok(())=>{sqlx::query("COMMIT").execute(&mut*c).await?;Ok(())},Err(error)=>{sqlx::query("ROLLBACK").execute(&mut*c).await?;Err(error)}}
        })).await?
    }
    /// Source's no-stream branch preserves duplicate map values in its input
    /// count. The later disappeared-stream branch groups by Channel first.
    pub(crate) async fn prune_page(
        &self,
        streams: &super::auto_sync_streams::Streams,
        after: i64,
        all_empty: bool,
    ) -> Result<Vec<(i64, i64)>, StoreError> {
        let rows = self.rows.clone();
        let mapping = self.mapping.clone();
        let processed = streams.processed_relation().to_owned();
        self.session.run(move|c|Box::pin(async move{
            let selection=if all_empty{
                format!("SELECT m.ordinal,r.id FROM {mapping} m JOIN {rows} r ON r.id=m.channel_id WHERE NOT(r.draft->>'hidden_from_output')::boolean")
            }else{
                format!("SELECT min(m.ordinal) AS ordinal,r.id FROM {mapping} m JOIN {rows} r ON r.id=m.channel_id WHERE NOT(r.draft->>'hidden_from_output')::boolean GROUP BY r.id HAVING NOT EXISTS(SELECT 1 FROM {mapping} own JOIN {processed} p ON p.id=own.stream_id AND p.processed WHERE own.channel_id=r.id)")
            };
            let selected=sqlx::query(&format!("SELECT ordinal,id FROM({selection}) chosen WHERE ordinal>$1 ORDER BY ordinal LIMIT 500")).bind(after).fetch_all(c).await?;
            selected.into_iter().map(|row|Ok((row.try_get("ordinal")?,row.try_get("id")?))).collect::<Result<Vec<_>,StoreError>>()
        })).await?
    }
    pub(crate) async fn overflow_page(
        &self,
        after: i64,
        start: f64,
        end: f64,
    ) -> Result<Vec<(i64, i64, f64)>, StoreError> {
        let rows = self.rows.clone();
        self.session.run(move|c|Box::pin(async move{
            let selected=sqlx::query(&format!("SELECT ordinal,id,(draft->>'channel_number')::double precision AS number FROM {rows} WHERE ordinal>$1 AND NOT(draft->>'hidden_from_output')::boolean AND((draft->>'channel_number')::double precision<$2 OR(draft->>'channel_number')::double precision>$3) ORDER BY ordinal LIMIT 500")).bind(after).bind(start).bind(end).fetch_all(c).await?;
            selected.into_iter().map(|row|Ok((row.try_get("ordinal")?,row.try_get("id")?,row.try_get("number")?))).collect::<Result<Vec<_>,StoreError>>()
        })).await?
    }
    pub(crate) async fn forget(&self, ids: Vec<i64>) -> Result<(), StoreError> {
        if ids.len() > 500 {
            return Err(unsupported());
        }
        let rows = self.rows.clone();
        let mapping = self.mapping.clone();
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query(&format!("DELETE FROM {mapping} WHERE channel_id=ANY($1)"))
                        .bind(&ids)
                        .execute(&mut *c)
                        .await?;
                    sqlx::query(&format!("DELETE FROM {rows} WHERE id=ANY($1)"))
                        .bind(ids)
                        .execute(c)
                        .await?;
                    Ok::<_, StoreError>(())
                })
            })
            .await?
    }
    /// As Source bulk_update, union all dirty fields and write those fields for
    /// all canonical dirty objects in one transaction over bounded500 pages.
    pub(crate) async fn flush(&self) -> Result<u64, StoreError> {
        let rows = self.rows.clone();
        self.session.run(move|c|Box::pin(async move{
            let fields:Vec<String>=sqlx::query_scalar(&format!("SELECT DISTINCT unnest(dirty) FROM {rows}")).fetch_all(&mut*c).await?;
            if fields.is_empty(){return Ok(0)}
            if fields.iter().any(|name|!FIELDS.contains(&name.as_str())){return Err(unsupported())}
            let logical:u64=sqlx::query_scalar::<_,i64>(&format!("SELECT count(*) FROM {rows} WHERE cardinality(dirty)>0")).fetch_one(&mut*c).await?.try_into().map_err(|_|unsupported())?;
            let assignments=fields.iter().map(|field|format!("{field}=x.{field}")).collect::<Vec<_>>().join(",");
            sqlx::query("BEGIN").execute(&mut*c).await?;
            let body=async{
                let mut after=0i64;let mut count=0u64;
                loop{
                    let high:Option<i64>=sqlx::query_scalar(&format!("SELECT max(ordinal) FROM(SELECT ordinal FROM {rows} WHERE ordinal>$1 AND cardinality(dirty)>0 ORDER BY ordinal LIMIT 500) p")).bind(after).fetch_one(&mut*c).await?;
                    let Some(high)=high else{break};
                    let changed=sqlx::query(&format!("UPDATE dispatcharr_channels_channel c SET {assignments} FROM {rows} r CROSS JOIN LATERAL jsonb_populate_record(NULL::dispatcharr_channels_channel,r.draft) x WHERE c.id=r.id AND r.ordinal>$1 AND r.ordinal<=$2 AND cardinality(r.dirty)>0")).bind(after).bind(high).execute(&mut*c).await?.rows_affected();
                    count=count.checked_add(changed).ok_or_else(unsupported)?;after=high;
                }
                Ok::<_,StoreError>(count)
            }.await;
            match body{Ok(_actual)=>{sqlx::query("COMMIT").execute(&mut*c).await?;Ok(logical)},Err(error)=>{sqlx::query("ROLLBACK").execute(&mut*c).await?;Err(error)}}
        })).await?
    }
    pub(crate) async fn epg_page(&self, after: i64) -> Result<Vec<(i64, Option<i64>)>, StoreError> {
        let table = self.rows.clone();
        self.session.run(move|c|Box::pin(async move{
            let rows=sqlx::query(&format!("SELECT ordinal,(draft->>'epg_data_id')::bigint AS epg_data_id FROM {table} WHERE ordinal>$1 AND epg_dirty ORDER BY ordinal LIMIT 500")).bind(after).fetch_all(c).await?;
            rows.into_iter().map(|row|Ok((row.try_get("ordinal")?,row.try_get("epg_data_id")?))).collect::<Result<Vec<_>,StoreError>>()
        })).await?
    }
    pub(crate) fn relation(&self) -> &str {
        &self.rows
    }
    pub(crate) async fn close(self) -> Result<(), StoreError> {
        let rows = self.rows;
        let mapping = self.mapping;
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query(&format!("DROP TABLE {rows},{mapping}"))
                        .execute(c)
                        .await?;
                    Ok::<_, StoreError>(())
                })
            })
            .await?
    }
}
