//! Actual Source dispatch_program_refresh_for_epg_ids plan on one owned session.
//! Original Dispatcharr authors retain application/algorithm credit.
//! Draft component: production auto-sync orchestrator/producer assembly pending.
//! Create one Touched instance per actual Source helper invocation: group-new
//! and group-EPG-dirty are separate calls. Combining them would change SD's
//! threshold3 batching/lineage and is not Source-equivalent.
use crate::{task_sql_session::TaskSqlSession, StoreError};
use sqlx::Row;

pub struct Touched {
    session: TaskSqlSession,
    ids: Option<String>,
}
pub struct Plan {
    session: TaskSqlSession,
    tasks: Option<String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Task {
    ParseProgrammes(i64),
    ParseSdProgrammes(i64),
    FetchSdMappedGuide(i64),
}
pub struct Item {
    pub ordinal: i64,
    pub task: Task,
}
fn unsupported() -> StoreError {
    StoreError::Unsupported("Auto-sync EPG dispatch plan is outside the Source identifier domain")
}
impl Touched {
    pub async fn begin(session: TaskSqlSession) -> Result<Self, StoreError> {
        // Source's empty input is a genuine immediate return. Delay physical
        // table creation until an actual non-null assigned EPG ID is observed.
        Ok(Self { session, ids: None })
    }
    /// Source callers provide the non-null EPG IDs of actual newly created or
    /// EPG-dirty channels. Repeated bounded pages dedup in SQL, not a host set.
    pub async fn add(&mut self, ids: Vec<i64>) -> Result<(), StoreError> {
        if ids.len() > 500 || ids.iter().any(|id| !(1..=i64::from(i32::MAX)).contains(id)) {
            return Err(unsupported());
        }
        if ids.is_empty() {
            return Ok(());
        }
        if self.ids.is_none() {
            let name = format!("native_auto_epgs_{}", uuid::Uuid::new_v4().simple());
            let table = name.clone();
            self.session.run(move|c|Box::pin(async move{sqlx::query(&format!("CREATE TEMP TABLE {table}(id bigint PRIMARY KEY) ON COMMIT PRESERVE ROWS")).execute(c).await?;Ok::<_,StoreError>(())})).await??;
            self.ids = Some(name);
        }
        let table = self.ids.clone().ok_or_else(unsupported)?;
        self.session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("INSERT INTO {table}(id) SELECT * FROM unnest($1::bigint[]) ON CONFLICT DO NOTHING")).bind(ids).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await?
    }
    pub async fn seal(self) -> Result<Plan, StoreError> {
        let Some(ids) = self.ids else {
            return Ok(Plan {
                session: self.session,
                tasks: None,
            });
        };
        let tasks = format!("native_auto_epg_plan_{}", uuid::Uuid::new_v4().simple());
        let table = tasks.clone();
        self.session.run(move|c|Box::pin(async move{
            let selected=format!("{table}_selected");
            sqlx::query(&format!("CREATE TEMP TABLE {selected}(ordinal bigint GENERATED ALWAYS AS IDENTITY,id bigint,source_id bigint,kind text,has_program boolean NOT NULL DEFAULT false) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
            // Source has no model/default ordering on EPGData. Preserve this
            // actual query's observed iteration for the subsequent FIFO plan.
            sqlx::query(&format!("INSERT INTO {selected}(id,source_id,kind) SELECT e.id::bigint,s.id::bigint,s.source_type FROM epg_epgdata e LEFT JOIN epg_epgsource s ON s.id=e.epg_source_id WHERE e.id IN(SELECT id FROM {ids})")).execute(&mut*c).await?;
            // Keep the second ProgramData statement/snapshot, as in Source.
            sqlx::query(&format!("UPDATE {selected} e SET has_program=true WHERE EXISTS(SELECT 1 FROM epg_programdata p WHERE p.epg_id=e.id)")).execute(&mut*c).await?;
            sqlx::query(&format!("CREATE TEMP TABLE {table}(ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,kind text,target bigint,publication uuid) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
            // Pinned sd_tasks.py SD_BULK_GUIDE_FETCH_THRESHOLD=3. Source emits
            // all non-SD IDs first, then SD sources in first-observed order.
            sqlx::query(&format!("WITH sd AS(SELECT *,min(ordinal) OVER(PARTITION BY source_id) AS source_order,min(ordinal) FILTER(WHERE NOT has_program) OVER(PARTITION BY source_id) AS first_needed,count(*) FILTER(WHERE NOT has_program) OVER(PARTITION BY source_id) AS needed FROM {selected} WHERE kind='schedules_direct'),pending AS(SELECT 'parse'::text AS task,id AS target,0 AS phase,ordinal AS source_order,0::bigint AS inner_order FROM {selected} WHERE kind IS NULL OR kind NOT IN('dummy','schedules_direct') UNION ALL SELECT CASE WHEN needed>=3 THEN 'sd_batch' ELSE 'parse_sd' END,CASE WHEN needed>=3 THEN source_id ELSE id END,1,source_order,ordinal FROM sd WHERE NOT has_program AND(needed<3 OR ordinal=first_needed)) INSERT INTO {table}(kind,target) SELECT task,target FROM pending ORDER BY phase,source_order,inner_order")).execute(&mut*c).await?;
            sqlx::query(&format!("DROP TABLE {selected},{ids}")).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await??;
        Ok(Plan {
            session: self.session,
            tasks: Some(tasks),
        })
    }
}
impl Plan {
    /// Called only with completed actual broker receipts by the Jobs producer.
    /// Pages remain bounded; metadata generation need not retain all child IDs.
    pub async fn record_publications(
        &self,
        receipts: Vec<(i64, uuid::Uuid)>,
    ) -> Result<(), StoreError> {
        if receipts.len() > 500 {
            return Err(unsupported());
        }
        if receipts.is_empty() {
            return Ok(());
        }
        let expected = receipts.len() as u64;
        let data = serde_json::Value::Array(
            receipts
                .into_iter()
                .map(|(ordinal, id)| serde_json::json!({"ordinal":ordinal,"id":id}))
                .collect(),
        );
        let table = self.tasks.clone().ok_or_else(unsupported)?;
        self.session.run(move|c|Box::pin(async move{
            let changed=sqlx::query(&format!("UPDATE {table} p SET publication=x.id FROM jsonb_to_recordset($1) x(ordinal bigint,id uuid) WHERE p.ordinal=x.ordinal AND p.publication IS NULL")).bind(data).execute(c).await?.rows_affected();
            if changed!=expected{return Err(unsupported())}
            Ok::<_,StoreError>(())
        })).await?
    }
    pub async fn children(&self, after: i64) -> Result<Vec<(i64, uuid::Uuid)>, StoreError> {
        let Some(table) = self.tasks.clone() else {
            return Ok(Vec::new());
        };
        self.session.run(move|c|Box::pin(async move{
            let rows=sqlx::query(&format!("SELECT ordinal,publication FROM {table} WHERE ordinal>$1 ORDER BY ordinal LIMIT 500")).bind(after).fetch_all(c).await?;
            rows.into_iter().map(|row|Ok((row.try_get("ordinal")?,row.try_get("publication")?))).collect::<Result<Vec<_>,StoreError>>()
        })).await?
    }
    pub async fn contains_sd(&self) -> Result<bool, StoreError> {
        let Some(table) = self.tasks.clone() else {
            return Ok(false);
        };
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query_scalar::<_, bool>(&format!(
                        "SELECT EXISTS(SELECT 1 FROM {table} WHERE kind IN('parse_sd','sd_batch'))"
                    ))
                    .fetch_one(c)
                    .await
                    .map_err(StoreError::from)
                })
            })
            .await?
    }
    pub async fn next(&self, after: i64) -> Result<Vec<Item>, StoreError> {
        let Some(table) = self.tasks.clone() else {
            return Ok(Vec::new());
        };
        self.session.run(move|c|Box::pin(async move{
            let rows=sqlx::query(&format!("SELECT ordinal,kind,target FROM {table} WHERE ordinal>$1 ORDER BY ordinal LIMIT 500")).bind(after).fetch_all(c).await?;
            rows.into_iter().map(|row|{
                let target:i64=row.try_get("target")?;
                let task=match row.try_get::<&str,_>("kind")?{
                    "parse"=>Task::ParseProgrammes(target),
                    "parse_sd"=>Task::ParseSdProgrammes(target),
                    "sd_batch"=>Task::FetchSdMappedGuide(target),
                    _=>return Err(unsupported()),
                };
                Ok(Item{ordinal:row.try_get("ordinal")?,task})
            }).collect::<Result<Vec<_>,StoreError>>()
        })).await?
    }
    pub async fn close(self) -> Result<(), StoreError> {
        let Some(table) = self.tasks else {
            return Ok(());
        };
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
