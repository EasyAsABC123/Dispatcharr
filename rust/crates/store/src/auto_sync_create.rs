//! Source auto-sync bulk-create/dependent-row stages on the same SQL session.
//! Original Dispatcharr authors retain all application/algorithm credit.
//! Draft component; resolver/orchestrator assembly and runtime proof pending.
use crate::{task_sql_session::TaskSqlSession, StoreError};
use chrono::Utc;
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

pub(crate) struct Pending {
    session: TaskSqlSession,
    rows: String,
}
pub(crate) struct ChannelDraft {
    pub stream: i64,
    pub account: i64,
    pub number: f64,
    pub name: String,
    pub tvg_id: Option<String>,
    pub tvc_guide_stationid: Option<String>,
    pub group: i64,
    pub logo: Option<i64>,
    pub epg: Option<i64>,
    pub profile: Option<i64>,
    pub is_radio: bool,
}
const COLUMNS:&str="channel_number,name,logo_id,channel_group_id,tvg_id,tvc_guide_stationid,epg_data_id,stream_profile_id,uuid,user_level,is_adult,auto_created,auto_created_by_id,is_catchup,catchup_days,is_radio,hidden_from_output,created_at,updated_at";
fn unsupported() -> StoreError {
    StoreError::Unsupported("Auto-sync channel batch must contain actual bounded Source rows")
}
impl Pending {
    pub(crate) async fn begin(session: TaskSqlSession) -> Result<Self, StoreError> {
        let rows = format!("native_auto_create_{}", Uuid::new_v4().simple());
        let table = rows.clone();
        session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("CREATE TEMP TABLE {table}(ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,stream_id bigint NOT NULL,draft jsonb NOT NULL,channel_id bigint) ON COMMIT PRESERVE ROWS")).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await??;
        Ok(Self { session, rows })
    }
    pub(crate) async fn add(&self, drafts: Vec<ChannelDraft>) -> Result<(), StoreError> {
        if drafts.len() > 500 {
            return Err(unsupported());
        }
        let mut values = Vec::with_capacity(drafts.len());
        for row in drafts {
            if row.stream <= 0 || row.account <= 0 || row.group <= 0 || !row.number.is_finite() {
                return Err(unsupported());
            }
            let now = Utc::now();
            values.push(json!({"stream_id":row.stream,"draft":{
                "channel_number":row.number,"name":row.name.chars().take(512).collect::<String>(),
                "logo_id":row.logo,"channel_group_id":row.group,"tvg_id":row.tvg_id,
                "tvc_guide_stationid":row.tvc_guide_stationid,"epg_data_id":row.epg,
                "stream_profile_id":row.profile,"uuid":Uuid::new_v4().to_string(),
                "user_level":0,"is_adult":false,"auto_created":true,"auto_created_by_id":row.account,
                "is_catchup":false,"catchup_days":0,"is_radio":row.is_radio,
                "hidden_from_output":false,"created_at":now,"updated_at":now,
            }}));
        }
        let data = Value::Array(values);
        let table = self.rows.clone();
        self.session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("INSERT INTO {table}(stream_id,draft) SELECT stream_id,draft FROM jsonb_to_recordset($1) x(stream_id bigint,draft jsonb)")).bind(data).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await?
    }
    /// Bulk-create ALL channel pages under one real transaction as Django's
    /// bulk_create does. ChannelStream/profile inserts have their own subsequent
    /// statement scopes, so a later error never fabricates rollback of channels.
    pub(crate) async fn create_channels(&self) -> Result<u64, StoreError> {
        let table = self.rows.clone();
        self.session.run(move|c|Box::pin(async move{
            sqlx::query("BEGIN").execute(&mut*c).await?;
            let body=async {
                let mut after=0i64;let mut created=0u64;
                loop {
                    let high:Option<i64>=sqlx::query_scalar(&format!("SELECT max(ordinal) FROM(SELECT ordinal FROM {table} WHERE ordinal>$1 ORDER BY ordinal LIMIT 500) page")).bind(after).fetch_one(&mut*c).await?;
                    let Some(high)=high else{break};
                    let selected=COLUMNS.split(',').map(|field|format!("x.{field}")).collect::<Vec<_>>().join(",");
                    let changed=sqlx::query(&format!("INSERT INTO dispatcharr_channels_channel({COLUMNS}) SELECT {selected} FROM {table} p CROSS JOIN LATERAL jsonb_populate_record(NULL::dispatcharr_channels_channel,p.draft) x WHERE p.ordinal>$1 AND p.ordinal<=$2 ORDER BY p.ordinal")).bind(after).bind(high).execute(&mut*c).await?.rows_affected();
                    created=created.checked_add(changed).ok_or_else(unsupported)?;after=high;
                }
                sqlx::query(&format!("UPDATE {table} p SET channel_id=c.id FROM dispatcharr_channels_channel c WHERE c.uuid=(p.draft->>'uuid')::uuid")).execute(&mut*c).await?;
                Ok::<_,StoreError>(created)
            }.await;
            match body {
                Ok(created)=>{sqlx::query("COMMIT").execute(&mut*c).await?;Ok(created)},
                Err(error)=>{sqlx::query("ROLLBACK").execute(&mut*c).await?;Err(error)},
            }
        })).await?
    }
    pub(crate) async fn link_streams(&self) -> Result<(), StoreError> {
        let table = self.rows.clone();
        self.session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("INSERT INTO dispatcharr_channels_channelstream(channel_id,stream_id,\"order\") SELECT channel_id,stream_id,0 FROM {table} ORDER BY ordinal")).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await?
    }
    pub(crate) async fn memberships(
        &self,
        profiles: &super::auto_sync_profiles::Profiles,
    ) -> Result<(), StoreError> {
        let Some(profiles) = profiles.relation().map(str::to_owned) else {
            return Ok(());
        };
        let table = self.rows.clone();
        self.session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("INSERT INTO dispatcharr_channels_channelprofilemembership(channel_id,channel_profile_id,enabled) SELECT n.channel_id,p.id,true FROM {table} n CROSS JOIN {profiles} p ON CONFLICT DO NOTHING")).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await?
    }
    /// Bounded actual changed EPG IDs for Source's explicit post-bulk dispatch.
    pub(crate) async fn epg_page(&self, after: i64) -> Result<Vec<(i64, Option<i64>)>, StoreError> {
        let table = self.rows.clone();
        self.session.run(move|c|Box::pin(async move{
            let rows=sqlx::query(&format!("SELECT ordinal,(draft->>'epg_data_id')::bigint AS epg_data_id FROM {table} WHERE ordinal>$1 ORDER BY ordinal LIMIT 500")).bind(after).fetch_all(c).await?;
            rows.into_iter().map(|row|Ok((row.try_get("ordinal")?,row.try_get("epg_data_id")?))).collect::<Result<Vec<_>,StoreError>>()
        })).await?
    }
    pub(crate) async fn close(self) -> Result<(), StoreError> {
        let table = self.rows;
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
