//! Source per-group logo/EPG/profile resolution on the existing owned session.
//! Original Dispatcharr authors retain all application/algorithm credit.
//! Draft component; generic regex/sort/orchestration and acceptance pending.
use crate::{task_sql_session::TaskSqlSession, StoreError};
use sqlx::Row;
use uuid::Uuid;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Logo {
    Absent,
    Rejected,
    Use(i64),
}
pub(crate) struct Resolver {
    session: TaskSqlSession,
    logos: String,
    epgs: String,
    custom_logo: Option<i64>,
    custom_source: Option<String>,
    dummy_epg: Option<i64>,
    force_dummy: bool,
}
impl Resolver {
    /// `streams` is the crate-private, generated actual snapshot relation. No
    /// untrusted SQL names or alternate connection enter this method.
    pub(crate) async fn prepare(
        session: TaskSqlSession,
        streams: &super::auto_sync_streams::Streams,
        custom_logo: Option<i64>,
        custom_epg: Option<i64>,
        force_dummy: bool,
    ) -> Result<Self, StoreError> {
        let logos = format!("native_auto_logos_{}", Uuid::new_v4().simple());
        let epgs = format!("{logos}_epgs");
        let l = logos.clone();
        let e = epgs.clone();
        let selected = streams.processed_relation().to_owned();
        let context=session.run(move|c|Box::pin(async move{
            let custom_logo=match custom_logo {Some(id)=>sqlx::query_scalar::<_,i64>("SELECT id::bigint FROM dispatcharr_channels_logo WHERE id=$1").bind(id).fetch_optional(&mut*c).await?,None=>None};
            let source=match custom_epg {Some(id)=>sqlx::query("SELECT id::bigint,source_type FROM epg_epgsource WHERE id=$1").bind(id).fetch_optional(&mut*c).await?,None=>None};
            let (source_id,source_type)=match source{Some(row)=>(Some(row.try_get::<i64,_>("id")?),Some(row.try_get::<String,_>("source_type")?)),None=>(None,None)};
            let dummy=if source_type.as_deref()==Some("dummy") {
                sqlx::query_scalar::<_,i64>("SELECT id::bigint FROM epg_epgdata WHERE epg_source_id=$1 ORDER BY id LIMIT 1").bind(source_id).fetch_optional(&mut*c).await?
            }else{None};
            sqlx::query(&format!("CREATE TEMP TABLE {l}(url text PRIMARY KEY,id bigint NOT NULL) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
            sqlx::query(&format!("INSERT INTO {l}(url,id) SELECT lg.url,lg.id::bigint FROM dispatcharr_channels_logo lg WHERE lg.url IN(SELECT logo_url FROM {selected} WHERE logo_url IS NOT NULL AND logo_url<>'' AND octet_length(logo_url)<=2000)")).execute(&mut*c).await?;
            sqlx::query(&format!("CREATE TEMP TABLE {e}(ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,tvg_id text,id bigint NOT NULL) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
            if !force_dummy || custom_epg.is_some() {
                let scope=if source_type.as_deref().is_some_and(|t|t!="dummy"){source_id}else{None};
                sqlx::query(&format!("INSERT INTO {e}(tvg_id,id) SELECT d.tvg_id,d.id::bigint FROM epg_epgdata d WHERE d.tvg_id IN(SELECT tvg_id FROM {selected} WHERE tvg_id IS NOT NULL AND tvg_id<>'') AND($1::bigint IS NULL OR d.epg_source_id=$1)")).bind(scope).execute(&mut*c).await?;
            }
            sqlx::query(&format!("CREATE INDEX ON {e}(tvg_id,ordinal DESC)")).execute(c).await?;
            Ok::<_,StoreError>((custom_logo,source_type,dummy))
        })).await??;
        Ok(Self {
            session,
            logos,
            epgs,
            custom_logo: context.0,
            custom_source: context.1,
            dummy_epg: context.2,
            force_dummy,
        })
    }
    pub(crate) async fn logo(
        &self,
        url: Option<&str>,
        rejected: bool,
        name: &str,
        tvg_id: Option<&str>,
    ) -> Result<Logo, StoreError> {
        if let Some(id) = self.custom_logo {
            return Ok(Logo::Use(id));
        }
        if rejected {
            return Ok(Logo::Rejected);
        }
        let Some(url) = url.filter(|u| !u.is_empty()) else {
            return Ok(Logo::Absent);
        };
        // Source validate_logo_url is a byte/type index guard, NOT an HTTP
        // validity probe. Rejected provider data must preserve existing logos.
        if url.len() > 2000 {
            return Ok(Logo::Rejected);
        }
        let url = url.to_owned();
        let name = if !name.is_empty() {
            name.to_owned()
        } else {
            tvg_id
                .filter(|id| !id.is_empty())
                .unwrap_or("Unknown")
                .to_owned()
        };
        let table = self.logos.clone();
        self.session.run(move|c|Box::pin(async move{
            if let Some(id)=sqlx::query_scalar::<_,i64>(&format!("SELECT id FROM {table} WHERE url=$1")).bind(&url).fetch_optional(&mut*c).await?{return Ok(Logo::Use(id))}
            // Do NOT clamp the Source defaults name or rename an existing Logo.
            // Actual insert errors are caught by the per-stream caller only
            // after Ready is observed, matching Source get_or_create behavior.
            sqlx::query("INSERT INTO dispatcharr_channels_logo(name,url) VALUES($1,$2) ON CONFLICT(url) DO NOTHING").bind(name).bind(&url).execute(&mut*c).await?;
            let id:i64=sqlx::query_scalar("SELECT id::bigint FROM dispatcharr_channels_logo WHERE url=$1").bind(&url).fetch_one(&mut*c).await?;
            sqlx::query(&format!("INSERT INTO {table}(url,id) VALUES($1,$2) ON CONFLICT DO NOTHING")).bind(url).bind(id).execute(c).await?;
            Ok::<_,StoreError>(Logo::Use(id))
        })).await?
    }
    pub(crate) async fn epg(&self, tvg_id: Option<&str>) -> Result<Option<i64>, StoreError> {
        if self.custom_source.as_deref() == Some("dummy") {
            return Ok(self.dummy_epg);
        }
        if self.custom_source.is_none() && self.force_dummy {
            return Ok(None);
        }
        let Some(tvg_id) = tvg_id.filter(|id| !id.is_empty()) else {
            return Ok(None);
        };
        let table = self.epgs.clone();
        let tvg_id = tvg_id.to_owned();
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query_scalar::<_, i64>(&format!(
                        "SELECT id FROM {table} WHERE tvg_id=$1 ORDER BY ordinal DESC LIMIT 1"
                    ))
                    .bind(tvg_id)
                    .fetch_optional(c)
                    .await
                    .map_err(StoreError::from)
                })
            })
            .await?
    }
    pub(crate) async fn close(self) -> Result<(), StoreError> {
        let logos = self.logos;
        let epgs = self.epgs;
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query(&format!("DROP TABLE {logos},{epgs}"))
                        .execute(c)
                        .await?;
                    Ok::<_, StoreError>(())
                })
            })
            .await?
    }
}
