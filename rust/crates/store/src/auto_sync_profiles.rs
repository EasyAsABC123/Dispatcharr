//! Source per-group ChannelProfile snapshot and membership reconciliation.
//! Original Dispatcharr authors retain all application/algorithm credit.
//! Draft component; no runtime or default-graph acceptance is claimed.
use crate::{task_sql_session::TaskSqlSession, StoreError};
pub(crate) struct Profiles {
    session: TaskSqlSession,
    table: Option<String>,
}
impl Profiles {
    pub(crate) async fn capture(
        session: TaskSqlSession,
        ids: Option<Vec<i64>>,
        skip: bool,
    ) -> Result<Self, StoreError> {
        if skip {
            return Ok(Self {
                session,
                table: None,
            });
        }
        let table = format!("native_auto_profiles_{}", uuid::Uuid::new_v4().simple());
        let rows = table.clone();
        session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("CREATE TEMP TABLE {rows}(id bigint PRIMARY KEY) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
            // Capture once BEFORE renumber/stream work, just like Source's
            // profiles_to_assign=list(...). Do not observe later profile adds.
            sqlx::query(&format!("INSERT INTO {rows}(id) SELECT id::bigint FROM dispatcharr_channels_channelprofile WHERE $1::bigint[] IS NULL OR id=ANY($1)")).bind(ids).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await??;
        Ok(Self {
            session,
            table: Some(table),
        })
    }
    pub(crate) fn relation(&self) -> Option<&str> {
        self.table.as_deref()
    }
    pub(crate) async fn reconcile(
        &self,
        existing: &super::auto_sync_existing::Existing,
    ) -> Result<(), StoreError> {
        let Some(profiles) = self.table.clone() else {
            return Ok(());
        };
        let channels = existing.relation().to_owned();
        self.session.run(move|c|Box::pin(async move{
            // Source reconciles every canonical existing map Channel, including
            // hidden rows. It flips only enabled and inserts missing targets;
            // unrelated memberships are not deleted or reported as dirty names.
            sqlx::query(&format!("UPDATE dispatcharr_channels_channelprofilemembership m SET enabled=EXISTS(SELECT 1 FROM {profiles} p WHERE p.id=m.channel_profile_id) WHERE m.channel_id IN(SELECT id FROM {channels}) AND m.enabled IS DISTINCT FROM EXISTS(SELECT 1 FROM {profiles} p WHERE p.id=m.channel_profile_id)")).execute(&mut*c).await?;
            sqlx::query(&format!("INSERT INTO dispatcharr_channels_channelprofilemembership(channel_id,channel_profile_id,enabled) SELECT c.id,p.id,true FROM {channels} c CROSS JOIN {profiles} p ON CONFLICT DO NOTHING")).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await?
    }
    pub(crate) async fn close(self) -> Result<(), StoreError> {
        let Some(table) = self.table else {
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
