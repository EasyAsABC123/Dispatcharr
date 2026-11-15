//! Source orphan auto-channel selection, captured before any stop/delete effect.
//! Original Dispatcharr authors retain all application/algorithm credit.
//! Draft component; no standalone/Main acceptance has been established.
use crate::{task_sql_session::TaskSqlSession, StoreError};
use sqlx::Row;
pub(crate) struct Orphans {
    session: TaskSqlSession,
    table: String,
}
impl Orphans {
    pub(crate) async fn capture(
        session: TaskSqlSession,
        account: i64,
        preserve_customized: bool,
    ) -> Result<Self, StoreError> {
        let table = format!("native_auto_orphans_{}", uuid::Uuid::new_v4().simple());
        let name = table.clone();
        session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("CREATE TEMP TABLE {name}(ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,id bigint) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
            sqlx::query(&format!("INSERT INTO {name}(id) SELECT c.id::bigint FROM dispatcharr_channels_channel c WHERE c.auto_created AND c.auto_created_by_id=$1 AND NOT c.hidden_from_output AND NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id WHERE cs.channel_id=c.id AND s.m3u_account_id=$1) AND(NOT $2 OR NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channeloverride o WHERE o.channel_id=c.id))")).bind(account).bind(preserve_customized).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await??;
        Ok(Self { session, table })
    }
    pub(crate) async fn next(&self, after: i64) -> Result<Vec<(i64, i64)>, StoreError> {
        let table = self.table.clone();
        self.session.run(move|c|Box::pin(async move{
            let rows=sqlx::query(&format!("SELECT ordinal,id FROM {table} WHERE ordinal>$1 ORDER BY ordinal LIMIT 500")).bind(after).fetch_all(c).await?;
            rows.into_iter().map(|row|Ok((row.try_get("ordinal")?,row.try_get("id")?))).collect::<Result<Vec<_>,StoreError>>()
        })).await?
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
