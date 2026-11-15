//! Account-scoped catchup rollup from Original Dispatcharr; same owned session.
use crate::{task_sql_session::TaskSqlSession, StoreError};
// Keep Source's two statement snapshots and account scope; no host-side list of
// all channel IDs and no extra pool checkout while this session is held.
pub(crate) async fn account(session: &TaskSqlSession, id: i64) -> Result<(), StoreError> {
    let result=session.run(move|c|Box::pin(async move{
        sqlx::query("WITH agg AS (SELECT cs.channel_id,bool_or(s.is_catchup AND a.is_active) AS any_catchup,MAX(s.catchup_days) FILTER (WHERE s.is_catchup AND a.is_active) AS max_days FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id WHERE cs.channel_id IN(SELECT DISTINCT ac.channel_id FROM dispatcharr_channels_channelstream ac JOIN dispatcharr_channels_stream ast ON ast.id=ac.stream_id WHERE ast.m3u_account_id=$1) GROUP BY cs.channel_id) UPDATE dispatcharr_channels_channel c SET is_catchup=COALESCE(agg.any_catchup,FALSE),catchup_days=COALESCE(agg.max_days,0) FROM agg WHERE c.id=agg.channel_id").bind(id).execute(&mut*c).await?;
        sqlx::query("UPDATE dispatcharr_channels_channel c SET is_catchup=FALSE,catchup_days=0 WHERE c.is_catchup=TRUE AND c.id IN(SELECT DISTINCT cs.channel_id FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id WHERE s.m3u_account_id=$1) AND NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id WHERE cs.channel_id=c.id AND s.is_catchup=TRUE AND a.is_active=TRUE)").bind(id).execute(&mut*c).await?;
        Ok::<_,StoreError>(())
    })).await?;
    // Source catches rollup exceptions after they actually occur. TaskSqlSession
    // observes Ready on this SAME connection before delivering inner errors.
    match result {
        Ok(()) => Ok(()),
        Err(StoreError::Database(sqlx::Error::Database(_))) => Ok(()),
        Err(error) => Err(error),
    }
}
