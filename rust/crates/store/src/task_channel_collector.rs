//! Source M3U stop-before-Collector deletion, bounded without splitting atomicity.
//! Original Dispatcharr/Django authors retain application and algorithm credit.
use crate::{
    recording_http::{RecordingEffectError, RecordingJobs, RecordingTaskAbsent},
    AuthService, StoreError, TaskSqlSession,
};
use dispatcharr_contracts::ChannelControl;
use sqlx::{PgConnection, Row};
use std::sync::Arc;
use uuid::Uuid;
const CHANNEL_PAGE: usize = 500;
const RECORDING_PAGE: usize = 512;
#[derive(Debug)]
pub struct ChannelDeletion {
    /// Source len(input channel instances), preserving repeated stream-map values.
    pub selected: u64,
    /// Actual UUID-bearing rows reobserved for those inputs (duplicates included).
    pub observed: u64,
    /// Actual Channel DELETE command-tag count, excluding cascades.
    pub deleted: u64,
    pub recording_rows: u64,
    pub stop_failures: u64,
    pub effect_outcome: Option<RecordingEffectError>,
}
fn retain_outcome(current: &mut Option<RecordingEffectError>, next: RecordingEffectError) {
    use RecordingEffectError::{SettledFailure, Unsettled, Unsupported};
    *current = Some(match (*current, next) {
        (Some(Unsettled), _) | (_, Unsettled) => Unsettled,
        (Some(Unsupported), _) | (_, Unsupported) => Unsupported,
        _ => SettledFailure,
    });
}
async fn idle(c: &mut PgConnection) -> Result<(), StoreError> {
    if !c.protocol_transaction_is_idle().await? {
        return Err(StoreError::Unsupported(
            "Channel Collector requires actual idle transaction state",
        ));
    }
    Ok(())
}
async fn reverse_relations(c: &mut PgConnection) -> Result<(), StoreError> {
    let rows:Vec<(String,String,String,i64)>=sqlx::query_as("SELECT f.conrelid::regclass::text,a.attname::text,p.attname::text,cardinality(f.conkey)::bigint FROM pg_constraint f JOIN pg_attribute a ON a.attrelid=f.conrelid AND a.attnum=f.conkey[1] JOIN pg_attribute p ON p.attrelid=f.confrelid AND p.attnum=f.confkey[1] WHERE f.contype='f' AND f.confrelid='dispatcharr_channels_channel'::regclass ORDER BY f.conname LIMIT 33").fetch_all(&mut*c).await?;
    let expected = [
        "dispatcharr_channels_channelstream",
        "dispatcharr_channels_channelprofilemembership",
        "dispatcharr_channels_channeloverride",
        "dispatcharr_channels_recording",
        "dispatcharr_channels_recurringrecordingrule",
    ];
    if rows.len() != expected.len()
        || rows.iter().any(|(table, column, parent, count)| {
            column != "channel_id"
                || parent != "id"
                || *count != 1
                || !expected.contains(&table.as_str())
        })
        || expected
            .iter()
            .any(|table| rows.iter().filter(|row| row.0 == *table).count() != 1)
    {
        return Err(StoreError::Unsupported(
            "Channel Collector reverse relations differ from pinned Source",
        ));
    }
    Ok(())
}
/// SQL-backed, operation-scoped selection on the caller's existing connection.
/// Not Clone: append/execute ordering belongs to one actual accepted task.
/// Dropping an unfinished selection never commits anything; caller owns final
/// TaskSqlSession physical closure, which also drops its private temp storage.
pub struct ChannelDeletionSelection {
    session: TaskSqlSession,
    control: Arc<dyn ChannelControl>,
    jobs: Arc<dyn RecordingJobs>,
    owner: Arc<dyn Send + Sync>,
    operation: Uuid,
    selected: u64,
    observed: u64,
}
impl AuthService {
    pub async fn prepare_task_channel_deletion(
        &self,
        session: TaskSqlSession,
        jobs: Arc<dyn RecordingJobs>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<ChannelDeletionSelection, StoreError> {
        let control = self.channel_control.clone().ok_or(StoreError::Unsupported(
            "Actual channel stop control is required before task deletion",
        ))?;
        session.run(|c|Box::pin(async move{
            idle(c).await?;reverse_relations(c).await?;
            // PRESERVE ROWS is required across bounded autocommit append pages.
            // No unbounded IDs/Recording instances are transferred into Rust.
            sqlx::query("CREATE TEMP TABLE IF NOT EXISTS native_task_channel_selection(operation uuid NOT NULL,ordinal bigint GENERATED ALWAYS AS IDENTITY,channel_id bigint NOT NULL,channel_uuid uuid NOT NULL) ON COMMIT PRESERVE ROWS").execute(&mut*c).await?;
            Ok::<(),StoreError>(())
        })).await??;
        Ok(ChannelDeletionSelection {
            session,
            control,
            jobs,
            owner,
            operation: Uuid::new_v4(),
            selected: 0,
            observed: 0,
        })
    }
    /// Convenience for one bounded page. Large Source selections must append all
    /// pages to ONE builder, then execute once (never delete page-by-page).
    pub async fn delete_task_channels(
        &self,
        session: TaskSqlSession,
        ids: Vec<i64>,
        jobs: Arc<dyn RecordingJobs>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<ChannelDeletion, StoreError> {
        let mut selected = self
            .prepare_task_channel_deletion(session, jobs, owner)
            .await?;
        selected.append(ids).await?;
        selected.execute().await
    }
}
impl ChannelDeletionSelection {
    pub async fn append(&mut self, ids: Vec<i64>) -> Result<(), StoreError> {
        if ids.len() > CHANNEL_PAGE || ids.iter().any(|id| *id <= 0) {
            return Err(StoreError::Unsupported(
                "Channel selection append requires bounded positive IDs",
            ));
        }
        let next = self
            .selected
            .checked_add(ids.len() as u64)
            .ok_or(StoreError::Unsupported("Channel selection count overflow"))?;
        let operation = self.operation;
        let count=self.session.run(move|c|Box::pin(async move{
            idle(c).await?;
            // WITH ORDINALITY preserves input repetitions and order. Source's
            // existing_channel_map.values() can contain one Channel repeatedly.
            let result=sqlx::query("INSERT INTO native_task_channel_selection(operation,channel_id,channel_uuid) SELECT $1,c.id,c.uuid FROM unnest($2::bigint[]) WITH ORDINALITY AS requested(id,position) JOIN dispatcharr_channels_channel c ON c.id=requested.id ORDER BY requested.position").bind(operation).bind(ids).execute(&mut*c).await?;
            Ok::<u64,StoreError>(result.rows_affected())
        })).await??;
        self.selected = next;
        self.observed = self
            .observed
            .checked_add(count)
            .ok_or(StoreError::Unsupported("Observed channel count overflow"))?;
        Ok(())
    }
    pub async fn execute(self) -> Result<ChannelDeletion, StoreError> {
        self.execute_with_stop(true).await
    }
    /// Source ChannelViewSet.destroy defaults to preserving active playback.
    /// Its optional stop_stream control already ran outside Collector atomic.
    pub(crate) async fn execute_without_stops(self) -> Result<ChannelDeletion, StoreError> {
        self.execute_with_stop(false).await
    }
    pub(crate) async fn execute_with_account(
        self,
        auth: AuthService,
        account: i64,
    ) -> Result<ChannelDeletion, StoreError> {
        self.execute_with_stop_and_account(true, Some((auth, account)))
            .await
    }
    async fn execute_with_stop(self, stop: bool) -> Result<ChannelDeletion, StoreError> {
        self.execute_with_stop_and_account(stop, None).await
    }
    async fn execute_with_stop_and_account(
        self,
        stop: bool,
        account: Option<(AuthService, i64)>,
    ) -> Result<ChannelDeletion, StoreError> {
        let operation = self.operation;
        let mut report = ChannelDeletion {
            selected: self.selected,
            observed: self.observed,
            deleted: 0,
            recording_rows: 0,
            stop_failures: 0,
            effect_outcome: None,
        };
        if stop {
            // The dynamically named cursor consists only of a generated UUID, never
            // input SQL. Every row is stopped before any Collector DELETE/BEGIN.
            let cursor = format!("native_channel_stop_{}", operation.simple());
            let declare=format!("DECLARE {cursor} NO SCROLL CURSOR WITH HOLD FOR SELECT channel_uuid FROM native_task_channel_selection WHERE operation=$1 ORDER BY ordinal");
            self.session
                .run(move |c| {
                    Box::pin(async move {
                        idle(c).await?;
                        reverse_relations(c).await?;
                        sqlx::query(&declare)
                            .bind(operation)
                            .execute(&mut *c)
                            .await?;
                        Ok::<(), StoreError>(())
                    })
                })
                .await??;
            loop {
                let statement = format!("FETCH FORWARD 500 FROM {cursor}");
                let rows: Vec<(Uuid,)> = self
                    .session
                    .run(move |c| {
                        Box::pin(async move {
                            idle(c).await?;
                            sqlx::query_as(&statement)
                                .fetch_all(&mut *c)
                                .await
                                .map_err(StoreError::from)
                        })
                    })
                    .await??;
                if rows.is_empty() {
                    break;
                }
                if rows.len() > CHANNEL_PAGE {
                    return Err(StoreError::Unsupported("Channel stop page exceeded"));
                }
                for (uuid,) in rows {
                    if let Err(error) = self.control.stop_channel(uuid.to_string()).await {
                        report.stop_failures += 1;
                        retain_outcome(
                            &mut report.effect_outcome,
                            if error.status >= 500 {
                                RecordingEffectError::Unsettled
                            } else {
                                RecordingEffectError::SettledFailure
                            },
                        );
                    }
                }
            }
            let close = format!("CLOSE {cursor}");
            self.session
                .run(move |c| {
                    Box::pin(async move {
                        sqlx::query(&close)
                            .execute(&mut *c)
                            .await
                            .map(|_| ())
                            .map_err(StoreError::from)
                    })
                })
                .await??;
        }
        let owner = self.owner;
        let jobs = self.jobs;
        self.session.run(move|c|Box::pin(async move{
            let _owner=owner.clone();idle(c).await?;
            sqlx::query("BEGIN").execute(&mut*c).await?;
            let result:Result<ChannelDeletion,StoreError>=async{
                sqlx::query("CREATE TEMP TABLE native_channel_recording_collector ON COMMIT DROP AS SELECT id::bigint AS id,task_id FROM dispatcharr_channels_recording WHERE channel_id IN(SELECT channel_id FROM native_task_channel_selection WHERE operation=$1)").bind(operation).execute(&mut*c).await?;
                report.recording_rows=sqlx::query("DELETE FROM dispatcharr_channels_recording WHERE id IN(SELECT id FROM native_channel_recording_collector)").execute(&mut*c).await?.rows_affected();
                sqlx::query("DECLARE native_channel_recording_signals NO SCROLL CURSOR FOR SELECT id,task_id FROM native_channel_recording_collector ORDER BY id DESC").execute(&mut*c).await?;
                loop{
                    let rows=sqlx::query("FETCH FORWARD 512 FROM native_channel_recording_signals").fetch_all(&mut*c).await?;
                    if rows.is_empty(){break;}
                    if rows.len()>RECORDING_PAGE{return Err(StoreError::Unsupported("Recording Collector page exceeded"));}
                    for row in rows{
                        let task:Option<String>=row.try_get("task_id")?;
                        let Some(task)=task.filter(|value|!value.is_empty())else{continue;};
                        if !crate::recording_http::delete_periodic_in_collector(c,&task).await?{
                            if let Err(error)=jobs.revoke_observed_absent(RecordingTaskAbsent::from_collector(task),owner.clone()).await{retain_outcome(&mut report.effect_outcome,error);}
                        }
                    }
                }
                sqlx::query("CLOSE native_channel_recording_signals").execute(&mut*c).await?;
                for statement in[
                    "DELETE FROM dispatcharr_channels_channelstream WHERE channel_id IN(SELECT channel_id FROM native_task_channel_selection WHERE operation=$1)",
                    "DELETE FROM dispatcharr_channels_channelprofilemembership WHERE channel_id IN(SELECT channel_id FROM native_task_channel_selection WHERE operation=$1)",
                    "DELETE FROM dispatcharr_channels_channeloverride WHERE channel_id IN(SELECT channel_id FROM native_task_channel_selection WHERE operation=$1)",
                    "DELETE FROM dispatcharr_channels_recurringrecordingrule WHERE channel_id IN(SELECT channel_id FROM native_task_channel_selection WHERE operation=$1)",
                ]{sqlx::query(statement).bind(operation).execute(&mut*c).await?;}
                report.deleted=sqlx::query("DELETE FROM dispatcharr_channels_channel WHERE id IN(SELECT channel_id FROM native_task_channel_selection WHERE operation=$1)").bind(operation).execute(&mut*c).await?.rows_affected();
                if let Some((auth,id))=account {
                    crate::provider_account_writes::delete_in_collector(c,auth,id,owner.clone()).await?;
                }
                // Same-channel rollup rows are deleted; no save/API file/event
                // hooks or RecurringRule ViewSet purge belong to Collector.
                sqlx::query("COMMIT").execute(&mut*c).await?;
                Ok(report)
            }.await;
            if result.is_err(){sqlx::query("ROLLBACK").execute(&mut*c).await?;}
            // Clean only this operation; another builder on the same session
            // never loses its own selected identities. Session closure is the
            // physical backstop if the connection itself becomes unavailable.
            sqlx::query("DELETE FROM native_task_channel_selection WHERE operation=$1").bind(operation).execute(&mut*c).await?;
            result
        })).await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{
        postgres::{PgConnectOptions, PgSslMode},
        Connection, Executor,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    async fn frame(socket: &mut TcpStream, tag: u8, bytes: &[u8]) {
        socket.write_all(&[tag]).await.unwrap();
        socket
            .write_all(&((bytes.len() + 4) as u32).to_be_bytes())
            .await
            .unwrap();
        socket.write_all(bytes).await.unwrap();
    }
    #[tokio::test]
    async fn actual_wire_idle_fence_observes_raw_readonly_and_aborted_transactions_without_sql() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let peer = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let length = socket.read_u32().await.unwrap() as usize;
                let mut startup = vec![0; length - 4];
                socket.read_exact(&mut startup).await.unwrap();
                frame(&mut socket, b'R', &0u32.to_be_bytes()).await;
                frame(&mut socket, b'S', b"server_version\x0017.0\x00").await;
                frame(&mut socket, b'S', b"client_encoding\x00UTF8\x00").await;
                frame(&mut socket, b'K', &[0, 0, 0, 1, 0, 0, 0, 1]).await;
                frame(&mut socket, b'Z', b"I").await;
                let mut queries = Vec::new();
                loop {
                    let tag = socket.read_u8().await.unwrap();
                    let length = socket.read_u32().await.unwrap() as usize;
                    let mut body = vec![0; length - 4];
                    socket.read_exact(&mut body).await.unwrap();
                    if tag == b'X' {
                        break;
                    }
                    assert_eq!(
                        tag, b'Q',
                        "fence unexpectedly emitted a non-query protocol operation"
                    );
                    let text = std::str::from_utf8(&body[..body.len() - 1])
                        .unwrap()
                        .to_owned();
                    queries.push(text.clone());
                    let status = match text.as_str() {
                        "BEGIN" | "BEGIN READ ONLY" => b'T',
                        "ROLLBACK" => b'I',
                        "BROKEN" => b'E',
                        other => panic!("unexpected SQL from idle fence: {other}"),
                    };
                    if status == b'E' {
                        frame(
                            &mut socket,
                            b'E',
                            b"SERROR\x00VERROR\x00C42601\x00Mretained syntax failure\x00\x00",
                        )
                        .await;
                    } else {
                        frame(
                            &mut socket,
                            b'C',
                            if status == b'T' {
                                b"BEGIN\x00"
                            } else {
                                b"ROLLBACK\x00"
                            },
                        )
                        .await;
                    }
                    frame(&mut socket, b'Z', &[status]).await;
                }
                queries
            });
            let options = PgConnectOptions::new()
                .host("127.0.0.1")
                .port(port)
                .username("owned-wire-peer")
                .database("owned-wire-peer")
                .ssl_mode(PgSslMode::Disable);
            let mut connection = PgConnection::connect_with(&options).await.unwrap();
            assert!(connection.protocol_transaction_is_idle().await.unwrap());
            connection.execute("BEGIN").await.unwrap();
            assert!(!connection.protocol_transaction_is_idle().await.unwrap());
            assert!(connection.execute("BROKEN").await.is_err());
            // ErrorResponse can leave pending ReadyForQuery; the actual fence must
            // drain it and distinguish aborted E from idle I without ROLLBACK.
            assert!(!connection.protocol_transaction_is_idle().await.unwrap());
            connection.execute("ROLLBACK").await.unwrap();
            assert!(connection.protocol_transaction_is_idle().await.unwrap());
            connection.execute("BEGIN READ ONLY").await.unwrap();
            assert!(!connection.protocol_transaction_is_idle().await.unwrap());
            connection.execute("ROLLBACK").await.unwrap();
            assert!(connection.protocol_transaction_is_idle().await.unwrap());
            connection.close().await.unwrap();
            let queries = tokio::time::timeout(std::time::Duration::from_secs(2), peer)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                queries,
                ["BEGIN", "BROKEN", "ROLLBACK", "BEGIN READ ONLY", "ROLLBACK"]
            );
        })
        .await
        .expect("bounded actual protocol-state peer");
    }
}
