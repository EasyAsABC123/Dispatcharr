//! Prepared copied-SQLite relation hook for single-stream channel creation.
//! Original Dispatcharr authors retain the roll-up algorithm and application credit.
//! This does not admit a database, insert a channel/link, commit, or publish.
use crate::StoreError;
use sqlx::{Sqlite, Transaction};

const ROLLUP: &str = r#"
    UPDATE dispatcharr_channels_channel AS c
    SET is_catchup=EXISTS(
        SELECT 1 FROM dispatcharr_channels_channelstream cs
        JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id
        JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id
        WHERE cs.channel_id=c.id AND s.is_catchup AND a.is_active
    ), catchup_days=COALESCE((
        SELECT max(s.catchup_days) FROM dispatcharr_channels_channelstream cs
        JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id
        JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id
        WHERE cs.channel_id=c.id AND s.is_catchup AND a.is_active
    ),0)
    WHERE c.id=?1
"#;

/// Call after a ChannelStream insertion/deletion, inside its caller-owned write
/// transaction. Only the selected channel's stored roll-up fields are updated.
/// Keep the serializer's original object snapshot: Source's queryset update
/// does not refresh it, so a later GET can differ from the create response.
#[allow(dead_code)] // Prepared component; no partial copied-SQLite creation route is exposed.
pub(crate) async fn after_link_change(
    tx: &mut Transaction<'_, Sqlite>,
    channel_id: i64,
) -> Result<u64, StoreError> {
    Ok(sqlx::query(ROLLUP)
        .bind(channel_id)
        .execute(&mut **tx)
        .await?
        .rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use sqlx::{sqlite::SqliteConnectOptions, Connection, Row, SqliteConnection};
    use std::path::Path;

    // Exact existing captured application DDL, not a reduced invented schema
    // or an application donor. No migration records/admission are fabricated.
    const CATALOG: &str = include_str!("../datasets/sqlite-operator-upgraded-catalog.json");

    async fn schema(db: &mut SqliteConnection) {
        let catalog: Vec<Vec<Value>> = serde_json::from_str(CATALOG).unwrap();
        for kind in ["table", "index"] {
            for row in &catalog {
                if row[0] == kind {
                    sqlx::raw_sql(row[3].as_str().unwrap())
                        .execute(&mut *db)
                        .await
                        .unwrap();
                }
            }
        }
        let tables: i64 =
            sqlx::query_scalar("SELECT count(*) FROM sqlite_schema WHERE type='table'")
                .fetch_one(&mut *db)
                .await
                .unwrap();
        assert_eq!(tables, 63); // 62 captured application tables plus sqlite_sequence.
        let migrations: i64 = sqlx::query_scalar("SELECT count(*) FROM django_migrations")
            .fetch_one(&mut *db)
            .await
            .unwrap();
        assert_eq!(migrations, 0); // This is only a unit database, never an admitted donor.
    }

    async fn seed(db: &mut SqliteConnection) {
        sqlx::raw_sql(
            r#"
            INSERT INTO m3u_m3uaccount
              (id,name,max_streams,is_active,created_at,locked,refresh_interval,
               stale_stream_days,account_type,status,priority)
            VALUES (1,'active-unit',0,1,'2026-01-01',0,0,0,'STD','idle',0),
                   (2,'inactive-unit',0,0,'2026-01-01',0,0,0,'STD','idle',0);
            INSERT INTO dispatcharr_channels_stream
              (id,name,current_viewers,updated_at,is_custom,last_seen,is_stale,
               is_adult,is_catchup,catchup_days,is_radio,m3u_account_id)
            VALUES (1,'short-unit',0,'2026-01-01',0,'2026-01-01',0,0,1,3,0,1),
                   (2,'long-unit',0,'2026-01-01',0,'2026-01-01',0,0,1,7,0,1),
                   (3,'inactive-unit',0,'2026-01-01',0,'2026-01-01',0,0,1,99,0,2),
                   (4,'no-provider-unit',0,'2026-01-01',0,'2026-01-01',0,0,1,88,0,NULL),
                   (5,'not-catchup-unit',0,'2026-01-01',0,'2026-01-01',0,0,0,77,0,1);
            INSERT INTO dispatcharr_channels_channel
              (id,name,uuid,user_level,auto_created,created_at,updated_at,is_adult,
               hidden_from_output,is_catchup,catchup_days,is_radio)
            VALUES (1,'selected-unit','00000000000000000000000000000001',0,0,
                    '2026-01-01','2026-01-01',0,0,0,0,0),
                   (2,'empty-unit','00000000000000000000000000000002',0,0,
                    '2026-01-01','2026-01-01',0,0,1,42,0),
                   (3,'untouched-unit','00000000000000000000000000000003',0,0,
                    '2026-01-01','2026-01-01',0,0,1,55,0);
        "#,
        )
        .execute(db)
        .await
        .unwrap();
    }

    async fn flags(db: &mut SqliteConnection, id: i64) -> (bool, i64) {
        sqlx::query_as(
            "SELECT is_catchup,catchup_days FROM dispatcharr_channels_channel WHERE id=?",
        )
        .bind(id)
        .fetch_one(db)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn sqlite_relation_rollup_matches_active_source_predicate_and_preserves_other_fields() {
        let mut db = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .in_memory(true)
                .foreign_keys(true),
        )
        .await
        .unwrap();
        schema(&mut db).await;
        seed(&mut db).await;
        sqlx::raw_sql("INSERT INTO dispatcharr_channels_channelstream(channel_id,stream_id,\"order\") VALUES (1,1,0),(1,2,1),(1,3,2),(1,4,3),(1,5,4)").execute(&mut db).await.unwrap();
        let captured_object = flags(&mut db, 1).await;
        let schema_before: Vec<(String, String)> = sqlx::query_as(
            "SELECT name,sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY name",
        )
        .fetch_all(&mut db)
        .await
        .unwrap();
        let mut tx = db.begin_with("BEGIN IMMEDIATE").await.unwrap();
        assert_eq!(after_link_change(&mut tx, 1).await.unwrap(), 1);
        assert_eq!(after_link_change(&mut tx, 2).await.unwrap(), 1);
        assert_eq!(after_link_change(&mut tx, 999).await.unwrap(), 0);
        tx.commit().await.unwrap();
        assert_eq!(captured_object, (false, 0));
        assert_eq!(flags(&mut db, 1).await, (true, 7));
        assert_eq!(flags(&mut db, 2).await, (false, 0));
        assert_eq!(flags(&mut db, 3).await, (true, 55));
        let row =
            sqlx::query("SELECT name,updated_at FROM dispatcharr_channels_channel WHERE id=1")
                .fetch_one(&mut db)
                .await
                .unwrap();
        assert_eq!(row.get::<String, _>("name"), "selected-unit");
        assert_eq!(row.get::<String, _>("updated_at"), "2026-01-01");
        let schema_after: Vec<(String, String)> = sqlx::query_as(
            "SELECT name,sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY name",
        )
        .fetch_all(&mut db)
        .await
        .unwrap();
        assert_eq!(schema_before, schema_after);
        assert!(sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&mut db)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn sqlite_relation_hook_tracks_deletion_deactivation_and_caller_rollback() {
        let mut db = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .in_memory(true)
                .foreign_keys(true),
        )
        .await
        .unwrap();
        schema(&mut db).await;
        seed(&mut db).await;
        let mut tx = db.begin_with("BEGIN IMMEDIATE").await.unwrap();
        sqlx::query("INSERT INTO dispatcharr_channels_channelstream(channel_id,stream_id,\"order\") VALUES(1,2,0)").execute(&mut *tx).await.unwrap();
        after_link_change(&mut tx, 1).await.unwrap();
        assert_eq!(flags(&mut tx, 1).await, (true, 7));
        tx.rollback().await.unwrap();
        assert_eq!(flags(&mut db, 1).await, (false, 0));
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM dispatcharr_channels_channelstream")
                .fetch_one(&mut db)
                .await
                .unwrap();
        assert_eq!(count, 0);
        let mut tx = db.begin_with("BEGIN IMMEDIATE").await.unwrap();
        sqlx::query("INSERT INTO dispatcharr_channels_channelstream(channel_id,stream_id,\"order\") VALUES(1,1,0),(1,2,1)").execute(&mut *tx).await.unwrap();
        after_link_change(&mut tx, 1).await.unwrap();
        sqlx::query(
            "DELETE FROM dispatcharr_channels_channelstream WHERE channel_id=1 AND stream_id=2",
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        after_link_change(&mut tx, 1).await.unwrap();
        assert_eq!(flags(&mut tx, 1).await, (true, 3));
        sqlx::query("UPDATE m3u_m3uaccount SET is_active=0 WHERE id=1")
            .execute(&mut *tx)
            .await
            .unwrap();
        after_link_change(&mut tx, 1).await.unwrap();
        assert_eq!(flags(&mut tx, 1).await, (false, 0));
        tx.commit().await.unwrap();
    }

    #[tokio::test]
    async fn sqlite_relation_hook_commit_reopens_and_uniqueness_prevents_duplicate_link() {
        let path = std::env::temp_dir().join(format!(
            "dispatcharr-relation-unit-{}.db",
            uuid::Uuid::new_v4()
        ));
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .foreign_keys(true);
        let mut db = SqliteConnection::connect_with(&options).await.unwrap();
        schema(&mut db).await;
        seed(&mut db).await;
        let mut tx = db.begin_with("BEGIN IMMEDIATE").await.unwrap();
        sqlx::query("INSERT INTO dispatcharr_channels_channelstream(channel_id,stream_id,\"order\") VALUES(1,1,0)").execute(&mut *tx).await.unwrap();
        after_link_change(&mut tx, 1).await.unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();
        let mut db = SqliteConnection::connect_with(&options).await.unwrap();
        assert_eq!(flags(&mut db, 1).await, (true, 3));
        let error=sqlx::query("INSERT INTO dispatcharr_channels_channelstream(channel_id,stream_id,\"order\") VALUES(1,1,0)").execute(&mut db).await.unwrap_err();
        assert!(error.as_database_error().unwrap().is_unique_violation());
        assert_eq!(flags(&mut db, 1).await, (true, 3));
        db.close().await.unwrap();
        std::fs::remove_file(&path).unwrap();
        for suffix in ["-wal", "-shm", "-journal"] {
            assert!(!Path::new(&format!("{}{suffix}", path.display())).exists());
        }
    }
}
