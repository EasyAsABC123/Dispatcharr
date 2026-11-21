//! Owned physical file witnesses for the selected scoped SQLite readiness.
use super::*;

#[tokio::test]
#[ignore = "requires owned SQLite readiness files"]
async fn exact_owned_sqlite_readiness_uses_actual_selected_connection() {
    let root =
        std::path::PathBuf::from(std::env::var_os("DISPATCHARR_SQLITE_READY_COPIES").unwrap());
    let admin = std::env::var("DISPATCHARR_SQLITE_READY_ADMIN").unwrap();
    for name in ["historical.db", "partial.db", "same-count-wrong-history.db"] {
        let result = SqliteIdentity::open_current_login_groups(&root.join(name)).await;
        assert!(
            matches!(
                result,
                Err(StoreError::Unsupported(
                    "SQLite login and groups require the pinned migration history"
                ))
            ),
            "{name} must fail the migration gate"
        );
    }
    let absent = root.join("does-not-exist.db");
    assert!(SqliteIdentity::open_current_login_groups(&absent)
        .await
        .is_err());
    assert!(!absent.exists());
    assert!(matches!(
        SqliteIdentity::open_current_login_groups(&root.join("missing-table.db")).await,
        Err(StoreError::Unsupported(
            "SQLite login and groups schema tables are missing"
        ))
    ));
    assert!(
        SqliteIdentity::open_current_login_groups(&root.join("missing-column.db"))
            .await
            .is_err()
    );

    for name in ["current.db", "current-fresh-catalog.db"] {
        let path = root.join(name);
        let identity = SqliteIdentity::open_current_login_groups(&path)
            .await
            .unwrap();
        let actor = if name == "current.db" {
            admin.as_str()
        } else {
            "owned-admin"
        };
        let user = identity.user_by_username(actor).await.unwrap().unwrap();
        assert_eq!(user.user_level, 10);
        let mut connection = identity.pool().acquire().await.unwrap();
        let database: (i64, String, String) = sqlx::query_as("PRAGMA database_list")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(database.1, "main");
        assert_eq!(std::fs::canonicalize(database.2).unwrap(), path);
        let groups = crate::channel_groups::sqlite::collection(&mut connection, &user)
            .await
            .unwrap();
        assert!(groups.len() >= 5);
        assert!(groups.iter().any(|group| group["m3u_account_count"] == 1));
        // The admitted runtime pool can reserve a write transaction without
        // mutating either copy during the readiness test.
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .unwrap();
        drop(connection);
        identity.close().await;
    }
}
