use super::*;

#[tokio::test]
#[ignore = "requires detached historical SQLite Core migration clones"]
async fn early_core_event_indexes_and_vlc_primary_fallback_skip_and_rollback() {
    let root = std::path::PathBuf::from(std::env::var_os("DISPATCHARR_OWNED_SQLITE_CORE").unwrap());
    for name in [
        "cold.db",
        "fallback.db",
        "no-agent.db",
        "existing-vlc.db",
        "rollback.db",
    ] {
        let path = root.join(name);
        assert_eq!(
            super::super::run(&path).await.unwrap(),
            Upgrade::AppliedBoth
        );
        assert_eq!(app_data::run(&path).await.unwrap(), 3);
    }
    let cold = root.join("cold.db");
    assert_eq!(run(&cold).await.unwrap(), 3);
    let completed = std::fs::read(&cold).unwrap();
    assert_eq!(run(&cold).await.unwrap(), 0);
    assert_eq!(std::fs::read(&cold).unwrap(), completed);

    for name in ["fallback.db", "no-agent.db", "existing-vlc.db"] {
        assert_eq!(run(&root.join(name)).await.unwrap(), 3);
    }

    let resume = root.join("resume-138.db");
    assert_eq!(
        super::super::run(&resume).await.unwrap(),
        Upgrade::AppliedBoth
    );
    assert_eq!(app_data::run(&resume).await.unwrap(), 3);
    // Create a genuine 138 intermediate on this connection and stop before
    // the state-only choice edit or VLC insert.
    let opts = SqliteConnectOptions::new()
        .filename(&resume)
        .foreign_keys(true);
    let mut connection = SqliteConnection::connect_with(&opts).await.unwrap();
    let mut tx = connection.begin().await.unwrap();
    for statement in EVENT_DDL.lines() {
        sqlx::query(statement).execute(&mut *tx).await.unwrap();
    }
    record(&mut tx, STEPS[0]).await.unwrap();
    sqlx::query("INSERT INTO core_systemevent(event_type,timestamp,details) VALUES('owned','2020-01-01 00:00:00','{}')")
        .execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    connection.close().await.unwrap();
    assert_eq!(run(&resume).await.unwrap(), 2);

    let blocked = root.join("rollback.db");
    let before = std::fs::read(&blocked).unwrap();
    let Failure::Database(error) = run(&blocked).await.unwrap_err() else {
        panic!("expected SQLite recorder refusal after actual event and profile writes");
    };
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("1811")
    );
    assert_eq!(std::fs::read(&blocked).unwrap(), before);
}
