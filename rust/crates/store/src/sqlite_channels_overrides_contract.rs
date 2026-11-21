use super::*;

async fn fixture(path: &Path, statements: &str) {
    let opts = SqliteConnectOptions::new()
        .filename(path)
        .foreign_keys(true);
    let mut connection = SqliteConnection::connect_with(&opts).await.unwrap();
    let mut tx = connection.begin().await.unwrap();
    for statement in statements.lines() {
        sqlx::query(statement).execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();
    connection.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires owned historical SQLite Channels override fixtures"]
async fn channels_override_compatible_orphans_match_and_rollback() {
    let root = std::path::PathBuf::from(
        std::env::var_os("DISPATCHARR_OWNED_SQLITE_CHANNEL_OVERRIDE").unwrap(),
    );
    let identity_fixture = std::fs::read_to_string(root.join("identity-fixture.sql")).unwrap();
    for (label, mode) in [
        ("no-orphans", "stock-empty"),
        ("populated", "compatible-populated"),
        ("late-recorder", "compatible-populated"),
    ] {
        let path = root.join(format!("{label}.db"));
        assert_eq!(
            super::super::run(&path).await.unwrap(),
            Upgrade::AppliedBoth
        );
        assert_eq!(app_data::run(&path).await.unwrap(), 3);
        assert_eq!(core_event_profile::run(&path).await.unwrap(), 3);
        assert_eq!(epg_guide_prefix::run(&path).await.unwrap(), 3);
        assert_eq!(epg_index::run(&path).await.unwrap(), 5);
        assert_eq!(plugins::run(&path).await.unwrap(), 3);
        assert_eq!(m3u_expiration::run(&path).await.unwrap(), 1);
        assert_eq!(connect::run(&path).await.unwrap(), 5);
        assert_eq!(core_settings::run(&path, "").await.unwrap(), 3);
        assert_eq!(core_profiles::run(&path).await.unwrap(), 6);
        assert_eq!(core_final::run(&path).await.unwrap(), 4);
        assert_eq!(channels_flags::run(&path).await.unwrap(), 2);
        fixture(&path, &identity_fixture).await;
        assert_eq!(channels_identity::run(&path).await.unwrap(), 4);
        let configured = std::fs::read_to_string(root.join(format!("{mode}.sql"))).unwrap();
        fixture(&path, &configured).await;
        let before = std::fs::read(&path).unwrap();

        let actual = run(&path).await;
        if label == "late-recorder" {
            let Failure::Database(error) = actual.unwrap_err() else {
                panic!("expected real SQLite migration recorder refusal");
            };
            assert_eq!(
                error.as_database_error().unwrap().code().as_deref(),
                Some("1811")
            );
            assert_eq!(std::fs::read(&path).unwrap(), before);
        } else {
            assert_eq!(actual.unwrap(), 1);
            let after = std::fs::read(&path).unwrap();
            assert_eq!(run(&path).await.unwrap(), 0);
            assert_eq!(std::fs::read(&path).unwrap(), after);
        }
    }
}
