use super::*;

async fn fixture(path: &Path, statements: &str) {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .foreign_keys(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    let mut tx = connection.begin().await.unwrap();
    for statement in statements.lines() {
        sqlx::query(statement).execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();
    connection.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires owned historical SQLite custom account relation fixtures"]
async fn channels_custom_account_and_relations_match_pinned_source() {
    let root = std::path::PathBuf::from(
        std::env::var_os("DISPATCHARR_OWNED_SQLITE_CHANNEL_RELATIONS").unwrap(),
    );
    let identity = std::fs::read_to_string(root.join("basis/basis/identity-fixture.sql")).unwrap();
    let overrides =
        std::fs::read_to_string(root.join("basis/basis/compatible-populated.sql")).unwrap();
    let catchup = std::fs::read_to_string(root.join("basis/compatible.sql")).unwrap();
    for (label, scenario) in [
        ("existing-locked", "existing-locked"),
        ("occupied-prefix", "occupied-prefix"),
        ("missing-custom", "missing-custom"),
        ("late-recorder", "occupied-prefix"),
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
        fixture(&path, &identity).await;
        assert_eq!(channels_identity::run(&path).await.unwrap(), 4);
        fixture(&path, &overrides).await;
        assert_eq!(channels_overrides::run(&path).await.unwrap(), 1);
        fixture(&path, &catchup).await;
        assert_eq!(channels_catchup::run(&path).await.unwrap(), 2);
        let statements = std::fs::read_to_string(root.join(format!("{scenario}.sql"))).unwrap();
        fixture(&path, &statements).await;

        let before = std::fs::read(&path).unwrap();
        let lower = clock();
        let result = run(&path).await;
        let upper = clock();
        std::fs::write(
            root.join(format!("{label}-clock.json")),
            serde_json::to_vec(&(lower, upper)).unwrap(),
        )
        .unwrap();
        if label == "late-recorder" {
            let Failure::Database(error) = result.unwrap_err() else {
                panic!("expected actual recorder refusal after private writes");
            };
            assert_eq!(
                error.as_database_error().unwrap().code().as_deref(),
                Some("1811")
            );
            assert_eq!(std::fs::read(&path).unwrap(), before);
        } else {
            assert_eq!(result.unwrap(), 1);
            let after = std::fs::read(&path).unwrap();
            assert_eq!(run(&path).await.unwrap(), 0);
            assert_eq!(std::fs::read(&path).unwrap(), after);
        }
    }
}
