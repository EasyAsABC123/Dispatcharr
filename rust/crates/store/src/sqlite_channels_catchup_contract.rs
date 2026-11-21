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
#[ignore = "requires owned historical SQLite catch-up and radio fixtures"]
async fn channels_catchup_radio_matches_compatible_original() {
    let root = std::path::PathBuf::from(
        std::env::var_os("DISPATCHARR_OWNED_SQLITE_CHANNEL_CATCHUP").unwrap(),
    );
    let identity_fixture =
        std::fs::read_to_string(root.join("basis/identity-fixture.sql")).unwrap();
    let orphans = std::fs::read_to_string(root.join("basis/compatible-populated.sql")).unwrap();
    for (label, scope) in [
        ("compatible", "compatible"),
        ("resume-178", "compatible"),
        ("overflow", "overflow"),
        ("late-recorder", "compatible"),
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
        fixture(&path, &orphans).await;
        assert_eq!(channels_overrides::run(&path).await.unwrap(), 1);
        let statements = std::fs::read_to_string(root.join(format!("{scope}.sql"))).unwrap();
        fixture(&path, &statements).await;
        if label == "resume-178" {
            let opts = SqliteConnectOptions::new()
                .filename(&path)
                .foreign_keys(false);
            let mut connection = SqliteConnection::connect_with(&opts).await.unwrap();
            let mut tx = connection.begin().await.unwrap();
            apply_step(&mut tx, 0).await.unwrap();
            tx.commit().await.unwrap();
            connection.close().await.unwrap();
        }

        let before = std::fs::read(&path).unwrap();
        let result = run(&path).await;
        match label {
            "overflow" => {
                assert!(matches!(result, Err(Failure::OutsideKnownState)));
                assert_eq!(std::fs::read(&path).unwrap(), before);
            }
            "late-recorder" => {
                let Failure::Database(error) = result.unwrap_err() else {
                    panic!("expected SQLite recorder refusal after both copied steps");
                };
                assert_eq!(
                    error.as_database_error().unwrap().code().as_deref(),
                    Some("1811")
                );
                assert_eq!(std::fs::read(&path).unwrap(), before);
            }
            _ => {
                assert_eq!(result.unwrap(), if label == "resume-178" { 1 } else { 2 });
                let after = std::fs::read(&path).unwrap();
                assert_eq!(run(&path).await.unwrap(), 0);
                assert_eq!(std::fs::read(&path).unwrap(), after);
            }
        }
    }
}
