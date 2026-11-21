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
#[ignore = "requires owned historical SQLite VOD migration fixtures"]
async fn compatible_vod_logo_adult_language_and_complete_history() {
    let root =
        std::path::PathBuf::from(std::env::var_os("DISPATCHARR_OWNED_SQLITE_VOD_UPGRADE").unwrap());
    let seed = std::fs::read_to_string(root.join("vod-fixture.sql")).unwrap();
    for (label, prior_steps) in [
        ("populated", 0),
        ("empty", 0),
        ("resume-181", 1),
        ("resume-182", 2),
        ("resume-183", 3),
        ("late-recorder", 0),
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
        assert_eq!(channels_identity::run(&path).await.unwrap(), 4);
        assert_eq!(channels_overrides::run(&path).await.unwrap(), 1);
        assert_eq!(channels_catchup::run(&path).await.unwrap(), 2);
        assert_eq!(channels_relations::run(&path).await.unwrap(), 1);

        if label != "empty" {
            fixture(&path, &seed).await;
        }
        if prior_steps > 0 {
            let opts = SqliteConnectOptions::new()
                .filename(&path)
                .foreign_keys(false);
            let mut connection = SqliteConnection::connect_with(&opts).await.unwrap();
            let mut tx = connection.begin().await.unwrap();
            for index in 0..prior_steps {
                apply_step(&mut tx, index).await.unwrap();
            }
            tx.commit().await.unwrap();
            connection.close().await.unwrap();
        }

        let before = std::fs::read(&path).unwrap();
        let outcome = run(&path).await;
        if label == "late-recorder" {
            let Failure::Database(error) = outcome.unwrap_err() else {
                panic!("expected scoped final migration recorder refusal");
            };
            assert_eq!(
                error.as_database_error().unwrap().code().as_deref(),
                Some("1811")
            );
            assert_eq!(std::fs::read(&path).unwrap(), before);
        } else {
            assert_eq!(outcome.unwrap(), STEPS.len() - prior_steps);
            let after = std::fs::read(&path).unwrap();
            assert_eq!(run(&path).await.unwrap(), 0);
            assert_eq!(std::fs::read(&path).unwrap(), after);
        }
    }
}
