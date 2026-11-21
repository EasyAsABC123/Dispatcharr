use super::*;

#[tokio::test]
#[ignore = "requires detached historical SQLite final Core migration clones"]
async fn original_last_core_profiles_and_proxy_setting_boundary() {
    let root =
        std::path::PathBuf::from(std::env::var_os("DISPATCHARR_OWNED_SQLITE_CORE_FINAL").unwrap());
    for (name, fixture, prefix) in [
        ("cold", "cold", 0),
        ("explicit", "explicit", 0),
        ("locked-case-variant", "locked-case-variant", 0),
        ("resume-167", "cold", 1),
        ("resume-168", "cold", 2),
        ("resume-169", "cold", 3),
        ("duplicate-stream-name", "duplicate-stream-name", 1),
        ("late-recorder", "cold", 0),
    ] {
        let path = root.join(format!("{name}.db"));
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

        let options = SqliteConnectOptions::new()
            .filename(&path)
            .foreign_keys(true);
        let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
        let mut tx = connection.begin().await.unwrap();
        let seed = std::fs::read_to_string(root.join(format!("{fixture}.sql"))).unwrap();
        for statement in seed.lines() {
            sqlx::query(statement).execute(&mut *tx).await.unwrap();
        }
        for (index, step) in STEPS.iter().copied().enumerate().take(prefix) {
            apply_step(&mut tx, index, step).await.unwrap();
        }
        tx.commit().await.unwrap();
        connection.close().await.unwrap();

        let before = std::fs::read(&path).unwrap();
        let result = run(&path).await;
        if name == "late-recorder" {
            let Failure::Database(error) = result.unwrap_err() else {
                panic!("expected late recorder refusal after actual Core work");
            };
            assert_eq!(
                error.as_database_error().unwrap().code().as_deref(),
                Some("1811")
            );
            assert_eq!(std::fs::read(&path).unwrap(), before);
        } else {
            assert_eq!(result.unwrap(), STEPS.len() - prefix);
            let after = std::fs::read(&path).unwrap();
            assert_eq!(run(&path).await.unwrap(), 0);
            assert_eq!(std::fs::read(&path).unwrap(), after);
        }
    }
}
