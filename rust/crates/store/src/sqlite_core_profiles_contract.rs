use super::*;

#[tokio::test]
#[ignore = "requires detached historical SQLite Core profile migration clones"]
async fn core_profiles_settings_event_choices_resume_and_rollback() {
    let root = std::path::PathBuf::from(
        std::env::var_os("DISPATCHARR_OWNED_SQLITE_CORE_PROFILES").unwrap(),
    );
    let custom = std::fs::read_to_string(root.join("custom-output.sql")).unwrap();
    for (name, fixture, resume) in [
        ("cold", "cold", 0),
        ("explicit-resume-162", "explicit", 2),
        ("nonobjects", "nonobjects", 0),
        ("missing", "missing", 0),
        ("resume-161", "cold", 1),
        ("resume-163", "cold", 3),
        ("resume-164", "cold", 4),
        ("resume-165", "cold", 5),
        ("bad-proxy-null", "bad-proxy-null", 3),
        ("bad-proxy-text", "bad-proxy-text", 3),
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

        let opts = SqliteConnectOptions::new()
            .filename(&path)
            .foreign_keys(true);
        let mut connection = SqliteConnection::connect_with(&opts).await.unwrap();
        let mut tx = connection.begin().await.unwrap();
        let seed = std::fs::read_to_string(root.join(format!("{fixture}.sql"))).unwrap();
        for line in seed.lines() {
            sqlx::query(line).execute(&mut *tx).await.unwrap();
        }
        for (index, step) in STEPS.iter().copied().enumerate().take(resume) {
            apply_step(&mut tx, index, step).await.unwrap();
        }
        if fixture == "explicit" {
            sqlx::query(custom.trim_end())
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
        connection.close().await.unwrap();

        let prior = std::fs::read(&path).unwrap();
        let result = run(&path).await;
        match name {
            "bad-proxy-null" | "bad-proxy-text" => {
                assert!(matches!(result, Err(Failure::OutsideKnownState)));
                assert_eq!(std::fs::read(&path).unwrap(), prior);
            }
            "late-recorder" => {
                let Failure::Database(error) = result.unwrap_err() else {
                    panic!("expected recorder rejection after owned Core effects");
                };
                assert_eq!(
                    error.as_database_error().unwrap().code().as_deref(),
                    Some("1811")
                );
                assert_eq!(std::fs::read(&path).unwrap(), prior);
            }
            _ => {
                assert_eq!(result.unwrap(), STEPS.len() - resume);
                let complete = std::fs::read(&path).unwrap();
                assert_eq!(run(&path).await.unwrap(), 0);
                assert_eq!(std::fs::read(&path).unwrap(), complete);
            }
        }
    }
}
