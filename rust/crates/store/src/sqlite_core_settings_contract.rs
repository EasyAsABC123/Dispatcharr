use super::*;

#[tokio::test]
#[ignore = "requires detached historical SQLite Core settings migration clones"]
async fn core_settings_notifications_and_user_limits_match_original() {
    let root = std::path::PathBuf::from(
        std::env::var_os("DISPATCHARR_OWNED_SQLITE_CORE_SETTINGS").unwrap(),
    );
    let notice_sql = std::fs::read_to_string(root.join("source-notifications.sql")).unwrap();
    for (name, zone) in [
        ("cold", ""),
        ("custom", "America/Chicago"),
        ("empty-clock", "America/Chicago"),
        ("invalid-clock", "not/a-real-zone"),
        ("proxy-collision", ""),
        ("network-collision", ""),
        ("malformed-offset", ""),
        ("nonfinite", ""),
        ("resume-158", "America/Chicago"),
        ("resume-159", "America/Chicago"),
        ("late-recorder", ""),
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

        let prefix = match name {
            "resume-158" => 1,
            "resume-159" => 2,
            _ => 0,
        };
        if prefix > 0 {
            let opts = SqliteConnectOptions::new()
                .filename(&path)
                .foreign_keys(false);
            let mut connection = SqliteConnection::connect_with(&opts).await.unwrap();
            let mut tx = connection.begin().await.unwrap();
            for i in 0..prefix {
                apply_step(&mut tx, i, zone).await.unwrap();
            }
            if prefix == 2 {
                for statement in notice_sql.lines() {
                    sqlx::query(statement).execute(&mut *tx).await.unwrap();
                }
            }
            tx.commit().await.unwrap();
            connection.close().await.unwrap();
        }

        let prior = std::fs::read(&path).unwrap();
        let result = run(&path, zone).await;
        match name {
            "proxy-collision" | "network-collision" | "late-recorder" => {
                let Failure::Database(error) = result.unwrap_err() else {
                    panic!("expected an actual SQLite collision or recorder refusal");
                };
                let expected = if name == "late-recorder" {
                    "1811"
                } else {
                    "2067"
                };
                assert_eq!(
                    error.as_database_error().unwrap().code().as_deref(),
                    Some(expected)
                );
                assert_eq!(std::fs::read(&path).unwrap(), prior);
            }
            "malformed-offset" | "nonfinite" => {
                assert!(matches!(result, Err(Failure::OutsideKnownState)));
                assert_eq!(std::fs::read(&path).unwrap(), prior);
            }
            _ => {
                assert_eq!(result.unwrap(), STEPS.len() - prefix);
                let completed = std::fs::read(&path).unwrap();
                assert_eq!(run(&path, zone).await.unwrap(), 0);
                assert_eq!(std::fs::read(&path).unwrap(), completed);
            }
        }
    }
}
