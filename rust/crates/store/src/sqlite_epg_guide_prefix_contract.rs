use super::*;

#[tokio::test]
#[ignore = "requires detached historical SQLite EPG migration clones"]
async fn historical_epg_guide_prefix_keeps_links_resumes_and_rolls_back() {
    let root = std::path::PathBuf::from(std::env::var_os("DISPATCHARR_OWNED_SQLITE_EPG").unwrap());
    for name in [
        "empty",
        "cold",
        "resume-141",
        "resume-142",
        "rollback",
        "unexpected",
    ] {
        let path = root.join(format!("{name}.db"));
        let historical = std::fs::read(&path).unwrap();
        assert!(matches!(run(&path).await, Err(Failure::OutsideKnownState)));
        assert_eq!(std::fs::read(&path).unwrap(), historical);
        assert_eq!(
            super::super::run(&path).await.unwrap(),
            Upgrade::AppliedBoth
        );
        assert_eq!(app_data::run(&path).await.unwrap(), 3);
        assert_eq!(core_event_profile::run(&path).await.unwrap(), 3);

        let prefix = match name {
            "resume-141" => 1,
            "resume-142" => 2,
            _ => 0,
        };
        if prefix > 0 {
            let opts = SqliteConnectOptions::new()
                .filename(&path)
                .foreign_keys(false);
            let mut connection = SqliteConnection::connect_with(&opts).await.unwrap();
            let mut tx = connection.begin().await.unwrap();
            for (statements, step) in SQL.into_iter().zip(STEPS).take(prefix) {
                for statement in statements.lines() {
                    sqlx::query(statement).execute(&mut *tx).await.unwrap();
                }
                record(&mut tx, step).await.unwrap();
            }
            if prefix == 2 {
                sqlx::query("UPDATE epg_epgsource SET priority=7 WHERE id=1")
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }
            tx.commit().await.unwrap();
            connection.close().await.unwrap();
        }

        let prior = std::fs::read(&path).unwrap();
        match name {
            "rollback" => {
                let Failure::Database(error) = run(&path).await.unwrap_err() else {
                    panic!("expected EPG 0023 recorder refusal");
                };
                assert_eq!(
                    error.as_database_error().unwrap().code().as_deref(),
                    Some("1811")
                );
                assert_eq!(std::fs::read(&path).unwrap(), prior);
            }
            "unexpected" => {
                assert!(matches!(run(&path).await, Err(Failure::OutsideKnownState)));
                assert_eq!(std::fs::read(&path).unwrap(), prior);
            }
            _ => {
                assert_eq!(run(&path).await.unwrap(), STEPS.len() - prefix);
                let completed = std::fs::read(&path).unwrap();
                assert_eq!(run(&path).await.unwrap(), 0);
                assert_eq!(std::fs::read(&path).unwrap(), completed);
            }
        }
    }
}
