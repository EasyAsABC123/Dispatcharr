use super::*;

#[tokio::test]
#[ignore = "requires detached historical SQLite Connect migration clones"]
async fn pinned_connect_schema_history_data_and_rollback() {
    let root =
        std::path::PathBuf::from(std::env::var_os("DISPATCHARR_OWNED_SQLITE_CONNECT").unwrap());
    let seed = std::fs::read_to_string(root.join("source-fixture.sql")).unwrap();
    for name in [
        "cold",
        "resume-153",
        "resume-154",
        "resume-155",
        "resume-156",
        "late-recorder-refusal",
        "unknown-layout",
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

        let resume = match name {
            "resume-153" | "unknown-layout" => 1,
            "resume-154" => 2,
            "resume-155" => 3,
            "resume-156" => 4,
            _ => 0,
        };
        if resume > 0 {
            let opts = SqliteConnectOptions::new()
                .filename(&path)
                .foreign_keys(true);
            let mut connection = SqliteConnection::connect_with(&opts).await.unwrap();
            let mut tx = connection.begin().await.unwrap();
            for (index, step) in STEPS.iter().copied().enumerate().take(resume) {
                apply_step(&mut tx, index, step).await.unwrap();
                if index == 0 {
                    for statement in seed.lines() {
                        sqlx::query(statement).execute(&mut *tx).await.unwrap();
                    }
                }
            }
            if name == "unknown-layout" {
                sqlx::query("CREATE INDEX owned_extra_connect_index ON dispatcharr_connect_integration(name)")
                    .execute(&mut *tx).await.unwrap();
            }
            tx.commit().await.unwrap();
            connection.close().await.unwrap();
        }
        let before = std::fs::read(&path).unwrap();
        match name {
            "unknown-layout" => {
                assert!(matches!(run(&path).await, Err(Failure::OutsideKnownState)));
                assert_eq!(std::fs::read(&path).unwrap(), before);
            }
            "late-recorder-refusal" => {
                let Failure::Database(error) = run(&path).await.unwrap_err() else {
                    panic!("expected the fifth Connect recorder row to be refused");
                };
                assert_eq!(
                    error.as_database_error().unwrap().code().as_deref(),
                    Some("1811")
                );
                assert_eq!(std::fs::read(&path).unwrap(), before);
            }
            _ => {
                assert_eq!(run(&path).await.unwrap(), STEPS.len() - resume);
                let completed = std::fs::read(&path).unwrap();
                assert_eq!(run(&path).await.unwrap(), 0);
                assert_eq!(std::fs::read(&path).unwrap(), completed);
            }
        }
    }
}
