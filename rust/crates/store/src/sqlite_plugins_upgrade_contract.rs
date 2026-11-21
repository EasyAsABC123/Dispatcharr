use super::*;

fn boundary() -> chrono::NaiveDateTime {
    let now = Utc::now();
    now.naive_utc() - chrono::Duration::nanoseconds(i64::from(now.timestamp_subsec_nanos() % 1000))
}

async fn official_clocks(connection: &mut SqliteConnection) -> (String, String) {
    sqlx::query_as("SELECT created_at,updated_at FROM plugins_pluginrepo WHERE is_official=1")
        .fetch_one(connection)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires detached historical SQLite Plugins migration clones"]
async fn owned_plugins_match_pinned_repository_and_metadata_migrations() {
    let root =
        std::path::PathBuf::from(std::env::var_os("DISPATCHARR_OWNED_SQLITE_PLUGINS").unwrap());
    let layouts = catalogs().unwrap();
    for name in ["empty", "cold", "resume-149", "resume-150", "rollback"] {
        let path = root.join(format!("{name}.db"));
        assert_eq!(
            super::super::run(&path).await.unwrap(),
            Upgrade::AppliedBoth
        );
        assert_eq!(app_data::run(&path).await.unwrap(), 3);
        assert_eq!(core_event_profile::run(&path).await.unwrap(), 3);
        assert_eq!(epg_guide_prefix::run(&path).await.unwrap(), 3);
        assert_eq!(epg_index::run(&path).await.unwrap(), 5);

        let prefix = match name {
            "resume-149" => 1,
            "resume-150" => 2,
            _ => 0,
        };
        let before_clock = boundary();
        let opts = SqliteConnectOptions::new()
            .filename(&path)
            .foreign_keys(false);
        let mut connection = SqliteConnection::connect_with(&opts).await.unwrap();
        let prior_clocks = if prefix != 0 {
            let package = std::fs::read_to_string(root.join("custom-repo.sql")).unwrap();
            let mut tx = connection.begin().await.unwrap();
            apply_step(&mut tx, 0, &layouts).await.unwrap();
            for statement in package.lines() {
                sqlx::query(statement).execute(&mut *tx).await.unwrap();
            }
            if prefix == 2 {
                apply_step(&mut tx, 1, &layouts).await.unwrap();
            }
            tx.commit().await.unwrap();
            Some(official_clocks(&mut connection).await)
        } else {
            None
        };
        connection.close().await.unwrap();

        if name == "rollback" {
            let prior = std::fs::read(&path).unwrap();
            let Failure::Database(error) = run(&path).await.unwrap_err() else {
                panic!("expected Plugins 0004 recorder refusal");
            };
            assert_eq!(
                error.as_database_error().unwrap().code().as_deref(),
                Some("1811")
            );
            assert_eq!(std::fs::read(&path).unwrap(), prior);
            continue;
        }

        assert_eq!(run(&path).await.unwrap(), STEPS.len() - prefix);
        let after_clock = boundary();
        let mut connection = SqliteConnection::connect_with(&opts).await.unwrap();
        let clocks = official_clocks(&mut connection).await;
        if let Some(prior) = prior_clocks {
            assert_eq!(clocks, prior);
        }
        let parse =
            |raw: &str| chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f").unwrap();
        assert!(before_clock <= parse(&clocks.0));
        assert!(parse(&clocks.0) <= parse(&clocks.1) && parse(&clocks.1) <= after_clock);
        connection.close().await.unwrap();

        let completed = std::fs::read(&path).unwrap();
        assert_eq!(run(&path).await.unwrap(), 0);
        assert_eq!(std::fs::read(&path).unwrap(), completed);
    }
}
