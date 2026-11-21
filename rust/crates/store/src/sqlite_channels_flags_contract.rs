use super::*;

fn now() -> chrono::NaiveDateTime {
    let instant = Utc::now();
    instant.naive_utc()
        - chrono::Duration::nanoseconds(i64::from(instant.timestamp_subsec_nanos() % 1000))
}

async fn observed_clock(path: &Path) -> Option<String> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .foreign_keys(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    let clock =
        sqlx::query_scalar("SELECT last_seen FROM dispatcharr_channels_channelgroupm3uaccount")
            .fetch_optional(&mut connection)
            .await
            .unwrap();
    connection.close().await.unwrap();
    clock
}

#[tokio::test]
#[ignore = "requires owned historical SQLite Channels fixtures"]
async fn channels_stale_adult_clock_links_and_rollback_match_original() {
    let root = std::path::PathBuf::from(
        std::env::var_os("DISPATCHARR_OWNED_SQLITE_CHANNEL_FLAGS").unwrap(),
    );
    for case in ["populated", "empty", "resume-171", "late-recorder"] {
        let path = root.join(format!("{case}.db"));
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

        let lower = now();
        let saved = if case == "resume-171" {
            let options = SqliteConnectOptions::new()
                .filename(&path)
                .foreign_keys(false);
            let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
            let mut tx = connection.begin().await.unwrap();
            apply_step(&mut tx, 0).await.unwrap();
            tx.commit().await.unwrap();
            connection.close().await.unwrap();
            observed_clock(&path).await
        } else {
            None
        };

        let prior = std::fs::read(&path).unwrap();
        let outcome = run(&path).await;
        if case == "late-recorder" {
            let Failure::Database(error) = outcome.unwrap_err() else {
                panic!("expected scoped recorder refusal after both actual remakes");
            };
            assert_eq!(
                error.as_database_error().unwrap().code().as_deref(),
                Some("1811")
            );
            assert_eq!(std::fs::read(&path).unwrap(), prior);
            continue;
        }
        assert_eq!(outcome.unwrap(), if case == "resume-171" { 1 } else { 2 });
        let upper = now();
        let clock = observed_clock(&path).await;
        if case == "empty" {
            assert!(clock.is_none());
        } else {
            let parsed = chrono::NaiveDateTime::parse_from_str(
                clock.as_ref().unwrap(),
                "%Y-%m-%d %H:%M:%S%.f",
            )
            .unwrap();
            assert!(lower <= parsed && parsed <= upper);
            if case == "resume-171" {
                assert_eq!(clock, saved);
            }
        }
        let complete = std::fs::read(&path).unwrap();
        assert_eq!(run(&path).await.unwrap(), 0);
        assert_eq!(std::fs::read(&path).unwrap(), complete);
    }
}
