use super::*;

#[tokio::test]
#[ignore = "requires owned historical SQLite Channels identity fixtures"]
async fn channels_identity_hash_backfill_and_linked_duplicates_match_original() {
    let root = std::path::PathBuf::from(
        std::env::var_os("DISPATCHARR_OWNED_SQLITE_CHANNEL_IDENTITY").unwrap(),
    );
    for (label, fixture, prior_steps) in [
        ("url-keys", "url-keys", 0),
        ("non-url-keys", "non-url-keys", 0),
        ("resume-173", "url-keys", 1),
        ("resume-174", "url-keys", 2),
        ("resume-175", "url-keys", 3),
        ("late-recorder", "url-keys", 0),
        ("unknown-incoming", "url-keys", 0),
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

        let options = SqliteConnectOptions::new()
            .filename(&path)
            .foreign_keys(false);
        let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
        let mut tx = connection.begin().await.unwrap();
        let statements = std::fs::read_to_string(root.join(format!("{fixture}.sql"))).unwrap();
        for statement in statements.lines() {
            sqlx::query(statement).execute(&mut *tx).await.unwrap();
        }
        for index in 0..prior_steps {
            apply_step(&mut tx, index).await.unwrap();
        }
        if label == "unknown-incoming" {
            sqlx::query("CREATE TABLE owned_extra_stream_reference (stream_id bigint REFERENCES dispatcharr_channels_stream(id))")
                .execute(&mut *tx).await.unwrap();
        }
        tx.commit().await.unwrap();
        connection.close().await.unwrap();

        let previous = std::fs::read(&path).unwrap();
        let result = run(&path).await;
        match label {
            "late-recorder" => {
                let Failure::Database(error) = result.unwrap_err() else {
                    panic!("expected late scoped recorder refusal");
                };
                assert_eq!(
                    error.as_database_error().unwrap().code().as_deref(),
                    Some("1811")
                );
                assert_eq!(std::fs::read(&path).unwrap(), previous);
            }
            "unknown-incoming" => {
                assert!(matches!(result, Err(Failure::OutsideKnownState)));
                assert_eq!(std::fs::read(&path).unwrap(), previous);
            }
            _ => {
                assert_eq!(result.unwrap(), STEPS.len() - prior_steps);
                let complete = std::fs::read(&path).unwrap();
                assert_eq!(run(&path).await.unwrap(), 0);
                assert_eq!(std::fs::read(&path).unwrap(), complete);
            }
        }
    }
}
