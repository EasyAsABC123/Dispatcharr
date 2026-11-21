use super::*;

#[tokio::test]
#[ignore = "requires detached historical SQLite M3U expiration migration clones"]
async fn backfill_matches_pinned_original_rows_and_rolls_back() {
    let root = std::path::PathBuf::from(std::env::var_os("DISPATCHARR_OWNED_SQLITE_M3U").unwrap());
    for name in [
        "ordinary",
        "empty",
        "settings-json-null",
        "user-info-not-object",
        "numeric-overflow",
        "recorder-refusal",
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

        let prior = std::fs::read(&path).unwrap();
        match name {
            "ordinary" | "empty" => {
                assert_eq!(run(&path).await.unwrap(), 1);
                let completed = std::fs::read(&path).unwrap();
                assert_eq!(run(&path).await.unwrap(), 0);
                assert_eq!(std::fs::read(&path).unwrap(), completed);
            }
            "recorder-refusal" => {
                let Failure::Database(error) = run(&path).await.unwrap_err() else {
                    panic!("expected recorder refusal after real backfill");
                };
                assert_eq!(
                    error.as_database_error().unwrap().code().as_deref(),
                    Some("1811")
                );
                assert_eq!(std::fs::read(&path).unwrap(), prior);
            }
            _ => {
                assert!(matches!(run(&path).await, Err(Failure::OutsideKnownState)));
                assert_eq!(std::fs::read(&path).unwrap(), prior);
            }
        }
    }
}
