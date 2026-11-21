use super::*;

fn migration_clock() -> chrono::NaiveDateTime {
    let now = Utc::now().naive_utc();
    now - chrono::Duration::nanoseconds(i64::from(now.and_utc().timestamp_subsec_nanos() % 1000))
}

#[tokio::test]
#[ignore = "requires detached historical SQLite EPG source-index migration clones"]
async fn epg_source_index_matches_original_and_keeps_owned_links() {
    let root = std::path::PathBuf::from(std::env::var_os("DISPATCHARR_OWNED_SQLITE_EPG").unwrap());
    let inputs: Vec<(i64, Option<String>)> =
        serde_json::from_slice(&std::fs::read(root.join("inputs.json")).unwrap()).unwrap();
    let expected: Vec<(i64, Option<String>)> =
        serde_json::from_slice(&std::fs::read(root.join("expected.json")).unwrap()).unwrap();
    for name in [
        "empty",
        "cold",
        "resume-144",
        "resume-145",
        "resume-146",
        "resume-147",
        "rollback",
    ] {
        let path = root.join(format!("{name}.db"));
        assert_eq!(
            super::super::run(&path).await.unwrap(),
            Upgrade::AppliedBoth
        );
        assert_eq!(app_data::run(&path).await.unwrap(), 3);
        assert_eq!(core_event_profile::run(&path).await.unwrap(), 3);
        assert_eq!(epg_guide_prefix::run(&path).await.unwrap(), 3);

        let prefix = match name {
            "resume-144" => 1,
            "resume-145" => 2,
            "resume-146" => 3,
            "resume-147" => 4,
            _ => 0,
        };
        let opts = SqliteConnectOptions::new()
            .filename(&path)
            .foreign_keys(true);
        let mut connection = SqliteConnection::connect_with(&opts).await.unwrap();
        let lower = migration_clock();
        let mut tx = connection.begin().await.unwrap();
        if name != "empty" {
            for (source, value) in &inputs {
                sqlx::query("UPDATE epg_epgsource SET programme_index=?,priority=id WHERE id=?")
                    .bind(value)
                    .bind(source)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }
        }
        for index in 0..prefix {
            apply_step(&mut tx, index).await.unwrap();
        }
        if prefix != 0 {
            sqlx::query("UPDATE epg_epgsource SET username='owned-later-value' WHERE id=3")
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        let prior_indexes = if prefix >= 3 {
            sqlx::query_as::<_, (i64, Option<String>, String)>(
                "SELECT source_id,data,updated_at FROM epg_epgsourceindex ORDER BY source_id",
            )
            .fetch_all(&mut *tx)
            .await
            .unwrap()
        } else {
            Vec::new()
        };
        tx.commit().await.unwrap();
        connection.close().await.unwrap();

        if name == "rollback" {
            let prior = std::fs::read(&path).unwrap();
            let Failure::Database(error) = run(&path).await.unwrap_err() else {
                panic!("expected EPG 0028 recorder refusal after copied index rows");
            };
            assert_eq!(
                error.as_database_error().unwrap().code().as_deref(),
                Some("1811")
            );
            assert_eq!(std::fs::read(&path).unwrap(), prior);
            continue;
        }

        assert_eq!(run(&path).await.unwrap(), STEPS.len() - prefix);
        let upper = migration_clock();
        let mut connection = SqliteConnection::connect_with(&opts).await.unwrap();
        let observed = sqlx::query_as::<_, (i64, Option<String>, String)>(
            "SELECT source_id,data,updated_at FROM epg_epgsourceindex ORDER BY source_id",
        )
        .fetch_all(&mut connection)
        .await
        .unwrap();
        connection.close().await.unwrap();
        if name == "empty" {
            assert!(observed.is_empty());
        } else {
            assert!(observed
                .iter()
                .map(|(source, data, _)| (*source, data))
                .eq(expected.iter().map(|(source, data)| (*source, data)),));
            assert!(observed.iter().all(|(_, _, clock)| {
                let parsed =
                    chrono::NaiveDateTime::parse_from_str(clock, "%Y-%m-%d %H:%M:%S%.f").unwrap();
                lower <= parsed && parsed <= upper
            }));
            if prefix >= 3 {
                assert_eq!(observed, prior_indexes);
            }
        }
        let completed = std::fs::read(&path).unwrap();
        assert_eq!(run(&path).await.unwrap(), 0);
        assert_eq!(std::fs::read(&path).unwrap(), completed);
    }
}
