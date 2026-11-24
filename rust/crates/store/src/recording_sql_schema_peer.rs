//! Private schema-only probe of exact compiled DVR production source literals.
//! Original Dispatcharr authors retain application/model/schema credit.
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::{postgres::PgTypeInfo, Column, Connection, Executor, Statement, Type, TypeInfo};
use std::{path::Path, str::FromStr};
use uuid::Uuid;

fn actual_statements() -> Vec<(String, String)> {
    let quoted = regex::Regex::new(r#""(?:[^"\\]|\\.)*""#).expect("fixed quoted-literal scanner");
    let mut rows = Vec::new();
    for (name, source) in [
        ("recording_capture.rs", include_str!("recording_capture.rs")),
        (
            "recording_beat_cleanup.rs",
            include_str!("recording_beat_cleanup.rs"),
        ),
        (
            "ingest-epg/cache_policy.rs",
            include_str!("../../ingest-epg/src/cache_policy.rs"),
        ),
        (
            "recording_connect_events.rs",
            include_str!("recording_connect_events.rs"),
        ),
        (
            "recording_event_enrichment.rs",
            include_str!("recording_event_enrichment.rs"),
        ),
    ] {
        for item in quoted.find_iter(source) {
            let Ok(text) = serde_json::from_str::<String>(item.as_str()) else {
                continue;
            };
            if [
                "SELECT ",
                "INSERT ",
                "UPDATE ",
                "DELETE ",
                "SAVEPOINT ",
                "RELEASE SAVEPOINT ",
            ]
            .iter()
            .any(|prefix| text.starts_with(prefix))
                || ["BEGIN", "COMMIT", "ROLLBACK"].contains(&text.as_str())
            {
                rows.push((name.into(), text));
            }
        }
    }
    rows
}

// Positional types audited against the actual production .bind expressions.
// These are parameter type authority, never a separate copy of a SQL body.
fn parameter_types(index: usize) -> Vec<PgTypeInfo> {
    let int = <i64 as Type<sqlx::Postgres>>::type_info();
    let boolean = <bool as Type<sqlx::Postgres>>::type_info();
    let text = <String as Type<sqlx::Postgres>>::type_info();
    let value = <Value as Type<sqlx::Postgres>>::type_info();
    let time = <DateTime<Utc> as Type<sqlx::Postgres>>::type_info();
    let uuid = <Uuid as Type<sqlx::Postgres>>::type_info();
    match index {
        0 | 3 | 7 | 10 | 12 | 13 | 16 | 17 | 24 | 25 | 30 | 32 | 40 | 41 | 46 | 48 | 56 | 57
        | 62 | 64 | 79 | 80 | 82 | 83 | 84 => vec![int],
        1 | 2 | 18 | 21 | 22 | 23 | 27 | 28 | 29 | 31 | 33 | 34 | 35 | 37 | 38 | 39 | 43 | 44
        | 45 | 47 | 49 | 50 | 51 | 52 | 53 | 54 | 55 | 59 | 60 | 61 | 63 | 65 | 66 | 67 | 69
        | 73 | 77 => vec![],
        4 | 20 | 36 | 71 | 74 | 75 | 76 => vec![text],
        5 | 9 | 72 => vec![int.clone(), int],
        6 => vec![boolean],
        8 => vec![text, time.clone(), time],
        11 | 14 => vec![int, value.clone(), value],
        15 | 19 | 78 => vec![text.clone(), time, uuid, text, value],
        26 | 42 | 58 => vec![int.clone(), text, int],
        68 | 70 => vec![time],
        81 => vec![uuid],
        _ => panic!("unreviewed production statement index"),
    }
}

#[test]
fn compiled_statement_inventory_and_actual_bound_types() {
    let statements = actual_statements();
    assert_eq!(statements.len(), 85);
    assert_eq!(statements[1].1, statements[18].1);
    assert!(statements[1]
        .1
        .contains("FROM dispatcharr_connect_integration)"));
    assert!(!statements
        .iter()
        .any(|(_, text)| text.contains("FROM connect_")));
    let parameters = regex::Regex::new(r"\$(\d+)").expect("fixed parameter scanner");
    for (index, (_, sql)) in statements.iter().enumerate() {
        let count = parameter_types(index).len();
        let actual = parameters
            .captures_iter(sql)
            .map(|row| row[1].parse::<usize>().expect("parameter ordinal"))
            .max()
            .unwrap_or(0);
        assert_eq!(
            count, actual,
            "statement {index}: actual source ordinal/type authority"
        );
    }
    assert_eq!(parameter_types(6)[0].name(), "BOOL");
    assert_eq!(parameter_types(15)[2].name(), "UUID");
    assert_eq!(parameter_types(78)[2].name(), "UUID");
    assert_eq!(parameter_types(81)[0].name(), "UUID");
    assert_eq!(parameter_types(11)[1].name(), "JSONB");
}

#[tokio::test]
#[ignore = "owned-schema-only actual PostgreSQL, separately admitted; no task/media"]
async fn actual_compiled_dvr_schema_prepare_only() {
    let database_url = std::env::var("OWNED_DVR_SCHEMA_URL").expect("explicit owned SQL URL");
    let options = sqlx::postgres::PgConnectOptions::from_str(&database_url).expect("SQL options");
    assert_eq!(options.get_host(), "localhost");
    assert_eq!(options.get_port(), 5432);
    assert_eq!(options.get_username(), "dispatch");
    assert!(options
        .get_database()
        .is_some_and(|name| name.starts_with("rust_probe_backup_jobs_") && name.len() <= 64));
    assert_eq!(
        std::env::var("OWNED_DVR_SCHEMA_DISPOSABLE").as_deref(),
        Ok("copied-schema-only-no-tasks")
    );
    let mut connection = sqlx::PgConnection::connect_with(
        &options.options([("default_transaction_read_only", "on")]),
    )
    .await
    .expect("owned connection");
    let readonly: String = sqlx::query_scalar("SHOW transaction_read_only")
        .fetch_one(&mut connection)
        .await
        .expect("actual read-only setting");
    assert_eq!(readonly, "on");
    let statements = actual_statements();
    assert_eq!(statements.len(), 85);
    let mut rows = Vec::new();
    for (index, (source, sql)) in statements.into_iter().enumerate() {
        let types = parameter_types(index);
        let result = (&mut connection).prepare_with(&sql, &types).await;
        rows.push(match result {
            Ok(statement) => json!({"index":index,"source":source,"sql":sql,"prepared":true,"provided_parameter_types":types.iter().map(TypeInfo::name).collect::<Vec<_>>(),"columns":statement.columns().iter().map(|c|json!({"name":c.name(),"type":c.type_info().name()})).collect::<Vec<_>>() }),
            Err(error) => json!({"index":index,"source":source,"sql":sql,"prepared":false,"error":format!("{error:?}")}),
        });
    }
    let ready = connection.ping().await;
    let closed = connection.close().await;
    let passed = rows.iter().all(|row| row["prepared"] == true) && ready.is_ok() && closed.is_ok();
    let report = json!({"passed":passed,"schema_only":true,"DML_executed":false,"task_or_media_executed":false,"actual_readonly":readonly,"statements":rows,"ready":format!("{ready:?}"),"closed":format!("{closed:?}")});
    let path = Path::new("/evidence/native-dvr-schema-private.json");
    let bytes = serde_json::to_vec_pretty(&report).expect("bounded report");
    assert!(bytes.len() <= 131072);
    std::fs::write(path, &bytes).expect("raw before assertion");
    println!(
        "NATIVE_DVR_SCHEMA_JSON={}",
        serde_json::to_string(&report).expect("report")
    );
    assert!(
        passed,
        "actual compiled production SQL schema prepare failed"
    );
}
