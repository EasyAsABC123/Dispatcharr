//! Exact compiled matching, station and aggregate SQL against an owned migrated Source PostgreSQL schema.
//! Original Dispatcharr/Django/SQLx authors retain model/algorithm/library credit.
//! This peer prepares production statements; it never executes application DML.
use serde_json::{json, Value};
use sqlx::{postgres::PgTypeInfo, Column, Connection, Executor, Statement, Type, TypeInfo};
use std::{collections::BTreeSet, path::Path, str::FromStr};

fn statements() -> Vec<(&'static str, String)> {
    // Actual constants used by production queries, including the SAME dynamic
    // serializer formatter. No query body or stand-in table is copied here.
    let mut rows = crate::channel_epg_match_task::schema_statements();
    rows.extend([
        (
            "station_existing",
            crate::sd_station_discovery::EXISTING.to_owned(),
        ),
        (
            "station_insert",
            crate::sd_station_discovery::INSERT.to_owned(),
        ),
        (
            "station_update",
            crate::sd_station_discovery::UPDATE.to_owned(),
        ),
        (
            "aggregate_active",
            crate::m3u_refresh_all::schema_active_selection().to_owned(),
        ),
        ("comskip_select", crate::comskip_task::SELECT.to_owned()),
        ("comskip_update", crate::comskip_task::UPDATE.to_owned()),
        ("comskip_settings", crate::comskip_task::SETTINGS.to_owned()),
        ("comskip_exists", crate::comskip_http::EXISTS.to_owned()),
    ]);
    rows.extend(crate::vod_ingestion::schema_statements());
    rows
}
fn parameter_types(name: &str) -> Vec<PgTypeInfo> {
    // Named descriptors audited against each actual .bind expression. These
    // provide PostgreSQL the production Rust wire types, rather than asking it
    // to infer convenient types from otherwise-unbound SQL placeholders.
    match name {
        "list_logo_insert" | "list_movie_insert" | "list_movie_update" => {
            vec![<Value as Type<sqlx::Postgres>>::type_info()]
        }
        "list_logo_select" => vec![<Vec<String> as Type<sqlx::Postgres>>::type_info()],
        "list_movie_relation_insert" => vec![
            <i64 as Type<sqlx::Postgres>>::type_info(),
            <Value as Type<sqlx::Postgres>>::type_info(),
            <chrono::DateTime<chrono::Utc> as Type<sqlx::Postgres>>::type_info(),
        ],
        "list_movie_foreign_account" | "list_series_foreign_account" => vec![
            <i64 as Type<sqlx::Postgres>>::type_info(),
            <Value as Type<sqlx::Postgres>>::type_info(),
        ],
        "advanced_category_insert" => vec![
            <Value as Type<sqlx::Postgres>>::type_info(),
            <String as Type<sqlx::Postgres>>::type_info(),
            <chrono::DateTime<chrono::Utc> as Type<sqlx::Postgres>>::type_info(),
        ],
        "advanced_uncategorized_insert" => vec![
            <String as Type<sqlx::Postgres>>::type_info(),
            <chrono::DateTime<chrono::Utc> as Type<sqlx::Postgres>>::type_info(),
        ],
        "advanced_uncategorized_relation_insert" => vec![
            <i64 as Type<sqlx::Postgres>>::type_info(),
            <i64 as Type<sqlx::Postgres>>::type_info(),
            <bool as Type<sqlx::Postgres>>::type_info(),
            <chrono::DateTime<chrono::Utc> as Type<sqlx::Postgres>>::type_info(),
        ],
        "advanced_series_relation_insert" => vec![
            <i64 as Type<sqlx::Postgres>>::type_info(),
            <Value as Type<sqlx::Postgres>>::type_info(),
            <chrono::DateTime<chrono::Utc> as Type<sqlx::Postgres>>::type_info(),
        ],
        "advanced_series_existing" | "advanced_guard" | "advanced_orphan_series" => vec![],
        "advanced_series_insert" | "advanced_series_update" => {
            vec![<Value as Type<sqlx::Postgres>>::type_info()]
        }
        "advanced_stale_series" => vec![
            <i64 as Type<sqlx::Postgres>>::type_info(),
            <chrono::DateTime<chrono::Utc> as Type<sqlx::Postgres>>::type_info(),
        ],
        "advanced_stale_children"
        | "advanced_delete_episode_relations"
        | "advanced_delete_series_relations"
        | "advanced_orphan_recheck"
        | "advanced_orphan_episodes"
        | "advanced_orphan_children"
        | "advanced_delete_episodes"
        | "advanced_delete_series" => vec![<Vec<i64> as Type<sqlx::Postgres>>::type_info()],
        "station_existing" | "comskip_select" | "comskip_exists" => {
            vec![<i64 as Type<sqlx::Postgres>>::type_info()]
        }
        "station_insert" | "station_update" | "comskip_update" => vec![
            <i64 as Type<sqlx::Postgres>>::type_info(),
            <Value as Type<sqlx::Postgres>>::type_info(),
        ],
        "channels" => vec![
            <bool as Type<sqlx::Postgres>>::type_info(),
            <Vec<i64> as Type<sqlx::Postgres>>::type_info(),
        ], // .bind(all:bool).bind(ids.unwrap_or_default():Vec<i64>)
        "single_exact" => vec![<String as Type<sqlx::Postgres>>::type_info()],
        "catalog" | "mark_custom" => vec![<Option<i64> as Type<sqlx::Postgres>>::type_info()],
        "bulk_candidates" | "bulk_update" => vec![<Value as Type<sqlx::Postgres>>::type_info()],
        "child_epgs" => vec![<Vec<i64> as Type<sqlx::Postgres>>::type_info()],
        "single_update" => vec![
            <i64 as Type<sqlx::Postgres>>::type_info(),
            <i64 as Type<sqlx::Postgres>>::type_info(),
        ],
        "serialize" => vec![<i64 as Type<sqlx::Postgres>>::type_info()],
        "settings" | "plugins" | "aggregate_active" | "comskip_settings" => vec![],
        _ => panic!("unreviewed production matching statement: {name}"),
    }
}
#[derive(Clone, Copy)]
enum Output {
    Int4,
    Int8,
    Text,
    Bool,
    Jsonb,
}
fn outputs(name: &str) -> Vec<(&'static str, Output)> {
    use Output::*;
    match name {
        "list_logo_insert" | "list_movie_update" | "list_movie_relation_insert" => vec![],
        "list_logo_select" => vec![("id", Int8), ("url", Text)],
        "list_movie_insert" => vec![("id", Int8), ("name", Text), ("year", Int4)],
        "list_movie_foreign_account" | "list_series_foreign_account" => vec![("unsupported", Bool)],
        "advanced_category_insert"
        | "advanced_uncategorized_insert"
        | "advanced_uncategorized_relation_insert"
        | "advanced_series_relation_insert" => vec![],
        "advanced_series_existing" | "advanced_series_insert" => {
            vec![("id", Int8), ("name", Text), ("year", Int4)]
        }
        "advanced_guard" => vec![("?column?", Bool)],
        "advanced_stale_series"
        | "advanced_stale_children"
        | "advanced_orphan_series"
        | "advanced_orphan_recheck"
        | "advanced_orphan_episodes"
        | "advanced_orphan_children" => vec![("id", Int8)],
        "advanced_series_update"
        | "advanced_delete_episode_relations"
        | "advanced_delete_series_relations"
        | "advanced_delete_episodes"
        | "advanced_delete_series" => vec![],
        "station_existing" => vec![("tvg_id", Text)],
        "aggregate_active" | "comskip_exists" => vec![("id", Int8)],
        "comskip_select" => vec![("properties", Jsonb), ("bounded", Bool)],
        "comskip_settings" => vec![("value", Jsonb)],
        "station_insert" | "station_update" | "comskip_update" => vec![],
        "channels" => vec![
            ("id", Int8),
            ("name", Text),
            ("tvg_id", Text),
            ("tvc_guide_stationid", Text),
            ("epg_data_id", Int8),
            ("auto_created", Bool),
            ("channel_group_id", Int8),
        ],
        "single_exact" => vec![("id", Int8)],
        "catalog" => vec![
            ("id", Int8),
            ("name", Text),
            ("tvg_id", Text),
            ("epg_source_id", Int8),
            ("priority", Int8),
            ("source_type", Text),
            ("has_programs", Bool),
        ],
        "settings" => vec![("key", Text), ("value", Jsonb), ("supported", Bool)],
        "bulk_candidates" => vec![("id", Int8), ("epg", Int8)],
        "child_epgs" => vec![
            ("id", Int8),
            ("name", Text),
            ("tvg_id", Text),
            ("epg_source_id", Int8),
            ("source_type", Text),
            ("has_programs", Bool),
        ],
        "serialize" => vec![
            ("raw", Jsonb),
            ("ov", Jsonb),
            ("account_name", Text),
            ("streams", Jsonb),
        ],
        "plugins" => vec![("exists", Bool)],
        "bulk_update" | "single_update" | "mark_custom" => vec![],
        _ => panic!("unreviewed matching row decoder: {name}"),
    }
}
fn compatible(kind: Output, actual: &PgTypeInfo) -> bool {
    match kind {
        Output::Int4 => actual.name() == "INT4",
        Output::Int8 => actual.name() == "INT8",
        Output::Text => <String as Type<sqlx::Postgres>>::compatible(actual),
        Output::Bool => actual.name() == "BOOL",
        Output::Jsonb => actual.name() == "JSONB",
    }
}
#[test]
fn compiled_matching_statements_and_bind_ordinals_are_complete() {
    let rows = statements();
    assert_eq!(rows.len(), 44);
    let names: BTreeSet<_> = rows.iter().map(|(name, _)| *name).collect();
    assert_eq!(names.len(), rows.len());
    assert_eq!(
        names,
        [
            "channels",
            "single_exact",
            "catalog",
            "settings",
            "bulk_candidates",
            "bulk_update",
            "child_epgs",
            "single_update",
            "mark_custom",
            "serialize",
            "plugins",
            "station_existing",
            "station_insert",
            "station_update",
            "aggregate_active",
            "comskip_select",
            "comskip_update",
            "comskip_settings",
            "comskip_exists",
            "list_logo_insert",
            "list_logo_select",
            "list_movie_insert",
            "list_movie_update",
            "list_movie_relation_insert",
            "list_movie_foreign_account",
            "list_series_foreign_account",
            "advanced_category_insert",
            "advanced_uncategorized_insert",
            "advanced_uncategorized_relation_insert",
            "advanced_series_relation_insert",
            "advanced_series_existing",
            "advanced_series_insert",
            "advanced_series_update",
            "advanced_guard",
            "advanced_stale_series",
            "advanced_stale_children",
            "advanced_delete_episode_relations",
            "advanced_delete_series_relations",
            "advanced_orphan_series",
            "advanced_orphan_recheck",
            "advanced_orphan_episodes",
            "advanced_orphan_children",
            "advanced_delete_episodes",
            "advanced_delete_series",
        ]
        .into_iter()
        .collect()
    );
    let ordinal = regex::Regex::new(r"\$(\d+)").expect("fixed ordinal scanner");
    for (name, sql) in rows {
        assert!(
            sql.starts_with("SELECT ")
                || sql.starts_with("UPDATE ")
                || sql.starts_with("INSERT ")
                || sql.starts_with("DELETE ")
                || (matches!(name, "list_movie_update" | "advanced_series_update")
                    && sql.starts_with("WITH projected AS MATERIALIZED (SELECT "))
        );
        let used: BTreeSet<_> = ordinal
            .captures_iter(&sql)
            .map(|m| m[1].parse::<usize>().unwrap())
            .collect();
        assert_eq!(
            used,
            (1..=parameter_types(name).len()).collect(),
            "actual bind ordinals: {name}"
        );
        let _ = outputs(name); // Every production result has an explicit decoder audit.
    }
    assert_eq!(
        parameter_types("channels")
            .iter()
            .map(TypeInfo::name)
            .collect::<Vec<_>>(),
        ["BOOL", "INT8[]"]
    );
    assert_eq!(parameter_types("catalog")[0].name(), "INT8");
    assert_eq!(parameter_types("bulk_update")[0].name(), "JSONB");
    let rows = statements();
    let exact = &rows.iter().find(|(n, _)| *n == "single_exact").unwrap().1;
    assert!(exact.contains("UPPER(e.tvg_id::text)=UPPER($1)"));
    assert!(exact.contains("ORDER BY e.id ASC LIMIT 1"));
    let serialize = &rows.iter().find(|(n, _)| *n == "serialize").unwrap().1;
    assert_eq!(
        serialize,
        &format!("{} AND c.id=$1", crate::channels::SELECT)
    );
}

#[tokio::test]
#[ignore = "owned migrated Source PostgreSQL schema-only; separately admitted; no matching task/media"]
async fn actual_compiled_matching_schema_prepare_only() {
    let url = std::env::var("OWNED_MATCHING_SCHEMA_URL").expect("explicit owned Source schema URL");
    let options = sqlx::postgres::PgConnectOptions::from_str(&url).expect("SQL options");
    assert_eq!(options.get_host(), "localhost");
    assert_eq!(options.get_port(), 5432);
    assert_eq!(options.get_username(), "dispatch");
    assert!(options
        .get_database()
        .is_some_and(|v| v.starts_with("rust_probe_backup_jobs_") && v.len() <= 64));
    assert_eq!(
        std::env::var("OWNED_MATCHING_SCHEMA_DISPOSABLE").as_deref(),
        Ok("copied-schema-only-no-tasks")
    );
    // Disable caching so every positive AND deliberately wrong-type prepare
    // crosses actual PostgreSQL Parse/Describe, not a cached prior type vector.
    let mut connection =
        sqlx::PgConnection::connect_with(&options.statement_cache_capacity(0).options([
            ("default_transaction_read_only", "on"),
            ("statement_timeout", "5000"),
        ]))
        .await
        .expect("owned schema connection");
    let readonly: String = sqlx::query_scalar("SHOW transaction_read_only")
        .fetch_one(&mut connection)
        .await
        .expect("read-only setting");
    assert_eq!(readonly, "on");
    let mut rows = Vec::new();
    for (name, sql) in statements() {
        let types = parameter_types(name);
        let result = (&mut connection).prepare_with(&sql, &types).await;
        rows.push(match result {
            Ok(statement)=>{
                let server_parameters=match statement.parameters(){Some(sqlx::Either::Left(values))=>values.to_vec(),_=>Vec::new()};
                let parameter_check=server_parameters==types;
                let expected=outputs(name);let columns=statement.columns();
                let column_check=expected.len()==columns.len()&&expected.iter().zip(columns).all(|((name,kind),actual)|*name==actual.name()&&compatible(*kind,actual.type_info()));
                json!({"name":name,"sql":sql,"prepared":true,"row_decoder_compatible":column_check,"server_parameter_types_match":parameter_check,
                    "server_parameter_types":server_parameters.iter().map(TypeInfo::name).collect::<Vec<_>>(),
                    "provided_parameter_types":types.iter().map(TypeInfo::name).collect::<Vec<_>>(),
                    "columns":columns.iter().map(|c|json!({"name":c.name(),"type":c.type_info().name()})).collect::<Vec<_>>()})
            },
            Err(error)=>json!({"name":name,"sql":sql,"prepared":false,"row_decoder_compatible":false,"error":format!("{error:?}")}),
        });
    }
    let mut negatives = Vec::new();
    for (name, index) in [
        ("channels", 0),
        ("bulk_candidates", 0),
        ("station_insert", 1),
        ("station_existing", 0),
        ("comskip_select", 0),
        ("comskip_update", 1),
        ("advanced_series_insert", 0),
        ("advanced_stale_series", 0),
        ("advanced_stale_series", 1),
        ("advanced_orphan_episodes", 0),
        ("list_logo_insert", 0),
        ("list_logo_select", 0),
        ("list_movie_insert", 0),
        ("list_movie_update", 0),
        ("list_movie_relation_insert", 0),
        ("list_movie_relation_insert", 1),
        ("list_movie_relation_insert", 2),
        ("advanced_series_update", 0),
        ("list_movie_foreign_account", 0),
        ("list_movie_foreign_account", 1),
        ("list_series_foreign_account", 0),
        ("list_series_foreign_account", 1),
    ] {
        let sql = statements()
            .into_iter()
            .find(|(n, _)| *n == name)
            .unwrap()
            .1;
        let mut types = parameter_types(name);
        types[index] = if name == "list_logo_select" {
            <Vec<bool> as Type<sqlx::Postgres>>::type_info()
        } else if matches!(
            name,
            "list_movie_foreign_account"
                | "list_series_foreign_account"
                | "list_movie_relation_insert"
        ) && index == 0
        {
            <bool as Type<sqlx::Postgres>>::type_info()
        } else if name == "advanced_orphan_episodes" {
            <Vec<bool> as Type<sqlx::Postgres>>::type_info()
        } else if matches!(name, "station_existing" | "comskip_select")
            || (name == "advanced_stale_series" && index == 0)
        {
            <bool as Type<sqlx::Postgres>>::type_info()
        } else {
            <i64 as Type<sqlx::Postgres>>::type_info()
        };
        let result = (&mut connection).prepare_with(&sql, &types).await;
        negatives.push(json!({"name":name,"wrong_parameter_index":index,"same_production_SQL":true,"deliberately_wrong_types":types.iter().map(TypeInfo::name).collect::<Vec<_>>(),"rejected":result.is_err(),"error":result.err().map(|e|format!("{e:?}"))}));
    }
    let ready = connection.ping().await;
    let closed = connection.close().await;
    let passed = rows.len() == 44
        && rows.iter().all(|r| {
            r["prepared"] == true
                && r["row_decoder_compatible"] == true
                && r["server_parameter_types_match"] == true
        })
        && negatives.len() == 22
        && negatives.iter().all(|r| r["rejected"] == true)
        && ready.is_ok()
        && closed.is_ok();
    let report = json!({"passed":passed,"schema_only":true,"DML_executed":false,"task_or_media_executed":false,
        "actual_readonly":readonly,"statement_cache_disabled":true,"components":{"matching":11,"station":3,"aggregate":1,"comskip":4,"vod_advanced":18,"vod_list_metadata":7},"statements":rows,"wrong_bind_type_controls":negatives,
        "ready":format!("{ready:?}"),"closed":format!("{closed:?}"),"scope":"Compiled production SQL/constants and audited actual Rust bind types; prepares/row shapes only, not DML/data/event/task parity."});
    let bytes = serde_json::to_vec_pretty(&report).expect("report");
    assert!(bytes.len() <= 131072);
    std::fs::write(
        Path::new("/evidence/native-channel-matching-schema-private.json"),
        &bytes,
    )
    .expect("persist before assertion");
    println!(
        "NATIVE_MATCHING_SCHEMA_JSON={}",
        serde_json::to_string(&report).unwrap()
    );
    assert!(
        passed,
        "actual compiled matching/station/aggregate SQL/schema/type prepare failed"
    );
}
