//! Exact compiled matching, station and aggregate SQL against an owned migrated Source PostgreSQL schema.
//! Original Dispatcharr/Django/SQLx authors retain model/algorithm/library credit.
//! The 84/69 schema peer only prepares statements. A separate ignored control
//! executes production UPDATEs on transaction-local shadow tables, then rolls back.
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
    rows.extend(crate::vod_category_settings::schema_statements());
    rows
}
fn parameter_types(name: &str) -> Vec<PgTypeInfo> {
    if let Some(types) = crate::vod_ingestion::language_schema_parameters(name) {
        return types;
    }
    // Named descriptors audited against each actual .bind expression. These
    // provide PostgreSQL the production Rust wire types, rather than asking it
    // to infer convenient types from otherwise-unbound SQL placeholders.
    match name {
        "list_category_languages" => vec![<i64 as Type<sqlx::Postgres>>::type_info()],
        "list_uncategorized_language" => vec![
            <i64 as Type<sqlx::Postgres>>::type_info(),
            <i64 as Type<sqlx::Postgres>>::type_info(),
        ],
        "vod_live_group_settings_upsert" => vec![
            <i64 as Type<sqlx::Postgres>>::type_info(),
            <Value as Type<sqlx::Postgres>>::type_info(),
        ],
        "vod_category_settings_existing" => vec![
            <i64 as Type<sqlx::Postgres>>::type_info(),
            <Vec<i64> as Type<sqlx::Postgres>>::type_info(),
        ],
        "vod_category_settings_upsert" => vec![
            <i64 as Type<sqlx::Postgres>>::type_info(),
            <Value as Type<sqlx::Postgres>>::type_info(),
        ],
        "list_logo_insert"
        | "list_movie_insert"
        | "list_movie_update"
        | "advanced_series_existing"
        | "list_movie_existing" => {
            vec![<Value as Type<sqlx::Postgres>>::type_info()]
        }
        "list_logo_select" => vec![<Vec<String> as Type<sqlx::Postgres>>::type_info()],
        "list_movie_relation_existing" | "advanced_series_relation_existing" => vec![
            <i64 as Type<sqlx::Postgres>>::type_info(),
            <Vec<String> as Type<sqlx::Postgres>>::type_info(),
        ],
        "list_movie_relation_insert"
        | "list_movie_relation_update"
        | "advanced_series_relation_update" => vec![
            <i64 as Type<sqlx::Postgres>>::type_info(),
            <Value as Type<sqlx::Postgres>>::type_info(),
            <chrono::DateTime<chrono::Utc> as Type<sqlx::Postgres>>::type_info(),
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
        "advanced_guard" | "advanced_orphan_series" => vec![],
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
    Uuid,
}
fn outputs(name: &str) -> Vec<(&'static str, Output)> {
    use Output::*;
    if let Some(columns) = crate::vod_ingestion::language_schema_columns(name) {
        return columns
            .into_iter()
            .map(|(column, kind)| {
                (
                    column,
                    match kind {
                        "JSONB" => Jsonb,
                        "BOOL" => Bool,
                        "INT8" => Int8,
                        other => panic!("unreviewed language decoder {name}/{column}: {other}"),
                    },
                )
            })
            .collect();
    }
    match name {
        "list_category_languages" => vec![("category", Int8), ("language", Text)],
        "list_uncategorized_language" => vec![("coalesce", Text)],
        "vod_live_group_settings_upsert" => vec![],
        "vod_category_settings_existing" => vec![("category", Int8), ("properties", Jsonb)],
        "vod_category_settings_upsert" => vec![],
        "list_logo_insert"
        | "list_movie_update"
        | "list_movie_relation_insert"
        | "list_movie_relation_update"
        | "advanced_series_relation_update" => vec![],
        "list_logo_select" => vec![("id", Int8), ("url", Text)],
        "list_movie_relation_existing" | "advanced_series_relation_existing" => {
            vec![("provider", Text), ("target", Int8), ("language", Text)]
        }
        "list_movie_insert" | "advanced_series_insert" => vec![("id", Int8), ("uuid", Uuid)],
        "advanced_category_insert"
        | "advanced_uncategorized_insert"
        | "advanced_uncategorized_relation_insert"
        | "advanced_series_relation_insert" => vec![],
        "advanced_series_existing" | "list_movie_existing" => {
            vec![
                ("id", Int8),
                ("name", Text),
                ("year", Int4),
                ("tmdb_id", Text),
                ("imdb_id", Text),
                ("language", Text),
            ]
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
        Output::Uuid => <uuid::Uuid as Type<sqlx::Postgres>>::compatible(actual),
    }
}
#[test]
fn compiled_matching_statements_and_bind_ordinals_are_complete() {
    let rows = statements();
    assert_eq!(rows.len(), 84);
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
            "list_movie_existing",
            "list_movie_relation_existing",
            "advanced_series_relation_existing",
            "list_movie_relation_update",
            "advanced_series_relation_update",
            "list_movie_insert",
            "list_movie_update",
            "list_movie_relation_insert",
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
            "vod_live_group_settings_upsert",
            "vod_category_settings_existing",
            "vod_category_settings_upsert",
            "language_movie_candidates",
            "language_movie_min_rows",
            "language_movie_row",
            "language_movie_relations",
            "language_movie_slot",
            "language_movie_bulk_language",
            "language_movie_insert",
            "language_movie_move",
            "language_movie_delete",
            "language_movie_metadata",
            "language_series_candidates",
            "language_series_min_rows",
            "language_series_row",
            "language_series_relations",
            "language_series_slot",
            "language_series_bulk_language",
            "language_series_insert",
            "language_series_move",
            "language_series_delete",
            "language_series_metadata",
            "language_episode_rows",
            "language_episode_relations",
            "language_episode_collision",
            "language_episode_move",
            "language_episode_move_all_relations",
            "language_episode_move_relations",
            "language_episode_insert",
            "language_episode_delete_relations",
            "language_episode_delete",
            "language_episode_empty",
            "language_episode_has_relations",
            "language_category_map",
            "list_category_languages",
            "list_uncategorized_language",
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
                || (matches!(name, "list_movie_existing" | "advanced_series_existing")
                    && sql.starts_with("WITH wanted AS MATERIALIZED (SELECT "))
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
        ("list_movie_relation_update", 0),
        ("list_movie_relation_update", 1),
        ("list_movie_relation_update", 2),
        ("advanced_series_relation_update", 0),
        ("advanced_series_relation_update", 1),
        ("advanced_series_relation_update", 2),
        ("list_movie_existing", 0),
        ("advanced_series_existing", 0),
        ("list_movie_relation_existing", 0),
        ("list_movie_relation_existing", 1),
        ("advanced_series_relation_existing", 0),
        ("advanced_series_relation_existing", 1),
        ("language_movie_candidates", 0),
        ("language_movie_min_rows", 0),
        ("language_movie_row", 0),
        ("language_movie_relations", 0),
        ("language_movie_slot", 0),
        ("language_movie_bulk_language", 0),
        ("language_movie_insert", 0),
        ("language_movie_move", 0),
        ("language_movie_delete", 0),
        ("language_movie_metadata", 0),
        ("language_series_candidates", 0),
        ("language_series_min_rows", 0),
        ("language_series_row", 0),
        ("language_series_relations", 0),
        ("language_series_slot", 0),
        ("language_series_bulk_language", 0),
        ("language_series_insert", 0),
        ("language_series_move", 0),
        ("language_series_delete", 0),
        ("language_series_metadata", 0),
        ("language_episode_rows", 0),
        ("language_episode_relations", 0),
        ("language_episode_collision", 0),
        ("language_episode_move", 0),
        ("language_episode_move_all_relations", 0),
        ("language_episode_move_relations", 0),
        ("language_episode_insert", 0),
        ("language_episode_delete_relations", 0),
        ("language_episode_delete", 0),
        ("language_episode_empty", 0),
        ("language_episode_has_relations", 0),
        ("language_category_map", 0),
        ("language_category_map", 1),
        ("list_category_languages", 0),
        ("list_uncategorized_language", 0),
        ("vod_live_group_settings_upsert", 0),
        ("vod_live_group_settings_upsert", 1),
        ("vod_category_settings_existing", 0),
        ("vod_category_settings_upsert", 0),
    ] {
        let sql = statements()
            .into_iter()
            .find(|(n, _)| *n == name)
            .unwrap()
            .1;
        let mut types = parameter_types(name);
        types[index] = if name.starts_with("language_")
            || name.starts_with("vod_category_settings_")
            || name.starts_with("vod_live_group_settings_")
            || matches!(
                name,
                "list_category_languages" | "list_uncategorized_language"
            ) {
            if types[index] == <i64 as Type<sqlx::Postgres>>::type_info() {
                <bool as Type<sqlx::Postgres>>::type_info()
            } else if types[index] == <Vec<i64> as Type<sqlx::Postgres>>::type_info() {
                <Vec<bool> as Type<sqlx::Postgres>>::type_info()
            } else {
                <i64 as Type<sqlx::Postgres>>::type_info()
            }
        } else if name == "list_logo_select"
            || (matches!(
                name,
                "list_movie_relation_existing" | "advanced_series_relation_existing"
            ) && index == 1)
        {
            <Vec<bool> as Type<sqlx::Postgres>>::type_info()
        } else if matches!(
            name,
            "list_movie_relation_insert"
                | "list_movie_relation_update"
                | "advanced_series_relation_update"
                | "list_movie_relation_existing"
                | "advanced_series_relation_existing"
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
        assert_ne!(
            types[index],
            parameter_types(name)[index],
            "Negative control must change the actual production wire type: {name}/{index}"
        );
        let result = (&mut connection).prepare_with(&sql, &types).await;
        negatives.push(json!({"name":name,"wrong_parameter_index":index,"same_production_SQL":true,"deliberately_wrong_types":types.iter().map(TypeInfo::name).collect::<Vec<_>>(),"rejected":result.is_err(),"error":result.err().map(|e|format!("{e:?}"))}));
    }
    let ready = connection.ping().await;
    let closed = connection.close().await;
    let passed = rows.len() == 84
        && rows.iter().all(|r| {
            r["prepared"] == true
                && r["row_decoder_compatible"] == true
                && r["server_parameter_types_match"] == true
        })
        && negatives.len() == 69
        && negatives.iter().all(|r| r["rejected"] == true)
        && ready.is_ok()
        && closed.is_ok();
    let report = json!({"passed":passed,"schema_only":true,"DML_executed":false,"task_or_media_executed":false,
        "actual_readonly":readonly,"statement_cache_disabled":true,"components":{"matching":11,"station":3,"aggregate":1,"comskip":4,"vod_advanced":20,"vod_list_metadata":8,"vod_language_pinning":2,"vod_language_reconcile":32,"vod_category_settings":3},"statements":rows,"wrong_bind_type_controls":negatives,
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

#[tokio::test]
#[ignore = "owned copied Source schema; actual production UPDATE on transaction-local shadow tables, then rollback"]
async fn actual_compiled_vod_name_queue_rollback() {
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
    // This distinct connection intentionally permits DML. The existing 47/30
    // Parse/Describe test remains read-only and publishes its separate receipt.
    let mut connection =
        sqlx::PgConnection::connect_with(&options.statement_cache_capacity(0).options([
            ("default_transaction_read_only", "off"),
            ("statement_timeout", "5000"),
        ]))
        .await
        .expect("owned semantic-control connection");
    let public_sql = "SELECT jsonb_build_object('movie',COALESCE((SELECT jsonb_agg(to_jsonb(m) ORDER BY m.id) FROM public.vod_movie m),'[]'::jsonb),'series',COALESCE((SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) FROM public.vod_series s),'[]'::jsonb),'sequences',COALESCE((SELECT jsonb_agg(to_jsonb(q) ORDER BY q.sequencename) FROM pg_sequences q WHERE q.schemaname='public'),'[]'::jsonb))";
    for table in ["vod_movie", "vod_series"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM public.{table}"))
            .fetch_one(&mut connection)
            .await
            .expect("bounded public observation");
        assert!(count <= 4096);
    }
    let public_before: Value = sqlx::query_scalar(public_sql)
        .fetch_one(&mut connection)
        .await
        .expect("public baseline");
    let mut tx = connection
        .begin()
        .await
        .expect("owned rollback transaction");
    sqlx::query("SET LOCAL search_path TO pg_temp,public")
        .execute(&mut *tx)
        .await
        .expect("explicit temporary namespace");
    let mut cases = Vec::new();
    for (table, statement) in [
        ("vod_movie", "list_movie_update"),
        ("vod_series", "advanced_series_update"),
    ] {
        sqlx::query(&format!(
            "CREATE TEMP TABLE {table} ON COMMIT DROP AS SELECT * FROM public.{table} WITH NO DATA"
        ))
        .execute(&mut *tx)
        .await
        .expect("shadow table without defaults, FKs or sequence allocation");
        let shadowed: bool = sqlx::query_scalar(&format!("SELECT to_regclass('{table}')=to_regclass('pg_temp.{table}') AND to_regclass('{table}')<>to_regclass('public.{table}')"))
            .fetch_one(&mut *tx).await.expect("temporary table resolution");
        assert!(shadowed);
        let sql = statements()
            .into_iter()
            .find(|(name, _)| *name == statement)
            .expect("actual exported production UPDATE")
            .1;
        for reversed in [false, true] {
            sqlx::query(&format!("DELETE FROM pg_temp.{table}"))
                .execute(&mut *tx)
                .await
                .expect("reset temporary row only");
            let extra_columns = if table == "vod_movie" {
                ",is_adult,duration_secs"
            } else {
                ""
            };
            let extra_values = if table == "vod_movie" {
                ",false,NULL"
            } else {
                ""
            };
            let insert = format!("INSERT INTO pg_temp.{table}(id,uuid,name,description,year,tmdb_id,imdb_id,language,genre,rating,logo_id,custom_properties,created_at,updated_at{extra_columns}) VALUES(1700000001,'39124190-2be3-42d6-9ccf-18e13d72a8d2'::uuid,'Owned old','old description',2010,'7101','tt8101','','',NULL,NULL,NULL,'2020-01-01T00:00:00Z'::timestamptz,'2020-01-02T00:00:00Z'::timestamptz{extra_values})");
            sqlx::query(&insert)
                .execute(&mut *tx)
                .await
                .expect("explicit seed without nextval");
            let select = format!("SELECT to_jsonb(m) FROM pg_temp.{table} m WHERE id=1700000001");
            let before: Value = sqlx::query_scalar(&select)
                .fetch_one(&mut *tx)
                .await
                .expect("complete seed row");
            let first = json!({"initial_id":1700000001,"name":"Incoming renamed","description":"","year":2010,"tmdb_id":"7101","imdb_id":"tt8101","genre":"","rating":null,"duration":null,"adult":null,"properties":null,"logo_id":null});
            let mut second = first.clone();
            second["name"] = json!("Owned old");
            second["description"] = json!("second description");
            let payload = if reversed {
                json!([second, first])
            } else {
                json!([first, second])
            };
            let observed = sqlx::query(&sql)
                .bind(payload)
                .execute(&mut *tx)
                .await
                .expect("actual production UPDATE control");
            let after: Value = sqlx::query_scalar(&select)
                .fetch_one(&mut *tx)
                .await
                .expect("complete resulting row");
            let mut expected = before.clone();
            if reversed {
                expected["description"] = json!("second description");
            }
            let affected_expected = if reversed { 1u64 } else { 0u64 };
            cases.push(json!({"statement":statement,"reverse_order":reversed,"production_SQL":sql,"actual_rows_affected":observed.rows_affected(),"expected_rows_affected":affected_expected,"full_seed_row_expected":expected,"full_observed_row":after,"passed":after==expected && observed.rows_affected()==affected_expected,"temporary_table_resolution_verified":shadowed}));
        }
    }
    tx.rollback()
        .await
        .expect("rollback all temporary schema and actual DML");
    let public_after: Value = sqlx::query_scalar(public_sql)
        .fetch_one(&mut connection)
        .await
        .expect("public state after rollback");
    let no_temp: bool = sqlx::query_scalar("SELECT to_regclass('pg_temp.vod_movie') IS NULL AND to_regclass('pg_temp.vod_series') IS NULL")
        .fetch_one(&mut connection).await.expect("rollback removes shadow tables");
    let ready = connection.ping().await;
    let closed = connection.close().await;
    let passed = cases.len() == 4
        && cases.iter().all(|v| v["passed"] == true)
        && public_before == public_after
        && no_temp
        && ready.is_ok()
        && closed.is_ok();
    let report = json!({"passed":passed,"schema_only":false,"DML_executed":true,"DML_scope":"Actual production Movie/Series UPDATE on transaction-local shadow tables only","transaction_rolled_back":true,"public_model_rows_and_sequence_catalog_exact":public_before==public_after,"temporary_tables_absent_after_rollback":no_temp,"explicit_seed_no_nextval":true,"statement_cache_disabled":true,"cases":cases,"ready":format!("{ready:?}"),"closed":format!("{closed:?}"),"task_or_media_executed":false,"performance_verified":false,"full_milestone_accepted":false});
    let bytes = serde_json::to_vec_pretty(&report).expect("semantic report");
    assert!(bytes.len() <= 131072);
    std::fs::write(
        Path::new("/evidence/native-vod-name-queue-rollback-private.json"),
        bytes,
    )
    .expect("persist semantic receipt before assertion");
    assert!(
        passed,
        "actual production name-only queue ordering rollback control failed"
    );
}

#[tokio::test]
#[ignore = "owned copied Source schema; actual temporary DML and deferred COMMIT controls"]
async fn actual_live_group_settings_transactions() {
    let url = std::env::var("OWNED_MATCHING_SCHEMA_URL").expect("owned URL");
    let options = sqlx::postgres::PgConnectOptions::from_str(&url).expect("options");
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
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect_with(options.statement_cache_capacity(0).options([
            ("default_transaction_read_only", "off"),
            ("statement_timeout", "5000"),
        ]))
        .await
        .expect("one owned physical connection");
    let mut connection = pool.acquire().await.expect("owned connection");
    let original_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *connection)
        .await
        .expect("PID");
    let public_before = live_public_state(&mut connection).await;
    std::fs::write(
        "/evidence/live-settings-public-before-private.json",
        serde_json::to_vec_pretty(&public_before).unwrap(),
    )
    .unwrap();
    sqlx::query("SET search_path TO pg_temp,public")
        .execute(&mut *connection)
        .await
        .expect("temporary namespace");
    for table in [
        "m3u_m3uaccount",
        "dispatcharr_channels_channelgroup",
        "vod_vodcategory",
        "dispatcharr_channels_channelgroupm3uaccount",
        "vod_m3uvodcategoryrelation",
    ] {
        sqlx::query(&format!(
            "CREATE TEMP TABLE {table} AS SELECT * FROM public.{table} WITH NO DATA"
        ))
        .execute(&mut *connection)
        .await
        .expect("typed clone without public defaults");
        sqlx::query(&format!("ALTER TABLE pg_temp.{table} ADD PRIMARY KEY(id)"))
            .execute(&mut *connection)
            .await
            .expect("temporary PK");
        let resolves: bool = sqlx::query_scalar(&format!("SELECT to_regclass('{table}')=to_regclass('pg_temp.{table}') AND to_regclass('{table}')<>to_regclass('public.{table}')"))
            .fetch_one(&mut *connection).await.expect("namespace proof");
        assert!(resolves);
    }
    for (table, sequence, unique) in [
        (
            "dispatcharr_channels_channelgroupm3uaccount",
            "live_settings_owned_ids",
            "channel_group_id,m3u_account_id",
        ),
        (
            "vod_m3uvodcategoryrelation",
            "category_settings_owned_ids",
            "m3u_account_id,category_id",
        ),
    ] {
        sqlx::query(&format!("CREATE TEMP SEQUENCE {sequence}"))
            .execute(&mut *connection)
            .await
            .expect("TEMP sequence");
        sqlx::query(&format!(
            "ALTER SEQUENCE pg_temp.{sequence} OWNED BY pg_temp.{table}.id"
        ))
        .execute(&mut *connection)
        .await
        .expect("TEMP-owned sequence");
        sqlx::query(&format!("ALTER TABLE pg_temp.{table} ALTER COLUMN id SET DEFAULT nextval('pg_temp.{sequence}'::regclass), ADD UNIQUE({unique})"))
            .execute(&mut *connection).await.expect("only TEMP nextval and unique index");
    }
    let mut constraint_names = std::collections::BTreeMap::new();
    for (table, column, target) in [
        (
            "dispatcharr_channels_channelgroupm3uaccount",
            "channel_group_id",
            "dispatcharr_channels_channelgroup",
        ),
        (
            "dispatcharr_channels_channelgroupm3uaccount",
            "m3u_account_id",
            "m3u_m3uaccount",
        ),
        (
            "vod_m3uvodcategoryrelation",
            "category_id",
            "vod_vodcategory",
        ),
        (
            "vod_m3uvodcategoryrelation",
            "m3u_account_id",
            "m3u_m3uaccount",
        ),
    ] {
        let name: String = sqlx::query_scalar("SELECT c.conname FROM pg_constraint c JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=c.conkey[1] WHERE c.conrelid=$1::regclass AND c.contype='f' AND array_length(c.conkey,1)=1 AND a.attname=$2")
            .bind(format!("public.{table}")).bind(column).fetch_one(&mut *connection).await.expect("actual Source FK name");
        constraint_names.insert((table, column), name.clone());
        let name = name.replace('"', "\"\"");
        sqlx::query(&format!("ALTER TABLE pg_temp.{table} ADD CONSTRAINT \"{name}\" FOREIGN KEY({column}) REFERENCES pg_temp.{target}(id) DEFERRABLE INITIALLY DEFERRED"))
            .execute(&mut *connection).await.expect("real deferred FK, no SET CONSTRAINTS stand-in");
    }
    let deferred: bool = sqlx::query_scalar("SELECT count(*)=4 AND bool_and(condeferrable AND condeferred) FROM pg_constraint WHERE conrelid IN(to_regclass('pg_temp.dispatcharr_channels_channelgroupm3uaccount'),to_regclass('pg_temp.vod_m3uvodcategoryrelation')) AND contype='f'")
        .fetch_one(&mut *connection).await.expect("actual deferred constraint catalog");
    assert!(deferred);
    drop(connection);
    let mut cases = Vec::new();
    for kind in [
        "duplicate-live",
        "missing-live-FK",
        "missing-category-FK",
        "healthy-combined",
    ] {
        let mut connection = pool.acquire().await.expect("reset owned fixture");
        sqlx::raw_sql("DELETE FROM pg_temp.dispatcharr_channels_channelgroupm3uaccount; DELETE FROM pg_temp.vod_m3uvodcategoryrelation; DELETE FROM pg_temp.m3u_m3uaccount; DELETE FROM pg_temp.dispatcharr_channels_channelgroup; DELETE FROM pg_temp.vod_vodcategory;
INSERT INTO pg_temp.m3u_m3uaccount(id,name,custom_properties,max_streams,is_active,account_type) VALUES(1700000011,'Owned physical API','{}',0,true,'XC');
INSERT INTO pg_temp.dispatcharr_channels_channelgroup(id,name) VALUES(1700000021,'Owned first'),(1700000022,'Owned second');
INSERT INTO pg_temp.vod_vodcategory(id,name,category_type,created_at,updated_at) VALUES(1700000031,'Owned Movie','movie','2020-01-01T00:00:00Z','2020-01-02T00:00:00Z'),(1700000032,'Owned Series','series','2020-01-01T00:00:00Z','2020-01-02T00:00:00Z');
INSERT INTO pg_temp.dispatcharr_channels_channelgroupm3uaccount(id,channel_group_id,m3u_account_id,enabled,auto_channel_sync,auto_sync_channel_start,auto_sync_channel_end,custom_properties,last_seen,is_stale) VALUES(41,1700000021,1700000011,true,false,1,NULL,'{\"baseline\":true}','2020-01-01T00:00:00Z',false),(42,1700000022,1700000011,true,false,1,NULL,'{\"baseline\":true}','2020-01-01T00:00:00Z',false);
INSERT INTO pg_temp.vod_m3uvodcategoryrelation(id,m3u_account_id,category_id,enabled,custom_properties,created_at,updated_at) VALUES(51,1700000011,1700000031,true,'{\"language\":\"en\",\"keep\":true}','2020-01-01T00:00:00Z','2020-01-02T00:00:00Z'),(52,1700000011,1700000032,true,'{\"language\":\"en\",\"keep\":true}','2020-01-01T00:00:00Z','2020-01-02T00:00:00Z');
SELECT setval('pg_temp.live_settings_owned_ids',100,true); SELECT setval('pg_temp.category_settings_owned_ids',200,true);")
            .execute(&mut *connection).await.expect("explicit TEMP seed; never public nextval");
        let before = live_temporary_state(&mut connection).await;
        let branches_before = crate::vod_category_settings::physical_error_branches();
        let live = |group| json!({"channel_group":group,"enabled":false,"auto_channel_sync":true,"auto_sync_channel_start":1.5,"auto_sync_channel_end":9.5,"custom_properties":{"channel_profile_ids":["1","bad"],"sent":true}});
        let category = |id| json!({"id":id,"enabled":false,"custom_properties":{"quality":"720p"}});
        let mut groups = vec![
            live(1700000021),
            live(if kind == "missing-live-FK" {
                1700000299
            } else {
                1700000022
            }),
        ];
        if kind == "duplicate-live" {
            groups.push(live(1700000021));
        }
        let body = json!({"group_settings":groups,"category_settings":[category(1700000031),category(if kind=="missing-category-FK" {1700000399} else {1700000032})]});
        drop(connection);
        let outcome = crate::vod_category_settings::physical_apply_settings(
            pool.acquire().await.expect("actual helper owns connection"),
            1700000011,
            body.clone(),
        )
        .await;
        let mut connection = pool.acquire().await.expect("immediate same-pool GET");
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *connection)
            .await
            .expect("PID after request");
        let idle = !connection.is_in_transaction();
        let ready = connection.ping().await;
        let get = crate::provider_reads::physical_account_detail(&mut connection, 1700000011).await;
        let after = live_temporary_state(&mut connection).await;
        let branches_after = crate::vod_category_settings::physical_error_branches();
        let (status, response) = match outcome {
            Ok(value) => (200, value.0),
            Err(crate::StoreError::StatusBody(status, value)) => (status, value),
            Err(error) => (0, json!({"unexpected_error":error.to_string()})),
        };
        let success = kind == "healthy-combined";
        let expected_branches = if kind == "duplicate-live" {
            (1, 0)
        } else if success {
            (0, 0)
        } else {
            (0, 1)
        };
        let branch_delta = (
            branches_after.0 - branches_before.0,
            branches_after.1 - branches_before.1,
        );
        let expected_live_delta = if kind == "duplicate-live" { 3 } else { 2 };
        let expected_category_delta = if kind == "duplicate-live" { 0 } else { 2 };
        let live_delta = after["live_sequence"]["last_value"].as_i64().unwrap()
            - before["live_sequence"]["last_value"].as_i64().unwrap();
        let category_delta = after["category_sequence"]["last_value"].as_i64().unwrap()
            - before["category_sequence"]["last_value"].as_i64().unwrap();
        let mut rows_expected = before["rows"].clone();
        if success {
            for row in rows_expected["live"].as_array_mut().unwrap() {
                row["enabled"] = json!(false);
                row["auto_channel_sync"] = json!(true);
                row["auto_sync_channel_start"] = json!(1.5);
                row["auto_sync_channel_end"] = json!(9.5);
                row["custom_properties"] = json!({"channel_profile_ids":[1],"sent":true});
            }
            for row in rows_expected["categories"].as_array_mut().unwrap() {
                row["enabled"] = json!(false);
                row["custom_properties"] = json!({"language":"en","keep":true,"quality":"720p"});
            }
        }
        let error_text = response["error"].as_str().unwrap_or("");
        let message_ok = if success {
            response == json!({"message":"Group settings updated successfully"})
        } else if kind == "duplicate-live" {
            error_text=="Failed to update group settings: ON CONFLICT DO UPDATE command cannot affect row a second time\nHINT:  Ensure that no rows proposed for insertion within the same command have duplicate constrained values."
        } else {
            let (table, column, invalid, target) = if kind == "missing-live-FK" {
                (
                    "dispatcharr_channels_channelgroupm3uaccount",
                    "channel_group_id",
                    1700000299_i64,
                    "dispatcharr_channels_channelgroup",
                )
            } else {
                (
                    "vod_m3uvodcategoryrelation",
                    "category_id",
                    1700000399_i64,
                    "vod_vodcategory",
                )
            };
            let constraint = &constraint_names[&(table, column)];
            error_text == format!("Failed to update group settings: insert or update on table \"{table}\" violates foreign key constraint \"{constraint}\"\nDETAIL:  Key ({column})=({invalid}) is not present in table \"{target}\".")
        };
        let get_ok = get.as_ref().is_ok_and(|value| {
            value["id"] == 1700000011
                && value["channel_groups"]
                    .as_array()
                    .is_some_and(|v| v.len() == 2)
        });
        let passed = status == if success { 200 } else { 400 }
            && message_ok
            && branch_delta == expected_branches
            && pid == original_pid
            && idle
            && ready.is_ok()
            && get_ok
            && after["rows"] == rows_expected
            && live_delta == expected_live_delta
            && category_delta == expected_category_delta;
        cases.push(json!({"case":kind,"passed":passed,"request":body,"status":status,"response":response,"before":before,"after":after,"expected_rows":rows_expected,"actual_operation_error_branch_delta":branch_delta.0,"actual_commit_error_branch_delta":branch_delta.1,"same_backend_PID":pid==original_pid,"transaction_idle":idle,"Ready":format!("{ready:?}"),"actual_production_GET_query_and_serializer":get.as_ref().ok(),"GET_succeeded":get_ok,"live_sequence_delta":live_delta,"category_sequence_delta":category_delta}));
        std::fs::write(
            "/evidence/native-live-settings-transactions-private.json",
            serde_json::to_vec_pretty(&json!({"passed":false,"DML_executed":true,"cases":cases}))
                .unwrap(),
        )
        .unwrap();
    }
    let mut connection = pool.acquire().await.expect("final owned connection");
    sqlx::raw_sql("DROP TABLE pg_temp.dispatcharr_channels_channelgroupm3uaccount; DROP TABLE pg_temp.vod_m3uvodcategoryrelation; DROP TABLE pg_temp.m3u_m3uaccount; DROP TABLE pg_temp.dispatcharr_channels_channelgroup; DROP TABLE pg_temp.vod_vodcategory;")
        .execute(&mut *connection).await.expect("drop temporary fixtures/owned sequences");
    let absent: bool=sqlx::query_scalar("SELECT to_regclass('pg_temp.live_settings_owned_ids') IS NULL AND to_regclass('pg_temp.category_settings_owned_ids') IS NULL AND to_regclass('pg_temp.dispatcharr_channels_channelgroupm3uaccount') IS NULL AND to_regclass('pg_temp.vod_m3uvodcategoryrelation') IS NULL")
        .fetch_one(&mut *connection).await.expect("temporary cleanup");
    let public_after = live_public_state(&mut connection).await;
    std::fs::write(
        "/evidence/live-settings-public-after-private.json",
        serde_json::to_vec_pretty(&public_after).unwrap(),
    )
    .unwrap();
    let final_ready = connection.ping().await;
    drop(connection);
    pool.close().await;
    let passed = cases.len() == 4
        && cases.iter().all(|v| v["passed"] == true)
        && absent
        && public_before == public_after
        && final_ready.is_ok();
    let report = json!({"passed":passed,"DML_executed":true,"schema_only":false,"temporary_typed_tables_and_OWNED_sequences_only":true,"real_deferred_COMMIT_error_cases":2,"SET_CONSTRAINTS_standin_used":false,"actual_production_parser_transaction_and_GET_serializer":true,"HTTP_auth_tested":false,"public15_tables_and_all_public_sequence_states_exact":public_before==public_after,"temporary_tables_and_sequences_absent":absent,"cases":cases,"final_Ready":format!("{final_ready:?}"),"pool_closed_awaited":true,"task_or_media_executed":false,"performance_verified":false,"full_milestone_accepted":false});
    std::fs::write(
        "/evidence/native-live-settings-transactions-private.json",
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert!(
        passed,
        "actual production live-settings transaction controls failed; receipt retained"
    );
}

async fn live_temporary_state(connection: &mut sqlx::PgConnection) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object('rows',jsonb_build_object('live',(SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM pg_temp.dispatcharr_channels_channelgroupm3uaccount t),'categories',(SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM pg_temp.vod_m3uvodcategoryrelation t)),'live_sequence',(SELECT jsonb_build_object('last_value',s.last_value,'log_cnt',s.log_cnt,'is_called',s.is_called) FROM pg_temp.live_settings_owned_ids s),'category_sequence',(SELECT jsonb_build_object('last_value',s.last_value,'log_cnt',s.log_cnt,'is_called',s.is_called) FROM pg_temp.category_settings_owned_ids s))")
        .fetch_one(connection).await.expect("full temporary rows and actual sequence states")
}
async fn live_public_state(connection: &mut sqlx::PgConnection) -> Value {
    let mut rows = serde_json::Map::new();
    for table in [
        "m3u_m3uaccount",
        "vod_movie",
        "vod_series",
        "vod_episode",
        "vod_m3umovierelation",
        "vod_m3useriesrelation",
        "vod_m3uepisoderelation",
        "vod_vodlogo",
        "vod_vodcategory",
        "vod_m3uvodcategoryrelation",
        "core_systemevent",
        "dispatcharr_channels_channelgroup",
        "dispatcharr_channels_channelgroupm3uaccount",
        "dispatcharr_channels_channelprofile",
        "dispatcharr_channels_channelprofilemembership",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM public.{table}"))
            .fetch_one(&mut *connection)
            .await
            .expect("bounded public table");
        assert!(count <= 4096);
        let value: Value = sqlx::query_scalar(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY id),'[]'::jsonb) FROM public.{table} t"
        ))
        .fetch_one(&mut *connection)
        .await
        .expect("literal public rows");
        rows.insert(table.to_owned(), value);
    }
    let names:Vec<String>=sqlx::query_scalar("SELECT sequencename::text FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename").fetch_all(&mut *connection).await.expect("public sequence names");
    assert!(names.len() <= 256);
    let mut sequences = serde_json::Map::new();
    for name in names {
        let quoted = name.replace('"', "\"\"");
        let value: Value =
            sqlx::query_scalar(&format!("SELECT jsonb_build_object('last_value',s.last_value,'log_cnt',s.log_cnt,'is_called',s.is_called) FROM public.\"{quoted}\" s"))
                .fetch_one(&mut *connection)
                .await
                .expect("literal public sequence state; never nextval");
        sequences.insert(name, value);
    }
    json!({"rows":rows,"sequences":sequences})
}
