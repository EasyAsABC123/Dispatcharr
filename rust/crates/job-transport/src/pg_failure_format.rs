//! Bounded P0001 message renderer from actual pinned Django/psycopg observations.
//! No generic SQLSTATE classification, worker attachment or Python frame generation.
//! Original Dispatcharr/PostgreSQL/Django/psycopg authors retain full credit/licenses.
use sqlx::postgres::PgDatabaseError;
const MAX_FIELD: usize = 8192;
const MAX_OUTPUT: usize = 8192;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    UnsupportedSqlstate,
    UnsupportedPosition,
    Capacity,
}
#[derive(Clone, Copy, Debug)]
pub struct Fields<'a> {
    pub sqlstate: &'a str,
    pub message: &'a str,
    pub detail: Option<&'a str>,
    pub hint: Option<&'a str>,
    pub context: Option<&'a str>,
    pub has_position: bool,
}
/// No driver Display prefixes, severity headers or invented QUERY/caret lines.
/// Cursor/internal positions are rejected until their own Source vectors exist.
pub fn render_pg(error: &PgDatabaseError) -> Result<String, Error> {
    render(Fields {
        sqlstate: error.code(),
        message: error.message(),
        detail: error.detail(),
        hint: error.hint(),
        context: error.r#where(),
        has_position: error.position().is_some(),
    })
}
pub fn render(fields: Fields<'_>) -> Result<String, Error> {
    if fields.sqlstate != "P0001" {
        return Err(Error::UnsupportedSqlstate);
    }
    if fields.has_position {
        return Err(Error::UnsupportedPosition);
    }
    if [
        Some(fields.message),
        fields.detail,
        fields.hint,
        fields.context,
    ]
    .into_iter()
    .flatten()
    .any(|value| value.len() > MAX_FIELD)
    {
        return Err(Error::Capacity);
    }
    // At most four individually bounded fields plus fixed labels are built;
    // final Python-style stripping may shorten an otherwise valid result.
    let mut text = String::from(fields.message);
    for (label, value) in [
        ("DETAIL", fields.detail),
        ("HINT", fields.hint),
        ("CONTEXT", fields.context),
    ] {
        if let Some(value) = value {
            text.push('\n');
            text.push_str(label);
            text.push_str(":  ");
            text.push_str(value);
        }
    }
    let trimmed=text.trim_matches(|ch:char|matches!(ch,'\u{0009}'..='\u{000d}'|'\u{001c}'..='\u{0020}'|'\u{0085}'|'\u{00a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'));
    if trimmed.len() > MAX_OUTPUT {
        return Err(Error::Capacity);
    }
    Ok(trimmed.to_owned())
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    fn corpus() -> Value {
        serde_json::from_str(include_str!("../datasets/source-pg-error-format-2.json")).unwrap()
    }
    #[test]
    fn actual_source_p0001_fields_render_exactly_for_all35_observations() {
        let source = corpus();
        let cases = source["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 35);
        for case in cases {
            let f = &case["fields"];
            let got = render(Fields {
                sqlstate: f["sqlstate"].as_str().unwrap(),
                message: f["message_primary"].as_str().unwrap(),
                detail: f["message_detail"].as_str(),
                hint: f["message_hint"].as_str(),
                context: f["context"].as_str(),
                has_position: !f["statement_position"].is_null()
                    || !f["internal_position"].is_null(),
            })
            .unwrap();
            assert_eq!(got, case["rendered"].as_str().unwrap(), "{}", case["name"]);
        }
    }
    #[test]
    fn unrelated_sqlstates_positions_and_oversized_fields_are_not_classified() {
        let good = Fields {
            sqlstate: "P0001",
            message: "owned",
            detail: None,
            hint: None,
            context: None,
            has_position: false,
        };
        assert_eq!(
            render(Fields {
                sqlstate: "23505",
                ..good
            }),
            Err(Error::UnsupportedSqlstate)
        );
        assert_eq!(
            render(Fields {
                has_position: true,
                ..good
            }),
            Err(Error::UnsupportedPosition)
        );
        assert_eq!(
            render(Fields {
                message: &"x".repeat(MAX_FIELD + 1),
                ..good
            }),
            Err(Error::Capacity)
        );
        assert_eq!(
            render(Fields {
                message: &"x".repeat(MAX_FIELD),
                detail: Some("y"),
                ..good
            }),
            Err(Error::Capacity)
        );
    }
    #[tokio::test]
    #[ignore = "CopiedDB actual PostgreSQL error-field adapter, Source-created owned functions only."]
    async fn native_pg_failure_format_peer() {
        use sqlx::{postgres::PgConnectOptions, Connection, PgConnection};
        assert_eq!(
            std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
        );
        let db = std::env::var("POSTGRES_DB").unwrap();
        assert!(
            db.starts_with("rust_probe_backup_jobs_")
                && db.len() <= 63
                && db
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        );
        let path = std::env::var("PG_FAILURE_INPUT_PATH").unwrap();
        assert!(
            path.starts_with("/evidence/pg-input-")
                && path.ends_with(".json")
                && !path.contains("..")
                && path.len() < 160
        );
        let meta = std::fs::metadata(&path).unwrap();
        assert!(meta.is_file() && meta.len() < 65536);
        let input: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let function = input["function"].as_str().unwrap();
        assert!(
            function.starts_with("owned_pg_error_")
                && function.len() <= 63
                && function
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        );
        let output = input["output"].as_str().unwrap();
        assert!(
            output.starts_with("/evidence/pg-output-")
                && output.ends_with(".json")
                && !output.contains("..")
                && output.len() < 160
        );
        let options = PgConnectOptions::new()
            .host("127.0.0.1")
            .port(5432)
            .username("dispatch")
            .password("playback-fixture-only")
            .database(&db);
        let mut connection = PgConnection::connect_with(&options).await.unwrap();
        let query = format!("SELECT \"{function}\"()");
        let error = sqlx::query(&query)
            .execute(&mut connection)
            .await
            .unwrap_err();
        let database = error
            .as_database_error()
            .unwrap()
            .try_downcast_ref::<PgDatabaseError>()
            .unwrap();
        let formatted = render_pg(database).unwrap();
        let value = json!({"sqlstate":database.code(),"message_primary":database.message(),"message_detail":database.detail(),"message_hint":database.hint(),"context":database.r#where(),"has_position":database.position().is_some(),"rendered":formatted});
        connection.close().await.unwrap();
        std::fs::write(output, serde_json::to_vec(&value).unwrap()).unwrap();
    }
}
