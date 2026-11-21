//! Private prototype for Original Accounts 0006 to 0008 on detached SQLite files.
//! Original Dispatcharr and Django authors retain all application credit.
use super::*;
use serde_json::Value;

const STREAM: (&str, &str) = ("accounts", "0006_user_stream_limit");
const DATA: (&str, &str) = ("accounts", "0007_user_app_data");
const SPLIT: (&str, &str) = ("accounts", "0008_userappdata_and_remove_user_app_data");
pub(super) const STEPS: [(&str, &str); 3] = [STREAM, DATA, SPLIT];
const USER_SQL: &str = include_str!("../datasets/sqlite-accounts_user.sql");
const APP_DATA_SQL: &str = include_str!("../datasets/sqlite-accounts_userappdata.sql");
const APP_DATA_INDEX: &str =
    include_str!("../datasets/sqlite-accounts_userappdata_user_id_e0a86b31.sql");
const TEMP_DATA_COLUMN: &str =
    r#", "app_data" text NOT NULL CHECK ((JSON_VALID("app_data") OR "app_data" IS NULL))"#;
const ORIGINAL_COLUMNS: &str = "id,password,last_login,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined,avatar_config,user_level,custom_properties,api_key";

/// Complete only the unbroken Accounts prefix selected by the exact history.
pub(super) async fn run(path: &Path) -> Result<usize, Failure> {
    if !std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_file()) {
        return Err(Failure::OutsideKnownState);
    }
    // Match the SQLite schema editor's remake lifetime. This connection is
    // private, and checks every foreign key before the atomic commit.
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .foreign_keys(false)
        .busy_timeout(Duration::from_secs(1));
    let mut conn = SqliteConnection::connect_with(&options).await?;
    let result = async {
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await?;
        match perform(&mut tx).await {
            Ok(steps) => {
                tx.commit().await?;
                Ok(steps)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }
    .await;
    conn.close().await?;
    result
}

fn user_schema(prefix: usize) -> String {
    let current = USER_SQL.trim_end();
    match prefix {
        0 => {
            current
                .strip_suffix(r#", "stream_limit" integer NOT NULL)"#)
                .unwrap()
                .to_owned()
                + ")"
        }
        2 => current.strip_suffix(')').unwrap().to_owned() + TEMP_DATA_COLUMN + ")",
        _ => current.to_owned(),
    }
}

async fn schema(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    name: &str,
) -> Result<Option<String>, Failure> {
    Ok(
        sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE name=?")
            .bind(name)
            .fetch_optional(&mut **tx)
            .await?,
    )
}

async fn current_prefix(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<usize, Failure> {
    let history = sqlx::query_as::<_, (String, String)>(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations ORDER BY app,name LIMIT 138",
    )
    .fetch_all(&mut **tx)
    .await?;
    let prefix = history
        .len()
        .checked_sub(134)
        .filter(|n| *n <= 3)
        .ok_or(Failure::OutsideKnownState)?;
    let mut before = HISTORICAL.to_vec();
    before.extend([KEY, MANAGER]);
    before.extend(&STEPS[..prefix]);
    before.sort_unstable();
    let matched = history
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .eq(before);
    let table = schema(tx, "accounts_user").await?;
    let known_index = schema(tx, "accounts_user_api_key_dd94a1a5").await?;
    let index_count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM sqlite_schema WHERE tbl_name='accounts_user' AND type='index'",
    )
    .fetch_one(&mut **tx)
    .await?;
    let app_table = schema(tx, "accounts_userappdata").await?;
    let app_index = schema(tx, "accounts_userappdata_user_id_e0a86b31").await?;
    let app_schema = if prefix == 3 {
        app_table.as_deref() == Some(APP_DATA_SQL.trim_end())
            && app_index.as_deref() == Some(APP_DATA_INDEX.trim_end())
    } else {
        app_table.is_none() && app_index.is_none()
    };
    if !matched
        || table.as_deref() != Some(&user_schema(prefix))
        || known_index.as_deref() != Some(KEY_INDEX)
        || index_count != 2
        || !app_schema
    {
        return Err(Failure::OutsideKnownState);
    }
    Ok(prefix)
}

async fn perform(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<usize, Failure> {
    let prefix = current_prefix(tx).await?;
    for (ordinal, step) in STEPS.iter().copied().enumerate().skip(prefix) {
        match ordinal {
            0 => remake_user(tx, 1, ORIGINAL_COLUMNS, "stream_limit", "0").await?,
            1 => remake_user(tx, 2, "id,password,last_login,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined,avatar_config,user_level,custom_properties,api_key,stream_limit", "app_data", "'{}'").await?,
            2 => split_app_data(tx).await?,
            _ => unreachable!(),
        }
        record(tx, step).await?;
    }
    if sqlx::query("PRAGMA foreign_key_check")
        .fetch_optional(&mut **tx)
        .await?
        .is_some()
    {
        return Err(Failure::OutsideKnownState);
    }
    if current_prefix(tx).await? != 3 {
        return Err(Failure::OutsideKnownState);
    }
    Ok(STEPS.len() - prefix)
}

async fn remake_user(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    after: usize,
    prior_columns: &str,
    added: &str,
    backfill: &str,
) -> Result<(), Failure> {
    let ddl = user_schema(after).replacen(
        r#"CREATE TABLE "accounts_user""#,
        r#"CREATE TABLE "new__accounts_user""#,
        1,
    );
    sqlx::query(&ddl).execute(&mut **tx).await?;
    let copy = format!("INSERT INTO new__accounts_user({prior_columns},{added}) SELECT {prior_columns},{backfill} FROM accounts_user");
    sqlx::query(&copy).execute(&mut **tx).await?;
    for ddl in [
        r#"DROP TABLE "accounts_user""#,
        r#"ALTER TABLE "new__accounts_user" RENAME TO "accounts_user""#,
        KEY_INDEX,
    ] {
        sqlx::query(ddl).execute(&mut **tx).await?;
    }
    Ok(())
}

async fn split_app_data(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<(), Failure> {
    sqlx::query(APP_DATA_SQL).execute(&mut **tx).await?;
    sqlx::query(APP_DATA_INDEX).execute(&mut **tx).await?;
    let mut after = None;
    loop {
        let rows = sqlx::query_as::<_, (i64, String)>(
            "SELECT id,app_data FROM accounts_user WHERE (?1 IS NULL OR id>?1) ORDER BY id LIMIT 32",
        ).bind(after).fetch_all(&mut **tx).await?;
        if rows.is_empty() {
            break;
        }
        for (user, raw) in rows {
            let blob: Value = serde_json::from_str(&raw).map_err(|_| Failure::OutsideKnownState)?;
            if let Value::Object(entries) = blob {
                for (key, value) in entries.into_iter().filter(|(_, value)| value.is_object()) {
                    let Ok(compact) = crate::json_compat::compact_json_bytes(&value) else {
                        continue;
                    };
                    let bytes = compact
                        .len()
                        .try_into()
                        .map_err(|_| Failure::OutsideKnownState)?;
                    let storage = django_storage_json(&compact)?;
                    let key: String = key.chars().take(64).collect();
                    let now = Utc::now()
                        .naive_utc()
                        .format("%Y-%m-%d %H:%M:%S%.6f")
                        .to_string();
                    sqlx::query(
                        "INSERT OR IGNORE INTO accounts_userappdata(user_id,key,value,byte_size,updated_at) VALUES(?,?,?,?,?)",
                    ).bind(user).bind(key).bind(storage).bind::<i64>(bytes).bind(now)
                        .execute(&mut **tx).await?;
                }
            }
            after = Some(user);
        }
    }
    sqlx::query(r#"ALTER TABLE "accounts_user" DROP COLUMN "app_data""#)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Django's default SQLite JSONField text uses ASCII escapes and the normal
/// comma/colon spaces. Size accounting keeps the separate compact UTF-8 form.
pub(super) fn django_storage_json(compact: &[u8]) -> Result<String, Failure> {
    use std::fmt::Write;
    let raw = std::str::from_utf8(compact).map_err(|_| Failure::OutsideKnownState)?;
    let mut output = String::with_capacity(raw.len());
    let (mut quoted, mut escaped) = (false, false);
    for ch in raw.chars() {
        if ch > '~' {
            for unit in ch.encode_utf16(&mut [0; 2]) {
                write!(output, "\\u{unit:04x}").unwrap();
            }
        } else {
            output.push(ch);
        }
        if quoted {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                quoted = false;
            }
        } else if ch == '"' {
            quoted = true;
        } else if matches!(ch, ',' | ':') {
            output.push(' ');
        }
    }
    Ok(output)
}

#[cfg(test)]
#[path = "sqlite_accounts_appdata_contract.rs"]
mod contract;
