//! Exact ordinary PostgreSQL COPY projections for the read-only taskless plan.
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug)]
pub(super) struct Invalid;

const PLUGIN: &[&str] = &[
    "id",
    "key",
    "name",
    "version",
    "description",
    "enabled",
    "ever_enabled",
    "settings",
    "owned_tasks",
    "suspended_schedules",
    "source_repo_id",
    "slug",
    "installed_version_is_prerelease",
    "deprecated",
    "created_at",
    "updated_at",
];
const PERIODIC: &[&str] = &[
    "id",
    "name",
    "task",
    "enabled",
    "queue",
    "exchange",
    "routing_key",
    "args",
    "kwargs",
    "interval_id",
    "crontab_id",
    "solar_id",
    "clocked_id",
];

type Row<'a> = BTreeMap<&'a str, Option<String>>;

fn decode(text: &str) -> Result<Option<String>, Invalid> {
    if text == r"\N" {
        return Ok(None);
    }
    let mut input = text.bytes();
    let mut out = Vec::with_capacity(text.len());
    while let Some(byte) = input.next() {
        if byte != b'\\' {
            out.push(byte);
            continue;
        }
        // A real pg_restore COPY text projection escapes these literal cells.
        // Other formats remain outside this initial ordinary fixture scope.
        out.push(match input.next().ok_or(Invalid)? {
            b'\\' => b'\\',
            b'b' => 8,
            b'f' => 12,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'v' => 11,
            _ => return Err(Invalid),
        });
    }
    String::from_utf8(out).map(Some).map_err(|_| Invalid)
}

fn projection<'a>(
    bytes: &'a [u8],
    table: &str,
    expected: &[&str],
    max_rows: usize,
) -> Result<Vec<Row<'a>>, Invalid> {
    if bytes.len() > 1024 * 1024 {
        return Err(Invalid);
    }
    let mut lines = std::str::from_utf8(bytes).map_err(|_| Invalid)?.lines();
    let prefix = format!("COPY public.{table} (");
    let columns: Vec<_> = lines
        .by_ref()
        .find(|line| line.starts_with("COPY "))
        .and_then(|line| line.strip_prefix(&prefix)?.strip_suffix(") FROM stdin;"))
        .ok_or(Invalid)?
        .split(", ")
        .collect();
    let distinct = columns.iter().copied().collect::<BTreeSet<_>>();
    if columns.len() != distinct.len()
        || columns.len() > 64
        || (table == "plugins_pluginconfig" && columns.len() != expected.len())
        || expected.iter().any(|key| !distinct.contains(key))
    {
        return Err(Invalid);
    }
    let mut rows = Vec::new();
    for line in lines.by_ref() {
        if line == r"\." {
            if lines.any(|after| after.starts_with("COPY ") || after == r"\.") {
                return Err(Invalid);
            }
            return Ok(rows);
        }
        let values = line.splitn(columns.len() + 1, '\t').collect::<Vec<_>>();
        if rows.len() >= max_rows || values.len() != columns.len() {
            return Err(Invalid);
        }
        rows.push(
            columns
                .iter()
                .copied()
                .zip(values)
                .filter(|(name, _)| expected.contains(name))
                .map(|(name, raw)| Ok((name, decode(raw)?)))
                .collect::<Result<Row<'_>, Invalid>>()?,
        );
    }
    Err(Invalid)
}

fn timestamp(text: &str) -> Result<String, Invalid> {
    let at = DateTime::parse_from_rfc3339(text)
        .or_else(|_| DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z"))
        .map_err(|_| Invalid)?;
    Ok(at.with_timezone(&Utc).to_rfc3339())
}

fn translated(row: &Row<'_>, fields: &[&str]) -> Result<Value, Invalid> {
    let mut result = Map::new();
    for &key in fields {
        let cell = row.get(key).ok_or(Invalid)?.as_deref();
        let value = match key {
            "id" | "source_repo_id" | "interval_id" | "crontab_id" | "solar_id" | "clocked_id" => {
                match cell {
                    Some(raw) => {
                        Value::from(raw.parse::<i64>().ok().filter(|v| *v > 0).ok_or(Invalid)?)
                    }
                    None if key != "id" => Value::Null,
                    None => return Err(Invalid),
                }
            }
            "enabled" | "ever_enabled" | "installed_version_is_prerelease" | "deprecated" => {
                Value::Bool(match cell {
                    Some("t") => true,
                    Some("f") => false,
                    _ => return Err(Invalid),
                })
            }
            "settings" | "owned_tasks" | "suspended_schedules" => {
                serde_json::from_str(cell.ok_or(Invalid)?).map_err(|_| Invalid)?
            }
            "created_at" | "updated_at" => Value::from(timestamp(cell.ok_or(Invalid)?)?),
            "queue" | "exchange" | "routing_key" => cell.map_or(Value::Null, Value::from),
            _ => Value::from(cell.ok_or(Invalid)?),
        };
        result.insert(key.to_owned(), value);
    }
    Ok(Value::Object(result))
}

fn canonical_plugin(row: &Value) -> Result<Value, Invalid> {
    let mut object = row
        .as_object()
        .filter(|row| row.len() == PLUGIN.len())
        .cloned()
        .ok_or(Invalid)?;
    if PLUGIN.iter().any(|key| !object.contains_key(*key))
        || object["enabled"] != true
        || object["ever_enabled"] != true
        || object["source_repo_id"] != Value::Null
        || object["slug"] != ""
        || object["deprecated"] != false
        || object["installed_version_is_prerelease"] != false
        || object["owned_tasks"]
            .as_array()
            .is_none_or(|v| !v.is_empty())
        || object["suspended_schedules"]
            .as_array()
            .is_none_or(|v| !v.is_empty())
        || !object["settings"].is_object()
        || object["id"].as_i64().is_none_or(|v| v <= 0)
        || object["key"]
            .as_str()
            .is_none_or(|s| s.is_empty() || s.len() > 128)
        || ["name", "version", "description"]
            .iter()
            .any(|key| !object[*key].is_string())
    {
        return Err(Invalid);
    }
    for field in ["created_at", "updated_at"] {
        let value = timestamp(object[field].as_str().ok_or(Invalid)?)?;
        object.insert(field.to_owned(), Value::from(value));
    }
    Ok(Value::Object(object))
}

fn schedules(
    rows: &[Value],
    builtin: &BTreeSet<String>,
) -> Result<BTreeSet<(String, String)>, Invalid> {
    let mut names = BTreeSet::new();
    let mut identities = BTreeSet::new();
    if rows.len() > 256 {
        return Err(Invalid);
    }
    for row in rows {
        let row = row
            .as_object()
            .filter(|row| row.len() == PERIODIC.len())
            .ok_or(Invalid)?;
        let task = row
            .get("task")
            .and_then(Value::as_str)
            .filter(|name| builtin.contains(*name))
            .ok_or(Invalid)?;
        let name = row
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .ok_or(Invalid)?;
        if row["enabled"] != false
            || PERIODIC.iter().any(|key| !row.contains_key(*key))
            || !names.insert(name)
            || !identities.insert((name.to_owned(), task.to_owned()))
        {
            return Err(Invalid);
        }
    }
    Ok(identities)
}

pub(super) fn taskless_projection(
    plugin: &[u8],
    periodic: &[u8],
    current: &Value,
    live_periodic: &[Value],
    builtin: &BTreeSet<String>,
) -> Result<(Value, Vec<Value>), Invalid> {
    let [plugin] = projection(plugin, "plugins_pluginconfig", PLUGIN, 1)?
        .try_into()
        .map_err(|_| Invalid)?;
    let archived = canonical_plugin(&translated(&plugin, PLUGIN)?)?;
    let current = canonical_plugin(current)?;
    if ["key", "name", "version", "description"]
        .iter()
        .any(|field| archived[*field] != current[*field])
    {
        return Err(Invalid);
    }
    let mut periodic = projection(periodic, "django_celery_beat_periodictask", PERIODIC, 256)?
        .iter()
        .map(|row| translated(row, PERIODIC))
        .collect::<Result<Vec<_>, _>>()?;
    if schedules(&periodic, builtin)? != schedules(live_periodic, builtin)? {
        return Err(Invalid);
    }
    periodic.sort_by_key(|row| row["id"].as_i64());
    if periodic
        .windows(2)
        .any(|pair| pair[0]["id"] == pair[1]["id"])
    {
        return Err(Invalid);
    }
    Ok((archived, periodic))
}

pub(super) fn matches_post_restore(
    archived: &Value,
    schedules: &[Value],
    plugin: &Value,
    periodic: &[Value],
) -> bool {
    let Ok(plugin) = canonical_plugin(plugin) else {
        return false;
    };
    let mut periodic = periodic.to_vec();
    periodic.sort_by_key(|row| row["id"].as_i64());
    archived == &plugin && schedules == periodic
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn copy(table: &str, columns: &[&str], cells: &[&str]) -> Vec<u8> {
        format!(
            "-- PostgreSQL projection\nCOPY public.{table} ({}) FROM stdin;\n{}\n\\.\n",
            columns.join(", "),
            cells.join("\t")
        )
        .into_bytes()
    }

    fn current() -> Value {
        json!({"id": 8, "key": "local_fixture", "name": "Fixture\tName", "version": "1",
               "description": "restored\\description", "enabled": true, "ever_enabled": true,
               "settings": {"prefix":"current"}, "owned_tasks":[], "suspended_schedules":[],
               "source_repo_id": null, "slug":"", "installed_version_is_prerelease":false,
               "deprecated":false, "created_at":"2025-01-02T03:04:05+00:00",
               "updated_at":"2025-01-02T03:04:05+00:00"})
    }
    fn archived_fields() -> Vec<&'static str> {
        vec![
            "3",
            "local_fixture",
            r"Fixture\tName",
            "1",
            r"restored\\description",
            "t",
            "t",
            r#"{"prefix":"archived","extra":true}"#,
            "[]",
            "[]",
            r"\N",
            "",
            "f",
            "f",
            "2025-01-01 03:04:05+00",
            "2025-01-01 03:04:05.100+00",
        ]
    }
    fn periodic() -> (Vec<u8>, Vec<Value>, BTreeSet<String>) {
        let encoded = copy(
            "django_celery_beat_periodictask",
            PERIODIC,
            &[
                "7",
                "backup-task",
                "apps.backups.tasks.scheduled_backup_task",
                "f",
                r"\N",
                r"\N",
                r"\N",
                "[]",
                "{}",
                "5",
                r"\N",
                r"\N",
                r"\N",
            ],
        );
        let live = vec![
            json!({"id":5,"name":"backup-task","task":"apps.backups.tasks.scheduled_backup_task",
               "enabled":false,"queue":null,"exchange":null,"routing_key":null,"args":"[]","kwargs":"{}",
               "interval_id":1,"crontab_id":null,"solar_id":null,"clocked_id":null}),
        ];
        (
            encoded,
            live,
            BTreeSet::from(["apps.backups.tasks.scheduled_backup_task".into()]),
        )
    }

    #[test]
    fn compares_current_trust_but_preserves_archived_row_identity_settings_and_periodic() {
        let (encoded, live, builtins) = periodic();
        let (plugin, scheduled) = taskless_projection(
            &copy("plugins_pluginconfig", PLUGIN, &archived_fields()),
            &encoded,
            &current(),
            &live,
            &builtins,
        )
        .unwrap();
        assert_eq!(plugin["id"], 3);
        assert_eq!(
            plugin["settings"],
            json!({"prefix":"archived","extra":true})
        );
        assert_eq!(plugin["created_at"], "2025-01-01T03:04:05+00:00");
        assert!(matches_post_restore(
            &plugin, &scheduled, &plugin, &scheduled
        ));
        assert!(!matches_post_restore(
            &plugin,
            &scheduled,
            &current(),
            &scheduled
        ));
        assert!(!matches_post_restore(&plugin, &scheduled, &plugin, &live));
        let mut drift = plugin.clone();
        drift["updated_at"] = json!("2025-01-01T03:04:05.101+00:00");
        assert!(!matches_post_restore(
            &plugin, &scheduled, &drift, &scheduled
        ));
    }

    #[test]
    fn extra_rows_columns_unowned_periodic_or_archive_enabled_bits_cannot_supply_trust() {
        let (encoded, live, builtins) = periodic();
        let fields = archived_fields();
        let good = copy("plugins_pluginconfig", PLUGIN, &fields);
        let mut duplicate = good.clone();
        duplicate.extend(good.clone());
        assert!(taskless_projection(&duplicate, &encoded, &current(), &live, &builtins).is_err());
        let mut columns = PLUGIN.to_vec();
        columns[1] = "id";
        assert!(taskless_projection(
            &copy("plugins_pluginconfig", &columns, &fields),
            &encoded,
            &current(),
            &live,
            &builtins
        )
        .is_err());
        let mut disabled = current();
        disabled["enabled"] = json!(false);
        assert!(taskless_projection(&good, &encoded, &disabled, &live, &builtins).is_err());
        let mut enabled = live.clone();
        enabled[0]["enabled"] = json!(true);
        assert!(taskless_projection(&good, &encoded, &current(), &enabled, &builtins).is_err());
        let components = std::str::from_utf8(&encoded).unwrap();
        assert!(components.contains("\tf\t"));
        let archived_enabled = components.replace("\tf\t", "\tt\t");
        assert!(taskless_projection(
            &good,
            archived_enabled.as_bytes(),
            &current(),
            &live,
            &builtins
        )
        .is_err());
        let mut unknown = live.clone();
        unknown[0]["task"] = json!("fixture.unknown_task");
        assert!(taskless_projection(&good, &encoded, &current(), &unknown, &builtins).is_err());
        let mut drift = current();
        drift["version"] = json!("changed");
        assert!(taskless_projection(&good, &encoded, &drift, &live, &builtins).is_err());
    }
}
