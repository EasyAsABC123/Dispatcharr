//! Active XC VOD settings changes for the Selected job runtime.
//! All Dispatcharr, Django and Celery application credit remains with their authors.

use super::*;
use crate::xc_account_http::AdmittedVodSettings;

enum Publication {
    Required,
    Unchanged,
}

fn matches_request(input: &Map<String, Value>) -> bool {
    const SCOPED: &[&str] = &["name", "max_streams", "refresh_interval", "enable_vod"];
    const REACT_EXTRAS: &[&str] = &[
        "server_url",
        "user_agent",
        "server_group",
        "is_active",
        "cron_expression",
        "account_type",
        "username",
        "stale_stream_days",
        "priority",
        "hash_key",
    ];
    if input.get("enable_vod") != Some(&Value::Bool(true))
        || !SCOPED.iter().all(|name| input.contains_key(*name))
    {
        return false;
    }
    input.len() == SCOPED.len()
        || (input.len() == SCOPED.len() + REACT_EXTRAS.len()
            && REACT_EXTRAS.iter().all(|name| input.contains_key(*name)))
}

pub(super) fn eligible(input: &Map<String, Value>, old: &Value, cron: &str) -> bool {
    old["refresh_task_id"].as_i64().is_some()
        && old["refresh_interval"] == 0
        && cron.is_empty()
        && matches_request(input)
        && settings(input.clone(), old, cron).is_ok()
}

fn existing_react_values_only(input: &Map<String, Value>, old: &Value) -> bool {
    if input.len() == 4 {
        return true;
    }
    const NULLABLE_ECHOES: [(&str, &str); 2] = [
        ("user_agent", "user_agent_id"),
        ("server_group", "server_group_id"),
    ];
    NULLABLE_ECHOES
        .iter()
        .all(|(input_name, old_name)| input[*input_name].is_null() && old[*old_name].is_null())
        && input["hash_key"].is_null()
        && old["custom_properties"].get("hash_key").is_none()
        && input["cron_expression"] == ""
        && input["is_active"] == true
        && input["account_type"] == "XC"
        && input["server_url"].is_string()
        && input["username"].is_string()
        && input["stale_stream_days"].is_i64()
        && input["priority"].is_i64()
}

fn settings(
    mut input: Map<String, Value>,
    old: &Value,
    cron: &str,
) -> Result<(Value, bool, Publication), StoreError> {
    if !matches_request(&input)
        || old["account_type"] != "XC"
        || old["is_active"] != true
        || !old["custom_properties"].is_object()
        || !existing_react_values_only(&input, old)
    {
        return Err(unsupported());
    }
    input.remove("enable_vod");
    let validated = validate(input, old, cron, true, false)?;
    if validated.saved != *old
        || validated.cron != cron
        || validated.expiration.is_some()
        || validated.hash_changed
        || !validated.group_selection.is_empty()
    {
        return Err(unsupported());
    }
    let mut saved = validated.saved;
    let properties = saved["custom_properties"]
        .as_object_mut()
        .ok_or_else(unsupported)?;
    let publication = match properties.get("enable_vod").and_then(Value::as_bool) {
        Some(false) => Publication::Required,
        Some(true) => Publication::Unchanged,
        None => return Err(unsupported()),
    };
    let enabled = match properties.get("auto_enable_new_groups_vod") {
        None => true,
        Some(Value::Bool(enabled)) => *enabled,
        _ => return Err(unsupported()),
    };
    properties.insert("enable_vod".into(), Value::Bool(true));
    Ok((saved, enabled, publication))
}

async fn ensure_default(
    c: &mut PgConnection,
    account: i64,
    kind: &str,
    enabled: bool,
) -> Result<(), StoreError> {
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO vod_vodcategory(name,category_type,created_at,updated_at) \
         SELECT 'Uncategorized',$1,$2,$2 WHERE NOT EXISTS( \
             SELECT 1 FROM vod_vodcategory WHERE name='Uncategorized' AND category_type=$1 \
         ) ON CONFLICT(name,category_type) DO NOTHING",
    )
    .bind(kind)
    .bind(now)
    .execute(&mut *c)
    .await?;
    let category: i64 = sqlx::query_scalar(
        "SELECT id::bigint FROM vod_vodcategory WHERE name='Uncategorized' AND category_type=$1 FOR KEY SHARE",
    )
    .bind(kind)
    .fetch_one(&mut *c)
    .await?;
    sqlx::query(
        "INSERT INTO vod_m3uvodcategoryrelation(m3u_account_id,category_id,enabled,custom_properties,created_at,updated_at) \
         SELECT $1,$2,$3,'{}'::jsonb,$4,$4 WHERE NOT EXISTS( \
             SELECT 1 FROM vod_m3uvodcategoryrelation WHERE m3u_account_id=$1 AND category_id=$2 \
         ) ON CONFLICT(m3u_account_id,category_id) DO NOTHING",
    )
    .bind(account)
    .bind(category)
    .bind(enabled)
    .bind(now)
    .execute(c)
    .await?;
    Ok(())
}

async fn stage_sql(
    c: &mut PgConnection,
    account: i64,
    input: Map<String, Value>,
) -> Result<(Json<Value>, Publication), StoreError> {
    let mut tx = c.begin().await?;
    let row = sqlx::query(
        "SELECT CASE WHEN octet_length(to_jsonb(a)::text)<=1048576 THEN to_jsonb(a) END AS raw, \
          COALESCE(concat_ws(' ',ct.minute,ct.hour,ct.day_of_month,ct.month_of_year,ct.day_of_week),'') AS cron \
         FROM m3u_m3uaccount a \
         LEFT JOIN django_celery_beat_periodictask t ON t.id=a.refresh_task_id \
         LEFT JOIN django_celery_beat_crontabschedule ct ON ct.id=t.crontab_id \
         WHERE a.id=$1 FOR UPDATE OF a",
    )
    .bind(account)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(missing)?;
    let old: Option<Value> = row.try_get("raw")?;
    let old = old.ok_or_else(unsupported)?;
    if old["locked"] == true {
        return Err(locked());
    }
    let cron: String = row.try_get("cron")?;
    let task = old["refresh_task_id"].as_i64().ok_or_else(unsupported)?;
    let interval = old["refresh_interval"].as_i64().ok_or_else(unsupported)?;
    if interval != 0 || !cron.is_empty() {
        return Err(unsupported());
    }
    let (saved, enabled, publication) = settings(input, &old, &cron)?;
    preflight(&mut tx, Some(account), &saved).await?;
    let task_name = format!("m3u_account-refresh-{account}");
    let task_type = "apps.m3u.tasks.refresh_single_m3u_account";
    let present: Option<i64> = sqlx::query_scalar(
        "SELECT t.id::bigint FROM django_celery_beat_periodictask t \
         JOIN django_celery_beat_intervalschedule i ON i.id=t.interval_id \
         WHERE t.id=$1 AND t.name=$2 AND t.task=$3 AND NOT t.enabled \
         AND t.crontab_id IS NULL AND t.solar_id IS NULL AND t.clocked_id IS NULL \
         AND i.every=1 AND i.period='hours' FOR UPDATE OF t FOR SHARE OF i",
    )
    .bind(task)
    .bind(&task_name)
    .bind(task_type)
    .fetch_optional(&mut *tx)
    .await?;
    if present != Some(task) {
        return Err(unsupported());
    }
    let max = saved["max_streams"]
        .as_i64()
        .and_then(|v| i32::try_from(v).ok())
        .ok_or_else(unsupported)?;
    write(&mut tx, Some(account), saved).await?;
    update_default_capacity(&mut tx, account, max).await?;
    let kwargs = format!("{{\"account_id\": {account}}}");
    let linked = crate::provider_schedule::upsert(
        &mut tx,
        crate::provider_schedule::Spec {
            name: &task_name,
            task: task_type,
            kwargs: &kwargs,
            interval_hours: 0,
            cron: "",
            active: true,
            timezone: "UTC",
        },
    )
    .await?;
    if linked != task {
        return Err(unsupported());
    }
    if matches!(publication, Publication::Required) {
        for category in ["movie", "series"] {
            ensure_default(&mut tx, account, category, enabled).await?;
        }
    }
    let mut response = crate::provider_reads::physical_account_detail(&mut tx, account).await?;
    response["cron_expression"] = json!(cron);
    tx.commit().await?;
    Ok((Json(response), publication))
}

pub(super) async fn apply_then_publish(
    session: &TaskSqlSession,
    admitted: AdmittedVodSettings,
    account: i64,
    input: Map<String, Value>,
    owner: Owner,
) -> Result<Json<Value>, StoreError> {
    // The account operation is already detached. Retire SQL before handing
    // admission to the physical publisher and never replay a lost reply.
    let outcome = session
        .run(move |c| Box::pin(stage_sql(c, account, input)))
        .await;
    let closed = session.close().await;
    let (response, publication) = outcome??;
    closed?;
    if matches!(publication, Publication::Required) {
        admitted.publish(account, owner).await?;
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn old() -> Value {
        json!({"name":"Owned account","account_type":"XC","is_active":true,"max_streams":2,"refresh_interval":0,
            "custom_properties":{"enable_vod":false,"auto_enable_new_groups_vod":false,
                                 "auto_enable_new_groups_series":true,"unrelated":"preserved"}})
    }

    fn request() -> Map<String, Value> {
        json!({"enable_vod":true,"name":"Owned account","max_streams":2,"refresh_interval":0})
            .as_object()
            .unwrap()
            .clone()
    }

    fn react_echo() -> (Value, Map<String, Value>) {
        let mut stored = old();
        let readonly_echo = json!({
            "server_url":"http://origin:8080/owned-xc-vod",
            "username":"owned-form-account", "stale_stream_days":7, "priority":0
        });
        stored
            .as_object_mut()
            .unwrap()
            .extend(readonly_echo.as_object().unwrap().clone());
        stored["user_agent_id"] = Value::Null;
        stored["server_group_id"] = Value::Null;
        let mut submitted = request();
        submitted.extend(readonly_echo.as_object().unwrap().clone());
        submitted.extend(
            json!({"user_agent":null,"server_group":null,"is_active":true,
                               "cron_expression":"","account_type":"XC","hash_key":null})
            .as_object()
            .unwrap()
            .clone(),
        );
        (stored, submitted)
    }

    #[test]
    fn xc_settings_transition_changes_only_vod_and_keeps_movie_series_preference() {
        let old = old();
        let (saved, category_enabled, publication) = settings(request(), &old, "").unwrap();
        let mut expected = old;
        expected["custom_properties"]["enable_vod"] = json!(true);
        assert_eq!(saved, expected);
        assert!(!category_enabled);
        assert!(matches!(publication, Publication::Required));
        let (retry, _, publication) = settings(request(), &saved, "").unwrap();
        assert_eq!(retry, saved);
        assert!(matches!(publication, Publication::Unchanged));
    }

    #[test]
    fn xc_settings_transition_keeps_other_writes_in_existing_paths() {
        for (field, value) in [
            ("name", json!("Different name")),
            ("max_streams", json!(3)),
            ("refresh_interval", json!(1)),
            ("enable_vod", json!("true")),
        ] {
            let mut changed = request();
            changed.insert(field.into(), value);
            assert!(settings(changed, &old(), "").is_err());
        }
        let mut inactive = old();
        inactive["is_active"] = json!(false);
        assert!(settings(request(), &inactive, "").is_err());
    }

    #[test]
    fn xc_vod_settings_react_echo_preserves_all_existing_editor_fields() {
        let (old, input) = react_echo();
        let (saved, enabled, publication) = settings(input.clone(), &old, "").unwrap();
        let mut expected = old;
        expected["custom_properties"]["enable_vod"] = Value::Bool(true);
        assert_eq!(saved, expected);
        assert!(!enabled);
        assert!(matches!(publication, Publication::Required));
        assert!(matches!(
            settings(input, &saved, "").unwrap().2,
            Publication::Unchanged
        ));
    }

    #[test]
    fn xc_vod_settings_react_echo_rejects_unverified_side_effects() {
        let (old, input) = react_echo();
        for (name, value) in [
            ("server_url", json!("http://origin:8080/changed")),
            ("username", json!("changed")),
            ("stale_stream_days", json!(1)),
            ("hash_key", json!("url")),
            ("user_agent", json!(9)),
            ("server_group", json!(9)),
            ("is_active", json!(false)),
            ("password", json!("fixture omitted password")),
            ("cron_expression", json!("0 3 * * *")),
        ] {
            let mut attempt = input.clone();
            attempt.insert(name.into(), value);
            assert!(settings(attempt, &old, "").is_err(), "{name}");
        }
    }

    #[test]
    fn xc_vod_settings_leaves_existing_enabled_name_edit_with_the_old_parser() {
        let (mut old, mut input) = react_echo();
        old["custom_properties"]["enable_vod"] = json!(true);
        old["refresh_task_id"] = json!(7);
        assert!(eligible(&input, &old, ""));
        input.insert("name".into(), json!("Changed name"));
        assert!(!eligible(&input, &old, ""));
        let prior = validate(input, &old, "", true, false).unwrap();
        assert_eq!(prior.saved["name"], "Changed name");
        assert_eq!(prior.saved["custom_properties"], old["custom_properties"]);
    }
}
