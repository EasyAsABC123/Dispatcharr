//! Read the copied M3U account collection required by the original React UI.
//! Original Dispatcharr authors and contributors retain application credit.
use crate::{provider_reads::account_value, StoreError};
use futures_util::TryStreamExt;
use serde_json::{json, Map, Value};
use sqlx::{types::Json, Row, SqliteConnection};

// The copied Django SQLite store persists naive UTC timestamps. Feed the
// shared projection ISO values with the original stored fractional precision.
// The ready broker and scheduled producer are outside this read transaction.
pub(crate) const QUERY: &str = r#"
    WITH profiles AS (
        SELECT p.m3u_account_id, json_group_array(json_object(
            'id', p.id, 'name', p.name, 'max_streams', p.max_streams,
            'is_active', json(CASE WHEN p.is_active THEN 'true' ELSE 'false' END),
            'is_default', json(CASE WHEN p.is_default THEN 'true' ELSE 'false' END),
            'current_viewers', p.current_viewers, 'search_pattern', p.search_pattern,
            'replace_pattern', p.replace_pattern, 'custom_properties', json(p.custom_properties),
            'exp_date', replace(p.exp_date, ' ', 'T') || 'Z') ORDER BY p.id) AS rows
        FROM m3u_m3uaccountprofile p GROUP BY p.m3u_account_id
    ), filters AS (
        SELECT f.m3u_account_id, json_group_array(json_object(
            'id', f.id, 'filter_type', f.filter_type, 'regex_pattern', f.regex_pattern,
            'exclude', json(CASE WHEN f.exclude THEN 'true' ELSE 'false' END),
            'order', f."order", 'custom_properties', json(f.custom_properties)) ORDER BY f.id) AS rows
        FROM m3u_m3ufilter f GROUP BY f.m3u_account_id
    ), counts AS (
        SELECT m3u_account_id, channel_group_id, count(*) AS stream_count
        FROM dispatcharr_channels_stream
        WHERE m3u_account_id IS NOT NULL AND channel_group_id IS NOT NULL
        GROUP BY m3u_account_id, channel_group_id
    ), groups AS (
        SELECT r.m3u_account_id, json_group_array(json_object(
            'channel_group_id', r.channel_group_id,
            'enabled', json(CASE WHEN r.enabled THEN 'true' ELSE 'false' END),
            'auto_channel_sync', json(CASE WHEN r.auto_channel_sync THEN 'true' ELSE 'false' END),
            'auto_sync_channel_start', r.auto_sync_channel_start,
            'auto_sync_channel_end', r.auto_sync_channel_end,
            'custom_properties', json(r.custom_properties),
            'is_stale', json(CASE WHEN r.is_stale THEN 'true' ELSE 'false' END),
            'last_seen', replace(r.last_seen, ' ', 'T') || 'Z',
            'stream_count', coalesce(c.stream_count, 0)) ORDER BY r.id) AS rows
        FROM dispatcharr_channels_channelgroupm3uaccount r LEFT JOIN counts c
            ON c.m3u_account_id=r.m3u_account_id AND c.channel_group_id=r.channel_group_id
        GROUP BY r.m3u_account_id
    )
    SELECT json_object(
        'id', a.id, 'name', a.name, 'server_url', a.server_url, 'file_path', a.file_path,
        'server_group_id', a.server_group_id, 'max_streams', a.max_streams,
        'is_active', json(CASE WHEN a.is_active THEN 'true' ELSE 'false' END),
        'created_at', replace(a.created_at, ' ', 'T') || 'Z',
        'updated_at', replace(a.updated_at, ' ', 'T') || 'Z',
        'user_agent_id', a.user_agent_id,
        'locked', json(CASE WHEN a.locked THEN 'true' ELSE 'false' END),
        'refresh_interval', a.refresh_interval, 'custom_properties', json(a.custom_properties),
        'account_type', a.account_type, 'username', a.username,
        'password', CASE WHEN ?1 THEN a.password END, 'stale_stream_days', a.stale_stream_days,
        'priority', a.priority, 'status', a.status, 'last_message', a.last_message
    ) AS raw, coalesce(p.rows,'[]') AS profiles, coalesce(f.rows,'[]') AS filters,
        coalesce(g.rows,'[]') AS groups,
        CASE WHEN ct.id IS NULL THEN ''
             ELSE ct.minute || ' ' || ct.hour || ' ' || ct.day_of_month || ' ' ||
                  ct.month_of_year || ' ' || ct.day_of_week END AS cron
    FROM m3u_m3uaccount a
    LEFT JOIN django_celery_beat_periodictask t ON t.id=a.refresh_task_id
    LEFT JOIN django_celery_beat_crontabschedule ct ON ct.id=t.crontab_id
    LEFT JOIN profiles p ON p.m3u_account_id=a.id
    LEFT JOIN filters f ON f.m3u_account_id=a.id
    LEFT JOIN groups g ON g.m3u_account_id=a.id
    ORDER BY a.id
"#;

const BASE_FIELDS: &[&str] = &[
    "id",
    "name",
    "server_url",
    "file_path",
    "server_group",
    "max_streams",
    "is_active",
    "created_at",
    "updated_at",
    "filters",
    "user_agent",
    "profiles",
    "locked",
    "channel_groups",
    "refresh_interval",
    "cron_expression",
    "custom_properties",
    "account_type",
    "username",
    "stale_stream_days",
    "priority",
    "status",
    "last_message",
    "earliest_expiration",
    "all_expirations",
];
const DERIVED_FIELDS: &[&str] = &[
    "enable_vod",
    "auto_enable_new_groups_live",
    "auto_enable_new_groups_vod",
    "auto_enable_new_groups_series",
    "hash_key",
    "exp_date",
];
const GROUP_FIELDS: &[&str] = &[
    "channel_group",
    "enabled",
    "auto_channel_sync",
    "auto_sync_channel_start",
    "auto_sync_channel_end",
    "custom_properties",
    "is_stale",
    "last_seen",
    "stream_count",
];

fn ordered(mut value: Value, fields: impl IntoIterator<Item = &'static &'static str>) -> Value {
    Value::Object(
        fields
            .into_iter()
            .map(|key| (String::from(*key), value[*key].take()))
            .collect::<Map<_, _>>(),
    )
}

fn drf_order(mut value: Value, admin: bool) -> Value {
    if let Some(groups) = value["channel_groups"].as_array_mut() {
        // The through model has m3u_account, so the serializer's optional
        // m3u_accounts.id source is absent and DRF skips that field.
        for group in groups {
            *group = ordered(group.take(), GROUP_FIELDS);
        }
    }
    ordered(
        value,
        BASE_FIELDS
            .iter()
            .chain(admin.then_some(&"password"))
            .chain(DERIVED_FIELDS),
    )
}

pub(crate) async fn read(
    connection: &mut SqliteConnection,
    admin: bool,
    response_limit: usize,
) -> Result<Vec<u8>, StoreError> {
    let mut rows = sqlx::query(QUERY).bind(admin).fetch(connection);
    let mut body = vec![b'['];
    while let Some(row) = rows.try_next().await? {
        let raw = row.try_get::<Json<Value>, _>("raw")?.0;
        let profiles = row.try_get::<Json<Vec<Value>>, _>("profiles")?.0;
        let filters = row.try_get::<Json<Vec<Value>>, _>("filters")?.0;
        let groups = row.try_get::<Json<Vec<Value>>, _>("groups")?.0;
        let value = drf_order(
            account_value(
                &raw,
                &profiles,
                filters,
                &groups,
                row.try_get("cron")?,
                admin,
            ),
            admin,
        );
        let encoded = crate::json_compat::compact_json_bytes(&value)
            .map_err(|_| StoreError::UnhandledRestFault)?;
        let mut encoded = String::from_utf8(encoded).map_err(|_| StoreError::UnhandledRestFault)?;
        if encoded.contains(['\u{2028}', '\u{2029}']) {
            encoded = encoded
                .replace('\u{2028}', "\\u2028")
                .replace('\u{2029}', "\\u2029");
        }
        let comma = body.len() > 1;
        if body
            .len()
            .saturating_add(encoded.len())
            .saturating_add(1 + usize::from(comma))
            > response_limit
        {
            return Err(StoreError::StatusBody(
                503,
                json!({"detail":"Copied SQLite M3U Account collection exceeds the read budget."}),
            ));
        }
        if comma {
            body.push(b',');
        }
        body.extend_from_slice(encoded.as_bytes());
    }
    body.push(b']');
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection;

    #[tokio::test]
    async fn copied_m3u_accounts_project_stock_order_roles_cron_and_relations() {
        let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            r#"CREATE TABLE m3u_m3uaccount(
                id INTEGER PRIMARY KEY,name TEXT,server_url TEXT,file_path TEXT,server_group_id INTEGER,
                max_streams INTEGER,is_active BOOLEAN,created_at TEXT,updated_at TEXT,user_agent_id INTEGER,
                locked BOOLEAN,refresh_interval INTEGER,custom_properties TEXT,account_type TEXT,username TEXT,
                password TEXT,stale_stream_days INTEGER,priority INTEGER,status TEXT,last_message TEXT,
                refresh_task_id INTEGER
            );
            CREATE TABLE m3u_m3uaccountprofile (
                id INTEGER PRIMARY KEY,m3u_account_id INTEGER,name TEXT,max_streams INTEGER,
                is_active BOOLEAN,is_default BOOLEAN,current_viewers INTEGER,search_pattern TEXT,
                replace_pattern TEXT,custom_properties TEXT,exp_date TEXT
            );
            CREATE TABLE m3u_m3ufilter (
                id INTEGER PRIMARY KEY,m3u_account_id INTEGER,filter_type TEXT,regex_pattern TEXT,
                exclude BOOLEAN,"order" INTEGER,custom_properties TEXT
            );
            CREATE TABLE dispatcharr_channels_stream (m3u_account_id INTEGER,channel_group_id INTEGER);
            CREATE TABLE dispatcharr_channels_channelgroupm3uaccount (
                id INTEGER PRIMARY KEY,m3u_account_id INTEGER,channel_group_id INTEGER,enabled BOOLEAN,
                auto_channel_sync BOOLEAN,auto_sync_channel_start REAL,auto_sync_channel_end REAL,
                custom_properties TEXT,is_stale BOOLEAN,last_seen TEXT
            );
            CREATE TABLE django_celery_beat_periodictask (id INTEGER PRIMARY KEY,crontab_id INTEGER);
            CREATE TABLE django_celery_beat_crontabschedule (
                id INTEGER PRIMARY KEY,minute TEXT,hour TEXT,day_of_month TEXT,month_of_year TEXT,day_of_week TEXT
            );
            INSERT INTO m3u_m3uaccount VALUES
                (2,'Owned XC','http://127.0.0.1:9',NULL,3,2,1,'2025-05-01 02:03:04.123456',NULL,
                 NULL,0,60,'{"hash_key":"name","enable_vod":true,"auto_enable_new_groups_vod":false}',
                 'XC','owned-user','private-fixture-password',10,7,'idle',NULL,11),
                (3,'Owned Empty',NULL,NULL,NULL,0,0,'2025-05-01 02:03:04',NULL,NULL,0,0,
                  NULL,'STD',NULL,NULL,0,0,'disabled',NULL,NULL);
            INSERT INTO m3u_m3uaccountprofile VALUES
                (4,2,'Default',2,1,1,0,'','',NULL,'2025-08-03 11:22:33.123400'),
                (5,2,'Early inactive',1,0,0,0,'','',NULL,'2025-08-01 11:22:33'),
                (6,2,'Active',1,1,0,0,'','','{}','2025-08-02 11:22:33');
            INSERT INTO m3u_m3ufilter VALUES
                (15,2,'group','Other',0,2,NULL),(16,2,'name','Owned',1,1,'{}');
            INSERT INTO dispatcharr_channels_channelgroupm3uaccount VALUES
                (7,2,8,1,1,2.0,5.5,'{"channel_profile_ids":["7",8]}',0,'2025-05-21 10:11:12'),
                (8,2,9,0,0,NULL,NULL,NULL,1,'2025-05-21 10:11:12.123456');
            INSERT INTO dispatcharr_channels_stream VALUES (2,8),(2,8),(3,8),(2,NULL);
            INSERT INTO django_celery_beat_periodictask VALUES (11,6);
            INSERT INTO django_celery_beat_crontabschedule VALUES (6,'7','2','*','*','1');"#,
        )
        .execute(&mut connection)
        .await
        .unwrap();

        let standard = read(&mut connection, false, 8192).await.unwrap();
        let admin = read(&mut connection, true, 8192).await.unwrap();
        let standard: Value = serde_json::from_slice(&standard).unwrap();
        let admin: Value = serde_json::from_slice(&admin).unwrap();
        let standard_keys: Vec<_> = standard[0]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let admin_keys: Vec<_> = admin[0]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            standard_keys,
            BASE_FIELDS
                .iter()
                .copied()
                .chain(DERIVED_FIELDS.iter().copied())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            admin_keys,
            BASE_FIELDS
                .iter()
                .copied()
                .chain(["password"])
                .chain(DERIVED_FIELDS.iter().copied())
                .collect::<Vec<_>>()
        );
        assert!(admin[0]["password"] == "private-fixture-password");
        assert!(admin[1]["password"] == "");
        assert_eq!(
            standard[0]["profiles"][0]["account"]["is_xtream_codes"],
            true
        );
        assert_eq!(
            standard[0]["profiles"][0]["exp_date"],
            "2025-08-03T11:22:33.123400Z"
        );
        assert_eq!(standard[0]["exp_date"], "2025-08-03T11:22:33Z");
        assert_eq!(standard[0]["earliest_expiration"], "2025-08-02T11:22:33Z");
        assert_eq!(standard[0]["all_expirations"][0]["is_active"], false);
        assert_eq!(standard[0]["filters"][0]["id"], 16);
        assert_eq!(standard[0]["cron_expression"], "7 2 * * 1");
        assert_eq!(standard[0]["enable_vod"], true);
        assert_eq!(standard[0]["auto_enable_new_groups_vod"], false);
        assert_eq!(standard[0]["channel_groups"][0]["stream_count"], 2);
        assert_eq!(standard[0]["channel_groups"][1]["stream_count"], 0);
        assert_eq!(
            standard[0]["channel_groups"][0]["custom_properties"]["channel_profile_ids"],
            json!([7, 8])
        );
        assert!(standard[0]["channel_groups"][0]
            .get("m3u_accounts")
            .is_none());
        assert_eq!(standard[1]["profiles"], json!([]));
        assert!(matches!(
            read(&mut connection, false, 16).await,
            Err(StoreError::StatusBody(503, _))
        ));
    }
}
