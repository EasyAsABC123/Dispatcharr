//! Read the copied guide collections used by the original React bootstrap.
//! Original Dispatcharr authors and contributors retain application credit.

use crate::{accounts::datetime, StoreError};
use axum::http::HeaderValue;
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{sqlite::SqliteRow, types::Json, Row, SqliteConnection};

const SOURCES: &str = "
    SELECT e.id,e.name,e.source_type,e.url,e.username,e.is_active,e.file_path,
           e.refresh_interval,e.priority,e.status,e.last_message,e.created_at,e.updated_at,
           e.custom_properties,
           CASE WHEN ct.id IS NULL THEN ''
                ELSE ct.minute || ' ' || ct.hour || ' ' || ct.day_of_month || ' ' ||
                     ct.month_of_year || ' ' || ct.day_of_week END AS cron,
           (SELECT count(*) FROM epg_epgdata d WHERE d.epg_source_id=e.id) AS data_count,
           EXISTS(SELECT 1 FROM dispatcharr_channels_channel c
                  JOIN epg_epgdata d ON d.id=c.epg_data_id WHERE d.epg_source_id=e.id) AS has_channels
    FROM epg_epgsource e
    LEFT JOIN django_celery_beat_periodictask t ON t.id=e.refresh_task_id
    LEFT JOIN django_celery_beat_crontabschedule ct ON ct.id=t.crontab_id
    ORDER BY e.id";

const DATA: &str = "
    SELECT id,tvg_id,name,icon_url,epg_source_id FROM epg_epgdata ORDER BY id";

pub(crate) const STATEMENTS: [&str; 2] = [SOURCES, DATA];

#[derive(Clone, Copy)]
pub(crate) enum Collection {
    Sources,
    Data,
}

impl Collection {
    pub(crate) fn allow(self) -> HeaderValue {
        match self {
            Self::Sources => crate::channels::COLLECTION_ALLOW,
            Self::Data => HeaderValue::from_static("GET, HEAD, OPTIONS"),
        }
    }

    pub(crate) async fn read(
        self,
        connection: &mut SqliteConnection,
        response_limit: usize,
    ) -> Result<Vec<u8>, StoreError> {
        let query = match self {
            Self::Sources => SOURCES,
            Self::Data => DATA,
        };
        let mut rows = sqlx::query(query).fetch(connection);
        let mut body = vec![b'['];
        while let Some(row) = rows.try_next().await? {
            let value = self.project(&row)?;
            let encoded = crate::json_compat::compact_json_bytes(&value)
                .map_err(|_| StoreError::UnhandledRestFault)?;
            let mut encoded =
                String::from_utf8(encoded).map_err(|_| StoreError::UnhandledRestFault)?;
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
                    json!({"detail": "Copied SQLite EPG collection exceeds the read budget."}),
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

    fn project(self, row: &SqliteRow) -> Result<Value, sqlx::Error> {
        let id: i64 = row.try_get("id")?;
        let name: String = row.try_get("name")?;
        let optional_text = |key| row.try_get::<Option<String>, _>(key);
        Ok(match self {
            Self::Sources => {
                let custom = row
                    .try_get::<Option<Json<Value>>, _>("custom_properties")?
                    .map(|json| json.0);
                json!({
                    "id": id,
                    "name": name,
                    "source_type": row.try_get::<String, _>("source_type")?,
                    "url": optional_text("url")?,
                    "username": optional_text("username")?,
                    "is_active": row.try_get::<bool, _>("is_active")?,
                    "file_path": optional_text("file_path")?,
                    "refresh_interval": row.try_get::<i64, _>("refresh_interval")?,
                    "cron_expression": row.try_get::<String, _>("cron")?,
                    "priority": row.try_get::<i64, _>("priority")?,
                    "status": row.try_get::<String, _>("status")?,
                    "last_message": optional_text("last_message")?,
                    "created_at": datetime(row.try_get("created_at")?),
                    "updated_at": datetime(row.try_get("updated_at")?),
                    "custom_properties": custom,
                    "epg_data_count": row.try_get::<i64, _>("data_count")?,
                    "has_channels": row.try_get::<bool, _>("has_channels")?,
                })
            }
            Self::Data => json!({
                "id": id,
                "tvg_id": optional_text("tvg_id")?,
                "name": name,
                "icon_url": optional_text("icon_url")?,
                "epg_source": row.try_get::<Option<i64>, _>("epg_source_id")?,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection;

    #[tokio::test]
    async fn copied_epg_collections_keep_stock_shape_without_selecting_passwords() {
        let mut database = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE epg_epgsource (id INTEGER PRIMARY KEY,name TEXT,source_type TEXT,url TEXT,
             username TEXT,password TEXT,is_active BOOLEAN,file_path TEXT,refresh_interval INTEGER,
             priority INTEGER,status TEXT,last_message TEXT,created_at DATETIME,updated_at DATETIME,
             custom_properties TEXT,refresh_task_id INTEGER);
            CREATE TABLE epg_epgdata (id INTEGER PRIMARY KEY,tvg_id TEXT,name TEXT,icon_url TEXT,epg_source_id INTEGER);
            CREATE TABLE dispatcharr_channels_channel (epg_data_id INTEGER);
            CREATE TABLE django_celery_beat_periodictask (id INTEGER PRIMARY KEY, crontab_id INTEGER);
            CREATE TABLE django_celery_beat_crontabschedule (id INTEGER PRIMARY KEY, minute TEXT, hour TEXT,
             day_of_month TEXT, month_of_year TEXT, day_of_week TEXT);
            INSERT INTO django_celery_beat_crontabschedule VALUES (6,'7','2','*','*','1');
            INSERT INTO django_celery_beat_periodictask VALUES (11,6);
            INSERT INTO epg_epgsource VALUES
                (2,'Owned Guide','xmltv',NULL,NULL,'only-fixture',1,NULL,30,7,'idle',NULL,
                 '2025-05-01 02:03:04.123456',NULL,'{\"empty\":[],\"limit\":1e-8}',11),
                (3,'Other','dummy',NULL,NULL,NULL,0,NULL,0,0,'disabled',NULL,
                 '2025-05-01 02:03:04',NULL,NULL,NULL);
            INSERT INTO epg_epgdata VALUES (15,'guide.1','First',NULL,2),(17,NULL,'Other record',NULL,3),
                (19,NULL,'No source',NULL,NULL);
            INSERT INTO dispatcharr_channels_channel VALUES (15);",
        )
        .execute(&mut database)
        .await
        .unwrap();

        for statement in STATEMENTS {
            use sqlx::Executor;
            database.prepare(statement).await.unwrap();
        }
        let source = Collection::Sources.read(&mut database, 4096).await.unwrap();
        let raw = std::str::from_utf8(&source).unwrap();
        assert!(raw.starts_with(
            "[{\"id\":2,\"name\":\"Owned Guide\",\"source_type\":\"xmltv\",\"url\":null,\"username\":null,\
             \"is_active\":true,\"file_path\":null,\"refresh_interval\":30,\"cron_expression\":\"7 2 * * 1\""
        ));
        assert!(!raw.contains("password") && !raw.contains("only-fixture"));
        let values: Value = serde_json::from_slice(&source).unwrap();
        assert_eq!(values[0]["created_at"], "2025-05-01T02:03:04.123456Z");
        assert_eq!(values[0]["updated_at"], Value::Null);
        assert_eq!(values[0]["custom_properties"]["limit"].as_f64(), Some(1e-8));
        assert!(raw.contains("\"limit\":1e-08"));
        assert_eq!(values[0]["epg_data_count"], 1);
        assert_eq!(values[0]["has_channels"], true);
        assert_eq!(values[1]["cron_expression"], "");
        assert_eq!(values[1]["has_channels"], false);
        assert_eq!(values[1]["custom_properties"], Value::Null);

        let data = Collection::Data.read(&mut database, 4096).await.unwrap();
        assert_eq!(
            data,
            br#"[{"id":15,"tvg_id":"guide.1","name":"First","icon_url":null,"epg_source":2},{"id":17,"tvg_id":null,"name":"Other record","icon_url":null,"epg_source":3},{"id":19,"tvg_id":null,"name":"No source","icon_url":null,"epg_source":null}]"#
        );
        assert!(matches!(
            Collection::Data.read(&mut database, 32).await,
            Err(StoreError::StatusBody(503, _))
        ));
        assert_eq!(Collection::Sources.allow(), "GET, POST, HEAD, OPTIONS");
        assert_eq!(Collection::Data.allow(), "GET, HEAD, OPTIONS");
    }
}
