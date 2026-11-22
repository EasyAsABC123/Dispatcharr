//! Read the Core catalogs used by React's copied-data bootstrap.
//! Original Dispatcharr authors retain all application credit.

use crate::StoreError;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};
use sqlx::{sqlite::SqliteRow, Row, SqliteConnection};

#[derive(Clone, Copy)]
pub(crate) enum Catalog {
    UserAgents,
    StreamProfiles,
    OutputProfiles,
    ServerGroups,
}

impl Catalog {
    const USER_AGENTS: &str =
        "SELECT id,name,user_agent,description,is_active,created_at,updated_at FROM core_useragent";
    const STREAM_PROFILES: &str =
        "SELECT id,name,command,parameters,is_active,user_agent_id,locked FROM core_streamprofile";
    const OUTPUT_PROFILES: &str =
        "SELECT id,name,command,parameters,is_active,locked FROM core_outputprofile";

    const SERVER_GROUPS: &str = "SELECT id,name FROM m3u_servergroup ORDER BY id";

    pub(crate) const STATEMENTS: [&str; 4] = [
        Self::USER_AGENTS,
        Self::STREAM_PROFILES,
        Self::OUTPUT_PROFILES,
        Self::SERVER_GROUPS,
    ];

    pub(crate) async fn read(self, connection: &mut SqliteConnection) -> Result<Value, StoreError> {
        let query = match self {
            Self::UserAgents => Self::USER_AGENTS,
            Self::StreamProfiles => Self::STREAM_PROFILES,
            Self::OutputProfiles => Self::OUTPUT_PROFILES,
            Self::ServerGroups => Self::SERVER_GROUPS,
        };
        let rows = sqlx::query(query).fetch_all(connection).await?;
        Ok(Value::Array(
            rows.into_iter()
                .map(|row| self.serialize(row))
                .collect::<Result<_, _>>()?,
        ))
    }

    fn serialize(self, row: SqliteRow) -> Result<Value, sqlx::Error> {
        let id: i64 = row.try_get("id")?;
        let name: String = row.try_get("name")?;
        if matches!(self, Self::ServerGroups) {
            return Ok(json!({"id": id, "name": name}));
        }
        let is_active: bool = row.try_get("is_active")?;
        if matches!(self, Self::UserAgents) {
            return Ok(json!({
                "id": id,
                "name": name,
                "user_agent": row.try_get::<String, _>("user_agent")?,
                "description": row.try_get::<String, _>("description")?,
                "is_active": is_active,
                "created_at": source_date(row.try_get("created_at")?),
                "updated_at": source_date(row.try_get("updated_at")?),
            }));
        }
        let mut profile = json!({
            "id": id,
            "name": name,
            "command": row.try_get::<String, _>("command")?,
            "parameters": row.try_get::<String, _>("parameters")?,
            "is_active": is_active,
        });
        if matches!(self, Self::StreamProfiles) {
            profile["user_agent"] = row.try_get::<Option<i64>, _>("user_agent_id")?.into();
        }
        profile["locked"] = row.try_get::<bool, _>("locked")?.into();
        Ok(profile)
    }
}

fn source_date(date: DateTime<Utc>) -> String {
    let precision = if date.timestamp_subsec_micros() == 0 {
        SecondsFormat::Secs
    } else {
        SecondsFormat::Micros
    };
    date.to_rfc3339_opts(precision, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection;

    #[tokio::test]
    async fn copied_catalog_keeps_original_field_order_and_typed_values() {
        let mut database = SqliteConnection::connect(":memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE core_useragent (id INTEGER PRIMARY KEY, name TEXT, user_agent TEXT,
               description TEXT, is_active BOOLEAN, created_at DATETIME, updated_at DATETIME);
             CREATE TABLE core_streamprofile (id INTEGER PRIMARY KEY, name TEXT, command TEXT,
               parameters TEXT, is_active BOOLEAN, user_agent_id INTEGER, locked BOOLEAN);
             CREATE TABLE core_outputprofile (id INTEGER PRIMARY KEY, name TEXT, command TEXT,
               parameters TEXT, is_active BOOLEAN, locked BOOLEAN);
             INSERT INTO core_useragent VALUES
               (4,'Test agent','Owned/1.0','',0,'2025-01-02 03:04:05','2025-01-02 03:04:05.120000');
             INSERT INTO core_streamprofile VALUES
               (1,'Proxy','','',1,NULL,1),(3,'Alternate','copy','--sample',0,4,0);
             INSERT INTO core_outputprofile VALUES (2,'Output','copy','',1,1);
             CREATE TABLE m3u_servergroup (id INTEGER PRIMARY KEY, name TEXT);
             INSERT INTO m3u_servergroup VALUES (8,'East'),(3,'West');",
        )
        .execute(&mut database)
        .await
        .unwrap();

        let user_agents = Catalog::UserAgents.read(&mut database).await.unwrap();
        assert_eq!(
            user_agents.to_string(),
            r#"[{"id":4,"name":"Test agent","user_agent":"Owned/1.0","description":"","is_active":false,"created_at":"2025-01-02T03:04:05Z","updated_at":"2025-01-02T03:04:05.120000Z"}]"#
        );
        let stream_profiles = Catalog::StreamProfiles.read(&mut database).await.unwrap();
        assert_eq!(
            stream_profiles.to_string(),
            r#"[{"id":1,"name":"Proxy","command":"","parameters":"","is_active":true,"user_agent":null,"locked":true},{"id":3,"name":"Alternate","command":"copy","parameters":"--sample","is_active":false,"user_agent":4,"locked":false}]"#
        );
        let output_profiles = Catalog::OutputProfiles.read(&mut database).await.unwrap();
        assert_eq!(
            output_profiles.to_string(),
            r#"[{"id":2,"name":"Output","command":"copy","parameters":"","is_active":true,"locked":true}]"#
        );
        let groups = Catalog::ServerGroups.read(&mut database).await.unwrap();
        assert_eq!(
            groups.to_string(),
            r#"[{"id":3,"name":"West"},{"id":8,"name":"East"}]"#
        );
    }
}
