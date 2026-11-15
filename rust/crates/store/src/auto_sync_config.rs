//! Source group/account configuration for normal auto-channel synchronization.
//! Original Dispatcharr authors retain all application/algorithm credit.
//! Draft component; production orchestration is not yet registered.
use crate::{task_sql_session::TaskSqlSession, StoreError};
use serde_json::{Map, Value};
use sqlx::Row;
pub(crate) enum Error {
    Store(StoreError),
    Caught(String),
    Unsupported,
}
impl From<StoreError> for Error {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}
impl From<sqlx::Error> for Error {
    fn from(value: sqlx::Error) -> Self {
        Self::Store(value.into())
    }
}
pub(crate) struct Groups {
    session: TaskSqlSession,
    table: String,
}
pub(crate) struct Group {
    pub ordinal: i64,
    pub id: i64,
    pub name: String,
    pub start: f64,
    pub end: Option<f64>,
    pub properties: Map<String, Value>,
}
fn bounded_properties(raw: Option<String>) -> Result<Map<String, Value>, Error> {
    use crate::provider_json_admission::{admit, Error as AdmissionError, Limits};
    let Some(raw) = raw else {
        return Ok(Map::new());
    };
    let limits = || Limits {
        work_bytes: 512 * 1024,
        depth: 64,
        string_bytes: 65536,
        string_copies: 2,
    };
    admit(raw.as_bytes(), limits()).map_err(|_| Error::Unsupported)?;
    let mut value: Value = serde_json::from_str(&raw).map_err(|_| Error::Unsupported)?;
    crate::json_compat::normalize_input_numbers(&mut value).map_err(|_| Error::Unsupported)?;
    Ok(match value {
        Value::Object(value) => value,
        Value::String(text) => match admit(text.as_bytes(), limits()) {
            Ok(()) => match serde_json::from_str::<Value>(&text) {
                Ok(mut value) => {
                    crate::json_compat::normalize_input_numbers(&mut value)
                        .map_err(|_| Error::Unsupported)?;
                    match value {
                        Value::Object(value) => value,
                        _ => Map::new(),
                    }
                }
                _ => Map::new(),
            },
            Err(AdmissionError::Syntax(_)) => Map::new(),
            Err(AdmissionError::Capacity) => return Err(Error::Unsupported),
        },
        _ => Map::new(),
    })
}
pub(crate) async fn account_properties(
    session: &TaskSqlSession,
    account: i64,
) -> Result<Map<String, Value>, Error> {
    let row=session.run(move|c|Box::pin(async move{sqlx::query("SELECT CASE WHEN COALESCE(octet_length(custom_properties::text),0)<=65536 THEN custom_properties::text END AS properties,COALESCE(octet_length(custom_properties::text),0)<=65536 AS fits FROM m3u_m3uaccount WHERE id=$1").bind(account).fetch_optional(c).await})).await??;
    let Some(row) = row else {
        return Err(Error::Caught(
            "M3UAccount matching query does not exist.".into(),
        ));
    };
    if !row.try_get::<bool, _>("fits")? {
        return Err(Error::Unsupported);
    }
    bounded_properties(row.try_get("properties")?)
}
impl Groups {
    pub(crate) async fn capture(session: TaskSqlSession, account: i64) -> Result<Self, Error> {
        let table = format!("native_auto_groups_{}", uuid::Uuid::new_v4().simple());
        let name = table.clone();
        session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("CREATE TEMP TABLE {name}(ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,id bigint,name text,start double precision,finish double precision,properties text,fits boolean) ON COMMIT PRESERVE ROWS")).execute(&mut*c).await?;
            // Source relation Meta has no ordering. Preserve the actual query
            // iteration as the group snapshot, without allocating all models.
            sqlx::query(&format!("INSERT INTO {name}(id,name,start,finish,properties,fits) SELECT r.channel_group_id::bigint,g.name,r.auto_sync_channel_start,r.auto_sync_channel_end,CASE WHEN COALESCE(octet_length(r.custom_properties::text),0)<=65536 THEN r.custom_properties::text END,COALESCE(octet_length(r.custom_properties::text),0)<=65536 FROM dispatcharr_channels_channelgroupm3uaccount r JOIN dispatcharr_channels_channelgroup g ON g.id=r.channel_group_id WHERE r.m3u_account_id=$1 AND r.enabled AND r.auto_channel_sync")).bind(account).execute(c).await?;
            Ok::<_,StoreError>(())
        })).await??;
        Ok(Self { session, table })
    }
    pub(crate) async fn next(&self, after: i64) -> Result<Option<Group>, Error> {
        let table = self.table.clone();
        let raw=self.session.run(move|c|Box::pin(async move{
            sqlx::query(&format!("SELECT ordinal,id,name,start,finish,properties,fits FROM {table} WHERE ordinal>$1 ORDER BY ordinal LIMIT 1")).bind(after).fetch_optional(c).await
        })).await??;
        let Some(row) = raw else { return Ok(None) };
        if !row.try_get::<bool, _>("fits")? {
            return Err(Error::Unsupported);
        }
        let start = row
            .try_get::<Option<f64>, _>("start")?
            .filter(|n| *n != 0.0)
            .unwrap_or(1.0);
        Ok(Some(Group {
            ordinal: row.try_get("ordinal")?,
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            start,
            end: row.try_get("finish")?,
            properties: bounded_properties(row.try_get("properties")?)?,
        }))
    }
    pub(crate) async fn close(self) -> Result<(), Error> {
        let table = self.table;
        self.session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query(&format!("DROP TABLE {table}"))
                        .execute(c)
                        .await?;
                    Ok::<_, StoreError>(())
                })
            })
            .await??;
        Ok(())
    }
}
/// Same pinned IntegerField preparation grammar already used by normal channel
/// maintenance: bool/int/finite float and Source Unicode integer spelling.
fn model_pk(value: &Value) -> Result<Option<i64>, Error> {
    match value {
        Value::Null => return Ok(None),
        Value::Bool(value) => return Ok(Some(i64::from(*value))),
        Value::Number(value) => {
            if let Some(id) = value.as_i64() {
                return Ok(Some(id));
            }
            let raw = value.to_string();
            if !raw.contains(['.', 'e', 'E']) {
                return Ok(None);
            }
            let number = value
                .as_f64()
                .filter(|n| n.is_finite())
                .ok_or(Error::Unsupported)?;
            return Ok((number >= i64::MIN as f64 && number < i64::MAX as f64)
                .then(|| number.trunc() as i64));
        }
        Value::String(value) => {
            if let Some(text) = dispatcharr_ingest_core::source_integer_literal(value) {
                return Ok(text.parse().ok());
            }
        }
        _ => {}
    }
    let text = crate::python_action::collection_text(value, 8192).ok_or(Error::Unsupported)?;
    Err(Error::Caught(format!(
        "Field 'id' expected a number but got {text}."
    )))
}
impl Group {
    pub(crate) fn optional_fk(&self, key: &str) -> Result<Option<i64>, Error> {
        match self
            .properties
            .get(key)
            .filter(|v| crate::xc_live_provider::truthy(v))
        {
            None => Ok(None),
            Some(value) => model_pk(value),
        }
    }
    pub(crate) fn profiles(&self) -> Result<Option<Vec<i64>>, Error> {
        match self.properties.get("channel_profile_ids") {
            Some(Value::Array(ids)) if !ids.is_empty() => {
                let mut converted = Vec::with_capacity(ids.len());
                for id in ids {
                    // int(None) is caught for the WHOLE Source comprehension;
                    // a valid huge integer merely cannot match the actual PK.
                    if id.is_null() {
                        return Ok(Some(Vec::new()));
                    }
                    match model_pk(id) {
                        Ok(Some(id)) => converted.push(id),
                        Ok(None) => {}
                        Err(Error::Caught(_)) => return Ok(Some(Vec::new())),
                        Err(error) => return Err(error),
                    }
                }
                Ok(Some(converted))
            }
            _ => Ok(None),
        }
    }
    pub(crate) fn skip_memberships(&self) -> bool {
        self.properties.get("skip_channel_profile_memberships") == Some(&Value::Bool(true))
    }
}
