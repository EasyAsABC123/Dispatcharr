//! Direct Channel UUID playback for the feature-gated copied SQLite runtime.
//! Original Dispatcharr contributors retain application credit.
use super::CopiedSqliteRuntime;
use crate::{
    auth::source_client_ip_for_config,
    playback::{
        assemble_playback_source, playback_error, resolve_output_profile_id,
        resolve_std_provider_url, ProviderStream, ResolvedChannel,
    },
    source_input_profile, StoreError,
};
use dispatcharr_contracts::{
    BoxFuture, PlaybackError, PlaybackRequest, PlaybackResolver, PlaybackSource,
};
use serde_json::Value;
use sqlx::{Row, SqliteConnection};
use std::time::Duration;
use uuid::Uuid;

const CHANNEL: &str = r#"
SELECT c.id, COALESCE(o.logo_id,c.logo_id) AS logo,
       COALESCE(o.stream_profile_id,c.stream_profile_id,?3) AS profile,
       CASE WHEN length(CAST(COALESCE(o.name,c.name) AS BLOB))<=4096
            THEN COALESCE(o.name,c.name) END AS name
FROM dispatcharr_channels_channel c
LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id
WHERE c.uuid IN (?1,?2)
LIMIT 2
"#;
const PROFILE: &str = "SELECT id FROM core_streamprofile WHERE id=?1 AND locked AND name='Proxy'";
const DOMAIN: &str =
    "SELECT EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription WHERE enabled)
    OR EXISTS(SELECT 1 FROM plugins_pluginconfig WHERE enabled)";
macro_rules! candidate_query {
    ($from:literal, $filter:literal) => {
        concat!(r#"
SELECT s.id AS stream_id, p.id AS provider_id, p.max_streams,
       a.account_type, a.server_group_id,
       CASE WHEN length(CAST(s.stream_hash AS BLOB))<=1024 THEN s.stream_hash END AS stream_hash,
       CASE WHEN length(CAST(s.name AS BLOB))<=4096 THEN s.name END AS stream_name,
       CASE WHEN length(CAST(s.url AS BLOB))<=65536 THEN s.url END AS url,
       CASE WHEN length(CAST(p.name AS BLOB))<=4096 THEN p.name END AS provider_name,
       CASE WHEN length(CAST(p.search_pattern AS BLOB))<=65536 THEN p.search_pattern END AS search,
       CASE WHEN length(CAST(p.replace_pattern AS BLOB))<=65536 THEN p.replace_pattern END AS replace,
       CASE WHEN length(CAST(COALESCE(NULLIF(ua.user_agent,''),du.user_agent) AS BLOB))<=65536
            THEN COALESCE(NULLIF(ua.user_agent,''),du.user_agent) END AS user_agent,
       COALESCE(length(CAST(COALESCE(NULLIF(ua.user_agent,''),du.user_agent) AS BLOB)),0)>65536 AS oversized_user_agent,
       COALESCE(length(CAST(s.stream_hash AS BLOB)),0)>1024 AS oversized_hash
"#, $from, r#"
JOIN m3u_m3uaccountprofile p ON p.m3u_account_id=s.m3u_account_id
JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id
LEFT JOIN core_useragent ua ON ua.id=a.user_agent_id
LEFT JOIN core_useragent du ON du.id=?2
"#, $filter)
    };
}

const CURRENT_CANDIDATES: &str = candidate_query!(
    "FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id",
    r#"WHERE cs.channel_id=?1 AND a.is_active AND p.is_active
       AND EXISTS(SELECT 1 FROM m3u_m3uaccountprofile d
                  WHERE d.m3u_account_id=a.id AND d.is_default AND d.is_active)
ORDER BY cs."order",cs.id,p.is_default DESC,p.id
LIMIT 33"#
);
const BORROWED_CANDIDATE: &str = candidate_query!(
    "FROM dispatcharr_channels_stream s",
    "WHERE s.id=?1 AND p.id=?3 LIMIT 2"
);

pub(crate) const STATEMENTS: [&str; 5] = [
    CHANNEL,
    PROFILE,
    DOMAIN,
    CURRENT_CANDIDATES,
    BORROWED_CANDIDATE,
];

fn scope_error() -> StoreError {
    StoreError::Unsupported(
        "Copied SQLite Preview requires a locked Proxy HTTP TS standard provider",
    )
}

fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        serde_json::json!({"error":"Stream candidate capacity reached"}),
    )
}

fn setting_id(settings: &Value, key: &str) -> Option<i64> {
    settings
        .get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}

fn credential_shape(request: &PlaybackRequest) -> Result<(), PlaybackError> {
    if request.header("x-api-key").is_some()
        || request.header("authorization").is_some_and(|header| {
            header
                .split_whitespace()
                .next()
                .is_some_and(|scheme| !scheme.eq_ignore_ascii_case("Bearer"))
        })
    {
        return Err(PlaybackError::new(
            501,
            "Copied SQLite Preview requires Bearer authentication",
        ));
    }
    Ok(())
}

struct Channel {
    id: i64,
    name: String,
    logo: Option<i64>,
    profile: i64,
}
async fn channel(
    db: &mut SqliteConnection,
    uuid: Uuid,
    default_profile: Option<i64>,
) -> Result<Channel, StoreError> {
    if sqlx::query_scalar::<_, bool>(DOMAIN)
        .fetch_one(&mut *db)
        .await?
    {
        return Err(StoreError::Unsupported(
            "Copied SQLite Preview with configured live hooks is unavailable",
        ));
    }
    let mut rows = sqlx::query(CHANNEL)
        .bind(uuid.simple().to_string())
        .bind(uuid.to_string())
        .bind(default_profile)
        .fetch_all(&mut *db)
        .await?;
    let row = match rows.len() {
        0 => return Err(StoreError::NotFound),
        1 => rows.remove(0),
        _ => return Err(capacity()),
    };
    let profile = row
        .try_get::<Option<i64>, _>("profile")?
        .ok_or_else(scope_error)?;
    if sqlx::query_scalar::<_, i64>(PROFILE)
        .bind(profile)
        .fetch_optional(&mut *db)
        .await?
        .is_none()
    {
        return Err(scope_error());
    }
    let resolved = Channel {
        id: row.try_get("id")?,
        name: row
            .try_get::<Option<String>, _>("name")?
            .ok_or_else(capacity)?,
        logo: row.try_get("logo")?,
        profile,
    };
    Ok(resolved)
}

async fn candidates(
    db: &mut SqliteConnection,
    channel: i64,
    logo: Option<i64>,
    default_ua: Option<i64>,
    observed: Option<(i64, i64)>,
) -> Result<Vec<ProviderStream>, StoreError> {
    let rows = if let Some((stream, profile)) = observed {
        sqlx::query(BORROWED_CANDIDATE)
            .bind(stream)
            .bind(default_ua)
            .bind(profile)
            .fetch_all(db)
            .await?
    } else {
        sqlx::query(CURRENT_CANDIDATES)
            .bind(channel)
            .bind(default_ua)
            .fetch_all(db)
            .await?
    };
    if rows.len() > 32 {
        return Err(capacity());
    }
    if observed.is_some() && rows.len() != 1 {
        return Err(StoreError::Unsupported(
            "Copied SQLite Preview assignment is unavailable",
        ));
    }
    let mut streams = Vec::with_capacity(rows.len());
    for row in rows {
        let account_type: String = row.try_get("account_type")?;
        let max_streams: i32 = row.try_get("max_streams")?;
        if account_type != "STD"
            || (max_streams > 0 && row.try_get::<Option<i64>, _>("server_group_id")?.is_some())
        {
            return Err(scope_error());
        }
        if row.try_get::<bool, _>("oversized_user_agent")?
            || row.try_get::<bool, _>("oversized_hash")?
        {
            return Err(capacity());
        }
        let stored: Option<String> = row.try_get("url")?;
        let search: Option<String> = row.try_get("search")?;
        let replace: Option<String> = row.try_get("replace")?;
        let url = resolve_std_provider_url(
            stored.as_deref().ok_or_else(capacity)?,
            search.as_deref().ok_or_else(capacity)?,
            replace.as_deref().ok_or_else(capacity)?,
        )?
        .ok_or_else(scope_error)?;
        if url.is_empty() {
            continue;
        }
        let absolute = url::Url::parse(&url).map_err(|_| scope_error())?;
        if !matches!(absolute.scheme(), "http" | "https")
            || absolute.host_str().is_none()
            || source_input_profile::kind(&url) != source_input_profile::Kind::Ts
        {
            return Err(scope_error());
        }
        let name = row
            .try_get::<Option<String>, _>("stream_name")?
            .ok_or_else(capacity)?;
        let provider = row
            .try_get::<Option<String>, _>("provider_name")?
            .ok_or_else(capacity)?;
        let user_agent = row
            .try_get::<Option<String>, _>("user_agent")?
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "Dispatcharr/0.33.0".to_owned());
        let mut metadata = vec![
            ("channel_id".to_owned(), channel.to_string()),
            ("stream_name".to_owned(), name),
            ("m3u_profile_name".to_owned(), provider),
            ("stream_profile".to_owned(), "Proxy".to_owned()),
        ];
        if let Some(hash) = row.try_get::<Option<String>, _>("stream_hash")? {
            metadata.push(("source_stream_hash".to_owned(), hash));
        }
        if observed.is_some() {
            metadata.push(("source_assignment_required".to_owned(), "true".to_owned()));
        }
        if let Some(logo) = logo {
            metadata.push(("logo_id".to_owned(), logo.to_string()));
        }
        streams.push(ProviderStream {
            input_command: None,
            stream_id: row.try_get("stream_id")?,
            provider_profile_id: row.try_get("provider_id")?,
            provider_max_streams: u32::try_from(max_streams).map_err(|_| scope_error())?,
            credential_pool: None,
            url,
            user_agent,
            metadata,
        });
    }
    Ok(streams)
}

impl CopiedSqliteRuntime {
    pub async fn preview_engine_settings(&self) -> Result<Value, &'static str> {
        let reads = self
            .allocate_read()
            .await
            .map_err(|_| "Copied SQLite Preview configuration unavailable")?;
        reads
            .preview_setting("proxy_settings")
            .await
            .map_err(|_| "Copied SQLite Preview configuration unavailable")
    }

    async fn resolve_preview(
        &self,
        request: PlaybackRequest,
    ) -> Result<PlaybackSource, PlaybackError> {
        credential_shape(&request)?;
        let reads = self.allocate_read().await.map_err(playback_error)?;
        let subject = reads
            .preview_viewer(&request)
            .await
            .map_err(playback_error)?;
        let uuid = Uuid::parse_str(&request.channel_id)
            .map_err(|_| playback_error(StoreError::NotFound))?;
        if request.parameter("output_format") != Some("mpegts")
            || !request
                .parameter("skip_redirect")
                .is_some_and(|value| value.eq_ignore_ascii_case("true"))
            || resolve_output_profile_id(&request, Some(subject.user())).is_some()
        {
            return Err(PlaybackError::new(
                501,
                "Copied SQLite Preview supports direct unprofiled MPEG-TS only",
            ));
        }
        let settings = reads
            .preview_setting("stream_settings")
            .await
            .map_err(playback_error)?;
        let proxy = reads
            .preview_setting("proxy_settings")
            .await
            .map_err(playback_error)?;
        let limits = reads
            .preview_setting("user_limit_settings")
            .await
            .map_err(playback_error)?;
        let default_profile = setting_id(&settings, "default_stream_profile");
        let initial = reads
            .preview_sql(move |db| Box::pin(channel(db, uuid, default_profile)))
            .await
            .map_err(playback_error)?;
        let identity = uuid.to_string();
        let observed = self
            .cache
            .raw_source_assignment(initial.id, &identity)
            .await
            .map_err(playback_error)?;
        let default_ua = setting_id(&settings, "default_user_agent");
        let (channel, streams) = reads
            .preview_sql(move |db| {
                Box::pin(async move {
                    let mut snapshot = sqlx::Connection::begin(db).await?;
                    let resolved = channel(&mut snapshot, uuid, default_profile).await?;
                    if resolved.id != initial.id {
                        return Err(StoreError::NotFound);
                    }
                    let streams = candidates(
                        &mut snapshot,
                        resolved.id,
                        resolved.logo,
                        default_ua,
                        observed,
                    )
                    .await?;
                    snapshot.commit().await?;
                    Ok((resolved, streams))
                })
            })
            .await
            .map_err(playback_error)?;
        let channel = ResolvedChannel {
            redirect: false,
            numeric_channel_id: channel.id,
            has_assigned_streams: !streams.is_empty(),
            redirect_error: None,
            id: identity,
            name: channel.name,
            profile_id: channel.profile,
            streams,
            shutdown_delay: Duration::from_secs(
                proxy
                    .get("channel_shutdown_delay")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            ),
            startup_behind: Duration::from_secs(
                proxy
                    .get("new_client_behind_seconds")
                    .and_then(Value::as_u64)
                    .unwrap_or(5),
            ),
        };
        assemble_playback_source(
            &request,
            Some(subject.user()),
            source_client_ip_for_config(&self.config, &request),
            "mpegts",
            None,
            &limits,
            channel,
        )
    }
}

impl PlaybackResolver for CopiedSqliteRuntime {
    fn resolve(
        &self,
        request: PlaybackRequest,
    ) -> BoxFuture<'_, Result<PlaybackSource, PlaybackError>> {
        Box::pin(self.resolve_preview(request))
    }

    fn authenticate_optional(
        &self,
        request: PlaybackRequest,
    ) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async move {
            credential_shape(&request)?;
            self.allocate_read()
                .await
                .map_err(playback_error)?
                .authenticate_bearer(&request)
                .await
                .map_err(playback_error)?;
            Ok(())
        })
    }

    fn authorize_admin(
        &self,
        _request: PlaybackRequest,
    ) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async {
            Err(PlaybackError::new(
                501,
                "Copied SQLite media controls are unavailable",
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection;

    #[tokio::test]
    async fn indexed_preview_keeps_link_order_effective_profile_and_borrowed_identity() {
        let mut db = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        let fixture = r#"
CREATE TABLE dispatcharr_channels_channel(id INTEGER PRIMARY KEY, uuid TEXT UNIQUE, name TEXT, logo_id INTEGER, stream_profile_id INTEGER);
CREATE TABLE dispatcharr_channels_channeloverride(channel_id INTEGER UNIQUE,name TEXT,logo_id INTEGER,stream_profile_id INTEGER);
CREATE TABLE dispatcharr_channels_channelstream(id INTEGER PRIMARY KEY,"order" INTEGER,channel_id INTEGER,stream_id INTEGER,UNIQUE(channel_id,stream_id));
CREATE TABLE dispatcharr_channels_stream(id INTEGER PRIMARY KEY,name TEXT,url TEXT,stream_hash TEXT,m3u_account_id INTEGER);
CREATE TABLE core_streamprofile(id INTEGER PRIMARY KEY,name TEXT,locked BOOLEAN);
CREATE TABLE dispatcharr_connect_eventsubscription(enabled BOOLEAN);
CREATE TABLE plugins_pluginconfig(enabled BOOLEAN);
CREATE TABLE core_useragent(id INTEGER PRIMARY KEY,user_agent TEXT);
CREATE TABLE m3u_m3uaccountprofile(id INTEGER PRIMARY KEY,m3u_account_id INTEGER,name TEXT,is_default BOOLEAN,max_streams INTEGER,is_active BOOLEAN,search_pattern TEXT,replace_pattern TEXT);
CREATE TABLE m3u_m3uaccount(id INTEGER PRIMARY KEY,account_type TEXT,server_group_id INTEGER,is_active BOOLEAN,user_agent_id INTEGER);
INSERT INTO dispatcharr_channels_channel VALUES(1,'11bbd73ac44f4fea9ebb03a3720f6813','base',NULL,8);
INSERT INTO dispatcharr_channels_channeloverride VALUES(1,'override',25,9);
INSERT INTO core_streamprofile VALUES(8,'Redirect',1),(9,'Proxy',1);
INSERT INTO core_useragent VALUES(1,'account agent'),(2,'default agent');
INSERT INTO m3u_m3uaccount VALUES(5,'STD',NULL,1,1),(6,'STD',NULL,1,NULL),(7,'STD',NULL,0,NULL);
INSERT INTO m3u_m3uaccountprofile VALUES
 (10,5,'secondary',0,2,1,'^(.*)$','$1'),(11,5,'default',1,1,1,'',''),
 (12,6,'fallback',1,0,1,'',''),(13,7,'inactive account',1,0,1,'',''),
 (14,5,'inactive profile',0,0,0,'','');
INSERT INTO dispatcharr_channels_stream VALUES
 (15,'first','http://fixture.invalid/program.ts','hash-a',5),
 (16,'second','http://fixture.invalid/other.ts','hash-b',6),
 (17,'inactive','http://fixture.invalid/inactive.ts','hash-c',7);
INSERT INTO dispatcharr_channels_channelstream VALUES(1,5,1,16),(2,2,1,15),(3,1,1,17);
"#;
        sqlx::raw_sql(fixture).execute(&mut db).await.unwrap();
        for query in STATEMENTS {
            sqlx::Executor::prepare(&mut db, query).await.unwrap();
        }
        let uuid = Uuid::parse_str("11bbd73a-c44f-4fea-9ebb-03a3720f6813").unwrap();
        let resolved = channel(&mut db, uuid, None).await.unwrap();
        assert_eq!(
            (
                resolved.id,
                &*resolved.name,
                resolved.logo,
                resolved.profile
            ),
            (1, "override", Some(25), 9)
        );
        let choices = candidates(&mut db, resolved.id, resolved.logo, Some(2), None)
            .await
            .unwrap();
        assert_eq!(
            choices
                .iter()
                .map(|item| (
                    item.stream_id,
                    item.provider_profile_id,
                    item.provider_max_streams,
                    item.user_agent.as_str()
                ))
                .collect::<Vec<_>>(),
            [
                (15, 11, 1, "account agent"),
                (15, 10, 2, "account agent"),
                (16, 12, 0, "default agent")
            ]
        );
        let borrowed = candidates(&mut db, resolved.id, None, Some(2), Some((17, 13)))
            .await
            .unwrap();
        assert_eq!(borrowed.len(), 1);
        assert_eq!(
            (borrowed[0].stream_id, borrowed[0].provider_profile_id),
            (17, 13)
        );
        assert!(borrowed[0]
            .metadata
            .iter()
            .any(|(key, value)| key == "source_assignment_required" && value == "true"));
        assert!(
            candidates(&mut db, resolved.id, None, Some(2), Some((15, 13)))
                .await
                .is_err()
        );
        assert!(matches!(
            channel(&mut db, Uuid::nil(), None).await,
            Err(StoreError::NotFound)
        ));

        sqlx::query("UPDATE dispatcharr_channels_stream SET url='http://fixture.invalid/playlist.m3u8' WHERE id=15")
            .execute(&mut db).await.unwrap();
        assert!(matches!(
            candidates(&mut db, resolved.id, None, None, None).await,
            Err(StoreError::Unsupported(_))
        ));
        sqlx::query("INSERT INTO dispatcharr_connect_eventsubscription VALUES (TRUE)")
            .execute(&mut db)
            .await
            .unwrap();
        assert!(matches!(
            channel(&mut db, uuid, None).await,
            Err(StoreError::Unsupported(_))
        ));
        db.close().await.unwrap();
    }
}
