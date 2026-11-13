//! Cached XC info relation snapshot. Source uses one ordered select_related
//! query here, outside the list helper's two-statement atomic boundary.
//! Original Dispatcharr authors retain application/model/SQL credit.
use crate::{
    reader::{self, ReadLimit},
    vod_info_refresh::{self, Decision, Flags},
    vod_json,
    vod_priority::{Family, ListRow},
    vod_properties::{Field, Properties},
};
use dispatcharr_store::{Store, StoreError, User};
use sqlx::{Connection, Row};
use std::mem::size_of;

pub(crate) struct InfoRow {
    pub object: ListRow,
    pub relation_created: chrono::DateTime<chrono::Utc>,
    pub relation_modified: chrono::DateTime<chrono::Utc>,
    pub relation_properties: Option<String>,
    pub last_refresh: Option<chrono::DateTime<chrono::Utc>>,
    pub retained_bytes: usize,
}
impl InfoRow {
    /// No network refresh is performed by this reader. The caller must not
    /// reinterpret Required/CheckFailed as a fresh cached success.
    pub fn refresh_decision(
        &self,
        family: Family,
        mut now: impl FnMut() -> i64,
    ) -> Result<Decision, vod_json::Error> {
        let last = self.last_refresh.map(|at| at.timestamp_micros());
        // Series computes timestamp eligibility before accessing property
        // flags. Capture that single clock read before parsing/getting them;
        // Movie retains its flags/missing-timestamp short circuit instead.
        let series_now = match family {
            Family::Series if last.is_some() => Some(now()),
            _ => None,
        };
        let props = Properties::parse(self.relation_properties.as_deref())?;
        let flags = match props.get(Field::DetailedFetched) {
            Ok(detailed) => Flags::Mapping {
                detailed: vod_json::truthy(detailed)?,
                episodes: match family {
                    Family::Movie => false,
                    Family::Series => vod_json::truthy(props.get(Field::EpisodesFetched)?)?,
                },
            },
            Err(vod_json::Error::SourceObject) => Flags::TruthyNonMapping,
            Err(error) => return Err(error),
        };
        Ok(match family {
            Family::Movie => vod_info_refresh::movie(flags, last, &mut now),
            Family::Series => vod_info_refresh::series(flags, last, || {
                series_now.expect("Series reads the clock only with a timestamp")
            }),
        })
    }
}

pub(crate) async fn read(
    store: &Store,
    user: &User,
    family: Family,
    id: i64,
    limits: &ReadLimit,
    available_work_bytes: usize,
) -> Result<Option<InfoRow>, StoreError> {
    let dynamic = available_work_bytes
        .checked_sub(16 * 1024)
        .ok_or_else(reader::capacity)?;
    let metadata = limits.max_metadata.min(dynamic / 4);
    if metadata < size_of::<InfoRow>() {
        return Err(reader::capacity());
    }
    let adult = matches!(family, Family::Movie)
        && user.user_level < 10
        && reader::truthy(user.custom_properties.get("hide_adult_content"));
    let sql = sql(family, adult);
    let mut connection = store.pool().acquire().await?;
    let result = sqlx::query(&sql)
        .bind(id)
        .bind(i64::try_from(metadata).map_err(|_| reader::capacity())?)
        .fetch_optional(&mut *connection)
        .await;
    // The same supervised outer request/byte owners remain held until real
    // ReadyForQuery/close, including backend errors and observer/caller loss.
    if let Err(error) = Connection::flush(&mut *connection).await {
        let _closed = connection.close().await;
        return Err(error.into());
    }
    Connection::shrink_buffers(&mut *connection);
    drop(connection);
    let Some(row) = result? else {
        return Ok(None);
    };
    if row.try_get::<bool, _>("large_metadata")? {
        return Err(reader::capacity());
    }
    let object = ListRow {
        content_id: row.try_get("content_id")?,
        category: row.try_get("category_id")?,
        extension: row.try_get("extension")?,
        name: row
            .try_get::<Option<String>, _>("name")?
            .ok_or_else(reader::capacity)?,
        language: row
            .try_get::<Option<String>, _>("language")?
            .ok_or_else(reader::capacity)?,
        rating: row.try_get("rating")?,
        description: row.try_get("description")?,
        genre: row.try_get("genre")?,
        year: row.try_get("year")?,
        tmdb: row.try_get("tmdb")?,
        imdb: row.try_get("imdb")?,
        adult: row.try_get("adult")?,
        created: row.try_get("created_at")?,
        modified: row.try_get("modified_at")?,
        logo: row.try_get("logo_id")?,
        properties: row.try_get("properties")?,
        // Info artwork uses the full Source Python relation shape, including
        // info.info/root candidates; do not substitute list SQL's lean shape.
        image: String::new(),
        backdrop: None,
    };
    let relation_properties: Option<String> = row.try_get("relation_properties")?;
    let retained_bytes = object
        .heap_bytes()?
        .checked_add(relation_properties.as_ref().map_or(0, String::capacity))
        .and_then(|n| n.checked_add(size_of::<InfoRow>()))
        .ok_or_else(reader::capacity)?;
    if retained_bytes > metadata {
        return Err(reader::capacity());
    }
    Ok(Some(InfoRow {
        object,
        relation_created: row.try_get("relation_created")?,
        relation_modified: row.try_get("modified_at")?,
        relation_properties,
        last_refresh: row.try_get("last_refresh")?,
        retained_bytes,
    }))
}

fn sql(family: Family, adult_filter: bool) -> String {
    let (relation, object, foreign, extension, adult, last) = match family {
        Family::Movie => (
            "vod_m3umovierelation",
            "vod_movie",
            "movie_id",
            "r.container_extension",
            "o.is_adult",
            "r.last_advanced_refresh",
        ),
        Family::Series => (
            "vod_m3useriesrelation",
            "vod_series",
            "series_id",
            "NULL::varchar",
            "false",
            "r.last_episode_refresh",
        ),
    };
    let fields = [
        "extension",
        "name",
        "language",
        "rating",
        "description",
        "genre",
        "tmdb",
        "imdb",
        "properties",
        "relation_properties",
    ];
    let bytes = fields
        .iter()
        .map(|field| format!("COALESCE(octet_length({field}),0)::bigint"))
        .collect::<Vec<_>>()
        .join("+");
    let bounded = fields
        .iter()
        .map(|field| format!("CASE WHEN row_bytes<=$2 THEN {field} END AS {field}"))
        .collect::<Vec<_>>()
        .join(",");
    let visibility = if adult_filter {
        " AND NOT o.is_adult"
    } else {
        ""
    };
    format!("SELECT content_id,category_id,year,adult,created_at,modified_at,relation_created,last_refresh,logo_id,(row_bytes>$2) AS large_metadata,{bounded} FROM (SELECT selected.*,({bytes})::bigint AS row_bytes FROM (SELECT r.{foreign}::bigint AS content_id,r.category_id::bigint AS category_id,{extension} AS extension,o.name,o.language,o.rating,o.description,o.genre,o.year,o.tmdb_id AS tmdb,o.imdb_id AS imdb,{adult} AS adult,o.created_at,r.updated_at AS modified_at,r.created_at AS relation_created,{last} AS last_refresh,o.logo_id::bigint AS logo_id,o.custom_properties::text AS properties,r.custom_properties::text AS relation_properties FROM {relation} r JOIN {object} o ON o.id=r.{foreign} JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.{foreign}=$1 AND a.is_active{visibility} ORDER BY a.priority DESC,r.id LIMIT 1) AS selected) AS bounded")
}

#[cfg(test)]
mod tests {
    use super::*;
    fn row(properties: &str, last: Option<i64>) -> InfoRow {
        let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap();
        InfoRow {
            object: ListRow {
                content_id: 1,
                category: None,
                extension: None,
                name: String::new(),
                language: String::new(),
                rating: None,
                description: None,
                genre: None,
                year: None,
                tmdb: None,
                imdb: None,
                adult: false,
                created: epoch,
                modified: epoch,
                logo: None,
                properties: None,
                image: String::new(),
                backdrop: None,
            },
            relation_created: epoch,
            relation_modified: epoch,
            relation_properties: Some(properties.into()),
            last_refresh: last.map(|n| chrono::DateTime::from_timestamp(n, 0).unwrap()),
            retained_bytes: 0,
        }
    }
    #[test]
    fn series_adapter_reads_clock_before_property_decode_failure() {
        let row = row("[", Some(0));
        let mut calls = 0;
        assert!(row
            .refresh_decision(Family::Series, || {
                calls += 1;
                0
            })
            .is_err());
        assert_eq!(
            calls, 1,
            "property access cannot move before Source Series clock read"
        );
        let mut calls = 0;
        assert!(row
            .refresh_decision(Family::Movie, || {
                calls += 1;
                0
            })
            .is_err());
        assert_eq!(
            calls, 0,
            "Movie property failure remains before its clock read"
        );
    }
    #[test]
    fn missing_series_timestamp_and_incomplete_movie_flags_do_not_read_clock() {
        assert_eq!(
            row("{}", None)
                .refresh_decision(Family::Series, || panic!("missing timestamp short circuit"))
                .unwrap(),
            Decision::RefreshRequired
        );
        assert_eq!(
            row("{}", Some(0))
                .refresh_decision(Family::Movie, || panic!("Movie flags short circuit"))
                .unwrap(),
            Decision::RefreshRequired
        );
        assert_eq!(
            row(r#"{"detailed_fetched":true}"#, Some(0))
                .refresh_decision(Family::Movie, || 0)
                .unwrap(),
            Decision::Fresh
        );
    }
    #[test]
    fn info_selection_is_single_statement_and_action_specific() {
        let movie = sql(Family::Movie, true);
        let series = sql(Family::Series, false);
        assert!(movie
            .contains("AND a.is_active AND NOT o.is_adult ORDER BY a.priority DESC,r.id LIMIT 1"));
        assert!(!series.contains("NOT o.is_adult"));
        assert!(movie.contains("r.last_advanced_refresh AS last_refresh"));
        assert!(series.contains("r.last_episode_refresh AS last_refresh"));
        for text in [&movie, &series] {
            assert!(
                text.len() < 8192,
                "fixed SQL fits its existing query scratch quote"
            );
            assert!(!text.contains(';'));
            assert!(!text.contains("WITH "));
            assert!(text.contains("CASE WHEN row_bytes<=$2 THEN relation_properties END"));
            assert!(text.contains("(row_bytes>$2) AS large_metadata"));
        }
        let _ = read;
        let _ = |row: &InfoRow| row.refresh_decision(Family::Movie, || 0);
    }
}
