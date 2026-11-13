//! Source Series-info episode snapshots: two independent autocommit statements.
//! This is not the list helper's transaction.atomic boundary. Original
//! Dispatcharr authors retain model/query/application/algorithm credit.
use crate::{
    reader::{self, ReadLimit},
    vod_json,
};
use dispatcharr_store::{Store, StoreError};
use futures_util::StreamExt;
use sqlx::{Connection, Row};
use std::mem::size_of;

pub(crate) struct Episode {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub rating: Option<String>,
    pub imdb: Option<String>,
    pub air_date: Option<chrono::NaiveDate>,
    pub duration: Option<i32>,
    pub season: Option<i32>,
    pub number: Option<i32>,
    pub properties: Option<String>,
    pub created: chrono::DateTime<chrono::Utc>,
}
impl Episode {
    pub(crate) fn heap_bytes(&self) -> Result<usize, StoreError> {
        let mut bytes = self.name.capacity();
        for value in [
            &self.description,
            &self.rating,
            &self.imdb,
            &self.properties,
        ]
        .into_iter()
        .flatten()
        {
            bytes = bytes
                .checked_add(value.capacity())
                .ok_or_else(reader::capacity)?;
        }
        Ok(bytes)
    }
}
pub(crate) struct Relation {
    pub episode: i64,
    pub extension: Option<String>,
    pub properties: Option<String>,
    pub created: chrono::DateTime<chrono::Utc>,
}
impl Relation {
    pub(crate) fn heap_bytes(&self) -> Result<usize, StoreError> {
        self.extension
            .as_ref()
            .map_or(0, String::capacity)
            .checked_add(self.properties.as_ref().map_or(0, String::capacity))
            .ok_or_else(reader::capacity)
    }
}
pub(crate) struct Rows {
    pub episodes: Vec<Episode>,
    /// Winning relations sorted by episode ID. Binary search does not change
    /// episode order or add a Source tie-breaker.
    pub relations: Vec<Relation>,
    pub retained_bytes: usize,
}
impl Rows {
    pub fn relation(&self, id: i64) -> Option<&Relation> {
        self.relations
            .binary_search_by_key(&id, |r| r.episode)
            .ok()
            .map(|i| &self.relations[i])
    }
}
fn properties_valid(text: Option<&str>) -> Result<(), StoreError> {
    if let Some(text) = text {
        let raw = crate::vod_span::parse(text).map_err(|_| {
            StoreError::Unsupported("XC episode stored JSON grammar is unsupported")
        })?;
        vod_json::validate_source_integer_limits(raw).map_err(|_| {
            StoreError::Unsupported("XC episode stored number grammar is unsupported")
        })?;
    }
    Ok(())
}
fn input_quote(ids: usize) -> Result<usize, StoreError> {
    ids.checked_mul(12)
        .and_then(|n| n.checked_add(1024))
        .and_then(|n| n.checked_mul(8))
        .and_then(|n| n.checked_add(64 * 1024))
        .ok_or_else(reader::capacity)
}
pub(crate) async fn read(
    store: &Store,
    series: i64,
    limits: &ReadLimit,
    work: usize,
) -> Result<Rows, StoreError> {
    let metadata = limits
        .max_metadata
        .min(work.checked_sub(16 * 1024).ok_or_else(reader::capacity)? / 5);
    if metadata < size_of::<Episode>() {
        return Err(reader::capacity());
    }
    let mut connection = store.pool().acquire().await?;
    let result = read_connection(&mut connection, series, limits.max_rows, metadata, work).await;
    if let Err(error) = Connection::flush(&mut *connection).await {
        let _closed = connection.close().await;
        return Err(error.into());
    }
    Connection::shrink_buffers(&mut *connection);
    drop(connection);
    result
}
async fn read_connection(
    connection: &mut sqlx::PgConnection,
    series: i64,
    maximum: usize,
    metadata: usize,
    work: usize,
) -> Result<Rows, StoreError> {
    let mut episodes = Vec::new();
    let mut ids = Vec::new();
    let mut heap = 0usize;
    let mut failure = None;
    {
        let text = episode_sql();
        let query = sqlx::query(&text)
            .bind(series)
            .bind(i64::try_from(metadata).map_err(|_| reader::capacity())?)
            .bind(
                i64::try_from(maximum.checked_add(1).ok_or_else(reader::capacity)?)
                    .map_err(|_| reader::capacity())?,
            );
        let mut stream = query.fetch(&mut *connection);
        while let Some(item) = stream.next().await {
            let row = match item {
                Ok(row) => row,
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(error.into());
                    }
                    continue;
                }
            };
            if failure.is_some() {
                continue;
            }
            let value = (|| -> Result<(), StoreError> {
                if episodes.len() >= maximum || row.try_get::<bool, _>("large_metadata")? {
                    return Err(reader::capacity());
                }
                let value = Episode {
                    id: row.try_get("id")?,
                    name: row
                        .try_get::<Option<String>, _>("name")?
                        .ok_or_else(reader::capacity)?,
                    description: row.try_get("description")?,
                    rating: row.try_get("rating")?,
                    imdb: row.try_get("imdb")?,
                    air_date: row.try_get("air_date")?,
                    duration: row.try_get("duration_secs")?,
                    season: row.try_get("season_number")?,
                    number: row.try_get("episode_number")?,
                    properties: row.try_get("properties")?,
                    created: row.try_get("created_at")?,
                };
                properties_valid(value.properties.as_deref())?;
                let next_heap = heap
                    .checked_add(value.heap_bytes()?)
                    .ok_or_else(reader::capacity)?;
                let minimum = episodes
                    .len()
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(size_of::<Episode>() + size_of::<i64>()))
                    .and_then(|n| n.checked_add(next_heap))
                    .ok_or_else(reader::capacity)?;
                if minimum > metadata {
                    return Err(reader::capacity());
                }
                episodes.try_reserve(1).map_err(|_| reader::capacity())?;
                ids.try_reserve(1).map_err(|_| reader::capacity())?;
                let actual = episodes
                    .capacity()
                    .checked_mul(size_of::<Episode>())
                    .and_then(|n| n.checked_add(ids.capacity().checked_mul(size_of::<i64>())?))
                    .and_then(|n| n.checked_add(next_heap))
                    .ok_or_else(reader::capacity)?;
                if actual > metadata {
                    return Err(reader::capacity());
                }
                ids.push(value.id);
                episodes.push(value);
                heap = next_heap;
                Ok(())
            })();
            if let Err(error) = value {
                failure = Some(error);
            }
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    let episode_bytes = episodes
        .capacity()
        .checked_mul(size_of::<Episode>())
        .and_then(|n| n.checked_add(heap))
        .ok_or_else(reader::capacity)?;
    if ids.is_empty() {
        return Ok(Rows {
            episodes,
            relations: Vec::new(),
            retained_bytes: episode_bytes,
        });
    }
    let cap = limits_for_relations(work, metadata, ids.len())?;
    let ids_bytes = ids
        .capacity()
        .checked_mul(size_of::<i64>())
        .ok_or_else(reader::capacity)?;
    if episode_bytes
        .checked_add(ids_bytes)
        .ok_or_else(reader::capacity)?
        > cap
    {
        return Err(reader::capacity());
    }
    let mut relations = Vec::new();
    let mut relation_heap = 0usize;
    let mut seen = 0usize;
    let mut previous = None;
    {
        let query = sqlx::query(RELATION_SQL)
            .bind(&ids)
            .bind(i64::try_from(cap).map_err(|_| reader::capacity())?)
            .bind(
                i64::try_from(maximum.checked_add(1).ok_or_else(reader::capacity)?)
                    .map_err(|_| reader::capacity())?,
            );
        let mut stream = query.fetch(&mut *connection);
        while let Some(item) = stream.next().await {
            let row = match item {
                Ok(row) => row,
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(error.into());
                    }
                    continue;
                }
            };
            if failure.is_some() {
                continue;
            }
            let value = (|| -> Result<(), StoreError> {
                seen += 1;
                if seen > maximum || row.try_get::<bool, _>("large_metadata")? {
                    return Err(reader::capacity());
                }
                let episode: i64 = row.try_get("episode_id")?;
                let properties: Option<String> = row.try_get("properties")?;
                // Source's ORM materializes JSON for even lower-priority rows;
                // do not hide malformed integer grammar behind DISTINCT ON.
                properties_valid(properties.as_deref())?;
                if previous == Some(episode) {
                    return Ok(());
                }
                previous = Some(episode);
                let value = Relation {
                    episode,
                    extension: row.try_get("container_extension")?,
                    properties,
                    created: row.try_get("created_at")?,
                };
                let next_heap = relation_heap
                    .checked_add(value.heap_bytes()?)
                    .ok_or_else(reader::capacity)?;
                let minimum = relations
                    .len()
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(size_of::<Relation>()))
                    .and_then(|n| n.checked_add(next_heap))
                    .and_then(|n| n.checked_add(episode_bytes))
                    .and_then(|n| n.checked_add(ids_bytes))
                    .ok_or_else(reader::capacity)?;
                if minimum > cap {
                    return Err(reader::capacity());
                }
                relations.try_reserve(1).map_err(|_| reader::capacity())?;
                let actual = relations
                    .capacity()
                    .checked_mul(size_of::<Relation>())
                    .and_then(|n| n.checked_add(next_heap))
                    .and_then(|n| n.checked_add(episode_bytes))
                    .and_then(|n| n.checked_add(ids_bytes))
                    .ok_or_else(reader::capacity)?;
                if actual > cap {
                    return Err(reader::capacity());
                }
                relations.push(value);
                relation_heap = next_heap;
                Ok(())
            })();
            if let Err(error) = value {
                failure = Some(error);
            }
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    drop(ids);
    let retained_bytes = relations
        .capacity()
        .checked_mul(size_of::<Relation>())
        .and_then(|n| n.checked_add(relation_heap))
        .and_then(|n| n.checked_add(episode_bytes))
        .ok_or_else(reader::capacity)?;
    Ok(Rows {
        episodes,
        relations,
        retained_bytes,
    })
}
fn limits_for_relations(work: usize, maximum: usize, ids: usize) -> Result<usize, StoreError> {
    let available = work
        .checked_sub(16 * 1024)
        .and_then(|n| n.checked_sub(input_quote(ids).ok()?))
        .ok_or_else(reader::capacity)?;
    Ok(maximum.min(available / 5))
}
fn episode_sql() -> String {
    let fields = ["name", "description", "rating", "imdb", "properties"];
    let bytes = fields
        .iter()
        .map(|s| format!("COALESCE(octet_length({s}),0)::bigint"))
        .collect::<Vec<_>>()
        .join("+");
    let bound = fields
        .iter()
        .map(|s| format!("CASE WHEN row_bytes<=$2 THEN {s} END AS {s}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("SELECT id,air_date,duration_secs,season_number,episode_number,created_at,(row_bytes>$2) AS large_metadata,{bound} FROM (SELECT selected.*,({bytes}) AS row_bytes FROM (SELECT id::bigint AS id,name,description,rating,imdb_id AS imdb,custom_properties::text AS properties,air_date,duration_secs,season_number,episode_number,created_at FROM (SELECT DISTINCT e.* FROM vod_episode e JOIN vod_m3uepisoderelation r ON r.episode_id=e.id JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE e.series_id=$1 AND a.is_active) AS source_episodes) AS selected) AS guarded ORDER BY season_number,episode_number LIMIT $3")
}
const RELATION_SQL:&str="SELECT episode_id,created_at,(row_bytes>$2) AS large_metadata,CASE WHEN row_bytes<=$2 THEN container_extension END AS container_extension,CASE WHEN row_bytes<=$2 THEN properties END AS properties FROM (SELECT r.episode_id::bigint AS episode_id,r.created_at,r.container_extension,r.custom_properties::text AS properties,a.priority,r.id,(COALESCE(octet_length(r.container_extension),0)::bigint+COALESCE(octet_length(r.custom_properties::text),0)::bigint) AS row_bytes FROM vod_m3uepisoderelation r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.episode_id=ANY($1) AND a.is_active) AS selected ORDER BY episode_id,priority DESC,id LIMIT $3";

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn staged_reader_keeps_distinct_episode_rows_and_all_ordered_relations() {
        let episodes = episode_sql();
        assert!(episodes.contains("SELECT DISTINCT e.*"));
        assert!(episodes.contains("ORDER BY season_number,episode_number LIMIT $3"));
        assert!(!episodes.contains("episode_number,id"));
        assert!(RELATION_SQL.contains("ORDER BY episode_id,priority DESC,id LIMIT $3"));
        assert!(!RELATION_SQL.contains("DISTINCT ON"));
        let _ = read;
    }
    #[test]
    fn relation_input_scratch_is_charged_before_the_second_statement() {
        assert!(limits_for_relations(16 * 1024, 1024, 1).is_err());
        assert!(input_quote(usize::MAX).is_err());
        assert!(limits_for_relations(1024 * 1024, 1024, 16).unwrap() <= 1024);
    }
}
