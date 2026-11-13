//! Source XC list relation selection, with two independent READ COMMITTED
//! statements inside the actual Source transaction.atomic() boundary.
//! Original Dispatcharr authors retain SQL/application/algorithm credit.
use crate::reader::{self, ReadLimit};
use dispatcharr_store::{Store, StoreError, User};
use futures_util::StreamExt;
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder, Row, Transaction};
use std::mem::size_of;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Family {
    Movie,
    Series,
}
impl Family {
    fn tables(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::Movie => ("vod_m3umovierelation", "vod_movie", "movie_id"),
            Self::Series => ("vod_m3useriesrelation", "vod_series", "series_id"),
        }
    }
}
/// Strings remain owned/charged until all borrowed DTO projections finish.
/// No PgRow, decoded Value tree or connection escapes this reader.
pub(crate) struct ListRow {
    pub content_id: i64,
    pub category: Option<i64>,
    pub extension: Option<String>,
    pub name: String,
    pub language: String,
    pub rating: Option<String>,
    pub description: Option<String>,
    pub genre: Option<String>,
    pub year: Option<i32>,
    pub tmdb: Option<String>,
    pub imdb: Option<String>,
    pub adult: bool,
    pub created: chrono::DateTime<chrono::Utc>,
    pub modified: chrono::DateTime<chrono::Utc>,
    pub logo: Option<i64>,
    pub properties: Option<String>,
    pub image: String,
    pub backdrop: Option<String>,
}
impl ListRow {
    pub fn heap_bytes(&self) -> Result<usize, StoreError> {
        let mut total = self
            .name
            .capacity()
            .checked_add(self.language.capacity())
            .and_then(|n| n.checked_add(self.image.capacity()))
            .ok_or_else(reader::capacity)?;
        for text in [
            &self.extension,
            &self.rating,
            &self.description,
            &self.genre,
            &self.tmdb,
            &self.imdb,
            &self.properties,
            &self.backdrop,
        ]
        .into_iter()
        .flatten()
        {
            total = total
                .checked_add(text.capacity())
                .ok_or_else(reader::capacity)?;
        }
        Ok(total)
    }
}
pub(crate) struct ListRows {
    pub rows: Vec<ListRow>,
    pub retained_bytes: usize,
}

/// The one existing active gateway grant covers collection and projection.
/// The collection phase may simultaneously own prior rows, one physical PgRow
/// and its decoded copy; vector reallocation may temporarily retain its old
/// storage too. The aggregate cap is therefore at most one fifth of the
/// caller's available existing work quote (after request/auth accounting).
pub(crate) async fn read(
    store: &Store,
    user: &User,
    family: Family,
    category: Option<i64>,
    limit: &ReadLimit,
    work_bytes: usize,
) -> Result<ListRows, StoreError> {
    // Fixed SQL/query-builder strings and connection bookkeeping also live in
    // this existing quote; reject tiny custom grants before constructing them.
    let dynamic = work_bytes
        .checked_sub(16 * 1024)
        .ok_or_else(reader::capacity)?;
    let metadata = limit.max_metadata.min(dynamic / 5);
    if metadata < size_of::<ListRow>() {
        return Err(reader::capacity());
    }
    let mut connection = store.pool().acquire().await?;
    let result = read_connection(
        &mut connection,
        user,
        family,
        category,
        limit.max_rows,
        metadata,
        work_bytes,
    )
    .await;
    if let Err(error) = Connection::ping(&mut *connection).await {
        let _closed = connection.close().await;
        return Err(error.into());
    }
    Connection::shrink_buffers(&mut *connection);
    // Retain the original request/byte scope through the actual pool return.
    // Drop alone would spawn asynchronous release after losing our owner.
    connection.return_to_pool().await;
    drop(connection);
    result
}
async fn read_connection(
    connection: &mut PgConnection,
    user: &User,
    family: Family,
    category: Option<i64>,
    maximum: usize,
    metadata: usize,
    work_bytes: usize,
) -> Result<ListRows, StoreError> {
    // No repeatable-read, merged CTE, or fresh eligibility re-entry. Source's
    // second statement deliberately observes fresh row values for winning IDs.
    let mut transaction = connection.begin().await?;
    let result = read_transaction(
        &mut transaction,
        user,
        family,
        category,
        maximum,
        (metadata, work_bytes),
    )
    .await;
    match result {
        Ok(rows) => {
            transaction.commit().await?;
            Ok(rows)
        }
        Err(error) => {
            transaction.rollback().await?;
            Err(error)
        }
    }
}
async fn read_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    user: &User,
    family: Family,
    category: Option<i64>,
    maximum: usize,
    budget: (usize, usize),
) -> Result<ListRows, StoreError> {
    let (metadata, work_bytes) = budget;
    let (relation, object, foreign) = family.tables();
    let adult = matches!(family, Family::Movie)
        && user.user_level < 10
        && crate::reader::truthy(user.custom_properties.get("hide_adult_content"));
    let mut query=QueryBuilder::<Postgres>::new(format!("SELECT DISTINCT ON(r.{foreign}) r.id::bigint AS id FROM {relation} r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id"));
    if adult {
        query.push(format!(" JOIN {object} o ON o.id=r.{foreign}"));
    }
    query.push(" WHERE a.is_active");
    if let Some(id) = category {
        query.push(" AND r.category_id=").push_bind(id);
    }
    if adult {
        query.push(" AND NOT o.is_adult");
    }
    query
        .push(format!(" ORDER BY r.{foreign},a.priority DESC,r.id LIMIT "))
        .push_bind(
            i64::try_from(maximum.checked_add(1).ok_or_else(reader::capacity)?)
                .map_err(|_| reader::capacity())?,
        );
    let mut ids = Vec::new();
    let mut failure = None;
    {
        let built = query.build();
        let mut stream = built.fetch(&mut **transaction);
        while let Some(item) = stream.next().await {
            let row = match item {
                Ok(row) => row,
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(StoreError::from(error));
                    }
                    continue;
                }
            };
            if failure.is_some() {
                continue;
            }
            let decoded = (|| -> Result<(), StoreError> {
                if ids.len() >= maximum {
                    return Err(reader::capacity());
                }
                ids.try_reserve(1).map_err(|_| reader::capacity())?;
                if ids
                    .capacity()
                    .checked_mul(size_of::<i64>())
                    .ok_or_else(reader::capacity)?
                    > metadata
                {
                    return Err(reader::capacity());
                }
                ids.push(row.try_get::<i64, _>("id")?);
                Ok(())
            })();
            if let Err(error) = decoded {
                failure = Some(error);
            }
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    if ids.is_empty() {
        return Ok(ListRows {
            rows: Vec::new(),
            retained_bytes: 0,
        });
    }
    let id_bytes = ids
        .capacity()
        .checked_mul(size_of::<i64>())
        .ok_or_else(reader::capacity)?;
    // SQLx's int8[] parameter encodes a 4-byte argument length, 20-byte array
    // header and 12 bytes per ID. Keep argument/write-buffer growth overlap
    // separate from collected rows on the SAME grant; no second reservation.
    let collection = collection_metadata(work_bytes, metadata, ids.len())?;
    if id_bytes > collection {
        return Err(reader::capacity());
    }
    let metadata = collection;
    let sql = fresh_sql(family);
    let mut result = Vec::new();
    let mut heap = 0usize;
    let mut failure = None;
    {
        // Bind only the actual winning IDs. Do not repeat is_active/category/
        // adult predicates here: Source does not recheck them after selection.
        let query = sqlx::query(&sql)
            .bind(&ids)
            .bind(i64::try_from(metadata).map_err(|_| reader::capacity())?);
        let mut stream = query.fetch(&mut **transaction);
        while let Some(item) = stream.next().await {
            let row = match item {
                Ok(row) => row,
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(StoreError::from(error));
                    }
                    continue;
                }
            };
            if failure.is_some() {
                continue;
            }
            let decoded = (|| -> Result<(), StoreError> {
                if result.len() >= maximum || row.try_get::<bool, _>("large_metadata")? {
                    return Err(reader::capacity());
                }
                let value = ListRow {
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
                    image: row
                        .try_get::<Option<String>, _>("image")?
                        .ok_or_else(reader::capacity)?,
                    backdrop: row.try_get("backdrop")?,
                };
                let next_heap = heap
                    .checked_add(value.heap_bytes()?)
                    .ok_or_else(reader::capacity)?;
                let minimum = result
                    .len()
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(size_of::<ListRow>()))
                    .and_then(|n| n.checked_add(next_heap))
                    .and_then(|n| n.checked_add(id_bytes))
                    .ok_or_else(reader::capacity)?;
                if minimum > metadata {
                    return Err(reader::capacity());
                }
                // Amortized growth avoids quadratic copying of large catalogs.
                // Old/new vector capacity and driver frame overlap are already
                // included in the five-part collection quote below.
                result.try_reserve(1).map_err(|_| reader::capacity())?;
                let retained = result
                    .capacity()
                    .checked_mul(size_of::<ListRow>())
                    .and_then(|n| n.checked_add(next_heap))
                    .and_then(|n| n.checked_add(id_bytes))
                    .ok_or_else(reader::capacity)?;
                if retained > metadata {
                    return Err(reader::capacity());
                }
                heap = next_heap;
                result.push(value);
                Ok(())
            })();
            if let Err(error) = decoded {
                failure = Some(error);
            }
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    // IDs are not retained into DTO projection, just as Source exits its
    // selection helper before constructing the per-row response dictionaries.
    drop(ids);
    let retained_bytes = result
        .capacity()
        .checked_mul(size_of::<ListRow>())
        .and_then(|n| n.checked_add(heap))
        .ok_or_else(reader::capacity)?;
    Ok(ListRows {
        rows: result,
        retained_bytes,
    })
}

fn collection_metadata(work: usize, maximum: usize, ids: usize) -> Result<usize, StoreError> {
    let binary = ids
        .checked_mul(12)
        .and_then(|n| n.checked_add(1024))
        .ok_or_else(reader::capacity)?;
    // Eight copies cover argument-vector growth and old/new Bind/write buffers;
    // 64KiB covers fixed protocol/statement metadata. The caller's owned ID
    // vector itself remains counted in each retained collection calculation.
    let input = binary
        .checked_mul(8)
        .and_then(|n| n.checked_add(64 * 1024))
        .ok_or_else(reader::capacity)?;
    let available = work
        .checked_sub(16 * 1024)
        .and_then(|n| n.checked_sub(input))
        .ok_or_else(reader::capacity)?;
    let result = maximum.min(available / 5);
    if result < size_of::<ListRow>() {
        return Err(reader::capacity());
    }
    Ok(result)
}

fn fresh_sql(family: Family) -> String {
    let (relation, object, foreign) = family.tables();
    let extension = if matches!(family, Family::Movie) {
        "r.container_extension"
    } else {
        "NULL::varchar"
    };
    let adult = if matches!(family, Family::Movie) {
        "o.is_adult"
    } else {
        "false"
    };
    let mut images = Vec::new();
    for container in ["detailed_info", "basic_data"] {
        for key in ["movie_image", "cover_big", "stream_icon", "cover"] {
            images.push(format!(
                "NULLIF(TRIM(r.custom_properties #>> '{{{container},{key}}}'),'')"
            ));
        }
    }
    let image = format!("COALESCE({},'')", images.join(","));
    let backdrop="COALESCE(NULLIF(NULLIF(r.custom_properties #> '{detailed_info,backdrop_path}','[]'::jsonb),'\"\"'::jsonb),NULLIF(NULLIF(r.custom_properties #> '{basic_data,backdrop_path}','[]'::jsonb),'\"\"'::jsonb))::text";
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
        "image",
        "backdrop",
    ];
    let bytes = fields
        .iter()
        .map(|field| format!("COALESCE(octet_length({field}),0)::bigint"))
        .collect::<Vec<_>>()
        .join("+");
    let projection = fields
        .iter()
        .map(|field| format!("CASE WHEN row_bytes<=$2 THEN {field} END AS {field}"))
        .collect::<Vec<_>>()
        .join(",");
    // The bounded outer projection retains Source's exact selected/order keys;
    // NULL for an oversized payload is capacity, never a substituted DTO field.
    format!("SELECT content_id,category_id,year,adult,created_at,modified_at,logo_id,(row_bytes>$2) AS large_metadata,{projection} FROM (SELECT source_rows.*,({bytes})::bigint AS row_bytes FROM (SELECT r.{foreign}::bigint AS content_id,r.category_id::bigint AS category_id,{extension} AS extension,o.name,o.language,o.rating,o.description,o.genre,o.year,o.tmdb_id AS tmdb,o.imdb_id AS imdb,{adult} AS adult,o.created_at,r.updated_at AS modified_at,o.logo_id::bigint AS logo_id,o.custom_properties::text AS properties,{image} AS image,{backdrop} AS backdrop FROM {relation} r JOIN {object} o ON o.id=r.{foreign} WHERE r.id=ANY($1)) AS source_rows) AS guarded_rows ORDER BY LOWER(guarded_rows.name)")
}

#[cfg(test)]
mod input_budget_tests {
    use super::*;
    #[test]
    fn array_wire_and_row_collection_share_one_work_quote() {
        assert!(collection_metadata(64 * 1024, 8 * 1024, 2).is_err());
        let work = 48 * 1024 * 1024;
        let maximum = 8 * 1024 * 1024;
        assert_eq!(collection_metadata(work, maximum, 50_000).unwrap(), maximum);
        let smaller = collection_metadata(1024 * 1024, maximum, 4096).unwrap();
        assert!(smaller < 1024 * 1024 / 5);
        assert!(collection_metadata(1024 * 1024, maximum, usize::MAX).is_err());
    }
}
