//! Source-derived ordinary VOD DTOs used by the bounded read component.
//! Projection order/nullability is derived from original serializers.py and
//! actual source-vod-rest-1.json, independently of existing XC catalog DTOs.
//! Original Dispatcharr authors retain full application/algorithm credit.
use chrono::{DateTime, NaiveDate, Utc};
use md5::{Digest, Md5};
use serde_json::{Map, Value};
use super::vod_api_projection::truthy;

/// The future HTTP adapter must supply the shared tested trusted-forwarding
/// origin. Provider/series-episodes actions use None because Source creates
/// their nested serializers without request context. Never infer from a host.
pub(crate) struct LogoContext<'a> { pub absolute_origin: Option<&'a str> }
impl LogoContext<'_> {
    fn cache_url(&self, id: i64, url: &str) -> String {
        let digest = format!("{:x}", Md5::digest(url.as_bytes()));
        let path = format!("/api/vod/vodlogos/{id}/cache/?v={}", &digest[..8]);
        match self.absolute_origin { Some(origin) => format!("{origin}{path}"), None => path }
    }
}
/// Counts and item names include all associated content, not only visible or
/// active-provider rows. Source takes ten movies AND ten series, up to20 total.
pub(crate) struct LogoRow {
    pub id: i64, pub name: String, pub url: String,
    pub movie_count: i64, pub series_count: i64, pub is_used: bool,
    pub movie_names: Vec<String>, pub series_names: Vec<String>,
}
impl LogoRow {
    fn project(&self, context: &LogoContext<'_>, unified: bool) -> Value {
        let mut fields = Map::new();
        fields.insert("id".into(), self.id.into());
        fields.insert("name".into(), self.name.clone().into());
        fields.insert("url".into(), self.url.clone().into());
        fields.insert("cache_url".into(), context.cache_url(self.id, &self.url).into());
        fields.insert("movie_count".into(), if unified {0} else {self.movie_count}.into());
        fields.insert("series_count".into(), if unified {0} else {self.series_count}.into());
        fields.insert("is_used".into(), (unified || self.is_used).into());
        if !unified {
            let names: Vec<Value> = self.movie_names.iter().take(10).map(|name| format!("Movie: {name}").into())
                .chain(self.series_names.iter().take(10).map(|name| format!("Series: {name}").into())).collect();
            fields.insert("item_names".into(), names.into());
        }
        fields.into()
    }
}
/// PostgreSQL stores microseconds. Django TIME_ZONE='UTC' DRF isoformat keeps
/// six fractional digits when nonzero; its UTC suffix is Z. No clock rewriting.
fn datetime(value: DateTime<Utc>) -> Value {
    let head = value.format("%Y-%m-%dT%H:%M:%S").to_string();
    let micros = value.timestamp_subsec_micros();
    if micros == 0 { format!("{head}Z").into() } else { format!("{head}.{micros:06}Z").into() }
}
fn optional(value: &Option<String>) -> Value { value.clone().map(Value::String).unwrap_or(Value::Null) }
fn integer(value: Option<i64>) -> Value { value.map(Value::from).unwrap_or(Value::Null) }

pub(crate) struct CommonRow {
    pub id: i64, pub uuid: String, pub name: String,
    pub description: Option<String>, pub year: Option<i64>,
    pub rating: Option<String>, pub genre: Option<String>,
    pub tmdb_id: Option<String>, pub imdb_id: Option<String>, pub language: String,
    pub custom_properties: Value, pub created_at: DateTime<Utc>, pub updated_at: DateTime<Utc>,
}
pub(crate) struct MovieRow { pub common: CommonRow, pub logo: Option<LogoRow>, pub duration_secs: Option<i64>, pub is_adult: bool }
pub(crate) struct SeriesRow { pub common: CommonRow, pub logo: Option<LogoRow>, pub episode_count: i64 }

impl MovieRow {
    pub fn project(&self, context: &LogoContext<'_>) -> Value {
        let row = &self.common;
        // Insertion order is DRF ModelSerializer declared fields then model
        // fields, as witnessed by actual original response bytes.
        serde_json::json!({"id":row.id,"logo":self.logo.as_ref().map(|v|v.project(context,false)),
            "custom_properties":row.custom_properties,"uuid":row.uuid,"name":row.name,
            "description":optional(&row.description),"year":integer(row.year),"rating":optional(&row.rating),
            "genre":optional(&row.genre),"duration_secs":integer(self.duration_secs),
            "tmdb_id":optional(&row.tmdb_id),"imdb_id":optional(&row.imdb_id),"language":row.language,
            "is_adult":self.is_adult,"created_at":datetime(row.created_at),"updated_at":datetime(row.updated_at)})
    }
}
impl SeriesRow {
    pub fn project(&self, context: &LogoContext<'_>) -> Value {
        let row = &self.common;
        serde_json::json!({"id":row.id,"logo":self.logo.as_ref().map(|v|v.project(context,false)),
            "episode_count":self.episode_count,"custom_properties":row.custom_properties,
            "uuid":row.uuid,"name":row.name,"description":optional(&row.description),"year":integer(row.year),
            "rating":optional(&row.rating),"genre":optional(&row.genre),"tmdb_id":optional(&row.tmdb_id),
            "imdb_id":optional(&row.imdb_id),"language":row.language,
            "created_at":datetime(row.created_at),"updated_at":datetime(row.updated_at)})
    }
}

pub(crate) struct EpisodeRow {
    pub id: i64, pub uuid: String, pub name: String, pub series: SeriesRow,
    pub description: Option<String>, pub air_date: Option<NaiveDate>, pub rating: Option<String>,
    pub duration_secs: Option<i64>, pub season_number: Option<i64>, pub episode_number: Option<i64>,
    pub tmdb_id: Option<String>, pub imdb_id: Option<String>, pub custom_properties: Value,
    pub created_at: DateTime<Utc>, pub updated_at: DateTime<Utc>,
}
impl EpisodeRow {
    pub fn project(&self, context: &LogoContext<'_>) -> Value {
        serde_json::json!({"id":self.id,"series":self.series.project(context),
            "custom_properties":self.custom_properties,"uuid":self.uuid,"name":self.name,
            "description":optional(&self.description),"air_date":self.air_date.map(|v|v.format("%Y-%m-%d").to_string()),
            "rating":optional(&self.rating),"duration_secs":integer(self.duration_secs),
            "season_number":integer(self.season_number),"episode_number":integer(self.episode_number),
            "tmdb_id":optional(&self.tmdb_id),"imdb_id":optional(&self.imdb_id),
            "created_at":datetime(self.created_at),"updated_at":datetime(self.updated_at)})
    }
}

pub(crate) struct CategoryAccountRow { pub category: i64, pub m3u_account: i64, pub enabled: bool, pub custom_properties: Value }
pub(crate) struct CategoryRow { pub id: i64, pub name: String, pub category_type: String, pub accounts: Vec<CategoryAccountRow> }
impl CategoryRow {
    pub fn project(&self) -> Value {
        let display = match self.category_type.as_str() { "movie"=>"Movie", "series"=>"Series", other=>other };
        let accounts: Vec<Value> = self.accounts.iter().map(|v|serde_json::json!({"category":v.category,
            "m3u_account":v.m3u_account,"enabled":v.enabled,"custom_properties":v.custom_properties})).collect();
        serde_json::json!({"id":self.id,"name":self.name,"category_type":self.category_type,
            "category_type_display":display,"m3u_accounts":accounts})
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UnifiedError { InvalidRating, NonFiniteRating }
/// A distinct shape: rating is float, null strings become empty, falsy JSON
/// becomes an object, duration differs, and the slim logo has no item_names.
/// The ordinary list's adult predicate must NOT be applied by this projector.
pub(crate) fn unified(row: &CommonRow, logo: Option<&LogoRow>, duration: Option<i64>, kind: &str,
    context: &LogoContext<'_>) -> Result<Value, UnifiedError> {
    let rating = match row.rating.as_deref().filter(|v| !v.is_empty()) {
        Some(value) => dispatcharr_ingest_core::source_float_literal(value).ok_or(UnifiedError::InvalidRating)?,
        None => 0.0,
    };
    // Do not silently serialize nonfinite values as JSON null. The Source view
    // and renderer fault boundary still needs additional dedicated cases.
    let rating = serde_json::Number::from_f64(rating).ok_or(UnifiedError::NonFiniteRating)?;
    Ok(serde_json::json!({"id":row.id,"uuid":row.uuid,"name":row.name,
        "description":row.description.as_deref().unwrap_or(""),"year":integer(row.year),"rating":rating,
        "genre":row.genre.as_deref().unwrap_or(""),"duration":integer(duration),
        "created_at":datetime(row.created_at),"updated_at":datetime(row.updated_at),
        "custom_properties":if truthy(&row.custom_properties){row.custom_properties.clone()}else{serde_json::json!({})},
        "logo":logo.map(|v|v.project(context,true)),"content_type":kind}))
}
