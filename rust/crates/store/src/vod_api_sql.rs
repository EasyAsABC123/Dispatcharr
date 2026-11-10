//! UNREGISTERED/UNCOMPILED ordinary VOD SQL draft. Original authors retain
//! credit. No endpoint, fixture, SQL, ownership or performance gate has run.
//! Parsed filter/search inputs are not a substitute for the still-open DRF
//! raw-query grammar gate. Unsupported kinds fail explicitly, never empty.
use super::{vod_api_dto::*,vod_api_owned_read::ReadError,vod_api_pagination::Page,
    vod_api_projection::Visibility};
use serde_json::Value;
use sqlx::{postgres::{PgArguments,PgRow}, query::Query,Postgres,Row};
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) enum Resource {Movie,Series,Episode}
impl Resource {
    fn table(self)->&'static str {match self {Self::Movie=>"vod_movie",Self::Series=>"vod_series",Self::Episode=>"vod_episode"}}
    fn relation(self)->&'static str {match self {Self::Movie=>"vod_m3umovierelation",Self::Series=>"vod_m3useriesrelation",Self::Episode=>"vod_m3uepisoderelation"}}
    fn foreign(self)->&'static str {match self {Self::Movie=>"movie_id",Self::Series=>"series_id",Self::Episode=>"episode_id"}}
    pub fn enabled(self,visibility:&Visibility)->bool {match self {Self::Movie=>visibility.movies,_=>visibility.series}}
}
#[derive(Clone)]
pub(crate) enum Filter {NameContains(String),Account(i64),Category{name:String,kind:Option<String>},
    Year(i64),YearMin(i64),YearMax(i64),Adult(bool),ParentSeries(i64),Season(i64),Episode(i64),SearchTerms(Vec<String>)}
#[derive(Clone,Copy)]
pub(crate) enum OrderField {Name,Year,CreatedAt,Season,Episode}
pub(crate) struct Order {pub field:OrderField,pub descending:bool}
pub(crate) struct Selection {pub resource:Resource,pub detail:Option<i64>,pub filters:Vec<Filter>,pub order:Vec<Order>}
#[derive(Clone)]
enum Bind {Integer(i64),String(String),Boolean(bool)}
pub(crate) struct Statement {sql:String,binds:Vec<Bind>,page:Option<String>}
impl Statement {
    pub fn query(&self)->Query<'_,Postgres,PgArguments> {
        let mut query=sqlx::query(&self.sql).persistent(false);
        for bind in &self.binds {query=match bind {Bind::Integer(v)=>query.bind(*v),Bind::String(v)=>query.bind(v.as_str()),Bind::Boolean(v)=>query.bind(*v)};}
        query
    }
    pub fn explain(&self)->Self {Self {sql:format!("EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) {}",self.sql),binds:self.binds.clone(),page:None}}
    pub fn page_probe(&self)->Option<Self> {Some(Self {sql:self.page.clone()?,binds:self.binds.clone(),page:None})}
    fn bind(&mut self,value:Bind)->String {self.binds.push(value);format!("${}",self.binds.len())}
}
fn contains(value:&str)->String {
    // Django PostgreSQL prep_for_like_query escapes slash, %, and _. Unified
    // raw LIKE must NOT use this helper. Case mapping remains in PostgreSQL.
    format!("%{}%",value.replace('\\',"\\\\").replace('%',"\\%").replace('_',"\\_"))
}
impl Selection {
    fn predicates(&self,visibility:&Visibility,row_bytes:usize)->Result<(Statement,String),ReadError> {
        if self.filters.len()>32||self.order.len()>16||row_bytes>i64::MAX as usize{return Err(ReadError::Capacity);}
        let mut statement=Statement {sql:String::new(),binds:vec![Bind::Integer(row_bytes as i64)],page:None};
        let mut predicates=vec!["$1::bigint>=0".to_owned(),"active_account.is_active=true".to_owned()];
        if !self.resource.enabled(visibility){predicates.push("false".into());}
        if self.resource==Resource::Movie&&visibility.hide_adult_movies{predicates.push("c.is_adult=false".into());}
        if let Some(id)=self.detail {let arg=statement.bind(Bind::Integer(id));predicates.push(format!("c.id={arg}"));}
        for filter in &self.filters {
            let condition=match filter {
                Filter::NameContains(value)=>{let arg=statement.bind(Bind::String(contains(value)));format!("UPPER(c.name::text) LIKE UPPER({arg})")},
                Filter::Account(id)=>{let arg=statement.bind(Bind::Integer(*id));format!("EXISTS(SELECT 1 FROM {} filter_account WHERE filter_account.{}=c.id AND filter_account.m3u_account_id={arg})",self.resource.relation(),self.resource.foreign())},
                Filter::Category{name,kind} if self.resource!=Resource::Episode=>{
                    let name=statement.bind(Bind::String(name.clone()));
                    let kind=if let Some(kind)=kind {let arg=statement.bind(Bind::String(kind.clone()));format!(" AND category.category_type={arg}")}else{String::new()};
                    format!("EXISTS(SELECT 1 FROM {} filter_category JOIN vod_vodcategory category ON category.id=filter_category.category_id WHERE filter_category.{}=c.id AND category.name={name}{kind})",self.resource.relation(),self.resource.foreign())
                },
                Filter::Year(value)|Filter::YearMin(value)|Filter::YearMax(value) if self.resource!=Resource::Episode=>{
                    let arg=statement.bind(Bind::Integer(*value));let op=match filter {Filter::YearMin(_)=>">=",Filter::YearMax(_)=>"<=",_=>"="};format!("c.year{op}{arg}")
                },
                Filter::Adult(value) if self.resource==Resource::Movie=>{let arg=statement.bind(Bind::Boolean(*value));format!("c.is_adult={arg}")},
                Filter::ParentSeries(value)|Filter::Season(value)|Filter::Episode(value) if self.resource==Resource::Episode=>{
                    let arg=statement.bind(Bind::Integer(*value));let field=match filter {Filter::ParentSeries(_)=>"series_id",Filter::Season(_)=>"season_number",_=>"episode_number"};format!("c.{field}={arg}")
                },
                Filter::SearchTerms(terms)=>{
                    if terms.len()>32{return Err(ReadError::Capacity);}
                    let fields=if self.resource==Resource::Episode{&["name","description"][..]}else{&["name","description","genre"][..]};
                    let mut all=Vec::new();
                    for term in terms {
                        let arg=statement.bind(Bind::String(contains(term)));
                        all.push(format!("({})",fields.iter().map(|field|format!("UPPER(c.{field}::text) LIKE UPPER({arg})")).collect::<Vec<_>>().join(" OR ")));
                    }
                    if all.is_empty(){"true".into()}else{all.join(" AND ")}
                },
                _=>return Err(ReadError::Unsupported("Filter/entity normalization needs Source grammar gate")),
            };
            predicates.push(condition);
        }
        Ok((statement,predicates.join(" AND ")))
    }
    pub fn count(&self,visibility:&Visibility,row_bytes:usize)->Result<Statement,ReadError> {
        let (mut statement,predicates)=self.predicates(visibility,row_bytes)?;
        statement.sql=format!("/* rust-vod-ordinary-count */ SELECT COUNT(DISTINCT c.id)::bigint AS count {} WHERE {predicates}",self.base_from());Ok(statement)
    }
    pub fn rows(&self,visibility:&Visibility,row_bytes:usize,page:Option<&Page>)->Result<Statement,ReadError> {
        let (mut statement,predicates)=self.predicates(visibility,row_bytes)?;
        let parent=if self.resource==Resource::Episode {" JOIN vod_series parent ON parent.id=c.series_id"}else{""};
        let mut lengths=Vec::new();
        for field in self.metadata_fields("c") {lengths.push(format!("coalesce(octet_length({field}),0)::bigint"));}
        if self.resource==Resource::Episode {
            for field in common_metadata_source("c","source_parent_") {lengths.push(format!("coalesce(octet_length({field}),0)::bigint"));}
        } else {
            lengths.extend(["coalesce(octet_length(c.source_logo_name),0)::bigint".into(),"coalesce(octet_length(c.source_logo_url),0)::bigint".into()]);
        }
        let allowed=format!("({})<=$1::bigint",lengths.join("+"));
        let mut fields=vec![format!("NOT({allowed}) AS oversized")];
        if self.resource==Resource::Episode {
            fields.extend(episode_fields(&allowed));
            fields.extend(common_fields_source("c","series","source_parent_",&allowed));
            fields.extend(["c.source_parent_logo_id::bigint AS logo_id".into(),"NULL::text AS logo_name".into(),"NULL::text AS logo_url".into()]);
        } else {
            fields.extend(common_fields("c","content",&allowed));
            if self.resource==Resource::Movie {fields.push("c.duration_secs::bigint AS content_duration_secs".into());fields.push("c.is_adult AS content_is_adult".into());}
            fields.extend(["c.source_logo_id::bigint AS logo_id".into(),format!("CASE WHEN {allowed} THEN c.source_logo_name END AS logo_name"),format!("CASE WHEN {allowed} THEN c.source_logo_url END AS logo_url")]);
        }
        let order=self.order_sql()?;
        let window=if let Some(page)=page {
            let limit=statement.bind(Bind::Integer(i64::try_from(page.size).map_err(|_|ReadError::Capacity)?));
            let offset=statement.bind(Bind::Integer(i64::try_from(page.offset).map_err(|_|ReadError::Capacity)?));format!(" LIMIT {limit} OFFSET {offset}")
        }else if self.detail.is_some(){" LIMIT 2".into()}else{return Err(ReadError::Unsupported("Unpaginated ordinary list is not a Source policy"));};
        let page_logo=if self.resource==Resource::Episode {""}else{" LEFT JOIN vod_vodlogo page_logo ON page_logo.id=c.logo_id"};
        let columns=self.page_columns();
        let page=format!("SELECT DISTINCT {columns} {}{parent}{page_logo} WHERE {predicates} ORDER BY {order}{window}",self.base_from());
        statement.page=Some(page.clone());
        // Isolate the Source-shaped page plan. Only a guarded scalar CTEScan
        // follows it: no outer join/sort/window can rearrange tied rows. Retain
        // raw PgRows in this emitted order before fresh serializer lookups.
        statement.sql=format!("/* rust-vod-ordinary-rows */ WITH source_page AS MATERIALIZED({page}) SELECT {} FROM source_page c",fields.join(","));Ok(statement)
    }
    fn base_from(&self)->String {
        format!("FROM {} c JOIN {} active_relation ON active_relation.{}=c.id JOIN m3u_m3uaccount active_account ON active_account.id=active_relation.m3u_account_id",self.resource.table(),self.resource.relation(),self.resource.foreign())
    }
    fn page_columns(&self)->String {
        // Mirror Source ORM model/select_related column ordering. SELECT c.*
        // uses physical migration order and changes DISTINCT tie behavior.
        // Source SQL2 exposed reversed equal-name Alpha rows under EXISTS.
        let fields=match self.resource {
            Resource::Movie=>&["id","uuid","name","description","year","rating","genre","duration_secs","logo_id","tmdb_id","imdb_id","language","is_adult","custom_properties","created_at","updated_at"][..],
            Resource::Series=>&["id","uuid","name","description","year","rating","genre","logo_id","tmdb_id","imdb_id","language","custom_properties","created_at","updated_at"][..],
            Resource::Episode=>&["id","uuid","name","description","air_date","rating","duration_secs","series_id","season_number","episode_number","tmdb_id","imdb_id","custom_properties","created_at","updated_at"][..],
        };
        let mut columns:Vec<String>=fields.iter().map(|field|format!("c.{field}")).collect();
        if self.resource==Resource::Episode {
            for field in ["id","uuid","name","description","year","rating","genre","logo_id","tmdb_id","imdb_id","language","custom_properties","created_at","updated_at"] {
                columns.push(format!("parent.{field} AS source_parent_{field}"));
            }
        } else {
            for field in ["id","name","url"] {columns.push(format!("page_logo.{field} AS source_logo_{field}"));}
        }
        columns.join(",")
    }
    fn metadata_fields(&self,alias:&str)->Vec<String> {if self.resource==Resource::Episode {
        ["name","description","rating","tmdb_id","imdb_id","custom_properties::text"].iter().map(|field|format!("{alias}.{field}")).collect()
    }else{common_metadata(alias)}}
    fn order_sql(&self)->Result<String,ReadError> {
        if self.order.is_empty(){return Ok(if self.resource==Resource::Episode{"parent.name,c.season_number,c.episode_number"}else{"c.name"}.into());}
        self.order.iter().map(|order| {
            let field=match order.field {OrderField::Name=>"name",OrderField::CreatedAt=>"created_at",
                OrderField::Year if self.resource!=Resource::Episode=>"year",
                OrderField::Season if self.resource==Resource::Episode=>"season_number",
                OrderField::Episode if self.resource==Resource::Episode=>"episode_number",
                _=>return Err(ReadError::Unsupported("Ordering/entity normalization needs Source gate")),};
            Ok(format!("c.{field}{}",if order.descending{" DESC"}else{""}))
        }).collect::<Result<Vec<_>,_>>().map(|v|v.join(","))
    }
}
fn common_metadata(alias:&str)->Vec<String> {common_metadata_source(alias,"")}
fn common_metadata_source(alias:&str,source_prefix:&str)->Vec<String> {
    ["name","description","rating","genre","tmdb_id","imdb_id","language","custom_properties::text"].iter().map(|field|format!("{alias}.{source_prefix}{field}")).collect()
}
fn common_fields(alias:&str,prefix:&str,allowed:&str)->Vec<String> {common_fields_source(alias,prefix,"",allowed)}
fn common_fields_source(alias:&str,prefix:&str,source_prefix:&str,allowed:&str)->Vec<String> {
    let mut fields=vec![format!("{alias}.{source_prefix}id::bigint AS {prefix}_id"),format!("{alias}.{source_prefix}uuid::text AS {prefix}_uuid"),format!("{alias}.{source_prefix}year::bigint AS {prefix}_year"),format!("{alias}.{source_prefix}created_at AS {prefix}_created_at"),format!("{alias}.{source_prefix}updated_at AS {prefix}_updated_at")];
    for field in ["name","description","rating","genre","tmdb_id","imdb_id","language","custom_properties"] {
        fields.push(format!("CASE WHEN {allowed} THEN {alias}.{source_prefix}{field} END AS {prefix}_{field}"));
    }
    fields
}
fn episode_fields(allowed:&str)->Vec<String> {
    let mut fields=vec!["c.id::bigint AS content_id".into(),"c.uuid::text AS content_uuid".into(),"c.created_at AS content_created_at".into(),"c.updated_at AS content_updated_at".into(),"c.air_date AS content_air_date".into()];
    for field in ["duration_secs","season_number","episode_number"] {fields.push(format!("c.{field}::bigint AS content_{field}"));}
    for field in ["name","description","rating","tmdb_id","imdb_id","custom_properties"] {fields.push(format!("CASE WHEN {allowed} THEN c.{field} END AS content_{field}"));}
    fields
}
fn common(row:&PgRow,prefix:&str)->Result<CommonRow,ReadError> {
    let name=|field:&str|format!("{prefix}_{field}");
    Ok(CommonRow {id:row.try_get(name("id").as_str())?,uuid:row.try_get(name("uuid").as_str())?,name:row.try_get(name("name").as_str())?,
        description:row.try_get(name("description").as_str())?,year:row.try_get(name("year").as_str())?,rating:row.try_get(name("rating").as_str())?,genre:row.try_get(name("genre").as_str())?,
        tmdb_id:row.try_get(name("tmdb_id").as_str())?,imdb_id:row.try_get(name("imdb_id").as_str())?,language:row.try_get(name("language").as_str())?,
        custom_properties:row.try_get::<Option<Value>,_>(name("custom_properties").as_str())?.unwrap_or(Value::Null),
        created_at:row.try_get(name("created_at").as_str())?,updated_at:row.try_get(name("updated_at").as_str())?})
}
async fn fresh_logo(row:&PgRow,resource:Resource,work:&super::vod_api_owned_read::ReadWork,connection:&mut super::vod_api_owned_read::OwnedConnection)->Result<Option<LogoRow>,ReadError> {
    let Some(id)=row.try_get::<Option<i64>,_>("logo_id")? else{return Ok(None);};
    let (name,url)=if resource==Resource::Episode {
        connection.configure(work).await?;
        let current=sqlx::query("SELECT (octet_length(name)::bigint+octet_length(url)>$2) AS oversized,CASE WHEN octet_length(name)::bigint+octet_length(url)<=$2 THEN name END AS name,CASE WHEN octet_length(name)::bigint+octet_length(url)<=$2 THEN url END AS url FROM vod_vodlogo WHERE id=$1").persistent(false).bind(id).bind(work.row_bytes()as i64).fetch_optional(connection.raw()).await?
            .ok_or(ReadError::Unsupported("Concurrent missing episode logo fault needs Source gate"))?;
        if current.try_get::<bool,_>("oversized")?{return Err(ReadError::Capacity);}
        (current.try_get::<String,_>("name")?,current.try_get::<String,_>("url")?)
    }else{(row.try_get("logo_name")?,row.try_get("logo_url")?)};
    // Preserve Source method order and snapshot boundaries; no per-page logo
    // cache. is_used is a fresh exists lookup, not derived from earlier counts.
    connection.configure(work).await?;
    let movie_count=sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM vod_movie WHERE logo_id=$1").persistent(false).bind(id).fetch_one(connection.raw()).await?;
    connection.configure(work).await?;
    let series_count=sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM vod_series WHERE logo_id=$1").persistent(false).bind(id).fetch_one(connection.raw()).await?;
    connection.configure(work).await?;
    let mut is_used=sqlx::query("SELECT 1 FROM vod_movie WHERE logo_id=$1 LIMIT 1").persistent(false).bind(id).fetch_optional(connection.raw()).await?.is_some();
    if !is_used {
        connection.configure(work).await?;
        is_used=sqlx::query("SELECT 1 FROM vod_series WHERE logo_id=$1 LIMIT 1").persistent(false).bind(id).fetch_optional(connection.raw()).await?.is_some();
    }
    connection.configure(work).await?;
    let movie_names=sqlx::query_scalar::<_,String>("SELECT name FROM vod_movie WHERE logo_id=$1 ORDER BY name LIMIT 10").persistent(false).bind(id).fetch_all(connection.raw()).await?;
    connection.configure(work).await?;
    let series_names=sqlx::query_scalar::<_,String>("SELECT name FROM vod_series WHERE logo_id=$1 ORDER BY name LIMIT 10").persistent(false).bind(id).fetch_all(connection.raw()).await?;
    Ok(Some(LogoRow {id,name,url,movie_count,series_count,is_used,movie_names,series_names}))
}
pub(crate) async fn project_fresh(row:&PgRow,resource:Resource,context:&LogoContext<'_>,work:&super::vod_api_owned_read::ReadWork,connection:&mut super::vod_api_owned_read::OwnedConnection)->Result<Value,ReadError> {
    if row.try_get::<bool,_>("oversized")?{return Err(ReadError::Capacity);}
    let logo=fresh_logo(row,resource,work,connection).await?;
    let series_count=if resource!=Resource::Movie {
        let prefix=if resource==Resource::Episode {"series_id"}else{"content_id"};
        connection.configure(work).await?;
        sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM vod_episode WHERE series_id=$1").persistent(false).bind(row.try_get::<i64,_>(prefix)?).fetch_one(connection.raw()).await?
    }else{0};
    Ok(match resource {
        Resource::Movie=>MovieRow {common:common(row,"content")?,logo,duration_secs:row.try_get("content_duration_secs")?,is_adult:row.try_get("content_is_adult")?}.project(context),
        Resource::Series=>SeriesRow {common:common(row,"content")?,logo,episode_count:series_count}.project(context),
        Resource::Episode=>EpisodeRow {id:row.try_get("content_id")?,uuid:row.try_get("content_uuid")?,name:row.try_get("content_name")?,
            series:SeriesRow {common:common(row,"series")?,logo,episode_count:series_count},
            description:row.try_get("content_description")?,air_date:row.try_get("content_air_date")?,rating:row.try_get("content_rating")?,
            duration_secs:row.try_get("content_duration_secs")?,season_number:row.try_get("content_season_number")?,episode_number:row.try_get("content_episode_number")?,
            tmdb_id:row.try_get("content_tmdb_id")?,imdb_id:row.try_get("content_imdb_id")?,custom_properties:row.try_get::<Option<Value>,_>("content_custom_properties")?.unwrap_or(Value::Null),
            created_at:row.try_get("content_created_at")?,updated_at:row.try_get("content_updated_at")?}.project(context),
    })
}

impl Selection {
    pub fn input_bytes(&self)->Result<usize,ReadError> {
        let mut total=0usize;
        for filter in &self.filters {
            let bytes=match filter {
                Filter::NameContains(value)=>value.len(),Filter::Category{name,kind}=>name.len().checked_add(kind.as_ref().map_or(0,String::len)).ok_or(ReadError::Capacity)?,
                Filter::SearchTerms(values)=>values.iter().try_fold(0usize,|n,v|n.checked_add(v.len()).ok_or(ReadError::Capacity))?,_=>8,
            };
            total=total.checked_add(bytes).ok_or(ReadError::Capacity)?;
        }
        Ok(total)
    }
}
