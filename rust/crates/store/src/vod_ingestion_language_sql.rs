//! Language reconciliation from Original Dispatcharr's VOD task helpers.
//! Original Dispatcharr/Django/PostgreSQL authors retain all application credit.
//! Source11 qualifies clean two-language owner/split/retag/clear/donor flows.
//! Dirty claim cycles, concurrency and fault outcomes still require acceptance.
use crate::{AuthService, StoreError};
use chrono::{DateTime, Utc};
use dispatcharr_contracts::BoxFuture;
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
#[path = "vod_ingestion_language_episodes.rs"]
mod episodes;
type Owner = Arc<dyn Send + Sync>;
const GRAPH_ROWS: usize = 2000;
const CLAIM_STUCK: &str = "VOD language title claim did not settle";
const NO_PARK: &str = "VOD language title has no free parking code";
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Movie,
    Series,
}
impl Kind {
    fn table(self) -> &'static str {
        match self {
            Self::Movie => "vod_movie",
            Self::Series => "vod_series",
        }
    }
    fn relation(self) -> &'static str {
        match self {
            Self::Movie => "vod_m3umovierelation",
            Self::Series => "vod_m3useriesrelation",
        }
    }
    fn fk(self) -> &'static str {
        match self {
            Self::Movie => "movie_id",
            Self::Series => "series_id",
        }
    }
}
#[derive(Default, Debug)]
pub(super) struct LanguageSummary {
    pub updated: usize,
    pub moved: usize,
    pub absorbed: usize,
    pub failed: usize,
}
pub(super) fn language_skip_note(movie_failed: usize, series_failed: usize) -> String {
    let mut notes = Vec::new();
    for (n, kind) in [(movie_failed, "movie"), (series_failed, "series")] {
        if n > 0 {
            notes.push(format!(
                "{n} {kind} title{} skipped during language reconcile",
                if n == 1 { "" } else { "s" }
            ));
        }
    }
    if notes.is_empty() {
        String::new()
    } else {
        format!(". {}", notes.join(". "))
    }
}
#[derive(Clone)]
struct Vod {
    id: i64,
    language: String,
    body: Value,
}
impl Vod {
    fn read(body: Value) -> Result<Self, StoreError> {
        Ok(Self {
            id: number(&body, "id")?,
            language: text(&body, "language")?.to_owned(),
            body,
        })
    }
}
fn unsupported() -> StoreError {
    StoreError::Unsupported("VOD language reconciliation ownership/domain unavailable")
}
fn number(v: &Value, key: &str) -> Result<i64, StoreError> {
    v[key].as_i64().ok_or_else(unsupported)
}
fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str, StoreError> {
    v[key].as_str().ok_or_else(unsupported)
}
fn bounded<T>(rows: Vec<T>) -> Result<Vec<T>, StoreError> {
    if rows.len() > GRAPH_ROWS {
        Err(unsupported())
    } else {
        Ok(rows)
    }
}
#[derive(Clone)]
struct Group {
    language: String,
    ids: Vec<i64>,
}
fn keep_language(source: &Vod, groups: &[Group]) -> Option<String> {
    if groups.is_empty() {
        return None;
    }
    if source.language.is_empty() && groups.iter().all(|g| g.language.is_empty()) {
        return None;
    }
    if !source.language.is_empty() && groups.iter().all(|g| g.language == source.language) {
        return None;
    }
    if source.language.is_empty() && groups.iter().any(|g| g.language.is_empty()) {
        return Some(String::new());
    }
    if groups.iter().any(|g| g.language == source.language) {
        return Some(source.language.clone());
    }
    groups
        .iter()
        .filter(|g| !g.language.is_empty())
        .max_by_key(|g| (g.ids.len(), &g.language))
        .map(|g| g.language.clone())
        .or_else(|| Some(String::new()))
}
#[derive(Default)]
struct Run {
    updated: usize,
    moved: usize,
    absorbed: usize,
    done: HashSet<i64>,
    absorbed_ids: HashSet<i64>,
    stack: Vec<i64>,
}
#[derive(Default)]
struct Context {
    languages: HashMap<(i64, i64), String>,
    done: HashSet<i64>,
    absorbed_ids: HashSet<i64>,
}
#[derive(Clone, Copy)]
enum Op {
    Candidates,
    MinRows,
    Row,
    Relations,
    Slot,
    BulkLanguage,
    Insert,
    Move,
    Delete,
    Metadata,
}
#[cfg(test)]
const OPS: [Op; 10] = [
    Op::Candidates,
    Op::MinRows,
    Op::Row,
    Op::Relations,
    Op::Slot,
    Op::BulkLanguage,
    Op::Insert,
    Op::Move,
    Op::Delete,
    Op::Metadata,
];
fn statement(kind: Kind, op: Op) -> String {
    let t = kind.table();
    let r = kind.relation();
    let fk = kind.fk();
    match op {
        Op::Candidates=>format!("SELECT DISTINCT m.id::bigint FROM {t} m JOIN {r} r ON r.{fk}=m.id LEFT JOIN vod_m3uvodcategoryrelation c ON c.m3u_account_id=r.m3u_account_id AND c.category_id=r.category_id WHERE r.m3u_account_id::bigint=$1 AND r.last_seen>=$2 AND COALESCE(lower(c.custom_properties->>'language'),'')<>m.language ORDER BY m.id::bigint"),
        Op::MinRows=>format!("SELECT jsonb_build_object('id',m.id,'name',m.name,'year',m.year,'tmdb_id',m.tmdb_id,'imdb_id',m.imdb_id,'language',m.language) AS item FROM {t} m WHERE m.id::bigint=ANY($1) ORDER BY m.id"),
        Op::Row=>format!("SELECT to_jsonb(m) AS item FROM {t} m WHERE m.id::bigint=$1"),
        Op::Relations=>format!("SELECT jsonb_build_object('id',r.id,'parent',r.{fk},'account',r.m3u_account_id,'category',r.category_id) AS item FROM {r} r WHERE r.{fk}::bigint=ANY($1) ORDER BY r.id LIMIT CASE WHEN cardinality($1)=1 THEN 2001 ELSE NULL END"),
        Op::Slot=>format!("SELECT to_jsonb(m) AS item FROM {t} m WHERE m.language=$2 AND ((COALESCE($1->>'tmdb_id','')<>'' AND m.tmdb_id=$1->>'tmdb_id') OR (COALESCE($1->>'imdb_id','')<>'' AND m.imdb_id=$1->>'imdb_id') OR (COALESCE($1->>'tmdb_id','')='' AND COALESCE($1->>'imdb_id','')='' AND m.tmdb_id IS NULL AND m.imdb_id IS NULL AND m.name=$1->>'name' AND m.year IS NOT DISTINCT FROM ($1->>'year')::integer)) ORDER BY CASE WHEN COALESCE($1->>'tmdb_id','')<>'' AND m.tmdb_id=$1->>'tmdb_id' THEN 0 ELSE 1 END,m.id LIMIT 1"),
        Op::BulkLanguage=>format!("UPDATE {t} m SET language=v->>'language' FROM jsonb_array_elements($1) v WHERE m.id::bigint=(v->>'id')::bigint"),
        Op::Insert=>if kind==Kind::Movie {
            "INSERT INTO vod_movie(uuid,name,description,year,rating,genre,duration_secs,logo_id,tmdb_id,imdb_id,language,is_adult,custom_properties,created_at,updated_at) SELECT (v->>'uuid')::uuid,v->>'name',NULL,(v->>'year')::integer,NULL,NULL,NULL,NULL,v->>'tmdb_id',v->>'imdb_id',v->>'language',false,NULL,(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz FROM (SELECT $1::jsonb AS v)s RETURNING to_jsonb(vod_movie) AS item".to_owned()
        }else{
            "INSERT INTO vod_series(uuid,name,description,year,rating,genre,logo_id,tmdb_id,imdb_id,language,custom_properties,created_at,updated_at) SELECT (v->>'uuid')::uuid,v->>'name',NULL,(v->>'year')::integer,NULL,NULL,NULL,v->>'tmdb_id',v->>'imdb_id',v->>'language',NULL,(v->>'stamp')::timestamptz,(v->>'stamp')::timestamptz FROM (SELECT $1::jsonb AS v)s RETURNING to_jsonb(vod_series) AS item".to_owned()
        },
        Op::Move=>{let flags=if kind==Kind::Series{"jsonb_build_object('detailed_fetched',false,'episodes_fetched',false)"}else{"jsonb_build_object('detailed_fetched',false)"};format!("UPDATE {r} SET {fk}=$2::bigint,custom_properties=COALESCE(custom_properties,'{{}}'::jsonb)||{flags} WHERE id::bigint=ANY($1)")},
        Op::Delete=>format!("DELETE FROM {t} WHERE id::bigint=$1"),
        Op::Metadata=>{let duration=if kind==Kind::Movie{",duration_secs=($1->>'duration_secs')::bigint"}else{""};format!("UPDATE {t} SET description=$1->>'description',rating=$1->>'rating',genre=$1->>'genre',year=($1->>'year')::integer,logo_id=($1->>'logo_id')::bigint,custom_properties=NULLIF($1->'custom_properties','null'::jsonb){duration} WHERE id::bigint=($1->>'id')::bigint")},
    }
}
const CATEGORY_MAP:&str="SELECT jsonb_build_object('account',c.m3u_account_id,'category',c.category_id,'language',CASE WHEN jsonb_typeof(c.custom_properties->'language')='string' THEN lower(c.custom_properties->>'language') ELSE '' END) AS item FROM vod_m3uvodcategoryrelation c WHERE c.m3u_account_id::bigint=ANY($1) AND c.category_id::bigint=ANY($2) ORDER BY c.id LIMIT 2001";
fn merge_languages(rows: &[Value], context: &mut Context) -> Result<(), StoreError> {
    for r in rows {
        context.languages.insert(
            (number(r, "account")?, number(r, "category")?),
            text(r, "language")?.to_owned(),
        );
    }
    Ok(())
}
async fn load_languages(
    c: &mut PgConnection,
    rows: &[Value],
    context: &mut Context,
) -> Result<(), StoreError> {
    let mut accounts = HashSet::new();
    let mut categories = HashSet::new();
    for r in rows {
        if let Some(category) = r["category"].as_i64() {
            accounts.insert(number(r, "account")?);
            categories.insert(category);
        }
    }
    if categories.is_empty() {
        return Ok(());
    }
    let values = bounded(
        sqlx::query_scalar::<_, Value>(CATEGORY_MAP)
            .bind(accounts.into_iter().collect::<Vec<_>>())
            .bind(categories.into_iter().collect::<Vec<_>>())
            .fetch_all(c)
            .await?,
    )?;
    merge_languages(&values, context)
}
async fn row(c: &mut PgConnection, kind: Kind, id: i64) -> Result<Option<Vod>, StoreError> {
    sqlx::query_scalar::<_, Value>(&statement(kind, Op::Row))
        .bind(id)
        .fetch_optional(c)
        .await?
        .map(Vod::read)
        .transpose()
}
fn relation_language(r: &Value, context: &Context) -> Result<String, StoreError> {
    let Some(category) = r["category"].as_i64() else {
        return Ok(String::new());
    };
    Ok(context
        .languages
        .get(&(number(r, "account")?, category))
        .cloned()
        .unwrap_or_default())
}

fn grouped(rows: &[Value], context: &mut Context) -> Result<Vec<Group>, StoreError> {
    let mut groups = Vec::<Group>::new();
    for r in rows {
        let language = relation_language(r, context)?;
        let id = number(r, "id")?;
        if let Some(g) = groups.iter_mut().find(|g| g.language == language) {
            g.ids.push(id);
        } else {
            groups.push(Group {
                language,
                ids: vec![id],
            });
        }
    }
    Ok(groups)
}

async fn groups(
    c: &mut PgConnection,
    kind: Kind,
    id: i64,
    context: &mut Context,
) -> Result<Vec<Group>, StoreError> {
    let rows = bounded(
        sqlx::query_scalar::<_, Value>(&statement(kind, Op::Relations))
            .bind(vec![id])
            .fetch_all(&mut *c)
            .await?,
    )?;
    let missing: Vec<Value> = rows
        .iter()
        .filter(|r| {
            r["category"].as_i64().is_some_and(|category| {
                r["account"]
                    .as_i64()
                    .is_some_and(|account| !context.languages.contains_key(&(account, category)))
            })
        })
        .cloned()
        .collect();
    if !missing.is_empty() {
        load_languages(c, &missing, context).await?;
    }
    grouped(&rows, context)
}

async fn slot(
    c: &mut PgConnection,
    kind: Kind,
    source: &Vod,
    language: &str,
) -> Result<Option<Vod>, StoreError> {
    sqlx::query_scalar::<_, Value>(&statement(kind, Op::Slot))
        .bind(&source.body)
        .bind(language)
        .fetch_optional(c)
        .await?
        .map(Vod::read)
        .transpose()
}
async fn set_language(
    c: &mut PgConnection,
    kind: Kind,
    source: &Vod,
    language: &str,
    run: &mut Run,
) -> Result<(), StoreError> {
    if source.language != language {
        sqlx::query(&statement(kind, Op::BulkLanguage))
            .bind(json!([{"id":source.id,"language":language}]))
            .execute(c)
            .await?;
        run.updated += 1;
    }
    Ok(())
}
async fn move_relations(
    c: &mut PgConnection,
    kind: Kind,
    ids: &[i64],
    target: i64,
) -> Result<(), StoreError> {
    sqlx::query(&statement(kind, Op::Move))
        .bind(ids)
        .bind(target)
        .execute(c)
        .await?;
    Ok(())
}
fn blank(value: &Value) -> bool {
    value.is_null()
        || value.as_str() == Some("")
        || value
            .as_array()
            .is_some_and(|v| v.iter().all(|x| x.is_null() || x.as_str() == Some("")))
}
fn fill_metadata(source: &Vod, donor: &Vod, kind: Kind) -> Option<Value> {
    let mut next = source.body.clone();
    for field in ["description", "rating", "genre", "year"] {
        if blank(&next[field]) && !blank(&donor.body[field]) {
            next[field] = donor.body[field].clone();
        }
    }
    if kind == Kind::Movie
        && next["duration_secs"].as_i64().unwrap_or(0) == 0
        && donor.body["duration_secs"].as_i64().unwrap_or(0) != 0
    {
        next["duration_secs"] = donor.body["duration_secs"].clone();
    }
    if next["logo_id"].as_i64().unwrap_or(0) == 0
        && donor.body["logo_id"].as_i64().unwrap_or(0) != 0
    {
        next["logo_id"] = donor.body["logo_id"].clone();
    }
    if let Some(properties) = donor.body["custom_properties"].as_object() {
        let mut target = next["custom_properties"]
            .as_object()
            .cloned()
            .unwrap_or_default();
        let mut changed = false;
        for (key, value) in properties {
            if !blank(value) && target.get(key).is_none_or(blank) {
                target.insert(key.clone(), value.clone());
                changed = true;
            }
        }
        if changed {
            next["custom_properties"] = Value::Object(target);
        }
    }
    if next == source.body {
        None
    } else {
        Some(next)
    }
}
async fn absorb(
    c: &mut PgConnection,
    kind: Kind,
    source: &Vod,
    donor: &Vod,
    context: &mut Context,
) -> Result<(), StoreError> {
    let donor_groups = groups(c, kind, donor.id, context).await?;
    let ids: Vec<i64> = donor_groups.into_iter().flat_map(|g| g.ids).collect();
    if !ids.is_empty() {
        move_relations(c, kind, &ids, source.id).await?;
    }
    if kind == Kind::Series {
        episodes::migrate(c, donor.id, source.id).await?;
    }
    if let Some(metadata) = fill_metadata(source, donor, kind) {
        sqlx::query(&statement(kind, Op::Metadata))
            .bind(metadata)
            .execute(&mut *c)
            .await?;
    }
    sqlx::query(&statement(kind, Op::Delete))
        .bind(donor.id)
        .execute(c)
        .await?;
    Ok(())
}
async fn all_match(
    c: &mut PgConnection,
    kind: Kind,
    id: i64,
    language: &str,
    context: &mut Context,
) -> Result<bool, StoreError> {
    Ok(groups(c, kind, id, context)
        .await?
        .iter()
        .all(|g| g.language == language))
}
async fn hand_off(
    c: &mut PgConnection,
    kind: Kind,
    source: &Vod,
    run: &mut Run,
    context: &mut Context,
) -> Result<(), StoreError> {
    let groups = groups(c, kind, source.id, context).await?;
    let Some(keep) = keep_language(source, &groups) else {
        return Ok(());
    };
    let mut homes = Vec::new();
    for g in &groups {
        if let Some(target) = slot(c, kind, source, &g.language).await? {
            if target.id != source.id
                && !run.absorbed_ids.contains(&target.id)
                && all_match(c, kind, target.id, &g.language, context).await?
            {
                homes.push((g, target.id));
            }
        }
    }
    let every = homes.len() == groups.len();
    for (g, target) in homes {
        if every && g.language == keep {
            continue;
        }
        move_relations(c, kind, &g.ids, target).await?;
        run.moved += g.ids.len();
    }
    Ok(())
}
fn claim<'a>(
    c: &'a mut PgConnection,
    kind: Kind,
    source_id: i64,
    language: String,
    run: &'a mut Run,
    context: &'a mut Context,
) -> BoxFuture<'a, Result<(), StoreError>> {
    Box::pin(async move {
        let mut seen = HashSet::new();
        loop {
            let Some(source) = row(c, kind, source_id).await? else {
                return Ok(());
            };
            if source.language == language {
                return Ok(());
            }
            let donor = slot(c, kind, &source, &language).await?;
            let Some(donor) =
                donor.filter(|d| d.id != source.id && !run.absorbed_ids.contains(&d.id))
            else {
                return set_language(c, kind, &source, &language, run).await;
            };
            if all_match(c, kind, donor.id, &language, context).await? {
                absorb(c, kind, &source, &donor, context).await?;
                run.absorbed += 1;
                run.absorbed_ids.insert(donor.id);
                let source = row(c, kind, source_id).await?.ok_or_else(unsupported)?;
                return set_language(c, kind, &source, &language, run).await;
            }
            if run.stack.contains(&source.id) {
                let forbidden = [
                    language.as_str(),
                    source.language.as_str(),
                    donor.language.as_str(),
                    "",
                ];
                let mut parked = false;
                for first in "zyxwvutsrqponmlkjihgfedcba".chars() {
                    for second in "zyxwvutsrqponmlkjihgfedcba".chars() {
                        let code = format!("{first}{second}");
                        if forbidden.contains(&code.as_str()) {
                            continue;
                        }
                        if slot(c, kind, &donor, &code)
                            .await?
                            .is_none_or(|v| v.id == donor.id)
                        {
                            sqlx::query(&statement(kind, Op::BulkLanguage))
                                .bind(json!([{"id":donor.id,"language":code}]))
                                .execute(&mut *c)
                                .await?;
                            parked = true;
                            break;
                        }
                    }
                    if parked {
                        break;
                    }
                }
                if !parked {
                    return Err(StoreError::Unsupported(NO_PARK));
                }
                continue;
            }
            if !seen.insert((donor.id, donor.language.clone())) {
                return Err(StoreError::Unsupported(CLAIM_STUCK));
            }
            if run.stack.len() >= GRAPH_ROWS {
                return Err(unsupported());
            }
            run.stack.push(source.id);
            let observed = reconcile_one(c, kind, donor.id, run, context).await;
            run.stack.pop();
            observed?;
        }
    })
}
fn reconcile_one<'a>(
    c: &'a mut PgConnection,
    kind: Kind,
    id: i64,
    run: &'a mut Run,
    context: &'a mut Context,
) -> BoxFuture<'a, Result<(), StoreError>> {
    Box::pin(async move {
        if run.done.contains(&id) || run.absorbed_ids.contains(&id) {
            return Ok(());
        }
        let Some(source) = row(c, kind, id).await? else {
            run.done.insert(id);
            return Ok(());
        };
        let moved = run.moved;
        hand_off(c, kind, &source, run, context).await?;
        let current = groups(c, kind, id, context).await?;
        if let Some(language) = keep_language(&source, &current) {
            claim(c, kind, id, language, run, context).await?;
        }
        let Some(source) = row(c, kind, id).await? else {
            run.done.insert(id);
            return Ok(());
        };
        for g in groups(c, kind, id, context).await? {
            if g.language == source.language || g.ids.is_empty() {
                continue;
            }
            let target = if let Some(target) = slot(c, kind, &source, &g.language).await? {
                target
            } else {
                let mut body = source.body.clone();
                body["language"] = json!(g.language);
                body["uuid"] = json!(uuid::Uuid::new_v4());
                body["stamp"] = json!(Utc::now());
                Vod::read(
                    sqlx::query_scalar::<_, Value>(&statement(kind, Op::Insert))
                        .bind(body)
                        .fetch_one(&mut *c)
                        .await?,
                )?
            };
            if target.id == id || run.absorbed_ids.contains(&target.id) {
                continue;
            }
            move_relations(c, kind, &g.ids, target.id).await?;
            run.moved += g.ids.len();
        }
        if kind == Kind::Series && run.moved > moved {
            episodes::split(c, id).await?;
        }
        run.done.insert(id);
        Ok(())
    })
}
fn identity(v: &Vod) -> Value {
    if v.body["tmdb_id"].as_str().is_some_and(|s| !s.is_empty()) {
        json!(["tmdb", v.body["tmdb_id"]])
    } else if v.body["imdb_id"].as_str().is_some_and(|s| !s.is_empty()) {
        json!(["imdb", v.body["imdb_id"]])
    } else {
        json!(["name", v.body["name"], v.body["year"]])
    }
}
async fn reconcile(
    c: &mut PgConnection,
    kind: Kind,
    account: i64,
    scan: DateTime<Utc>,
) -> Result<LanguageSummary, StoreError> {
    let ids: Vec<i64> = sqlx::query_scalar(&statement(kind, Op::Candidates))
        .bind(account)
        .bind(scan)
        .fetch_all(&mut *c)
        .await?;
    if ids.is_empty() {
        return Ok(LanguageSummary::default());
    }
    let mut minimal = Vec::new();
    for chunk in ids.chunks(500) {
        for v in sqlx::query_scalar::<_, Value>(&statement(kind, Op::MinRows))
            .bind(chunk)
            .fetch_all(&mut *c)
            .await?
        {
            minimal.push(Vod::read(v)?);
        }
    }
    let relation_snapshot = sqlx::query_scalar::<_, Value>(&statement(kind, Op::Relations))
        .bind(&ids)
        .fetch_all(&mut *c)
        .await?;
    let mut context = Context::default();
    // Source captures the account/category crossproduct once before mutations.
    load_languages(c, &relation_snapshot, &mut context).await?;
    let mut by_parent = HashMap::<i64, Vec<Value>>::new();
    for value in relation_snapshot {
        by_parent
            .entry(number(&value, "parent")?)
            .or_default()
            .push(value);
    }
    let mut plans = HashMap::<i64, String>::new();
    let mut counts = HashMap::<String, usize>::new();
    let mut groups_by_id = HashMap::new();
    for v in &minimal {
        let g = grouped(
            by_parent.get(&v.id).map_or(&[], Vec::as_slice),
            &mut context,
        )?;
        if g.iter().map(|g| g.ids.len()).sum::<usize>() > GRAPH_ROWS {
            return Err(unsupported());
        }
        if let Some(language) = keep_language(v, &g) {
            if g.iter().all(|g| g.language == language) && language != v.language {
                let key = json!([identity(v), language]).to_string();
                *counts.entry(key).or_default() += 1;
                plans.insert(v.id, language);
            }
        }
        groups_by_id.insert(v.id, g);
    }
    let mut simple = Vec::new();
    for v in &minimal {
        if let Some(language) = plans.get(&v.id) {
            let key = json!([identity(v), language]).to_string();
            let occupied = slot(c, kind, v, language)
                .await?
                .is_some_and(|o| o.id != v.id);
            if counts[&key] == 1 && !occupied {
                simple.push(json!({"id":v.id,"language":language}));
            }
        }
    }
    let mut summary = LanguageSummary::default();
    let mut done = HashSet::new();
    if !simple.is_empty() {
        let mut tx = c.begin().await?;
        let result = async {
            for chunk in simple.chunks(500) {
                sqlx::query(&statement(kind, Op::BulkLanguage))
                    .bind(json!(chunk))
                    .execute(&mut *tx)
                    .await?;
            }
            Ok::<_, sqlx::Error>(())
        }
        .await;
        let committed = if result.is_ok() {
            let committed = tx.commit().await.is_ok();
            if !committed {
                // A failed COMMIT may have left a rollback queued by SQLx.
                // Await a physical rollback before reusing this owned connection.
                sqlx::query("ROLLBACK").execute(&mut *c).await?;
            }
            committed
        } else {
            tx.rollback().await?;
            false
        };
        if committed {
            summary.updated = simple.len();
            for v in simple {
                done.insert(number(&v, "id")?);
            }
        }
    }
    let mut pending: Vec<Vod> = minimal
        .into_iter()
        .filter(|v| !done.contains(&v.id) && keep_language(v, &groups_by_id[&v.id]).is_some())
        .collect();
    pending.sort_by_key(|v| {
        (
            v.language.is_empty() || groups_by_id[&v.id].iter().any(|g| g.language == v.language),
            std::cmp::Reverse(v.id),
        )
    });
    for source in pending {
        if context.done.contains(&source.id) || context.absorbed_ids.contains(&source.id) {
            continue;
        }
        let mut tx = c.begin().await?;
        let mut run = Run::default();
        let observed = reconcile_one(&mut tx, kind, source.id, &mut run, &mut context).await;
        match observed {
            Ok(()) => {
                if tx.commit().await.is_err() {
                    sqlx::query("ROLLBACK").execute(&mut *c).await?;
                    summary.failed += 1;
                    continue;
                }
                summary.updated += run.updated;
                summary.moved += run.moved;
                summary.absorbed += run.absorbed;
                context.done.extend(run.done);
                context.absorbed_ids.extend(run.absorbed_ids);
            }
            Err(error) => {
                tx.rollback().await?;
                match error {
                    StoreError::Database(_)
                    | StoreError::Unsupported(CLAIM_STUCK)
                    | StoreError::Unsupported(NO_PARK) => summary.failed += 1,
                    other => return Err(other),
                }
            }
        }
    }
    if kind == Kind::Series {
        episodes::delete_empty_candidates(c, &ids).await?;
    }
    Ok(summary)
}
async fn owned(
    auth: &AuthService,
    owner: Owner,
    kind: Kind,
    account: i64,
    scan: DateTime<Utc>,
) -> Result<LanguageSummary, StoreError> {
    let session = auth.borrow_task_sql_session(owner).await?;
    let observed = session
        .run(move |c| Box::pin(reconcile(c, kind, account, scan)))
        .await;
    session.close().await?;
    observed?
}
pub(super) async fn reconcile_movies(
    auth: &AuthService,
    owner: Owner,
    account: i64,
    scan: DateTime<Utc>,
) -> Result<LanguageSummary, StoreError> {
    owned(auth, owner, Kind::Movie, account, scan).await
}
pub(super) async fn reconcile_series(
    auth: &AuthService,
    owner: Owner,
    account: i64,
    scan: DateTime<Utc>,
) -> Result<LanguageSummary, StoreError> {
    owned(auth, owner, Kind::Series, account, scan).await
}
#[cfg(test)]
pub(super) fn schema_statements() -> Vec<(&'static str, String)> {
    let names = [
        [
            "language_movie_candidates",
            "language_movie_min_rows",
            "language_movie_row",
            "language_movie_relations",
            "language_movie_slot",
            "language_movie_bulk_language",
            "language_movie_insert",
            "language_movie_move",
            "language_movie_delete",
            "language_movie_metadata",
        ],
        [
            "language_series_candidates",
            "language_series_min_rows",
            "language_series_row",
            "language_series_relations",
            "language_series_slot",
            "language_series_bulk_language",
            "language_series_insert",
            "language_series_move",
            "language_series_delete",
            "language_series_metadata",
        ],
    ];
    let mut rows = Vec::new();
    for (i, kind) in [Kind::Movie, Kind::Series].into_iter().enumerate() {
        for (j, op) in OPS.into_iter().enumerate() {
            rows.push((names[i][j], statement(kind, op)));
        }
    }
    rows.push(("language_category_map", CATEGORY_MAP.to_owned()));
    rows.extend(episodes::schema_statements());
    rows
}
#[cfg(test)]
pub(super) fn schema_parameters(name: &str) -> Option<Vec<sqlx::postgres::PgTypeInfo>> {
    use sqlx::Type;
    let i = <i64 as Type<sqlx::Postgres>>::type_info();
    let a = <Vec<i64> as Type<sqlx::Postgres>>::type_info();
    let j = <Value as Type<sqlx::Postgres>>::type_info();
    let t = <String as Type<sqlx::Postgres>>::type_info();
    let d = <DateTime<Utc> as Type<sqlx::Postgres>>::type_info();
    if name == "language_category_map" {
        return Some(vec![a.clone(), a]);
    }
    if name.starts_with("language_movie_") || name.starts_with("language_series_") {
        let op = name
            .strip_prefix("language_movie_")
            .or_else(|| name.strip_prefix("language_series_"))?;
        Some(match op {
            "candidates" => vec![i, d],
            "min_rows" => vec![a],
            "row" | "delete" => vec![i],
            "relations" => vec![a],
            "slot" => vec![j, t],
            "bulk_language" | "insert" | "metadata" => vec![j],
            "move" => vec![a, i],
            _ => return None,
        })
    } else {
        episodes::schema_parameters(name)
    }
}
#[cfg(test)]
pub(super) fn schema_columns(name: &str) -> Option<Vec<(&'static str, &'static str)>> {
    if name == "language_category_map" {
        return Some(vec![("item", "JSONB")]);
    }
    if name.starts_with("language_movie_") || name.starts_with("language_series_") {
        let op = name
            .strip_prefix("language_movie_")
            .or_else(|| name.strip_prefix("language_series_"))?;
        Some(match op {
            "candidates" => vec![("id", "INT8")],
            "min_rows" | "row" | "relations" | "slot" | "insert" => vec![("item", "JSONB")],
            "bulk_language" | "move" | "delete" | "metadata" => vec![],
            _ => return None,
        })
    } else {
        episodes::schema_columns(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample(language: &str) -> Vod {
        Vod::read(json!({"id":1,"language":language,"name":"Owned","year":2010,"tmdb_id":"96001","imdb_id":null,"description":null,"rating":null,"genre":null,"duration_secs":0,"logo_id":null,"custom_properties":{"keep":false,"empty":[]},"is_adult":false})).unwrap()
    }
    #[test]
    fn source_keep_rule_preserves_empty_current_then_count_and_code() {
        let groups = vec![
            Group {
                language: "en".to_owned(),
                ids: vec![1, 2],
            },
            Group {
                language: "".to_owned(),
                ids: vec![3],
            },
        ];
        assert_eq!(keep_language(&sample(""), &groups), Some(String::new()));
        assert_eq!(keep_language(&sample("en"), &groups), Some("en".to_owned()));
        let tied = vec![
            Group {
                language: "en".to_owned(),
                ids: vec![1],
            },
            Group {
                language: "fr".to_owned(),
                ids: vec![2],
            },
        ];
        assert_eq!(keep_language(&sample("de"), &tied), Some("fr".to_owned()));
        assert_eq!(keep_language(&sample("en"), &tied[..1]), None);
    }
    #[test]
    fn donor_fills_blanks_without_overwriting_false_zero_or_identity() {
        let source = sample("fr");
        let mut donor = sample("en");
        donor.body["description"] = json!("Original donor");
        donor.body["duration_secs"] = json!(60);
        donor.body["is_adult"] = json!(true);
        donor.body["custom_properties"] = json!({"keep":true,"empty":"filled","zero":0});
        let actual = fill_metadata(&source, &donor, Kind::Movie).unwrap();
        assert_eq!(actual["description"], "Original donor");
        assert_eq!(actual["duration_secs"], 60);
        assert_eq!(
            actual["custom_properties"],
            json!({"keep":false,"empty":"filled","zero":0})
        );
        assert_eq!(actual["is_adult"], false);
        assert_eq!(actual["language"], "fr");
        assert_eq!(actual["id"], 1);
        assert!(fill_metadata(&source, &source, Kind::Movie).is_none());
    }
    #[test]
    fn category_snapshot_keeps_seen_values_until_missing_donor_map_load() {
        let mut context = Context::default();
        merge_languages(
            &[json!({"account":5,"category":11,"language":"en"})],
            &mut context,
        )
        .unwrap();
        let rel = json!({"id":1,"parent":1,"account":5,"category":11});
        assert_eq!(
            grouped(std::slice::from_ref(&rel), &mut context).unwrap()[0].language,
            "en"
        );
        // A fresh relation read cannot overwrite a category already in the map.
        let mut reread = rel.clone();
        reread["language"] = json!("fr");
        assert_eq!(grouped(&[reread], &mut context).unwrap()[0].language, "en");
        let mut donor = rel;
        donor["category"] = json!(21);
        assert_eq!(
            grouped(std::slice::from_ref(&donor), &mut context).unwrap()[0].language,
            ""
        );
        merge_languages(
            &[json!({"account":5,"category":21,"language":"fr"})],
            &mut context,
        )
        .unwrap();
        assert_eq!(grouped(&[donor], &mut context).unwrap()[0].language, "fr");
    }

    #[test]
    fn skipped_title_suffix_preserves_source_counts_and_order() {
        assert_eq!(language_skip_note(0, 0), "");
        assert_eq!(language_skip_note(1,2),". 1 movie title skipped during language reconcile. 2 series titles skipped during language reconcile");
    }
}
