//! Actual original staging/swap comparisons on a disposable PostgreSQL fixture.
use chrono::{DateTime, Utc};
use dispatcharr_ingest_core::XmltvProgramme;
use dispatcharr_store::{EpgHistoryPolicy, Store};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

#[derive(Deserialize)]
struct Fixture {
    cases: Vec<Case>,
}
#[derive(Deserialize)]
struct Case {
    name: String,
    source_id: i64,
    epg_ids: Vec<i64>,
    mapped_ids: Vec<i64>,
    now: DateTime<Utc>,
    policy: EpgHistoryPolicy,
    before: Vec<Value>,
    staged: Vec<Input>,
    expected: Vec<Value>,
    deleted: Option<u64>,
    error: Option<String>,
    swap_batch_size: usize,
    current_mapped: Vec<i64>,
    lookup: std::collections::HashMap<String, i64>,
}
#[derive(Deserialize)]
struct Input {
    epg_id: i64,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    title: String,
    sub_title: Option<String>,
    description: Option<String>,
    tvg_id: String,
    custom_properties: Option<Value>,
}
fn input(row: &Input) -> XmltvProgramme {
    XmltvProgramme {
        epg_id: row.epg_id,
        start_time: row.start_time,
        end_time: row.end_time,
        title: row.title.clone(),
        sub_title: row.sub_title.clone(),
        description: row.description.clone(),
        tvg_id: row.tvg_id.clone(),
        custom_properties: row.custom_properties.clone(),
    }
}
fn normalize(rows: Vec<Value>, before: &[Value]) -> Vec<String> {
    let old: HashSet<_> = before
        .iter()
        .map(|row| row["id"].as_i64().unwrap())
        .collect();
    let mut normalized: Vec<_> = rows
        .into_iter()
        .map(|mut row| {
            if !old.contains(&row["id"].as_i64().unwrap()) {
                row["id"] = Value::String("new".into());
            }
            row.to_string()
        })
        .collect();
    normalized.sort();
    normalized
}
async fn rows(pool: &sqlx::PgPool, ids: &[i64]) -> Vec<Value> {
    sqlx::query("SELECT id,epg_id,start_time,end_time,title,sub_title,description,tvg_id,program_id,custom_properties FROM epg_programdata WHERE epg_id=ANY($1)")
        .bind(ids).fetch_all(pool).await.unwrap().into_iter().map(|row|json!({
            "id":row.get::<i64,_>("id"),"epg_id":row.get::<i64,_>("epg_id"),
            "start_time":row.get::<DateTime<Utc>,_>("start_time").to_rfc3339(),"end_time":row.get::<DateTime<Utc>,_>("end_time").to_rfc3339(),
            "title":row.get::<String,_>("title"),"sub_title":row.get::<Option<String>,_>("sub_title"),
            "description":row.get::<Option<String>,_>("description"),"tvg_id":row.get::<Option<String>,_>("tvg_id"),
            "program_id":row.get::<Option<String>,_>("program_id"),"custom_properties":row.get::<Option<Value>,_>("custom_properties"),
        })).collect()
}
struct DropWitness(Arc<AtomicBool>);
impl Drop for DropWitness {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[tokio::test]
#[ignore = "requires original disposable PostgreSQL EPG staging oracle"]
async fn original_atomic_swap_and_cancelled_waiter_keep_physical_guards() {
    assert_eq!(
        std::env::var("EPG_STAGING_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Fixture = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_STAGING_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let store = Store::connect(&url, 8).await.unwrap();
    for case in &fixture.cases {
        let session = store
            .begin_guide_staging(case.source_id, Arc::new(()))
            .await
            .unwrap();
        let (session, mapping) = session.mapping_snapshot().await.unwrap();
        let mut actual_ids = mapping.epg_ids;
        actual_ids.sort();
        assert_eq!(actual_ids, case.current_mapped, "{} mapping IDs", case.name);
        assert_eq!(
            mapping.by_tvg_id, case.lookup,
            "{} nonempty identifier lookup",
            case.name
        );
        let session = session
            .push(case.staged.iter().map(input).collect())
            .await
            .unwrap();
        let mut trigger_connection = None;
        if case.error.is_some() {
            let mut connection = pool.acquire().await.unwrap();
            sqlx::query("CREATE OR REPLACE FUNCTION public.rust_native_guide_swap_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.title='synthetic-swap-failure' THEN RAISE EXCEPTION 'synthetic swap failure'; END IF; RETURN NEW; END $$").execute(&mut *connection).await.unwrap();
            sqlx::query("CREATE TRIGGER rust_native_guide_swap_fail BEFORE INSERT ON epg_programdata FOR EACH ROW EXECUTE FUNCTION public.rust_native_guide_swap_fail()").execute(&mut *connection).await.unwrap();
            trigger_connection = Some(connection);
        }
        let outcome = session
            .swap(
                case.mapped_ids.clone(),
                case.now,
                case.policy,
                case.swap_batch_size,
            )
            .await;
        if let Some(mut connection) = trigger_connection {
            sqlx::query("DROP TRIGGER rust_native_guide_swap_fail ON epg_programdata")
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query("DROP FUNCTION public.rust_native_guide_swap_fail()")
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        match case.deleted {
            Some(deleted) => assert_eq!(outcome.unwrap().deleted, deleted, "{}", case.name),
            None => assert!(outcome.is_err(), "{} must roll back", case.name),
        }
        let actual = rows(&pool, &case.epg_ids).await;
        assert_eq!(
            normalize(actual, &case.before),
            normalize(case.expected.clone(), &case.before),
            "{}",
            case.name
        );
    }
    // A table-locked swap retains its actual job guard after its caller drops.
    let case = &fixture.cases[0];
    let released = Arc::new(AtomicBool::new(false));
    let session = store
        .begin_guide_staging(case.source_id, Arc::new(DropWitness(released.clone())))
        .await
        .unwrap();
    let session = session
        .push(case.staged.iter().map(input).collect())
        .await
        .unwrap();
    let pid = session.backend_pid();
    let mut lock = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE epg_programdata IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let mapped = case.mapped_ids.clone();
    let now = case.now;
    let policy = case.policy;
    let waiter = tokio::spawn(async move { session.swap(mapped, now, policy, 5000).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT COALESCE(wait_event_type='Lock',false) FROM pg_stat_activity WHERE pid=$1",
            )
            .bind(pid)
            .fetch_one(&pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    waiter.abort();
    let _ = waiter.await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !released.load(Ordering::Acquire),
        "physical SQL still holds the job guard"
    );
    lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !released.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

/// Cancellation while PostgreSQL holds the command must roll back the whole
/// swap, while the detached waiter still owns its physical guard and session.
#[tokio::test]
#[ignore = "requires original disposable PostgreSQL EPG staging oracle"]
async fn cancelled_blocked_swap_rolls_back_without_releasing_physical_guard_early() {
    assert_eq!(
        std::env::var("EPG_STAGING_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Fixture = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_STAGING_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let case = &fixture.cases[0];
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let store = Store::connect(&url, 8).await.unwrap();
    // Clone actual schema rows with fresh identities, so this test cannot race
    // the original fifteen cases even when Cargo schedules both tests together.
    let source:i64=sqlx::query_scalar("INSERT INTO epg_epgsource SELECT (jsonb_populate_record(NULL::epg_epgsource,to_jsonb(s)||jsonb_build_object('id',nextval(pg_get_serial_sequence('epg_epgsource','id')),'name','rust-cancelled-guide-'||txid_current()::text))).* FROM epg_epgsource s WHERE id=$1 RETURNING id")
        .bind(case.source_id).fetch_one(&pool).await.unwrap();
    let epg:i64=sqlx::query_scalar("INSERT INTO epg_epgdata SELECT (jsonb_populate_record(NULL::epg_epgdata,to_jsonb(e)||jsonb_build_object('id',nextval(pg_get_serial_sequence('epg_epgdata','id')),'epg_source_id',$2::bigint))).* FROM epg_epgdata e WHERE id=$1 RETURNING id")
        .bind(case.epg_ids[0]).bind(source).fetch_one(&pool).await.unwrap();
    sqlx::query("INSERT INTO epg_programdata SELECT (jsonb_populate_record(NULL::epg_programdata,to_jsonb(p)||jsonb_build_object('id',nextval(pg_get_serial_sequence('epg_programdata','id')),'epg_id',$2::bigint))).* FROM epg_programdata p WHERE epg_id=$1 LIMIT 1")
        .bind(case.epg_ids[0]).bind(epg).execute(&pool).await.unwrap();
    let before = rows(&pool, &[epg]).await;
    assert!(!before.is_empty());
    let dropped = Arc::new(AtomicBool::new(false));
    let witness: Arc<dyn Send + Sync> = Arc::new(DropWitness(dropped.clone()));
    let session = store.begin_guide_staging(source, witness).await.unwrap();
    let pid = session.backend_pid();
    let mut row = input(&case.staged[0]);
    row.epg_id = epg;
    let session = session.push(vec![row]).await.unwrap();
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE epg_programdata IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let cancelled = Arc::new(AtomicBool::new(false));
    let flag = cancelled.clone();
    let now = case.now;
    let policy = case.policy;
    let waiter = tokio::spawn(async move {
        session
            .swap_cancellable(vec![epg], now, policy, 100, flag)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5),async{
        loop{
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')")
                .bind(pid).fetch_one(&pool).await.unwrap();
            if waiting{break;}tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.unwrap();
    cancelled.store(true, Ordering::Release);
    waiter.abort();
    let _ = waiter.await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !dropped.load(Ordering::Acquire),
        "guard dropped before physical SQL reply"
    );
    blocker.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !dropped.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        rows(&pool, &[epg]).await,
        before,
        "cancelled swap changed live guide rows"
    );
    sqlx::query("DELETE FROM epg_programdata WHERE epg_id=$1")
        .bind(epg)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM epg_epgdata WHERE id=$1")
        .bind(epg)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM epg_epgsource WHERE id=$1")
        .bind(source)
        .execute(&pool)
        .await
        .unwrap();
}
