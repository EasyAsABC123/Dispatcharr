//! Real production SQL predicate/order regression. Original authors retain credit.
use super::*;
async fn query(c: &mut PgConnection, now: DateTime<Utc>) -> Vec<sqlx::postgres::PgRow> {
    let f = QueryFilters::new(now).unwrap();
    sqlx::query(LOAD_QUERY)
        .bind(f.cutoff)
        .bind(&f.hours)
        .bind(&f.numeric_hours)
        .bind(f.zones)
        .bind(f.offsets)
        .fetch_all(c)
        .await
        .unwrap()
}
fn ids(rows: &[sqlx::postgres::PgRow]) -> Vec<i64> {
    let mut ids = rows
        .iter()
        .filter_map(|r| r.try_get::<Option<String>, _>("raw").unwrap())
        .map(|v| {
            serde_json::from_str::<Value>(&v).unwrap()["id"]
                .as_i64()
                .unwrap()
        })
        .collect::<Vec<_>>();
    ids.sort();
    ids
}
#[tokio::test]
#[ignore = "Actual owned loopback PostgreSQL query; no fixture is started by this test. Root must grant its shared fixture/Cargo reservation."]
async fn source_predicates_precede_budget_in_actual_production_query() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    assert!(database.starts_with("rust_probe_") || database.starts_with("normal_dvr_"));
    assert!(
        database.len() <= 63
            && database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    );
    let url = std::env::var("JOB_DATABASE_URL").unwrap();
    let options: sqlx::postgres::PgConnectOptions = url.parse().unwrap();
    assert_eq!(options.get_database(), Some(database.as_str()));
    assert!(matches!(
        options.get_host(),
        "127.0.0.1" | "localhost" | "::1" | "/var/run/postgresql"
    ));
    let mut c = PgConnection::connect_with(&options).await.unwrap();
    // Shadow only unqualified model tables inside one transaction/session. No
    // persistent Source model, sequence, scheduling marker or default is changed.
    sqlx::query("BEGIN").execute(&mut c).await.unwrap();
    for statement in [
 "SET LOCAL search_path=pg_temp,pg_catalog",
 "CREATE TEMP TABLE django_celery_beat_periodictask(id bigint,name text,enabled boolean,interval_id bigint,crontab_id bigint,clocked_id bigint,payload text)",
 "CREATE TEMP TABLE django_celery_beat_clockedschedule(id bigint,clocked_time timestamptz)",
 "CREATE TEMP TABLE django_celery_beat_crontabschedule(id bigint,hour text,timezone text)",
 "CREATE TEMP TABLE django_celery_beat_intervalschedule(id bigint,every bigint,period text)",
 "INSERT INTO django_celery_beat_intervalschedule VALUES(1,60,'seconds')",
 ]{sqlx::query(statement).execute(&mut c).await.unwrap();}
    let now = DateTime::parse_from_rfc3339("2026-11-14T12:00:00+00:00")
        .unwrap()
        .to_utc();
    // >256 far-future rows are inserted FIRST and even contain >1MiB payloads.
    // They must neither consume capacity nor cross into the bounded reply.
    sqlx::query("INSERT INTO django_celery_beat_clockedschedule SELECT n,$1::timestamptz+interval '1 day' FROM generate_series(1,1026) n").bind(now).execute(&mut c).await.unwrap();
    sqlx::query("INSERT INTO django_celery_beat_periodictask SELECT n,'excluded-clock-'||n,true,NULL,NULL,n,repeat('x',2048) FROM generate_series(1,1026) n").execute(&mut c).await.unwrap();
    // Calendar Source include window is10..14 plus4. Other single numeric hours
    // are excluded BEFORE the same budget. Non-numeric/list hours stay eligible.
    sqlx::query("INSERT INTO django_celery_beat_crontabschedule VALUES(1,'18','UTC'),(2,'04','UTC'),(3,'12,18','UTC'),(4,'000','UTC'),(5,'07','America/New_York'),(6,'00','Asia/Kathmandu'),(7,'23','Australia/Lord_Howe'),(8,'13','Europe/Paris'),(9,'23','UTC')").execute(&mut c).await.unwrap();
    sqlx::query("INSERT INTO django_celery_beat_periodictask SELECT 2000+n,'excluded-cron-'||n,true,NULL,1,NULL,repeat('y',2048) FROM generate_series(1,1026) n").execute(&mut c).await.unwrap();
    sqlx::query("INSERT INTO django_celery_beat_clockedschedule VALUES(2000,$1::timestamptz+interval '300 seconds'),(2001,$1::timestamptz+interval '300 seconds 1 microsecond')").bind(now).execute(&mut c).await.unwrap();
    sqlx::query("INSERT INTO django_celery_beat_periodictask VALUES(10001,'interval',true,1,NULL,NULL,''),(10002,'clock-boundary',true,NULL,NULL,2000,''),(10003,'clock-too-future',true,NULL,NULL,2001,''),(10004,'cleanup4',true,NULL,2,NULL,''),(10005,'complex-hour',true,NULL,3,NULL,''),(10006,'non-valid-numeric-format',true,NULL,4,NULL,''),(10007,'NY-offset',true,NULL,5,NULL,''),(10008,'Kathmandu-offset-excluded',true,NULL,6,NULL,''),(10009,'disabled',false,1,NULL,NULL,''),(10010,'multiFK-future-excluded',true,1,NULL,2001,''),(10011,'LordHowe-offset',true,NULL,7,NULL,''),(10012,'Paris-offset',true,NULL,8,NULL,''),(10013,'midnight-wrap',true,NULL,9,NULL,'')").execute(&mut c).await.unwrap();
    let rows = query(&mut c, now).await;
    assert_eq!(
        ids(&rows),
        vec![10001, 10002, 10004, 10005, 10006, 10007, 10011, 10012]
    );
    assert_eq!(rows[0].try_get::<i64, _>("rows").unwrap(), 8);
    assert!(rows[0].try_get::<i64, _>("bytes").unwrap() < 1048576);
    let midnight = DateTime::parse_from_rfc3339("2026-11-14T00:00:00+00:00")
        .unwrap()
        .to_utc();
    assert_eq!(
        ids(&query(&mut c, midnight).await),
        vec![10001, 10004, 10005, 10006, 10013]
    );
    // No silent due-schedule cap increase:257 eligible rows still return ONLY a
    // budget refusal (raw NULL), rather than an incomplete accepted schedule.
    sqlx::query("TRUNCATE django_celery_beat_periodictask")
        .execute(&mut c)
        .await
        .unwrap();
    sqlx::query("INSERT INTO django_celery_beat_periodictask SELECT n,'eligible-'||n,true,1,NULL,NULL,'' FROM generate_series(1,257) n").execute(&mut c).await.unwrap();
    let rows = query(&mut c, now).await;
    assert_eq!(rows[0].try_get::<i64, _>("rows").unwrap(), 257);
    assert!(ids(&rows).is_empty());
    sqlx::query("TRUNCATE django_celery_beat_periodictask")
        .execute(&mut c)
        .await
        .unwrap();
    sqlx::query("INSERT INTO django_celery_beat_periodictask VALUES(1,'eligible-large',true,1,NULL,NULL,repeat('z',1048577))").execute(&mut c).await.unwrap();
    let rows = query(&mut c, now).await;
    assert!(rows[0].try_get::<i64, _>("bytes").unwrap() > 1048576);
    assert!(ids(&rows).is_empty());
    sqlx::query("ROLLBACK").execute(&mut c).await.unwrap();
    c.close().await.unwrap();
}
