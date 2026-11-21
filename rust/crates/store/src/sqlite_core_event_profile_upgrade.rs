//! Pinned Original Core 0017–0019 on an owned historical SQLite copy.
//! Original Dispatcharr and Django contributors retain full credit.
use super::*;

pub(super) const STEPS: [(&str, &str); 3] = [
    ("core", "0017_systemevent"),
    ("core", "0018_alter_systemevent_event_type"),
    ("core", "0019_add_vlc_stream_profile"),
];
const EVENT_DDL: &str = include_str!("../datasets/sqlite-core-systemevent.sql");
pub(super) const VLC_ARGS: &str = "-vv -I dummy --no-video-title-show --play-and-exit --http-user-agent {userAgent} {streamUrl} --sout #standard{access=file,mux=ts,dst=-}";

pub(super) async fn run(path: &Path) -> Result<usize, Failure> {
    if !std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_file()) {
        return Err(Failure::OutsideKnownState);
    }
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(1));
    let mut connection = SqliteConnection::connect_with(&options).await?;
    let result = async {
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        match execute(&mut tx).await {
            Ok(applied) => {
                tx.commit().await?;
                Ok(applied)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }
    .await;
    connection.close().await?;
    result
}

async fn check_event_layout(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    present: bool,
) -> Result<(), Failure> {
    let observed = sqlx::query_scalar::<_, String>(
        "SELECT sql FROM sqlite_schema WHERE tbl_name='core_systemevent' ORDER BY type DESC,name LIMIT 7",
    ).fetch_all(&mut **tx).await?;
    if present {
        if !observed.iter().map(String::as_str).eq(EVENT_DDL.lines()) {
            return Err(Failure::OutsideKnownState);
        }
    } else if !observed.is_empty() {
        return Err(Failure::OutsideKnownState);
    }
    Ok(())
}

async fn execute(tx: &mut Transaction<'_, sqlx::Sqlite>) -> Result<usize, Failure> {
    let names = sqlx::query_as::<_, (String, String)>(
        "SELECT CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
                CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
         FROM django_migrations ORDER BY app,name LIMIT 141",
    )
    .fetch_all(&mut **tx)
    .await?;
    let prefix = names
        .len()
        .checked_sub(137)
        .filter(|n| *n <= STEPS.len())
        .ok_or(Failure::OutsideKnownState)?;
    let mut expected = HISTORICAL.to_vec();
    expected.extend([KEY, MANAGER]);
    expected.extend(app_data::STEPS);
    expected.extend(&STEPS[..prefix]);
    expected.sort_unstable();
    if !names
        .iter()
        .map(|(app, name)| (app.as_str(), name.as_str()))
        .eq(expected)
    {
        return Err(Failure::OutsideKnownState);
    }
    check_event_layout(tx, prefix > 0).await?;
    for (index, step) in STEPS.iter().copied().enumerate().skip(prefix) {
        match index {
            0 => {
                for statement in EVENT_DDL.lines() {
                    sqlx::query(statement).execute(&mut **tx).await?;
                }
            }
            // 0018 changes choices on the same indexed varchar(50); there
            // is no SQL storage change or constraint on the display choices.
            1 => {}
            2 => {
                // UserAgent.objects.first() orders by primary key for this
                // unordered historical model. PK 1 still wins when present.
                sqlx::query(
                    "INSERT INTO core_streamprofile(name,command,parameters,is_active,locked,user_agent_id)
                     SELECT 'VLC','cvlc',?,1,1,id FROM core_useragent
                     WHERE NOT EXISTS(SELECT 1 FROM core_streamprofile WHERE name='VLC')
                     ORDER BY (id=1) DESC,id LIMIT 1",
                ).bind(VLC_ARGS).execute(&mut **tx).await?;
            }
            _ => unreachable!(),
        }
        record(tx, step).await?;
    }
    check_event_layout(tx, true).await?;
    if sqlx::query("PRAGMA foreign_key_check")
        .fetch_optional(&mut **tx)
        .await?
        .is_some()
    {
        return Err(Failure::OutsideKnownState);
    }
    Ok(STEPS.len() - prefix)
}

#[cfg(test)]
#[path = "sqlite_core_event_profile_contract.rs"]
mod contract;
