//! Owned Redis result-reader probe; no HTTP projection or exception imports.
//! Original Dispatcharr/Celery/Kombu authors retain application/framework credit.
use dispatcharr_job_transport::transport::{Config, Jobs};
use serde_json::{json, Value};
use std::{
    io::{self, Read},
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;

#[tokio::main]
async fn main() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let mut input = Vec::new();
    io::stdin().take(32769).read_to_end(&mut input).unwrap();
    assert!(input.len() <= 32768);
    let input: Value = serde_json::from_slice(&input).unwrap();
    let id = Uuid::parse_str(input["task_id"].as_str().unwrap()).unwrap();
    let database = std::env::var("POSTGRES_DB").unwrap();
    let jobs = Jobs::connect_fixture(
        "redis://:playback-fixture-only@127.0.0.1:6379/8",
        "redis://:playback-fixture-only@127.0.0.1:6379/9",
        &database,
        Config {
            operations: 1,
            ..Config::default()
        },
    )
    .await
    .unwrap();
    let value = match jobs.read(id, Arc::new(())).await {
        Ok(Some(metadata)) => {
            json!({"ok":true,"state":format!("{:?}",metadata.state),"task_id":metadata.task_id.to_string(),"result":metadata.result,"date_done":metadata.date_done,"traceback":metadata.traceback,"exception":metadata.exception.map(|value|json!({"class":value.class,"arguments":value.arguments}))})
        }
        Ok(None) => json!({"ok":true,"state":"Pending","task_id":id.to_string()}),
        Err(error) => json!({"ok":false,"error":format!("{error:?}"),"task_id":id.to_string()}),
    };
    assert_eq!(jobs.available_slots(), 1);
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    println!("{value}");
}
