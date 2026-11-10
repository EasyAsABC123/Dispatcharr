//! Disposable Source interoperability entrypoint, never application dispatch.
//! Original Dispatcharr authors retain application credit and AGPL licensing.
use dispatcharr_job_transport::{
    protocol::Task,
    transport::{Config, Jobs},
};
use serde_json::{json, Value};
use std::{
    io::{self, Read},
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;
#[tokio::main]
async fn main() {
    if std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref() != Ok("playback-fixture-only") {
        eprintln!("Owned fixture required");
        std::process::exit(2);
    }
    let mut input = String::new();
    io::stdin().take(16385).read_to_string(&mut input).unwrap();
    if input.len() > 16384 {
        std::process::exit(2);
    }
    let value: Value = serde_json::from_str(&input).unwrap();
    let database = std::env::var("POSTGRES_DB").unwrap();
    let jobs = Jobs::connect_fixture(
        value["broker_url"].as_str().unwrap(),
        value["result_url"].as_str().unwrap(),
        &database,
        Config::default(),
    )
    .await
    .unwrap();
    let caller: Arc<dyn Send + Sync> = Arc::new(());
    let output = match value["action"].as_str().unwrap() {
        "publish" => {
            let task = match value["task"].as_str().unwrap() {
                "create" => Task::CreateBackup,
                "restore" => Task::RestoreBackup {
                    filename: value["filename"].as_str().unwrap().to_owned(),
                },
                _ => std::process::exit(2),
            };
            let id = Uuid::new_v4();
            match jobs.publish(&task, id, caller).await {
                Ok(v) => {
                    json!({"published":true,"task_id":v.task_id.to_string(),"queue_length":v.queue_length})
                }
                Err(error) => {
                    eprintln!("Publish error: {error:?}");
                    std::process::exit(1);
                }
            }
        }
        "read" => {
            let id = Uuid::parse_str(value["task_id"].as_str().unwrap()).unwrap();
            match jobs.read(id, caller).await {
                Ok(Some(meta)) => {
                    json!({"missing":false,"state":format!("{:?}",meta.state),"result":meta.result,"tagged_values":meta.tagged_values.len(),"exception_class":meta.exception.map(|v|v.class)})
                }
                Ok(None) => json!({"missing":true}),
                Err(error) => {
                    eprintln!("Read error: {error:?}");
                    std::process::exit(1);
                }
            }
        }
        _ => std::process::exit(2),
    };
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    println!("{output}");
}
