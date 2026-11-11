//! Isolated fixture peer; external native helper uses the existing app scanner/bus.
//! Original Dispatcharr authors retain application/algorithm credit and AGPL.
use crate::{
    stats_worker::{Error, Source, StatsWorker},
    transport::{Config, Jobs},
};
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use serde_json::{json, Value};
use std::{
    io::{self, Read, Write},
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;
struct Dependencies;
async fn helper(input: Value, caller: Arc<dyn Send + Sync>) -> Result<Value, ()> {
    tokio::task::spawn_blocking(move || {
        let _caller = caller;
        let mut child = Command::new("/evidence/native-stats-dependencies")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|_| ())?;
        let written = child
            .stdin
            .take()
            .ok_or(())?
            .write_all(&serde_json::to_vec(&input).map_err(|_| ())?);
        let mut output = Vec::new();
        let mut stdout = child.stdout.take().ok_or(())?;
        let read = stdout
            .by_ref()
            .take(4 * 1024 * 1024 + 1)
            .read_to_end(&mut output);
        // Even a bounded-output failure must reap the real dependency process.
        // Discard excess bytes without retaining them; no detached child work.
        if output.len() > 4 * 1024 * 1024 {
            let _ = io::copy(&mut stdout, &mut io::sink());
        }
        drop(stdout);
        let status = child.wait().map_err(|_| ())?;
        if written.is_err() || read.is_err() || output.len() > 4 * 1024 * 1024 || !status.success()
        {
            return Err(());
        }
        let text = std::str::from_utf8(&output).map_err(|_| ())?;
        let line = text
            .lines()
            .find_map(|line| line.strip_prefix("NATIVE_STATS_DEPENDENCY_JSON="))
            .ok_or(())?;
        serde_json::from_str(line).map_err(|_| ())
    })
    .await
    .map_err(|_| ())?
}
impl Source for Dependencies {
    fn build(&self, caller: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<Value, Error>> {
        Box::pin(async move {
            let result = helper(json!({"action":"build"}), caller)
                .await
                .map_err(|_| Error::Build)?;
            if result["ok"] == true {
                Ok(result["value"].clone())
            } else {
                Err(Error::Build)
            }
        })
    }
}
impl UpdateEventSink for Dependencies {
    fn send<'a>(
        &'a self,
        group: &'a str,
        event: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        self.send_retained(group, event, Arc::new(()))
    }
    fn send_retained<'a>(
        &'a self,
        group: &'a str,
        event: Value,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            if group != "updates" {
                return Err(UpdateEventError::InvalidMessage);
            }
            let result = helper(json!({"action":"send","event":event}), caller)
                .await
                .map_err(|_| UpdateEventError::InvalidMessage)?;
            match (result["ok"].as_bool(), result["kind"].as_str()) {
                (Some(true), _) => Ok(()),
                (Some(false), Some("Unavailable")) => Err(UpdateEventError::Unavailable),
                (Some(false), Some("Capacity")) => Err(UpdateEventError::Capacity),
                _ => Err(UpdateEventError::InvalidMessage),
            }
        })
    }
}
#[tokio::test]
#[ignore = "Actual copied DB, broker8/result9, native app scanner and ChannelsRedis dependency helper"]
async fn native_stats_peer() {
    let mut raw = String::new();
    io::stdin().take(16385).read_to_string(&mut raw).unwrap();
    assert!(raw.len() <= 16384);
    let input: Value = serde_json::from_str(&raw).unwrap();
    let database = std::env::var("POSTGRES_DB").unwrap();
    let jobs = Jobs::connect_fixture(
        input["broker_url"].as_str().unwrap(),
        input["result_url"].as_str().unwrap(),
        &database,
        Config {
            operations: 1,
            command_timeout: Duration::from_secs(2),
            max_queued_jobs: 64,
        },
    )
    .await
    .unwrap();
    let id = Uuid::parse_str(input["task_id"].as_str().unwrap()).unwrap();
    let worker = StatsWorker::new_fixture(
        jobs.clone(),
        Arc::new(Dependencies),
        Arc::new(Dependencies),
        3 * 1024 * 1024,
    )
    .unwrap();
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let result = match input["action"].as_str().unwrap() {
        "publish" => {
            json!({"published":jobs.publish_stats_fixture(id, caller).await.unwrap().task_id.to_string()})
        }
        "run" => json!({"outcome":format!("{:?}",worker.run_once(caller).await)}),
        _ => panic!("Unknown fixture action"),
    };
    assert!(worker.shutdown(Duration::from_secs(5)).await);
    assert_eq!(worker.available_slots(), 1);
    jobs.shutdown(Duration::from_secs(5)).await.unwrap();
    assert!(weak.upgrade().is_none());
    println!(
        "NATIVE_STATS_TASK_JSON={}",
        json!({"result":result,"owner_released":true,"slots":jobs.available_slots()})
    );
}
