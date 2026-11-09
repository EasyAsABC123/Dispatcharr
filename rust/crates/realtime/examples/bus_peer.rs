//! Isolated protocol peer for actual original Python ChannelsRedis comparisons.
use dispatcharr_realtime::{BusConfig, RealtimeBus};
use serde_json::{json, Value};
use std::{
    io::{self, BufRead, Write},
    time::Duration,
};
#[tokio::main]
async fn main() {
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let bus = RealtimeBus::connect(&url, BusConfig::default())
        .await
        .unwrap();
    let member = bus.reserve_member().unwrap();
    member.join().await.unwrap();
    println!("{}", json!({"ready":true,"channel":member.id()}));
    io::stdout().flush().unwrap();
    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        assert!(line.len() <= 1024 * 1024);
        let request: Value = serde_json::from_str(&line).unwrap();
        let response = match request["op"].as_str().unwrap() {
            "send" => {
                bus.publish("updates", request["message"].clone())
                    .await
                    .unwrap();
                json!({"sent":true})
            }
            "receive" => {
                let timeout = request["timeout_ms"]
                    .as_u64()
                    .unwrap_or(3000)
                    .clamp(1, 5000);
                match tokio::time::timeout(Duration::from_millis(timeout),async {
                    loop {
                        if let Some(incoming)=bus.pop().await.unwrap() {
                            let response=json!({"message":&incoming.message,"recipient_count":incoming.recipients.len()});
                            incoming.ack().await.unwrap();break response;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }).await {Ok(response)=>response,Err(_)=>json!({"timeout":true})}
            }
            "close" => {
                member.release().await;
                println!("{}", json!({"closed":true}));
                io::stdout().flush().unwrap();
                break;
            }
            _ => panic!("unknown fixture operation"),
        };
        println!("{response}");
        io::stdout().flush().unwrap();
    }
    member.release().await;
}
