//! Compare actual global Redis metadata with original Python ChannelStatus.
//! Original Dispatcharr authors retain full application credit.
use dispatcharr_clients::{ClientConfig,ClientStore};
use dispatcharr_stream_http::project_status_at;
use serde_json::{json,Value};
use std::io::{self,Read};
#[tokio::main]
async fn main() {
    let mut input=String::new();io::stdin().take(65537).read_to_string(&mut input).unwrap();assert!(input.len()<=65536);
    let request:Value=serde_json::from_str(&input).unwrap();let at=request["at"].as_f64().unwrap();
    let store=ClientStore::connect(&std::env::var("REDIS_TEST_URL").unwrap(),ClientConfig::default()).await.unwrap();
    let mut cursor=0;let mut channels=Vec::new();let mut pages=0;
    loop {
        let page=store.channel_statistics_page(cursor).await.unwrap();cursor=page.cursor;pages+=1;assert!(pages<20000);
        for row in page.records {
            let mut projected=project_status_at(&row.channel_id,&row.metadata,row.index,row.clients,false,at);
            projected["client_count"]=json!(row.client_count);channels.push(projected);
        }
        if cursor==0{break}
    }
    println!("{}",json!({"count":channels.len(),"channels":channels}));store.shutdown().await;
}
