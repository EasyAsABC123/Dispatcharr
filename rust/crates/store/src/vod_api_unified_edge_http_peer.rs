//! UNCOMPILED during quiet. Fresh actual Source40 HTTP vs merged native peer;
//! SourceFactory40 is separate authority, never relabeled realHTTP.
use crate::{VodReads, VodReadLimits as ReadLimits};
use serde_json::{json,Value};
use sha2::{Digest,Sha256};
use sqlx::{postgres::PgPoolOptions,Row};
use std::{net::SocketAddr,time::Duration};
#[tokio::test]
#[ignore="Requires fresh actual Source40 edges and owned PostgreSQL/Redis"]
async fn actual_original_000_json_edges_repr_digitlimit_userprops(){
    assert_eq!(std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),Ok("playback-fixture-only"));
    let evidence=std::path::PathBuf::from(std::env::var("VOD_NATIVE_EVIDENCE").unwrap());
    let source:Value=serde_json::from_slice(&std::fs::read(evidence.join("unified-edges-source.json")).unwrap()).unwrap();assert_eq!(source["mode"],"http");assert_eq!(source["owned_rows_restored"],true);
    let base:Value=serde_json::from_slice(&std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap()).unwrap();
    let url=std::env::var("DATABASE_URL").unwrap();assert!(url.contains("playback-fixture-only"));let pool=PgPoolOptions::new().max_connections(8).connect(&url).await.unwrap();
    let redis=std::env::var("REDIS_TEST_URL").unwrap();assert!(redis.contains("playback-fixture-only"));
    let auth=crate::AuthService::new(crate::Store::from_pool(pool.clone()),crate::AuthConfig::new("playback-fixture-only".into()).unwrap()).with_notification_cache(crate::NotificationCache::connect(&redis).await.unwrap());
    let gateway=VodReads::new(auth,ReadLimits::default()).unwrap();let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();let router=gateway.unified_router();
    let server=tokio::spawn(async move{axum::serve(listener,router.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();});
    let client=reqwest::Client::builder().no_proxy().http1_only().timeout(Duration::from_secs(8)).build().unwrap();
    let user=sqlx::query("SELECT id::bigint AS id,custom_properties FROM accounts_user WHERE username='rust-probe-vod-rest-standard'").fetch_one(&pool).await.unwrap();let id:i64=user.get("id");let saved:Option<Value>=user.get("custom_properties");
    let token=base["cases"].as_array().unwrap().iter().find(|row|row["role"]=="standard"&&row["path"]=="/api/vod/movies/"&&row["method"]=="GET").unwrap()["headers"]["HTTP_AUTHORIZATION"].as_str().unwrap();
    let supplement:Value=serde_json::from_slice(&std::fs::read(evidence.join("integer-api-supplement-source.json")).unwrap()).unwrap();assert_eq!(supplement["mode"],"http");assert_eq!(supplement["owned_rows_restored"],true);
    let mut cases=source["records"].as_array().unwrap().clone();assert_eq!(cases.len(),40);assert_eq!(supplement["records"].as_array().unwrap().len(),37);
    for row in supplement["records"].as_array().unwrap(){
        let spec=&row["case"];let mut query=url::form_urlencoded::Serializer::new(String::new());for pair in spec["query_pairs"].as_array().unwrap(){query.append_pair(pair[0].as_str().unwrap(),pair[1].as_str().unwrap());}
        let mut label=json!({"supplement_case":spec["label"]});if let Some(props)=spec.get("properties"){label["user_properties"]=props.clone();}
        cases.push(json!({"path":format!("/api/vod/all/?{}",query.finish()),"method":spec["method"],"label":label,"expected":row["expected"]}));
    }
    let mut records=Vec::new();let mut differences=Vec::new();
    for(index,case)in cases.iter().enumerate(){
        let label=&case["label"];let props=if let Some(value)=label.get("user_properties").or_else(||label.get("user_properties_before_badpage")){if value.is_null(){None}else{Some(value.clone())}}else if label["both_disabled_earlyreturn"]==true{Some(json!({"vod_movies_enabled":false,"vod_series_enabled":false}))}else{saved.clone()};
        sqlx::query("UPDATE accounts_user SET custom_properties=$1 WHERE id=$2").bind(props).bind(id).execute(&pool).await.unwrap();
        let response=client.request(reqwest::Method::from_bytes(case["method"].as_str().unwrap().as_bytes()).unwrap(),format!("http://{address}{}",case["path"].as_str().unwrap())).header("Host","localhost").header("Accept","application/json").header("Authorization",token).send().await.unwrap();let status=response.status().as_u16();let headers=response.headers().clone();let body=response.bytes().await.unwrap();let expected=&case["expected"];
        let same_headers=["Allow","Vary","Content-Length","WWW-Authenticate"].iter().all(|key|headers.get(*key).and_then(|v|v.to_str().ok())==expected["headers"][*key].as_str());
        let same=status==expected["status"].as_u64().unwrap()as u16&&headers.get("content-type").and_then(|v|v.to_str().ok())==expected["content_type"].as_str()&&format!("{:x}",Sha256::digest(&body))==expected["body_sha256"].as_str().unwrap()&&same_headers;
        let item=json!({"index":index,"status":status,"method":case["method"],"body_bytes":body.len(),"body_sha256":format!("{:x}",Sha256::digest(&body)),"same_status_body_MIME_headers":same});if !same{differences.push(item.clone());}records.push(item);
    }
    sqlx::query("UPDATE accounts_user SET custom_properties=$1 WHERE id=$2").bind(&saved).bind(id).execute(&pool).await.unwrap();let restored:Option<Value>=sqlx::query_scalar("SELECT custom_properties FROM accounts_user WHERE id=$1").bind(id).fetch_one(&pool).await.unwrap();assert_eq!(saved,restored);
    assert!(gateway.shutdown(Duration::from_secs(2)).await);server.abort();pool.close().await;
    let result=json!({"passed":differences.is_empty(),"scope":"Actual Original40+37HTTP vs nativeJSON/context beforeview errors; oldFactory40/source-only SQL authority remains distinct. Fresh fixture, exactrawbody/MIME/status/4headers, no normalizing.","primary_Source40":40,"supplemental_Source37":37,"compared":records.len(),"records":records,"differences":differences,"owned_User_properties_restored_including_SQLNULL":true});std::fs::write(evidence.join("unified-edges-native.json"),serde_json::to_vec_pretty(&result).unwrap()).unwrap();assert_eq!(result["compared"],77);assert_eq!(result["passed"],true,"Sourceedge mismatch");
}
