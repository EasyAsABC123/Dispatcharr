//! Bounded category HTTP adapter; app registration is an explicit separate hook.
//! Original Dispatcharr authors retain application credit and AGPL attribution.
use super::{vod_api_categories::{CategoryReads,CategoryRequest},vod_api_owned_read::{ReadRuntime,ReadError},vod_api_http::{HttpGate,pretty_document,json_indent},vod_api_read_component::HttpRead};
use axum::{Router,body::{Body,to_bytes},extract::ConnectInfo,http::{HeaderValue,Method},response::{Response,IntoResponse},routing::any};
use std::{sync::Arc,net::SocketAddr};
#[cfg(test)]
use std::time::Duration;
use tokio::sync::OwnedSemaphorePermit;
#[cfg(test)]
use super::vod_api_owned_read::ReadLimits;
struct Frame {data:bytes::Bytes,_slot:Arc<OwnedSemaphorePermit>}
impl AsRef<[u8]> for Frame {fn as_ref(&self)->&[u8]{self.data.as_ref()}}
#[derive(Clone)]
pub(super) struct CategoryHttp {reader:CategoryReads,gate:HttpGate}
impl CategoryHttp {
    #[cfg(test)]
    fn new(auth:crate::AuthService,limits:ReadLimits)->Result<Self,crate::StoreError> {
        Ok(Self {reader:CategoryReads::new(auth,limits).map_err(|_|crate::StoreError::Unsupported("Invalid category read limits"))?,gate:HttpGate::new(limits)?})
    }
    pub(super) fn shared(auth:crate::AuthService,runtime:ReadRuntime,gate:HttpGate)->Self {Self{reader:CategoryReads::from_runtime(auth,runtime),gate}}
    pub(super) fn router(&self)->Router {
        let mut router=Router::new();
        for(path,detail)in[("/api/vod/categories/",false),("/api/vod/categories/{id}/",true)] {
            let state=self.clone();router=router.route(path,any(move|ConnectInfo(peer):ConnectInfo<SocketAddr>,request:axum::extract::Request|{let state=state.clone();async move{state.respond(peer,detail,request).await}}));
        }
        router
    }
    #[cfg(test)]
    async fn shutdown(&self,budget:Duration)->bool{self.gate.slots.close();self.reader.shutdown(budget).await}
    async fn respond(&self,peer:SocketAddr,detail:bool,request:axum::extract::Request)->Response {
        let(parts,body)=request.into_parts();drop(body);let head=parts.method==Method::HEAD;
        let input=parts.headers.iter().fold(parts.uri.path_and_query().map_or(0,|value|value.as_str().len()).checked_add(1024),|total,(name,value)|total.and_then(|n|n.checked_add(name.as_str().len())).and_then(|n|n.checked_add(value.as_bytes().len())))
            .and_then(|n|parts.headers.capacity().checked_mul(128).and_then(|metadata|n.checked_add(metadata)));
        let acquired=match input{Some(input)=>self.gate.acquire(input).await,None=>None};
        let Some((slot,deadline))=acquired else {
            let data=bytes::Bytes::from_static(b"{\"detail\":\"VOD HTTP capacity unavailable.\"}");return representation(503,data,None,head);
        };
        let indent=json_indent(&parts.headers);let detail=detail.then(||parts.uri.path().trim_end_matches('/').rsplit('/').next().unwrap_or("").to_owned());
        let input=CategoryRequest{peer,headers:parts.headers,query:Vec::new(),detail:detail.clone()};
        let result=self.reader.dispatch(input,Some(HttpRead{method:parts.method,uri:parts.uri,raw_detail:detail,scope:Some(slot.clone()),deadline})).await;
        match result {
            Ok(data)=>representation(200,data,Some(slot),head),
            Err(ReadError::Source(crate::StoreError::Database(_)|crate::StoreError::UiPermissionFault)|ReadError::SourceUnhandledFault(_))=>source_server_error(slot,head),
            Err(error)=>{
                let error=match error{ReadError::Source(error)=>error,ReadError::Unsupported(reason)=>crate::StoreError::Unsupported(reason),_=>crate::StoreError::StatusBody(503,serde_json::json!({"detail":"VOD read capacity unavailable."}))};
                let response=error.into_response();let(mut response_parts,body)=response.into_parts();
                let data=match to_bytes(body,64*1024).await {
                    Ok(data)=>match indent{Some(indent)=>match pretty_document(&data,indent,super::vod_api_owned_read::Document::bounded(64*1024)){Ok(data)=>bytes::Bytes::from(data),Err(_)=>{response_parts.status=axum::http::StatusCode::SERVICE_UNAVAILABLE;bytes::Bytes::from_static(b"{\"detail\":\"VOD error representation exceeds capacity.\"}")}},None=>data},
                    Err(_)=>{response_parts.status=axum::http::StatusCode::SERVICE_UNAVAILABLE;bytes::Bytes::from_static(b"{\"detail\":\"VOD error representation exceeds capacity.\"}")},
                };
                let length=data.len();let body=if head{Body::empty()}else{Body::from(bytes::Bytes::from_owner(Frame{data,_slot:slot}))};
                let mut response=Response::from_parts(response_parts,body);response.headers_mut().insert("content-length",length.into());common_headers(&mut response);response
            }
        }
    }
}
// Original uncaught Django500 response observed on the actual category route.
// This is an error representation, independent of DRF JSON negotiation/indent.
pub(super) fn source_server_error(slot:Arc<OwnedSemaphorePermit>,head:bool)->Response {
    let data=bytes::Bytes::from_static(b"\n<!doctype html>\n<html lang=\"en\">\n<head>\n  <title>Server Error (500)</title>\n</head>\n<body>\n  <h1>Server Error (500)</h1><p></p>\n</body>\n</html>\n");
    let length=data.len();let body=if head{Body::empty()}else{Body::from(bytes::Bytes::from_owner(Frame{data,_slot:slot}))};let mut response=Response::new(body);*response.status_mut()=axum::http::StatusCode::INTERNAL_SERVER_ERROR;
    response.headers_mut().insert("content-type",HeaderValue::from_static("text/html; charset=utf-8"));response.headers_mut().insert("content-length",length.into());response.headers_mut().insert("vary",HeaderValue::from_static("origin"));response
}
fn common_headers(response:&mut Response){for(name,value)in[("allow","GET, HEAD, OPTIONS"),("vary","Accept, origin")] {response.headers_mut().insert(name,HeaderValue::from_static(value));}}
fn representation(status:u16,data:bytes::Bytes,slot:Option<Arc<OwnedSemaphorePermit>>,head:bool)->Response {
    let length=data.len();let body=if head{Body::empty()}else{match slot{Some(slot)=>Body::from(bytes::Bytes::from_owner(Frame{data,_slot:slot})),None=>Body::from(data)}};
    let mut response=Response::new(body);*response.status_mut()=axum::http::StatusCode::from_u16(status).expect("internal fixed status");response.headers_mut().insert("content-type",HeaderValue::from_static("application/json"));response.headers_mut().insert("content-length",length.into());common_headers(&mut response);response
}

#[cfg(test)]
pub(super) async fn compare_original_network() {
    use serde_json::{Value,json};use sha2::{Digest,Sha256};
    assert_eq!(std::env::var("VOD_CATEGORY_NATIVE").as_deref(),Ok("playback-fixture-only"));
    let source:Value=serde_json::from_slice(&std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap()).unwrap();
    let url=std::env::var("DATABASE_URL").unwrap();assert!(url.contains("playback-fixture-only"));let pool=sqlx::postgres::PgPoolOptions::new().max_connections(8).connect(&url).await.unwrap();
    let auth=crate::AuthService::new(crate::Store::from_pool(pool.clone()),crate::AuthConfig::new("playback-fixture-only".into()).unwrap()).with_notification_cache(crate::NotificationCache::connect(&std::env::var("REDIS_TEST_URL").unwrap()).await.unwrap());
    let gateway=CategoryHttp::new(auth.clone(),ReadLimits::default()).unwrap();let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();let router=gateway.router();let server=tokio::spawn(async move{axum::serve(listener,router.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();});
    let client=reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(8)).build().unwrap();let mut records=Vec::new();let mut differences=Vec::new();let mut exclusions=Vec::new();
    for(index,case)in source["cases"].as_array().unwrap().iter().enumerate(){
        let path=case["path"].as_str().unwrap();let bare=path.split('?').next().unwrap();if !bare.starts_with("/api/vod/categories/")||!bare.ends_with('/'){continue;}
        let mut request=client.request(case["method"].as_str().unwrap().parse().unwrap(),format!("http://{address}{path}"));
        for(name,value)in case["headers"].as_object().unwrap(){request=request.header(name.strip_prefix("HTTP_").unwrap_or(name).replace('_',"-"),value.as_str().unwrap());}
        let response=request.send().await.unwrap();let status=response.status().as_u16();let headers=response.headers().clone();let body=response.bytes().await.unwrap();
        if status==501{exclusions.push(json!({"index":index,"path":path,"source_status":case["expected"]["status"],"parity_pass":false,"reason":String::from_utf8_lossy(&body)}));continue;}
        let expected=&case["expected"];let hash=format!("{:x}",Sha256::digest(&body));let mut header_differences=Vec::new();
        for header in ["Content-Length","Allow","Vary","WWW-Authenticate"]{let actual=headers.get(header).and_then(|value|value.to_str().ok());let wanted=expected["selected_headers"][header].as_str();if actual!=wanted{header_differences.push(json!({"header":header,"source":wanted,"native":actual}));}}
        let mime=headers.get("content-type").and_then(|value|value.to_str().ok());
        if status!=expected["status"].as_u64().unwrap()as u16||hash!=expected["body_sha256"]||mime!=expected["content_type"].as_str()||!header_differences.is_empty(){differences.push(json!({"index":index,"path":path,"method":case["method"],"role":case["role"],"status":status,"source_status":expected["status"],"source_hash":expected["body_sha256"],"native_hash":hash,"native_body":String::from_utf8_lossy(&body),"header_differences":header_differences}));}
        records.push(json!({"index":index,"status":status,"body_sha256":hash,"bytes":body.len()}));
    }
    let reference:Value=serde_json::from_slice(&std::fs::read(std::env::var("VOD_CATEGORY_BURST").unwrap()).unwrap()).unwrap();assert_eq!(reference["passed"],true);
    let case=source["cases"].as_array().unwrap().iter().find(|case|case["path"]=="/api/vod/categories/"&&case["role"]=="standard"&&case["method"]=="GET").unwrap();
    let barrier=Arc::new(tokio::sync::Barrier::new(11));let epoch=tokio::time::Instant::now();let mut jobs=Vec::new();
    for index in 0..10 {let client=client.clone();let barrier=barrier.clone();let case=case.clone();jobs.push(tokio::spawn(async move {
        let mut request=client.get(format!("http://{address}/api/vod/categories/"));for(name,value)in case["headers"].as_object().unwrap(){request=request.header(name.strip_prefix("HTTP_").unwrap_or(name).replace('_',"-"),value.as_str().unwrap());}
        barrier.wait().await;let began=epoch.elapsed().as_secs_f64();let response=request.send().await.unwrap();let status=response.status().as_u16();let body=response.bytes().await.unwrap();json!({"index":index,"started_seconds":began,"status":status,"body_sha256":format!("{:x}",Sha256::digest(&body)),"bytes":body.len()})
    }));}
    barrier.wait().await;let mut burst=Vec::new();for job in jobs{burst.push(job.await.unwrap());}
    let burst_pass=burst.iter().all(|row|row["status"]==200&&row["body_sha256"]==reference["records"][0]["body_sha256"]);
    std::fs::write(std::path::PathBuf::from(std::env::var("VOD_NATIVE_EVIDENCE").unwrap()).join("category-http-burst.json"),serde_json::to_vec_pretty(&json!({"passed":burst_pass,"scope":"Actual original/native unstaggered ten CategoryList requests, default two active workers and bounded waiting; not performance","original":reference,"native_records":burst})).unwrap()).unwrap();
    // Error body frames keep the HTTP slot until the final detached clone.
    let response=gateway.respond("127.0.0.1:30000".parse().unwrap(),false,axum::extract::Request::builder().uri("/api/vod/categories/").header("authorization","Bearer invalid").body(Body::empty()).unwrap()).await;
    assert_eq!(response.status(),401);let body=to_bytes(response.into_body(),65536).await.unwrap();assert_eq!(gateway.gate.slots.available_permits(),1);let clone=body.clone();drop(body);assert_eq!(gateway.gate.slots.available_permits(),1);drop(clone);assert_eq!(gateway.gate.slots.available_permits(),2);
    // Redis pauses only this owned disposable fixture. Caller abort must not
    // release HTTP/read admission while cached authentication is in flight.
    let mut connection=redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap()).unwrap().get_multiplexed_async_connection().await.unwrap();
    redis::cmd("CLIENT").arg("PAUSE").arg(400).arg("ALL").query_async::<()>(&mut connection).await.unwrap();
    let waiting=gateway.clone();let cancel_case=case.clone();let task=tokio::spawn(async move {let mut request=axum::extract::Request::builder().uri("/api/vod/categories/");for(name,value)in cancel_case["headers"].as_object().unwrap(){request=request.header(name.strip_prefix("HTTP_").unwrap_or(name).replace('_',"-"),value.as_str().unwrap());}waiting.respond("127.0.0.1:30000".parse().unwrap(),false,request.body(Body::empty()).unwrap()).await});
    tokio::time::timeout(Duration::from_secs(1),async {while gateway.reader.runtime.physical_workers()!=1{tokio::time::sleep(Duration::from_millis(2)).await;}}).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;task.abort();let _=task.await;tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(gateway.reader.runtime.physical_workers(),1);assert_eq!(gateway.gate.slots.available_permits(),1);let held_memory=gateway.reader.runtime.retained_memory_bytes();assert!(held_memory>52*1024*1024);
    tokio::time::timeout(Duration::from_secs(3),async {while gateway.reader.runtime.physical_workers()!=0||gateway.gate.slots.available_permits()!=2{tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
    let short=CategoryHttp::new(auth,ReadLimits{logical_deadline:Duration::from_millis(100),..ReadLimits::default()}).unwrap();
    redis::cmd("CLIENT").arg("PAUSE").arg(400).arg("ALL").query_async::<()>(&mut connection).await.unwrap();
    let mut request=axum::extract::Request::builder().uri("/api/vod/categories/");for(name,value)in case["headers"].as_object().unwrap(){request=request.header(name.strip_prefix("HTTP_").unwrap_or(name).replace('_',"-"),value.as_str().unwrap());}
    let response=short.respond("127.0.0.1:30000".parse().unwrap(),false,request.body(Body::empty()).unwrap()).await;assert_eq!(response.status(),503);drop(to_bytes(response.into_body(),65536).await.unwrap());
    assert_eq!(short.reader.runtime.physical_workers(),1);assert_eq!(short.gate.slots.available_permits(),1);
    tokio::time::timeout(Duration::from_secs(3),async{while short.reader.runtime.physical_workers()!=0||short.gate.slots.available_permits()!=2{tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();assert!(short.shutdown(Duration::from_secs(1)).await);
    std::fs::write(std::path::PathBuf::from(std::env::var("VOD_NATIVE_EVIDENCE").unwrap()).join("category-http-ownership.json"),serde_json::to_vec_pretty(&json!({"passed":true,"actual_owned_redis_pause_ms":400,"cancelled_handler_retained_HTTP_and_read_admission":true,"logical_deadline_and_consumed_503_body_still_retain_physical_admission":true,"physical_work_memory_bytes":held_memory,"error_frame_final_clone_retains_slot":true,"physical_replies_drained":true,"scope":"Category adapter admission and cached auth cancellation; not allocatorwide or production acceptance"})).unwrap()).unwrap();
    let drained=gateway.shutdown(Duration::from_secs(5)).await;server.abort();let _=server.await;pool.close().await;
    let report=json!({"passed":differences.is_empty()&&records.len()>100&&drained&&burst_pass,"scope":"Actual captured original network versus independent test-only category HTTP adapter; global app security middleware/React/production registration excluded","records":records,"differences":differences,"explicit_unsupported_not_parity_pass":exclusions,"workers_drained":drained});
    std::fs::write(std::path::PathBuf::from(std::env::var("VOD_NATIVE_EVIDENCE").unwrap()).join("category-http.json"),serde_json::to_vec_pretty(&report).unwrap()).unwrap();assert_eq!(report["passed"],true,"category HTTP differences retained");
}

#[cfg(test)]
pub(super) async fn compare_initial_fault() {
    use serde_json::{Value,json};use sha2::{Digest,Sha256};
    assert_eq!(std::env::var("VOD_CATEGORY_NATIVE").as_deref(),Ok("playback-fixture-only"));
    let source:Value=serde_json::from_slice(&std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap()).unwrap();let reference:Value=serde_json::from_slice(&std::fs::read(std::env::var("VOD_CATEGORY_INITIAL_REFERENCE").unwrap()).unwrap()).unwrap();assert_eq!(reference["passed"],true);
    let url=std::env::var("DATABASE_URL").unwrap();assert!(url.contains("playback-fixture-only"));let pool=sqlx::postgres::PgPoolOptions::new().max_connections(8).connect(&url).await.unwrap();
    let saved:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY c.id),'[]'::jsonb) FROM vod_vodcategory c WHERE name='Uncategorized'").fetch_one(&pool).await.unwrap();assert_eq!(saved.as_array().unwrap().len(),2);
    let ids:Vec<i64>=saved.as_array().unwrap().iter().map(|row|row["id"].as_i64().unwrap()).collect();
    let links:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.id),'[]'::jsonb) FROM vod_m3uvodcategoryrelation r WHERE category_id=ANY($1)").bind(&ids).fetch_one(&pool).await.unwrap();
    let related:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM vod_m3umovierelation WHERE category_id=ANY($1))+(SELECT count(*) FROM vod_m3useriesrelation WHERE category_id=ANY($1))").bind(&ids).fetch_one(&pool).await.unwrap();assert_eq!(related,0);
    let existing:Option<i64>=sqlx::query_scalar("SELECT to_regprocedure('public.rust_vod_category_network_fail_series()')::oid::bigint").fetch_one(&pool).await.unwrap();assert!(existing.is_none());
    sqlx::query("DELETE FROM vod_m3uvodcategoryrelation WHERE category_id=ANY($1)").bind(&ids).execute(&pool).await.unwrap();sqlx::query("DELETE FROM vod_vodcategory WHERE id=ANY($1)").bind(&ids).execute(&pool).await.unwrap();
    sqlx::query("CREATE FUNCTION public.rust_vod_category_network_fail_series() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.name='Uncategorized' AND NEW.category_type='series' THEN RAISE EXCEPTION 'owned-vod-network-series-failure'; END IF; RETURN NEW; END$$").execute(&pool).await.unwrap();
    sqlx::query("CREATE TRIGGER rust_vod_category_network_fail_series BEFORE INSERT ON vod_vodcategory FOR EACH ROW EXECUTE FUNCTION public.rust_vod_category_network_fail_series()").execute(&pool).await.unwrap();
    let auth=crate::AuthService::new(crate::Store::from_pool(pool.clone()),crate::AuthConfig::new("playback-fixture-only".into()).unwrap()).with_notification_cache(crate::NotificationCache::connect(&std::env::var("REDIS_TEST_URL").unwrap()).await.unwrap());
    let gateway=CategoryHttp::new(auth,ReadLimits::default()).unwrap();let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();let router=gateway.router();let server=tokio::spawn(async move{axum::serve(listener,router.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();});
    let case=source["cases"].as_array().unwrap().iter().find(|case|case["path"]=="/api/vod/categories/"&&case["role"]=="both_off"&&case["method"]=="GET").unwrap();
    let client=reqwest::Client::builder().timeout(Duration::from_secs(8)).build().unwrap();let mut request=client.get(format!("http://{address}/api/vod/categories/"));for(name,value)in case["headers"].as_object().unwrap(){request=request.header(name.strip_prefix("HTTP_").unwrap_or(name).replace('_',"-"),value.as_str().unwrap());}
    let response=request.send().await;let mut observed=json!({"request_error":true});
    if let Ok(response)=response{let status=response.status().as_u16();let headers=response.headers().clone();if let Ok(body)=response.bytes().await{observed=json!({"status":status,"body_bytes":body.len(),"body_sha256":format!("{:x}",Sha256::digest(&body)),"content_type":headers.get("content-type").and_then(|value|value.to_str().ok()),"selected_headers":{ "Content-Length":headers.get("content-length").and_then(|value|value.to_str().ok()),"Allow":headers.get("allow").and_then(|value|value.to_str().ok()),"Vary":headers.get("vary").and_then(|value|value.to_str().ok()),"WWW-Authenticate":headers.get("www-authenticate").and_then(|value|value.to_str().ok())}});}}
    let prefix:Vec<String>=sqlx::query_scalar("SELECT category_type FROM vod_vodcategory WHERE name='Uncategorized' ORDER BY category_type").fetch_all(&pool).await.unwrap();
    // Cleanup precedes assertions; restoration bypasses ORM auto timestamps.
    sqlx::query("DROP TRIGGER rust_vod_category_network_fail_series ON vod_vodcategory").execute(&pool).await.unwrap();sqlx::query("DROP FUNCTION public.rust_vod_category_network_fail_series()").execute(&pool).await.unwrap();
    sqlx::query("DELETE FROM vod_m3uvodcategoryrelation WHERE category_id IN (SELECT id FROM vod_vodcategory WHERE name='Uncategorized')").execute(&pool).await.unwrap();sqlx::query("DELETE FROM vod_vodcategory WHERE name='Uncategorized'").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO vod_vodcategory SELECT * FROM jsonb_populate_recordset(NULL::vod_vodcategory,$1)").bind(&saved).execute(&pool).await.unwrap();sqlx::query("INSERT INTO vod_m3uvodcategoryrelation SELECT * FROM jsonb_populate_recordset(NULL::vod_m3uvodcategoryrelation,$1)").bind(&links).execute(&pool).await.unwrap();
    let restored:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY c.id),'[]'::jsonb) FROM vod_vodcategory c WHERE id=ANY($1)").bind(&ids).fetch_one(&pool).await.unwrap();let restored_links:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.id),'[]'::jsonb) FROM vod_m3uvodcategoryrelation r WHERE category_id=ANY($1)").bind(&ids).fetch_one(&pool).await.unwrap();
    let mut wanted=reference["response"].clone();wanted.as_object_mut().unwrap().remove("body");let passed=observed==wanted&&json!(prefix)==reference["committed_category_type_prefix"]&&saved==restored&&links==restored_links;
    let drained=gateway.shutdown(Duration::from_secs(3)).await;server.abort();let _=server.await;pool.close().await;
    std::fs::write(std::path::PathBuf::from(std::env::var("VOD_NATIVE_EVIDENCE").unwrap()).join("category-initial-native.json"),serde_json::to_vec_pretty(&json!({"passed":passed&&drained,"scope":"Actual original network and independent nativeTCP owned trigger failure, same committed prefix, exact row restoration; one DB fault branch, not arbitrary fault acceptance","original_response":wanted,"native_response":observed,"committed_category_type_prefix":prefix,"category_and_relation_rows_exactly_restored":saved==restored&&links==restored_links,"workers_drained":drained})).unwrap()).unwrap();assert!(passed&&drained,"category initial fault differences retained");
}

#[cfg(test)]
pub(super) async fn compare_shared_ordinary_contention() {
    use serde_json::{Value,json};use sha2::{Digest,Sha256};
    assert_eq!(std::env::var("VOD_CATEGORY_NATIVE").as_deref(),Ok("playback-fixture-only"));
    let source:Value=serde_json::from_slice(&std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap()).unwrap();let reference:Value=serde_json::from_slice(&std::fs::read(std::env::var("VOD_MIXED_BURST").unwrap()).unwrap()).unwrap();assert_eq!(reference["passed"],true);
    let url=std::env::var("DATABASE_URL").unwrap();assert!(url.contains("playback-fixture-only"));let pool=sqlx::postgres::PgPoolOptions::new().max_connections(8).connect(&url).await.unwrap();let auth=crate::AuthService::new(crate::Store::from_pool(pool.clone()),crate::AuthConfig::new("playback-fixture-only".into()).unwrap()).with_notification_cache(crate::NotificationCache::connect(&std::env::var("REDIS_TEST_URL").unwrap()).await.unwrap());
    let ordinary=super::VodReads::new(auth.clone(),ReadLimits::default()).unwrap();let gate=ordinary.shared_gate();let categories=CategoryHttp::shared(auth.clone(),ordinary.reader.shared_runtime(),gate.clone());
    let router=ordinary.router().merge(ordinary.category_router());let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();let server=tokio::spawn(async move{axum::serve(listener,router.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();});
    let cases=source["cases"].as_array().unwrap();let base=cases.iter().find(|case|case["path"]=="/api/vod/categories/"&&case["role"]=="standard"&&case["method"]=="GET").unwrap();
    let client=reqwest::Client::builder().timeout(Duration::from_secs(8)).build().unwrap();let barrier=Arc::new(tokio::sync::Barrier::new(11));let mut jobs=Vec::new();
    for index in 0..10 {let client=client.clone();let base=base.clone();let barrier=barrier.clone();let path=if index%2==0{"/api/vod/movies/"}else{"/api/vod/categories/"};jobs.push(tokio::spawn(async move{let mut request=client.get(format!("http://{address}{path}"));for(name,value)in base["headers"].as_object().unwrap(){request=request.header(name.strip_prefix("HTTP_").unwrap_or(name).replace('_',"-"),value.as_str().unwrap());}barrier.wait().await;let response=request.send().await.unwrap();let status=response.status().as_u16();let body=response.bytes().await.unwrap();json!({"index":index,"path":path,"status":status,"body_sha256":format!("{:x}",Sha256::digest(&body))})}));}
    barrier.wait().await;let mut burst=Vec::new();for job in jobs{burst.push(job.await.unwrap());}let burst_pass=burst.iter().all(|row|row["status"]==200&&reference["records"].as_array().unwrap().iter().find(|original|original["index"]==row["index"]).unwrap()["body_sha256"]==row["body_sha256"]);
    let request=|path:&str|{let mut request=axum::extract::Request::builder().uri(path);for(name,value)in base["headers"].as_object().unwrap(){request=request.header(name.strip_prefix("HTTP_").unwrap_or(name).replace('_',"-"),value.as_str().unwrap());}request.body(Body::empty()).unwrap()};let peer="127.0.0.1:30000".parse().unwrap();
    let movie=super::vod_api_http::respond(ordinary.reader.clone(),auth.clone(),gate.clone(),peer,super::vod_api_sql::Resource::Movie,false,request("/api/vod/movies/")).await;assert_eq!(movie.status(),200);let movie=to_bytes(movie.into_body(),16*1024*1024).await.unwrap();
    let category=categories.respond(peer,false,request("/api/vod/categories/")).await;assert_eq!(category.status(),200);let category=to_bytes(category.into_body(),16*1024*1024).await.unwrap();let clone=category.clone();drop(category);assert_eq!(gate.slots.available_permits(),0);assert_eq!(ordinary.reader.physical_workers(),0);let both_memory=ordinary.reader.retained_memory_bytes();assert!(both_memory>movie.len()+clone.len());
    let reader=ordinary.reader.clone();let waiting_auth=auth.clone();let waiting_gate=gate.clone();let waiting_request=request("/api/vod/movies/");let queued=tokio::spawn(async move{super::vod_api_http::respond(reader,waiting_auth,waiting_gate,peer,super::vod_api_sql::Resource::Movie,false,waiting_request).await});
    tokio::time::sleep(Duration::from_millis(30)).await;assert!(!queued.is_finished());assert_eq!(ordinary.reader.physical_workers(),0);assert_eq!(gate.waiters.available_permits(),255);
    drop(clone);let third=queued.await.unwrap();assert_eq!(third.status(),200);let third=to_bytes(third.into_body(),16*1024*1024).await.unwrap();assert_eq!(gate.slots.available_permits(),0);assert_eq!(format!("{:x}",Sha256::digest(&third)),format!("{:x}",Sha256::digest(&movie)));drop(third);
    let remaining_memory=ordinary.reader.retained_memory_bytes();assert!(remaining_memory>0&&remaining_memory<both_memory);assert_eq!(gate.slots.available_permits(),1);
    let drained=ordinary.shutdown(Duration::from_secs(3)).await;assert!(drained);assert_eq!(ordinary.reader.retained_memory_bytes(),remaining_memory);drop(movie);assert_eq!(ordinary.reader.retained_memory_bytes(),0);assert_eq!(gate.slots.available_permits(),2);
    server.abort();let _=server.await;pool.close().await;let report=json!({"passed":burst_pass&&drained,"scope":"Actual original/native mixed ten Movie/Category requests and shared final-frame contention; not performance or app registration","original":reference,"native_records":burst,"shared_active_limit":2,"shared_memory_limit_bytes":ReadLimits::default().memory_bytes,"two_families_held_frame_memory_bytes":both_memory,"one_movie_frame_memory_bytes":remaining_memory,"category_final_clone_unblocks_ordinary_queue":true,"queued_request_did_not_start_SQL":true,"same_shutdown_drains_both_routes":true,"held_frame_memory_survives_shutdown_until_finaldrop":true});std::fs::write(std::path::PathBuf::from(std::env::var("VOD_NATIVE_EVIDENCE").unwrap()).join("category-shared-ordinary.json"),serde_json::to_vec_pretty(&report).unwrap()).unwrap();assert_eq!(report["passed"],true,"mixed category/ordinary differences retained");
}

#[cfg(test)]
pub(super) async fn compare_default_fault_wire() {
    use serde_json::{Value,json};use sha2::{Digest,Sha256};
    assert_eq!(std::env::var("VOD_CATEGORY_NATIVE").as_deref(),Ok("playback-fixture-only"));let source:Value=serde_json::from_slice(&std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap()).unwrap();let reference:Value=serde_json::from_slice(&std::fs::read(std::env::var("VOD_CATEGORY_DEFAULTS_WIRE_REFERENCE").unwrap()).unwrap()).unwrap();assert_eq!(reference["passed"],true);
    let url=std::env::var("DATABASE_URL").unwrap();assert!(url.contains("playback-fixture-only"));let pool=sqlx::postgres::PgPoolOptions::new().max_connections(8).connect(&url).await.unwrap();let account:i64=sqlx::query_scalar("SELECT id::bigint FROM m3u_m3uaccount WHERE name='rust-probe-vod-rest-second'").fetch_one(&pool).await.unwrap();let props:Value=sqlx::query_scalar("SELECT custom_properties FROM m3u_m3uaccount WHERE id=$1").bind(account).fetch_one(&pool).await.unwrap();
    let saved:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.id),'[]'::jsonb) FROM vod_m3uvodcategoryrelation r JOIN vod_vodcategory c ON c.id=r.category_id WHERE r.m3u_account_id=$1 AND c.name='Uncategorized'").bind(account).fetch_one(&pool).await.unwrap();assert_eq!(saved.as_array().unwrap().len(),2);
    let auth=crate::AuthService::new(crate::Store::from_pool(pool.clone()),crate::AuthConfig::new("playback-fixture-only".into()).unwrap()).with_notification_cache(crate::NotificationCache::connect(&std::env::var("REDIS_TEST_URL").unwrap()).await.unwrap());let gateway=CategoryHttp::new(auth,ReadLimits::default()).unwrap();let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();let router=gateway.router();let server=tokio::spawn(async move{axum::serve(listener,router.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();});
    let case=source["cases"].as_array().unwrap().iter().find(|case|case["path"]=="/api/vod/categories/"&&case["role"]=="both_off"&&case["method"]=="GET").unwrap();let client=reqwest::Client::builder().timeout(Duration::from_secs(8)).build().unwrap();let mut records=Vec::new();let mut differences=Vec::new();
    for record in reference["records"].as_array().unwrap(){
        sqlx::query("DELETE FROM vod_m3uvodcategoryrelation r USING vod_vodcategory c WHERE r.category_id=c.id AND r.m3u_account_id=$1 AND c.name='Uncategorized'").bind(account).execute(&pool).await.unwrap();
        if record["missing"]==false{sqlx::query("INSERT INTO vod_m3uvodcategoryrelation SELECT * FROM jsonb_populate_recordset(NULL::vod_m3uvodcategoryrelation,$1)").bind(&saved).execute(&pool).await.unwrap();}
        sqlx::query("UPDATE m3u_m3uaccount SET custom_properties=$1 WHERE id=$2").bind(json!({"enable_vod":true,"auto_enable_new_groups_vod":record["default"]})).bind(account).execute(&pool).await.unwrap();
        let mut request=client.get(format!("http://{address}/api/vod/categories/"));for(name,value)in case["headers"].as_object().unwrap(){request=request.header(name.strip_prefix("HTTP_").unwrap_or(name).replace('_',"-"),value.as_str().unwrap());}let response=request.send().await.unwrap();let status=response.status().as_u16();let headers=response.headers().clone();let body=response.bytes().await.unwrap();let actual=json!({"status":status,"body_bytes":body.len(),"body_sha256":format!("{:x}",Sha256::digest(&body)),"content_type":headers.get("content-type").and_then(|value|value.to_str().ok()),"selected_headers":{"Content-Length":headers.get("content-length").and_then(|value|value.to_str().ok()),"Allow":headers.get("allow").and_then(|value|value.to_str().ok()),"Vary":headers.get("vary").and_then(|value|value.to_str().ok()),"WWW-Authenticate":headers.get("www-authenticate").and_then(|value|value.to_str().ok())}});
        let effects:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(jsonb_build_object('category__category_type',c.category_type,'enabled',r.enabled,'custom_properties',r.custom_properties) ORDER BY c.category_type),'[]'::jsonb) FROM vod_m3uvodcategoryrelation r JOIN vod_vodcategory c ON c.id=r.category_id WHERE r.m3u_account_id=$1 AND c.name='Uncategorized'").bind(account).fetch_one(&pool).await.unwrap();let mut wanted=record["outcome"].clone();wanted.as_object_mut().unwrap().remove("body");let equal=actual==wanted&&effects==record["committed_links"];
        let row=json!({"default":record["default"],"missing":record["missing"],"actual":actual,"effects":effects,"passed":equal});if !equal{differences.push(row.clone());}records.push(row);
    }
    sqlx::query("UPDATE m3u_m3uaccount SET custom_properties=$1 WHERE id=$2").bind(&props).bind(account).execute(&pool).await.unwrap();sqlx::query("DELETE FROM vod_m3uvodcategoryrelation r USING vod_vodcategory c WHERE r.category_id=c.id AND r.m3u_account_id=$1 AND c.name='Uncategorized'").bind(account).execute(&pool).await.unwrap();sqlx::query("INSERT INTO vod_m3uvodcategoryrelation SELECT * FROM jsonb_populate_recordset(NULL::vod_m3uvodcategoryrelation,$1)").bind(&saved).execute(&pool).await.unwrap();
    let restored:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.id),'[]'::jsonb) FROM vod_m3uvodcategoryrelation r JOIN vod_vodcategory c ON c.id=r.category_id WHERE r.m3u_account_id=$1 AND c.name='Uncategorized'").bind(account).fetch_one(&pool).await.unwrap();let drained=gateway.shutdown(Duration::from_secs(3)).await;server.abort();let _=server.await;pool.close().await;
    let report=json!({"passed":differences.is_empty()&&records.len()==18&&saved==restored&&drained,"scope":"Actual original/nativeTCP category BooleanField/default responses plus committed effects, including seven source500 errors and existing-invalid-default bypass; no arbitrary property grammar/fullcategory acceptance","records":records,"differences":differences,"exact_link_rows_restored":saved==restored,"workers_drained":drained});std::fs::write(std::path::PathBuf::from(std::env::var("VOD_NATIVE_EVIDENCE").unwrap()).join("category-defaults-wire-native.json"),serde_json::to_vec_pretty(&report).unwrap()).unwrap();assert_eq!(report["passed"],true,"default wire differences retained");
}
