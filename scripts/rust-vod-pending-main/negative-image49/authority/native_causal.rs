use dispatcharr_coordination::{AcquireRequest,Coordinator,Error,LeaseConfig,ProviderReservation};
use std::{time::{Duration,Instant},path::PathBuf};
#[tokio::test]
#[ignore]
async fn actual_unchanged_two_second_observer_loses_success_before_physical_reply() {
 assert_eq!(std::env::var("PENDING_CAUSAL_FIXTURE").unwrap(),"playback-fixture-only");
 let url=std::env::var("REDIS_TEST_URL").unwrap();let output=PathBuf::from(std::env::var("PENDING_CAUSAL_OUTPUT").unwrap());
 let namespace=format!("rust:pending-causal:{}",uuid::Uuid::new_v4());
 let config=LeaseConfig{namespace:namespace.clone(),heartbeat_interval:None,reap_interval:None,..LeaseConfig::default()};
 assert_eq!(config.command_timeout,Duration::from_secs(2));
 let coordinator=Coordinator::connect(&url,config).await.unwrap();
 let lease=coordinator.acquire_vod(AcquireRequest{channel_id:"physical-causal-owned".into(),provider_profile_id:None,provider_max_streams:0,credential_pool:None}).await.unwrap();
 let began=Instant::now();let result=lease.reserve_provider(ProviderReservation{profile_id:Some(987654321),max_streams:1,credential_pool:None}).await;
 let observer_ms=began.elapsed().as_millis();assert_eq!(result,Err(Error::RedisUnavailable));assert!(!lease.settlement().is_settled());assert!(!coordinator.is_idle());
 lease.settlement().wait().await;let settled_ms=began.elapsed().as_millis();assert!((1900..2400).contains(&observer_ms));assert!((2400..=4000).contains(&settled_ms));
 let client=redis::Client::open(url).unwrap();let mut connection=client.get_multiplexed_async_connection().await.unwrap();
 let count:i64=redis::cmd("GET").arg("profile_connections:987654321").query_async(&mut connection).await.unwrap();assert_eq!(count,1);
 let ledger=format!("{namespace}:lease:{}",lease.token());let reserved:String=redis::cmd("HGET").arg(&ledger).arg("reserved").query_async(&mut connection).await.unwrap();assert_eq!(reserved,"1");
 assert!(lease.release().await.unwrap());lease.settlement().wait().await;let final_count:i64=redis::cmd("GET").arg("profile_connections:987654321").query_async(&mut connection).await.unwrap();assert_eq!(final_count,0);
 let keys:Vec<String>=redis::cmd("KEYS").arg(format!("{namespace}:*")).query_async(&mut connection).await.unwrap();assert!(keys.len()<16);if !keys.is_empty(){let _:u64=redis::cmd("DEL").arg(keys).query_async(&mut connection).await.unwrap();}
 let _:u64=redis::cmd("DEL").arg("profile_connections:987654321").query_async(&mut connection).await.unwrap();
 std::fs::write(output,format!("{{\"passed\":true,\"scope\":\"unchanged Coordinator component, not Main HTTP\",\"observer_ms\":{observer_ms},\"settled_ms\":{settled_ms},\"observer_result\":\"RedisUnavailable\",\"actual_mutation_reserved\":true,\"final_counter\":0}}\n")).unwrap();
}
