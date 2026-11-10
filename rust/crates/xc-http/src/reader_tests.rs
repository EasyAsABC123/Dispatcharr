//! Original missing-group helper behavior with real original-schema SQL.
//! Original Dispatcharr application authors retain credit.
use super::*;
#[tokio::test]
#[ignore = "requires disposable original PostgreSQL XC fixture"]
async fn missing_default_group_matches_source_and_concurrent_creation() {
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    assert!(url.contains("playback-fixture-only"));
    let store = Store::connect(&url, 4).await.unwrap();
    let original: Value = sqlx::query_scalar(
        "SELECT to_jsonb(g) FROM dispatcharr_channels_channelgroup g WHERE name='Default Group'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    let relations:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(r) FROM dispatcharr_channels_channelgroupm3uaccount r WHERE r.channel_group_id=$1").bind(original["id"].as_i64().unwrap()).fetch_all(store.pool()).await.unwrap();
    assert!(relations.len() <= 4, "owned fixture only");
    // Django performs on_delete collection in the ORM; its original SQL FK is
    // NO ACTION. The fixture must remove the owned child rows first as it does.
    sqlx::query("DELETE FROM dispatcharr_channels_channelgroupm3uaccount WHERE channel_group_id=(SELECT id FROM dispatcharr_channels_channelgroup WHERE name='Default Group')").execute(store.pool()).await.unwrap();
    sqlx::query("DELETE FROM dispatcharr_channels_channelgroup WHERE name='Default Group'")
        .execute(store.pool())
        .await
        .unwrap();
    let missing:bool=sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroup WHERE name='Default Group')").fetch_one(store.pool()).await.unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let store = store.clone();
        tasks.spawn(async move { default_group(&store).await });
    }
    let mut ids = Vec::new();
    let mut errors = 0;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(id)) => ids.push(id),
            _ => errors += 1,
        }
    }
    let actual:Result<Value,_>=sqlx::query_scalar("SELECT jsonb_build_object('missing_before',$1::boolean,'repeated_get_same_id',$2::boolean,'group_name',g.name,'matching_groups',(SELECT count(*) FROM dispatcharr_channels_channelgroup WHERE name='Default Group'),'matching_custom_relations',(SELECT count(*) FROM dispatcharr_channels_channelgroupm3uaccount x JOIN m3u_m3uaccount a ON a.id=x.m3u_account_id WHERE x.channel_group_id=g.id AND a.name='custom' AND a.locked),'relation',jsonb_build_object('custom_properties',r.custom_properties,'enabled',r.enabled,'auto_channel_sync',r.auto_channel_sync,'auto_sync_channel_start',r.auto_sync_channel_start,'auto_sync_channel_end',r.auto_sync_channel_end,'is_stale',r.is_stale,'last_seen_present',r.last_seen IS NOT NULL)) FROM dispatcharr_channels_channelgroup g JOIN dispatcharr_channels_channelgroupm3uaccount r ON r.channel_group_id=g.id JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE g.name='Default Group' AND a.name='custom' AND a.locked").bind(missing).bind(ids.len()==8&&ids.iter().all(|id|Some(id)==ids.first())).fetch_one(store.pool()).await;
    // Restore only this fixture's original records before any assertions; keep
    // sequences monotonic rather than rewinding another connection's allocator.
    sqlx::query("DELETE FROM dispatcharr_channels_channelgroupm3uaccount WHERE channel_group_id=(SELECT id FROM dispatcharr_channels_channelgroup WHERE name='Default Group')").execute(store.pool()).await.unwrap();
    sqlx::query("DELETE FROM dispatcharr_channels_channelgroup WHERE name='Default Group'")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO dispatcharr_channels_channelgroup SELECT * FROM jsonb_populate_record(NULL::dispatcharr_channels_channelgroup,$1)").bind(original).execute(store.pool()).await.unwrap();
    for relation in relations {
        sqlx::query("INSERT INTO dispatcharr_channels_channelgroupm3uaccount SELECT * FROM jsonb_populate_record(NULL::dispatcharr_channels_channelgroupm3uaccount,$1)").bind(relation).execute(store.pool()).await.unwrap();
    }
    assert_eq!(errors, 0);
    let source: Value =
        serde_json::from_str(include_str!("../datasets/source-default-group.json")).unwrap();
    assert_eq!(actual.unwrap(), source);
}
