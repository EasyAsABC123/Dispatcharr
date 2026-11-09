//! Actual migrated Django schema contracts; explicitly disposable fixture only.
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_store::{AuthConfig, AuthService, Store, StoreError};
use dispatcharr_vod_http::{ContentKind, SqlVodResolver, VodRequest, VodResolver};
use std::net::{IpAddr, Ipv4Addr};
fn request(kind: &str, id: &str) -> VodRequest {
    VodRequest {
        resolved_client_ip: None,
        kind: kind.into(),
        content_id: id.into(),
        session_id: Some("sql-fixture-session".into()),
        profile_id: None,
        playback: PlaybackRequest {
            channel_id: id.into(),
            query: vec![],
            headers: vec![],
            peer_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        },
    }
}
#[tokio::test]
#[ignore = "requires DATABASE_TEST_URL for the explicitly seeded disposable VOD fixture"]
async fn actual_django_movie_episode_series_xc_profiles_flags_and_caps() {
    assert_eq!(
        std::env::var("VOD_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
        .await
        .unwrap();
    let resolver = SqlVodResolver::new(
        store.clone(),
        AuthService::new(
            store.clone(),
            AuthConfig::new("playback-fixture-only".into()).unwrap(),
        ),
    );
    assert!(!resolver.default_redirect().await.unwrap());
    let movie = request("movie", "a4d78cd1-bf9f-4a63-a746-f40566825cf9");
    assert!(resolver.authorize(&movie).await.unwrap().is_none());
    let source = resolver.resolve(&movie, None).await.unwrap();
    assert_eq!(source.kind, ContentKind::Movie);
    assert_eq!(source.candidates.len(), 1);
    assert_eq!(source.candidates[0].max_streams, 2);
    assert_eq!(
        source.candidates[0].url,
        "http://origin:8080/movie/fixture/fixture/fixture.mp4"
    );
    let episode = resolver
        .resolve(
            &request("episode", "3c884edf-a48f-4e91-adf3-f876c8094814"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(episode.kind, ContentKind::Episode);
    assert_eq!(
        episode.candidates[0].url,
        "http://origin:8080/series/fixture/fixture/fixture.mp4"
    );
    let series = resolver
        .resolve(
            &request("series", "2fb292d4-1236-4ed4-bf70-98d6308a3773"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(series.content_uuid, episode.content_uuid);
    let mut stale = request("movie", &uuid::Uuid::new_v4().to_string());
    stale
        .playback
        .query
        .push(("stream_id".into(), "fixture".into()));
    assert_eq!(
        resolver.resolve(&stale, None).await.unwrap().content_uuid,
        source.content_uuid
    );
    let denied = store
        .user_by_username("rust-vod-fixture-flag-disabled")
        .await
        .unwrap();
    assert!(matches!(
        resolver.resolve(&movie, denied).await,
        Err(StoreError::Forbidden)
    ));
    let capped = store
        .user_by_username("rust-vod-fixture-capped")
        .await
        .unwrap();
    assert_eq!(
        resolver
            .resolve(&movie, capped)
            .await
            .unwrap()
            .user
            .unwrap()
            .stream_limit,
        1
    );
    let mut offset = movie.clone();
    offset.playback.query.push(("offset".into(), "4".into()));
    assert!(matches!(
        resolver.resolve(&offset, None).await,
        Err(StoreError::Unsupported(_))
    ));
    // An unknown transform must not silently substitute another identity
    // profile. This fixture is disposable; restore its exact default after.
    let profile = source.candidates[0].profile_id;
    let backup:i64=sqlx::query_scalar("INSERT INTO m3u_m3uaccountprofile(m3u_account_id,name,is_default,max_streams,is_active,search_pattern,replace_pattern,current_viewers,custom_properties) VALUES($1,'Unsupported-transform fallback probe',false,2,true,'^(.*)$','$1',0,'{}'::jsonb) RETURNING id::bigint").bind(source.candidates[0].account_id).fetch_one(store.pool()).await.unwrap();
    sqlx::query("UPDATE m3u_m3uaccountprofile SET search_pattern='fixture',replace_pattern='other-credentials' WHERE id=$1").bind(profile).execute(store.pool()).await.unwrap();
    assert!(matches!(
        resolver.resolve(&movie, None).await,
        Err(StoreError::Unsupported(_))
    ));
    sqlx::query(
        "UPDATE m3u_m3uaccountprofile SET search_pattern='^(.*)$',replace_pattern='$1' WHERE id=$1",
    )
    .bind(profile)
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM m3u_m3uaccountprofile WHERE id=$1")
        .bind(backup)
        .execute(store.pool())
        .await
        .unwrap();
}
