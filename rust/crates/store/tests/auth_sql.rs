//! Actual original login hash-upgrade contracts on disposable Django SQL.
//! Original application credit remains with Dispatcharr contributors.
use axum::response::IntoResponse;
use dispatcharr_contracts::PlaybackRequest;
use dispatcharr_store::{verify_password, AuthConfig, AuthService, Store};
use serde_json::{json, Value};

#[tokio::test]
#[ignore = "requires AUTH_DISPOSABLE_FIXTURE and captured original login/SQL contracts"]
async fn original_pbkdf2_login_upgrade_before_active_check_and_no_wrong_password_write() {
    assert_eq!(
        std::env::var("AUTH_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let golden: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("AUTH_COMPAT_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
        .await
        .unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    );
    for case in golden["hash_cases"].as_array().unwrap() {
        let request = PlaybackRequest {
            channel_id: String::new(),
            query: vec![],
            headers: vec![],
            peer_ip: case["ip"].as_str().unwrap().parse().unwrap(),
        };
        let result = auth.login(&request, json!({"username":case["username"],"password":if case["correct"] == true {"hash-fixture-only"} else {"wrong-fixture-only"}})).await;
        let status = match result {
            Ok(_) => 200,
            Err(error) => error.into_response().status().as_u16(),
        };
        let encoded: String = sqlx::query_scalar("SELECT password FROM accounts_user WHERE id=$1")
            .bind(case["user_id"].as_i64().unwrap())
            .fetch_one(store.pool())
            .await
            .unwrap();
        let fields: Vec<_> = encoded.split('$').collect();
        let actual = json!({"status":status,"changed":encoded!=case["original_hash"].as_str().unwrap(),"password_valid":verify_password("hash-fixture-only",&encoded),"algorithm":fields[0],"iterations":fields[1].parse::<u32>().unwrap(),"salt_characters":fields[2].chars().count()});
        assert_eq!(
            actual,
            case["expected"],
            "source hash policy case {}",
            case["name"].as_str().unwrap()
        );
    }
}
