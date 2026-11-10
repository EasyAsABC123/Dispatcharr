//! Original-owned path corpus against real fresh-index SQL/file component.
//! Original Dispatcharr authors retain all application and algorithm credit.
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_ingest_epg::{
    current_lookup::{
        CurrentLookup, CurrentLookupConfig, CurrentLookupServices, MissingIndexDispatcher,
        SourcePathContext,
    },
    ImportError, SwapPolicyContext,
};
use dispatcharr_store::{CurrentFileOutcome, CurrentFileRequest, Store};
use serde_json::{json, Value};
use sqlx::Connection;
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
struct NeverPublish;
impl MissingIndexDispatcher for NeverPublish {
    fn dispatch(&self, _: i64, _: SwapPolicyContext) -> BoxFuture<'_, Result<(), ImportError>> {
        Box::pin(async { panic!("Known-index fixture must not publish") })
    }
}
struct Owned(PathBuf);
impl Drop for Owned {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn mapped(path: &str, app: &Path) -> PathBuf {
    let p = Path::new(path);
    if p.is_absolute() {
        assert!(p.starts_with("/app/media"));
        app.join(p.strip_prefix("/app").unwrap())
    } else {
        app.join(p)
    }
}
fn fixture() -> Value {
    assert_eq!(
        std::env::var("EPG_PATH_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    serde_json::from_slice(&std::fs::read(std::env::var("EPG_PATH_FIXTURE").unwrap()).unwrap())
        .unwrap()
}
#[tokio::test]
#[ignore = "requires owned original path corpus and copied PostgreSQL"]
async fn literal_paths_precedence_cache_and_fresh_index_keep_owned_resources() {
    let fixture = fixture();
    let cases = fixture["path_context"]["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 20);
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 1)
        .await
        .unwrap();
    let root = Owned(
        std::env::temp_dir().join(format!("owned-current-path-native-{}", std::process::id())),
    );
    assert!(!root.0.exists());
    let app = root.0.join("app");
    std::fs::create_dir_all(app.join("media")).unwrap();
    let mut compared = 0;
    for case in cases {
        let name = case["name"].as_str().unwrap();
        // Controlled two-open replacement/deletion is separately tested through the
        // same public physical file helper. The test binary runs as Source fixture UID0 inside the same
        // baseline, including the actual mode000 file observation.
        if matches!(name, "relative-replaced" | "relative-deleted") {
            continue;
        }
        for row in case["files_before"].as_array().unwrap() {
            let path = mapped(row["path"].as_str().unwrap(), &app);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).unwrap()
            };
            match row["kind"].as_str().unwrap() {
                "directory" => std::fs::create_dir_all(path).unwrap(),
                "symlink" => {
                    if path.symlink_metadata().is_err() {
                        std::os::unix::fs::symlink(
                            mapped(row["target"].as_str().unwrap(), &app),
                            path,
                        )
                        .unwrap()
                    }
                }
                "file" => {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::write(
                        &path,
                        STANDARD.decode(row["bytes"].as_str().unwrap()).unwrap(),
                    )
                    .unwrap();
                    std::fs::set_permissions(
                        path,
                        std::fs::Permissions::from_mode(row["mode"].as_u64().unwrap() as u32),
                    )
                    .unwrap();
                }
                _ => panic!("fixturekind"),
            }
        }
        let id = case["source_id"].as_i64().unwrap();
        let owner: String = sqlx::query_scalar("SELECT name FROM epg_epgsource WHERE id=$1")
            .bind(id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert!(owner.starts_with("rust-path-"));
        sqlx::query("UPDATE epg_epgsourceindex SET data=$2 WHERE source_id=$1")
            .bind(id)
            .bind(sqlx::types::Json(case["initial_index"].clone()))
            .execute(store.pool())
            .await
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let fresh = name == "fresh-index-after-clock";
        let url = std::env::var("DATABASE_TEST_URL").unwrap();
        let now = case["now"].as_str().unwrap().parse().unwrap();
        let gateway = CurrentLookup::new(
            CurrentLookupServices {
                store: store.clone(),
                dispatcher: Arc::new(NeverPublish),
                clock: Arc::new(move || {
                    observed.fetch_add(1, Ordering::SeqCst);
                    if fresh {
                        let url = url.clone();
                        std::thread::spawn(move || {
                            tokio::runtime::Builder::new_current_thread()
                                .enable_all()
                                .build()
                                .unwrap()
                                .block_on(async move {
                                    let mut connection =
                                        sqlx::PgConnection::connect(&url).await.unwrap();
                                    sqlx::query(
                                        "UPDATE epg_epgsourceindex SET data=$2 WHERE source_id=$1",
                                    )
                                    .bind(id)
                                    .bind(sqlx::types::Json(json!({"channels":{"other":[0]}})))
                                    .execute(&mut connection)
                                    .await
                                    .unwrap();
                                    connection.close().await.unwrap()
                                })
                        })
                        .join()
                        .unwrap();
                    }
                    now
                }),
            },
            CurrentLookupConfig {
                max_jobs: 1,
                path_context: Some(SourcePathContext::new(app.clone(), app.join("media")).unwrap()),
                ..Default::default()
            },
        )
        .unwrap();
        let convert = |key: &str| {
            case[key].as_str().map(|s| {
                if Path::new(s).is_absolute() {
                    mapped(s, &app)
                } else {
                    PathBuf::from(s)
                }
            })
        };
        let req = CurrentFileRequest {
            source_id: id,
            epg_entry_id: case["epg_entry_id"].as_i64().unwrap(),
            source_type: "xmltv".into(),
            tvg_id: "one".into(),
            file_path: convert("file_path"),
            extracted_file_path: convert("extracted_file_path"),
            cache_file_path: PathBuf::from("/deliberately-unused-context-fallback"),
        };
        let actual = gateway.lookup(req, Arc::new(())).await.unwrap();
        match actual {
            CurrentFileOutcome::Found(bytes) => {
                let value: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(value, case["result"], "{name}");
                let clone = bytes.clone();
                drop(bytes);
                assert!(gateway.retained_memory_bytes() > 0);
                drop(clone)
            }
            CurrentFileOutcome::Missing => assert!(case["result"].is_null(), "{name}"),
            CurrentFileOutcome::Parsing => panic!("unexpected dispatch"),
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            usize::from(
                case["calls"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|v| v == "clock")
            ),
            "{name}"
        );
        assert!(gateway.shutdown(Duration::from_secs(2)).await);
        assert_eq!(gateway.retained_memory_bytes(), 0);
        compared += 1;
    }
    assert_eq!(compared, 18);
    store.pool().close().await;
}

#[test]
#[ignore = "requires owned original two-open relative-path corpus"]
fn relative_two_open_replacement_and_deletion_keep_original_file_semantics() {
    use dispatcharr_ingest_epg::current_file::{
        read_current_with_opener, CurrentFileError, CurrentFileLimits, CurrentFileResult,
    };
    use std::sync::atomic::AtomicBool;
    let fixture = fixture();
    let root = Owned(std::env::temp_dir().join(format!(
        "owned-current-path-two-open-{}",
        std::process::id()
    )));
    assert!(!root.0.exists());
    let app = root.0.join("app");
    for name in ["relative-replaced", "relative-deleted"] {
        let case = fixture["path_context"]["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == name)
            .unwrap();
        let relative = case["file_path"].as_str().unwrap();
        assert!(!Path::new(relative).is_absolute());
        let path = app.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let row = case["files_before"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| {
                v["path"]
                    .as_str()
                    .is_some_and(|s| s.ends_with("/two-open.xml"))
            })
            .unwrap();
        std::fs::write(
            &path,
            STANDARD.decode(row["bytes"].as_str().unwrap()).unwrap(),
        )
        .unwrap();
        let mut opens = 0;
        let actual = read_current_with_opener(
            || {
                opens += 1;
                if opens == 2 {
                    if name == "relative-replaced" {
                        let replacement = path.with_extension("replacement");
                        std::fs::write(&replacement,b"<programme channel='one' start='20261110110000 +0000' stop='20261110130000 +0000'><title>reopened</title></programme>")?;
                        std::fs::rename(replacement, &path)?;
                    } else {
                        std::fs::remove_file(&path)?;
                    }
                }
                std::fs::File::open(&path)
            },
            "one",
            &[0],
            true,
            case["now"].as_str().unwrap().parse().unwrap(),
            CurrentFileLimits::default(),
            &AtomicBool::new(false),
        );
        assert_eq!(opens, 2);
        if name == "relative-replaced" {
            let CurrentFileResult::Found(value) = actual.unwrap() else {
                panic!("expected replacement")
            };
            assert_eq!(value.source_value(), case["result"]);
        } else {
            assert_eq!(case["failure"]["type"], "FileNotFoundError");
            assert!(
                matches!(actual,Err(CurrentFileError::Runtime(ImportError::Io(error)))if error.kind()==std::io::ErrorKind::NotFound)
            );
        }
    }
}
