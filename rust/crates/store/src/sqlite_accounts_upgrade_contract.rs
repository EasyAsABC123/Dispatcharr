use super::*;

#[tokio::test]
#[ignore = "requires disposable exact SQLite migration copies"]
async fn first_two_accounts_steps_migrate_resume_noop_and_rollback() {
    let root =
        std::path::PathBuf::from(std::env::var_os("DISPATCHARR_OWNED_SQLITE_UPGRADE").unwrap());
    let cold = root.join("cold.db");
    assert_eq!(run(&cold).await.unwrap(), Upgrade::AppliedBoth);
    let committed = std::fs::read(&cold).unwrap();
    assert_eq!(run(&cold).await.unwrap(), Upgrade::Unchanged);
    assert!(std::fs::read(&cold).unwrap() == committed);
    assert_eq!(
        run(&root.join("resume.db")).await.unwrap(),
        Upgrade::AppliedManager
    );
    let blocked = root.join("rollback.db");
    let unchanged = std::fs::read(&blocked).unwrap();
    let rejected = run(&blocked).await.unwrap_err();
    let Failure::Database(error) = rejected else {
        panic!("expected ordinary SQLite recorder refusal");
    };
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("1811")
    );
    assert!(std::fs::read(&blocked).unwrap() == unchanged);
    let wrong = root.join("wrong-schema.db");
    assert!(matches!(run(&wrong).await, Err(Failure::OutsideKnownState)));
    let absent = root.join("absent.db");
    assert!(matches!(
        run(&absent).await,
        Err(Failure::OutsideKnownState)
    ));
    assert!(!absent.exists());
}
