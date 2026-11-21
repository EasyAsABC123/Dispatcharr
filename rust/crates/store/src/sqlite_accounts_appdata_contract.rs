use super::*;

#[tokio::test]
#[ignore = "requires owned SQLite Accounts migration copies"]
async fn later_accounts_apply_cold_resume_preserve_and_roll_back() {
    let root =
        std::path::PathBuf::from(std::env::var_os("DISPATCHARR_OWNED_SQLITE_APPDATA").unwrap());
    let cold = root.join("cold.db");
    assert_eq!(
        super::super::run(&cold).await.unwrap(),
        Upgrade::AppliedBoth
    );
    assert_eq!(run(&cold).await.unwrap(), 3);
    assert_eq!(run(&root.join("resume-135.db")).await.unwrap(), 2);
    assert_eq!(run(&root.join("populated-136.db")).await.unwrap(), 1);
    let complete = std::fs::read(root.join("populated-136.db")).unwrap();
    assert_eq!(run(&root.join("populated-136.db")).await.unwrap(), 0);
    assert_eq!(
        std::fs::read(root.join("populated-136.db")).unwrap(),
        complete
    );
    let rollback = root.join("rollback.db");
    let before = std::fs::read(&rollback).unwrap();
    let Failure::Database(error) = run(&rollback).await.unwrap_err() else {
        panic!("expected SQLite recorder rollback after 0008 schema and copy");
    };
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("1811")
    );
    assert_eq!(std::fs::read(&rollback).unwrap(), before);
    let uncertain = root.join("other-index.db");
    let before = std::fs::read(&uncertain).unwrap();
    assert!(matches!(
        run(&uncertain).await,
        Err(Failure::OutsideKnownState)
    ));
    assert_eq!(std::fs::read(&uncertain).unwrap(), before);
}
