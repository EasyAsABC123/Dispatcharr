//! Raised TVG INSERT P0001 only, after actual Store settlement and Source finally.
//! Original Dispatcharr/Django/psycopg/Celery authors retain all credit/licenses.
use crate::index_terminal::Failure;
use dispatcharr_store::tvg_programme::Error;
/// Private payload; arbitrary operational transport errors cannot mint it.
pub(crate) struct InsertFailure(Failure);
impl InsertFailure {
    pub(crate) fn classify(error: &Error) -> Option<Self> {
        let Error::Sql {
            stage: "insert",
            source,
            rollback: None,
            ready: None,
            close: None,
        } = error
        else {
            return None;
        };
        let pg = source
            .as_database_error()?
            .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()?;
        if pg.code() != "P0001" {
            return None;
        }
        let message = crate::pg_failure_format::render_pg(pg).ok()?;
        Some(Self(Failure::tvg_programme_insert(&message).ok()?))
    }
    pub(crate) fn metadata(
        self,
        id: uuid::Uuid,
        date: &str,
    ) -> Result<Vec<u8>, crate::index_terminal::FailureError> {
        self.0.metadata(id, None, &[], date)
    }
}
#[cfg(test)]
mod source_contract {
    use super::*;
    use serde_json::Value;
    #[test]
    fn retained_source_failure_dto_matches_without_fabricated_python_frames() {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-tvg-insert-failure.json"))
                .unwrap();
        let expected = &source["Source_metadata"];
        let message = expected["result"]["exc_message"][0].as_str().unwrap();
        let failure = InsertFailure(Failure::tvg_programme_insert(message).unwrap());
        let encoded = failure
            .metadata(
                uuid::Uuid::parse_str(expected["task_id"].as_str().unwrap()).unwrap(),
                expected["date_done"].as_str().unwrap(),
            )
            .unwrap();
        let actual: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(actual["status"], expected["status"]);
        assert_eq!(actual["result"], expected["result"]);
        assert_eq!(actual["children"], expected["children"]);
        let trace = actual["traceback"].as_str().unwrap();
        assert!(trace.starts_with("Native Dispatcharr per-channel programme task diagnostic"));
        assert!(!trace.contains("Traceback (most recent call last)"));
    }
    #[test]
    fn non_database_and_unsettled_errors_cannot_mint_failure() {
        assert!(InsertFailure::classify(&Error::Unsupported).is_none());
        assert!(InsertFailure::classify(&Error::Sql {
            stage: "insert",
            source: sqlx::Error::PoolClosed,
            rollback: None,
            ready: None,
            close: None
        })
        .is_none());
        assert!(InsertFailure::classify(&Error::Settlement {
            ready: Some(sqlx::Error::PoolClosed),
            close: None
        })
        .is_none());
    }
}
