//! Observed settled notification P0001 failures, data only. No Python imports
//! or frame generation. Original Dispatcharr/Django/psycopg/Celery authors retain
//! application/framework algorithm, code and license credit.
use crate::{default_domains::Error, default_protocol::Normal};
use serde_json::json;
use uuid::Uuid;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Effect {
    OppositeCollector,
    ProfileUpsert,
    FinalCollector,
}
impl Effect {
    fn name(self) -> &'static str {
        match self {
            Self::OppositeCollector => "opposite_collector",
            Self::ProfileUpsert => "profile_upsert",
            Self::FinalCollector => "final_collector",
        }
    }
}
#[derive(Debug)]
pub(crate) struct ReportError {
    pub(crate) error: Error,
    stage: Option<Effect>,
}
impl From<Error> for ReportError {
    fn from(error: Error) -> Self {
        Self { error, stage: None }
    }
}
impl ReportError {
    pub(crate) fn effect(error: sqlx::Error, stage: Effect) -> Self {
        Self {
            error: Error::Sql(error),
            stage: Some(stage),
        }
    }
}
/// Preserve legacy operation-vs-close error precedence. A failed close prevents
/// classification even if a known statement failed; physical settlement wins.
pub(crate) fn settled<T>(
    result: Result<T, ReportError>,
    close: Result<(), sqlx::Error>,
) -> Result<T, ReportError> {
    match result {
        Err(mut report) => {
            if close.is_err() {
                report.stage = None;
            }
            Err(report)
        }
        Ok(value) => {
            close.map_err(Error::Sql)?;
            Ok(value)
        }
    }
}
#[derive(Clone, Debug)]
pub struct ProgrammingFailure {
    message: String,
    diagnostic: String,
}
impl ProgrammingFailure {
    pub fn argument(&self) -> &str {
        &self.message
    }
    pub fn diagnostic(&self) -> &str {
        &self.diagnostic
    }
    pub(crate) fn metadata(
        &self,
        id: Uuid,
        date: &str,
    ) -> Result<Vec<u8>, crate::transport::Error> {
        if self.message.len() > 8192
            || self.diagnostic.len() > 16384
            || date.len() > 40
            || chrono::DateTime::parse_from_rfc3339(date).is_err()
        {
            return Err(crate::transport::Error::Protocol);
        }
        let raw=serde_json::to_vec(&json!({"status":"FAILURE","result":{"exc_type":"ProgrammingError","exc_module":"django.db.utils","exc_message":[self.message]},"traceback":self.diagnostic,"children":[],"date_done":date,"task_id":id.to_string()})).map_err(|_|crate::transport::Error::Protocol)?;
        crate::decode::decode(&raw, id).map_err(|_| crate::transport::Error::Protocol)?;
        Ok(raw)
    }
}
#[derive(Debug)]
pub enum Terminal {
    Normal(Normal),
    Raised(ProgrammingFailure),
}
impl Terminal {
    pub(crate) fn raised(&self) -> bool {
        matches!(self, Self::Raised(_))
    }
}
pub(crate) fn classify(result: Result<(), ReportError>) -> Result<Terminal, Error> {
    let report = match result {
        Ok(()) => return Ok(Terminal::Normal(Normal::Null)),
        Err(error) => error,
    };
    if let (Some(stage), Error::Sql(error)) = (&report.stage, &report.error) {
        if let Some(pg) = error
            .as_database_error()
            .and_then(|v| v.try_downcast_ref::<sqlx::postgres::PgDatabaseError>())
        {
            if pg.severity() == sqlx::postgres::PgSeverity::Error {
                if let Ok(message) = crate::pg_failure_format::render_pg(pg) {
                    return Ok(Terminal::Raised(ProgrammingFailure{message,diagnostic:format!("Native Dispatcharr account expiration task diagnostic [{}]: settled PostgreSQL ERROR P0001; transaction rollback and connection close completed; prior notification commits remain.",stage.name())}));
                }
            }
        }
    }
    Err(report.error)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn typed_data_is_programming_error_with_truthful_opaque_diagnostic() {
        let id = Uuid::new_v4();
        let failure=ProgrammingFailure{message:"owned\nCONTEXT:  controlled".into(),diagnostic:"Native Dispatcharr account expiration task diagnostic [profile_upsert]: settled ERROR".into()};
        let raw = failure
            .metadata(id, "2026-11-11T12:00:00.123456+00:00")
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(value["status"], "FAILURE");
        assert_eq!(
            value["result"],
            json!({"exc_type":"ProgrammingError","exc_module":"django.db.utils","exc_message":[failure.message]})
        );
        assert_eq!(value["children"], json!([]));
        assert_eq!(value["task_id"], id.to_string());
        assert!(!value["traceback"]
            .as_str()
            .unwrap()
            .contains("Traceback (most recent call last)"));
        assert_eq!(
            crate::decode::decode(&raw, id).unwrap().state,
            crate::decode::State::Failure
        );
    }
    #[test]
    fn message_and_date_bounds_are_enforced_before_writer() {
        let id = Uuid::new_v4();
        let mut f = ProgrammingFailure {
            message: "x".repeat(8192),
            diagnostic: "Native diagnostic".into(),
        };
        assert!(f.metadata(id, "2026-11-11T12:00:00+00:00").is_ok());
        f.message.push('x');
        assert!(f.metadata(id, "2026-11-11T12:00:00+00:00").is_err());
        f.message.clear();
        assert!(f.metadata(id, "invalid").is_err());
    }
    #[test]
    fn uncertain_close_and_nonserver_controls_never_become_python_failures() {
        let report = settled::<()>(
            Err(ReportError::effect(
                sqlx::Error::RowNotFound,
                Effect::FinalCollector,
            )),
            Err(sqlx::Error::Protocol("owned close uncertainty".into())),
        )
        .unwrap_err();
        assert!(report.stage.is_none());
        assert!(matches!(report.error, Error::Sql(sqlx::Error::RowNotFound)));
        assert!(matches!(
            classify(Err(ReportError::effect(
                sqlx::Error::RowNotFound,
                Effect::ProfileUpsert
            ))),
            Err(Error::Sql(sqlx::Error::RowNotFound))
        ));
        assert!(matches!(
            classify(Err(Error::Unsupported.into())),
            Err(Error::Unsupported)
        ));
    }
}
