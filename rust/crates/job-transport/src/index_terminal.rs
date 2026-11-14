//! Bounded index task terminal values; classification is a separate capability.
//! Original Dispatcharr/Celery/Kombu authors retain application/framework credit.
use serde_json::{json, Value};
use uuid::Uuid;

const MAX_ARGUMENT: usize = 8192;
const MAX_DIAGNOSTIC: usize = 16384;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureKind {
    Directory,
    Programming,
    CacheConnection,
    CacheAuthentication,
    CachePermission,
    UninitializedCache,
    ChildPublication,
}
impl FailureKind {
    fn identity(self) -> (&'static str, &'static str) {
        match self {
            Self::Directory => ("builtins", "IsADirectoryError"),
            Self::Programming => ("django.db.utils", "ProgrammingError"),
            Self::CacheConnection => ("redis.exceptions", "ConnectionError"),
            Self::CacheAuthentication => ("redis.exceptions", "AuthenticationError"),
            Self::CachePermission => ("redis.exceptions", "NoPermissionError"),
            Self::UninitializedCache => ("builtins", "AttributeError"),
            Self::ChildPublication => ("kombu.exceptions", "OperationalError"),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureError {
    Arguments,
    Capacity,
    Diagnostic,
}
/// Construct only after a separately verified execution-stage classifier has
/// identified a raised error. Transport/control errors are never auto-converted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    kind: FailureKind,
    arguments: Vec<Value>,
    diagnostic: String,
}
impl Failure {
    pub fn new(
        kind: FailureKind,
        arguments: Vec<Value>,
        stage: &str,
        detail: &str,
    ) -> Result<Self, FailureError> {
        let valid = match kind {
            FailureKind::Directory => {
                arguments.len() == 2
                    && arguments[0].as_i64() == Some(21)
                    && arguments[1].as_str() == Some("Is a directory")
            }
            _ => arguments.len() == 1 && arguments[0].is_string(),
        };
        if !valid {
            return Err(FailureError::Arguments);
        }
        if arguments
            .iter()
            .filter_map(Value::as_str)
            .any(|text| text.len() > MAX_ARGUMENT)
        {
            return Err(FailureError::Capacity);
        }
        if stage.is_empty()
            || stage.len() > 128
            || !stage
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(FailureError::Diagnostic);
        }
        const PREFIX: &str = "Native Dispatcharr index task diagnostic";
        if detail.len() > MAX_DIAGNOSTIC {
            return Err(FailureError::Capacity);
        }
        // Celery accepts an opaque traceback string. This truthful native
        // diagnostic does not represent Python frames as having executed.
        let diagnostic = format!("{PREFIX} [{stage}]: {detail}");
        if diagnostic.len() > MAX_DIAGNOSTIC {
            return Err(FailureError::Capacity);
        }
        Ok(Self {
            kind,
            arguments,
            diagnostic,
        })
    }
    /// Same qualified Django/psycopg DTO, with truthful task-specific native
    /// diagnostics. This does not claim Original Python frames executed.
    pub(crate) fn tvg_programme_insert(message: &str) -> Result<Self, FailureError> {
        let mut failure = Self::new(
            FailureKind::Programming,
            vec![json!(message)],
            "programme_insert",
            message,
        )?;
        failure.diagnostic = format!("Native Dispatcharr per-channel programme task diagnostic [programme_insert]: {message}");
        if failure.diagnostic.len() > MAX_DIAGNOSTIC {
            return Err(FailureError::Capacity);
        }
        Ok(failure)
    }
    pub fn kind(&self) -> FailureKind {
        self.kind
    }
    pub(crate) fn metadata(
        &self,
        id: Uuid,
        parent: Option<Uuid>,
        children: &[Uuid],
        date_done: &str,
    ) -> Result<Vec<u8>, FailureError> {
        if children.len() > 1 || date_done.len() > 64 {
            return Err(FailureError::Capacity);
        }
        let (module, class) = self.kind.identity();
        let children: Vec<Value> = children
            .iter()
            .map(|child| json!([[child.to_string(), null], null]))
            .collect();
        let mut value = json!({"status":"FAILURE",
            "result":{"exc_type":class,"exc_message":self.arguments,"exc_module":module},
            "traceback":self.diagnostic,"children":children,"date_done":date_done});
        if let Some(parent) = parent {
            value["parent_id"] = json!(parent.to_string());
        }
        value["task_id"] = json!(id.to_string());
        let bytes = serde_json::to_vec(&value).map_err(|_| FailureError::Arguments)?;
        if bytes.len() > crate::decode::MAX_RESULT_BYTES {
            return Err(FailureError::Capacity);
        }
        Ok(bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Terminal {
    NullSuccess,
    Raised(Failure),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raised_metadata_preserves_typed_arguments_and_context_without_python_frames() {
        let id = Uuid::new_v4();
        let parent = Uuid::new_v4();
        let child = Uuid::new_v4();
        let failure = Failure::new(
            FailureKind::Directory,
            vec![json!(21), json!("Is a directory")],
            "file_open",
            "Owned directory",
        )
        .unwrap();
        let value: Value = serde_json::from_slice(
            &failure
                .metadata(
                    id,
                    Some(parent),
                    &[child],
                    "2026-11-10T00:00:00.000000+00:00",
                )
                .unwrap(),
        )
        .unwrap();
        assert_eq!(value["status"], "FAILURE");
        assert_eq!(
            value["result"],
            json!({"exc_type":"IsADirectoryError","exc_module":"builtins","exc_message":[21,"Is a directory"]})
        );
        assert_eq!(value["task_id"], id.to_string());
        assert_eq!(value["parent_id"], parent.to_string());
        assert_eq!(
            value["children"],
            json!([[[child.to_string(), null], null]])
        );
        assert!(value["traceback"]
            .as_str()
            .unwrap()
            .starts_with("Native Dispatcharr index task diagnostic [file_open]:"));
    }

    #[test]
    fn unproven_shapes_and_unbounded_diagnostics_do_not_authorize_failure_metadata() {
        assert_eq!(
            Failure::new(
                FailureKind::Directory,
                vec![json!(21.0), json!("Is a directory")],
                "file_open",
                ""
            ),
            Err(FailureError::Arguments)
        );
        assert_eq!(
            Failure::new(
                FailureKind::Programming,
                vec![json!(false)],
                "index_write",
                ""
            ),
            Err(FailureError::Arguments)
        );
        assert_eq!(
            Failure::new(
                FailureKind::Programming,
                vec![json!("message")],
                "python frame\n",
                ""
            ),
            Err(FailureError::Diagnostic)
        );
        assert_eq!(
            Failure::new(
                FailureKind::Programming,
                vec![json!("x".repeat(MAX_ARGUMENT + 1))],
                "index_write",
                ""
            ),
            Err(FailureError::Capacity)
        );
        assert_eq!(
            Failure::new(
                FailureKind::Programming,
                vec![json!("message")],
                "index_write",
                &"x".repeat(MAX_DIAGNOSTIC)
            ),
            Err(FailureError::Capacity)
        );
    }

    #[test]
    fn diagnostic_limit_counts_prefix_stage_and_utf8_bytes() {
        let overhead = "Native Dispatcharr index task diagnostic [index_write]: ".len();
        let limit = MAX_DIAGNOSTIC - overhead;
        let accepted = Failure::new(
            FailureKind::Programming,
            vec![json!("message")],
            "index_write",
            &"x".repeat(limit),
        )
        .unwrap();
        assert_eq!(accepted.diagnostic.len(), MAX_DIAGNOSTIC);
        assert_eq!(
            Failure::new(
                FailureKind::Programming,
                vec![json!("message")],
                "index_write",
                &"x".repeat(limit + 1)
            ),
            Err(FailureError::Capacity)
        );
        let unicode = format!("{}é", "x".repeat(limit - 1));
        assert_eq!(
            Failure::new(
                FailureKind::Programming,
                vec![json!("message")],
                "index_write",
                &unicode
            ),
            Err(FailureError::Capacity)
        );
    }
}
