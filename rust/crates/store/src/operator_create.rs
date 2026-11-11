//! Bounded noninteractive Original createsuperuser component.
//! Original Dispatcharr and Django authors retain application/algorithm credit.
use crate::{operator_commands::OperatorError, Store};
use rand::{distributions::Alphanumeric, Rng};
use sqlx::Connection;
use std::sync::Arc;

#[derive(Clone, PartialEq, Eq)]
pub struct CreateSuperuser {
    username: Option<String>,
    email: Option<String>,
    password: Option<String>,
    verbosity: u8,
}
impl std::fmt::Debug for CreateSuperuser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CreateSuperuser { credentials: [redacted] }")
    }
}
impl CreateSuperuser {
    /// Explicit --noinput grammar; environment values are snapshotted by the
    /// caller, never read from inside a retained operation or printed in errors.
    pub fn parse(
        args: &[String],
        environment: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, OperatorError> {
        let mut username = None;
        let mut email = None;
        let mut verbosity = 1;
        let mut noinput = false;
        let mut index = 0;
        while let Some(arg) = args.get(index) {
            index += 1;
            if arg == "--noinput" || arg == "--no-input" {
                noinput = true;
                continue;
            }
            let (name, value) = if let Some((name, value)) = arg.split_once('=') {
                (name, value.to_owned())
            } else {
                let value = args.get(index).ok_or(OperatorError::Arguments)?;
                index += 1;
                if value.starts_with('-') {
                    return Err(OperatorError::Arguments);
                }
                (arg.as_str(), value.clone())
            };
            if value.len() > 4096 {
                return Err(OperatorError::Capacity);
            }
            match name {
                "--username" => username = Some(value),
                "--email" => email = Some(value),
                "--verbosity" | "-v" => {
                    verbosity = value.parse().map_err(|_| OperatorError::Arguments)?;
                    if verbosity > 3 {
                        return Err(OperatorError::Arguments);
                    }
                }
                _ => return Err(OperatorError::Arguments),
            }
        }
        if !noinput {
            return Err(OperatorError::Arguments);
        }
        if username.is_none() {
            username = environment("DJANGO_SUPERUSER_USERNAME");
        }
        let environment_email = environment("DJANGO_SUPERUSER_EMAIL");
        // Django uses the environment for an empty option, but preserves an
        // explicitly empty field when no nonempty environment value exists.
        if email.as_deref().is_none_or(str::is_empty) {
            email = environment_email.or(email);
        }
        let password = environment("DJANGO_SUPERUSER_PASSWORD");
        if [&username, &email, &password]
            .iter()
            .any(|value| value.as_ref().is_some_and(|v| v.len() > 4096))
        {
            return Err(OperatorError::Capacity);
        }
        Ok(Self {
            username,
            email,
            password,
            verbosity,
        })
    }

    pub(crate) async fn execute(
        self,
        store: &Store,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<String, OperatorError> {
        let command = |message: &str| OperatorError::Command(message.to_owned());
        let raw = self
            .username
            .ok_or_else(|| command("You must use --username with --noinput."))?;
        let mut connection = store.pool().acquire().await?;
        let result: Result<String, OperatorError> = async {
        // Source checks the raw natural key before field validation; the manager
        // normalizes only later. Do not convert this into an upsert or admin-exists
        // guard. Concurrent/normalized collisions retain the database failure.
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM accounts_user WHERE username=$1)")
                .bind(&raw)
                .fetch_one(&mut *connection)
                .await?;
        if exists {
            return Err(command("Error: That username is already taken."));
        }
        if raw.is_empty() {
            return Err(command("Username cannot be blank."));
        }
        if raw.chars().count() > 150 {
            return Err(command("Username exceeds supported field length."));
        }
        // Narrow accepted domain: ASCII Django username characters plus fullwidth
        // Latin/digits, whose actual NFKC manager behavior is captured. Other
        // Unicode categories require a separate original validator acceptance.
        for c in raw.chars() {
            if c.is_ascii() {
                if !(c.is_ascii_alphanumeric() || "_@.+-".contains(c)) {
                    return Err(command("Enter a valid username. This value may contain only letters, numbers, and @/./+/-/_ characters."));
                }
            } else if !(('\u{ff10}'..='\u{ff19}').contains(&c)
                || ('\u{ff21}'..='\u{ff3a}').contains(&c)
                || ('\u{ff41}'..='\u{ff5a}').contains(&c))
            {
                return Err(OperatorError::UnsupportedCreateInput);
            }
        }
        let email = self
            .email
            .ok_or_else(|| command("You must use --email with --noinput."))?;
        if email.len() > 254 {
            return Err(command("Email exceeds supported field length."));
        }
        if !email.is_ascii() || email.contains(['"', '[', ']', '\\']) {
            return Err(OperatorError::UnsupportedCreateInput);
        }
        if !email.is_empty() {
            let Some((local, domain)) = email.rsplit_once('@') else {
                return Err(command("Enter a valid email address."));
            };
            let last = domain
                .rsplit('.')
                .next()
                .unwrap_or_default()
                .to_ascii_lowercase();
            let top_level = (last.len() >= 2
                && last.bytes().all(|c| c.is_ascii_alphabetic() || c == b'-'))
                || last.strip_prefix("xn--").is_some_and(|tail| {
                    !tail.is_empty()
                        && tail.len() <= 59
                        && tail.bytes().all(|c| c.is_ascii_alphanumeric())
                });
            if local.is_empty()
                || local.starts_with('.')
                || local.ends_with('.')
                || local.contains("..")
                || !local
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || ".!#$%&'*+-/=?^_`{|}~".contains(c))
                || !(domain == "localhost" || (domain.split('.').count() >= 2 && top_level))
                || domain.split('.').any(|s| {
                    s.is_empty()
                        || s.starts_with('-')
                        || s.ends_with('-')
                        || s.len() > 63
                        || !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
                })
            {
                return Err(command("Enter a valid email address."));
            }
        }
        let email = if let Some((local, domain)) = email.rsplit_once('@') {
            format!("{local}@{}", domain.to_ascii_lowercase())
        } else {
            email
        };
        let username = crate::username::normalize_username(&raw);
        // The outer supervised single admission retains physical ownership even
        // if the observer is cancelled during this non-abortable CPU operation.
        let password = self.password;
        let encoded = tokio::task::spawn_blocking(move || {
            let _caller = caller;
            match password {
                Some(password) => crate::passwords::encode_password(&password),
                None => Some(format!(
                    "!{}",
                    rand::rngs::OsRng
                        .sample_iter(&Alphanumeric)
                        .take(40)
                        .map(char::from)
                        .collect::<String>()
                )),
            }
        })
        .await
        .map_err(|_| OperatorError::Unavailable)?
        .ok_or(OperatorError::Capacity)?;
        sqlx::query("INSERT INTO accounts_user(password,last_login,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined,user_level,custom_properties,avatar_config,stream_limit,api_key) VALUES($1,NULL,TRUE,$2,'','',$3,TRUE,TRUE,NOW(),10,'{}'::jsonb,'{}'::jsonb,0,NULL)")
            .bind(encoded).bind(username).bind(email).execute(&mut *connection).await?;
        Ok(if self.verbosity >= 1 {
            "Superuser created successfully.\n".into()
        } else {
            String::new()
        })
        }.await;
        // SQLx may report ERROR before trailing ReadyForQuery. Keep the actual
        // connection (and the outer physical caller/admission) through a Sync
        // barrier and explicit close; never leave that drain to pool Drop.
        let settled = connection.ping().await;
        let closed = connection.close().await;
        if settled.is_err() || closed.is_err() {
            return Err(OperatorError::Unavailable);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn environment_missing_and_empty_password_are_distinct_and_redacted() {
        let args = [
            "--noinput".to_string(),
            "--username=owned".to_string(),
            "--email=".to_string(),
        ];
        let missing = CreateSuperuser::parse(&args, |_| None).unwrap();
        let empty = CreateSuperuser::parse(&args, |k| {
            (k == "DJANGO_SUPERUSER_PASSWORD").then(String::new)
        })
        .unwrap();
        assert!(missing.password.is_none());
        assert_eq!(empty.password.as_deref(), Some(""));
        assert_ne!(missing, empty);
        assert_eq!(
            format!("{missing:?}"),
            "CreateSuperuser { credentials: [redacted] }"
        );
    }
    #[test]
    fn environment_precedence_and_refusal() {
        let args = [
            "--noinput".into(),
            "--username=argument".into(),
            "--email=".into(),
        ];
        let parsed = CreateSuperuser::parse(&args, |k| Some(k.to_owned())).unwrap();
        assert_eq!(parsed.username.as_deref(), Some("argument"));
        assert_eq!(parsed.email.as_deref(), Some("DJANGO_SUPERUSER_EMAIL"));
        assert!(CreateSuperuser::parse(&[], |_| None).is_err());
        assert!(
            CreateSuperuser::parse(&["--noinput".into(), "--unknown".into()], |_| None).is_err()
        );
        assert!(CreateSuperuser::parse(&["--noinput".into()], |_| Some("x".repeat(4097))).is_err());
    }
}
