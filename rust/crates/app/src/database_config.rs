//! Original Dispatcharr authors retain application/configuration credit.
use std::path::Path;

pub fn url() -> Result<String, String> {
    from_environment(
        |name| std::env::var(name).ok(),
        |path| Path::new(path).is_file(),
    )
}

fn from_environment(
    read: impl Fn(&str) -> Option<String>,
    is_file: impl Fn(&str) -> bool,
) -> Result<String, String> {
    // Source selects SQLite before PostgreSQL. Never silently connect to a
    // different store while the required SQLite backend is still unfinished.
    if read("DB_ENGINE").as_deref() == Some("sqlite") {
        return Err("SQLite backend is not yet implemented in this incomplete Rust port".into());
    }
    // Preserve this existing Rust-only explicit URL override.
    if let Some(value) = read("DATABASE_URL") {
        return Ok(value);
    }
    let value = |name, default: &str| read(name).unwrap_or_else(|| default.into());
    let mut url = url::Url::parse("postgresql://localhost/dispatcharr").unwrap();
    let host = value("POSTGRES_HOST", "localhost");
    let socket = host.is_empty() || host.starts_with('/');
    if socket {
        // The pinned Debian libpq default socket directory. SQLx accepts its
        // explicit path through the host query parameter.
        url.query_pairs_mut().append_pair(
            "host",
            if host.is_empty() {
                "/var/run/postgresql"
            } else {
                &host
            },
        );
    } else {
        let host = if host.parse::<std::net::Ipv6Addr>().is_ok() {
            format!("[{host}]")
        } else {
            host
        };
        url.set_host(Some(&host))
            .map_err(|_| "Invalid POSTGRES_HOST")?;
    }
    url.set_port(Some(
        value("POSTGRES_PORT", "5432")
            .parse()
            .map_err(|_| "Invalid POSTGRES_PORT")?,
    ))
    .map_err(|_| "Invalid POSTGRES_PORT")?;
    // Discrete settings are already literal. URL setters leave existing percent
    // sequences intact, while SQLx decodes them on connection.
    let escape = |text: String| text.replace('%', "%25");
    url.set_username(&escape(value("POSTGRES_USER", "dispatch")))
        .map_err(|_| "Invalid POSTGRES_USER")?;
    let password = read("POSTGRES_PASSWORD").ok_or("POSTGRES_PASSWORD is required")?;
    url.set_password(Some(&escape(password)))
        .map_err(|_| "Invalid POSTGRES_PASSWORD")?;
    url.set_path(&escape(value("POSTGRES_DB", "dispatcharr")));
    if value("POSTGRES_SSL", "false").eq_ignore_ascii_case("true") {
        let paths = [
            ("POSTGRES_SSL_CA_CERT", "sslrootcert"),
            ("POSTGRES_SSL_CERT", "sslcert"),
            ("POSTGRES_SSL_KEY", "sslkey"),
        ]
        .map(|(name, parameter)| (name, parameter, value(name, "")));
        for (name, _, path) in &paths {
            if !path.is_empty() && !is_file(path) {
                return Err(format!("PostgreSQL TLS: {name}={path:?} — file not found. Check that the certificate file exists and the volume is mounted correctly."));
            }
        }
        let requested = value("POSTGRES_SSL_MODE", "verify-full");
        if !matches!(
            requested.as_str(),
            "disable" | "allow" | "prefer" | "require" | "verify-ca" | "verify-full"
        ) {
            return Err("Invalid POSTGRES_SSL_MODE".into());
        }
        // libpq ignores SSL on Unix sockets; require with an explicit CA is
        // verify-ca. SQLx's raw modes do not implement either compatibility rule.
        let mode = if socket {
            "disable"
        } else if requested == "require" && !paths[0].2.is_empty() {
            "verify-ca"
        } else {
            &requested
        };
        let mut query = url.query_pairs_mut();
        query.append_pair("sslmode", mode);
        for (_, parameter, path) in paths {
            if !path.is_empty() {
                query.append_pair(parameter, &path);
            }
        }
    } else if socket {
        url.query_pairs_mut().append_pair("sslmode", "disable");
    }
    Ok(url.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn build(values: &[(&str, &str)], files: &[&str]) -> Result<String, String> {
        from_environment(
            |name| {
                values
                    .iter()
                    .find(|row| row.0 == name)
                    .map(|row| row.1.into())
            },
            |path| files.contains(&path),
        )
    }
    #[test]
    fn socket_ipv6_and_require_ca_follow_source_connection_selection() {
        for host in ["", "/var/run/postgresql", "/owned/socket dir"] {
            let text = build(
                &[
                    ("POSTGRES_PASSWORD", "synthetic"),
                    ("POSTGRES_HOST", host),
                    ("POSTGRES_SSL", "true"),
                ],
                &[],
            )
            .unwrap();
            let url = url::Url::parse(&text).unwrap();
            let pairs = url
                .query_pairs()
                .collect::<std::collections::BTreeMap<_, _>>();
            assert_eq!(pairs.get("sslmode").unwrap(), "disable");
            assert_eq!(
                pairs.get("host").unwrap(),
                if host.is_empty() {
                    "/var/run/postgresql"
                } else {
                    host
                }
            );
        }
        let text = build(
            &[("POSTGRES_PASSWORD", "synthetic"), ("POSTGRES_HOST", "::1")],
            &[],
        )
        .unwrap();
        assert_eq!(url::Url::parse(&text).unwrap().host_str(), Some("[::1]"));
        let text = build(
            &[
                ("POSTGRES_PASSWORD", "synthetic"),
                ("POSTGRES_SSL", "true"),
                ("POSTGRES_SSL_MODE", "require"),
                ("POSTGRES_SSL_CA_CERT", "/owned/ca.pem"),
            ],
            &["/owned/ca.pem"],
        )
        .unwrap();
        assert!(url::Url::parse(&text)
            .unwrap()
            .query_pairs()
            .any(|(k, v)| k == "sslmode" && v == "verify-ca"));
        assert!(build(
            &[
                ("POSTGRES_PASSWORD", "synthetic"),
                ("POSTGRES_SSL", "true"),
                ("POSTGRES_SSL_MODE", "VERIFY-FULL")
            ],
            &[]
        )
        .is_err());
    }
    #[test]
    fn source_sqlite_selection_cannot_silently_use_postgres_or_explicit_url() {
        let error = build(
            &[
                ("DB_ENGINE", "sqlite"),
                ("DATABASE_URL", "postgresql://ignored"),
            ],
            &[],
        )
        .unwrap_err();
        assert!(error.starts_with("SQLite backend is not yet implemented"));
    }
    #[test]
    fn postgres_defaults_and_existing_explicit_url_are_preserved() {
        let text = build(&[("POSTGRES_PASSWORD", "synthetic /@?#")], &[]).unwrap();
        let url = url::Url::parse(&text).unwrap();
        assert_eq!(url.host_str(), Some("localhost"));
        assert_eq!(url.username(), "dispatch");
        assert_eq!(url.path(), "/dispatcharr");
        assert_eq!(url.query(), None);
        let literal = "postgresql://owned.invalid/owned?sslmode=require";
        assert_eq!(
            build(
                &[
                    ("DATABASE_URL", literal),
                    ("POSTGRES_SSL", "true"),
                    ("POSTGRES_SSL_KEY", "/missing")
                ],
                &[]
            )
            .unwrap(),
            literal
        );
    }
    #[test]
    fn discrete_postgres_values_keep_literal_percent_sequences() {
        let encoded = build(
            &[
                ("POSTGRES_USER", "owned%2Fname"),
                ("POSTGRES_PASSWORD", "owned %2F /@?#"),
                ("POSTGRES_DB", "owned%2Fdb"),
            ],
            &[],
        )
        .unwrap();
        let parsed = url::Url::parse(&encoded).unwrap();
        assert_eq!(parsed.username(), "owned%252Fname");
        assert!(parsed.password() == Some("owned%20%252F%20%2F%40%3F%23"));
        assert_eq!(parsed.path(), "/owned%252Fdb");
    }

    #[test]
    fn tls_paths_are_encoded_and_missing_files_fail_before_connection() {
        let values = [
            ("POSTGRES_PASSWORD", "synthetic"),
            ("POSTGRES_SSL", "TrUe"),
            ("POSTGRES_SSL_CA_CERT", "/owned/CA file.pem"),
            ("POSTGRES_SSL_CERT", "/owned/client.pem"),
            ("POSTGRES_SSL_KEY", "/owned/key.pem"),
        ];
        let text = build(
            &values,
            &["/owned/CA file.pem", "/owned/client.pem", "/owned/key.pem"],
        )
        .unwrap();
        let url = url::Url::parse(&text).unwrap();
        let parameters = url
            .query_pairs()
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(parameters.get("sslmode").unwrap(), "verify-full");
        assert_eq!(parameters.get("sslrootcert").unwrap(), "/owned/CA file.pem");
        assert_eq!(parameters.get("sslcert").unwrap(), "/owned/client.pem");
        assert_eq!(parameters.get("sslkey").unwrap(), "/owned/key.pem");
        assert!(build(&values, &[])
            .unwrap_err()
            .contains("POSTGRES_SSL_CA_CERT"));
        assert!(build(
            &[
                ("POSTGRES_PASSWORD", "synthetic"),
                ("POSTGRES_SSL", "1"),
                ("POSTGRES_SSL_KEY", "/missing")
            ],
            &[]
        )
        .is_ok());
    }
}
