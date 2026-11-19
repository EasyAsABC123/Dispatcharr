//! Original Dispatcharr authors retain application and configuration credit.
use url::Url;

pub fn url() -> Result<Option<String>, String> {
    from_environment(|name| std::env::var(name).ok()).map_err(str::to_owned)
}

fn from_environment(read: impl Fn(&str) -> Option<String>) -> Result<Option<String>, &'static str> {
    const TLS_UNAVAILABLE: &str = "Redis TLS transport is unavailable in this Rust build";
    let tls = read("REDIS_SSL").is_some_and(|value| value.eq_ignore_ascii_case("true"));
    if let Some(address) = read("REDIS_URL").filter(|value| !value.is_empty()) {
        let secure_url =
            Url::parse(&address).is_ok_and(|url| matches!(url.scheme(), "rediss" | "valkeys"));
        return match (tls, secure_url) {
            (true, false) => Err("REDIS_SSL=true requires a TLS Redis URL"),
            (false, true) => Err("A TLS Redis URL requires REDIS_SSL=true"),
            (true, true) => Err(TLS_UNAVAILABLE),
            (false, false) => Ok(Some(address)),
        };
    }
    if tls {
        return Err(TLS_UNAVAILABLE);
    }
    let defaults = [
        ("REDIS_HOST", "localhost"),
        ("REDIS_PORT", "6379"),
        ("REDIS_DB", "0"),
        ("REDIS_USER", ""),
        ("REDIS_PASSWORD", ""),
    ];
    let overrides = defaults.map(|(name, _)| read(name));
    if overrides.iter().all(Option::is_none) {
        return Ok(None);
    }
    let [host, port, database, username, password] =
        std::array::from_fn(|index| overrides[index].as_deref().unwrap_or(defaults[index].1));
    if host.is_empty() {
        return Err("Invalid REDIS_HOST");
    }
    let port = port.parse().map_err(|_| "Invalid REDIS_PORT")?;
    let database: u32 = database.parse().map_err(|_| "Invalid REDIS_DB")?;
    let mut address = Url::parse("redis://localhost:6379/0").expect("static Redis URL");
    let ipv6 = host.parse::<std::net::Ipv6Addr>().ok();
    let ipv6_host = ipv6.map(|value| format!("[{value}]"));
    address
        .set_host(Some(ipv6_host.as_deref().unwrap_or(host)))
        .map_err(|_| "Invalid REDIS_HOST")?;
    address
        .set_port(Some(port))
        .map_err(|_| "Invalid REDIS_PORT")?;
    address.set_path(&database.to_string());
    if !password.is_empty() {
        // The Redis URL parser percent-decodes userinfo. Encode literal percent
        // sequences and spaces before the URL setters, which retain percent escapes.
        let encode = |text: &str| {
            url::form_urlencoded::byte_serialize(text.as_bytes())
                .collect::<String>()
                .replace('+', "%20")
        };
        address
            .set_username(&encode(username))
            .map_err(|_| "Invalid REDIS_USER")?;
        address
            .set_password(Some(&encode(password)))
            .map_err(|_| "Invalid REDIS_PASSWORD")?;
    }
    Ok(Some(address.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(values: &[(&str, &str)]) -> Result<Option<String>, &'static str> {
        from_environment(|name| {
            values
                .iter()
                .find_map(|(key, value)| (*key == name).then(|| value.to_string()))
        })
    }

    #[test]
    fn explicit_url_and_unconfigured_mode_keep_existing_behavior() {
        assert_eq!(build(&[]), Ok(None));
        assert_eq!(
            build(&[
                ("REDIS_URL", "unix:///owned.sock?db=4"),
                ("REDIS_HOST", "ignored")
            ]),
            Ok(Some("unix:///owned.sock?db=4".to_owned()))
        );
        assert_eq!(
            build(&[
                ("REDIS_URL", ""),
                ("REDIS_HOST", "redis"),
                ("REDIS_PORT", "6380"),
                ("REDIS_DB", "4")
            ]),
            Ok(Some("redis://redis:6380/4".to_owned()))
        );
    }

    #[test]
    fn legacy_environment_resolves_acl_and_password_only_addresses() {
        assert_eq!(
            build(&[
                ("REDIS_HOST", "redis"),
                ("REDIS_USER", "owned +%2F"),
                ("REDIS_PASSWORD", "fixture:@/+# %2F")
            ]),
            Ok(Some(
                "redis://owned%20%2B%252F:fixture%3A%40%2F%2B%23%20%252F@redis:6379/0".to_owned()
            ))
        );
        assert_eq!(
            build(&[("REDIS_PASSWORD", "fixture-only")]),
            Ok(Some("redis://:fixture-only@localhost:6379/0".to_owned()))
        );
        assert_eq!(
            build(&[
                ("REDIS_HOST", "redis"),
                ("REDIS_USER", "ignored-without-password")
            ]),
            Ok(Some("redis://redis:6379/0".to_owned()))
        );
    }

    #[test]
    fn explicit_redis_url_cannot_bypass_the_tls_requirement() {
        assert_eq!(
            build(&[("REDIS_URL", "redis://redis:6379/0"), ("REDIS_SSL", "TRUE")]),
            Err("REDIS_SSL=true requires a TLS Redis URL")
        );
        assert_eq!(
            build(&[("REDIS_URL", "unix:///owned.sock"), ("REDIS_SSL", "true")]),
            Err("REDIS_SSL=true requires a TLS Redis URL")
        );
        assert_eq!(
            build(&[("REDIS_URL", "rediss://redis:6379/0")]),
            Err("A TLS Redis URL requires REDIS_SSL=true")
        );
        assert_eq!(
            build(&[
                ("REDIS_URL", "rediss://redis:6379/0"),
                ("REDIS_SSL", "true")
            ]),
            Err("Redis TLS transport is unavailable in this Rust build")
        );
    }

    #[test]
    fn invalid_discrete_values_and_implicit_tls_do_not_connect_as_plaintext() {
        assert_eq!(build(&[("REDIS_HOST", "")]), Err("Invalid REDIS_HOST"));
        assert_eq!(build(&[("REDIS_DB", "-1")]), Err("Invalid REDIS_DB"));
        assert_eq!(build(&[("REDIS_PORT", "bad")]), Err("Invalid REDIS_PORT"));
        assert_eq!(
            build(&[("REDIS_HOST", "redis"), ("REDIS_SSL", "TrUe")]),
            Err("Redis TLS transport is unavailable in this Rust build")
        );
    }
}
