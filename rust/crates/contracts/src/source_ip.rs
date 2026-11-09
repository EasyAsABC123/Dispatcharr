//! Original Dispatcharr/Python IP normalization, including scoped IPv6 text.
//! Application credit remains with the original Dispatcharr contributors.
use std::{fmt, net::IpAddr, str::FromStr};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SourceIpAddr {
    address: IpAddr,
    scope: Option<String>,
}
impl SourceIpAddr {
    pub fn address(&self) -> IpAddr {
        self.address
    }
    pub fn scope(&self) -> Option<&str> {
        self.scope.as_deref()
    }
}
impl From<IpAddr> for SourceIpAddr {
    fn from(address: IpAddr) -> Self {
        let address = match address {
            IpAddr::V6(value) => value.to_ipv4_mapped().map_or(address, IpAddr::V4),
            value => value,
        };
        Self {
            address,
            scope: None,
        }
    }
}
impl FromStr for SourceIpAddr {
    type Err = &'static str;
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        // Python str.strip additionally recognizes these four C0 separators.
        let raw = raw.trim_matches(|value: char| {
            value.is_whitespace() || matches!(value, '\u{1c}'..='\u{1f}')
        });
        let (address, scope) = if let Some((address, scope)) = raw.split_once('%') {
            if scope.is_empty() || scope.contains('%') {
                return Err("invalid scoped address");
            }
            let address = address
                .parse::<std::net::Ipv6Addr>()
                .map_err(|_| "invalid IPv6 address")?;
            (IpAddr::V6(address), Some(scope))
        } else {
            (
                raw.parse::<IpAddr>().map_err(|_| "invalid IP address")?,
                None,
            )
        };
        let mut normalized = Self::from(address);
        if matches!(normalized.address, IpAddr::V6(_)) {
            normalized.scope = scope.map(str::to_owned)
        }
        Ok(normalized)
    }
}
impl fmt::Display for SourceIpAddr {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.address.fmt(output)?;
        if let Some(scope) = &self.scope {
            write!(output, "%{scope}")?
        }
        Ok(())
    }
}
