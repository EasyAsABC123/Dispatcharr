//! Opt-in loopback listener for the selected direct ModelAdmin read.
use axum::Router;
use dispatcharr_store::AuthService;
use std::{ffi::OsStr, io, net::SocketAddr};
use tokio::{net::TcpListener, sync::watch};

pub(crate) async fn prepare(auth: AuthService) -> Result<Option<(TcpListener, Router)>, String> {
    let Some(address) = configured(std::env::var_os("DISPATCHARR_DIRECT_ADMIN_BIND").as_deref())?
    else {
        return Ok(None);
    };
    let router = auth
        .direct_channel_group_admin_router(address)
        .ok_or("Invalid dedicated admin address")?;
    let listener = TcpListener::bind(address)
        .await
        .map_err(|_| "Cannot bind dedicated loopback admin listener")?;
    Ok(Some((listener, router)))
}

fn configured(value: Option<&OsStr>) -> Result<Option<SocketAddr>, &'static str> {
    let Some(value) = value else {
        return Ok(None);
    };
    value
        .to_str()
        .and_then(|value| value.parse::<SocketAddr>().ok())
        .filter(|address| address.ip().is_loopback() && address.port() != 0)
        .map(Some)
        .ok_or("Dedicated admin requires an explicit loopback address and nonzero port")
}

pub(crate) async fn serve(
    server: Option<(TcpListener, Router)>,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    if let Some((listener, router)) = server {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = stop.changed().await;
            })
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_off_and_accepts_only_explicit_loopback_binding() {
        assert_eq!(configured(None), Ok(None));
        for address in ["127.0.0.1:5656", "[::1]:5656"] {
            assert_eq!(
                configured(Some(OsStr::new(address))),
                Ok(address.parse().ok())
            );
        }
        for address in [
            "",
            "localhost:5656",
            "127.0.0.1:0",
            "0.0.0.0:5656",
            "[::]:5656",
        ] {
            assert!(configured(Some(OsStr::new(address))).is_err());
        }
    }
}
