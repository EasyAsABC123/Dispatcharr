use crate::connection::ConnectOptions;
use crate::error::Error;
use crate::{PgConnectOptions, PgConnection, PgSslMode};
use futures_core::future::BoxFuture;
use log::LevelFilter;
use sqlx_core::Url;
use std::time::Duration;

impl ConnectOptions for PgConnectOptions {
    type Connection = PgConnection;

    fn from_url(url: &Url) -> Result<Self, Error> {
        Self::parse_from_url(url)
    }

    fn to_url_lossy(&self) -> Url {
        self.build_url()
    }

    fn connect(&self) -> BoxFuture<'_, Result<Self::Connection, Error>>
    where
        Self::Connection: Sized,
    {
        Box::pin(async move {
            // libpq allow preserves plaintext preference, then retries TLS after
            // a PostgreSQL startup/authentication ErrorResponse. This is per
            // physical connection, including all existing and replacement pools.
            // Never reinterpret a query error or retry an established session.
            let result = PgConnection::establish(self).await;
            if matches!(self.ssl_mode, PgSslMode::Allow)
                && self.fetch_socket().is_none()
                && result
                    .as_ref()
                    .err()
                    .is_some_and(|error| matches!(error, Error::Database(_)))
            {
                let mut encrypted = self.clone();
                encrypted.ssl_mode = if self.ssl_root_cert.is_some() {
                    PgSslMode::VerifyCa
                } else {
                    PgSslMode::Require
                };
                PgConnection::establish(&encrypted).await
            } else {
                result
            }
        })
    }

    fn log_statements(mut self, level: LevelFilter) -> Self {
        self.log_settings.log_statements(level);
        self
    }

    fn log_slow_statements(mut self, level: LevelFilter, duration: Duration) -> Self {
        self.log_settings.log_slow_statements(level, duration);
        self
    }
}
