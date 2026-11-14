//! Compile the exact patched SQLx TLS module without SQLx's all-backend devdeps.
//! Original SQLx/rustls and Dispatcharr authors retain all respective credit.
//! No verifier implementation is copied, extracted, stubbed or reimplemented.
#![allow(dead_code, clippy::all)]
pub mod error {
    pub use sqlx_core::error::Error;
}
pub mod io {
    pub use sqlx_core::io::ReadBuf;
}
pub mod net {
    pub use sqlx_core::net::Socket;
    pub mod tls {
        pub(crate) use crate::upstream_util as util;
        pub use sqlx_core::net::tls::TlsConfig;
    }
}
#[path = "../../../vendor/sqlx-core-0.8.6/src/net/tls/tls_rustls.rs"]
mod upstream_tls_rustls;
#[path = "../../../vendor/sqlx-core-0.8.6/src/net/tls/util.rs"]
pub(crate) mod upstream_util;
