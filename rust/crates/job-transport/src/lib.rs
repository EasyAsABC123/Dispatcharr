//! App-unwired Source Celery transport component under development.
//! Original Dispatcharr authors retain full application credit and AGPL licensing.
//! No production router or HTTP202 authority is provided. The worker is fixture-only.
pub mod decode;
pub mod eta;
pub mod index_dispatch;
pub mod index_protocol;
pub mod index_worker;
pub mod protocol;
pub mod scheduled_index;
pub mod transport;
pub mod worker;

pub mod index_driver;

// Component-only visibility recovery; production driver has no recovery hook.
#[cfg(test)]
pub mod visibility;

// Bounded observed exception payload codec; worker classification/writer attachment are separate.
#[cfg(test)]
pub mod index_failure;
