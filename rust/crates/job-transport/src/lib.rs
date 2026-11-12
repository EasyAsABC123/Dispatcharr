//! App-unwired Source Celery transport component under development.
//! Original Dispatcharr authors retain full application credit and AGPL licensing.
//! No HTTP202 router or scheduler authority is provided by this crate.
//! Explicit index/refresh constructors restrict consumer and publication families.
pub mod decode;
pub mod eta;
pub mod index_dispatch;
pub mod index_protocol;
pub mod index_terminal;
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

// Source-first bounded P0001 field renderer; classification is stage-scoped.
pub mod pg_failure_format;

// Known observed stage mappings only; production service adapter is unchanged.
#[cfg(test)]
pub mod index_classifier;

// Observed bounded refresh wire shapes; raw Source API shapes beyond this model remain unsupported.
pub mod refresh_protocol;

// Immutable refresh child-lineage adapter; broker authority is explicit in Jobs.
pub mod refresh_dispatch;

// Observed bounded normal refresh result decoder.
pub mod refresh_result;

// Fixture-only refresh publication/result interoperability; no consumer.
#[cfg(test)]
pub mod refresh_publish;

// Explicit immediate refresh execution; unrelated/ETA heads are not consumed.
pub mod refresh_worker;

// Controlled-clock interval decision component; no scheduler/application hook.
pub mod interval_schedule;

// Bounded supervised immediate-refresh polling; unsupported families stay queued.
pub mod refresh_driver;

// Bounded positive-ID refresh HTTP assembly with explicit application configuration.
pub mod refresh_application;

pub mod index_application;

pub mod series_protocol;

pub mod series_worker;

pub mod series_driver;

pub mod series_application;

pub mod refresh_eta;

pub mod scheduled_refresh;

pub mod scheduled_refresh_driver;

pub mod database_schedule;

mod periodic_headers;

#[cfg(test)]
mod stats_protocol;
#[cfg(test)]
mod stats_worker;
#[cfg(test)]
mod stats_peer;

#[cfg(test)]
mod stats_resources;

pub mod default_protocol;

pub mod default_domains;

pub mod default_worker;

pub mod default_application;

#[cfg(test)]
mod default_peer;

#[cfg(test)]
mod default_resources;

mod inactive_notifications;

#[cfg(test)]
mod inactive_resources;

pub mod cleanup_expiry;

#[cfg(test)]
mod default_expiry_tests;

#[cfg(test)]
mod expiry_resources;

pub mod active_notifications;

#[cfg(test)]
mod active_resources;

pub mod notification_terminal;

#[cfg(test)]
mod notification_failure_resources;

// Inert bounded prototype; no intake/task/result/Main registration.
pub mod logo_scanner;
pub mod logo_scanner_effects;
#[cfg(test)]
mod logo_scanner_tests;

#[cfg(test)]
mod logo_scanner_peer;

#[cfg(test)]
mod logo_scanner_resources;

pub mod default_task_terminal;
pub mod logo_task;
pub mod logo_application;

#[cfg(test)]
mod logo_task_peer;

#[cfg(test)]
mod logo_task_resources;

// Explicit scanner first-index helper; no default application registration.
pub mod scanner_index_helper;
#[cfg(test)]
mod scanner_index_peer;

#[cfg(test)]
mod logo_spool_peer;

#[cfg(test)]
mod logo_spool_resources;
