//! Unwired XC read-output draft. No native route or parity acceptance yet.
//! Original Dispatcharr authors retain full application/algorithm credit.
//! See ATTRIBUTION.md and the repository AGPL-3.0 license.
pub mod counts;
pub mod epg;
pub mod epg_hash;
mod gateway;
pub mod numbers;
mod project;
mod reader;
pub use gateway::{LiveDispatcher, XcConfig, XcGateway};

// Private Source38-backed VOD categories; no app/Main wiring change.
mod vod;

// Private DTO primitives: not routed until row/resource/HTTP gates pass.
#[cfg(test)]
mod vod_json;
#[cfg(test)]
mod vod_rating;

#[cfg(test)]
mod vod_lists;
#[cfg(test)]
mod vod_priority;
#[cfg(test)]
mod vod_properties;

#[cfg(test)]
mod vod_object;

#[cfg(test)]
mod vod_info_refresh;
#[cfg(test)]
mod vod_info_row;

#[cfg(test)]
mod vod_movie_info;

// Uncompiled Series-info draft; remains excluded from production routing.
#[cfg(test)]
mod vod_episode_rows;
#[cfg(test)]
mod vod_scalar;
#[cfg(test)]
mod vod_seasons;
#[cfg(test)]
mod vod_series_info;

#[cfg(test)]
mod vod_span;

#[cfg(test)]
mod vod_info_stage;
