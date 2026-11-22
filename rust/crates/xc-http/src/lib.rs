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
mod vod_json;
mod vod_rating;

mod vod_lists;
mod vod_priority;
mod vod_properties;

mod vod_object;

mod vod_info_refresh;
mod vod_info_row;

mod vod_movie_info;

mod vod_episode_rows;
mod vod_scalar;
mod vod_seasons;
mod vod_series_info;

mod vod_span;

mod vod_info_stage;
