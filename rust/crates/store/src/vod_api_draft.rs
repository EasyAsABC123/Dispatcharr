//! Bounded ordinary VOD read component derived from original Dispatcharr.
//! App attachment and packaged acceptance are separate from host proofs. Original Dispatcharr authors retain credit.
// Additional captured VOD category/action policies are not routed yet.
#![allow(dead_code)]
#[path = "vod_api_projection.rs"] mod vod_api_projection;
#[path = "vod_api_dto.rs"] mod vod_api_dto;
#[path = "vod_api_pagination.rs"] mod vod_api_pagination;
#[path = "vod_api_query.rs"] mod vod_api_query;
#[path = "vod_api_sql.rs"] mod vod_api_sql;
#[path = "vod_api_owned_read.rs"] mod vod_api_owned_read;
#[path = "vod_api_read_component.rs"] mod vod_api_read_component;
#[path = "vod_api_category_seed.rs"] mod vod_api_category_seed;
#[cfg(test)]
#[path = "vod_api_source_tests.rs"] mod vod_api_source_tests;
#[cfg(test)]
#[path = "vod_api_native_tests.rs"] mod vod_api_native_tests;

#[path = "vod_api_http.rs"] mod vod_api_http;

pub use vod_api_http::VodReads;
pub use vod_api_owned_read::ReadLimits as VodReadLimits;

#[path = "vod_api_categories.rs"] mod vod_api_categories;

#[path = "vod_api_category_http.rs"] mod vod_api_category_http;

// Source-validated unified JSON extension; canonical app/React and open HTML contracts remain separate.
#[path = "vod_api_unified_query.rs"] mod vod_api_unified_query;
#[path = "vod_api_unified_sql.rs"] mod vod_api_unified_sql;
#[path = "vod_api_unified_read.rs"] mod vod_api_unified_read;

// Explicit unified JSON router extension, sharing the ordinary/category runtime and gate.
#[path = "vod_api_unified_dispatch.rs"] mod vod_api_unified_dispatch;
#[path = "vod_api_unified_http.rs"] mod vod_api_unified_http;
