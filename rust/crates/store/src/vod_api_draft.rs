//! Test-only unshipped VOD draft compilation/corpus harness. Production routing
//! and module exports remain absent. Original Dispatcharr authors retain credit.
// These draft interfaces intentionally have no production callers yet.
#![allow(dead_code)]
#[path = "vod_api_projection.rs"] mod vod_api_projection;
#[path = "vod_api_dto.rs"] mod vod_api_dto;
#[path = "vod_api_pagination.rs"] mod vod_api_pagination;
#[path = "vod_api_query.rs"] mod vod_api_query;
#[path = "vod_api_sql.rs"] mod vod_api_sql;
#[path = "vod_api_owned_read.rs"] mod vod_api_owned_read;
#[path = "vod_api_read_component.rs"] mod vod_api_read_component;
#[path = "vod_api_category_seed.rs"] mod vod_api_category_seed;
#[path = "vod_api_source_tests.rs"] mod vod_api_source_tests;
#[path = "vod_api_native_tests.rs"] mod vod_api_native_tests;
