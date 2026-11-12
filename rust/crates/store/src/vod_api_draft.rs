//! Bounded ordinary VOD read component derived from original Dispatcharr.
//! App attachment and packaged acceptance are separate from host proofs. Original Dispatcharr authors retain credit.
// Additional captured VOD category/action policies are not routed yet.
#![allow(dead_code)]
#[path = "vod_api_category_seed.rs"]
mod vod_api_category_seed;
#[path = "vod_api_dto.rs"]
mod vod_api_dto;
#[cfg(test)]
#[path = "vod_api_native_tests.rs"]
mod vod_api_native_tests;
#[path = "vod_api_owned_read.rs"]
mod vod_api_owned_read;
#[path = "vod_api_pagination.rs"]
mod vod_api_pagination;
#[path = "vod_api_projection.rs"]
mod vod_api_projection;
#[path = "vod_api_query.rs"]
mod vod_api_query;
#[path = "vod_api_read_component.rs"]
mod vod_api_read_component;
#[cfg(test)]
#[path = "vod_api_source_tests.rs"]
mod vod_api_source_tests;
#[path = "vod_api_sql.rs"]
mod vod_api_sql;

#[path = "vod_api_http.rs"]
mod vod_api_http;

pub use vod_api_http::VodReads;
pub use vod_api_owned_read::ReadLimits as VodReadLimits;

/// Existing captured Django/DRF CSRF masking/cookie algorithm, reusable by a
/// bounded browsable renderer. This does not authenticate or register a route.
pub fn source_browsable_csrf(cookie: Option<&str>) -> (String, String, String) {
    let csrf = vod_api_unified_html::Csrf::new(cookie);
    let header = csrf.set_cookie();
    (csrf.secret, csrf.masked, header)
}

#[path = "vod_api_categories.rs"]
mod vod_api_categories;

#[path = "vod_api_category_http.rs"]
mod vod_api_category_http;

// Source-validated unified JSON extension; canonical app/React and open HTML contracts remain separate.
#[path = "vod_api_unified_query.rs"]
mod vod_api_unified_query;
#[path = "vod_api_unified_read.rs"]
mod vod_api_unified_read;
#[path = "vod_api_unified_sql.rs"]
mod vod_api_unified_sql;

// Explicit unified JSON router extension, sharing the ordinary/category runtime and gate.
#[path = "vod_api_unified_dispatch.rs"]
mod vod_api_unified_dispatch;
#[path = "vod_api_unified_http.rs"]
mod vod_api_unified_http;

#[path = "vod_api_unified_html.rs"]
mod vod_api_unified_html;

#[path = "vod_api_actions_dto.rs"]
mod vod_api_actions_dto;

#[path = "vod_api_actions_sql.rs"]
mod vod_api_actions_sql;

#[path = "vod_api_actions_content.rs"]
mod vod_api_actions_content;

#[path = "vod_api_actions_account.rs"]
mod vod_api_actions_account;

#[path = "vod_api_actions_read.rs"]
mod vod_api_actions_read;

#[path = "vod_api_actions_http.rs"]
mod vod_api_actions_http;

#[cfg(test)]
#[path = "vod_api_actions_peer.rs"]
mod vod_api_actions_peer;

#[cfg(test)]
#[path = "vod_api_actions_schedule.rs"]
mod vod_api_actions_schedule;

#[allow(dead_code)]
#[path = "vod_api_provider_artwork.rs"]
mod vod_api_provider_artwork;

#[allow(dead_code)]
#[path = "vod_api_provider_info_dto.rs"]
mod vod_api_provider_info_dto;

#[cfg(test)]
#[allow(dead_code)]
#[path = "vod_api_provider_info_tests.rs"]
mod vod_api_provider_info_tests;

#[allow(dead_code)]
#[path = "vod_api_provider_info_policy.rs"]
mod vod_api_provider_info_policy;

#[allow(dead_code)]
#[path = "vod_api_provider_info_sql.rs"]
mod vod_api_provider_info_sql;

#[allow(dead_code)]
#[path = "vod_api_provider_info_read.rs"]
mod vod_api_provider_info_read;

#[cfg(test)]
#[path = "vod_api_provider_info_peer.rs"]
mod vod_api_provider_info_peer;

#[path = "vod_api_images_resolve.rs"]
mod vod_api_images_resolve;

#[path = "vod_api_images_sql.rs"]
mod vod_api_images_sql;

#[path = "vod_api_images_read.rs"]
mod vod_api_images_read;

#[path = "vod_api_images_http.rs"]
mod vod_api_images_http;

#[cfg(test)]
#[path = "vod_api_images_peer.rs"]
mod vod_api_images_peer;

// Source-qualified refresh implementation; fixture peers remain test-only.
#[path = "vod_api_provider_refresh_actor.rs"]
mod vod_api_provider_refresh_actor;
#[path = "vod_api_provider_refresh_episodes.rs"]
mod vod_api_provider_refresh_episodes;
#[path = "vod_api_provider_refresh_http.rs"]
mod vod_api_provider_refresh_http;
#[path = "vod_api_provider_refresh_movie.rs"]
mod vod_api_provider_refresh_movie;
#[cfg(test)]
#[path = "vod_api_provider_refresh_peer.rs"]
mod vod_api_provider_refresh_peer;
#[path = "vod_api_provider_refresh_series_value.rs"]
mod vod_api_provider_refresh_series_value;
#[path = "vod_api_provider_refresh_sql.rs"]
mod vod_api_provider_refresh_sql;
#[path = "vod_api_provider_refresh_value.rs"]
mod vod_api_provider_refresh_value;

#[path = "vod_api_fair_budget.rs"]
mod vod_api_fair_budget;

#[path = "vod_api_phase_children.rs"]
mod vod_api_phase_children;

#[path = "vod_api_phase_scope.rs"]
mod vod_api_phase_scope;

#[path = "vod_api_provider_refresh_episode_sql.rs"]
mod vod_api_provider_refresh_episode_sql;

#[path = "vod_api_provider_refresh_plan.rs"]
mod vod_api_provider_refresh_plan;

#[path = "vod_api_provider_refresh_budget.rs"]
mod vod_api_provider_refresh_budget;

#[path = "vod_api_provider_refresh_body.rs"]
mod vod_api_provider_refresh_body;
