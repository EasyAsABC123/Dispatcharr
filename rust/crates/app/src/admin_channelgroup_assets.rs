//! Allowlisted assets from Django 6.0.9. See the adjacent distribution notices.
use axum::{
    body::Body,
    http::{header, Method},
    response::Response,
};

macro_rules! django_assets {
    ($($asset:literal => $mime:literal),* $(,)?) => {
        fn content(path: &str) -> Option<(&'static str, &'static [u8])> {
            match path {
                $(concat!("/static/admin/", $asset) => Some(($mime, include_bytes!(concat!("../assets/django-admin-6.0.9/", $asset)))),)*
                _ => None,
            }
        }
    };
}

django_assets! {
    "css/base.css" => "text/css",
    "css/changelists.css" => "text/css",
    "css/dark_mode.css" => "text/css",
    "css/nav_sidebar.css" => "text/css",
    "css/responsive.css" => "text/css",
    "img/icon-addlink.svg" => "image/svg+xml",
    "img/icon-alert-dark.svg" => "image/svg+xml",
    "img/icon-alert.svg" => "image/svg+xml",
    "img/icon-changelink.svg" => "image/svg+xml",
    "img/icon-debug-dark.svg" => "image/svg+xml",
    "img/icon-debug.svg" => "image/svg+xml",
    "img/icon-deletelink.svg" => "image/svg+xml",
    "img/icon-hidelink.svg" => "image/svg+xml",
    "img/icon-info-dark.svg" => "image/svg+xml",
    "img/icon-info.svg" => "image/svg+xml",
    "img/icon-no-dark.svg" => "image/svg+xml",
    "img/icon-no.svg" => "image/svg+xml",
    "img/icon-viewlink.svg" => "image/svg+xml",
    "img/icon-yes-dark.svg" => "image/svg+xml",
    "img/icon-yes.svg" => "image/svg+xml",
    "img/search.svg" => "image/svg+xml",
    "img/sorting-icons.svg" => "image/svg+xml",
    "img/tooltag-add.svg" => "image/svg+xml",
    "img/tooltag-arrowright.svg" => "image/svg+xml",
    "js/actions.js" => "application/javascript",
    "js/admin/RelatedObjectLookups.js" => "application/javascript",
    "js/core.js" => "application/javascript",
    "js/filters.js" => "application/javascript",
    "js/jquery.init.js" => "application/javascript",
    "js/nav_sidebar.js" => "application/javascript",
    "js/prepopulate.js" => "application/javascript",
    "js/theme.js" => "application/javascript",
    "js/urlify.js" => "application/javascript",
    "js/vendor/jquery/jquery.min.js" => "application/javascript",
    "js/vendor/xregexp/xregexp.min.js" => "application/javascript",
}

pub(super) fn serve(path: &str, method: &Method) -> Option<Response> {
    if !matches!(*method, Method::GET | Method::HEAD) {
        return None;
    }
    let (mime, bytes) = content(path)?;
    let mut response = Response::builder()
        .header(header::CONTENT_TYPE, mime)
        .header(header::CONTENT_LENGTH, bytes.len())
        .body(if *method == Method::HEAD {
            Body::empty()
        } else {
            Body::from(bytes)
        })
        .expect("pinned static asset headers");
    response
        .extensions_mut()
        .insert(super::response_headers::SourceStaticResponse);
    Some(response)
}
