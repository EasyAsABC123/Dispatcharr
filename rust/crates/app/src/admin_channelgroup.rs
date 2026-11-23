//! Existing signed Django ChannelGroup changelist, for the public ingress GET.
use std::fmt::Write;

use axum::{
    body::Body,
    http::{header, HeaderMap, HeaderValue, StatusCode, Uri},
    response::Response,
};
use chrono::{Duration, Utc};
use dispatcharr_store::{
    DjangoAdminSessions, DjangoChannelGroupOrder, DjangoChannelGroupPage,
    DjangoChannelGroupPageResult, DjangoChannelGroupQuery, DjangoChannelGroupRead,
};
use rand::{rngs::OsRng, Rng};
use url::form_urlencoded;

pub(super) const PATH: &str = "/admin/dispatcharr_channels/channelgroup/";
const LOGIN: &str = "/admin/login/";
const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const TOKEN_LEN: usize = 32;
const CSRF_MAX_AGE: i64 = 52 * 7 * 24 * 60 * 60;
const NO_CACHE: &str = "max-age=0, no-cache, no-store, must-revalidate, private";
const STYLES: &[&str] = &[
    "base",
    "dark_mode",
    "nav_sidebar",
    "changelists",
    "responsive",
];
const SCRIPTS: &[&str] = &[
    "theme",
    "nav_sidebar",
    "vendor/jquery/jquery.min",
    "jquery.init",
    "core",
    "admin/RelatedObjectLookups",
    "actions",
    "urlify",
    "vendor/xregexp/xregexp.min",
    "prepopulate",
    "filters",
];

#[derive(Default)]
struct Selection {
    search: String,
    page: Option<i64>,
    all: bool,
    ordering: String,
}

impl Selection {
    fn parse(query: Option<&str>) -> Self {
        let mut result = Self::default();
        for (name, value) in form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
            match name.as_ref() {
                "q" => result.search = value.into_owned(),
                "p" => result.page = value.trim().parse().ok(),
                "all" => result.all = true,
                "o" => {
                    result.ordering = match value.as_ref() {
                        "0" | "-0" | "1" | "-1" => value.into_owned(),
                        _ => String::new(),
                    };
                }
                _ => {}
            }
        }
        result
    }

    fn query(&self) -> DjangoChannelGroupQuery<'_> {
        use DjangoChannelGroupOrder::*;
        DjangoChannelGroupQuery {
            search: &self.search,
            page: self.page.unwrap_or(1),
            show_all: self.all,
            order: match self.ordering.as_str() {
                "0" => IdAscending,
                "1" => NameAscending,
                "-1" => NameDescending,
                _ => IdDescending,
            },
        }
    }

    fn link(&self, page: Option<i64>, ordering: &str, all: bool) -> String {
        let mut query = form_urlencoded::Serializer::new(String::new());
        if !self.search.is_empty() {
            query.append_pair("q", &self.search);
        }
        if !ordering.is_empty() {
            query.append_pair("o", ordering);
        }
        if let Some(page) = page {
            query.append_pair("p", &page.to_string());
        }
        if all {
            query.append_pair("all", "");
        }
        format!("{PATH}?{}", query.finish())
    }
}

pub(super) async fn handle(
    sessions: Option<&DjangoAdminSessions>,
    uri: &Uri,
    headers: &HeaderMap,
) -> Response {
    let Some(key) = cookie(headers, "sessionid") else {
        return login_required(uri);
    };
    let Some(sessions) = sessions else {
        return login_required(uri);
    };
    let selection = Selection::parse(uri.query());
    match sessions.channel_group_page(key, selection.query()).await {
        Ok(DjangoChannelGroupPageResult::View(page)) => {
            let csrf = Csrf::for_response(cookie(headers, "csrftoken"));
            html_response(StatusCode::OK, render(&page, &selection, &csrf.token), Some(&csrf))
        }
        Ok(DjangoChannelGroupPageResult::Denied(DjangoChannelGroupRead::Forbidden)) => {
            html_response(
                StatusCode::FORBIDDEN,
                "<!doctype html><html lang=\"en\"><head><title>403 Forbidden</title></head><body><h1>403 Forbidden</h1></body></html>".to_owned(),
                None,
            )
        }
        Ok(DjangoChannelGroupPageResult::Denied(_)) => login_required(uri),
        Ok(DjangoChannelGroupPageResult::PageOutOfRange) => redirect(&format!("{PATH}?e=1")),
        Err(_) => html_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "<!doctype html><html lang=\"en\"><title>Server Error (500)</title><h1>Server Error (500)</h1></html>".to_owned(),
            None,
        ),
    }
}

fn cookie<'a>(headers: &'a HeaderMap, wanted: &str) -> Option<&'a str> {
    let mut found = None;
    for header in headers.get_all(header::COOKIE) {
        for pair in header.to_str().ok()?.split(';') {
            if let Some((name, value)) = pair.trim().split_once('=') {
                if name.trim() == wanted && found.replace(value.trim()).is_some() {
                    return None;
                }
            }
        }
    }
    found
}

fn login_required(uri: &Uri) -> Response {
    let next = uri.path_and_query().map_or(PATH, |value| value.as_str());
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("next", next)
        .finish()
        .replace("%2F", "/");
    redirect(&format!("{LOGIN}?{query}"))
}

fn redirect(location: &str) -> Response {
    let mut response = html_response(StatusCode::FOUND, String::new(), None);
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(location).expect("local admin redirect is URL encoded"),
    );
    response
}

fn http_date(time: chrono::DateTime<Utc>) -> String {
    time.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

fn html_response(status: StatusCode, html: String, csrf: Option<&Csrf>) -> Response {
    let now = Utc::now();
    let mut response = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::VARY, "Cookie")
        .body(Body::from(html))
        .expect("admin HTML headers");
    if status != StatusCode::FORBIDDEN {
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static(NO_CACHE));
        response.headers_mut().insert(
            header::EXPIRES,
            HeaderValue::from_str(&http_date(now)).expect("formatted HTTP date"),
        );
    }
    if let Some(csrf) = csrf {
        let expires = http_date(now + Duration::seconds(CSRF_MAX_AGE));
        let value = format!(
            "csrftoken={}; expires={expires}; Max-Age={CSRF_MAX_AGE}; Path=/; SameSite=Lax",
            csrf.secret
        );
        response.headers_mut().insert(
            header::SET_COOKIE,
            HeaderValue::from_str(&value).expect("CSRF secret uses an alphanumeric alphabet"),
        );
    }
    response
}

struct Csrf {
    secret: String,
    token: String,
}

impl Csrf {
    fn for_response(existing: Option<&str>) -> Self {
        let secret = existing
            .filter(|value| {
                value.len() == TOKEN_LEN && value.bytes().all(|b| b.is_ascii_alphanumeric())
            })
            .map_or_else(random_chars, str::to_owned);
        let token = mask(&secret, random_chars());
        Self { secret, token }
    }
}

fn random_chars() -> String {
    let mut rng = OsRng;
    (0..TOKEN_LEN)
        .map(|_| char::from(CHARSET[rng.gen_range(0..CHARSET.len())]))
        .collect()
}

fn mask(secret: &str, mut mask: String) -> String {
    let cipher: String = secret
        .bytes()
        .zip(mask.bytes())
        .map(|(secret, mask)| {
            let secret = CHARSET.iter().position(|value| *value == secret).unwrap();
            let mask = CHARSET.iter().position(|value| *value == mask).unwrap();
            char::from(CHARSET[(secret + mask) % CHARSET.len()])
        })
        .collect();
    mask.push_str(&cipher);
    mask
}

fn escape(value: &str) -> String {
    let mut html = String::with_capacity(value.len());
    for char in value.chars() {
        match char {
            '&' => html.push_str("&amp;"),
            '<' => html.push_str("&lt;"),
            '>' => html.push_str("&gt;"),
            '"' => html.push_str("&quot;"),
            '\'' => html.push_str("&#x27;"),
            _ => html.push(char),
        }
    }
    html
}

fn render(page: &DjangoChannelGroupPage, selection: &Selection, token: &str) -> String {
    let mut html = String::from(
        "<!doctype html><html lang=\"en-us\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <title>Select channel group to view | Django site admin</title>",
    );
    for style in STYLES {
        write!(
            html,
            "<link rel=\"stylesheet\" href=\"/static/admin/css/{style}.css\">"
        )
        .unwrap();
    }
    for script in SCRIPTS {
        write!(
            html,
            "<script src=\"/static/admin/js/{script}.js\" defer></script>"
        )
        .unwrap();
    }
    write!(html, "</head><body class=\"app-dispatcharr_channels model-channelgroup change-list\"><div id=\"container\">\
        <header id=\"header\"><div id=\"branding\"><a href=\"/admin/\">Django administration</a></div>\
        <div id=\"user-tools\">Welcome, <strong>{}</strong>.\
        <button type=\"button\" class=\"theme-toggle\">Toggle theme</button>\
        <form id=\"logout-form\" method=\"post\" action=\"/admin/logout/\">\
        <input type=\"hidden\" name=\"csrfmiddlewaretoken\" value=\"{token}\"><button>Log out</button></form></div></header>\
        <nav class=\"breadcrumbs\"><a href=\"/admin/\">Home</a> &rsaquo; Channels &rsaquo; Channel groups</nav>\
        <main id=\"main\"><button class=\"sticky toggle-nav-sidebar\" id=\"toggle-nav-sidebar\" type=\"button\" aria-label=\"Toggle navigation\"></button>\
        <nav id=\"nav-sidebar\" class=\"sticky\" aria-label=\"Sidebar\"><div class=\"module current-app\">\
        <a href=\"{PATH}\" aria-current=\"page\">Channel groups</a></div></nav><div id=\"content\"><h1>Select channel group to view</h1>\
        <div id=\"changelist\" class=\"module\"><div class=\"changelist-form-container\">\
        <div id=\"toolbar\"><form id=\"changelist-search\" method=\"get\">\
        <label for=\"searchbar\"><img src=\"/static/admin/img/search.svg\" alt=\"Search\"></label>\
        <input type=\"text\" id=\"searchbar\" name=\"q\" value=\"{}\">\
        <input type=\"submit\" value=\"Search\">", escape(&page.username), escape(&selection.search)).unwrap();
    if !selection.ordering.is_empty() {
        write!(
            html,
            "<input type=\"hidden\" name=\"o\" value=\"{}\">",
            selection.ordering
        )
        .unwrap();
    }
    if page.matches != page.total {
        write!(
            html,
            " <span class=\"small quiet\">{} results ({} total)</span>",
            page.matches, page.total
        )
        .unwrap();
    }
    write!(
        html,
        "</form></div><form id=\"changelist-form\" method=\"post\">\
        <input type=\"hidden\" name=\"csrfmiddlewaretoken\" value=\"{token}\">"
    )
    .unwrap();
    if !page.rows.is_empty() {
        let id_sort = if selection.ordering == "0" { "-0" } else { "0" };
        let name_sort = if selection.ordering == "1" { "-1" } else { "1" };
        write!(html, "<div class=\"results\"><table id=\"result_list\"><thead><tr>\
            <th class=\"sortable column-id\"><div class=\"text\"><a href=\"{}\">ID</a></div></th>\
            <th class=\"sortable column-name\"><div class=\"text\"><a href=\"{}\">Name</a></div></th>\
            </tr></thead><tbody>", escape(&selection.link(None, id_sort, selection.all)),
            escape(&selection.link(None, name_sort, selection.all))).unwrap();
        for (id, name) in &page.rows {
            write!(
                html,
                "<tr><th class=\"field-id\"><a href=\"{PATH}{id}/change/\">{id}</a></th>\
                <td class=\"field-name\">{}</td></tr>",
                escape(name)
            )
            .unwrap();
        }
        html.push_str("</tbody></table></div>");
    }
    write_paginator(&mut html, page, selection);
    html.push_str("</form></div></div></div></main></div></body></html>");
    html
}

fn write_paginator(html: &mut String, page: &DjangoChannelGroupPage, selection: &Selection) {
    html.push_str("<p class=\"paginator\">");
    if page.pages > 1 && !page.show_all {
        let mut visible = vec![1, page.pages];
        visible.extend(
            (page.page.saturating_sub(2).max(1))..=(page.page.saturating_add(2).min(page.pages)),
        );
        visible.sort_unstable();
        visible.dedup();
        let mut previous = 0;
        for number in visible {
            if number > previous + 1 {
                html.push_str(" &hellip; ");
            }
            if number == page.page {
                write!(html, "<span class=\"this-page\">{number}</span> ").unwrap();
            } else {
                let link = selection.link(Some(number), &selection.ordering, false);
                write!(html, "<a href=\"{}\">{number}</a> ", escape(&link)).unwrap();
            }
            previous = number;
        }
        if (101..=200).contains(&page.matches) {
            let link = selection.link(None, &selection.ordering, true);
            write!(
                html,
                "<a href=\"{}\" class=\"showall\">Show all</a> ",
                escape(&link)
            )
            .unwrap();
        }
    }
    let label = if page.matches == 1 {
        "channel group"
    } else {
        "channel groups"
    };
    write!(html, "{} {label}</p>", page.matches).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn anonymous_route_has_exact_local_next_and_private_headers() {
        let uri: Uri = PATH.parse().unwrap();
        let reply = handle(None, &uri, &HeaderMap::new()).await;
        assert_eq!(reply.status(), StatusCode::FOUND);
        assert_eq!(
            reply.headers()[header::LOCATION],
            format!("{LOGIN}?next={PATH}")
        );
        assert_eq!(reply.headers()[header::CACHE_CONTROL], NO_CACHE);
        assert_eq!(reply.headers()[header::VARY], "Cookie");
        assert!(!reply.headers().contains_key(header::SET_COOKIE));
        assert!(to_bytes(reply.into_body(), 16).await.unwrap().is_empty());

        let uri: Uri = format!("{PATH}?q=owned%26safe").parse().unwrap();
        let reply = handle(None, &uri, &HeaderMap::new()).await;
        assert_eq!(
            reply.headers()[header::LOCATION],
            format!("{LOGIN}?next={PATH}%3Fq%3Downed%2526safe")
        );
    }

    #[test]
    fn signed_view_escapes_rows_and_query_and_preserves_scope() {
        let selection = Selection::parse(Some("q=%3Cb%3Ea%26b%3C%2Fb%3E&o=1"));
        let page = DjangoChannelGroupPage {
            username: "Example <viewer>".to_owned(),
            rows: vec![(12, "<b>a&b</b>".to_owned()), (7, "Second".to_owned())],
            total: 120,
            matches: 102,
            page: 1,
            pages: 2,
            show_all: false,
        };
        let csrf = Csrf::for_response(None);
        let html = render(&page, &selection, &csrf.token);
        assert!(html.contains("id=\"result_list\"") && html.contains("name=\"q\""));
        assert!(
            html.contains("Example &lt;viewer&gt;") && html.contains("&lt;b&gt;a&amp;b&lt;/b&gt;")
        );
        assert!(!html.contains("<b>a&b</b>"));
        assert!(html.contains("class=\"sortable column-name\""));
        assert!(html.find("/12/change/").unwrap() < html.find("/7/change/").unwrap());
        assert!(html.contains("&amp;p=2") && html.contains("&amp;all="));
        assert!(!html.contains("_selected_action") && !html.contains("delete_selected"));
        assert!(!html.contains("/jsi18n/") && !html.contains("/channelgroup/add/"));
        assert!(html.contains(&format!("value=\"{}\"", csrf.token)));
        let response = html_response(StatusCode::OK, html, Some(&csrf));
        assert_eq!(response.headers()[header::VARY], "Cookie");
        assert!(response.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .starts_with("csrftoken="));
        assert!(response.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("Max-Age=31449600; Path=/; SameSite=Lax"));
    }

    #[test]
    fn stock_csrf_mask_preserves_current_cookie_authority() {
        let source = String::from_utf8(CHARSET[..32].to_vec()).unwrap();
        let identity_mask = "a".repeat(TOKEN_LEN);
        assert_eq!(
            mask(&source, identity_mask.clone()),
            identity_mask + &source
        );
        let selected = Csrf::for_response(Some(&source));
        assert_eq!(selected.secret, source);
        let (mask, cipher) = selected.token.split_at(TOKEN_LEN);
        let decoded: String = mask
            .bytes()
            .zip(cipher.bytes())
            .map(|(mask, cipher)| {
                let mask = CHARSET.iter().position(|value| *value == mask).unwrap();
                let cipher = CHARSET.iter().position(|value| *value == cipher).unwrap();
                char::from(CHARSET[(cipher + CHARSET.len() - mask) % CHARSET.len()])
            })
            .collect();
        assert_eq!(decoded, source);
    }
}
