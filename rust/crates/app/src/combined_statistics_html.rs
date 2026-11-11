//! Finite, actual Original DRF combined-stats HTML contexts.
//! Original Dispatcharr, Django/DRF authors retain template/algorithm credit.
use super::combined_statistics_wire::{self as wire, Output};
use serde_json::Value;

const ANONYMOUS: &str = include_str!("../datasets/combined-stats-html/anonymous.html.in");
const STANDARD: &str = include_str!("../datasets/combined-stats-html/standard.html.in");
const ADMIN_ERROR: &str = include_str!("../datasets/combined-stats-html/admin-error.html.in");
const ADMIN_OPTIONS: &str = include_str!("../datasets/combined-stats-html/admin-options.html.in");
const ADMIN_OPTIONS_API: &str =
    include_str!("../datasets/combined-stats-html/admin-options-api.html.in");

fn word(output: &mut Output, text: &str) -> Result<(), ()> {
    let (prefix, tail) = if text.starts_with("http://") || text.starts_with("https://") {
        (None, text)
    } else if text.starts_with("www.") {
        (Some("http://"), text)
    } else {
        return wire::escape(output, text);
    };
    let end = tail.trim_end_matches(['.', ',', ':', ';', ')', ']']).len();
    let (url, suffix) = tail.split_at(end);
    if !url.is_ascii() || url.len() > 200 {
        return wire::escape(output, text);
    }
    output.text("<a href=\"")?;
    if let Some(prefix) = prefix {
        output.text(prefix)?;
    }
    wire::escape(output, url)?;
    output.text("\" rel=\"nofollow\">")?;
    wire::escape(output, url)?;
    output.text("</a>")?;
    wire::escape(output, suffix)
}
fn content(output: &mut Output, text: &str) -> Result<(), ()> {
    let mut start = 0;
    for (index, c) in text.char_indices() {
        if c.is_whitespace() || matches!(c, '<' | '>' | '"' | '\'') {
            if start < index {
                word(output, &text[start..index])?;
            }
            let mut bytes = [0u8; 4];
            wire::escape(output, c.encode_utf8(&mut bytes))?;
            start = index + c.len_utf8();
        }
    }
    if start < text.len() {
        word(output, &text[start..])?;
    }
    Ok(())
}
pub(super) struct Rendered {
    pub data: Vec<u8>,
    pub set_cookie: String,
}
pub(super) struct Context<'a> {
    pub status: u16,
    pub method: &'a str,
    pub uri: &'a str,
    pub identity: Option<(&'a str, i32)>,
    pub cookie: Option<&'a str>,
    pub origin: &'a str,
}
pub(super) fn render(context: Context<'_>, body: &Value) -> Result<Rendered, ()> {
    let api = context.uri == "/proxy/stats/?format=api";
    if context.uri != "/proxy/stats/" && !api {
        return Err(());
    }
    let source = match (context.status, context.method, context.identity, api) {
        (401, _, None, false) => ANONYMOUS,
        (403, _, Some((_, level)), false) if level < 10 => STANDARD,
        (405, _, Some((_, level)), false) if level >= 10 => ADMIN_ERROR,
        (200, "OPTIONS", Some((_, level)), false) if level >= 10 => ADMIN_OPTIONS,
        (200, "OPTIONS", Some((_, level)), true) if level >= 10 => ADMIN_OPTIONS_API,
        _ => return Err(()),
    };
    let pretty = wire::json(body, wire::Format::Drf(Some(4)), 256 * 1024)?;
    let pretty = std::str::from_utf8(&pretty).map_err(|_| ())?;
    let (_, masked, set_cookie) = dispatcharr_store::source_browsable_csrf(context.cookie);
    let mut output = Output::new(1024 * 1024);
    let mut remaining = source;
    while let Some(start) = remaining.find("@@STATS_") {
        output.text(&remaining[..start])?;
        let tail = &remaining[start..];
        let end = tail[2..].find("@@").ok_or(())? + 4;
        match &tail[..end] {
            "@@STATS_CONTENT@@" => content(&mut output, pretty)?,
            "@@STATS_CSRF@@" => output.text(&masked)?,
            "@@STATS_METHOD@@" => wire::escape(&mut output, context.method)?,
            "@@STATS_USERNAME@@" => wire::escape(&mut output, context.identity.ok_or(())?.0)?,
            "@@STATS_ALLOW@@" => output.text("GET, OPTIONS")?,
            "@@STATS_ORIGIN@@" => wire::escape(&mut output, context.origin)?,
            _ => return Err(()),
        }
        remaining = &tail[end..];
    }
    output.text(remaining)?;
    Ok(Rendered {
        data: output.finish(),
        set_cookie,
    })
}
