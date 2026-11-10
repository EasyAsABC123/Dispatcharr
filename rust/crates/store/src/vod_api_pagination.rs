//! UNREGISTERED/UNCOMPILED Source VOD REST pagination draft. No route or gate.
//! Source api_views.VODPagination and UnifiedContentViewSet.list deliberately
//! use different defaults and errors. All original authors retain full credit.
use num_bigint::BigInt;

pub(crate) fn last_query<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs.iter().rev().find(|(name, _)|name == key).map(|(_, value)|value.as_str())
}
fn integer(value: &str) -> Option<BigInt> {
    dispatcharr_ingest_core::source_integer_literal(value)?.parse().ok()
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct InvalidPage;
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Page { pub number: u64, pub size: u64, pub offset: u64, pub pages: u64 }
impl Page {
    pub fn has_next(&self) -> bool { self.number < self.pages }
    pub fn has_previous(&self) -> bool { self.number > 1 }
}
/// DRF _positive_int(...strict=True,cutoff=100) defaults to20 on invalid size;
/// page uses Django Paginator, allow_empty_first_page=True. QueryDict is last.
/// Error mapping is Source404 {"detail":"Invalid page."}, not400/empty list.
pub(crate) fn ordinary(pairs: &[(String,String)], count: u64) -> Result<Page, InvalidPage> {
    let zero = BigInt::from(0);
    let size = last_query(pairs, "page_size").and_then(integer).filter(|n|n > &zero)
        .map(|n|n.min(BigInt::from(100)).to_string().parse::<u64>().expect("positive cutoff"))
        .unwrap_or(20);
    let pages = count.div_ceil(size).max(1);
    let raw = last_query(pairs, "page").filter(|v|!v.is_empty()).unwrap_or("1");
    let number = if raw == "last" { BigInt::from(pages) } else { integer(raw).ok_or(InvalidPage)? };
    if number < BigInt::from(1) || number > BigInt::from(pages) { return Err(InvalidPage); }
    let number: u64 = number.to_string().parse().map_err(|_|InvalidPage)?;
    Ok(Page { number, size, offset:(number-1)*size, pages })
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UnifiedParameterError { InvalidInteger { name: &'static str, raw: String } }
/// Keep BigInt until SQL binding: Python multiplication is not usize/i64 and
/// invalid negative LIMIT/OFFSET fails at PostgreSQL, inside Source's500 catch.
/// page_size=0 is valid and returns zero results, possibly a next-page URL.
pub(crate) struct UnifiedPage { pub number: BigInt, pub size: BigInt, pub offset: BigInt }
pub(crate) fn unified(pairs: &[(String,String)]) -> Result<UnifiedPage,UnifiedParameterError> {
    let parse = |name, default| {
        let raw = last_query(pairs, name).unwrap_or(default);
        integer(raw).ok_or_else(||UnifiedParameterError::InvalidInteger{name,raw:raw.into()})
    };
    let number = parse("page", "1")?;
    let size = parse("page_size", "24")?;
    let offset = (&number - BigInt::from(1)) * &size;
    Ok(UnifiedPage { number, size, offset })
}
// The eventual SQL adapter must retain exact Source error messages for invalid
// Python int spellings and PG LIMIT/OFFSET errors. Merely returning this enum
// is not500 wire parity, and imposing the ordinary100-row cap here is wrong.

/// DRF utils.urls sorts parse_qs keys, preserves non-page duplicate values,
/// uses Python quote_plus (not WHATWG '~' escaping), then replaces/removes page.
/// Caller supplies an already validated Source absolute origin/path; this does
/// not authorize forwarding headers or derive an origin from arbitrary input.
pub(crate) fn page_url(origin: &str, path: &str, query: &[(String,String)], page: Option<u64>) -> String {
    use std::collections::BTreeMap;
    let mut groups: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (key,value) in query {
        if key != "page" { groups.entry(key).or_default().push(value); }
    }
    let page = page.map(|value|value.to_string());
    if let Some(value) = &page { groups.insert("page",vec![value.as_str()]); }
    let mut result = format!("{origin}{path}");
    let mut first = true;
    for (key,values) in groups {
        for value in values {
            result.push(if first {'?'}else{'&'});first=false;
            quote_plus(&mut result,key);result.push('=');quote_plus(&mut result,value);
        }
    }
    result
}
fn quote_plus(target: &mut String, value: &str) {
    const HEX: &[u8] = b"0123456789ABCDEF";
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z'|b'a'..=b'z'|b'0'..=b'9'|b'-'|b'.'|b'_'|b'~'=>target.push(char::from(byte)),
            b' '=>target.push('+'),
            value=>{target.push('%');target.push(char::from(HEX[(value>>4)as usize]));target.push(char::from(HEX[(value&15)as usize]));}
        }
    }
}
