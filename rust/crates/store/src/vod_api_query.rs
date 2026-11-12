//! Source-validated ordinary VOD query subset used by bounded read routes.
//! Source: source-vod-rest-1 + original api_views.py/permissions.py.
//! Only captured filter/search/ordering grammar is accepted; unsupported forms
//! fail explicitly. URL grammar beyond the network corpus remains a separate
//! gate. Original Dispatcharr authors retain all application credit.
use super::{
    vod_api_pagination::last_query,
    vod_api_projection::{category_filter, Permission},
    vod_api_sql::{Filter, Order, OrderField, Resource, Selection},
};
use serde_json::{json, Map, Value};

#[derive(Debug, PartialEq)]
pub(crate) enum QueryError {
    Source400(Value),
    Unsupported(&'static str),
    Capacity,
}
/// Fixed pinned-Python whitespace set, including ASCII information separators.
/// Do not use numeric whitespace here: form CharField.strip accepts four more
/// characters than Python int/float's direct string decoder.
fn space(value: char) -> bool {
    matches!(value,'\t'..='\r'|' '|'\u{1c}'..='\u{1f}'|'\u{85}'|'\u{a0}'|'\u{1680}'|
        '\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}')
}
fn trimmed(value: &str) -> &str {
    value.trim_matches(space)
}
fn field<'a>(query: &'a [(String, String)], name: &str) -> Option<&'a str> {
    last_query(query, name)
        .map(trimmed)
        .filter(|value| !value.is_empty())
}
/// This is the captured integral subset of django_filters.NumberFilter's
/// DecimalField, NOT a general Decimal parser or Python int substitute. The
/// corpus's literal 'bad' is an exact observed400. Fractions/exponents/Nd/
/// NaN/Infinity/very-large values remain unsupported pending their own Source
/// form/lookup corpus; never truncate them to integers or claim generic400.
fn number(
    query: &[(String, String)],
    name: &str,
    errors: &mut Map<String, Value>,
    unknown: &mut bool,
) -> Option<i64> {
    let value = field(query, name)?;
    if value == "bad" {
        errors.insert(name.into(), json!(["Enter a number."]));
        return None;
    }
    let digits = value
        .strip_prefix('+')
        .or_else(|| value.strip_prefix('-'))
        .unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        *unknown = true;
        return None;
    }
    match value.parse() {
        Ok(value) => Some(value),
        Err(_) => {
            *unknown = true;
            None
        }
    }
}

// Invocation order matters: content negotiation -> shared authentication ->
// action permission -> get_queryset visibility -> form validation/filtering ->
// count/page. Disabled access flags do not bypass form/page validation. Bad
// detail PK conversion is also a later get_object operation, not pre-auth404.
pub(crate) fn ordinary(
    resource: Resource,
    detail: Option<i64>,
    query: &[(String, String)],
) -> Result<Selection, QueryError> {
    if query.len() > 128
        || query
            .iter()
            .try_fold(0usize, |n, (k, v)| {
                n.checked_add(k.len())?.checked_add(v.len())
            })
            .is_none_or(|n| n > 65536)
    {
        return Err(QueryError::Capacity);
    }
    let mut filters = Vec::new();
    let mut errors = Map::new();
    let mut unknown = false;
    if let Some(value) = field(query, "name") {
        filters.push(Filter::NameContains(value.into()));
    }
    if resource == Resource::Episode {
        if let Some(value) = number(query, "series", &mut errors, &mut unknown) {
            filters.push(Filter::ParentSeries(value));
        }
    }
    if let Some(value) = number(query, "m3u_account", &mut errors, &mut unknown) {
        filters.push(Filter::Account(value));
    }
    if resource != Resource::Episode {
        if let Some(value) = field(query, "category") {
            let value = category_filter(value);
            filters.push(Filter::Category {
                name: value.name.into(),
                kind: value.category_type.map(str::to_owned),
            });
        }
        if let Some(value) = number(query, "year", &mut errors, &mut unknown) {
            filters.push(Filter::Year(value));
        }
        if let Some(value) = number(query, "year_gte", &mut errors, &mut unknown) {
            filters.push(Filter::YearMin(value));
        }
        if let Some(value) = number(query, "year_lte", &mut errors, &mut unknown) {
            filters.push(Filter::YearMax(value));
        }
    } else {
        if let Some(value) = number(query, "season_number", &mut errors, &mut unknown) {
            filters.push(Filter::Season(value));
        }
        if let Some(value) = number(query, "episode_number", &mut errors, &mut unknown) {
            filters.push(Filter::Episode(value));
        }
    }
    if resource == Resource::Movie {
        // NullBoolean/BooleanWidget's remaining accepted/ignored spellings
        // need a source-specific corpus. Source1 contains only these literals.
        if let Some(value) = field(query, "is_adult") {
            match value {
                "true" => filters.push(Filter::Adult(true)),
                "false" => filters.push(Filter::Adult(false)),
                _ => unknown = true,
            }
        }
    }
    // Filterset form errors precede filtering/search/ordering. Do not replace
    // an observed400 with a speculative parser error from a later backend.
    if !errors.is_empty() {
        return Err(QueryError::Source400(errors.into()));
    }
    if unknown {
        return Err(QueryError::Unsupported(
            "Uncaptured Decimal/Boolean form grammar",
        ));
    }
    if let Some(search) = last_query(query, "search") {
        // Both legacy split and smart-split Source algorithms agree on this
        // unquoted subset. Quoting/backslashes/NUL need actual package vectors.
        if search
            .chars()
            .any(|c| matches!(c, '\'' | '"' | '\\' | '\0'))
        {
            return Err(QueryError::Unsupported(
                "Quoted/control SearchFilter grammar",
            ));
        }
        let terms: Vec<_> = search
            .split(|c| c == ',' || space(c))
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect();
        if terms.len() > 32 {
            return Err(QueryError::Capacity);
        }
        if !terms.is_empty() {
            filters.push(Filter::SearchTerms(terms));
        }
    }
    let mut order = Vec::new();
    if let Some(raw) = last_query(query, "ordering").filter(|v| !v.is_empty()) {
        for raw in raw.split(',') {
            let raw = trimmed(raw);
            let descending = raw.starts_with('-');
            if raw.starts_with("--") {
                return Err(QueryError::Unsupported(
                    "Multiple-minus ordering fault grammar",
                ));
            }
            let value = raw.strip_prefix('-').unwrap_or(raw);
            let field = match value {
                "name" => Some(OrderField::Name),
                "created_at" => Some(OrderField::CreatedAt),
                "year" if resource != Resource::Episode => Some(OrderField::Year),
                "season_number" if resource == Resource::Episode => Some(OrderField::Season),
                "episode_number" if resource == Resource::Episode => Some(OrderField::Episode),
                _ => None,
            };
            // DRF discards invalid fields; no surviving fields means default
            // ordering. The captured literal ordering=bad is successful.
            if let Some(field) = field {
                order.push(Order { field, descending });
            }
        }
    }
    if order.len() > 16 {
        return Err(QueryError::Capacity);
    }
    // Source ignores query fields not declared by this resource. In particular
    // year/category/is_adult on episodes and season/episode on movies do not
    // become errors or accidental predicates. Page parsing occurs after count.
    // format/Accept negotiation must be handled before authentication in the
    // eventual HTTP adapter; this function is not that negotiation layer.
    Ok(Selection {
        resource,
        detail,
        filters,
        order,
    })
}

#[derive(Clone, Copy)]
pub(crate) enum Surface {
    OrdinaryList,
    OrdinaryDetail,
    Providers,
    ProviderInfo,
    SeriesEpisodes,
    Image,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Dispatch {
    Get { head: bool },
    OptionsMetadata,
    MethodNotAllowed,
}
pub(crate) struct MethodPlan {
    pub permission: Permission,
    pub dispatch: Dispatch,
}
pub(crate) fn method(surface: Surface, method: &str) -> MethodPlan {
    if matches!(method, "GET" | "HEAD") {
        let permission = match surface {
            Surface::OrdinaryList | Surface::OrdinaryDetail => Permission::Standard,
            Surface::Providers | Surface::ProviderInfo | Surface::SeriesEpisodes => {
                Permission::Authenticated
            }
            Surface::Image => Permission::AllowAny,
        };
        return MethodPlan {
            permission,
            dispatch: Dispatch::Get {
                head: method == "HEAD",
            },
        };
    }
    // DRF's metadata action and unmapped methods fail closed via IsAdmin;
    // authenticate/check UI policy before returning OPTIONS/405/unsupported.
    MethodPlan {
        permission: Permission::Admin,
        dispatch: if method == "OPTIONS" {
            Dispatch::OptionsMetadata
        } else {
            Dispatch::MethodNotAllowed
        },
    }
}
// These demands are not authentication implementations. AllowAny still runs
// presented credential authentication first. Inactive/invalid users and UI
// network policy stay in the real shared retained-authorizer, not a fake DTO.
// HEAD invokes the GET action (including lazy writes), then suppresses wire
// body while preserving the generated length. OPTIONSmetadata remains an
// explicit unimplemented adapter capability, not an invented empty200.
