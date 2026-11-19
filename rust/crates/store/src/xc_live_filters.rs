//! Ordered Source _stream_passes_m3u_filters, using the existing regex crate.
//! Original Dispatcharr/Python regex authors retain all application credit.
use crate::{task_sql_session::TaskSqlSession, StoreError};
use regex::{RegexSet, RegexSetBuilder};
use sqlx::Row;
fn unsupported() -> StoreError {
    StoreError::Unsupported("XC filter grammar/value/compiled capacity is unsupported")
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Target {
    Name,
    Url,
    Group,
}
struct Set {
    regex: RegexSet,
    order: Vec<(usize, bool)>,
    ascii: bool,
    target: Target,
    insensitive: bool,
}
pub(crate) struct Filters {
    sets: Vec<Set>,
}
struct Rule {
    kind: Target,
    pattern: String,
    exclude: bool,
    insensitive: bool,
}
impl Filters {
    pub(crate) fn empty() -> Self {
        Self { sets: Vec::new() }
    }
    pub(crate) async fn read(session: &TaskSqlSession, id: i64) -> Result<Self, StoreError> {
        let rules=session.run(move|c|Box::pin(async move{let rows=sqlx::query("SELECT filter_type,CASE WHEN octet_length(regex_pattern)<=8192 THEN regex_pattern END AS pattern,(octet_length(regex_pattern)<=8192 AND (custom_properties IS NULL OR jsonb_typeof(custom_properties)='object' OR custom_properties IN('null'::jsonb,'false'::jsonb,'0'::jsonb,'\"\"'::jsonb,'[]'::jsonb))) AS fits,exclude,COALESCE(custom_properties->'case_sensitive'='false'::jsonb,false) AS insensitive FROM m3u_m3ufilter WHERE m3u_account_id=$1 ORDER BY \"order\" LIMIT 257").bind(id).fetch_all(&mut*c).await?;if rows.len()>256{return Err(unsupported())}rows.into_iter().map(|row|{if !row.try_get::<bool,_>("fits")?{return Err(unsupported())}let kind=match row.try_get::<String,_>("filter_type")?.as_str(){"url"=>Target::Url,"group"=>Target::Group,_=>Target::Name};Ok(Rule{kind,pattern:row.try_get("pattern")?,exclude:row.try_get("exclude")?,insensitive:row.try_get("insensitive")?})}).collect::<Result<Vec<_>,StoreError>>()})).await??;
        Self::compile(rules)
    }
    fn compile(rules: Vec<Rule>) -> Result<Self, StoreError> {
        let mut sets = Vec::new();
        for target in [Target::Name, Target::Url, Target::Group] {
            for insensitive in [false, true] {
                let mut patterns = Vec::new();
                let mut order = Vec::new();
                let mut ascii = false;
                for (index, rule) in rules.iter().enumerate() {
                    if rule.kind != target || rule.insensitive != insensitive {
                        continue;
                    }
                    if insensitive && !rule.pattern.is_ascii() {
                        return Err(unsupported());
                    }
                    let (pattern, guard) = source_pattern(&rule.pattern)?;
                    ascii |= guard;
                    patterns.push(pattern);
                    order.push((index, rule.exclude));
                }
                if patterns.is_empty() {
                    continue;
                }
                // Six bounded programs total <=6MiB, with <=1.5MiB DFA caches.
                let regex = RegexSetBuilder::new(patterns)
                    .case_insensitive(insensitive)
                    .size_limit(1024 * 1024)
                    .dfa_size_limit(256 * 1024)
                    .nest_limit(64)
                    .build()
                    .map_err(|_| unsupported())?;
                sets.push(Set {
                    regex,
                    order,
                    ascii,
                    target,
                    insensitive,
                });
            }
        }
        Ok(Self { sets })
    }
    pub(crate) fn allows(&self, name: &str, url: &str, group: &str) -> Result<bool, StoreError> {
        let mut first = None;
        for set in &self.sets {
            let text = match set.target {
                Target::Name => name,
                Target::Url => url,
                Target::Group => group,
            };
            // Python/Rust Unicode category/case-fold sets differ. This is a
            // visible grammar/target gate, never a nonmatch hiding the rule.
            if set.ascii && (!text.is_ascii() || text.bytes().any(|b| (0x1c..=0x1f).contains(&b))) {
                return Err(unsupported());
            }
            // Python's ASCII-pattern IGNORECASE includes the dotted/dotless I
            // pair in addition to Unicode simple folding. Normalize only these
            // four ASCII-equivalent codepoints, only for the insensitive set;
            // case-sensitive rules always see the untouched original string.
            let normalized;
            let text = if set.insensitive
                && text.chars().any(|c| matches!(c, 'İ' | 'ı' | 'ſ' | 'K'))
            {
                normalized = text
                    .chars()
                    .map(|c| match c {
                        'İ' | 'ı' => 'i',
                        'ſ' => 's',
                        'K' => 'k',
                        c => c,
                    })
                    .collect::<String>();
                normalized.as_str()
            } else {
                text
            };
            if let Some(local) = set.regex.matches(text).iter().next() {
                let (index, exclude) = set.order[local];
                if first.is_none_or(|(old, _)| index < old) {
                    first = Some((index, exclude));
                }
            }
        }
        Ok(first.is_none_or(|(_, exclude)| !exclude))
    }
}
// Boolean search equivalence for this regular-language subset. Python's final
// `$` can match immediately before ONE final newline; Rust's `$` cannot, so
// explicitly translate the final anchor. Interior `$`, lookaround/backrefs,
// inline flags and Python-only grammar stay unsupported rather than changed.
fn source_pattern(pattern: &str) -> Result<(String, bool), StoreError> {
    let mut out = String::new();
    let mut escaped = false;
    let mut class = false;
    let mut ascii = false;
    let chars = pattern.chars().collect::<Vec<_>>();
    for (index, c) in chars.iter().copied().enumerate() {
        if escaped {
            escaped = false;
            if c == 'Z' {
                out.push_str("\\z");
                continue;
            }
            if c == 'z'
                || c.is_ascii_digit()
                || (c.is_alphabetic() && !"AbBdDsSwWZafnrtuvUx".contains(c))
                || (matches!(c, 'u' | 'U' | 'x') && chars.get(index + 1) == Some(&'{'))
            {
                return Err(unsupported());
            }
            if "dDsSwWbB".contains(c) {
                ascii = true
            }
            out.push('\\');
            out.push(c);
            continue;
        }
        if c == '\\' {
            escaped = true;
            continue;
        }
        if class
            && index + 1 < chars.len()
            && matches!(
                (c, chars[index + 1]),
                ('&', '&') | ('-', '-') | ('~', '~') | ('|', '|')
            )
        {
            return Err(unsupported());
        }
        if c == '[' {
            if class {
                return Err(unsupported());
            }
            class = true
        } else if c == ']' {
            if !class {
                return Err(unsupported());
            }
            class = false
        }
        if !class
            && c == '('
            && chars.get(index + 1) == Some(&'?')
            && chars.get(index + 2) != Some(&':')
        {
            return Err(unsupported());
        }
        if !class && c == '$' {
            if index + 1 != chars.len() {
                return Err(unsupported());
            }
            out.push_str("(?:\\n)?\\z");
            continue;
        }
        out.push(c);
    }
    if escaped || class {
        return Err(unsupported());
    }
    Ok((out, ascii))
}
#[cfg(test)]
mod controls {
    use super::*;
    fn rule(kind: Target, pattern: &str, exclude: bool, insensitive: bool) -> Rule {
        Rule {
            kind,
            pattern: pattern.into(),
            exclude,
            insensitive,
        }
    }
    #[test]
    fn source_first_match_wins_across_url_group_and_name_targets() {
        let filters = Filters::compile(vec![
            rule(Target::Group, "Sports", false, false),
            rule(Target::Name, "^US", true, false),
            rule(Target::Url, "private", true, false),
        ])
        .unwrap();
        assert!(filters
            .allows("US One", "http://private", "Sports")
            .unwrap());
        assert!(!filters.allows("US One", "http://owned", "News").unwrap());
        assert!(filters.allows("Other", "http://owned", "News").unwrap());
    }
    #[test]
    fn python_ascii_ignorecase_folds_four_extra_letters_without_changing_sensitive_rules() {
        let filters = Filters::compile(vec![rule(Target::Name, "^kis$", true, true)]).unwrap();
        for name in ["KIS", "Kİſ", "kıs"] {
            assert!(!filters.allows(name, "", "").unwrap());
        }
        assert!(filters.allows("界", "", "").unwrap());
        let sensitive = Filters::compile(vec![rule(Target::Name, "^kis$", true, false)]).unwrap();
        assert!(sensitive.allows("Kİſ", "", "").unwrap());
        let shorthand = Filters::compile(vec![rule(Target::Name, r"\w+", true, false)]).unwrap();
        assert!(shorthand.allows("界", "", "").is_err());
    }
    #[test]
    fn source_case_flag_final_newline_anchor_and_unsupported_grammar_are_visible() {
        let filters = Filters::compile(vec![rule(Target::Name, "^US$", true, true)]).unwrap();
        assert!(!filters.allows("us\n", "", " ").unwrap());
        assert!(filters.allows("us\n\n", "", " ").unwrap());
        assert!(filters.allows("UŞ", "", " ").unwrap());
        assert!(Filters::compile(vec![rule(Target::Name, "(?<=US)One", true, false)]).is_err());
        assert!(Filters::compile(vec![rule(Target::Name, r"(US)\1", true, false)]).is_err());
    }
}
