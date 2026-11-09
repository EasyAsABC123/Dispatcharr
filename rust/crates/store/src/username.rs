//! Original Django UserManager NFKC with pinned Python Unicode15.1 assignment.
//! Original Dispatcharr contributors retain all application credit.
use std::sync::OnceLock;
use unicode_normalization::UnicodeNormalization;
fn source_assigned(character: char) -> bool {
    static RANGES: OnceLock<Vec<[u32; 2]>> = OnceLock::new();
    let ranges = RANGES.get_or_init(|| {
        serde_json::from_str(include_str!("../datasets/python-assigned-ranges-15-1.json"))
            .expect("embedded original Unicode assignment ranges")
    });
    let code = character as u32;
    ranges
        .partition_point(|range| range[0] <= code)
        .checked_sub(1)
        .is_some_and(|index| code <= ranges[index][1])
}
pub(crate) fn normalize_username(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut start = 0;
    for (index, character) in value.char_indices() {
        if !source_assigned(character) {
            // Source treats unassigned codepoints as unchanged, CCC0 barriers.
            // Newer Rust normalization data must not decompose these or compose
            // surrounding source characters across their boundary.
            output.extend(value[start..index].nfkc());
            output.push(character);
            start = index + character.len_utf8();
        }
    }
    output.extend(value[start..].nfkc());
    output
}
pub(crate) fn lower_source(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut start = 0;
    for (index, character) in value.char_indices() {
        if !source_assigned(character) {
            output.push_str(&value[start..index].to_lowercase());
            output.push(character);
            start = index + character.len_utf8();
        }
    }
    output.push_str(&value[start..].to_lowercase());
    output
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_changed_maps_and_unchanged_defaults_cover_all_unicode_scalars() {
        use std::collections::HashMap;
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../datasets/python-nfkc-15-1.json")).unwrap();
        let mappings = |key: &str| {
            fixture[key]
                .as_array()
                .unwrap()
                .iter()
                .map(|case| {
                    (
                        case[0].as_str().unwrap().chars().next().unwrap() as u32,
                        case[1].as_str().unwrap(),
                    )
                })
                .collect::<HashMap<_, _>>()
        };
        let nfkc = mappings("changed_codepoints");
        let lower = mappings("lower_changed_codepoints");
        for code in 0..=0x10ffff {
            let Some(character) = char::from_u32(code) else {
                continue;
            };
            let mut bytes = [0u8; 4];
            let raw = character.encode_utf8(&mut bytes);
            assert_eq!(
                normalize_username(raw),
                nfkc.get(&code).copied().unwrap_or(raw),
                "NFKC U+{code:04X}"
            );
            assert_eq!(
                lower_source(raw),
                lower.get(&code).copied().unwrap_or(raw),
                "lower U+{code:04X}"
            );
        }
    }
    #[test]
    fn every_original_nfkc_changed_codepoint_and_boundary_string_matches() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../datasets/python-nfkc-15-1.json")).unwrap();
        assert_eq!(fixture["unicode_version"], "15.1.0");
        assert_eq!(
            fixture["changed_codepoints"].as_array().unwrap().len(),
            4928
        );
        for case in fixture["changed_codepoints"]
            .as_array()
            .unwrap()
            .iter()
            .chain(fixture["strings"].as_array().unwrap())
        {
            assert_eq!(
                normalize_username(case[0].as_str().unwrap()),
                case[1].as_str().unwrap()
            );
        }
        for case in fixture["lower_changed_codepoints"]
            .as_array()
            .unwrap()
            .iter()
            .chain(fixture["lower_strings"].as_array().unwrap())
        {
            assert_eq!(
                lower_source(case[0].as_str().unwrap()),
                case[1].as_str().unwrap()
            );
        }
        assert!(!source_assigned('\u{1ccd6}'));
        assert_eq!(normalize_username("A\u{1ccd6}\u{301}"), "A\u{1ccd6}\u{301}");
    }
}
