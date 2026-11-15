//! Pinned Source Python Unicode lower/isdigit semantics, without Python runtime.
//! Original Python/Unicode and Dispatcharr authors retain all applicable credit.
//! Uses byte-exact captured CPython3.13.18/Unicode15.1 metadata; runtime parity remains unaccepted.
use crate::StoreError;
use serde::Deserialize;
use std::sync::OnceLock;
#[derive(Deserialize)]
struct Data {
    implementation: String,
    unicode_version: String,
    maximum_codepoint: u32,
    lowercase_map: Vec<(u32, Vec<u32>)>,
    cased_ranges: Vec<(u32, u32)>,
    case_ignorable_ranges: Vec<(u32, u32)>,
    non_decimal_digit_ranges: Vec<(u32, u32)>,
    controls: std::collections::BTreeMap<String, String>,
}
pub(crate) struct Language {
    data: Data,
}
fn unavailable() -> StoreError {
    StoreError::Unsupported("Pinned auto-sync Source Python language metadata is unavailable")
}
fn ranges_valid(ranges: &[(u32, u32)]) -> bool {
    ranges
        .iter()
        .all(|(start, end)| start <= end && *end <= 0x10ffff)
        && ranges.windows(2).all(|pair| pair[0].1 < pair[1].0)
}
fn contains(ranges: &[(u32, u32)], point: u32) -> bool {
    let index = ranges.partition_point(|(_, end)| *end < point);
    ranges.get(index).is_some_and(|(start, _)| *start <= point)
}
impl Language {
    pub(crate) fn source() -> Result<&'static Self, StoreError> {
        static DATA: OnceLock<Result<Language, ()>> = OnceLock::new();
        DATA.get_or_init(|| {
            // Do NOT add a fabricated/host-Python placeholder. This file must
            // be actual output of the authorized pinned Source utility, with
            // its image/process/source receipt in the milestone publication.
            let data: Data =
                serde_json::from_str(include_str!("../datasets/auto-sync-python-language.json"))
                    .map_err(|_| ())?;
            if data.implementation != "cpython"
                || data.unicode_version != "15.1.0"
                || data.maximum_codepoint != 0x10ffff
                || !ranges_valid(&data.cased_ranges)
                || !ranges_valid(&data.case_ignorable_ranges)
                || !ranges_valid(&data.non_decimal_digit_ranges)
                || !data
                    .lowercase_map
                    .windows(2)
                    .all(|pair| pair[0].0 < pair[1].0)
                || data.lowercase_map.iter().any(|(point, mapping)| {
                    char::from_u32(*point).is_none()
                        || mapping.is_empty()
                        || mapping.iter().any(|p| char::from_u32(*p).is_none())
                })
            {
                return Err(());
            }
            let language = Language { data };
            if !language
                .data
                .controls
                .iter()
                .all(|(input, expected)| language.lower(input) == *expected)
            {
                return Err(());
            }
            Ok(language)
        })
        .as_ref()
        .map_err(|_| unavailable())
    }
    pub(crate) fn is_digit(&self, c: char) -> bool {
        contains(&self.data.non_decimal_digit_ranges, c as u32)
            || dispatcharr_ingest_core::source_integer_literal(&c.to_string()).is_some()
    }
    pub(crate) fn lower(&self, text: &str) -> String {
        let points = text.chars().map(|c| c as u32).collect::<Vec<_>>();
        let mut output = String::with_capacity(text.len());
        for (index, point) in points.iter().copied().enumerate() {
            if point == 0x03a3 {
                let before = points[..index]
                    .iter()
                    .rev()
                    .copied()
                    .find(|p| !contains(&self.data.case_ignorable_ranges, *p));
                let after = points[index + 1..]
                    .iter()
                    .copied()
                    .find(|p| !contains(&self.data.case_ignorable_ranges, *p));
                let final_sigma = before.is_some_and(|p| contains(&self.data.cased_ranges, p))
                    && !after.is_some_and(|p| contains(&self.data.cased_ranges, p));
                output.push(if final_sigma { 'ς' } else { 'σ' });
            } else if let Ok(found) = self
                .data
                .lowercase_map
                .binary_search_by_key(&point, |entry| entry.0)
            {
                for target in &self.data.lowercase_map[found].1 {
                    if let Some(c) = char::from_u32(*target) {
                        output.push(c)
                    }
                }
            } else if let Some(c) = char::from_u32(point) {
                output.push(c)
            }
        }
        output
    }
}
