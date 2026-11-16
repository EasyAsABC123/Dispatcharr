//! Pure decision portion of pinned Source apps/channels/epg_matching.py.
//! Original Dispatcharr and RapidFuzz authors retain algorithm/application credit.
//! PRIVATE, NOT JOINED: SQL, actual model inference, task ownership and HTTP are
//! deliberately not represented by this module. Model requests are explicit.
use regex::Regex;
use serde_json::Value;
use std::sync::OnceLock;

const EXTRANEOUS: &[&str] = &[
    "tv",
    "channel",
    "network",
    "television",
    "east",
    "west",
    "hd",
    "uhd",
    "24/7",
    "1080p",
    "720p",
    "540p",
    "480p",
    "film",
    "movie",
    "movies",
];
fn python_space(c: char) -> bool {
    c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}')
}
// Source lines126-133 explicitly discard non-list ignore settings; lines140-153
// skip false/non-string entries. This is a JSON settings-snapshot adapter, not
// arbitrary Python-object iterability or exception equivalence.
fn strings<'a>(settings: &'a Value, key: &str) -> impl Iterator<Item = &'a str> {
    settings
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|s| !s.is_empty())
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NormalizationError {
    TruthyNonStringName,
}
/// Preserve Source's early false-name return; reject truthy non-string values.
/// The error is typed capability rejection, not an invented Python exception
/// class. ORM Channel/EPG names normally enter through the &str interface.
pub fn normalize_value(name: &Value, settings: &Value) -> Result<String, NormalizationError> {
    let false_name = match name {
        Value::Null => true,
        Value::Bool(v) => !v,
        Value::Number(v) => v.as_f64().is_some_and(|v| v == 0.0),
        Value::String(v) => v.is_empty(),
        Value::Array(v) => v.is_empty(),
        Value::Object(v) => v.is_empty(),
    };
    if false_name {
        return Ok(String::new());
    }
    let name = name
        .as_str()
        .ok_or(NormalizationError::TruthyNonStringName)?;
    Ok(normalize(name, settings))
}
/// Caller supplies one actual CoreSettings.get_epg_settings snapshot per run.
/// Does not share a process-global settings cache across separate tasks.
pub fn normalize(name: &str, settings: &Value) -> String {
    static BRACKETS: OnceLock<Regex> = OnceLock::new();
    static PARENS: OnceLock<Regex> = OnceLock::new();
    static SIGN: OnceLock<Regex> = OnceLock::new();
    static NON_WORD: OnceLock<Regex> = OnceLock::new();
    if name.is_empty() {
        return String::new();
    }
    let mut value = name.to_owned();
    if settings.get("epg_match_mode").and_then(Value::as_str) == Some("advanced") {
        for p in strings(settings, "epg_match_ignore_prefixes") {
            if let Some(rest) = value.strip_prefix(p) {
                value = rest.to_owned();
                break;
            }
        }
        for s in strings(settings, "epg_match_ignore_suffixes") {
            if let Some(rest) = value.strip_suffix(s) {
                value = rest.to_owned();
                break;
            }
        }
        for custom in strings(settings, "epg_match_ignore_custom") {
            value = value.replace(custom, "");
        }
    }
    let lowered = value.to_lowercase();
    // Python dot does not match LF. Explicit classes preserve that distinction.
    let stripped = BRACKETS
        .get_or_init(|| Regex::new(r"\[[^\n]*?\]").unwrap())
        .replace_all(&lowered, "");
    let mut stripped = PARENS
        .get_or_init(|| Regex::new(r"\([^\n]*?\)").unwrap())
        .replace_all(&stripped, "")
        .into_owned();
    if let Some(c) = SIGN
        .get_or_init(|| Regex::new(r"\(([A-Z]{3,5})\)").unwrap())
        .captures(name)
    {
        stripped.push(' ');
        stripped.push_str(&c[1].to_lowercase());
    }
    // Rust char::is_alphanumeric uses Derived_Alphabetic and admits marks such
    // as U+0345/U+05B0; Python re \w does not. Use general letter/number classes,
    // underscore and Python's extra whitespace controls. Unicode-version edge
    // parity still requires the actual pinned Source vectors.
    let stripped = NON_WORD
        .get_or_init(|| Regex::new(r"[^\p{L}\p{N}_\s\x1c-\x1f]").unwrap())
        .replace_all(&stripped, "");
    stripped
        .split(python_space)
        .filter(|s| !s.is_empty() && !EXTRANEOUS.contains(s))
        .collect::<Vec<_>>()
        .join(" ")
}
/// RapidFuzz fuzz.ratio is normalized Indel similarity (LCS), not normalized
/// Levenshtein distance. Compute on Unicode scalar values as Python strings do.
/// This quadratic draft needs bounded/cancellable execution in its future owner.
pub fn ratio(left: &str, right: &str) -> f64 {
    let a: Vec<_> = left.chars().collect();
    let b: Vec<_> = right.chars().collect();
    if a.is_empty() && b.is_empty() {
        return 100.0;
    }
    let mut previous = vec![0usize; b.len() + 1];
    let mut current = previous.clone();
    for x in &a {
        for (j, y) in b.iter().enumerate() {
            current[j + 1] = if x == y {
                previous[j] + 1
            } else {
                current[j].max(previous[j + 1])
            };
        }
        std::mem::swap(&mut previous, &mut current);
    }
    // Match normalized_distance's floating-point operation order.
    let total = (a.len() + b.len()) as f64;
    let distance = total - 2.0 * previous[b.len()] as f64;
    (1.0 - distance / total) * 100.0
}
pub fn fuzzy_score(
    query: &str,
    normalized_name: &str,
    tvg: &str,
    name: &str,
    region: Option<&str>,
) -> f64 {
    static REGIONS: OnceLock<Regex> = OnceLock::new();
    if normalized_name.is_empty() {
        return 0.0;
    }
    let mut score = ratio(query, normalized_name);
    if let Some(region) = region.filter(|s| !s.is_empty()).filter(|_| !tvg.is_empty()) {
        let combined = format!("{} {}", tvg.to_lowercase(), name.to_lowercase());
        let regions: Vec<_> = REGIONS
            .get_or_init(|| Regex::new(r"\.([a-z]{2})").unwrap())
            .captures_iter(&combined)
            .map(|c| c[1].to_owned())
            .collect();
        score += if !regions.is_empty() {
            if regions.iter().any(|r| r == region) {
                15.0
            } else {
                -15.0
            }
        } else if combined.contains(region) {
            10.0
        } else {
            0.0
        };
    }
    score
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Decision {
    AcceptBest,
    Reject,
    /// Actual SentenceTransformer cosine scores are required. Do not treat an
    /// unbound model as a zero-score model or silently accept the fuzzy winner.
    NeedBestModel {
        minimum: f64,
    },
    NeedCandidateModel {
        minimum: f64,
    },
}
pub fn decision(best_score: f64, bulk: bool, use_ml: bool) -> Decision {
    let (skip, medium, minimum_fuzzy, minimum_ml) = if bulk {
        (80.0, 70.0, 50.0, 0.65)
    } else {
        (75.0, 40.0, 20.0, 0.50)
    };
    // Source's high/skip branches both return the same best row.
    if best_score >= skip {
        return Decision::AcceptBest;
    }
    if !use_ml {
        return Decision::Reject;
    }
    if best_score >= medium {
        Decision::NeedBestModel {
            minimum: minimum_ml,
        }
    } else if best_score >= minimum_fuzzy {
        Decision::NeedCandidateModel {
            minimum: minimum_ml,
        }
    } else {
        Decision::Reject
    }
}

/// SQL caller supplies the exact Source catalog order and one immutable
/// normalization-settings snapshot. tvg_id and norm_name are already normalized.
/// Single exact lookup is a separate DB IEXACT/first-PK operation, not this index.
#[derive(Clone, Debug, PartialEq)]
pub struct EpgCandidate {
    pub id: i64,
    pub name: String,
    pub tvg_id: String,
    pub norm_name: String,
    pub priority: i64,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ScoredCandidate {
    pub index: usize,
    pub score: f64,
    pub candidate: EpgCandidate,
}
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Scan {
    pub best_score: f64,
    pub best: Option<ScoredCandidate>,
    pub top_candidates: Vec<ScoredCandidate>,
    pub scanned: usize,
}
#[derive(Clone)]
struct HeapEntry {
    value: ScoredCandidate,
    seq: usize,
}
fn heap_order(a: &HeapEntry, b: &HeapEntry) -> std::cmp::Ordering {
    a.value
        .score
        .total_cmp(&b.value.score)
        .then(a.value.candidate.priority.cmp(&b.value.candidate.priority))
        .then(a.seq.cmp(&b.seq))
}
// Pinned heapq's tuple order is (score, priority, increasing sequence, row).
// The unique sequence prevents Python from comparing row dictionaries. Retain
// its heap-array tie order before the stable final score/priority-only sort.
fn sift_down(heap: &mut [HeapEntry], start: usize, mut pos: usize) {
    let value = heap[pos].clone();
    while pos > start {
        let parent = (pos - 1) >> 1;
        if !heap_order(&value, &heap[parent]).is_lt() {
            break;
        }
        heap[pos] = heap[parent].clone();
        pos = parent;
    }
    heap[pos] = value;
}
fn sift_up(heap: &mut [HeapEntry], mut pos: usize) {
    let end = heap.len();
    let start = pos;
    let value = heap[pos].clone();
    let mut child = 2 * pos + 1;
    while child < end {
        let right = child + 1;
        if right < end && !heap_order(&heap[child], &heap[right]).is_lt() {
            child = right;
        }
        heap[pos] = heap[child].clone();
        pos = child;
        child = 2 * pos + 1;
    }
    heap[pos] = value;
    sift_down(heap, start, pos);
}
/// Streaming single-pass Source scan. Only the best and top20 rows are retained;
/// the caller still owns SQL snapshots, cancellation and the task lifecycle.
pub struct Scanner {
    query: String,
    region: Option<String>,
    result: Scan,
    heap: Vec<HeapEntry>,
    seq: usize,
}
impl Scanner {
    pub fn new(query: &str, region: Option<&str>) -> Self {
        Self {
            query: query.to_owned(),
            region: region.map(str::to_owned),
            result: Scan::default(),
            heap: Vec::with_capacity(20),
            seq: 0,
        }
    }
    pub fn push(&mut self, index: usize, row: &EpgCandidate) {
        if row.norm_name.is_empty() {
            return;
        }
        self.result.scanned += 1;
        let score = fuzzy_score(
            &self.query,
            &row.norm_name,
            &row.tvg_id,
            &row.name,
            self.region.as_deref(),
        );
        if score <= 0.0 {
            return;
        }
        let value = ScoredCandidate {
            index,
            score,
            candidate: row.clone(),
        };
        if score > self.result.best_score
            || (score == self.result.best_score
                && row.priority
                    > self
                        .result
                        .best
                        .as_ref()
                        .map_or(-1, |r| r.candidate.priority))
        {
            self.result.best_score = score;
            self.result.best = Some(value.clone());
        }
        self.seq += 1;
        let entry = HeapEntry {
            value,
            seq: self.seq,
        };
        if self.heap.len() < 20 {
            self.heap.push(entry);
            let pos = self.heap.len() - 1;
            sift_down(&mut self.heap, 0, pos);
        } else {
            let smallest = &self.heap[0].value;
            if score > smallest.score
                || (score == smallest.score && row.priority > smallest.candidate.priority)
            {
                self.heap[0] = entry;
                sift_up(&mut self.heap, 0);
            }
        }
    }
    pub fn finish(mut self) -> Scan {
        self.heap.sort_by(|a, b| {
            b.value
                .score
                .total_cmp(&a.value.score)
                .then(b.value.candidate.priority.cmp(&a.value.candidate.priority))
        });
        self.result.top_candidates = self.heap.into_iter().map(|v| v.value).collect();
        self.result
    }
}
pub fn scan(query: &str, rows: &[EpgCandidate], region: Option<&str>) -> Scan {
    let mut scanner = Scanner::new(query, region);
    for (index, row) in rows.iter().enumerate() {
        scanner.push(index, row);
    }
    scanner.finish()
}
/// Bulk exact index semantics: first normalized nonempty TVG key in the caller's
/// stable descending-priority catalog wins. Do not use for single DB IEXACT.
pub fn exact_tvg<'a>(normalized_tvg: &str, rows: &'a [EpgCandidate]) -> Option<&'a EpgCandidate> {
    if normalized_tvg.is_empty() {
        return None;
    }
    rows.iter().find(|r| r.tvg_id == normalized_tvg)
}
/// Bind the selection to an actual best row; absent candidates remain Reject.
/// A model-needed decision is an unresolved capability, never a fake NoMatch.
pub fn selection(result: &Scan, bulk: bool, use_ml: bool) -> Decision {
    if result.best.is_none() {
        Decision::Reject
    } else {
        decision(result.best_score, bulk, use_ml)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn pinned_normalization_examples_and_python_word_distinction() {
        let s = json!({"epg_match_mode":"advanced","epg_match_ignore_prefixes":["HD:","SD:"],
                     "epg_match_ignore_suffixes":[" 4K"],"epg_match_ignore_custom":["Plus"]});
        assert_eq!(
            normalize("HD:ABC 7 (WXYZ) - Springfield", &s),
            "abc 7 springfield wxyz"
        );
        assert_eq!(normalize("HD:SD:Channel 5", &s), "sdchannel 5");
        assert_eq!(normalize("HBO Plus East 4K", &s), "hbo");
        assert_eq!(normalize("a\u{301}\u{200d}_b", &json!({})), "a_b");
        assert_eq!(normalize("a\u{345}\u{5b0}_b", &json!({})), "a_b");
        let malformed = json!({"epg_match_mode":"advanced","epg_match_ignore_prefixes":"HD:"});
        assert_eq!(normalize("HD:Owned", &malformed), "hdowned");
        assert_eq!(normalize_value(&Value::Null, &json!({})), Ok(String::new()));
        assert_eq!(normalize_value(&json!([]), &json!({})), Ok(String::new()));
        assert_eq!(
            normalize_value(&json!(17), &json!({})),
            Err(NormalizationError::TruthyNonStringName)
        );
        assert_eq!(normalize("HD:ABC", &json!({})), "hdabc");
    }
    #[test]
    fn indel_not_levenshtein_and_regions_can_exceed_one_hundred() {
        assert!((ratio("ab", "ac") - 50.0).abs() < 1e-12);
        assert_eq!(ratio("", ""), 100.0);
        assert_eq!(ratio("é", "é"), 100.0);
        assert_eq!(
            fuzzy_score("abc", "abc", "abc.us", "ABC", Some("us")),
            115.0
        );
        assert_eq!(fuzzy_score("abc", "abc", "abc.gb", "ABC", Some("us")), 85.0);
    }
    #[test]
    fn bulk_versus_single_and_unbound_model_remain_distinct() {
        assert_eq!(decision(77.0, false, true), Decision::AcceptBest);
        assert_eq!(
            decision(77.0, true, true),
            Decision::NeedBestModel { minimum: 0.65 }
        );
        assert_eq!(
            decision(30.0, false, true),
            Decision::NeedCandidateModel { minimum: 0.50 }
        );
        assert_eq!(decision(30.0, true, true), Decision::Reject);
        assert_eq!(decision(60.0, false, false), Decision::Reject);
    }
}

#[cfg(test)]
mod source51_vectors {
    use super::*;
    use serde_json::Value;
    fn vectors() -> Value {
        serde_json::from_str(include_str!("../datasets/source-channel-epg-matching.json")).unwrap()
    }
    #[test]
    fn actual_source_normalization_values_and_errors_stay_distinct() {
        let source = vectors();
        let rows = source["normalization"].as_array().unwrap();
        assert_eq!(rows.len(), 38);
        for row in rows {
            let got = normalize_value(&row["input"], &row["settings_snapshot"]);
            if row["outcome"] == "error" {
                assert_eq!(row["error_class"], "AttributeError");
                assert!(
                    got.is_err(),
                    "Source error became a value at {}",
                    row["ordinal"]
                );
            } else {
                assert_eq!(
                    got.unwrap(),
                    row["actual"].as_str().unwrap(),
                    "Source vector {}",
                    row["ordinal"]
                );
            }
        }
    }
    #[test]
    fn actual_rapidfuzz_scores_and_source_region_adjustments() {
        let source = vectors();
        for row in source["ratios"].as_array().unwrap() {
            let got = ratio(
                row["left"].as_str().unwrap(),
                row["right"].as_str().unwrap(),
            );
            assert_eq!(got.to_bits(), row["actual"].as_f64().unwrap().to_bits());
        }
        for row in source["regions"].as_array().unwrap() {
            let got = fuzzy_score(
                "owned",
                "owned",
                "owned.us",
                "Owned",
                row["region"].as_str(),
            );
            assert_eq!(got, row["actual"].as_f64().unwrap());
        }
    }
    #[test]
    fn source_decision_vectors_keep_inference_boundary_explicit() {
        let source = vectors();
        let rows = source["threshold_decisions"].as_array().unwrap();
        assert_eq!(rows.len(), 80);
        for row in rows {
            let d = decision(
                row["fuzzy"].as_f64().unwrap(),
                row["bulk"].as_bool().unwrap(),
                true,
            );
            // Source execution supplied known test scores. This is NOT a model
            // implementation or proof actual inference/availability works.
            let matched = match d {
                Decision::AcceptBest => true,
                Decision::Reject => false,
                Decision::NeedBestModel { minimum } | Decision::NeedCandidateModel { minimum } => {
                    row["supplied_model_score"]
                        .as_f64()
                        .is_some_and(|v| v >= minimum - 1e-9)
                }
            };
            assert_eq!(
                matched,
                row["matched_id"].as_i64() == Some(1),
                "Source row {row}"
            );
        }
    }
}

#[cfg(test)]
mod source_scan_tests {
    use super::*;
    fn row(id: i64, priority: i64, norm: &str) -> EpgCandidate {
        EpgCandidate {
            id,
            name: "Owned".into(),
            tvg_id: "owned.us".into(),
            norm_name: norm.into(),
            priority,
        }
    }
    #[test]
    fn actual_source_top20_priority_vector_and_streaming_indices() {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-channel-epg-matching.json"))
                .unwrap();
        let rows: Vec<_> = (0..31).map(|n| row(n, n, "owned")).collect();
        let got = scan("owned", &rows, None);
        assert_eq!(got.scanned, 31);
        assert_eq!(got.best.as_ref().unwrap().candidate.id, 30);
        let actual: Vec<_> = got.top_candidates.iter().map(|r| r.candidate.id).collect();
        let wanted: Vec<_> = source["top20_actual_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_i64().unwrap())
            .collect();
        assert_eq!(actual, wanted);
        let mut stream = Scanner::new("owned", None);
        for (index, r) in rows.iter().enumerate() {
            stream.push(index, r);
        }
        assert_eq!(stream.finish(), got);
        assert!(got
            .top_candidates
            .iter()
            .all(|r| r.index == r.candidate.id as usize));
    }
    #[test]
    fn equal_scores_keep_first_best_and_do_not_replace_equal_heap_cutoff() {
        let rows: Vec<_> = (0..31).map(|n| row(n, 1, "owned")).collect();
        let got = scan("owned", &rows, None);
        assert_eq!(got.best.as_ref().unwrap().candidate.id, 0);
        assert_eq!(
            got.top_candidates
                .iter()
                .map(|r| r.candidate.id)
                .collect::<Vec<_>>(),
            (0..20).collect::<Vec<_>>()
        );
        assert_eq!(selection(&got, true, true), Decision::AcceptBest);
    }
    #[test]
    fn named_zero_or_negative_scores_are_scanned_but_never_model_candidates() {
        let mut a = row(1, 1, "");
        let mut b = row(2, 1, "zzzz");
        a.tvg_id = "owned.gb".into();
        b.tvg_id = "owned.gb".into();
        let got = scan("aaaa", &[a, b], Some("us"));
        assert_eq!(got.scanned, 1);
        assert!(got.best.is_none());
        assert!(got.top_candidates.is_empty());
        assert_eq!(selection(&got, false, true), Decision::Reject);
    }
    #[test]
    fn bulk_exact_uses_received_catalog_order_and_does_not_adopt_single_sql_rules() {
        let rows = vec![row(10, 100, "owned"), row(1, 1, "owned")];
        assert_eq!(exact_tvg("owned.us", &rows).unwrap().id, 10);
        assert!(exact_tvg("", &rows).is_none());
    }
}
