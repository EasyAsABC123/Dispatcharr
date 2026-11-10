//! Original XMLTV channel fields/progress; algorithm credit: Dispatcharr authors.
//! SQL batch/cache/cleanup and job orchestration are separate from this projector.
use dispatcharr_ingest_core::XmltvElement;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelRow {
    pub tvg_id: String,
    pub name: String,
    pub icon_url: Option<String>,
}
impl ChannelRow {
    pub(crate) fn allocated_fields(&self) -> usize {
        self.tvg_id
            .capacity()
            .saturating_add(self.name.capacity())
            .saturating_add(self.icon_url.as_ref().map_or(0, String::capacity))
    }
}
/// Exact Python3.13 Unicode15.1 str.strip whitespace (including U+001C–001F).
fn strip(value: &str) -> &str {
    value.trim_matches(|ch:char|matches!(ch,'\u{0009}'..='\u{000d}'|'\u{001c}'..='\u{0020}'|'\u{0085}'|'\u{00a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'))
}
/// Blank/missing IDs produce no row, but the original caller still increments
/// its processed-channel count. Namespace-qualified channel tags are ignored.
pub fn project_channel(node: &XmltvElement) -> Option<ChannelRow> {
    if node.tag != "channel" {
        return None;
    }
    let tvg = strip(node.attrs.get("id").map(String::as_str).unwrap_or(""));
    if tvg.is_empty() {
        return None;
    }
    let mut name = None;
    let mut icon = None;
    for child in &node.children {
        if name.is_none()
            && child.tag == "display-name"
            && child.text.as_ref().is_some_and(|text| !text.is_empty())
        {
            name = Some(strip(child.text.as_ref().unwrap()).to_owned());
        } else if child.tag == "icon" {
            let raw = strip(child.attrs.get("src").map(String::as_str).unwrap_or(""));
            icon = if raw.chars().count() > 500 {
                None
            } else {
                Some(raw.to_owned())
            };
        }
        if name.as_ref().is_some_and(|value| !value.is_empty())
            && icon.as_ref().is_some_and(|value| !value.is_empty())
        {
            break;
        }
    }
    let name = name
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| tvg.to_owned());
    // IDs are deliberately not truncated: Source exposes varchar255 errors.
    Some(ChannelRow {
        tvg_id: tvg.to_owned(),
        name: name.chars().take(512).collect(),
        icon_url: icon,
    })
}

pub fn progress(processed: u64, mut estimate: u64, had_baseline: bool) -> (u8, u64) {
    if !had_baseline {
        return ((processed / 100).saturating_add(25).min(97) as u8, estimate);
    }
    if processed > estimate {
        estimate = processed;
    }
    if estimate == 0 {
        return (25, estimate);
    }
    let value = 25 + ((processed as f64 / estimate as f64) * 73.0 * 0.98) as u8;
    (value.min(98), estimate)
}
pub fn should_publish(processed: u64, initial_total: u64, had_baseline: bool) -> bool {
    let interval = if had_baseline && initial_total > 0 && processed <= initial_total {
        (initial_total / 20).max(1)
    } else {
        100
    };
    processed.is_multiple_of(interval)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn node(value: serde_json::Value) -> XmltvElement {
        serde_json::from_value(value).unwrap()
    }
    #[test]
    fn blank_first_name_is_not_replaced_and_last_icon_wins() {
        let root = node(
            serde_json::json!({"tag":"channel","attrs":{"id":"\u{00a0}existing\u{00a0}"},"children":[{"tag":"display-name","text":" "},{"tag":"display-name","text":"Ignored"},{"tag":"icon","attrs":{"src":"first"}},{"tag":"icon","attrs":{"src":"last"}}]}),
        );
        assert_eq!(
            project_channel(&root),
            Some(ChannelRow {
                tvg_id: "existing".into(),
                name: "existing".into(),
                icon_url: Some("last".into())
            })
        );
    }
    #[test]
    fn character_limits_and_literal_empty_icon_match_source() {
        let root = node(
            serde_json::json!({"tag":"channel","attrs":{"id":"x".repeat(256)},"children":[{"tag":"display-name","text":"é🦀".repeat(300)},{"tag":"icon","attrs":{"src":"é".repeat(500)}}]}),
        );
        let row = project_channel(&root).unwrap();
        assert_eq!(row.tvg_id.len(), 256);
        assert_eq!(row.name.chars().count(), 512);
        assert_eq!(row.icon_url.unwrap().chars().count(), 500);
        let root = node(
            serde_json::json!({"tag":"channel","attrs":{"id":"x"},"children":[{"tag":"icon"}]}),
        );
        assert_eq!(
            project_channel(&root).unwrap().icon_url,
            Some(String::new())
        );
    }
    #[test]
    fn source_progress_growth_and_empty_identifier_ticks() {
        assert_eq!(progress(1, 6, true), (36, 6));
        assert_eq!(progress(6, 6, true), (96, 6));
        assert_eq!(progress(501, 100, true), (96, 501));
        assert_eq!(progress(500, 0, false), (30, 0));
        assert_eq!(progress(100_000, 0, false), (97, 0));
        assert!(should_publish(3, 6, true));
        assert!(!should_publish(7, 6, true));
        assert!(should_publish(100, 6, true));
    }
    #[test]
    fn retained_allocation_is_charged_even_when_content_is_short() {
        let mut name = String::with_capacity(4 * 1024 * 1024);
        name.push('A');
        let row = ChannelRow {
            tvg_id: "fixture".into(),
            name,
            icon_url: None,
        };
        assert!(row.allocated_fields() >= 4 * 1024 * 1024);
    }
}
