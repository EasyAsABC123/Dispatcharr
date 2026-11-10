//! Shared original XMLTV window rules. Original Dispatcharr authors retain credit.
pub use dispatcharr_output_http::epg_window::{resolve, Error, Window};
#[cfg(test)]
use dispatcharr_store::EpgHistoryPolicy;
#[cfg(test)]
use serde_json::Value;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_export_window_vectors() {
        let corpus: Value =
            serde_json::from_str(include_str!("../datasets/source-window.json")).unwrap();
        let cases = corpus["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 1664);
        for (index, case) in cases.iter().enumerate() {
            let params = case["params"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| {
                    (
                        v[0].as_str().unwrap().to_owned(),
                        v[1].as_str().unwrap().to_owned(),
                    )
                })
                .collect::<Vec<_>>();
            let policy: EpgHistoryPolicy = serde_json::from_value(case["policy"].clone()).unwrap();
            let got = resolve(&params, Some(&case["custom_properties"]), policy);
            if case.get("error").is_some() {
                assert!(got.is_err(), "case {index}");
                continue;
            }
            let expected = &case["expected"];
            assert_eq!(
                got,
                Ok(Window {
                    forward_days: expected["forward_days"].as_i64().unwrap(),
                    prev_days: expected["prev_days"].as_i64().unwrap()
                }),
                "case {index}"
            );
        }
    }
}
