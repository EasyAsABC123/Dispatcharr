//! Actual pinned original Python3.13/Unicode15.1 XC timestamp conversions.
//! Original Dispatcharr application authors retain credit.
#[test]
fn original_xc_float_grammar_and_finite_bits() {
    let source: serde_json::Value =
        serde_json::from_str(include_str!("../datasets/source-xc-float.json")).unwrap();
    assert_eq!(source["python"], "3.13.18");
    assert_eq!(source["unicode"], "15.1.0");
    let cases = source["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 2827);
    for row in cases {
        let input = row["input"].as_str().unwrap();
        let actual = crate::source_float_literal(input);
        if row.get("error").is_some() {
            assert!(actual.is_none(), "Source rejects {input:?}");
        } else {
            let actual = actual.unwrap_or_else(|| panic!("Source accepts {input:?}"));
            match row["class"].as_str().unwrap() {
                "nan" => assert!(actual.is_nan(), "{input:?}"),
                "inf" => assert!(actual.is_infinite(), "{input:?}"),
                "finite" => assert_eq!(
                    actual.to_bits(),
                    u64::from_str_radix(row["bits"].as_str().unwrap(), 16).unwrap(),
                    "{input:?}"
                ),
                _ => panic!("unknown Source class"),
            }
            assert_eq!(
                actual.is_sign_negative(),
                row["negative"].as_bool().unwrap(),
                "{input:?}"
            );
        }
    }
}
