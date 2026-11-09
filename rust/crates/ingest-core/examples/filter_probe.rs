//! Candidate comparison only; this does not establish Python re equivalence.
//! Original Dispatcharr algorithms retain original-author attribution.
use fancy_regex::RegexBuilder;
use serde_json::{json, Value};
use std::io::{self, BufRead};
fn compile(pattern: &str, ignore: bool) -> Result<fancy_regex::Regex, fancy_regex::Error> {
    let mut builder = RegexBuilder::new(pattern);
    builder
        .case_insensitive(ignore)
        .backtrack_limit(100_000)
        .delegate_size_limit(1024 * 1024)
        .delegate_dfa_size_limit(1024 * 1024);
    builder.build()
}
fn main() {
    for line in io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let result = if request["kind"] == "regex" {
            match compile(
                request["pattern"].as_str().unwrap(),
                request["ignore_case"].as_bool().unwrap(),
            ) {
                Ok(regex) => {
                    json!({"compiled":true,"matches":request["texts"].as_array().unwrap().iter().map(|value|regex.is_match(value.as_str().unwrap()).ok()).collect::<Vec<_>>()})
                }
                Err(error) => json!({"compiled":false,"error":error.to_string()}),
            }
        } else {
            let mut accepted = true;
            for filter in request["filters"].as_array().unwrap() {
                let insensitive =
                    filter["custom_properties"]["case_sensitive"].as_bool() == Some(false);
                let regex =
                    compile(filter["regex_pattern"].as_str().unwrap(), insensitive).unwrap();
                let field = match filter["filter_type"].as_str().unwrap() {
                    "url" => "url",
                    "group" => "group",
                    _ => "name",
                };
                if regex
                    .is_match(request["targets"][field].as_str().unwrap_or(""))
                    .unwrap()
                {
                    accepted = !filter["exclude"].as_bool().unwrap();
                    break;
                }
            }
            json!(accepted)
        };
        println!("{result}");
    }
}
