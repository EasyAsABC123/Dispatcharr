//! Source auto-sync regex dependencies; Rust retains every model/job effect.
//! Original Dispatcharr and regex authors retain algorithm/library credit.
use crate::{auto_sync::Error, auto_sync_streams::Streams, PythonRegexError, RegexExecutor};
use serde_json::{json, Map, Value};
use std::sync::Arc;
type Owner = Arc<dyn Send + Sync>;
pub(crate) struct Rules {
    executor: RegexExecutor,
    include: Option<Value>,
    exclude: Option<Value>,
    pattern: Value,
    replacement: Value,
}
pub(crate) enum Renamed {
    Name(String),
    Failure(String),
}
fn remote(error: PythonRegexError) -> Error {
    match error {
        PythonRegexError::Remote(value) => {
            if value["actual_class"] == "CompatibilityUnavailable" {
                return Error::Unsupported;
            }
            match (value["actual_class"].as_str(), value["message"].as_str()) {
                (Some(_), Some(message)) => Error::Caught(message.to_owned()),
                _ => Error::Unsettled,
            }
        }
        _ => Error::Unsettled,
    }
}
impl Rules {
    pub(crate) fn new(properties: &Map<String, Value>, executor: RegexExecutor) -> Self {
        Self {
            executor,
            include: properties
                .get("name_match_regex")
                .filter(|v| crate::xc_live_provider::truthy(v))
                .cloned(),
            exclude: properties
                .get("name_match_exclude_regex")
                .filter(|v| crate::xc_live_provider::truthy(v))
                .cloned(),
            pattern: properties
                .get("name_regex_pattern")
                .cloned()
                .unwrap_or(Value::Null),
            replacement: properties
                .get("name_replace_pattern")
                .cloned()
                .unwrap_or(Value::Null),
        }
    }
    pub(crate) fn may_filter(&self) -> bool {
        self.include.is_some() || self.exclude.is_some()
    }
    /// Compile/search include for its ENTIRE set, then compile/search exclude
    /// for the survivors, preserving Source's invalid-pattern/list distinction.
    pub(crate) async fn filter(&self, streams: &Streams, owner: Owner) -> Result<bool, Error> {
        let mut list = false;
        for (pattern, exclude) in [(&self.include, false), (&self.exclude, true)] {
            let Some(pattern) = pattern else { continue };
            let configured = json!({"pattern":pattern});
            let proof = self
                .executor
                .execute(
                    json!({"kind":"filter","names":[],"config":configured}),
                    owner.clone(),
                )
                .await
                .map_err(remote)?;
            let active = proof["active"].as_bool().ok_or(Error::Unsettled)?;
            if !active {
                continue;
            }
            list = true;
            let mut after = 0;
            loop {
                let page = streams.names(after).await?;
                if page.is_empty() {
                    break;
                }
                after = page.last().ok_or(Error::Unsettled)?.0;
                let names = page.iter().map(|(_, _, name)| name).collect::<Vec<_>>();
                let result = self
                    .executor
                    .execute(
                        json!({"kind":"filter","names":names,"config":configured}),
                        owner.clone(),
                    )
                    .await
                    .map_err(remote)?;
                if result["active"] != true {
                    return Err(Error::Unsettled);
                }
                let matches = result["matches"]
                    .as_array()
                    .filter(|m| m.len() == page.len())
                    .ok_or(Error::Unsettled)?;
                let mut rejected = Vec::new();
                for ((_, id, _), matched) in page.iter().zip(matches) {
                    if matched.as_bool().ok_or(Error::Unsettled)? == exclude {
                        rejected.push(*id)
                    }
                }
                streams.exclude(rejected).await?;
            }
        }
        Ok(list)
    }
    pub(crate) async fn rename(
        &self,
        names: Vec<String>,
        owner: Owner,
    ) -> Result<Vec<Renamed>, Error> {
        if self.pattern.is_null() {
            return Ok(names.into_iter().map(Renamed::Name).collect());
        }
        let expected = names.len();
        let result=self.executor.execute(json!({"kind":"rename","names":names,"config":{"pattern":self.pattern,"replacement":self.replacement}}),owner).await.map_err(remote)?;
        let values = result["items"]
            .as_array()
            .filter(|items| items.len() == expected)
            .ok_or(Error::Unsettled)?;
        values
            .iter()
            .map(|value| {
                if !value["other_failure"].is_null() {
                    return value["other_failure"]["message"]
                        .as_str()
                        .map(|message| Renamed::Failure(message.to_owned()))
                        .ok_or(Error::Unsettled);
                }
                let name = value["name"]
                    .as_str()
                    .filter(|name| name.chars().count() <= 512)
                    .ok_or(Error::Unsettled)?;
                // Actual regex.error/TimeoutError already selected Source's original
                // name in the library. Capacity/child/protocol loss never comes here.
                Ok(Renamed::Name(name.to_owned()))
            })
            .collect()
    }
}
