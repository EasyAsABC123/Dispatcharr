//! StreamProfile.build_command from Original Dispatcharr core/models.py.
//! Full application and algorithm credit remains with the Original authors.
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamCommand {
    pub argv: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandError {
    Quote,
    Escape,
    ReplacementType,
    ChannelType,
    Capacity,
}
impl CommandError {
    pub fn source_type(&self) -> &'static str {
        match self {
            Self::Quote | Self::Escape => "builtins.ValueError",
            Self::ReplacementType => "builtins.TypeError",
            Self::ChannelType | Self::Capacity => "unsupported-native-domain",
        }
    }
}
const LIMIT: usize = 256 * 1024;
/// Python shlex.split(posix=True, comments=False), without shell expansion.
/// Whitespace is the explicit Python shlex whitespace set, not Unicode trim.
fn split(parameters: &str) -> Result<Vec<String>, CommandError> {
    if parameters.len() > LIMIT {
        return Err(CommandError::Capacity);
    }
    let mut result = Vec::new();
    let mut part = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut began = false;
    for ch in parameters.chars() {
        if escaped {
            if quote == Some('"') && ch != '"' && ch != '\\' {
                part.push('\\');
            }
            part.push(ch);
            escaped = false;
        } else if quote == Some('\'') {
            if ch == '\'' {
                quote = None;
            } else {
                part.push(ch);
            }
        } else if ch == '\\' {
            escaped = true;
            began = true;
        } else if quote == Some('"') {
            if ch == '"' {
                quote = None;
            } else {
                part.push(ch);
            }
        } else if ch == '\'' || ch == '"' {
            quote = Some(ch);
            began = true;
        } else if matches!(ch, ' ' | '\t' | '\r' | '\n') {
            if began {
                result.push(std::mem::take(&mut part));
                began = false;
            }
        } else {
            part.push(ch);
            began = true;
        }
    }
    if escaped {
        return Err(CommandError::Escape);
    }
    if quote.is_some() {
        return Err(CommandError::Quote);
    }
    if began {
        result.push(part);
    }
    Ok(result)
}
fn channel_text(value: &Value) -> Result<String, CommandError> {
    match value {
        Value::Null | Value::Bool(false) => Ok(String::new()),
        Value::Bool(true) => Ok("True".into()),
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) if value.as_i64() == Some(0) || value.as_f64() == Some(0.0) => {
            Ok(String::new())
        }
        Value::Number(value) if value.is_i64() || value.is_u64() => Ok(value.to_string()),
        _ => Err(CommandError::ChannelType),
    }
}
fn replace(part: String, key: &str, value: &str) -> Result<String, CommandError> {
    let count = part.matches(key).count();
    let size = part
        .len()
        .checked_sub(count * key.len())
        .and_then(|n| {
            count
                .checked_mul(value.len())
                .and_then(|added| n.checked_add(added))
        })
        .ok_or(CommandError::Capacity)?;
    if size > LIMIT {
        return Err(CommandError::Capacity);
    }
    Ok(part.replace(key, value))
}
/// Split parameters BEFORE ordered replacements. The command is one literal
/// argv element. Proxy short-circuits even malformed parameters/None agent.
/// NUL remains in the model result; the physical spawn boundary rejects it.
impl StreamCommand {
    pub fn build(
        name: &str,
        locked: bool,
        command: &str,
        parameters: &str,
        stream_url: Option<&str>,
        user_agent: Option<&str>,
        channel_id: &Value,
    ) -> Result<Self, CommandError> {
        if locked && name == "Proxy" {
            return Ok(Self { argv: Vec::new() });
        }
        if command.len() > LIMIT {
            return Err(CommandError::Capacity);
        }
        let mut argv = vec![command.to_owned()];
        let channel = channel_text(channel_id)?;
        let mut total = command.len();
        for part in split(parameters)? {
            let part = replace(
                part,
                "{streamUrl}",
                stream_url.ok_or(CommandError::ReplacementType)?,
            )?;
            let part = replace(
                part,
                "{userAgent}",
                user_agent.ok_or(CommandError::ReplacementType)?,
            )?;
            let part = replace(part, "{channelId}", &channel)?;
            total = total
                .checked_add(part.len())
                .ok_or(CommandError::Capacity)?;
            if total > LIMIT {
                return Err(CommandError::Capacity);
            }
            argv.push(part);
        }
        Ok(Self { argv })
    }
    pub fn spawn_compatible(&self) -> bool {
        !self.argv.is_empty()
            && !self.argv[0].is_empty()
            && self.argv.iter().all(|arg| !arg.as_bytes().contains(&0))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_original_stream_profile18_argument_observations() {
        let data: Value =
            serde_json::from_str(include_str!("../datasets/stream-profile-source18.json")).unwrap();
        for row in data["cases"].as_array().unwrap() {
            let input = &row["input"];
            let result = StreamCommand::build(
                row["actual_profile_name"].as_str().unwrap(),
                input["locked"].as_bool().unwrap(),
                input["command"].as_str().unwrap(),
                input["parameters"].as_str().unwrap(),
                input["stream_url"].as_str(),
                input["user_agent"].as_str(),
                &input["channel_id"],
            );
            let expected = &row["observed"];
            if expected["kind"] == "return" {
                assert_eq!(
                    serde_json::json!(result.unwrap().argv),
                    expected["argv"],
                    "{}",
                    row["name"]
                );
            } else {
                assert_eq!(
                    result.unwrap_err().source_type(),
                    expected["exception_type"].as_str().unwrap(),
                    "{}",
                    row["name"]
                );
            }
        }
        assert_eq!(data["cases"].as_array().unwrap().len(), 18);
    }
}
