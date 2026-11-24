//! Source backup_status catches a plain propagated exception as HTTP500.
//! Original Dispatcharr authors retain full application credit and AGPL license.
//! This selector does not decode/import classes or broaden tagged/multi-arg data.
pub(crate) fn single_failure_message<'a>(
    is_failure: bool,
    module: Option<&str>,
    class: Option<&str>,
    tagged: bool,
    argument_count: usize,
    first_string: Option<&'a str>,
) -> Option<&'a str> {
    if !is_failure
        || module != Some("builtins")
        || class != Some("ValueError")
        || tagged
        || argument_count != 1
    {
        return None;
    }
    first_string.filter(|value| value.len() <= 8192)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn captured_plain_propagated_failure_reaches_source_message() {
        let message = single_failure_message(
            true,
            Some("builtins"),
            Some("ValueError"),
            false,
            1,
            Some("fixture uncaught"),
        );
        assert_eq!(message, Some("fixture uncaught"));
        assert_eq!(
            format!("Failed to get task status: {}", message.unwrap()),
            "Failed to get task status: fixture uncaught"
        );
    }
    #[test]
    fn nonfailure_is_not_reclassified() {
        assert_eq!(
            single_failure_message(
                false,
                Some("builtins"),
                Some("ValueError"),
                false,
                1,
                Some("fixture uncaught")
            ),
            None
        );
    }
    #[test]
    fn tagged_and_multi_argument_grammars_remain_explicitly_unhandled() {
        assert_eq!(
            single_failure_message(
                true,
                Some("builtins"),
                Some("ValueError"),
                true,
                1,
                Some("fixture uncaught")
            ),
            None
        );
        assert_eq!(
            single_failure_message(
                true,
                Some("builtins"),
                Some("ValueError"),
                false,
                2,
                Some("fixture uncaught")
            ),
            None
        );
    }
    #[test]
    fn missing_or_nonstring_argument_is_not_fabricated() {
        assert_eq!(
            single_failure_message(true, Some("builtins"), Some("ValueError"), false, 0, None),
            None
        );
        assert_eq!(
            single_failure_message(true, Some("builtins"), Some("ValueError"), false, 1, None),
            None
        );
    }
    #[test]
    fn other_exception_string_rules_are_not_guessed() {
        assert_eq!(
            single_failure_message(
                true,
                Some("builtins"),
                Some("KeyError"),
                false,
                1,
                Some("fixture uncaught")
            ),
            None
        );
        assert_eq!(
            single_failure_message(
                true,
                Some("unobserved"),
                Some("ValueError"),
                false,
                1,
                Some("fixture uncaught")
            ),
            None
        );
    }
    #[test]
    fn message_byte_budget_is_preserved() {
        let text = "x".repeat(8193);
        assert_eq!(
            single_failure_message(
                true,
                Some("builtins"),
                Some("ValueError"),
                false,
                1,
                Some(&text)
            ),
            None
        );
    }
}
