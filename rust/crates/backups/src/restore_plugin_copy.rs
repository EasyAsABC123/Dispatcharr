//! Read-only bounded COPY projection used before current-schema restore.
//! Original Dispatcharr and PostgreSQL authors retain full workflow credit.

const PREFIX: &str = "COPY public.plugins_pluginconfig (";
const SUFFIX: &str = ") FROM stdin;";
const MAX_COLUMNS: usize = 64;
const MAX_ROWS: usize = 256;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct InvalidPluginCopy;

struct PluginRow<'copy, 'columns> {
    columns: &'columns [&'copy str],
    values: Vec<&'copy str>,
}

impl<'copy> PluginRow<'copy, '_> {
    fn raw(&self, field: &str) -> Option<&'copy str> {
        self.columns
            .iter()
            .position(|name| *name == field)
            .map(|position| self.values[position])
    }
}

fn visit_rows(
    bytes: &[u8],
    mut visit: impl FnMut(PluginRow<'_, '_>) -> Result<(), InvalidPluginCopy>,
) -> Result<(), InvalidPluginCopy> {
    let text = std::str::from_utf8(bytes).map_err(|_| InvalidPluginCopy)?;
    let mut lines = text.lines();
    let names = lines
        .find_map(|line| line.strip_prefix(PREFIX)?.strip_suffix(SUFFIX))
        .ok_or(InvalidPluginCopy)?;
    let columns: Vec<_> = names.splitn(MAX_COLUMNS + 1, ", ").collect();
    if columns.len() > MAX_COLUMNS {
        return Err(InvalidPluginCopy);
    }

    for (index, line) in lines.enumerate() {
        if line == "\\." {
            return Ok(());
        }
        if index >= MAX_ROWS {
            return Err(InvalidPluginCopy);
        }
        let values: Vec<_> = line.splitn(columns.len() + 1, '\t').collect();
        if values.len() != columns.len() {
            return Err(InvalidPluginCopy);
        }
        visit(PluginRow {
            columns: &columns,
            values,
        })?;
    }
    Err(InvalidPluginCopy)
}

/// The existing restore domain admits only disabled plugins with no history.
pub(crate) fn require_disabled(bytes: &[u8]) -> Result<(), InvalidPluginCopy> {
    visit_rows(bytes, |row| {
        for (field, expected) in [
            ("enabled", "f"),
            ("ever_enabled", "f"),
            ("owned_tasks", "[]"),
            ("suspended_schedules", "[]"),
        ] {
            if row.raw(field) != Some(expected) {
                return Err(InvalidPluginCopy);
            }
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn copy(columns: &str, rows: &str) -> String {
        format!("-- pg_restore projection\n{PREFIX}{columns}{SUFFIX}\n{rows}\\.\n")
    }

    #[test]
    fn projection_retains_exact_copy_cells_and_column_order() {
        let text = copy(
            "source_repo_id, enabled, settings, name",
            "\\N\tt\t{\"prefix\": \"saved\"}\tName\\tWith\\tTabs\n",
        );
        let mut rows = 0;
        visit_rows(text.as_bytes(), |row| {
            rows += 1;
            assert_eq!(row.raw("source_repo_id"), Some(r"\N"));
            assert_eq!(row.raw("enabled"), Some("t"));
            assert_eq!(row.raw("settings"), Some(r#"{"prefix": "saved"}"#));
            assert_eq!(row.raw("name"), Some(r"Name\tWith\tTabs"));
            assert_eq!(row.raw("unprojected"), None);
            Ok(())
        })
        .unwrap();
        assert_eq!(rows, 1);
    }

    #[test]
    fn production_policy_keeps_the_existing_disabled_only_domain() {
        let columns = "enabled, ever_enabled, owned_tasks, suspended_schedules";
        assert!(require_disabled(copy(columns, "").as_bytes()).is_ok());
        assert!(require_disabled(copy(columns, "f\tf\t[]\t[]\n").as_bytes()).is_ok());
        for row in ["t\tt\t[]\t[]\n", "f\tt\t[]\t[]\n", "f\tf\t[]\t[1]\n"] {
            assert!(require_disabled(copy(columns, row).as_bytes()).is_err());
        }
    }

    #[test]
    fn bounds_apply_before_row_policy_and_terminator_is_required() {
        let columns = "enabled, ever_enabled, owned_tasks, suspended_schedules";
        let exactly = "f\tf\t[]\t[]\n".repeat(MAX_ROWS);
        assert!(require_disabled(copy(columns, &exactly).as_bytes()).is_ok());
        assert!(require_disabled(copy(columns, &(exactly + "f\tf\t[]\t[]\n")).as_bytes()).is_err());
        assert!(visit_rows(
            copy(&vec!["field"; MAX_COLUMNS].join(", "), "").as_bytes(),
            |_| Ok(())
        )
        .is_ok());
        assert!(visit_rows(
            copy(&vec!["field"; MAX_COLUMNS + 1].join(", "), "").as_bytes(),
            |_| Ok(())
        )
        .is_err());
        assert!(
            require_disabled(format!("{PREFIX}{columns}{SUFFIX}\nf\tf\t[]\t[]\n").as_bytes())
                .is_err()
        );
        assert!(require_disabled(copy(columns, "f\tf\t[]\t[]\textra\n").as_bytes()).is_err());
    }
}
