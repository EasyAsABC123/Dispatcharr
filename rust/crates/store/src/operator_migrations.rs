//! Ordinary operator migration inspection on the pinned Original migration graph.
//! Original Dispatcharr and Django authors retain full credit.
use crate::{operator_commands::OperatorError, Store};
use std::fmt::Write as _;

const PLAN: [(&str, &str); 184] = include!("../datasets/showmigrations-plan-order.in");

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Inspection {
    CoreList,
    Plan,
}

impl Inspection {
    pub fn parse(args: &[String]) -> Result<Self, OperatorError> {
        let args: Vec<_> = args.iter().map(String::as_str).collect();
        let head = args
            .strip_prefix(&["showmigrations"])
            .ok_or(OperatorError::Arguments)?;
        let mode = head
            .strip_suffix(&["--database", "default", "--no-color"])
            .unwrap_or(head);
        match mode {
            ["core", "--list"] => Ok(Self::CoreList),
            ["--plan"] => Ok(Self::Plan),
            _ => Err(OperatorError::Arguments),
        }
    }

    pub(crate) async fn execute(self, store: &Store) -> Result<String, OperatorError> {
        let (apps, names): (Vec<_>, Vec<_>) = PLAN.iter().copied().unzip();
        let rows = sqlx::query_as::<_, (i64, bool)>(
            "SELECT n.ordinal, EXISTS (
                 SELECT FROM public.django_migrations AS applied
                 WHERE applied.app = n.app AND applied.name = n.migration
             )
             FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS n(app, migration, ordinal)
             ORDER BY n.ordinal",
        )
        .bind(apps)
        .bind(names)
        .fetch_all(store.pool())
        .await?;
        let mut applied = [false; PLAN.len()];
        if rows.len() != applied.len() {
            return Err(OperatorError::Unavailable);
        }
        for (index, (ordinal, present)) in rows.into_iter().enumerate() {
            if ordinal != index as i64 + 1 {
                return Err(OperatorError::Unavailable);
            }
            applied[index] = present;
        }
        Ok(self.render(&applied))
    }

    fn render(self, applied: &[bool; PLAN.len()]) -> String {
        let mut text = String::new();
        if matches!(self, Self::CoreList) {
            text.push_str("core\n");
        }
        for (ordinal, (app, name)) in PLAN.iter().enumerate() {
            let mark = if applied[ordinal] { 'X' } else { ' ' };
            match self {
                Self::Plan => writeln!(text, "[{mark}]  {app}.{name}").unwrap(),
                Self::CoreList if *app == "core" => writeln!(text, " [{mark}] {name}").unwrap(),
                Self::CoreList => {}
            }
        }
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::collections::BTreeSet;

    #[test]
    fn checked_plan_is_the_current_schema_inventory_in_original_order() {
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../datasets/backup-restore-current-schema.json"
        ))
        .unwrap();
        let expected: Vec<(String, String)> =
            serde_json::from_value(schema["migrations"].clone()).unwrap();
        let graph: Vec<_> = expected
            .iter()
            .map(|(a, n)| (a.as_str(), n.as_str()))
            .collect();
        assert_eq!(graph.len(), PLAN.len());
        assert_eq!(PLAN.iter().collect::<BTreeSet<_>>(), graph.iter().collect());
        assert_eq!(&PLAN[..91], &graph[..91]);
        assert_ne!(PLAN, graph.as_slice());

        let list = Inspection::CoreList.render(&[true; PLAN.len()]);
        let plan = Inspection::Plan.render(&[true; PLAN.len()]);
        assert_eq!(list.lines().count(), 33);
        assert!(list.starts_with("core\n [X] 0001_initial\n"));
        assert_eq!(plan.lines().count(), 184);
        assert!(plan.starts_with("[X]  epg.0001_initial\n"));
        assert!(plan.ends_with("\n"));
        assert_eq!(
            format!("{:x}", Sha256::digest(list.as_bytes())),
            "59e5e0805ea783803c1043d9864626c571ce0332cfac75f4fe8a5f482c2dc0ab"
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(plan.as_bytes())),
            "b7498724cd126933385a8bf78904546934ab620ffc1d22882842417486e29527"
        );
    }

    #[test]
    fn pending_only_changes_the_live_record_mark() {
        let position = PLAN
            .iter()
            .position(|key| *key == ("core", "0032_limit_default_interleave_delta"))
            .unwrap();
        let mut applied = [true; PLAN.len()];
        applied[position] = false;
        let list = Inspection::CoreList.render(&applied);
        let plan = Inspection::Plan.render(&applied);
        assert_eq!(list.matches(" [ ] ").count(), 1);
        assert_eq!(plan.matches("[ ]  ").count(), 1);
        assert!(list.contains(" [ ] 0032_limit_default_interleave_delta\n"));
        assert!(plan.contains("[ ]  core.0032_limit_default_interleave_delta\n"));
    }

    #[test]
    fn bounded_operator_arguments() {
        let parse = |args: &[&str]| {
            Inspection::parse(&args.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        };
        assert_eq!(
            parse(&["showmigrations", "core", "--list"]).unwrap(),
            Inspection::CoreList
        );
        assert_eq!(
            parse(&[
                "showmigrations",
                "--plan",
                "--database",
                "default",
                "--no-color"
            ])
            .unwrap(),
            Inspection::Plan
        );
        assert!(parse(&["showmigrations", "--plan", "--database", "other"]).is_err());
        assert!(parse(&["showmigrations", "auth", "--list"]).is_err());
    }
}
