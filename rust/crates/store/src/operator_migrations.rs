//! Ordinary operator migration inspection on the pinned Original migration graph.
//! Original Dispatcharr and Django authors retain full credit.
use crate::{operator_commands::OperatorError, Store};
use chrono::{DateTime, Utc};
use std::{collections::BTreeSet, fmt::Write as _};

const PLAN: [(&str, &str); 184] = include!("../datasets/showmigrations-plan-order.in");
const PLAN_DEPENDENCIES: [(&str, &str); PLAN.len()] =
    include!("../datasets/showmigrations-plan-dependencies.in");
const APP_WITHOUT_MIGRATIONS: &str = "backups";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Inspection {
    List(Option<&'static str>),
    ListWithDates(Option<&'static str>),
    Plan,
    PlanWithDependencies,
}

#[derive(Clone, Copy, Default)]
struct AppliedMigration {
    present: bool,
    timestamp: Option<DateTime<Utc>>,
}

fn migration_verbosity(value: &str) -> Result<bool, OperatorError> {
    match value {
        "0" | "1" => Ok(false),
        "2" | "3" => Ok(true),
        _ => Err(OperatorError::Arguments),
    }
}

impl Inspection {
    pub fn parse(args: &[String]) -> Result<Self, OperatorError> {
        let mut args = args.iter().map(String::as_str);
        if args.next() != Some("showmigrations") {
            return Err(OperatorError::Arguments);
        }

        let mut plan = None;
        let mut app = None;
        let mut verbose = false;
        while let Some(argument) = args.next() {
            match argument {
                "--list" | "-l" | "--plan" | "-p" => {
                    let requested_plan = matches!(argument, "--plan" | "-p");
                    if plan
                        .replace(requested_plan)
                        .is_some_and(|old| old != requested_plan)
                    {
                        return Err(OperatorError::Arguments);
                    }
                }
                "--database" => {
                    if args.next() != Some("default") {
                        return Err(OperatorError::Arguments);
                    }
                }
                "--database=default" | "--no-color" => {}
                "--verbosity" | "-v" => {
                    verbose = migration_verbosity(args.next().ok_or(OperatorError::Arguments)?)?;
                }
                _ => {
                    let attached_verbosity = argument.strip_prefix("--verbosity=").or_else(|| {
                        argument
                            .strip_prefix("-v")
                            .map(|value| value.strip_prefix('=').unwrap_or(value))
                    });
                    if let Some(level) = attached_verbosity {
                        verbose = migration_verbosity(level)?;
                    } else if argument.starts_with('-') || app.replace(argument).is_some() {
                        return Err(OperatorError::Arguments);
                    }
                }
            }
        }

        if plan == Some(true) {
            if app.is_some() {
                return Err(OperatorError::Arguments);
            }
            return Ok(if verbose {
                Self::PlanWithDependencies
            } else {
                Self::Plan
            });
        }

        let selected = app
            .map(|name| {
                PLAN.iter()
                    .map(|(label, _)| *label)
                    .chain([APP_WITHOUT_MIGRATIONS])
                    .find(|label| *label == name)
                    .ok_or(OperatorError::Arguments)
            })
            .transpose()?;
        Ok(if verbose {
            Self::ListWithDates(selected)
        } else {
            Self::List(selected)
        })
    }

    pub(crate) async fn execute(self, store: &Store) -> Result<String, OperatorError> {
        let (apps, names): (Vec<_>, Vec<_>) = PLAN.iter().copied().unzip();
        let sql = if matches!(self, Self::ListWithDates(_)) {
            "SELECT n.ordinal, applied.id IS NOT NULL, applied.applied
             FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS n(app, migration, ordinal)
             LEFT JOIN LATERAL (
                 SELECT id, applied FROM public.django_migrations
                 WHERE app = n.app AND name = n.migration
                 ORDER BY id DESC LIMIT 1
             ) AS applied ON true
             ORDER BY n.ordinal"
        } else {
            "SELECT n.ordinal, EXISTS (
                 SELECT FROM public.django_migrations AS applied
                 WHERE applied.app = n.app AND applied.name = n.migration
             ), NULL::timestamptz
             FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS n(app, migration, ordinal)
             ORDER BY n.ordinal"
        };
        let rows = sqlx::query_as::<_, (i64, bool, Option<DateTime<Utc>>)>(sql)
            .bind(apps)
            .bind(names)
            .fetch_all(store.pool())
            .await?;
        let mut applied = [AppliedMigration::default(); PLAN.len()];
        if rows.len() != applied.len() {
            return Err(OperatorError::Unavailable);
        }
        for (index, (ordinal, present, timestamp)) in rows.into_iter().enumerate() {
            if ordinal != index as i64 + 1 {
                return Err(OperatorError::Unavailable);
            }
            applied[index] = AppliedMigration { present, timestamp };
        }
        Ok(self.render(&applied))
    }

    fn render(self, applied: &[AppliedMigration; PLAN.len()]) -> String {
        let mut text = String::new();
        let mark = |ordinal: usize| if applied[ordinal].present { 'X' } else { ' ' };
        match self {
            Self::Plan | Self::PlanWithDependencies => {
                for (index, (app, name)) in PLAN.iter().enumerate() {
                    write!(text, "[{}]  {app}.{name}", mark(index)).unwrap();
                    let (_, dependencies) = PLAN_DEPENDENCIES[index];
                    if self == Self::PlanWithDependencies && !dependencies.is_empty() {
                        write!(text, " ... ({dependencies})").unwrap();
                    }
                    text.push('\n');
                }
            }
            Self::List(selected) | Self::ListWithDates(selected) => {
                let verbose = matches!(self, Self::ListWithDates(_));
                let applications = match selected {
                    Some(app) => BTreeSet::from([app]),
                    None => PLAN
                        .iter()
                        .map(|(app, _)| *app)
                        .chain([APP_WITHOUT_MIGRATIONS])
                        .collect(),
                };
                for application in applications {
                    writeln!(text, "{application}").unwrap();
                    if application == APP_WITHOUT_MIGRATIONS {
                        text.push_str(" (no migrations)\n");
                    }
                    for (index, (app, name)) in PLAN.iter().enumerate() {
                        if *app == application {
                            write!(text, " [{}] {name}", mark(index)).unwrap();
                            if verbose {
                                if let Some(timestamp) = applied[index].timestamp {
                                    write!(
                                        text,
                                        " (applied at {})",
                                        timestamp.format("%Y-%m-%d %H:%M:%S")
                                    )
                                    .unwrap();
                                }
                            }
                            text.push('\n');
                        }
                    }
                }
            }
        }
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    const PRESENT: AppliedMigration = AppliedMigration {
        present: true,
        timestamp: None,
    };

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

        let list = Inspection::List(Some("core")).render(&[PRESENT; PLAN.len()]);
        let plan = Inspection::Plan.render(&[PRESENT; PLAN.len()]);
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
    fn default_and_accounts_lists_match_the_original_bytes() {
        let applied = [PRESENT; PLAN.len()];
        let list = Inspection::List(None).render(&applied);
        let accounts = Inspection::List(Some("accounts")).render(&applied);
        assert_eq!(
            format!("{:x}", Sha256::digest(list.as_bytes())),
            "9cf457d770363c6cb6c3f793b10f2b98b4dff721b608d9c5097902a624f53285"
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(accounts.as_bytes())),
            "182cd1e0e1a6bbe4349c2edf738d8464b048dcde19fd05eaf418a976bf640eea"
        );
        assert_eq!(
            Inspection::List(Some("backups")).render(&applied),
            "backups\n (no migrations)\n"
        );
    }

    #[test]
    fn verbose_plan_matches_original_complete_stdout_and_direct_dependency_order() {
        let mut seen = BTreeSet::new();
        let mut edge_count = 0;
        for ((app, name), (label, dependencies)) in PLAN.iter().zip(PLAN_DEPENDENCIES) {
            assert_eq!(label, format!("{app}.{name}"));
            let mut previous = "";
            for parent in dependencies.split(", ").filter(|parent| !parent.is_empty()) {
                assert!(seen.contains(parent) && previous < parent);
                previous = parent;
                edge_count += 1;
            }
            assert!(seen.insert(label));
        }
        assert_eq!(edge_count, 203);
        let plan = Inspection::PlanWithDependencies.render(&[PRESENT; PLAN.len()]);
        assert_eq!(plan.len(), 18671);
        assert_eq!(plan.lines().count(), 184);
        assert_eq!(
            format!("{:x}", Sha256::digest(plan.as_bytes())),
            "3337afece625efb09643c1eb06fe3caec3fd5aceb1574e6f31f0c2a14a3a4675"
        );
    }

    #[test]
    fn pending_only_changes_the_live_record_mark() {
        let position = PLAN
            .iter()
            .position(|key| *key == ("core", "0032_limit_default_interleave_delta"))
            .unwrap();
        let mut applied = [PRESENT; PLAN.len()];
        applied[position] = AppliedMigration::default();
        let list = Inspection::List(Some("core")).render(&applied);
        let plan = Inspection::Plan.render(&applied);
        assert_eq!(list.matches(" [ ] ").count(), 1);
        assert_eq!(plan.matches("[ ]  ").count(), 1);
        assert!(list.contains(" [ ] 0032_limit_default_interleave_delta\n"));
        assert!(plan.contains("[ ]  core.0032_limit_default_interleave_delta\n"));
        let verbose = Inspection::PlanWithDependencies.render(&applied);
        assert!(verbose.contains(
            "[ ]  core.0032_limit_default_interleave_delta ... (core.0031_default_reverse_proxy_auth_settings)\n"
        ));
    }

    #[test]
    fn verbose_inventory_formats_actual_applied_times_to_whole_utc_seconds() {
        let first = PLAN
            .iter()
            .position(|key| *key == ("accounts", "0001_initial"))
            .unwrap();
        let mut applied = [AppliedMigration::default(); PLAN.len()];
        applied[first] = AppliedMigration {
            present: true,
            timestamp: Some(
                DateTime::parse_from_rfc3339("2026-11-20T12:01:02.999999+01:00")
                    .unwrap()
                    .with_timezone(&Utc),
            ),
        };
        let accounts = Inspection::ListWithDates(Some("accounts")).render(&applied);
        assert!(
            accounts.starts_with("accounts\n [X] 0001_initial (applied at 2026-11-20 11:01:02)\n")
        );
        assert_eq!(accounts.matches(" (applied at ").count(), 1);
        assert_eq!(accounts.matches(" [ ] ").count(), 7);
        let all = Inspection::ListWithDates(None).render(&applied);
        assert_eq!(all.matches(" (applied at ").count(), 1);
        assert_eq!(all.matches(" [ ] ").count(), 183);
        assert_eq!(
            all.replace(" (applied at 2026-11-20 11:01:02)", ""),
            Inspection::List(None).render(&applied)
        );
    }

    #[test]
    fn explicit_verbosity_and_short_forms_select_the_existing_source_renderers() {
        let cases: &[(&[&str], Inspection)] = &[
            (&["-v", "3", "--plan"], Inspection::PlanWithDependencies),
            (
                &["--database", "default", "-p", "-v", "2", "--no-color"],
                Inspection::PlanWithDependencies,
            ),
            (
                &["-l", "core", "--verbosity=0"],
                Inspection::List(Some("core")),
            ),
            (
                &[
                    "--database=default",
                    "--verbosity=3",
                    "--no-color",
                    "accounts",
                ],
                Inspection::ListWithDates(Some("accounts")),
            ),
            (
                &["core", "-l", "--verbosity", "0"],
                Inspection::List(Some("core")),
            ),
            (&["-p", "--verbosity=1"], Inspection::Plan),
            (&["-p", "-v", "2"], Inspection::PlanWithDependencies),
            (&["--plan", "-v3"], Inspection::PlanWithDependencies),
            (
                &["accounts", "-l", "-v=3"],
                Inspection::ListWithDates(Some("accounts")),
            ),
            (
                &["--list", "--verbosity=3"],
                Inspection::ListWithDates(None),
            ),
        ];
        for (tail, expected) in cases {
            let args = std::iter::once("showmigrations")
                .chain(tail.iter().copied())
                .map(str::to_owned)
                .collect::<Vec<_>>();
            assert_eq!(Inspection::parse(&args).unwrap(), *expected, "{args:?}");
        }
        let rejected: &[&[&str]] = &[
            &["--list", "-v"],
            &["--plan", "--verbosity=4"],
            &["--plan", "-v-1"],
            &["--verbosity", "--plan"],
            &["--database", "other", "-v", "2"],
            &["--database=other", "-p"],
            &["--unknown", "--list"],
            &["-l", "-p"],
            &["-p", "core"],
            &["core", "accounts", "-l"],
        ];
        for tail in rejected {
            let args = std::iter::once("showmigrations")
                .chain(tail.iter().copied())
                .map(str::to_owned)
                .collect::<Vec<_>>();
            assert!(Inspection::parse(&args).is_err(), "{args:?}");
        }
    }

    #[test]
    fn bounded_operator_arguments() {
        let parse = |args: &[&str]| {
            Inspection::parse(&args.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        };
        assert_eq!(
            parse(&["showmigrations", "core", "--list"]).unwrap(),
            Inspection::List(Some("core"))
        );
        assert_eq!(parse(&["showmigrations"]).unwrap(), Inspection::List(None));
        assert_eq!(
            parse(&["showmigrations", "accounts"]).unwrap(),
            Inspection::List(Some("accounts"))
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
        assert_eq!(
            parse(&[
                "showmigrations",
                "--verbosity=2",
                "--database",
                "default",
                "--no-color"
            ])
            .unwrap(),
            Inspection::ListWithDates(None)
        );
        assert_eq!(
            parse(&[
                "showmigrations",
                "accounts",
                "--list",
                "--verbosity",
                "2",
                "--database",
                "default",
                "--no-color"
            ])
            .unwrap(),
            Inspection::ListWithDates(Some("accounts"))
        );
        assert_eq!(
            parse(&["showmigrations", "--plan", "--verbosity=2"]).unwrap(),
            Inspection::PlanWithDependencies
        );
        assert_eq!(
            parse(&[
                "showmigrations",
                "--plan",
                "--verbosity",
                "2",
                "--database",
                "default",
                "--no-color"
            ])
            .unwrap(),
            Inspection::PlanWithDependencies
        );
        assert!(parse(&["showmigrations", "--plan", "--database", "other"]).is_err());
        assert!(parse(&["showmigrations", "unknown_app", "--list"]).is_err());
    }
}
