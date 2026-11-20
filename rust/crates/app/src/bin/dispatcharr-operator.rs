//! Native safe operator CLI. Original Dispatcharr authors retain full credit.
#[path = "../database_config.rs"]
mod database_config;
#[path = "../redis_config.rs"]
mod redis_config;

use dispatcharr_store::{
    operator_commands::{OperatorCommands, OperatorError},
    operator_migrations::Inspection,
    Store,
};
use std::{sync::Arc, time::Duration};
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--help"] {
        println!("Native Dispatcharr safe operator\nUsage: dispatcharr-operator reset_network_access\n       dispatcharr-operator reset_user_network [--user USERNAME]\n       dispatcharr-operator createsuperuser --noinput [--username USERNAME] [--email EMAIL]\n       dispatcharr-operator showmigrations core --list\n       dispatcharr-operator showmigrations --plan\nOnly these safe commands and flags are supported. Django global flags are not emulated.");
        return;
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("operator runtime");
    let result = runtime.block_on(async {
        let inspection = args
            .first()
            .filter(|command| *command == "showmigrations")
            .map(|_| Inspection::parse(&args))
            .transpose()?;
        let database = database_config::url().map_err(|_| OperatorError::Unavailable)?;
        let redis = redis_config::url()
            .map_err(|_| OperatorError::Unavailable)?
            .ok_or(OperatorError::Unavailable)?;
        let store = Store::connect(&database, 2)
            .await
            .map_err(|_| OperatorError::Unavailable)?;
        let events = if inspection.is_some()
            || args.first().is_some_and(|s| {
                s == "reset_network_access" || s == "reset_user_network" || s == "createsuperuser"
            }) {
            dispatcharr_realtime::RealtimeBus::connect(&redis, Default::default())
                .await
                .ok()
                .map(|bus| Arc::new(bus) as Arc<dyn dispatcharr_contracts::UpdateEventSink>)
        } else {
            None
        };
        let startup = inspection.map(|mode| (mode, store.clone(), redis.clone()));
        let service = OperatorCommands::new(store, redis, events);
        let result = if let Some((inspection, store, redis)) = startup {
            service
                .show_migrations(inspection, Arc::new(()), move |owner| async move {
                    if dispatcharr_job_transport::initialize_operator_backup_schedule(
                        store, redis, owner,
                    )
                    .await
                    .is_err()
                    {
                        eprintln!("Backup schedule initialization did not settle");
                    }
                })
                .await
        } else {
            service.run_arguments(&args, Arc::new(())).await
        };
        if !service.shutdown(Duration::from_secs(30)).await {
            return Err(OperatorError::Unavailable);
        }
        result
    });
    match result {
        Ok(stdout) => print!("{stdout}"),
        Err(error) => {
            let code = if matches!(error, OperatorError::Arguments) {
                2
            } else {
                1
            };
            eprintln!("{error}");
            std::process::exit(code)
        }
    }
}
