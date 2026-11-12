//! Native safe operator CLI. Original Dispatcharr authors retain full credit.
use dispatcharr_store::{
    operator_commands::{OperatorCommands, OperatorError},
    Store,
};
use std::{sync::Arc, time::Duration};
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--help"] {
        println!("Native Dispatcharr safe operator\nUsage: dispatcharr-operator reset_network_access\n       dispatcharr-operator reset_user_network [--user USERNAME]\n       dispatcharr-operator createsuperuser --noinput [--username USERNAME] [--email EMAIL]\nOnly these safe commands and flags are supported. Django global flags are not emulated.");
        return;
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("operator runtime");
    let result = runtime.block_on(async {
        let database = std::env::var("DATABASE_URL").map_err(|_| OperatorError::Unavailable)?;
        let redis = std::env::var("REDIS_URL").map_err(|_| OperatorError::Unavailable)?;
        let store = Store::connect(&database, 2)
            .await
            .map_err(|_| OperatorError::Unavailable)?;
        let events = if args.first().is_some_and(|s| {
            s == "reset_network_access" || s == "reset_user_network" || s == "createsuperuser"
        }) {
            dispatcharr_realtime::RealtimeBus::connect(&redis, Default::default())
                .await
                .ok()
                .map(|bus| Arc::new(bus) as Arc<dyn dispatcharr_contracts::UpdateEventSink>)
        } else {
            None
        };
        let service = OperatorCommands::new(store, redis, events);
        let result = service.run_arguments(&args, Arc::new(())).await;
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
