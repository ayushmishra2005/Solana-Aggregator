use crate::builder::Builder;
use crate::cli::Cli;
use crate::lifecycle::{finish_within, termination_after_component, Lifecycle, Termination};
use crate::util::{
    Channel, ProtocolMessage, DATABASE_CHANNEL_CAPACITY, HANDLER_CHANNEL_CAPACITY,
    OUTCOME_CHANNEL_CAPACITY,
};
use log::error;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::mpsc::channel;
use tokio::task::JoinHandle;

mod block_importer;
mod builder;
mod cli;
mod db_handler;
mod error;
mod handler;
mod lifecycle;
mod parser;
mod server;
mod util;

struct StderrLogger;

impl log::Log for StderrLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        let first_party = matches!(
            metadata.target(),
            "subscriber" | "db" | "handler" | "server"
        );
        if first_party {
            metadata.level() <= log::Level::Info
        } else {
            metadata.level() <= log::Level::Error
        }
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            eprintln!("{}: {}", record.target(), record.args());
        }
    }

    fn flush(&self) {}
}

fn install_stderr_logger() {
    static LOGGER: StderrLogger = StderrLogger;
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(log::LevelFilter::Info);
}

#[derive(Debug)]
enum CheckpointPlan {
    Resume(u64),
    Establish(u64),
}

fn plan_checkpoint(
    existing: Option<u64>,
    has_indexed_records: bool,
    observed_tip: u64,
    start_slot: Option<u64>,
) -> Result<CheckpointPlan, String> {
    if let Some(start_slot) = start_slot {
        if start_slot == 0 {
            return Err(
                "--start-slot must be at least 1 so the first fetched slot is defined".to_string(),
            );
        }
    }
    if let Some(checkpoint) = existing {
        if let Some(start_slot) = start_slot {
            let boundary = start_slot - 1;
            if boundary != checkpoint {
                return Err(format!(
                    "last_processed_slot is already {checkpoint}. --start-slot {start_slot} was not applied and the checkpoint was not changed."
                ));
            }
        }
        return Ok(CheckpointPlan::Resume(checkpoint));
    }
    if has_indexed_records {
        return Err("RocksDB has indexed blocks but no last_processed_slot checkpoint. Use a fresh database directory or reindex explicitly. Existing records were not overwritten.".to_string());
    }
    let boundary = match start_slot {
        Some(start_slot) => start_slot - 1,
        None => observed_tip,
    };
    Ok(CheckpointPlan::Establish(boundary))
}

fn resume_subscriber(
    subscriber: &mut block_importer::Subscriber,
    database: &mut db_handler::RocksDb,
    start_slot: Option<u64>,
    genesis: &str,
) -> Result<(), String> {
    let identity_matches = database.verify_chain_identity(genesis)?;
    let plan = plan_checkpoint(
        database.read_checkpoint()?,
        database.has_indexed_records()?,
        subscriber.completed_slot(),
        start_slot,
    )?;
    match plan {
        CheckpointPlan::Resume(slot) => {
            if !identity_matches {
                return Err(
                    "Database checkpoint has no verified genesis identity. Existing records were not modified. Use a fresh database directory or reindex explicitly.".to_string(),
                );
            }
            subscriber.resume_from_checkpoint(slot);
            log::info!(
                target: "subscriber",
                "Loaded checkpoint slot {slot}"
            );
        }
        CheckpointPlan::Establish(slot) => {
            database.initialize_chain(slot, genesis)?;
            subscriber.resume_from_checkpoint(slot);
            match start_slot {
                Some(start_slot) => log::info!(
                    target: "subscriber",
                    "Initialized checkpoint slot {slot} so the first fetched slot is {start_slot}"
                ),
                None => log::info!(
                    target: "subscriber",
                    "Initialized checkpoint slot {slot}"
                ),
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    install_stderr_logger();
    let opt = Cli::from_args();
    let termination = run_indexer(opt).await;
    let code = termination.exit_code();
    match &termination {
        Termination::Shutdown => {
            log::info!(target: "subscriber", "Indexer shutdown complete");
            eprintln!("Indexer shutdown complete");
        }
        Termination::Failed(error) => {
            error!(target: "subscriber", "Fatal indexer failure: {error}");
            eprintln!("Fatal indexer failure: {error}");
        }
    }
    std::process::exit(code);
}

async fn run_indexer(opt: Cli) -> Termination {
    log::info!(target: "subscriber", "Solana aggregator starting");
    let (lifecycle, shutdown) = Lifecycle::new();
    let (handler_keepalive, handler_receiver) =
        Channel::<ProtocolMessage>::new(HANDLER_CHANNEL_CAPACITY).into_parts();
    let (db_keepalive, db_receiver) =
        Channel::<ProtocolMessage>::new(DATABASE_CHANNEL_CAPACITY).into_parts();
    let (block_outcome_sender, block_outcome_receiver) = channel(OUTCOME_CHANNEL_CAPACITY);
    let server_sender = handler_keepalive.clone();
    let mut subscriber = match Builder::default()
        .chain_url(opt.chain_url)
        .router_sender(handler_keepalive.clone())
        .build(lifecycle.stopping_flag())
        .await
    {
        Ok(subscriber) => subscriber,
        Err(error) => {
            error!(target: "subscriber", "Error from subscriber client {error}");
            return Termination::Failed(format!("subscriber failed to start: {error}"));
        }
    };
    subscriber.attach_runtime(Arc::clone(&lifecycle));
    subscriber.set_block_outcomes(block_outcome_sender.clone(), block_outcome_receiver);
    let mut handler = Builder::default()
        .db_sender(db_keepalive.clone())
        .router_receiver(handler_receiver)
        .build();
    handler.set_block_outcomes(block_outcome_sender.clone());
    let mut database = match Builder::default()
        .db_path(opt.db_path)
        .db_receiver(db_receiver)
        .build()
    {
        Ok(database) => database,
        Err(error) => {
            error!(target: "db", "Error from db client {error}");
            return Termination::Failed(format!("database failed to start: {error}"));
        }
    };
    database.set_block_outcomes(block_outcome_sender);
    database.attach_runtime(Arc::clone(&lifecycle));
    let genesis = match subscriber.chain_genesis_hash().await {
        Ok(genesis) => genesis,
        Err(error) => {
            error!(target: "subscriber", "Error reading RPC genesis hash {error}");
            return Termination::Failed(format!("RPC genesis hash is unavailable: {error}"));
        }
    };
    if let Err(error) = resume_subscriber(&mut subscriber, &mut database, opt.start_slot, &genesis)
    {
        error!(target: "subscriber", "Error restoring slot checkpoint {error}");
        return Termination::Failed(error);
    }
    if let Ok(Some(slot)) = database.read_checkpoint() {
        lifecycle.note_checkpoint(slot);
    }
    match database.indexed_block_height() {
        Ok(Some(height)) => lifecycle.note_block_height(height),
        Ok(None) => {}
        Err(error) => return Termination::Failed(error.to_string()),
    }
    let mut database_task = tokio::spawn(async move { database.run().await });
    let mut handler_task = tokio::spawn(async move { handler.run().await });
    let mut server = Box::pin(server::AggServer::run(
        server_sender,
        opt.port_no,
        Arc::clone(&lifecycle),
        shutdown.clone(),
    ));
    let mut subscriber_run = Box::pin(subscriber.run_with_shutdown(shutdown.clone()));

    let mut termination = Termination::Shutdown;
    let mut subscriber_done = false;
    let mut handler_done = false;
    let mut database_done = false;
    let mut server_done = false;
    let mut signal = std::pin::pin!(shutdown_signal());

    loop {
        tokio::select! {
            biased;
            _ = &mut signal, if !lifecycle.is_stopping() => {
                log::info!(target: "subscriber", "Shutdown requested");
                eprintln!("Shutdown requested");
                lifecycle.begin_shutdown();
            }
            result = &mut subscriber_run, if !subscriber_done => {
                subscriber_done = true;
                match result {
                    Ok(()) => lifecycle.begin_shutdown(),
                    Err(error) => {
                        lifecycle.fail(error.clone());
                        termination = Termination::Failed(error);
                    }
                }
            }
            result = &mut handler_task, if !handler_done => {
                handler_done = true;
                termination = prefer_failure(
                    termination,
                    termination_after_component(&lifecycle, "handler", result.is_err()),
                );
            }
            result = &mut database_task, if !database_done => {
                database_done = true;
                termination = prefer_failure(
                    termination,
                    termination_after_component(&lifecycle, "database worker", result.is_err()),
                );
            }
            result = &mut server, if !server_done => {
                server_done = true;
                match result {
                    Ok(()) if lifecycle.is_stopping() => {}
                    Ok(()) => {
                        lifecycle.fail("http server stopped unexpectedly".to_string());
                        termination = Termination::Failed(
                            "http server stopped unexpectedly".to_string(),
                        );
                    }
                    Err(error) => {
                        let message = format!("http server failed: {error}");
                        lifecycle.fail(message.clone());
                        termination = Termination::Failed(message);
                    }
                }
            }
        }
        if lifecycle.is_stopping() {
            break;
        }
    }

    let deadline = tokio::time::Instant::now() + crate::lifecycle::SHUTDOWN_DEADLINE;
    let mut subscriber_result = None;
    let mut server_result = None;
    loop {
        if (subscriber_done || subscriber_result.is_some())
            && (server_done || server_result.is_some())
        {
            break;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        tokio::select! {
            result = &mut subscriber_run, if !subscriber_done && subscriber_result.is_none() => {
                subscriber_result = Some(result);
            }
            result = &mut server, if !server_done && server_result.is_none() => {
                server_result = Some(result);
            }
            _ = tokio::time::sleep(remaining) => break,
        }
    }
    drop(subscriber_run);
    drop(server);
    match subscriber_result {
        Some(Ok(())) => log::info!(target: "subscriber", "Subscriber stopped"),
        Some(Err(error)) => {
            lifecycle.fail(error.clone());
            termination = Termination::Failed(error);
        }
        None if !subscriber_done => {
            log::error!(
                target: "subscriber",
                "Shutdown deadline exceeded while slot {} was still in progress; checkpoint remains {}",
                subscriber.completed_slot().saturating_add(1),
                subscriber.completed_slot()
            );
        }
        None => {}
    }
    match server_result {
        Some(Ok(())) => log::info!(target: "server", "HTTP server stopped"),
        Some(Err(error)) => {
            let message = format!("http server failed: {error}");
            if matches!(termination, Termination::Shutdown) {
                termination = Termination::Failed(message);
            }
        }
        None if !server_done => {
            log::error!(target: "server", "Shutdown deadline exceeded while stopping HTTP");
        }
        None => {}
    }
    subscriber
        .shutdown_workers(std::time::Duration::from_secs(1))
        .await;
    drop(subscriber);
    drop(handler_keepalive);

    termination = finish_worker(
        "handler",
        &lifecycle,
        termination,
        &mut handler_task,
        handler_done,
        std::time::Duration::from_secs(2),
    )
    .await;
    drop(db_keepalive);
    termination = finish_worker(
        "database worker",
        &lifecycle,
        termination,
        &mut database_task,
        database_done,
        crate::lifecycle::SHUTDOWN_DEADLINE,
    )
    .await;
    termination
}

fn prefer_failure(current: Termination, next: Termination) -> Termination {
    match current {
        Termination::Failed(_) => current,
        Termination::Shutdown => next,
    }
}

async fn finish_worker(
    name: &str,
    lifecycle: &Lifecycle,
    mut termination: Termination,
    task: &mut JoinHandle<()>,
    already_finished: bool,
    budget: std::time::Duration,
) -> Termination {
    if already_finished {
        log::info!(target: "subscriber", "{name} shutdown completed");
        return termination;
    }
    match finish_within(budget, &mut *task).await {
        Ok(result) => {
            let next = termination_after_component(lifecycle, name, result.is_err());
            if matches!(termination, Termination::Shutdown) {
                termination = next;
            }
            log::info!(target: "subscriber", "{name} shutdown completed");
        }
        Err(()) => {
            log::error!(
                target: "subscriber",
                "Shutdown deadline exceeded while waiting for {name}"
            );
        }
    }
    termination
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(error) => {
                    error!(target: "subscriber", "SIGTERM handler unavailable: {error}");
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    error!(target: "subscriber", "SIGINT handler failed: {error}");
                }
            }
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::{plan_checkpoint, CheckpointPlan};

    #[test]
    fn fresh_start_uses_the_observed_tip_unless_a_start_slot_is_set() {
        assert!(matches!(
            plan_checkpoint(None, false, 80, None).unwrap(),
            CheckpointPlan::Establish(80)
        ));
        assert!(matches!(
            plan_checkpoint(None, false, 80, Some(40)).unwrap(),
            CheckpointPlan::Establish(39)
        ));
        assert!(plan_checkpoint(None, false, 80, Some(0))
            .unwrap_err()
            .contains("at least 1"));
    }

    #[test]
    fn start_slot_does_not_replace_a_different_checkpoint() {
        assert!(matches!(
            plan_checkpoint(Some(39), false, 80, Some(40)).unwrap(),
            CheckpointPlan::Resume(39)
        ));
        let error = plan_checkpoint(Some(10), false, 80, Some(40)).unwrap_err();
        assert!(error.contains("already 10"), "{error}");
        assert!(error.contains("was not applied"), "{error}");
        assert!(matches!(
            plan_checkpoint(Some(10), true, 80, None).unwrap(),
            CheckpointPlan::Resume(10)
        ));
        assert!(plan_checkpoint(None, true, 80, Some(40))
            .unwrap_err()
            .contains("reindex"));
    }
}
