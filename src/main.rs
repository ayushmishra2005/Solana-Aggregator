use crate::builder::Builder;
use crate::cli::Cli;
use crate::util::{
    Channel, ProtocolMessage, DATABASE_CHANNEL_CAPACITY, HANDLER_CHANNEL_CAPACITY,
    OUTCOME_CHANNEL_CAPACITY,
};
use log::error;
use structopt::StructOpt;
use tokio::sync::mpsc::channel;

mod block_importer;
mod builder;
mod cli;
mod db_handler;
mod error;
mod handler;
mod parser;
mod server;
mod util;

struct StderrLogger;

impl log::Log for StderrLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Error
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
    log::set_max_level(log::LevelFilter::Error);
}

fn resume_subscriber(
    subscriber: &mut block_importer::Subscriber,
    database: &mut db_handler::RocksDb,
) -> Result<(), String> {
    if let Some(checkpoint) = database.read_checkpoint()? {
        subscriber.resume_from_checkpoint(checkpoint);
        return Ok(());
    }
    if database.has_indexed_records()? {
        return Err("RocksDB has indexed blocks but no last_processed_slot checkpoint. Use a fresh database directory or reindex explicitly. Existing records were not overwritten.".to_string());
    }
    database.establish_initial_checkpoint(subscriber.completed_slot())
}

#[tokio::main]
async fn main() {
    install_stderr_logger();
    let opt: Cli = Cli::from_args();
    let handler_channel = Channel::<ProtocolMessage>::new(HANDLER_CHANNEL_CAPACITY);
    let db_channel = Channel::<ProtocolMessage>::new(DATABASE_CHANNEL_CAPACITY);
    let (block_outcome_sender, block_outcome_receiver) = channel(OUTCOME_CHANNEL_CAPACITY);
    let handler_channel_receiver_server = handler_channel.sender();
    let mut subscriber_client = match Builder::default()
        .chain_url(opt.chain_url)
        .router_sender(handler_channel.sender())
        .build()
        .await
    {
        Ok(subscriber) => subscriber,
        Err(e) => {
            error!(target:"subscriber", "Error from subscriber client {}",e);
            eprintln!("Fatal subscriber failure: {e}");
            return;
        }
    };
    subscriber_client.set_block_outcomes(block_outcome_sender.clone(), block_outcome_receiver);
    let mut handler = Builder::default()
        .db_sender(db_channel.sender())
        .router_receiver(handler_channel.receiver)
        .build();
    handler.set_block_outcomes(block_outcome_sender.clone());
    let mut db_client = match Builder::default()
        .db_path(opt.db_path)
        .db_receiver(db_channel.receiver)
        .build()
    {
        Ok(db) => db,
        Err(e) => {
            error!(target:"db", "Error from db client {}",e);
            eprintln!("Fatal database failure: {e}");
            return;
        }
    };
    db_client.set_block_outcomes(block_outcome_sender);
    if let Err(error) = resume_subscriber(&mut subscriber_client, &mut db_client) {
        error!(target: "subscriber", "Error restoring slot checkpoint {}", error);
        eprintln!("Fatal subscriber failure: {error}");
        return;
    }
    tokio::spawn(async move {
        db_client.run().await;
    });
    tokio::spawn(async move {
        handler.run().await;
    });
    let server = server::AggServer::run(handler_channel_receiver_server, opt.port_no);
    tokio::pin!(server);
    tokio::select! {
        result = subscriber_client.run() => {
            if let Err(error) = result {
                eprintln!("Fatal subscriber failure: {error}");
            }
        }
        result = &mut server => {
            if let Err(error) = result {
                error!(target:"server", "Error from server client {}", error);
                eprintln!("Fatal server failure: {error}");
            }
        }
    }
}
