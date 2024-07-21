use crate::builder::Builder;
use crate::cli::Cli;
use crate::util::{Channel, ProtocolMessage};
use log::error;
use structopt::StructOpt;

mod block_importer;
mod builder;
mod cli;
mod db_handler;
mod error;
mod handler;
mod parser;
mod server;
mod util;

#[tokio::main]
async fn main() {
    let opt: Cli = Cli::from_args();
    let handler_channel = Channel::<ProtocolMessage>::new();
    let db_channel = Channel::<ProtocolMessage>::new();
    let handler_channel_receiver_server = handler_channel.sender();
    let mut subscriber_client = match Builder::default()
        .chain_url(opt.chain_url)
        .router_sender(handler_channel.sender())
        .build()
    {
        Ok(subscriber) => subscriber,
        Err(e) => {
            error!(target:"subscriber", "Error from subscriber client {}",e);
            return;
        }
    };
    let mut handler = Builder::default()
        .db_sender(db_channel.sender())
        .router_receiver(handler_channel.receiver)
        .build();
    let mut db_client = match Builder::default()
        .db_path(opt.db_path)
        .db_receiver(db_channel.receiver)
        .build()
    {
        Ok(db) => db,
        Err(e) => {
            error!(target:"db", "Error from db client {}",e);
            return;
        }
    };
    tokio::spawn(async move {
        db_client.run().await;
    });
    tokio::spawn(async move {
        handler.run().await;
    });
    tokio::spawn(async move {
        subscriber_client.run().await;
    });
    if let Err(error) = server::AggServer::run(handler_channel_receiver_server, opt.port_no).await {
        error!(target:"server", "Error from server client {}",error);
    }
}
