use crate::error::AggError;
use crate::util::{Block, ProtocolMessage, UnprocessedBlock};
use log::error;
use solana_program::clock::Slot;
use std::collections::HashMap;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub struct Handler {
    message_receiver: UnboundedReceiver<ProtocolMessage>,
    db_sender: UnboundedSender<ProtocolMessage>,
    unprocessed_block_collector: HashMap<Slot, UnprocessedBlock>,
}

impl Handler {

    /// This function initializes the handler
    ///
    /// # Arguments
    ///
    /// * `message_receiver` - A UnboundedReceiver<ProtocolMessage> that holds the message receiver
    /// * `db_sender` - A UnboundedSender<ProtocolMessage> that holds the db sender
    ///
    /// # Returns
    ///
    /// * `Self` - The handler
    pub fn initialize(
        message_receiver: UnboundedReceiver<ProtocolMessage>,
        db_sender: UnboundedSender<ProtocolMessage>,
    ) -> Self {
        Self {
            message_receiver,
            db_sender,
            unprocessed_block_collector: HashMap::new(),
        }
    }

    /// This function runs the handler
    pub async fn run(&mut self) {
        loop {
            if let Some(message) = self.message_receiver.recv().await {
                match message {
                    ProtocolMessage::ParsedBlock(block_no, total_chunks, chunk_no, block) => {
                        if let Err(err) =
                            self.handle_unprocessed_block(block_no, total_chunks, chunk_no, block)
                        {
                            error!(target: "handler", "Error from handle_unprocessed_block {}", err);
                            return;
                        }
                    }
                    ProtocolMessage::FetchTransactionDetails(tx_id, server_sender) => {
                        self.handle_tx_details(tx_id, server_sender);
                    }
                    ProtocolMessage::FetchBlockDetails(block_no, server_sender) => {
                        self.handle_block_details(block_no, server_sender);
                    }
                    ProtocolMessage::FetchLatestBlock(server_sender) => {
                        self.handle_latest_block_request(server_sender);
                    }
                    ProtocolMessage::FetchBlockRange(start, end, server_sender) => {
                        self.handle_block_range_request(start, end, server_sender);
                    }
                    ProtocolMessage::FetchAccountBalance(pubkey, block_no, server_sender) => {
                        self.handle_account_balance(pubkey, block_no, server_sender);
                    }

                    _ => {}
                }
            }
        }
    }

    /// This function handles the unprocessed block
    ///
    /// # Arguments
    ///
    /// * `block_no` - A Slot that holds the block number
    /// * `total_chunks` - A u64 that holds the total chunks
    /// * `chunk_no` - A u64 that holds the chunk number
    /// * `block` - A Block that holds the block
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    pub fn handle_unprocessed_block(
        &mut self,
        block_no: Slot,
        total_chunks: u64,
        chunk_no: u64,
        block: Block,
    ) -> Result<(), AggError> {
        if let Some(unprocessed_block) = self.unprocessed_block_collector.get_mut(&block_no) {
            unprocessed_block.insert_chunk(chunk_no, block);
            if unprocessed_block.is_complete() {
                let complete_block = unprocessed_block.complete_the_block();
                self.unprocessed_block_collector.remove(&block_no);
                self.db_sender
                    .send(ProtocolMessage::FinalizeBlock(block_no, complete_block))?;
            }
        } else {
            let mut unprocessed_block = UnprocessedBlock::new(total_chunks);
            unprocessed_block.insert_chunk(chunk_no, block);
            if unprocessed_block.is_complete() {
                let complete_block = unprocessed_block.complete_the_block();
                self.unprocessed_block_collector.remove(&block_no);
                self.db_sender
                    .send(ProtocolMessage::FinalizeBlock(block_no, complete_block))?;
            }
            self.unprocessed_block_collector
                .insert(block_no, unprocessed_block);
        }
        Ok(())
    }

    /// This function handles the transaction details
    ///
    /// # Arguments
    ///
    /// * `tx_id` - A String that holds the transaction id
    /// * `server_sender` - A UnboundedSender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    pub fn handle_tx_details(
        &mut self,
        tx_id: String,
        server_sender: UnboundedSender<ProtocolMessage>,
    ) {
        if let Err(error) = self
            .db_sender
            .send(ProtocolMessage::FetchTransactionDetails(
                tx_id,
                server_sender,
            ))
        {
            error!(target: "handler", "Error from db_sender {}", error);
        }
    }

    /// This function handles the block details
    ///
    /// # Arguments
    ///
    /// * `block_no` - A String that holds the block number
    /// * `server_sender` - A UnboundedSender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    pub fn handle_block_details(
        &mut self,
        block_no: String,
        server_sender: UnboundedSender<ProtocolMessage>,
    ) {
        if let Err(err) = self
            .db_sender
            .send(ProtocolMessage::FetchBlockDetails(block_no, server_sender))
        {
            error!(target: "handler", "Error from db_sender {}", err);
        }
    }

    /// This function handles the account balance
    ///
    /// # Arguments
    ///
    /// * `pubkey` - A String that holds the public key
    /// * `block_no` - An Option<u64> that holds the block number
    /// * `server_sender` - A UnboundedSender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    pub fn handle_account_balance(
        &mut self,
        pubkey: String,
        block_no: Option<u64>,
        server_sender: UnboundedSender<ProtocolMessage>,
    ) {
        if let Err(err) = self.db_sender.send(ProtocolMessage::FetchAccountBalance(
            pubkey,
            block_no,
            server_sender,
        )) {
            error!(target: "handler", "Error from db_sender {}", err);
        }
    }

    /// This function handles the latest block request
    ///
    /// # Arguments
    ///
    /// * `server_sender` - A UnboundedSender<ProtocolMessage> that holds the server sender
    pub fn handle_latest_block_request(&mut self, server_sender: UnboundedSender<ProtocolMessage>) {
        if let Err(err) = self
            .db_sender
            .send(ProtocolMessage::FetchLatestBlock(server_sender))
        {
            error!(target: "handler", "Error from db_sender {}", err);
        }
    }

    /// This function handles the block range request
    ///
    /// # Arguments
    ///
    /// * `start` - A u64 that holds the start
    /// * `end` - A u64 that holds the end
    /// * `server_sender` - A UnboundedSender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    pub fn handle_block_range_request(
        &mut self,
        start: u64,
        end: u64,
        server_sender: UnboundedSender<ProtocolMessage>,
    ) {
        if let Err(err) =
            self.db_sender
                .send(ProtocolMessage::FetchBlockRange(start, end, server_sender))
        {
            error!(target: "handler", "Error from db_sender {}", err);
        }
    }
}
