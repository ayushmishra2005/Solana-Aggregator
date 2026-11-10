use crate::error::AggError;
use crate::util::{Block, ChunkReject, ProtocolMessage, QueryFailureKind, UnprocessedBlock};
use log::error;
use solana_clock::Slot;
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::mpsc::{Receiver, Sender};

const TERMINAL_BLOCK_LIMIT: usize = 1024;

struct PartialBlock {
    fetch_slot: u64,
    aggregation: UnprocessedBlock,
}

pub struct Handler {
    message_receiver: Receiver<ProtocolMessage>,
    db_sender: Sender<ProtocolMessage>,
    outcome_sender: Option<Sender<ProtocolMessage>>,
    unprocessed_block_collector: HashMap<Slot, PartialBlock>,
    fetch_slots: HashMap<Slot, u64>,
    terminal_blocks: HashSet<Slot>,
    terminal_order: VecDeque<Slot>,
}

impl Handler {
    /// This function initializes the handler
    ///
    /// # Arguments
    ///
    /// * `message_receiver` - A Receiver<ProtocolMessage> that holds the message receiver
    /// * `db_sender` - A Sender<ProtocolMessage> that holds the db sender
    ///
    /// # Returns
    ///
    /// * `Self` - The handler
    pub fn initialize(
        message_receiver: Receiver<ProtocolMessage>,
        db_sender: Sender<ProtocolMessage>,
    ) -> Self {
        Self {
            message_receiver,
            db_sender,
            outcome_sender: None,
            unprocessed_block_collector: HashMap::new(),
            fetch_slots: HashMap::new(),
            terminal_blocks: HashSet::new(),
            terminal_order: VecDeque::new(),
        }
    }

    pub fn set_block_outcomes(&mut self, sender: Sender<ProtocolMessage>) {
        self.outcome_sender = Some(sender);
    }

    /// This function runs the handler
    pub async fn run(&mut self) {
        loop {
            let Some(message) = self.message_receiver.recv().await else {
                log::info!(target: "handler", "Handler stopped");
                return;
            };
            match message {
                ProtocolMessage::ExpectBlock(fetch_slot, block_no, total_chunks) => {
                    self.fetch_slots.insert(block_no, fetch_slot);
                    self.note_expected_total(block_no, total_chunks);
                }
                ProtocolMessage::RecordSkippedSlot(fetch_slot) => {
                    if self
                        .db_sender
                        .send(ProtocolMessage::RecordSkippedSlot(fetch_slot))
                        .await
                        .is_err()
                    {
                        self.report(fetch_slot, Err("database channel closed".to_string()));
                    }
                }
                ProtocolMessage::StoreEmptyBlock(block_height, fetch_slot) => {
                    if self
                        .db_sender
                        .send(ProtocolMessage::FinalizeBlock(
                            block_height,
                            Block::default(),
                            fetch_slot,
                        ))
                        .await
                        .is_err()
                    {
                        self.report(fetch_slot, Err("database channel closed".to_string()));
                    }
                }
                ProtocolMessage::ChunkFailed(block_no, fetch_slot, error) => {
                    self.fail_block(block_no, fetch_slot, error);
                }
                ProtocolMessage::ParsedBlock(block_no, total_chunks, chunk_no, block) => {
                    if let Err(err) =
                        self.handle_unprocessed_block(block_no, total_chunks, chunk_no, block)
                    {
                        error!(target: "handler", "Error from handle_unprocessed_block {}", err);
                    }
                }
                ProtocolMessage::FetchTransactionDetails(tx_id, server_sender) => {
                    self.handle_tx_details(tx_id, server_sender).await;
                }
                ProtocolMessage::FetchBlockDetails(block_no, server_sender) => {
                    self.handle_block_details(block_no, server_sender).await;
                }
                ProtocolMessage::FetchLatestBlock(server_sender) => {
                    self.handle_latest_block_request(server_sender).await;
                }
                ProtocolMessage::FetchBlockRange(start, end, server_sender) => {
                    self.handle_block_range_request(start, end, server_sender)
                        .await;
                }
                ProtocolMessage::FetchAccountBalance(pubkey, block_no, server_sender) => {
                    self.handle_account_balance(pubkey, block_no, server_sender)
                        .await;
                }

                _ => {}
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
        if self.terminal_blocks.contains(&block_no) {
            return Ok(());
        }
        let fetch_slot = self.fetch_slots.get(&block_no).copied().unwrap_or(block_no);
        let mismatched_total = self
            .unprocessed_block_collector
            .get(&block_no)
            .is_some_and(|existing| existing.aggregation.total_chunk_count() != total_chunks);
        if mismatched_total {
            self.fail_block(block_no, fetch_slot, "inconsistent chunk count".to_string());
            return Err(AggError::InvalidChunk);
        }
        let stored_new_chunk = {
            let partial = self
                .unprocessed_block_collector
                .entry(block_no)
                .or_insert_with(|| PartialBlock {
                    fetch_slot,
                    aggregation: UnprocessedBlock::new(total_chunks),
                });
            partial.aggregation.insert_chunk(chunk_no, block)
        };
        match stored_new_chunk {
            Ok(true)
                if self
                    .unprocessed_block_collector
                    .get(&block_no)
                    .is_some_and(|partial| partial.aggregation.is_complete()) =>
            {
                let partial = self
                    .unprocessed_block_collector
                    .remove(&block_no)
                    .expect("completed block");
                let complete_block = partial.aggregation.complete_the_block();
                let fetch_slot = partial.fetch_slot;
                self.fetch_slots.remove(&block_no);
                self.remember_terminal(block_no);
                self.db_sender
                    .try_send(ProtocolMessage::FinalizeBlock(
                        block_no,
                        complete_block,
                        fetch_slot,
                    ))
                    .map_err(|_| AggError::OneshotChannelError)?;
            }
            Ok(_) => {}
            Err(ChunkReject::OutOfRange) => {
                self.fail_block(block_no, fetch_slot, "chunk index out of range".to_string());
                return Err(AggError::InvalidChunk);
            }
            Err(ChunkReject::Conflict) => {
                self.fail_block(block_no, fetch_slot, "conflicting chunk".to_string());
                return Err(AggError::ConflictingChunk);
            }
        }
        Ok(())
    }

    fn note_expected_total(&mut self, block_no: Slot, total_chunks: u64) {
        let mismatch = self
            .unprocessed_block_collector
            .get(&block_no)
            .map(|existing| {
                (
                    existing.fetch_slot,
                    existing.aggregation.total_chunk_count() != total_chunks,
                )
            });
        if let Some((fetch_slot, true)) = mismatch {
            self.fail_block(block_no, fetch_slot, "inconsistent chunk count".to_string());
        }
    }

    fn fail_block(&mut self, block_no: Slot, fetch_slot: u64, error: String) {
        self.unprocessed_block_collector.remove(&block_no);
        self.fetch_slots.remove(&block_no);
        if self.remember_terminal(block_no) {
            self.report(fetch_slot, Err(error));
        }
    }

    fn remember_terminal(&mut self, block_no: Slot) -> bool {
        if !self.terminal_blocks.insert(block_no) {
            return false;
        }
        self.terminal_order.push_back(block_no);
        while self.terminal_order.len() > TERMINAL_BLOCK_LIMIT {
            if let Some(old) = self.terminal_order.pop_front() {
                self.terminal_blocks.remove(&old);
            }
        }
        true
    }

    fn report(&self, fetch_slot: u64, result: Result<(), String>) {
        let Some(sender) = &self.outcome_sender else {
            return;
        };
        if sender
            .try_send(ProtocolMessage::BlockProcessingResult(fetch_slot, result))
            .is_err()
        {
            error!(target: "handler", "Block outcome receiver is closed");
        }
    }

    #[cfg(test)]
    pub fn has_block_state(&self, block_no: Slot) -> bool {
        self.unprocessed_block_collector.contains_key(&block_no)
    }

    /// This function handles the transaction details
    ///
    /// # Arguments
    ///
    /// * `tx_id` - A String that holds the transaction id
    /// * `server_sender` - A Sender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    pub async fn handle_tx_details(
        &mut self,
        tx_id: String,
        server_sender: Sender<ProtocolMessage>,
    ) {
        self.forward_to_database(ProtocolMessage::FetchTransactionDetails(
            tx_id,
            server_sender,
        ))
        .await;
    }

    /// This function handles the block details
    ///
    /// # Arguments
    ///
    /// * `block_no` - A String that holds the block number
    /// * `server_sender` - A Sender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    pub async fn handle_block_details(
        &mut self,
        block_no: String,
        server_sender: Sender<ProtocolMessage>,
    ) {
        self.forward_to_database(ProtocolMessage::FetchBlockDetails(block_no, server_sender))
            .await;
    }

    /// This function handles the account balance
    ///
    /// # Arguments
    ///
    /// * `pubkey` - A String that holds the public key
    /// * `block_no` - An Option<u64> that holds the block number
    /// * `server_sender` - A Sender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    pub async fn handle_account_balance(
        &mut self,
        pubkey: String,
        block_no: Option<u64>,
        server_sender: Sender<ProtocolMessage>,
    ) {
        self.forward_to_database(ProtocolMessage::FetchAccountBalance(
            pubkey,
            block_no,
            server_sender,
        ))
        .await;
    }

    /// This function handles the latest block request
    ///
    /// # Arguments
    ///
    /// * `server_sender` - A Sender<ProtocolMessage> that holds the server sender
    pub async fn handle_latest_block_request(&mut self, server_sender: Sender<ProtocolMessage>) {
        self.forward_to_database(ProtocolMessage::FetchLatestBlock(server_sender))
            .await;
    }

    /// This function handles the block range request
    ///
    /// # Arguments
    ///
    /// * `start` - A u64 that holds the start
    /// * `end` - A u64 that holds the end
    /// * `server_sender` - A Sender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    pub async fn handle_block_range_request(
        &mut self,
        start: u64,
        end: u64,
        server_sender: Sender<ProtocolMessage>,
    ) {
        self.forward_to_database(ProtocolMessage::FetchBlockRange(start, end, server_sender))
            .await;
    }

    async fn forward_to_database(&self, message: ProtocolMessage) {
        if let Err(send_error) = self.db_sender.send(message).await {
            if let Some(reply) = detach_query_reply(send_error.0) {
                let _ = reply
                    .send(ProtocolMessage::QueryError(
                        QueryFailureKind::Unavailable,
                        "Database worker is unavailable",
                    ))
                    .await;
            }
        }
    }
}

fn detach_query_reply(message: ProtocolMessage) -> Option<Sender<ProtocolMessage>> {
    match message {
        ProtocolMessage::FetchTransactionDetails(_, reply)
        | ProtocolMessage::FetchBlockDetails(_, reply)
        | ProtocolMessage::FetchLatestBlock(reply)
        | ProtocolMessage::FetchBlockRange(_, _, reply)
        | ProtocolMessage::FetchAccountBalance(_, _, reply) => Some(reply),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::Handler;
    use crate::util::{Block, ProtocolMessage};

    #[test]
    fn duplicate_chunk_finalizes_a_completed_block_only_once() {
        let (_incoming_sender, incoming) = crate::util::test_channel();
        let (db_sender, mut db_receiver) = crate::util::test_channel();
        let mut handler = Handler::initialize(incoming, db_sender);
        let chunk = Block::default();

        handler
            .handle_unprocessed_block(7, 1, 0, chunk.clone())
            .expect("first chunk");
        match db_receiver.try_recv() {
            Ok(ProtocolMessage::FinalizeBlock(slot, _, _)) => assert_eq!(slot, 7),
            other => panic!("expected one finalized block, got {other:?}"),
        }

        handler
            .handle_unprocessed_block(7, 1, 0, chunk)
            .expect("duplicate chunk");
        assert!(db_receiver.try_recv().is_err());
        assert!(!handler.has_block_state(7));
    }

    #[test]
    fn out_of_order_chunks_finalize_once_and_clear_state() {
        let (_incoming_sender, incoming) = crate::util::test_channel();
        let (db_sender, mut db_receiver) = crate::util::test_channel();
        let mut handler = Handler::initialize(incoming, db_sender);
        handler
            .handle_unprocessed_block(4, 2, 1, Block::default())
            .expect("second index first");
        assert!(db_receiver.try_recv().is_err());
        assert!(handler.has_block_state(4));
        handler
            .handle_unprocessed_block(4, 2, 0, Block::default())
            .expect("first index second");
        match db_receiver.try_recv() {
            Ok(ProtocolMessage::FinalizeBlock(slot, _, fetch_slot)) => {
                assert_eq!((slot, fetch_slot), (4, 4));
            }
            other => panic!("expected one finalized block, got {other:?}"),
        }
        assert!(db_receiver.try_recv().is_err());
        assert!(!handler.has_block_state(4));
        handler
            .handle_unprocessed_block(4, 2, 0, Block::default())
            .expect("late duplicate");
        assert!(db_receiver.try_recv().is_err());
    }

    #[test]
    fn conflicting_and_invalid_chunks_fail_without_finalizing() {
        let (_incoming_sender, incoming) = crate::util::test_channel();
        let (db_sender, mut db_receiver) = crate::util::test_channel();
        let (outcome_sender, mut outcome_receiver) = crate::util::test_channel();
        let mut handler = Handler::initialize(incoming, db_sender);
        handler.set_block_outcomes(outcome_sender);
        handler
            .handle_unprocessed_block(8, 2, 0, Block::default())
            .expect("chunk");
        let mut other = Block::default();
        other.insert_account("changed".to_string(), 1);
        let conflict = handler.handle_unprocessed_block(8, 2, 0, other);
        assert!(matches!(
            conflict,
            Err(crate::error::AggError::ConflictingChunk)
        ));
        assert!(matches!(
            outcome_receiver.try_recv(),
            Ok(ProtocolMessage::BlockProcessingResult(8, Err(_)))
        ));
        assert!(!handler.has_block_state(8));
        handler
            .handle_unprocessed_block(8, 2, 1, Block::default())
            .expect("ignored after failure");
        assert!(db_receiver.try_recv().is_err());

        let range = handler.handle_unprocessed_block(9, 1, 3, Block::default());
        assert!(matches!(range, Err(crate::error::AggError::InvalidChunk)));
        assert!(db_receiver.try_recv().is_err());

        handler
            .handle_unprocessed_block(10, 2, 0, Block::default())
            .expect("first total");
        let mismatch = handler.handle_unprocessed_block(10, 3, 1, Block::default());
        assert!(matches!(
            mismatch,
            Err(crate::error::AggError::InvalidChunk)
        ));
        assert!(db_receiver.try_recv().is_err());
        assert!(!handler.has_block_state(10));
    }

    #[test]
    fn incomplete_block_does_not_finalize() {
        let (_incoming_sender, incoming) = crate::util::test_channel();
        let (db_sender, mut db_receiver) = crate::util::test_channel();
        let mut handler = Handler::initialize(incoming, db_sender);
        handler
            .handle_unprocessed_block(3, 2, 0, Block::default())
            .expect("only chunk");
        assert!(db_receiver.try_recv().is_err());
        assert!(handler.has_block_state(3));
    }

    #[test]
    fn parser_chunk_failure_prevents_completion() {
        let (_incoming_sender, incoming) = crate::util::test_channel();
        let (db_sender, mut db_receiver) = crate::util::test_channel();
        let (outcome_sender, mut outcome_receiver) = crate::util::test_channel();
        let mut handler = Handler::initialize(incoming, db_sender);
        handler.set_block_outcomes(outcome_sender);
        handler
            .handle_unprocessed_block(11, 2, 0, Block::default())
            .expect("partial");
        handler.fail_block(11, 77, "parser failed".to_string());
        assert!(matches!(
            outcome_receiver.try_recv(),
            Ok(ProtocolMessage::BlockProcessingResult(77, Err(_)))
        ));
        handler
            .handle_unprocessed_block(11, 2, 1, Block::default())
            .expect("late chunk after failure");
        assert!(db_receiver.try_recv().is_err());
        assert!(!handler.has_block_state(11));
    }
}
