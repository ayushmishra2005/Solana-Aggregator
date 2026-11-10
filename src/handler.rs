use crate::error::AggError;
use crate::util::{Block, ChunkReject, ProtocolMessage, QueryFailureKind, UnprocessedBlock};
use log::error;
use solana_clock::Slot;
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc::{Receiver, Sender};

const TERMINAL_BLOCK_LIMIT: usize = 1024;

struct PartialBlock {
    fetch_slot: u64,
    parent_slot: u64,
    aggregation: UnprocessedBlock,
}

pub struct Handler {
    message_receiver: Receiver<ProtocolMessage>,
    db_sender: Sender<ProtocolMessage>,
    outcome_sender: Option<Sender<ProtocolMessage>>,
    unprocessed_block_collector: HashMap<Slot, PartialBlock>,
    fetch_slots: HashMap<Slot, u64>,
    parent_slots: HashMap<Slot, u64>,
    terminal_blocks: HashMap<Slot, u64>,
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
            parent_slots: HashMap::new(),
            terminal_blocks: HashMap::new(),
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
                ProtocolMessage::ExpectBlock(fetch_slot, block_no, total_chunks, parent_slot) => {
                    if let Some(&completed_slot) = self.terminal_blocks.get(&block_no) {
                        if completed_slot != fetch_slot {
                            self.report(
                                fetch_slot,
                                Err("block height was already finalized for a different slot"
                                    .to_string()),
                            )
                            .await;
                            continue;
                        }
                    }
                    self.fetch_slots.insert(block_no, fetch_slot);
                    if let Some(partial) = self.unprocessed_block_collector.get_mut(&block_no) {
                        partial.fetch_slot = fetch_slot;
                        partial.parent_slot = parent_slot;
                    } else {
                        self.parent_slots.insert(block_no, parent_slot);
                    }
                    self.note_expected_total(block_no, total_chunks).await;
                }
                ProtocolMessage::RecordSkippedSlot(fetch_slot) => {
                    if self
                        .db_sender
                        .send(ProtocolMessage::RecordSkippedSlot(fetch_slot))
                        .await
                        .is_err()
                    {
                        self.report(fetch_slot, Err("database channel closed".to_string()))
                            .await;
                    }
                }
                ProtocolMessage::StoreEmptyBlock(block_height, fetch_slot, parent_slot) => {
                    if self
                        .db_sender
                        .send(ProtocolMessage::FinalizeBlock(
                            block_height,
                            Block::default(),
                            fetch_slot,
                            parent_slot,
                        ))
                        .await
                        .is_err()
                    {
                        self.report(fetch_slot, Err("database channel closed".to_string()))
                            .await;
                    }
                }
                ProtocolMessage::ChunkFailed(block_no, fetch_slot, error) => {
                    self.fail_block(block_no, fetch_slot, error).await;
                }
                ProtocolMessage::ParsedBlock(block_no, total_chunks, chunk_no, block) => {
                    if let Err(err) = self
                        .handle_unprocessed_block(block_no, total_chunks, chunk_no, block)
                        .await
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
    pub async fn handle_unprocessed_block(
        &mut self,
        block_no: Slot,
        total_chunks: u64,
        chunk_no: u64,
        block: Block,
    ) -> Result<(), AggError> {
        if let Some(&completed_slot) = self.terminal_blocks.get(&block_no) {
            let incoming = self.fetch_slots.get(&block_no).copied();
            if let Some(slot) = incoming {
                if slot != completed_slot {
                    self.report(
                        slot,
                        Err("block height was already finalized for a different slot".to_string()),
                    )
                    .await;
                    return Err(AggError::ConflictingBlock);
                }
            }
            return Ok(());
        }
        let fetch_slot = self.fetch_slots.get(&block_no).copied().unwrap_or(block_no);
        let mismatched_total = self
            .unprocessed_block_collector
            .get(&block_no)
            .is_some_and(|existing| existing.aggregation.total_chunk_count() != total_chunks);
        if mismatched_total {
            self.fail_block(block_no, fetch_slot, "inconsistent chunk count".to_string())
                .await;
            return Err(AggError::InvalidChunk);
        }
        let stored_new_chunk = {
            if !self.unprocessed_block_collector.contains_key(&block_no) {
                let parent_slot = self.parent_slots.remove(&block_no).unwrap_or(0);
                self.unprocessed_block_collector.insert(
                    block_no,
                    PartialBlock {
                        fetch_slot,
                        parent_slot,
                        aggregation: UnprocessedBlock::new(total_chunks),
                    },
                );
            }
            self.unprocessed_block_collector
                .get_mut(&block_no)
                .expect("partial block")
                .aggregation
                .insert_chunk(chunk_no, block)
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
                let fetch_slot = partial.fetch_slot;
                let parent_slot = partial.parent_slot;
                let complete_block = match partial.aggregation.complete_the_block() {
                    Ok(block) => block,
                    Err(()) => {
                        self.fail_block(
                            block_no,
                            fetch_slot,
                            "duplicate transaction signature".to_string(),
                        )
                        .await;
                        return Err(AggError::ConflictingBlock);
                    }
                };
                if self
                    .db_sender
                    .send(ProtocolMessage::FinalizeBlock(
                        block_no,
                        complete_block,
                        fetch_slot,
                        parent_slot,
                    ))
                    .await
                    .is_err()
                {
                    self.fail_block(block_no, fetch_slot, "database channel closed".to_string())
                        .await;
                    return Err(AggError::OneshotChannelError);
                }
                self.fetch_slots.remove(&block_no);
                self.parent_slots.remove(&block_no);
                self.remember_terminal(block_no, fetch_slot);
            }
            Ok(_) => {}
            Err(ChunkReject::OutOfRange) => {
                self.fail_block(block_no, fetch_slot, "chunk index out of range".to_string())
                    .await;
                return Err(AggError::InvalidChunk);
            }
            Err(ChunkReject::Conflict) => {
                self.fail_block(block_no, fetch_slot, "conflicting chunk".to_string())
                    .await;
                return Err(AggError::ConflictingChunk);
            }
        }
        Ok(())
    }

    async fn note_expected_total(&mut self, block_no: Slot, total_chunks: u64) {
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
            self.fail_block(block_no, fetch_slot, "inconsistent chunk count".to_string())
                .await;
        }
    }

    async fn fail_block(&mut self, block_no: Slot, fetch_slot: u64, error: String) {
        self.unprocessed_block_collector.remove(&block_no);
        self.fetch_slots.remove(&block_no);
        self.parent_slots.remove(&block_no);
        if self.remember_terminal(block_no, fetch_slot) {
            self.report(fetch_slot, Err(error)).await;
        }
    }

    fn remember_terminal(&mut self, block_no: Slot, fetch_slot: u64) -> bool {
        if self.terminal_blocks.contains_key(&block_no) {
            return false;
        }
        self.terminal_blocks.insert(block_no, fetch_slot);
        self.terminal_order.push_back(block_no);
        while self.terminal_order.len() > TERMINAL_BLOCK_LIMIT {
            if let Some(old) = self.terminal_order.pop_front() {
                self.terminal_blocks.remove(&old);
            }
        }
        true
    }

    async fn report(&self, fetch_slot: u64, result: Result<(), String>) {
        let Some(sender) = &self.outcome_sender else {
            return;
        };
        if sender
            .send(ProtocolMessage::BlockProcessingResult(fetch_slot, result))
            .await
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

    fn drive<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(future)
    }

    #[test]
    fn duplicate_chunk_finalizes_a_completed_block_only_once() {
        let (_incoming_sender, incoming) = crate::util::test_channel();
        let (db_sender, mut db_receiver) = crate::util::test_channel();
        let mut handler = Handler::initialize(incoming, db_sender);
        let chunk = Block::default();

        drive(handler.handle_unprocessed_block(7, 1, 0, chunk.clone())).expect("first chunk");
        match db_receiver.try_recv() {
            Ok(ProtocolMessage::FinalizeBlock(slot, _, _, _)) => assert_eq!(slot, 7),
            other => panic!("expected one finalized block, got {other:?}"),
        }

        drive(handler.handle_unprocessed_block(7, 1, 0, chunk)).expect("duplicate chunk");
        assert!(db_receiver.try_recv().is_err());
        assert!(!handler.has_block_state(7));
    }

    #[test]
    fn out_of_order_chunks_finalize_once_and_clear_state() {
        let (_incoming_sender, incoming) = crate::util::test_channel();
        let (db_sender, mut db_receiver) = crate::util::test_channel();
        let mut handler = Handler::initialize(incoming, db_sender);
        drive(handler.handle_unprocessed_block(4, 2, 1, Block::default()))
            .expect("second index first");
        assert!(db_receiver.try_recv().is_err());
        assert!(handler.has_block_state(4));
        drive(handler.handle_unprocessed_block(4, 2, 0, Block::default()))
            .expect("first index second");
        match db_receiver.try_recv() {
            Ok(ProtocolMessage::FinalizeBlock(slot, _, fetch_slot, _)) => {
                assert_eq!((slot, fetch_slot), (4, 4));
            }
            other => panic!("expected one finalized block, got {other:?}"),
        }
        assert!(db_receiver.try_recv().is_err());
        assert!(!handler.has_block_state(4));
        drive(handler.handle_unprocessed_block(4, 2, 0, Block::default())).expect("late duplicate");
        assert!(db_receiver.try_recv().is_err());
    }

    #[test]
    fn conflicting_and_invalid_chunks_fail_without_finalizing() {
        let (_incoming_sender, incoming) = crate::util::test_channel();
        let (db_sender, mut db_receiver) = crate::util::test_channel();
        let (outcome_sender, mut outcome_receiver) = crate::util::test_channel();
        let mut handler = Handler::initialize(incoming, db_sender);
        handler.set_block_outcomes(outcome_sender);
        drive(handler.handle_unprocessed_block(8, 2, 0, Block::default())).expect("chunk");
        let mut other = Block::default();
        other.insert_account("changed".to_string(), 1);
        let conflict = drive(handler.handle_unprocessed_block(8, 2, 0, other));
        assert!(matches!(
            conflict,
            Err(crate::error::AggError::ConflictingChunk)
        ));
        assert!(matches!(
            outcome_receiver.try_recv(),
            Ok(ProtocolMessage::BlockProcessingResult(8, Err(_)))
        ));
        assert!(!handler.has_block_state(8));
        drive(handler.handle_unprocessed_block(8, 2, 1, Block::default()))
            .expect("ignored after failure");
        assert!(db_receiver.try_recv().is_err());

        let range = drive(handler.handle_unprocessed_block(9, 1, 3, Block::default()));
        assert!(matches!(range, Err(crate::error::AggError::InvalidChunk)));
        assert!(db_receiver.try_recv().is_err());

        drive(handler.handle_unprocessed_block(10, 2, 0, Block::default())).expect("first total");
        let mismatch = drive(handler.handle_unprocessed_block(10, 3, 1, Block::default()));
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
        drive(handler.handle_unprocessed_block(3, 2, 0, Block::default())).expect("only chunk");
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
        drive(handler.handle_unprocessed_block(11, 2, 0, Block::default())).expect("partial");
        drive(handler.fail_block(11, 77, "parser failed".to_string()));
        assert!(matches!(
            outcome_receiver.try_recv(),
            Ok(ProtocolMessage::BlockProcessingResult(77, Err(_)))
        ));
        drive(handler.handle_unprocessed_block(11, 2, 1, Block::default()))
            .expect("late chunk after failure");
        assert!(db_receiver.try_recv().is_err());
        assert!(!handler.has_block_state(11));
    }

    #[test]
    fn full_database_queue_persists_the_completed_block_once() {
        drive(async {
            let directory = std::env::temp_dir().join(format!(
                "solana-agg-queue-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("clock")
                    .as_nanos()
            ));
            std::fs::create_dir_all(&directory).expect("temp database");
            let (incoming_sender, incoming) = tokio::sync::mpsc::channel(4);
            drop(incoming_sender);
            let (db_sender, mut db_receiver) = tokio::sync::mpsc::channel(1);
            db_sender
                .try_send(ProtocolMessage::RecordSkippedSlot(0))
                .expect("fill the database queue");
            let mut handler = Handler::initialize(incoming, db_sender);
            let admission = tokio::spawn(async move {
                handler
                    .handle_unprocessed_block(7, 1, 0, Block::default())
                    .await
            });
            assert!(matches!(
                db_receiver.recv().await,
                Some(ProtocolMessage::RecordSkippedSlot(0))
            ));
            let ProtocolMessage::FinalizeBlock(height, block, fetch_slot, parent_slot) =
                db_receiver
                    .recv()
                    .await
                    .expect("completed block is retained")
            else {
                panic!("database queue delivered a different message");
            };
            assert_eq!((height, fetch_slot, parent_slot), (7, 7, 0));
            admission
                .await
                .expect("handler task")
                .expect("database accepted the block");

            let (_sender, db_in) = crate::util::test_channel();
            let mut database = crate::db_handler::RocksDb::initialize(
                directory.to_string_lossy().to_string(),
                db_in,
            )
            .expect("database");
            let (db_outcome, mut db_outcomes) = crate::util::test_channel();
            database.set_block_outcomes(db_outcome);
            database
                .establish_initial_checkpoint(6)
                .expect("checkpoint");
            database
                .persist_and_report(height, fetch_slot, block.clone())
                .expect("persist");
            database
                .persist_test_block(height, fetch_slot, block)
                .expect("identical block is stored once");
            assert_eq!(database.read_checkpoint().expect("checkpoint"), Some(7));
            assert!(matches!(
                db_outcomes.try_recv(),
                Ok(ProtocolMessage::BlockProcessingResult(7, Ok(())))
            ));
            drop(database);
            let stored = rocksdb::DB::open_default(&directory).expect("reopen");
            assert!(stored.get("BlockNo7").expect("read").is_some());
            let _ = std::fs::remove_dir_all(directory);
        });
    }

    #[test]
    fn closed_database_channel_fails_the_slot_without_waiting() {
        let (finished, message) = drive(async {
            let (incoming_sender, incoming) = tokio::sync::mpsc::channel(1);
            drop(incoming_sender);
            let (db_sender, db_receiver) = tokio::sync::mpsc::channel(1);
            drop(db_receiver);
            let mut handler = Handler::initialize(incoming, db_sender);
            let (outcome_sender, mut outcome_receiver) = tokio::sync::mpsc::channel(1);
            handler.set_block_outcomes(outcome_sender);
            let finished = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handler.handle_unprocessed_block(4, 1, 0, Block::default()),
            )
            .await;
            let message = outcome_receiver.try_recv();
            (finished, message)
        });
        let finished = finished.expect("closed database channel must not wait forever");
        assert!(finished.is_err());
        match message {
            Ok(ProtocolMessage::BlockProcessingResult(4, Err(error))) => {
                assert!(error.contains("database channel closed"), "{error}");
            }
            other => panic!("expected a terminal slot error, got {other:?}"),
        }
    }

    fn signature(byte: u8) -> solana_signature::Signature {
        solana_signature::Signature::from([byte; 64])
    }

    fn observed(byte: u8, account: &str, balance: u64) -> Block {
        let mut block = Block::default();
        block
            .push_transaction(signature(byte), crate::util::TxRecord::default())
            .expect("transaction");
        block.insert_account(account.to_string(), balance);
        block
    }

    #[test]
    fn duplicate_signatures_do_not_persist_a_regressed_balance() {
        drive(async {
            let directory = std::env::temp_dir().join(format!(
                "solana-agg-dup-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("clock")
                    .as_nanos()
            ));
            std::fs::create_dir_all(&directory).expect("temp database");
            let (incoming_sender, incoming) = tokio::sync::mpsc::channel(8);
            let (db_sender, db_receiver) = tokio::sync::mpsc::channel(4);
            let mut handler = Handler::initialize(incoming, db_sender);
            let (outcome_sender, mut outcome_receiver) = tokio::sync::mpsc::channel(4);
            handler.set_block_outcomes(outcome_sender);
            let handler_task = tokio::spawn(async move { handler.run().await });
            let mut database = crate::db_handler::RocksDb::initialize(
                directory.to_string_lossy().to_string(),
                db_receiver,
            )
            .expect("database");
            database
                .establish_initial_checkpoint(4)
                .expect("checkpoint");

            let mut first = observed(1, "shared", 10);
            first
                .push_transaction(signature(2), crate::util::TxRecord::default())
                .expect("second transaction");
            first.insert_account("shared".to_string(), 99);
            let repeated = observed(1, "shared", 10);
            incoming_sender
                .send(ProtocolMessage::ExpectBlock(5, 3, 2, 4))
                .await
                .expect("expect");
            incoming_sender
                .send(ProtocolMessage::ParsedBlock(3, 2, 0, first))
                .await
                .expect("chunk 0");
            incoming_sender
                .send(ProtocolMessage::ParsedBlock(3, 2, 1, repeated))
                .await
                .expect("chunk 1");
            let outcome =
                tokio::time::timeout(std::time::Duration::from_secs(2), outcome_receiver.recv())
                    .await
                    .expect("duplicate signature must not leave the subscriber waiting")
                    .expect("outcome");
            match outcome {
                ProtocolMessage::BlockProcessingResult(5, Err(message)) => {
                    assert!(message.contains("duplicate"), "{message}");
                }
                other => panic!("expected a duplicate-signature failure, got {other:?}"),
            }
            handler_task.abort();
            drop(database);
            let stored = rocksdb::DB::open_default(&directory).expect("reopen");
            assert_eq!(
                stored
                    .get("last_processed_slot")
                    .expect("checkpoint")
                    .expect("bytes"),
                serde_json::to_vec(&4_u64).expect("slot")
            );
            assert!(stored.get("BlockNo3").expect("block").is_none());
            let _ = std::fs::remove_dir_all(directory);
        });
    }

    #[test]
    fn different_slot_reusing_a_height_fails_promptly() {
        drive(async {
            let (incoming_sender, incoming) = tokio::sync::mpsc::channel(8);
            let (db_sender, mut db_receiver) = tokio::sync::mpsc::channel(4);
            let mut handler = Handler::initialize(incoming, db_sender);
            let (outcome_sender, mut outcome_receiver) = tokio::sync::mpsc::channel(4);
            handler.set_block_outcomes(outcome_sender);
            let handler_task = tokio::spawn(async move { handler.run().await });
            incoming_sender
                .send(ProtocolMessage::ExpectBlock(7, 7, 1, 6))
                .await
                .expect("first expectation");
            incoming_sender
                .send(ProtocolMessage::ParsedBlock(
                    7,
                    1,
                    0,
                    observed(3, "shared", 42),
                ))
                .await
                .expect("first chunk");
            let ProtocolMessage::FinalizeBlock(_, block, fetch_slot, _) =
                db_receiver.recv().await.expect("first block")
            else {
                panic!("expected the first block");
            };
            assert_eq!(fetch_slot, 7);
            assert_eq!(block.get_account_balance("shared"), Some(42));
            incoming_sender
                .send(ProtocolMessage::ParsedBlock(
                    7,
                    1,
                    0,
                    observed(3, "shared", 42),
                ))
                .await
                .expect("redelivery");
            incoming_sender
                .send(ProtocolMessage::ExpectBlock(8, 7, 1, 7))
                .await
                .expect("conflicting height");
            let outcome =
                tokio::time::timeout(std::time::Duration::from_secs(1), outcome_receiver.recv())
                    .await
                    .expect("conflicting height must not wait")
                    .expect("outcome");
            match outcome {
                ProtocolMessage::BlockProcessingResult(8, Err(message)) => {
                    assert!(message.contains("different slot"), "{message}");
                }
                other => panic!("expected a conflict for slot 8, got {other:?}"),
            }
            assert!(db_receiver.try_recv().is_err());
            handler_task.abort();
        });
    }
}
