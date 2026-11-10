use crate::error::AggError;
use crate::util::{Block, ProtocolMessage, QueryFailureKind, MAX_API_BLOCK_RANGE};
use log::error;
use serde_json::{from_slice, to_vec};
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::mpsc::{Receiver, Sender};

const LATEST_BLOCK_NO_KEY: &str = "lst_blk_no";
const CHECKPOINT_KEY: &str = "last_processed_slot";
const STORAGE_FORMAT_KEY: &str = "solana_agg_storage_format";
const STORAGE_FORMAT_V1: &str = "canonical-signature-v1";
const STORAGE_FORMAT_VERSION: &str = "canonical-signature-v2";

enum ExistingBlock {
    Absent,
    Same,
}

#[derive(Debug)]
enum StoreFailure {
    Conflict,
    Database(rocksdb::Error),
    Json(serde_json::Error),
    Incompatible(String),
    InjectedWrite,
}

impl std::fmt::Display for StoreFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreFailure::Conflict => write!(formatter, "Stored block does not match this block"),
            StoreFailure::Database(err) => write!(formatter, "{err}"),
            StoreFailure::Json(err) => write!(formatter, "{err}"),
            StoreFailure::Incompatible(message) => write!(formatter, "{message}"),
            StoreFailure::InjectedWrite => write!(formatter, "Block write failed"),
        }
    }
}

fn store_failure(failure: StoreFailure) -> AggError {
    match failure {
        StoreFailure::Conflict => AggError::ConflictingBlock,
        StoreFailure::Database(err) => AggError::DbError(err),
        StoreFailure::Json(err) => AggError::JsonError(err),
        StoreFailure::Incompatible(message) => AggError::IncompatibleStorage(message),
        StoreFailure::InjectedWrite => AggError::WriteFailed,
    }
}

pub struct RocksDb {
    db: rocksdb::DB,
    receiver: Receiver<ProtocolMessage>,
    outcome_sender: Option<Sender<ProtocolMessage>>,
    temp_db: BTreeSet<u64>,
    fail_next_write: bool,
    fail_next_serialization: bool,
    runtime: Option<std::sync::Arc<crate::lifecycle::Lifecycle>>,
}

impl RocksDb {

    /// This function initializes the RocksDb client
    ///
    /// # Arguments
    ///
    /// * `path` - A string slice that holds the path to the database
    /// * `receiver` - A Receiver<ProtocolMessage> that holds the receiver
    ///
    /// # Returns
    ///
    /// * `Result<Self, AggError>` - A Result that holds the RocksDb client or an error
    pub fn initialize(path: String, receiver: Receiver<ProtocolMessage>) -> Result<Self, AggError> {
        let db = rocksdb::DB::open_default(&path)?;
        let mut database = Self {
            db,
            receiver,
            outcome_sender: None,
            temp_db: Default::default(),
            fail_next_write: false,
            fail_next_serialization: false,
            runtime: None,
        };
        database
            .ensure_storage_format(&path)
            .map_err(store_failure)?;
        database
            .restore_buffered_block_heights()
            .map_err(store_failure)?;
        Ok(database)
    }

    /// The highest slot that is a durable resume boundary.
    /// `None` means a fresh database has not chosen its start slot yet.
    pub fn read_checkpoint(&self) -> Result<Option<u64>, String> {
        self.read_checkpoint_value().map_err(|err| err.to_string())
    }

    /// Records the first-run slot boundary. Slots at or below it are not claimed as indexed.
    pub fn establish_initial_checkpoint(&mut self, slot: u64) -> Result<(), String> {
        self.write_initial_checkpoint(slot)
            .map_err(|err| err.to_string())
    }

    fn write_initial_checkpoint(&mut self, slot: u64) -> Result<(), StoreFailure> {
        if self.read_checkpoint_value()?.is_some() {
            return Err(StoreFailure::Incompatible(
                "last_processed_slot is already initialized".to_string(),
            ));
        }
        if self.indexed_records_present()? {
            return Err(StoreFailure::Incompatible(
                "RocksDB has indexed blocks but no last_processed_slot checkpoint. Use a fresh database directory or reindex explicitly. Existing records were not overwritten.".to_string(),
            ));
        }
        let mut batch = rocksdb::WriteBatch::default();
        batch.put(CHECKPOINT_KEY, to_vec(&slot).map_err(StoreFailure::Json)?);
        self.db
            .write_opt(batch, &Self::durable_write_options())
            .map_err(StoreFailure::Database)?;
        Ok(())
    }

    pub fn has_indexed_records(&self) -> Result<bool, String> {
        self.indexed_records_present()
            .map_err(|err| err.to_string())
    }

    pub fn set_block_outcomes(&mut self, sender: Sender<ProtocolMessage>) {
        self.outcome_sender = Some(sender);
    }

    pub fn attach_runtime(&mut self, runtime: std::sync::Arc<crate::lifecycle::Lifecycle>) {
        self.runtime = Some(runtime);
    }

    pub(crate) fn indexed_block_height(&self) -> Result<Option<u64>, &'static str> {
        match self.read_latest_height() {
            Ok(height) => Ok(height),
            Err(StoreFailure::Incompatible(_)) => Err("Corrupt latest block metadata"),
            Err(_) => Err("Latest block metadata is unreadable"),
        }
    }

    #[cfg(test)]
    pub fn inject_write_failure(&mut self) {
        self.fail_next_write = true;
    }

    #[cfg(test)]
    pub fn inject_serialization_failure(&mut self) {
        self.fail_next_serialization = true;
    }

    /// This function runs the RocksDb client
    pub(crate) async fn run(mut self) {
        loop {
            let Some(message) = self.receiver.recv().await else {
                log::info!(target: "db", "Database worker stopped");
                return;
            };
            self = match tokio::task::spawn_blocking(move || {
                self.handle_incoming(message);
                self
            })
            .await
            {
                Ok(database) => database,
                Err(error) => {
                    error!(target: "db", "Database worker stopped: {error}");
                    return;
                }
            };
        }
    }

    fn handle_incoming(&mut self, message: ProtocolMessage) {
        match message {
            ProtocolMessage::FinalizeBlock(block_no, block, fetch_slot) => {
                println!(
                    "here block no {:?} {:?}",
                    block_no,
                    block.get_tx_hash().len()
                );
                let result = self.handle_block(block_no, block, fetch_slot);
                if let Err(err) = &result {
                    error!(target: "db", "Error from handle_block {}", err);
                }
                self.report_block(fetch_slot, result);
            }
            ProtocolMessage::RecordSkippedSlot(fetch_slot) => {
                let result = self.record_skipped_slot(fetch_slot).map_err(store_failure);
                if let Err(err) = &result {
                    error!(target: "db", "Error recording skipped slot {}", err);
                }
                self.report_block(fetch_slot, result);
            }
            ProtocolMessage::FetchTransactionDetails(tx_id, server_sender) => {
                println!("Fetching tx details {:?}", tx_id);
                if let Err(error) = self.handle_tx_request(tx_id, server_sender.clone()) {
                    Self::handle_error(server_sender, error);
                }
            }
            ProtocolMessage::FetchBlockDetails(block_no, server_sender) => {
                println!("Fetching block details {:?}", block_no);
                if let Err(error) = self.handle_block_request(block_no, server_sender.clone()) {
                    Self::handle_error(server_sender, error);
                }
            }
            ProtocolMessage::FetchLatestBlock(server_sender) => {
                println!("Fetching latest block");
                if let Err(error) = self.handle_latest_block_request(server_sender.clone()) {
                    Self::handle_error(server_sender, error);
                }
            }
            ProtocolMessage::FetchBlockRange(start, end, server_sender) => {
                println!("Fetching block range");
                if let Err(error) =
                    self.handle_block_range_request(start, end, server_sender.clone())
                {
                    Self::handle_error(server_sender, error);
                }
            }
            ProtocolMessage::FetchAccountBalance(pubkey, block_no, server_sender) => {
                println!("Fetching account balance");
                if let Err(error) =
                    self.handle_account_balance_request(pubkey, block_no, server_sender.clone())
                {
                    Self::handle_error(server_sender, error);
                }
            }
            _ => {}
        }
    }

    /// This function handles the account balance request
    ///
    /// # Arguments
    ///
    /// * `pubkey` - A string slice that holds the public key
    /// * `block_no` - An Option<u64> that holds the block number
    /// * `server_sender` - A Sender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    fn handle_account_balance_request(
        &self,
        pubkey: String,
        block_no: Option<u64>,
        server_sender: Sender<ProtocolMessage>,
    ) -> Result<(), AggError> {
        let block_no = match block_no {
            Some(block_no) => block_no,
            None => self
                .read_latest_block_number()?
                .ok_or(AggError::NoBlockFinalised)?,
        };
        let Some(bytes) = self.db.get(format!("BlockNo{block_no}"))? else {
            return Err(AggError::BlockNotFound);
        };
        let block = from_slice::<Block>(&bytes)?;
        let Some(balance) = block.get_account_balance(&pubkey) else {
            return Err(AggError::AccountNotFound);
        };
        server_sender
            .try_send(ProtocolMessage::AccountBalance(balance))
            .map_err(|_| AggError::OneshotChannelError)?;
        Ok(())
    }

    /// This function handles the block range request
    ///
    /// # Arguments
    ///
    /// * `start` - A u64 that holds the start block number
    /// * `end` - A u64 that holds the end block number
    /// * `server_sender` - A Sender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    fn handle_block_range_request(
        &self,
        start: u64,
        end: u64,
        server_sender: Sender<ProtocolMessage>,
    ) -> Result<(), AggError> {
        let mut blocks = BTreeMap::new();
        for block_no in bounded_block_range(start, end)? {
            if let Some(block) = self.db.get(format!("BlockNo{block_no}"))? {
                let block = from_slice::<Block>(&block)?;
                blocks.insert(block_no, block);
            }
        }
        server_sender
            .try_send(ProtocolMessage::BlockRangeDetails(blocks))
            .map_err(|_| AggError::OneshotChannelError)?;
        Ok(())
    }

    /// This function handles the latest block request
    ///
    /// # Arguments
    ///
    /// * `server_sender` - A Sender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    fn handle_latest_block_request(
        &self,
        server_sender: Sender<ProtocolMessage>,
    ) -> Result<(), AggError> {
        let Some(block_no) = self.read_latest_block_number()? else {
            return Err(AggError::NoBlockFinalised);
        };
        let Some(bytes) = self.db.get(format!("BlockNo{block_no}"))? else {
            return Err(AggError::BlockNotFound);
        };
        let block = from_slice::<Block>(&bytes)?;
        server_sender
            .try_send(ProtocolMessage::LatestBlockDetails(block_no, block))
            .map_err(|_| AggError::OneshotChannelError)?;
        Ok(())
    }

    /// This function handles the block request
    ///
    /// # Arguments
    ///
    /// * `block_no` - A string slice that holds the block number
    /// * `server_sender` - A Sender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    fn handle_block_request(
        &self,
        block_no: String,
        server_sender: Sender<ProtocolMessage>,
    ) -> Result<(), AggError> {
        if let Some(block) = self.db.get(format!("BlockNo{}", block_no))? {
            let block = from_slice::<Block>(&block)?;
            server_sender
                .try_send(ProtocolMessage::BlockDetails(block.clone()))
                .map_err(|_| AggError::OneshotChannelError)?;
        } else {
            return Err(AggError::BlockNotFound);
        }
        Ok(())
    }

    /// This function handles the transaction request
    ///
    /// # Arguments
    ///
    /// * `tx_id` - A string slice that holds the transaction id
    /// * `server_sender` - A Sender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    fn handle_tx_request(
        &self,
        tx_id: String,
        server_sender: Sender<ProtocolMessage>,
    ) -> Result<(), AggError> {
        let signature_key = to_vec(&tx_id)?;
        if let Some(block_no) = self.db.get(signature_key)? {
            let block_no = from_slice::<u64>(&block_no)?;
            if let Some(block) = self.db.get(format!("BlockNo{}", block_no))? {
                let block = from_slice::<Block>(&block)?;
                let tx = block.get_tx_details(&tx_id).ok_or(AggError::TxNotFound)?;
                server_sender
                    .try_send(ProtocolMessage::TxDetails(tx.clone()))
                    .map_err(|_| AggError::OneshotChannelError)?;
            } else {
                return Err(AggError::BlockNotFound);
            }
        } else {
            return Err(AggError::TxNotFound);
        }
        Ok(())
    }

    /// This function handles the block
    ///
    /// # Arguments
    ///
    /// * `block_no` - A u64 that holds the block number
    /// * `block` - A Block that holds the block
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    fn report_block(&self, fetch_slot: u64, result: Result<(), AggError>) {
        let Some(sender) = &self.outcome_sender else {
            return;
        };
        let message = ProtocolMessage::BlockProcessingResult(
            fetch_slot,
            result.map_err(|err| err.to_string()),
        );
        if sender.try_send(message).is_err() {
            error!(target: "db", "Block outcome receiver is closed");
        }
    }

    fn handle_block(
        &mut self,
        block_no: u64,
        block: Block,
        fetch_slot: u64,
    ) -> Result<(), AggError> {
        if self.fail_next_write {
            self.fail_next_write = false;
            return Err(AggError::WriteFailed);
        }
        if self.fail_next_serialization {
            self.fail_next_serialization = false;
            return Err(AggError::JsonError(
                serde_json::from_str::<u64>("not-json").expect_err("invalid json"),
            ));
        }
        self.reject_foreign_signature_indexes(block_no, &block)
            .map_err(store_failure)?;
        let advance_checkpoint = self.checkpoint_action(fetch_slot).map_err(store_failure)?;
        match self
            .existing_block_status(block_no, &block)
            .map_err(store_failure)?
        {
            ExistingBlock::Same if !advance_checkpoint => return Ok(()),
            ExistingBlock::Absent if !advance_checkpoint => return Err(AggError::ConflictingBlock),
            ExistingBlock::Same | ExistingBlock::Absent => {}
        }

        let mut latest = self.get_latest_block()?;
        let mut planned: BTreeMap<u64, Block> = BTreeMap::new();
        let mut buffered = BTreeSet::new();
        if Self::extends_latest(latest, block_no) {
            let snapshot = self.cumulative_block(&planned, latest, block)?;
            latest = Some(block_no);
            planned.insert(block_no, snapshot);
        } else {
            buffered.insert(block_no);
            planned.insert(block_no, block);
        }
        buffered.extend(self.temp_db.iter().copied());
        let mut promoted = Vec::new();
        while let Some(latest_no) = latest {
            let next = latest_no.saturating_add(1);
            if !buffered.contains(&next) || planned.contains_key(&next) {
                break;
            }
            let Some(existing) = self.read_stored_block(next).map_err(store_failure)? else {
                break;
            };
            let snapshot = self.cumulative_block(&planned, latest, existing)?;
            buffered.remove(&next);
            promoted.push(next);
            planned.insert(next, snapshot);
            latest = Some(next);
        }

        let mut batch = rocksdb::WriteBatch::default();
        for (number, record) in &planned {
            batch.put(
                format!("BlockNo{number}"),
                to_vec(record)
                    .map_err(StoreFailure::Json)
                    .map_err(store_failure)?,
            );
            for signature in record.transaction_ids() {
                batch.put(
                    to_vec(signature)
                        .map_err(StoreFailure::Json)
                        .map_err(store_failure)?,
                    to_vec(number)
                        .map_err(StoreFailure::Json)
                        .map_err(store_failure)?,
                );
            }
        }
        if self.get_latest_block()? != latest {
            if let Some(latest_no) = latest {
                batch.put(
                    LATEST_BLOCK_NO_KEY,
                    to_vec(&latest_no)
                        .map_err(StoreFailure::Json)
                        .map_err(store_failure)?,
                );
            }
        }
        if advance_checkpoint {
            batch.put(
                CHECKPOINT_KEY,
                to_vec(&fetch_slot)
                    .map_err(StoreFailure::Json)
                    .map_err(store_failure)?,
            );
        }
        self.db
            .write_opt(batch, &Self::durable_write_options())
            .map_err(StoreFailure::Database)
            .map_err(store_failure)?;
        log::info!(
            target: "db",
            "Persisted block height {block_no} at slot {fetch_slot}"
        );
        self.publish_success(fetch_slot, true);
        for block_no in buffered {
            if planned.contains_key(&block_no) {
                self.temp_db.insert(block_no);
            }
        }
        for block_no in promoted {
            self.temp_db.remove(&block_no);
        }
        Ok(())
    }

    /// Sync the WAL before a block acknowledgment. The WAL stays enabled.
    /// `set_sync(true)` waits for the write to leave the OS page cache, so the
    /// acknowledgment is not returned while the batch exists only in memory.
    fn durable_write_options() -> rocksdb::WriteOptions {
        let mut options = rocksdb::WriteOptions::new();
        options.set_sync(true);
        options
    }

    fn record_skipped_slot(&mut self, fetch_slot: u64) -> Result<(), StoreFailure> {
        if self.fail_next_write {
            self.fail_next_write = false;
            return Err(StoreFailure::InjectedWrite);
        }
        if !self.checkpoint_action(fetch_slot)? {
            return Ok(());
        }
        let mut batch = rocksdb::WriteBatch::default();
        batch.put(
            CHECKPOINT_KEY,
            to_vec(&fetch_slot).map_err(StoreFailure::Json)?,
        );
        self.db
            .write_opt(batch, &Self::durable_write_options())
            .map_err(StoreFailure::Database)?;
        log::info!(
            target: "db",
            "Authoritative skipped slot {fetch_slot}; checkpoint advanced without a block"
        );
        self.publish_success(fetch_slot, false);
        Ok(())
    }

    fn publish_success(&self, fetch_slot: u64, update_height: bool) {
        let Some(runtime) = &self.runtime else {
            return;
        };
        runtime.note_checkpoint(fetch_slot);
        if update_height {
            if let Ok(Some(height)) = self.read_latest_height() {
                runtime.note_block_height(height);
            }
        }
        runtime.note_success();
    }

    fn checkpoint_action(&self, fetch_slot: u64) -> Result<bool, StoreFailure> {
        match self.read_checkpoint_value()? {
            None => Err(StoreFailure::Incompatible(
                "Refusing to persist slot progress before last_processed_slot is initialized. Use a fresh database or reindex explicitly.".to_string(),
            )),
            Some(current) if fetch_slot == current.saturating_add(1) => Ok(true),
            Some(current) if fetch_slot <= current => Ok(false),
            Some(current) => Err(StoreFailure::Incompatible(format!(
                "Refusing to move last_processed_slot from {current} to {fetch_slot}. Slot checkpoints advance one slot at a time."
            ))),
        }
    }

    fn read_checkpoint_value(&self) -> Result<Option<u64>, StoreFailure> {
        match self.db.get(CHECKPOINT_KEY).map_err(StoreFailure::Database)? {
            None => Ok(None),
            Some(bytes) => match from_slice::<u64>(&bytes) {
                Ok(slot) => Ok(Some(slot)),
                Err(_) => Err(StoreFailure::Incompatible(
                    "Corrupt last_processed_slot value. Use a fresh database or reindex explicitly. Existing records were not overwritten.".to_string(),
                )),
            },
        }
    }

    fn indexed_records_present(&self) -> Result<bool, StoreFailure> {
        for item in self.db.iterator(rocksdb::IteratorMode::Start) {
            let (key, _) = item.map_err(StoreFailure::Database)?;
            if key.as_ref() != STORAGE_FORMAT_KEY.as_bytes()
                && key.as_ref() != CHECKPOINT_KEY.as_bytes()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn restore_buffered_block_heights(&mut self) -> Result<(), StoreFailure> {
        let Some(latest) = self.read_latest_height()? else {
            return Ok(());
        };
        for item in self.db.iterator(rocksdb::IteratorMode::Start) {
            let (key, _) = item.map_err(StoreFailure::Database)?;
            let Ok(key) = std::str::from_utf8(&key) else {
                continue;
            };
            let Some(height) = key
                .strip_prefix("BlockNo")
                .and_then(|rest| rest.parse::<u64>().ok())
            else {
                continue;
            };
            if height > latest {
                self.temp_db.insert(height);
            }
        }
        Ok(())
    }

    fn ensure_storage_format(&self, path: &str) -> Result<(), StoreFailure> {
        match self
            .db
            .get(STORAGE_FORMAT_KEY)
            .map_err(StoreFailure::Database)?
        {
            Some(bytes) => {
                let version = String::from_utf8(bytes).unwrap_or_default();
                if version == STORAGE_FORMAT_VERSION {
                    return Ok(());
                }
                if version == STORAGE_FORMAT_V1 && !self.indexed_records_present()? {
                    let mut batch = rocksdb::WriteBatch::default();
                    batch.put(STORAGE_FORMAT_KEY, STORAGE_FORMAT_VERSION);
                    self.db
                        .write_opt(batch, &Self::durable_write_options())
                        .map_err(StoreFailure::Database)?;
                    return Ok(());
                }
                Err(Self::incompatible_storage(path, &version))
            }
            None if self.database_has_records()? => {
                Err(Self::incompatible_storage(path, "unversioned"))
            }
            None => {
                let mut batch = rocksdb::WriteBatch::default();
                batch.put(STORAGE_FORMAT_KEY, STORAGE_FORMAT_VERSION);
                self.db
                    .write_opt(batch, &Self::durable_write_options())
                    .map_err(StoreFailure::Database)?;
                Ok(())
            }
        }
    }

    fn incompatible_storage(path: &str, found: &str) -> StoreFailure {
        StoreFailure::Incompatible(format!(
            "Incompatible RocksDB at {path}: found {found}, expected {STORAGE_FORMAT_VERSION}. Use a fresh database directory or reindex explicitly. Existing records were not migrated or overwritten."
        ))
    }

    fn database_has_records(&self) -> Result<bool, StoreFailure> {
        match self.db.iterator(rocksdb::IteratorMode::Start).next() {
            Some(Ok(_)) => Ok(true),
            Some(Err(err)) => Err(StoreFailure::Database(err)),
            None => Ok(false),
        }
    }

    fn existing_block_status(
        &self,
        block_no: u64,
        incoming: &Block,
    ) -> Result<ExistingBlock, StoreFailure> {
        let Some(stored) = self.read_stored_block(block_no)? else {
            return Ok(ExistingBlock::Absent);
        };
        if Self::same_transactions(&stored, incoming) && Self::same_observations(&stored, incoming)
        {
            Ok(ExistingBlock::Same)
        } else {
            Err(StoreFailure::Conflict)
        }
    }

    fn same_transactions(stored: &Block, incoming: &Block) -> bool {
        let stored_ids: Vec<&String> = stored.transaction_ids().collect();
        let incoming_ids: Vec<&String> = incoming.transaction_ids().collect();
        if stored_ids.len() != incoming_ids.len() {
            return false;
        }
        incoming
            .transaction_ids()
            .all(|signature| stored.get_tx_details(signature) == incoming.get_tx_details(signature))
    }

    fn same_observations(stored: &Block, incoming: &Block) -> bool {
        let Some(incoming_accounts) = incoming.get_account_map() else {
            return true;
        };
        let Some(stored_accounts) = stored.get_account_map() else {
            return incoming_accounts.is_empty();
        };
        incoming_accounts
            .iter()
            .all(|(account, balance)| stored_accounts.get(account) == Some(balance))
    }

    fn reject_foreign_signature_indexes(
        &self,
        block_no: u64,
        block: &Block,
    ) -> Result<(), StoreFailure> {
        for signature in block.transaction_ids() {
            let Some(bytes) = self
                .db
                .get(to_vec(signature).map_err(StoreFailure::Json)?)
                .map_err(StoreFailure::Database)?
            else {
                continue;
            };
            let indexed_block = from_slice::<u64>(&bytes).map_err(StoreFailure::Json)?;
            if indexed_block != block_no {
                return Err(StoreFailure::Conflict);
            }
        }
        Ok(())
    }

    fn extends_latest(latest: Option<u64>, block_no: u64) -> bool {
        match latest {
            None => true,
            Some(latest) => block_no == latest.saturating_add(1),
        }
    }

    fn cumulative_block(
        &self,
        planned: &BTreeMap<u64, Block>,
        previous: Option<u64>,
        mut block: Block,
    ) -> Result<Block, AggError> {
        let mut account_map = BTreeMap::new();
        if let Some(previous) = previous {
            if let Some(planned_block) = planned.get(&previous) {
                if let Some(accounts) = planned_block.get_account_map() {
                    account_map.clone_from(&accounts);
                }
            } else if let Some(stored) = self.read_stored_block(previous).map_err(store_failure)? {
                if let Some(accounts) = stored.get_account_map() {
                    account_map.clone_from(&accounts);
                }
            }
        }
        if let Some(observed) = block.get_account_map() {
            for (account, balance) in observed {
                account_map.insert(account, balance);
            }
        }
        block.set_account_map(account_map);
        Ok(block)
    }

    /// Missing blocks are `Ok(None)`. Corrupt stored JSON is an error and is left untouched.
    fn read_stored_block(&self, block_no: u64) -> Result<Option<Block>, StoreFailure> {
        match self
            .db
            .get(format!("BlockNo{block_no}"))
            .map_err(StoreFailure::Database)?
        {
            None => Ok(None),
            Some(bytes) => match from_slice::<Block>(&bytes) {
                Ok(block) => Ok(Some(block)),
                Err(_) => Err(StoreFailure::Incompatible(
                    "Corrupt block record. Existing records were not overwritten.".to_string(),
                )),
            },
        }
    }

    #[cfg(test)]
    fn get_block(&self, block_no: u64) -> Result<Option<Block>, AggError> {
        self.read_stored_block(block_no).map_err(store_failure)
    }

    /// This function gets the latest block
    ///
    /// # Returns
    ///
    /// * `Option<u64>` - An Option that holds the block number
    fn read_latest_height(&self) -> Result<Option<u64>, StoreFailure> {
        match self
            .db
            .get(LATEST_BLOCK_NO_KEY)
            .map_err(StoreFailure::Database)?
        {
            None => Ok(None),
            Some(bytes) => match from_slice::<u64>(&bytes) {
                Ok(height) => Ok(Some(height)),
                Err(_) => Err(StoreFailure::Incompatible(
                    "Corrupt latest block metadata. Existing records were not overwritten."
                        .to_string(),
                )),
            },
        }
    }

    fn get_latest_block(&self) -> Result<Option<u64>, AggError> {
        self.read_latest_height().map_err(store_failure)
    }

    /// This function handles the error
    ///
    /// # Arguments
    ///
    /// * `server_sender` - A Sender<ProtocolMessage> that holds the server sender
    /// * `error` - An AggError that holds the error
    fn handle_error(server_sender: Sender<ProtocolMessage>, error: AggError) {
        error!(target: "db", "Query failed: {error}");
        let (kind, message) = classify_query_error(&error);
        if server_sender
            .try_send(ProtocolMessage::QueryError(kind, message))
            .is_err()
        {
            error!(target: "db", "Failed to send error message");
        }
    }

    fn read_latest_block_number(&self) -> Result<Option<u64>, AggError> {
        match self.db.get(LATEST_BLOCK_NO_KEY)? {
            None => Ok(None),
            Some(bytes) => Ok(Some(from_slice(&bytes)?)),
        }
    }

    #[cfg(test)]
    pub(crate) fn persist_test_block(
        &mut self,
        block_no: u64,
        fetch_slot: u64,
        block: Block,
    ) -> Result<(), AggError> {
        self.handle_block(block_no, block, fetch_slot)
    }

    #[cfg(test)]
    pub(crate) fn corrupt_block_record(&self, block_no: u64) -> Result<(), rocksdb::Error> {
        self.db.put(format!("BlockNo{block_no}"), b"not-json")
    }
}

fn bounded_block_range(start: u64, end: u64) -> Result<impl Iterator<Item = u64>, AggError> {
    if start > end {
        return Err(AggError::InvalidRequest);
    }
    let length = end
        .checked_sub(start)
        .and_then(|span| span.checked_add(1))
        .ok_or(AggError::InvalidRequest)?;
    if length > MAX_API_BLOCK_RANGE {
        return Err(AggError::InvalidRequest);
    }
    Ok(start..=end)
}

fn classify_query_error(error: &AggError) -> (QueryFailureKind, &'static str) {
    match error {
        AggError::BlockNotFound => (QueryFailureKind::NotFound, "Block not found"),
        AggError::TxNotFound => (QueryFailureKind::NotFound, "Transaction not found"),
        AggError::AccountNotFound => (QueryFailureKind::NotFound, "Account observation not found"),
        AggError::NoBlockFinalised => (QueryFailureKind::NotReady, "No block has been indexed yet"),
        AggError::InvalidRequest => (QueryFailureKind::BadRequest, "Invalid block range"),
        AggError::DbError(_) | AggError::JsonError(_) => {
            (QueryFailureKind::Storage, "Internal storage error")
        }
        _ => (QueryFailureKind::Storage, "Internal storage error"),
    }
}

#[cfg(test)]
mod tests {
    use super::RocksDb;
    use crate::error::AggError;
    use crate::parser::Parser;
    use crate::util::ProtocolMessage;
    use solana_hash::Hash;
    use solana_keypair::Keypair;
    use solana_message::Message;
    use solana_pubkey::Pubkey;
    use solana_signer::Signer;
    use solana_system_interface::instruction as system_instruction;
    use solana_transaction::versioned::{TransactionVersion, VersionedTransaction};
    use solana_transaction::Transaction;
    use solana_transaction_status::{
        EncodableWithMeta, EncodedTransaction, EncodedTransactionWithStatusMeta,
        TransactionStatusMeta, UiTransactionEncoding, UiTransactionStatusMeta,
    };
    use std::path::PathBuf;

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            static TEMP_SEQUENCE: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "solana-agg-signature-index-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system time")
                    .as_nanos(),
                TEMP_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).expect("create temp database directory");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(future)
    }

    fn status_meta(fee: u64, post_balances: Vec<u64>) -> UiTransactionStatusMeta {
        serde_json::from_value(serde_json::json!({
            "err": null,
            "status": {"Ok": null},
            "fee": fee,
            "preBalances": post_balances,
            "postBalances": post_balances,
        }))
        .expect("transaction metadata")
    }

    fn signed_transfer(
        lamports: u64,
        fee: u64,
    ) -> (VersionedTransaction, EncodedTransactionWithStatusMeta) {
        let payer = Keypair::new();
        let recipient = Pubkey::new_unique();
        let instruction = system_instruction::transfer(&payer.pubkey(), &recipient, lamports);
        let message = Message::new(&[instruction], Some(&payer.pubkey()));
        let mut transaction = Transaction::new_unsigned(message);
        transaction
            .try_sign(&[&payer], Hash::new_from_array([7; 32]))
            .expect("sign transfer");
        let versioned = VersionedTransaction::from(transaction);
        let meta = status_meta(fee, vec![1_000_000, lamports, 1]);
        let encoded = EncodedTransactionWithStatusMeta {
            transaction: versioned.encode_with_meta(
                UiTransactionEncoding::Base64,
                &TransactionStatusMeta::default(),
            ),
            meta: Some(meta),
            version: Some(TransactionVersion::LEGACY),
        };
        (versioned, encoded)
    }

    fn parse_chunk(transactions: Vec<EncodedTransactionWithStatusMeta>) -> crate::util::Block {
        let (sender, mut receiver) = crate::util::test_channel();
        block_on(Parser::invoke(ProtocolMessage::new_chuck(
            42,
            0,
            1,
            transactions,
            sender,
        )))
        .expect("parse signed transactions");
        match receiver.try_recv().expect("parsed block") {
            ProtocolMessage::ParsedBlock(_, _, _, block) => block,
            other => panic!("expected a parsed block, got {other:?}"),
        }
    }

    #[test]
    fn transactions_are_indexed_and_loaded_by_signature() {
        let (first, first_encoded) = signed_transfer(1_500, 5_000);
        let (second, second_encoded) = signed_transfer(2_500, 6_000);
        let first_signature = first.signatures[0].to_string();
        let second_signature = second.signatures[0].to_string();
        let first_message_hash = first.message.hash().to_string();
        let second_message_hash = second.message.hash().to_string();
        assert_ne!(first_signature, first_message_hash);
        assert_ne!(second_signature, second_message_hash);
        assert_ne!(first_signature, second_signature);

        let block = parse_chunk(vec![first_encoded, second_encoded]);
        assert!(block.get_tx_details(&first_message_hash).is_none());
        assert!(block.get_tx_details(&second_message_hash).is_none());
        let first_record = block
            .get_tx_details(&first_signature)
            .expect("first signature index")
            .clone();
        let second_record = block
            .get_tx_details(&second_signature)
            .expect("second signature index")
            .clone();

        let directory = TempDir::new();
        let (_db_sender, db_receiver) = crate::util::test_channel();
        let mut database =
            RocksDb::initialize(directory.path.to_string_lossy().to_string(), db_receiver)
                .expect("open temporary database");
        database
            .establish_initial_checkpoint(41)
            .expect("start boundary");
        database.handle_block(42, block, 42).expect("persist block");

        let (reply_sender, mut reply_receiver) = crate::util::test_channel();
        database
            .handle_tx_request(first_signature, reply_sender.clone())
            .expect("load first signature");
        database
            .handle_tx_request(second_signature, reply_sender.clone())
            .expect("load second signature");
        let loaded_first = match reply_receiver.try_recv().expect("first lookup") {
            ProtocolMessage::TxDetails(record) => record,
            other => panic!("expected transaction details, got {other:?}"),
        };
        let loaded_second = match reply_receiver.try_recv().expect("second lookup") {
            ProtocolMessage::TxDetails(record) => record,
            other => panic!("expected transaction details, got {other:?}"),
        };
        assert!(matches!(
            database.handle_tx_request(first_message_hash, reply_sender),
            Err(AggError::TxNotFound)
        ));

        let first_json = serde_json::to_value(&loaded_first).expect("json");
        let second_json = serde_json::to_value(&loaded_second).expect("json");
        assert_eq!(first_json, serde_json::to_value(&first_record).unwrap());
        assert_eq!(second_json, serde_json::to_value(&second_record).unwrap());
        for value in [&first_json, &second_json] {
            assert!(value.get("instruction").is_some());
            assert!(value
                .get("metadata")
                .and_then(|metadata| metadata.as_str())
                .is_some());
        }
        assert_ne!(first_json["metadata"], second_json["metadata"]);
        assert!(first_json["metadata"].as_str().unwrap().contains("5000"));
        assert!(second_json["metadata"].as_str().unwrap().contains("6000"));
    }

    #[test]
    fn missing_signature_is_rejected_without_a_fabricated_id() {
        let (mut transaction, mut encoded) = signed_transfer(10, 1);
        let message_hash = transaction.message.hash().to_string();
        transaction.signatures.clear();
        encoded.transaction = transaction.encode_with_meta(
            UiTransactionEncoding::Base64,
            &TransactionStatusMeta::default(),
        );
        let (sender, mut receiver) = crate::util::test_channel();
        let error = block_on(Parser::invoke(ProtocolMessage::new_chuck(
            7,
            0,
            1,
            vec![encoded],
            sender,
        )))
        .expect_err("missing signature");
        assert!(matches!(error, AggError::InvalidTransactionSignature));
        assert!(!error.to_string().contains(&message_hash));
        assert!(receiver.try_recv().is_err());

        let invalid = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Binary(
                "not-a-transaction".to_string(),
                solana_transaction_status::TransactionBinaryEncoding::Base64,
            ),
            meta: None,
            version: None,
        };
        let (sender, mut receiver) = crate::util::test_channel();
        let error = block_on(Parser::invoke(ProtocolMessage::new_chuck(
            8,
            0,
            1,
            vec![invalid],
            sender,
        )))
        .expect_err("invalid transaction");
        assert!(matches!(error, AggError::InvalidTransactionSignature));
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn database_write_failure_reports_the_fetch_slot() {
        let dir = TempDir::new();
        let (_sender, receiver) = crate::util::test_channel();
        let mut db = RocksDb::initialize(dir.path.to_string_lossy().to_string(), receiver)
            .expect("database");
        let (outcome_sender, mut outcome_receiver) = crate::util::test_channel();
        db.set_block_outcomes(outcome_sender);
        db.establish_initial_checkpoint(3).expect("start boundary");
        db.inject_write_failure();
        let result = db.handle_block(4, crate::util::Block::default(), 4);
        db.report_block(15, result);
        match outcome_receiver.try_recv() {
            Ok(ProtocolMessage::BlockProcessingResult(15, Err(message))) => {
                assert!(message.contains("Block write failed"), "{message}");
            }
            other => panic!("expected a write failure for slot 15, got {other:?}"),
        }
        assert!(db.get_block(4).unwrap().is_none());
        assert_eq!(db.read_checkpoint().expect("checkpoint"), Some(3));
    }

    #[test]
    fn database_write_success_reports_the_fetch_slot() {
        let dir = TempDir::new();
        let (_sender, receiver) = crate::util::test_channel();
        let mut db = RocksDb::initialize(dir.path.to_string_lossy().to_string(), receiver)
            .expect("database");
        let (outcome_sender, mut outcome_receiver) = crate::util::test_channel();
        db.set_block_outcomes(outcome_sender);
        let mut block = crate::util::Block::default();
        block.push_transaction(
            solana_signature::Signature::from([4; 64]),
            crate::util::TxRecord::default(),
        );
        db.establish_initial_checkpoint(3).expect("start boundary");
        let result = db.handle_block(4, block, 4);
        db.report_block(15, result);
        assert!(matches!(
            outcome_receiver.try_recv(),
            Ok(ProtocolMessage::BlockProcessingResult(15, Ok(())))
        ));
        let signature = solana_signature::Signature::from([4; 64]).to_string();
        let (server_sender, mut server_receiver) = crate::util::test_channel();
        db.handle_tx_request(signature.clone(), server_sender)
            .expect("signature lookup");
        assert!(matches!(
            server_receiver.try_recv(),
            Ok(ProtocolMessage::TxDetails(_))
        ));
        assert!(db.get_block(4).unwrap().is_some());
    }

    fn signature_key_count(db: &RocksDb) -> usize {
        db.db
            .iterator(rocksdb::IteratorMode::Start)
            .filter_map(|item| item.ok())
            .filter(|(key, _)| {
                let key = String::from_utf8_lossy(key);
                key != super::STORAGE_FORMAT_KEY
                    && key != super::LATEST_BLOCK_NO_KEY
                    && key != super::CHECKPOINT_KEY
                    && !key.starts_with("BlockNo")
            })
            .count()
    }

    #[test]
    fn atomic_block_write_is_readable_after_reopen() {
        let directory = TempDir::new();
        let path = directory.path.to_string_lossy().to_string();
        let (first, first_encoded) = signed_transfer(1_000, 5_000);
        let (second, second_encoded) = signed_transfer(2_000, 6_000);
        let first_signature = first.signatures[0].to_string();
        let second_signature = second.signatures[0].to_string();
        let block = parse_chunk(vec![first_encoded, second_encoded]);
        let balance = block
            .get_account_balance(&first.message.static_account_keys()[0].to_string())
            .expect("payer balance");
        {
            let (_sender, receiver) = crate::util::test_channel();
            let mut database = RocksDb::initialize(path.clone(), receiver).expect("open");
            let (outcome_sender, mut outcome_receiver) = crate::util::test_channel();
            database.set_block_outcomes(outcome_sender);
            database
                .establish_initial_checkpoint(8)
                .expect("start boundary");
            let result = database.handle_block(42, block.clone(), 9);
            database.report_block(9, result);
            assert!(matches!(
                outcome_receiver.try_recv(),
                Ok(ProtocolMessage::BlockProcessingResult(9, Ok(())))
            ));
            assert!(outcome_receiver.try_recv().is_err());
            assert_eq!(
                database
                    .get_block(42)
                    .expect("block")
                    .expect("stored block")
                    .get_account_balance(&first.message.static_account_keys()[0].to_string()),
                Some(balance)
            );
        }
        let (_sender, receiver) = crate::util::test_channel();
        let database = RocksDb::initialize(path, receiver).expect("reopen");
        assert!(database.get_block(42).unwrap().is_some());
        let (reply_sender, mut reply_receiver) = crate::util::test_channel();
        database
            .handle_tx_request(first_signature, reply_sender.clone())
            .expect("first signature");
        database
            .handle_tx_request(second_signature, reply_sender)
            .expect("second signature");
        assert!(matches!(
            reply_receiver.try_recv(),
            Ok(ProtocolMessage::TxDetails(_))
        ));
        assert!(matches!(
            reply_receiver.try_recv(),
            Ok(ProtocolMessage::TxDetails(_))
        ));
    }

    #[test]
    fn repeated_block_is_idempotent_and_a_conflict_writes_nothing() {
        let directory = TempDir::new();
        let (_sender, receiver) = crate::util::test_channel();
        let mut database =
            RocksDb::initialize(directory.path.to_string_lossy().to_string(), receiver)
                .expect("open");
        let mut block = crate::util::Block::default();
        block.push_transaction(
            solana_signature::Signature::from([7; 64]),
            crate::util::TxRecord::default(),
        );
        block.insert_account("payer".to_string(), 42);
        database
            .establish_initial_checkpoint(2)
            .expect("start boundary");
        database
            .handle_block(3, block.clone(), 3)
            .expect("first write");
        let stored = database.get_block(3).expect("read block");
        database
            .handle_block(3, block, 3)
            .expect("identical repeat");
        assert_eq!(database.get_block(3).expect("reread block"), stored);
        let mut changed = crate::util::Block::default();
        changed.push_transaction(
            solana_signature::Signature::from([8; 64]),
            crate::util::TxRecord::default(),
        );
        let conflict = database.handle_block(3, changed, 3);
        assert!(matches!(conflict, Err(AggError::ConflictingBlock)));
        assert_eq!(database.get_block(3).expect("unchanged block"), stored);
    }

    #[test]
    fn serialization_failure_leaves_the_database_unchanged() {
        let directory = TempDir::new();
        let (_sender, receiver) = crate::util::test_channel();
        let mut database =
            RocksDb::initialize(directory.path.to_string_lossy().to_string(), receiver)
                .expect("open");
        database
            .establish_initial_checkpoint(0)
            .expect("start boundary");
        database.inject_serialization_failure();
        let error = database
            .handle_block(1, crate::util::Block::default(), 1)
            .expect_err("serialization");
        assert!(matches!(error, AggError::JsonError(_)));
        assert!(database.get_block(1).unwrap().is_none());
        assert!(database.get_latest_block().unwrap().is_none());
        assert_eq!(database.read_checkpoint().expect("checkpoint"), Some(0));
    }

    #[test]
    fn fresh_database_records_its_storage_format() {
        let directory = TempDir::new();
        let (_sender, receiver) = crate::util::test_channel();
        let database = RocksDb::initialize(directory.path.to_string_lossy().to_string(), receiver)
            .expect("open");
        let marker = database
            .db
            .get(super::STORAGE_FORMAT_KEY)
            .expect("read marker")
            .expect("marker");
        assert_eq!(marker, super::STORAGE_FORMAT_VERSION.as_bytes());
    }

    #[test]
    fn unversioned_database_is_not_migrated() {
        let directory = TempDir::new();
        {
            let db = rocksdb::DB::open_default(&directory.path).expect("raw database");
            db.put(super::LATEST_BLOCK_NO_KEY, b"1")
                .expect("legacy key");
        }
        let (_sender, receiver) = crate::util::test_channel();
        let error =
            match RocksDb::initialize(directory.path.to_string_lossy().to_string(), receiver) {
                Ok(_) => panic!("legacy database was opened"),
                Err(error) => error,
            };
        let message = error.to_string();
        assert!(message.contains("fresh database"), "{message}");
        assert!(message.contains("reindex"), "{message}");
        let db = rocksdb::DB::open_default(&directory.path).expect("unchanged database");
        assert!(db.get(super::LATEST_BLOCK_NO_KEY).unwrap().is_some());
        assert!(db.get(super::STORAGE_FORMAT_KEY).unwrap().is_none());
    }

    #[test]
    fn empty_block_persists_no_transaction_indexes() {
        let directory = TempDir::new();
        let (_sender, receiver) = crate::util::test_channel();
        let mut database =
            RocksDb::initialize(directory.path.to_string_lossy().to_string(), receiver)
                .expect("open");
        database
            .establish_initial_checkpoint(0)
            .expect("start boundary");
        database
            .handle_block(1, crate::util::Block::default(), 1)
            .expect("empty block");
        assert!(database.get_block(1).unwrap().is_some());
        assert_eq!(database.get_latest_block().unwrap(), Some(1));
        assert_eq!(database.read_checkpoint().expect("checkpoint"), Some(1));
        assert_eq!(signature_key_count(&database), 0);
    }

    #[test]
    fn fresh_checkpoint_is_a_durable_start_boundary() {
        let directory = TempDir::new();
        let path = directory.path.to_string_lossy().to_string();
        let checkpoint = {
            let (_sender, receiver) = crate::util::test_channel();
            let mut database = RocksDb::initialize(path.clone(), receiver).expect("open");
            assert_eq!(database.read_checkpoint().expect("read"), None);
            database.establish_initial_checkpoint(80).expect("boundary");
            database.read_checkpoint().expect("read")
        };
        assert_eq!(checkpoint, Some(80));
        let (_sender, receiver) = crate::util::test_channel();
        let database = RocksDb::initialize(path, receiver).expect("reopen");
        assert_eq!(database.read_checkpoint().expect("read"), Some(80));
    }

    #[test]
    fn checkpoint_rejects_gaps_and_does_not_move_backward() {
        let directory = TempDir::new();
        let (_sender, receiver) = crate::util::test_channel();
        let mut database =
            RocksDb::initialize(directory.path.to_string_lossy().to_string(), receiver)
                .expect("open");
        database.establish_initial_checkpoint(10).expect("boundary");
        let gap = database.handle_block(1, crate::util::Block::default(), 12);
        assert!(matches!(gap, Err(AggError::IncompatibleStorage(_))));
        assert!(database.get_block(1).unwrap().is_none());
        assert_eq!(database.read_checkpoint().expect("checkpoint"), Some(10));
        database.record_skipped_slot(10).expect("already covered");
        database.record_skipped_slot(9).expect("older skip");
        assert_eq!(database.read_checkpoint().expect("checkpoint"), Some(10));
        let backwards = database.handle_block(2, crate::util::Block::default(), 8);
        assert!(matches!(backwards, Err(AggError::ConflictingBlock)));
        assert_eq!(database.read_checkpoint().expect("checkpoint"), Some(10));
    }

    #[test]
    fn skipped_slot_advances_only_the_slot_checkpoint() {
        let directory = TempDir::new();
        let path = directory.path.to_string_lossy().to_string();
        {
            let (_sender, receiver) = crate::util::test_channel();
            let mut database = RocksDb::initialize(path.clone(), receiver).expect("open");
            let (outcome_sender, mut outcome_receiver) = crate::util::test_channel();
            database.set_block_outcomes(outcome_sender);
            database.establish_initial_checkpoint(4).expect("boundary");
            let result = database
                .record_skipped_slot(5)
                .map_err(super::store_failure);
            database.report_block(5, result);
            assert!(matches!(
                outcome_receiver.try_recv(),
                Ok(ProtocolMessage::BlockProcessingResult(5, Ok(())))
            ));
            assert!(database.get_block(5).unwrap().is_none());
            assert_eq!(database.get_latest_block().unwrap(), None);
            assert_eq!(database.read_checkpoint().expect("checkpoint"), Some(5));
            database.inject_write_failure();
            let failed = database
                .record_skipped_slot(6)
                .map_err(super::store_failure);
            database.report_block(6, failed);
            assert!(matches!(
                outcome_receiver.try_recv(),
                Ok(ProtocolMessage::BlockProcessingResult(6, Err(_)))
            ));
            assert_eq!(database.read_checkpoint().expect("checkpoint"), Some(5));
        }
        let (_sender, receiver) = crate::util::test_channel();
        let database = RocksDb::initialize(path, receiver).expect("reopen");
        assert_eq!(database.read_checkpoint().expect("checkpoint"), Some(5));
    }

    #[test]
    fn slot_checkpoint_is_not_inferred_from_block_height() {
        let directory = TempDir::new();
        let (_sender, receiver) = crate::util::test_channel();
        let mut database =
            RocksDb::initialize(directory.path.to_string_lossy().to_string(), receiver)
                .expect("open");
        database.establish_initial_checkpoint(7).expect("boundary");
        database
            .handle_block(100, crate::util::Block::default(), 8)
            .expect("persist");
        assert_eq!(database.get_latest_block().unwrap(), Some(100));
        assert_eq!(database.read_checkpoint().expect("slot"), Some(8));
    }

    #[test]
    fn indexed_v1_database_without_a_checkpoint_is_rejected() {
        let directory = TempDir::new();
        {
            let db = rocksdb::DB::open_default(&directory.path).expect("raw database");
            db.put(
                super::STORAGE_FORMAT_KEY,
                super::STORAGE_FORMAT_V1.as_bytes(),
            )
            .expect("marker");
            db.put(super::LATEST_BLOCK_NO_KEY, b"1").expect("height");
        }
        let (_sender, receiver) = crate::util::test_channel();
        let error =
            match RocksDb::initialize(directory.path.to_string_lossy().to_string(), receiver) {
                Ok(_) => panic!("indexed v1 database was opened"),
                Err(error) => error,
            };
        assert!(error.to_string().contains("reindex"), "{error}");
        let db = rocksdb::DB::open_default(&directory.path).expect("unchanged");
        assert_eq!(
            db.get(super::STORAGE_FORMAT_KEY).unwrap().unwrap(),
            super::STORAGE_FORMAT_V1.as_bytes()
        );
        assert!(db.get(super::CHECKPOINT_KEY).unwrap().is_none());
    }

    #[test]
    fn empty_v1_database_upgrades_without_rewriting_records() {
        let directory = TempDir::new();
        {
            let db = rocksdb::DB::open_default(&directory.path).expect("raw database");
            db.put(
                super::STORAGE_FORMAT_KEY,
                super::STORAGE_FORMAT_V1.as_bytes(),
            )
            .expect("marker");
        }
        let (_sender, receiver) = crate::util::test_channel();
        let database = RocksDb::initialize(directory.path.to_string_lossy().to_string(), receiver)
            .expect("upgrade empty v1");
        assert_eq!(
            database.db.get(super::STORAGE_FORMAT_KEY).unwrap().unwrap(),
            super::STORAGE_FORMAT_VERSION.as_bytes()
        );
        assert!(database.read_checkpoint().expect("checkpoint").is_none());
        assert!(!database.has_indexed_records().expect("indexed"));
    }

    #[test]
    fn unsupported_storage_marker_is_rejected() {
        let directory = TempDir::new();
        {
            let db = rocksdb::DB::open_default(&directory.path).expect("raw database");
            db.put(super::STORAGE_FORMAT_KEY, b"future-format")
                .expect("marker");
        }
        let (_sender, receiver) = crate::util::test_channel();
        let error =
            match RocksDb::initialize(directory.path.to_string_lossy().to_string(), receiver) {
                Ok(_) => panic!("unsupported marker was accepted"),
                Err(error) => error,
            };
        assert!(error.to_string().contains("future-format"), "{error}");
        let db = rocksdb::DB::open_default(&directory.path).expect("unchanged");
        assert_eq!(
            db.get(super::STORAGE_FORMAT_KEY).unwrap().unwrap(),
            b"future-format"
        );
    }

    #[test]
    fn corrupt_checkpoint_is_rejected() {
        let directory = TempDir::new();
        let path = directory.path.to_string_lossy().to_string();
        {
            let (_sender, receiver) = crate::util::test_channel();
            let database = RocksDb::initialize(path.clone(), receiver).expect("open");
            database
                .db
                .put(super::CHECKPOINT_KEY, b"not-a-slot")
                .expect("corrupt checkpoint");
        }
        let (_sender, receiver) = crate::util::test_channel();
        let database = RocksDb::initialize(path, receiver).expect("reopen");
        let error = match database.read_checkpoint() {
            Ok(value) => panic!("corrupt checkpoint returned {value:?}"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("Corrupt last_processed_slot"),
            "{error}"
        );
    }

    #[test]
    fn out_of_order_block_heights_are_restored_after_reopen() {
        let directory = TempDir::new();
        let path = directory.path.to_string_lossy().to_string();
        {
            let (_sender, receiver) = crate::util::test_channel();
            let mut database = RocksDb::initialize(path.clone(), receiver).expect("open");
            database.establish_initial_checkpoint(0).expect("boundary");
            database
                .handle_block(1, crate::util::Block::default(), 1)
                .expect("height 1");
            database
                .handle_block(3, crate::util::Block::default(), 2)
                .expect("height 3");
            assert_eq!(database.get_latest_block().unwrap(), Some(1));
        }
        let (_sender, receiver) = crate::util::test_channel();
        let mut database = RocksDb::initialize(path, receiver).expect("reopen");
        assert!(database.temp_db.contains(&3));
        database
            .establish_initial_checkpoint(2)
            .expect_err("checkpoint exists");
        database
            .handle_block(2, crate::util::Block::default(), 3)
            .expect("fill the height gap");
        assert_eq!(database.get_latest_block().unwrap(), Some(3));
        assert!(!database.temp_db.contains(&3));
        assert_eq!(database.read_checkpoint().expect("slot"), Some(3));
    }

    #[test]
    fn committed_slot_survives_when_the_acknowledgment_is_lost() {
        let directory = TempDir::new();
        let path = directory.path.to_string_lossy().to_string();
        {
            let (_sender, receiver) = crate::util::test_channel();
            let mut database = RocksDb::initialize(path.clone(), receiver).expect("open");
            database.establish_initial_checkpoint(3).expect("boundary");
            database
                .handle_block(9, crate::util::Block::default(), 4)
                .expect("durable batch");
        }
        let (_sender, receiver) = crate::util::test_channel();
        let mut database = RocksDb::initialize(path, receiver).expect("reopen");
        assert_eq!(database.read_checkpoint().expect("checkpoint"), Some(4));
        assert!(database.get_block(9).unwrap().is_some());
        assert_eq!(database.get_latest_block().unwrap(), Some(9));
        let skipped = database.handle_block(10, crate::util::Block::default(), 6);
        assert!(matches!(skipped, Err(AggError::IncompatibleStorage(_))));
        assert_eq!(database.read_checkpoint().expect("checkpoint"), Some(4));
        assert!(database.get_block(10).unwrap().is_none());
    }

    #[test]
    fn versioned_records_without_a_checkpoint_are_not_assigned_one() {
        let directory = TempDir::new();
        {
            let db = rocksdb::DB::open_default(&directory.path).expect("raw database");
            db.put(
                super::STORAGE_FORMAT_KEY,
                super::STORAGE_FORMAT_VERSION.as_bytes(),
            )
            .expect("marker");
            db.put(super::LATEST_BLOCK_NO_KEY, b"1").expect("height");
        }
        let (_sender, receiver) = crate::util::test_channel();
        let mut database =
            RocksDb::initialize(directory.path.to_string_lossy().to_string(), receiver)
                .expect("open existing v2 data");
        assert_eq!(database.read_checkpoint().expect("checkpoint"), None);
        assert!(database.has_indexed_records().expect("indexed"));
        let error = database
            .establish_initial_checkpoint(99)
            .expect_err("must not invent a cursor");
        assert!(error.to_string().contains("reindex"), "{error}");
        assert!(database.db.get(super::CHECKPOINT_KEY).unwrap().is_none());
        assert!(database
            .db
            .get(super::LATEST_BLOCK_NO_KEY)
            .unwrap()
            .is_some());
    }

    #[test]
    fn corrupt_latest_block_metadata_is_a_controlled_error() {
        let directory = TempDir::new();
        let path = directory.path.to_string_lossy().to_string();
        {
            let (_sender, receiver) = crate::util::test_channel();
            let database = RocksDb::initialize(path.clone(), receiver).expect("open");
            database
                .db
                .put(super::LATEST_BLOCK_NO_KEY, b"not-json")
                .expect("corrupt height");
            let error = database.get_latest_block().expect_err("controlled error");
            let rendered = error.to_string();
            assert!(
                rendered.contains("Corrupt latest block metadata"),
                "{rendered}"
            );
            assert!(!rendered.contains(&path), "{rendered}");
            assert!(!rendered.contains("not-json"), "{rendered}");
        }
        let (_sender, receiver) = crate::util::test_channel();
        let Err(error) = RocksDb::initialize(path.clone(), receiver) else {
            panic!("reopen refuses corrupt metadata");
        };
        let rendered = error.to_string();
        assert!(
            rendered.contains("Corrupt latest block metadata"),
            "{rendered}"
        );
        assert!(!rendered.contains(&path), "{rendered}");
    }

    #[test]
    fn committed_block_survives_restart_and_a_failed_write_keeps_the_old_checkpoint() {
        let directory = TempDir::new();
        let path = directory.path.to_string_lossy().to_string();
        let (lifecycle, _shutdown) = crate::lifecycle::Lifecycle::new();
        {
            let (_sender, receiver) = crate::util::test_channel();
            let mut database = RocksDb::initialize(path.clone(), receiver).expect("open");
            database.attach_runtime(std::sync::Arc::clone(&lifecycle));
            database.establish_initial_checkpoint(10).expect("boundary");
            assert_eq!(lifecycle.snapshot().last_indexed_block_height, None);
            database
                .handle_block(4, crate::util::Block::default(), 11)
                .expect("persist");
            let status = lifecycle.snapshot();
            assert_eq!(status.last_processed_slot, Some(11));
            assert_eq!(status.last_indexed_block_height, Some(4));
            assert!(status.last_successful_processing_time.is_some());
            database.record_skipped_slot(12).expect("skip");
            let status = lifecycle.snapshot();
            assert_eq!(status.last_processed_slot, Some(12));
            assert_eq!(status.last_indexed_block_height, Some(4));
            database.inject_write_failure();
            let error = database
                .handle_block(5, crate::util::Block::default(), 13)
                .expect_err("write fails before the checkpoint moves");
            assert!(matches!(error, AggError::WriteFailed));
            assert_eq!(database.read_checkpoint().expect("checkpoint"), Some(12));
            assert_eq!(lifecycle.snapshot().last_processed_slot, Some(12));
            assert!(database.get_block(5).unwrap().is_none());
        }
        let (_sender, receiver) = crate::util::test_channel();
        let database = RocksDb::initialize(path, receiver).expect("reopen");
        assert_eq!(database.read_checkpoint().expect("checkpoint"), Some(12));
        assert_eq!(database.get_latest_block().unwrap(), Some(4));
        assert!(database.get_block(4).unwrap().is_some());
        assert!(database.get_block(5).unwrap().is_none());
    }

    #[test]
    fn corrupt_block_record_is_distinct_from_a_missing_block() {
        let directory = TempDir::new();
        let path = directory.path.to_string_lossy().to_string();
        let (_sender, receiver) = crate::util::test_channel();
        let mut database = RocksDb::initialize(path.clone(), receiver).expect("open");
        database.establish_initial_checkpoint(1).expect("boundary");
        database
            .handle_block(2, crate::util::Block::default(), 2)
            .expect("persist");
        database.corrupt_block_record(2).expect("corrupt");
        assert!(database.get_block(99).expect("missing read").is_none());
        let error = database.get_block(2).expect_err("corrupt read");
        let rendered = error.to_string();
        assert!(rendered.contains("Corrupt block record"), "{rendered}");
        assert!(!rendered.contains(&path), "{rendered}");
        assert!(!rendered.contains("not-json"), "{rendered}");
        let rejected = database
            .handle_block(3, crate::util::Block::default(), 3)
            .expect_err("do not acknowledge the next slot");
        assert!(matches!(rejected, AggError::IncompatibleStorage(_)));
        assert_eq!(database.read_checkpoint().expect("checkpoint"), Some(2));
        assert!(database.get_block(3).expect("next block").is_none());
        assert!(
            database
                .db
                .get("BlockNo2")
                .expect("raw read")
                .expect("corrupt bytes remain")
                .as_slice()
                == b"not-json"
        );
    }
}
