use crate::error::AggError;
use crate::util::{Block, ProtocolMessage};
use log::error;
use serde_json::{from_slice, to_vec};
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

const LATEST_BLOCK_NO_KEY: &str = "lst_blk_no";
const STORAGE_FORMAT_KEY: &str = "solana_agg_storage_format";
const STORAGE_FORMAT_VERSION: &str = "canonical-signature-v1";

enum ExistingBlock {
    Absent,
    Same,
}

enum StoreFailure {
    Conflict,
    Database(rocksdb::Error),
    Json(serde_json::Error),
    Incompatible(String),
}

fn store_failure(failure: StoreFailure) -> AggError {
    match failure {
        StoreFailure::Conflict => AggError::ConflictingBlock,
        StoreFailure::Database(err) => AggError::DbError(err),
        StoreFailure::Json(err) => AggError::JsonError(err),
        StoreFailure::Incompatible(message) => AggError::IncompatibleStorage(message),
    }
}

pub struct RocksDb {
    db: rocksdb::DB,
    receiver: UnboundedReceiver<ProtocolMessage>,
    outcome_sender: Option<UnboundedSender<ProtocolMessage>>,
    temp_db: BTreeSet<u64>,
    fail_next_write: bool,
    fail_next_serialization: bool,
}

impl RocksDb {

    /// This function initializes the RocksDb client
    ///
    /// # Arguments
    ///
    /// * `path` - A string slice that holds the path to the database
    /// * `receiver` - A UnboundedReceiver<ProtocolMessage> that holds the receiver
    ///
    /// # Returns
    ///
    /// * `Result<Self, AggError>` - A Result that holds the RocksDb client or an error
    pub fn initialize(
        path: String,
        receiver: UnboundedReceiver<ProtocolMessage>,
    ) -> Result<Self, AggError> {
        let db = rocksdb::DB::open_default(&path)?;
        let database = Self {
            db,
            receiver,
            outcome_sender: None,
            temp_db: Default::default(),
            fail_next_write: false,
            fail_next_serialization: false,
        };
        database
            .ensure_storage_format(&path)
            .map_err(store_failure)?;
        Ok(database)
    }

    pub fn set_block_outcomes(&mut self, sender: UnboundedSender<ProtocolMessage>) {
        self.outcome_sender = Some(sender);
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
    pub(crate) async fn run(&mut self) {
        loop {
            if let Some(message) = self.receiver.recv().await {
                match message {
                    ProtocolMessage::FinalizeBlock(block_no, block, fetch_slot) => {
                        println!(
                            "here block no {:?} {:?}",
                            block_no,
                            block.get_tx_hash().len()
                        );
                        let result = self.handle_block(block_no, block);
                        if let Err(err) = &result {
                            error!(target: "db", "Error from handle_block {}", err);
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
                        if let Err(error) =
                            self.handle_block_request(block_no, server_sender.clone())
                        {
                            Self::handle_error(server_sender, error);
                        }
                    }
                    ProtocolMessage::FetchLatestBlock(server_sender) => {
                        println!("Fetching latest block");
                        if let Err(error) = self.handle_latest_block_request(server_sender.clone())
                        {
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
                        if let Err(error) = self.handle_account_balance_request(
                            pubkey,
                            block_no,
                            server_sender.clone(),
                        ) {
                            Self::handle_error(server_sender, error);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// This function handles the account balance request
    ///
    /// # Arguments
    ///
    /// * `pubkey` - A string slice that holds the public key
    /// * `block_no` - An Option<u64> that holds the block number
    /// * `server_sender` - A UnboundedSender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    fn handle_account_balance_request(
        &self,
        pubkey: String,
        block_no: Option<u64>,
        server_sender: UnboundedSender<ProtocolMessage>,
    ) -> Result<(), AggError> {
        if let Some(block_no) = block_no {
            if let Some(block) = self.db.get(format!("BlockNo{}", block_no))? {
                let block = from_slice::<Block>(&block)?;
                let balance = block.get_account_balance(&pubkey);
                server_sender
                    .send(ProtocolMessage::AccountBalance(balance.unwrap_or_default()))
                    .map_err(|_| AggError::OneshotChannelError)?;
            }
        } else {
            if let Some(block_no) = self.get_latest_block() {
                if let Some(block) = self.db.get(format!("BlockNo{}", block_no))? {
                    let block = from_slice::<Block>(&block)?;
                    let balance = block.get_account_balance(&pubkey);
                    server_sender
                        .send(ProtocolMessage::AccountBalance(balance.unwrap_or_default()))
                        .map_err(|_| AggError::OneshotChannelError)?;
                }
            }
        }
        Ok(())
    }

    /// This function handles the block range request
    ///
    /// # Arguments
    ///
    /// * `start` - A u64 that holds the start block number
    /// * `end` - A u64 that holds the end block number
    /// * `server_sender` - A UnboundedSender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    fn handle_block_range_request(
        &self,
        start: u64,
        end: u64,
        server_sender: UnboundedSender<ProtocolMessage>,
    ) -> Result<(), AggError> {
        let mut blocks = BTreeMap::new();
        for block_no in start..=end {
            if let Some(block) = self.db.get(format!("BlockNo{}", block_no))? {
                let block = from_slice::<Block>(&block)?;
                blocks.insert(block_no, block);
            }
        }
        server_sender
            .send(ProtocolMessage::BlockRangeDetails(blocks))
            .map_err(|_| AggError::OneshotChannelError)?;
        Ok(())
    }

    /// This function handles the latest block request
    ///
    /// # Arguments
    ///
    /// * `server_sender` - A UnboundedSender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    fn handle_latest_block_request(
        &self,
        server_sender: UnboundedSender<ProtocolMessage>,
    ) -> Result<(), AggError> {
        if let Some(block_no) = self.get_latest_block() {
            if let Some(block) = self.db.get(format!("BlockNo{}", block_no))? {
                let block = from_slice::<Block>(&block)?;
                server_sender
                    .send(ProtocolMessage::LatestBlockDetails(block_no, block.clone()))
                    .map_err(|_| AggError::OneshotChannelError)?;
            } else {
                return Err(AggError::BlockNotFound);
            }
        } else {
            return Err(AggError::NoBlockFinalised);
        }
        Ok(())
    }

    /// This function handles the block request
    ///
    /// # Arguments
    ///
    /// * `block_no` - A string slice that holds the block number
    /// * `server_sender` - A UnboundedSender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    fn handle_block_request(
        &self,
        block_no: String,
        server_sender: UnboundedSender<ProtocolMessage>,
    ) -> Result<(), AggError> {
        if let Some(block) = self.db.get(format!("BlockNo{}", block_no))? {
            let block = from_slice::<Block>(&block)?;
            server_sender
                .send(ProtocolMessage::BlockDetails(block.clone()))
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
    /// * `server_sender` - A UnboundedSender<ProtocolMessage> that holds the server sender
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    fn handle_tx_request(
        &self,
        tx_id: String,
        server_sender: UnboundedSender<ProtocolMessage>,
    ) -> Result<(), AggError> {
        if let Some(block_no) = self.db.get(to_vec(&tx_id).unwrap())? {
            let block_no = from_slice::<u64>(&block_no)?;
            if let Some(block) = self.db.get(format!("BlockNo{}", block_no))? {
                let block = from_slice::<Block>(&block)?;
                let tx = block.get_tx_details(&tx_id).ok_or(AggError::TxNotFound)?;
                server_sender
                    .send(ProtocolMessage::TxDetails(tx.clone()))
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
        if sender.send(message).is_err() {
            error!(target: "db", "Block outcome receiver is closed");
        }
    }

    fn handle_block(&mut self, block_no: u64, block: Block) -> Result<(), AggError> {
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
        match self
            .existing_block_status(block_no, &block)
            .map_err(store_failure)?
        {
            ExistingBlock::Same => return Ok(()),
            ExistingBlock::Absent => {}
        }
        self.reject_foreign_signature_indexes(block_no, &block)
            .map_err(store_failure)?;

        let mut latest = self.get_latest_block();
        let mut planned: BTreeMap<u64, Block> = BTreeMap::new();
        let mut buffered = BTreeSet::new();
        if Self::extends_latest(latest, block_no) {
            let snapshot = self.cumulative_block(&planned, latest, block);
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
            let Some(existing) = self.get_block(next) else {
                break;
            };
            let snapshot = self.cumulative_block(&planned, latest, existing);
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
        if self.get_latest_block() != latest {
            if let Some(latest_no) = latest {
                batch.put(
                    LATEST_BLOCK_NO_KEY,
                    to_vec(&latest_no)
                        .map_err(StoreFailure::Json)
                        .map_err(store_failure)?,
                );
            }
        }
        self.db
            .write_opt(batch, &Self::durable_write_options())
            .map_err(StoreFailure::Database)
            .map_err(store_failure)?;
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

    fn ensure_storage_format(&self, path: &str) -> Result<(), StoreFailure> {
        match self
            .db
            .get(STORAGE_FORMAT_KEY)
            .map_err(StoreFailure::Database)?
        {
            Some(bytes) => {
                let version = String::from_utf8(bytes).unwrap_or_default();
                if version != STORAGE_FORMAT_VERSION {
                    return Err(Self::incompatible_storage(path, &version));
                }
                Ok(())
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
        let Some(stored) = self.get_block(block_no) else {
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
    ) -> Block {
        let mut account_map = BTreeMap::new();
        if let Some(previous) = previous {
            if let Some(planned_block) = planned.get(&previous) {
                if let Some(accounts) = planned_block.get_account_map() {
                    account_map.clone_from(&accounts);
                }
            } else if let Some(stored) = self.get_block(previous) {
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
        block
    }

    /// This function gets the block
    ///
    /// # Arguments
    ///
    /// * `block_no` - A u64 that holds the block number
    ///
    /// # Returns
    ///
    /// * `Option<Block>` - An Option that holds the block
    fn get_block(&self, block_no: u64) -> Option<Block> {
        if let Ok(Some(block)) = self.db.get(format!("BlockNo{}", block_no)) {
            Some(from_slice::<Block>(&block).unwrap())
        } else {
            None
        }
    }

    /// This function gets the latest block
    ///
    /// # Returns
    ///
    /// * `Option<u64>` - An Option that holds the block number
    fn get_latest_block(&self) -> Option<u64> {
        if let Ok(Some(block_no)) = self.db.get(LATEST_BLOCK_NO_KEY) {
            Some(from_slice::<u64>(&block_no).unwrap())
        } else {
            None
        }
    }

    /// This function handles the error
    ///
    /// # Arguments
    ///
    /// * `server_sender` - A UnboundedSender<ProtocolMessage> that holds the server sender
    /// * `error` - An AggError that holds the error
    fn handle_error(server_sender: UnboundedSender<ProtocolMessage>, error: AggError) {
        if let Err(error) = server_sender.send(ProtocolMessage::Error(error.to_string())) {
            error!(target: "db", "Failed to send error message {:?}", error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RocksDb;
    use crate::error::AggError;
    use crate::parser::Parser;
    use crate::util::ProtocolMessage;
    use solana_sdk::hash::Hash;
    use solana_sdk::message::Message;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::{Keypair, Signer};
    use solana_sdk::system_instruction;
    use solana_sdk::transaction::{Transaction, TransactionVersion, VersionedTransaction};
    use solana_transaction_status::{
        EncodableWithMeta, EncodedTransaction, EncodedTransactionWithStatusMeta,
        TransactionStatusMeta, UiTransactionEncoding, UiTransactionStatusMeta,
    };
    use std::path::PathBuf;
    use tokio::sync::mpsc::unbounded_channel;

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
        let (sender, mut receiver) = unbounded_channel();
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
        let (_db_sender, db_receiver) = unbounded_channel();
        let mut database =
            RocksDb::initialize(directory.path.to_string_lossy().to_string(), db_receiver)
                .expect("open temporary database");
        database.handle_block(42, block).expect("persist block");

        let (reply_sender, mut reply_receiver) = unbounded_channel();
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
        let (sender, mut receiver) = unbounded_channel();
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
        let (sender, mut receiver) = unbounded_channel();
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
        let (_sender, receiver) = unbounded_channel();
        let mut db = RocksDb::initialize(dir.path.to_string_lossy().to_string(), receiver)
            .expect("database");
        let (outcome_sender, mut outcome_receiver) = unbounded_channel();
        db.set_block_outcomes(outcome_sender);
        db.inject_write_failure();
        let result = db.handle_block(4, crate::util::Block::default());
        db.report_block(15, result);
        match outcome_receiver.try_recv() {
            Ok(ProtocolMessage::BlockProcessingResult(15, Err(message))) => {
                assert!(message.contains("Block write failed"), "{message}");
            }
            other => panic!("expected a write failure for slot 15, got {other:?}"),
        }
        assert!(db.get_block(4).is_none());
    }

    #[test]
    fn database_write_success_reports_the_fetch_slot() {
        let dir = TempDir::new();
        let (_sender, receiver) = unbounded_channel();
        let mut db = RocksDb::initialize(dir.path.to_string_lossy().to_string(), receiver)
            .expect("database");
        let (outcome_sender, mut outcome_receiver) = unbounded_channel();
        db.set_block_outcomes(outcome_sender);
        let mut block = crate::util::Block::default();
        block.push_transaction(
            solana_sdk::signature::Signature::from([4; 64]),
            crate::util::TxRecord::default(),
        );
        let result = db.handle_block(4, block);
        db.report_block(15, result);
        assert!(matches!(
            outcome_receiver.try_recv(),
            Ok(ProtocolMessage::BlockProcessingResult(15, Ok(())))
        ));
        let signature = solana_sdk::signature::Signature::from([4; 64]).to_string();
        let (server_sender, mut server_receiver) = unbounded_channel();
        db.handle_tx_request(signature.clone(), server_sender)
            .expect("signature lookup");
        assert!(matches!(
            server_receiver.try_recv(),
            Ok(ProtocolMessage::TxDetails(_))
        ));
        assert!(db.get_block(4).is_some());
    }

    fn signature_key_count(db: &RocksDb) -> usize {
        db.db
            .iterator(rocksdb::IteratorMode::Start)
            .filter_map(|item| item.ok())
            .filter(|(key, _)| {
                let key = String::from_utf8_lossy(key);
                key != super::STORAGE_FORMAT_KEY
                    && key != super::LATEST_BLOCK_NO_KEY
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
            let (_sender, receiver) = unbounded_channel();
            let mut database = RocksDb::initialize(path.clone(), receiver).expect("open");
            let (outcome_sender, mut outcome_receiver) = unbounded_channel();
            database.set_block_outcomes(outcome_sender);
            let result = database.handle_block(42, block.clone());
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
                    .get_account_balance(&first.message.static_account_keys()[0].to_string()),
                Some(balance)
            );
        }
        let (_sender, receiver) = unbounded_channel();
        let database = RocksDb::initialize(path, receiver).expect("reopen");
        assert!(database.get_block(42).is_some());
        let (reply_sender, mut reply_receiver) = unbounded_channel();
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
        let (_sender, receiver) = unbounded_channel();
        let mut database =
            RocksDb::initialize(directory.path.to_string_lossy().to_string(), receiver)
                .expect("open");
        let mut block = crate::util::Block::default();
        block.push_transaction(
            solana_sdk::signature::Signature::from([7; 64]),
            crate::util::TxRecord::default(),
        );
        block.insert_account("payer".to_string(), 42);
        database
            .handle_block(3, block.clone())
            .expect("first write");
        let stored = database.get_block(3);
        database.handle_block(3, block).expect("identical repeat");
        assert_eq!(database.get_block(3), stored);
        let mut changed = crate::util::Block::default();
        changed.push_transaction(
            solana_sdk::signature::Signature::from([8; 64]),
            crate::util::TxRecord::default(),
        );
        let conflict = database.handle_block(3, changed);
        assert!(matches!(conflict, Err(AggError::ConflictingBlock)));
        assert_eq!(database.get_block(3), stored);
    }

    #[test]
    fn serialization_failure_leaves_the_database_unchanged() {
        let directory = TempDir::new();
        let (_sender, receiver) = unbounded_channel();
        let mut database =
            RocksDb::initialize(directory.path.to_string_lossy().to_string(), receiver)
                .expect("open");
        database.inject_serialization_failure();
        let error = database
            .handle_block(1, crate::util::Block::default())
            .expect_err("serialization");
        assert!(matches!(error, AggError::JsonError(_)));
        assert!(database.get_block(1).is_none());
        assert!(database.get_latest_block().is_none());
    }

    #[test]
    fn fresh_database_records_its_storage_format() {
        let directory = TempDir::new();
        let (_sender, receiver) = unbounded_channel();
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
        let (_sender, receiver) = unbounded_channel();
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
        let (_sender, receiver) = unbounded_channel();
        let mut database =
            RocksDb::initialize(directory.path.to_string_lossy().to_string(), receiver)
                .expect("open");
        database
            .handle_block(1, crate::util::Block::default())
            .expect("empty block");
        assert!(database.get_block(1).is_some());
        assert_eq!(database.get_latest_block(), Some(1));
        assert_eq!(signature_key_count(&database), 0);
    }
}
