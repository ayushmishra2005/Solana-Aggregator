use crate::error::AggError;
use crate::util::{Block, ProtocolMessage};
use log::{debug, error};
use serde_json::{from_slice, to_vec};
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

const LATEST_BLOCK_NO_KEY: &str = "lst_blk_no";

pub struct RocksDb {
    db: rocksdb::DB,
    receiver: UnboundedReceiver<ProtocolMessage>,
    temp_db: BTreeSet<u64>,
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
        Ok(Self {
            db,
            receiver,
            temp_db: Default::default(),
        })
    }

    /// This function runs the RocksDb client
    pub(crate) async fn run(&mut self) {
        loop {
            if let Some(message) = self.receiver.recv().await {
                match message {
                    ProtocolMessage::FinalizeBlock(block_no, block) => {
                        println!(
                            "here block no {:?} {:?}",
                            block_no,
                            block.get_tx_hash().len()
                        );
                        if let Err(err) = self.handle_block(block_no, block) {
                            error!(target: "db", "Error from handle_block {}", err);
                        }
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
    fn handle_block(&mut self, block_no: u64, block: Block) -> Result<(), AggError> {
        if let Some(latest_block) = self.get_latest_block() {
            debug!("Latest block no {:?}", latest_block);
            if block_no == latest_block.saturating_add(1) {
                debug!("Added to db {:?}", block_no);
                self.add_block(block_no, &block)?;
                self.update_latest_block_no_and_account_map(block_no)?;
            } else {
                self.temp_db.insert(block_no);
                self.add_block(block_no, &block)?;
            }
        } else {
            debug!("Updated latest block no first time{:?}", block_no);
            self.add_block(block_no, &block)?;
            self.update_latest_block_no_and_account_map(block_no)?;
        }
        self.add_transactions(block, block_no)?;
        if !self.temp_db.is_empty() {
            let mut block_to_removed = vec![];
            for block_no in self.temp_db.iter() {
                if block_no.saturating_sub(1)
                    == self.get_latest_block().ok_or(AggError::NoBlockFinalised)?
                {
                    self.update_latest_block_no_and_account_map(*block_no)?;
                    block_to_removed.push(*block_no);
                }
            }
            for block_no in block_to_removed {
                self.temp_db.remove(&block_no);
            }
        }
        Ok(())
    }

    /// This function adds the transactions
    ///
    /// # Arguments
    ///
    /// * `block` - A Block that holds the block
    /// * `block_no` - A u64 that holds the block number
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    fn add_transactions(&mut self, block: Block, block_no: u64) -> Result<(), AggError> {
        for tx in block.get_tx_hash() {
            self.db.put(to_vec(&tx)?, to_vec(&block_no).unwrap())?;
        }
        Ok(())
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

    /// This function adds the block
    ///
    /// # Arguments
    ///
    /// * `block_no` - A u64 that holds the block number
    /// * `block` - A Block that holds the block
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    fn add_block(&self, block_no: u64, block: &Block) -> Result<(), AggError> {
        self.db
            .put(format!("BlockNo{}", block_no), to_vec(block).unwrap())?;
        Ok(())
    }

    /// This function updates the latest block number and account map
    ///
    /// # Arguments
    ///
    /// * `block_no` - A u64 that holds the block number
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    fn update_latest_block_no_and_account_map(&self, block_no: u64) -> Result<(), AggError> {
        if let Some(mut latest_block) = self.get_block(block_no) {
            let mut account_map = BTreeMap::new();
            if let Some(last_block_no) = self.get_latest_block() {
                if let Some(last_block) = self.get_block(last_block_no) {
                    if let Some(last_account_map) = last_block.get_account_map() {
                        println!("Size of AccountMap {:?}", last_account_map.len());
                        account_map = last_account_map;
                    }
                }
            }
            if let Some(block_account_map) = latest_block.get_account_map() {
                for (account, balance) in block_account_map.iter() {
                    account_map.insert(account.to_string(), *balance);
                }
            }
            latest_block.set_account_map(account_map);
            self.add_block(block_no, &latest_block)?;
            self.db
                .put(LATEST_BLOCK_NO_KEY, to_vec(&block_no).unwrap())?;
        } else {
            return Err(AggError::BlockNotFound);
        }
        Ok(())
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
