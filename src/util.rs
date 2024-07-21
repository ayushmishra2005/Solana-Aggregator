use serde::{Deserialize, Serialize};
use solana_client::rpc_config::RpcBlockConfig;
use solana_program::hash::Hash;
use solana_program::pubkey::Pubkey;
use solana_transaction_status::{EncodedTransactionWithStatusMeta, UiTransactionStatusMeta};
use std::collections::{BTreeMap, HashMap};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

type SlotNo = u64;
type ChunkNo = u64;
type TotalChunk = u64;

#[derive(Debug)]
pub enum ProtocolMessage {
    FetchBlock(String, RpcBlockConfig, SlotNo, UnboundedSender<Self>),
    NewChuck(
        SlotNo,
        ChunkNo,
        TotalChunk,
        Vec<EncodedTransactionWithStatusMeta>,
        UnboundedSender<Self>,
    ),
    ParsedBlock(SlotNo, TotalChunk, ChunkNo, Block),
    FinalizeBlock(SlotNo, Block),
    FetchTransactionDetails(String, UnboundedSender<Self>),
    TxDetails(TxRecord),
    FetchBlockDetails(String, UnboundedSender<Self>),
    FetchLatestBlock(UnboundedSender<Self>),
    LatestBlockDetails(u64, Block),
    BlockDetails(Block),
    FetchBlockRange(u64, u64, UnboundedSender<Self>),
    BlockRangeDetails(BTreeMap<u64, Block>),
    FetchAccountBalance(String, Option<u64>, UnboundedSender<Self>),
    AccountBalance(u64),
    Error(String),
}

impl ProtocolMessage {
    pub fn new_chuck(
        slot: SlotNo,
        chunk_no: ChunkNo,
        total_chunks: u64,
        txs: Vec<EncodedTransactionWithStatusMeta>,
        sender: UnboundedSender<Self>,
    ) -> Self {
        ProtocolMessage::NewChuck(slot, chunk_no, total_chunks, txs, sender)
    }

    pub fn fetch_block(
        client_url: String,
        rpc_block_config: RpcBlockConfig,
        slot: SlotNo,
        sender: UnboundedSender<ProtocolMessage>,
    ) -> Self {
        ProtocolMessage::FetchBlock(client_url, rpc_block_config, slot, sender)
    }

    pub fn parsed_block(slot: SlotNo, total_chunks: u64, chunk_no: u64, block: Block) -> Self {
        ProtocolMessage::ParsedBlock(slot, total_chunks, chunk_no, block)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Instruction {
    Transfer(String, String, f64),
}

impl Instruction {
    pub fn transfer(from: Pubkey, to: Pubkey, amount: f64) -> Self {
        Instruction::Transfer(from.to_string(), to.to_string(), amount)
    }
}

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
pub struct TxRecord {
    instruction: Vec<Instruction>,
    metadata: Option<String>,
}

impl TxRecord {
    pub fn new(instruction: Vec<Instruction>, metadata: Option<UiTransactionStatusMeta>) -> Self {
        let metadata = metadata.map(|meta| serde_json::to_string(&meta).unwrap());
        TxRecord {
            instruction,
            metadata,
        }
    }
}

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
pub struct Block {
    tx_map: HashMap<String, TxRecord>,
    account_map: Option<BTreeMap<String, u64>>,
}

impl Block {
    pub fn insert_account(&mut self, account: String, balance: u64) {
        if let Some(account_map) = &mut self.account_map {
            account_map.insert(account, balance);
        } else {
            let mut account_map = BTreeMap::new();
            account_map.insert(account, balance);
            self.account_map = Some(account_map);
        }
    }

    pub fn get_tx_details(&self, tx_hash: &str) -> Option<&TxRecord> {
        self.tx_map.get(tx_hash)
    }

    pub fn push_transaction(&mut self, tx_hash: Hash, tx: TxRecord) {
        self.tx_map.insert(tx_hash.to_string(), tx);
    }

    pub fn get_tx_hash(&self) -> Vec<String> {
        self.tx_map.keys().cloned().collect()
    }

    pub fn get_account_balance(&self, account: &str) -> Option<u64> {
        if let Some(account_map) = &self.account_map {
            account_map.get(account).cloned()
        } else {
            None
        }
    }

    pub fn get_account_map(&self) -> Option<BTreeMap<String, u64>> {
        self.account_map.clone()
    }

    pub fn set_account_map(&mut self, account_map: BTreeMap<String, u64>) {
        self.account_map = Some(account_map);
    }
}

#[derive(Default)]
pub struct UnprocessedBlock {
    total_chunks: u64,
    total_collected_chunks: u64,
    collected_partial_blocks: BTreeMap<ChunkNo, Block>,
}

impl UnprocessedBlock {
    pub fn new(total_chunks: u64) -> Self {
        UnprocessedBlock {
            total_chunks,
            total_collected_chunks: 0,
            collected_partial_blocks: BTreeMap::new(),
        }
    }

    pub fn is_complete(&self) -> bool {
        self.total_chunks == self.total_collected_chunks
    }

    pub fn insert_chunk(&mut self, chunk_no: ChunkNo, block: Block) {
        self.collected_partial_blocks.insert(chunk_no, block);
        self.total_collected_chunks += 1;
    }

    pub fn complete_the_block(&self) -> Block {
        let mut block = Block::default();
        for (_, partial_block) in self.collected_partial_blocks.iter() {
            block.tx_map.extend(partial_block.tx_map.clone());
            if let Some(account_map) = &partial_block.account_map {
                for (account, balance) in account_map.iter() {
                    block.insert_account(account.clone(), *balance);
                }
            }
        }
        block
    }
}

pub struct Channel<T> {
    sender: UnboundedSender<T>,
    pub receiver: UnboundedReceiver<T>,
}

impl<T> Channel<T> {
    pub fn new() -> Self {
        let (sender, receiver) = unbounded_channel::<T>();
        Channel { sender, receiver }
    }

    pub fn sender(&self) -> UnboundedSender<T> {
        self.sender.clone()
    }
}

#[derive(Deserialize)]
pub struct QueryParams {
    pub(crate) block_no: Option<u64>,
}
