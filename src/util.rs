use serde::{Deserialize, Serialize};
use solana_client::rpc_config::RpcBlockConfig;
use solana_program::pubkey::Pubkey;
use solana_sdk::signature::Signature;
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
    FinalizeBlock(SlotNo, Block, SlotNo),
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
    /// `fetch_slot`, `block_height`, `total_chunks`.
    ExpectBlock(SlotNo, SlotNo, TotalChunk),
    /// `block_height`, `fetch_slot`, sanitized error.
    ChunkFailed(SlotNo, SlotNo, String),
    /// `fetch_slot`, `Ok(())` only after the block is parsed and the writer accepts it.
    BlockProcessingResult(SlotNo, Result<(), String>),
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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum Instruction {
    Transfer(String, String, f64),
}

impl Instruction {
    pub fn transfer(from: Pubkey, to: Pubkey, amount: f64) -> Self {
        Instruction::Transfer(from.to_string(), to.to_string(), amount)
    }
}

#[derive(Default, Clone, Serialize, Deserialize, Debug, PartialEq)]
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

#[derive(Default, Clone, Serialize, Deserialize, Debug, PartialEq)]
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

    pub fn push_transaction(&mut self, signature: Signature, tx: TxRecord) {
        self.tx_map.insert(signature.to_string(), tx);
    }

    pub fn get_tx_hash(&self) -> Vec<String> {
        self.tx_map.keys().cloned().collect()
    }

    pub fn transaction_ids(&self) -> impl Iterator<Item = &String> {
        self.tx_map.keys()
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

#[derive(Debug, PartialEq, Eq)]
pub enum ChunkReject {
    OutOfRange,
    Conflict,
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

    pub fn total_chunk_count(&self) -> u64 {
        self.total_chunks
    }

    /// Stores a valid chunk index.
    ///
    /// An identical repeat is ignored. A different payload for the same index is rejected.
    pub fn insert_chunk(&mut self, chunk_no: ChunkNo, block: Block) -> Result<bool, ChunkReject> {
        if self.total_chunks == 0 || chunk_no >= self.total_chunks {
            return Err(ChunkReject::OutOfRange);
        }
        if let Some(existing) = self.collected_partial_blocks.get(&chunk_no) {
            if existing == &block {
                return Ok(false);
            }
            return Err(ChunkReject::Conflict);
        }
        self.collected_partial_blocks.insert(chunk_no, block);
        self.total_collected_chunks = self.collected_partial_blocks.len() as u64;
        Ok(true)
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

#[cfg(test)]
mod tests {
    use super::{Block, UnprocessedBlock};
    use solana_sdk::signature::Signature;

    fn tx_signature(chunk_no: u64) -> Signature {
        let mut bytes = [0u8; 64];
        bytes[..8].copy_from_slice(&chunk_no.to_le_bytes());
        Signature::from(bytes)
    }

    fn partial(chunk_no: u64, balance: u64) -> Block {
        let mut block = Block::default();
        block.push_transaction(tx_signature(chunk_no), super::TxRecord::default());
        block.insert_account(format!("chunk-{chunk_no}"), balance);
        block.insert_account("merged".to_string(), balance);
        block
    }

    fn insert_sequence(total_chunks: u64, chunks: &[(u64, u64)]) -> (UnprocessedBlock, Vec<bool>) {
        let mut block = UnprocessedBlock::new(total_chunks);
        let mut completed = Vec::with_capacity(chunks.len());
        for &(chunk_no, balance) in chunks {
            block
                .insert_chunk(chunk_no, partial(chunk_no, balance))
                .expect("identical chunk");
            completed.push(block.is_complete());
        }
        (block, completed)
    }

    #[test]
    fn duplicate_chunk_does_not_complete_block() {
        let (_block, completed) = insert_sequence(2, &[(0, 1), (0, 1)]);
        assert_eq!(completed, vec![false, false]);
    }

    #[test]
    fn distinct_chunks_complete_block() {
        let (block, completed) = insert_sequence(2, &[(0, 1), (1, 2)]);
        assert_eq!(completed, vec![false, true]);
        let merged = block.complete_the_block();
        assert!(merged
            .get_tx_details(&tx_signature(0).to_string())
            .is_some());
        assert!(merged
            .get_tx_details(&tx_signature(1).to_string())
            .is_some());
        assert_eq!(merged.get_account_balance("merged"), Some(2));
    }

    #[test]
    fn out_of_order_chunks_merge_in_chunk_index_order() {
        let (block, completed) = insert_sequence(2, &[(1, 2), (0, 1)]);
        assert_eq!(completed, vec![false, true]);
        let merged = block.complete_the_block();
        assert!(merged
            .get_tx_details(&tx_signature(0).to_string())
            .is_some());
        assert!(merged
            .get_tx_details(&tx_signature(1).to_string())
            .is_some());
        assert_eq!(merged.get_account_balance("chunk-0"), Some(1));
        assert_eq!(merged.get_account_balance("chunk-1"), Some(2));
        assert_eq!(merged.get_account_balance("merged"), Some(2));
    }

    #[test]
    fn block_completes_only_after_missing_chunk_arrives() {
        let (block, completed) = insert_sequence(2, &[(0, 1), (0, 1), (1, 2)]);
        assert_eq!(completed, vec![false, false, true]);
        let merged = block.complete_the_block();
        assert!(merged
            .get_tx_details(&tx_signature(0).to_string())
            .is_some());
        assert!(merged
            .get_tx_details(&tx_signature(1).to_string())
            .is_some());
    }

    #[test]
    fn repeated_chunk_leaves_three_chunk_block_incomplete() {
        let (_block, completed) = insert_sequence(3, &[(0, 1), (0, 1), (1, 2)]);
        assert_eq!(completed, vec![false, false, false]);
    }

    #[test]
    fn three_distinct_chunks_complete_block() {
        let (block, completed) = insert_sequence(3, &[(0, 1), (1, 2), (2, 3)]);
        assert_eq!(completed, vec![false, false, true]);
        let merged = block.complete_the_block();
        assert_eq!(merged.get_account_balance("chunk-0"), Some(1));
        assert_eq!(merged.get_account_balance("chunk-1"), Some(2));
        assert_eq!(merged.get_account_balance("chunk-2"), Some(3));
        assert_eq!(merged.get_account_balance("merged"), Some(3));
    }

    #[test]
    fn conflicting_duplicate_chunk_is_rejected() {
        let mut block = UnprocessedBlock::new(2);
        block.insert_chunk(0, partial(0, 1)).expect("first chunk");
        assert!(matches!(
            block.insert_chunk(0, partial(0, 9)),
            Err(super::ChunkReject::Conflict)
        ));
        assert!(!block.is_complete());
        assert_eq!(
            block.complete_the_block().get_account_balance("chunk-0"),
            Some(1)
        );
    }

    #[test]
    fn out_of_range_chunk_is_rejected() {
        let mut block = UnprocessedBlock::new(2);
        assert!(matches!(
            block.insert_chunk(2, partial(2, 1)),
            Err(super::ChunkReject::OutOfRange)
        ));
        assert!(!block.is_complete());
    }
}
