use crate::error::AggError;
use crate::util::{Block, Instruction, ProtocolMessage, TxRecord};
use log::debug;
use solana_program::instruction::CompiledInstruction;
use solana_program::message::VersionedMessage;
use solana_program::pubkey::Pubkey;
use solana_program::system_program;
use solana_transaction_status::option_serializer::OptionSerializer;
use solana_transaction_status::UiTransactionStatusMeta;
use std::str::FromStr;

const SYSTEM_TRANSFER_DISCRIMINANT: u32 = 2;
const SYSTEM_TRANSFER_DATA_LEN: usize = 12;

enum AccountLookup {
    OutOfRange,
    Metadata,
}

pub struct Parser;

impl Parser {

    /// This function invokes the parser
    ///
    /// # Arguments
    ///
    /// * `message` - A ProtocolMessage that holds the message
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    pub async fn invoke(message: ProtocolMessage) -> Result<(), AggError> {
        if let ProtocolMessage::NewChuck(block_no, chunk_no, total_chunks, txs, sender) = message {
            let mut partial_block = Block::default();
            for (_, tx) in txs.iter().enumerate() {
                let mut instructions = vec![];
                let Some(transaction) = tx.transaction.decode() else {
                    return Err(AggError::InvalidTransactionSignature);
                };
                let Some(signature) = transaction.signatures.first().copied() else {
                    return Err(AggError::InvalidTransactionSignature);
                };
                let message = &transaction.message;
                let meta = tx.meta.as_ref();
                for (_, instruction) in message.instructions().iter().enumerate() {
                    if Self::is_transfer_instruction(message, instruction, meta)? {
                        instructions.push(Self::decode_transfer_instruction(
                            message,
                            instruction,
                            meta,
                        )?);
                    }
                }
                if let Some(meta) = tx.meta.clone() {
                    Self::record_observed_balances(&mut partial_block, message, &meta)
                        .map_err(|()| AggError::InvalidBalanceMetadata)?;
                }
                partial_block
                    .push_transaction(signature, TxRecord::new(instructions, tx.meta.clone()));
            }
            sender.send(ProtocolMessage::parsed_block(
                block_no,
                total_chunks,
                chunk_no,
                partial_block,
            ))?;
        };
        Ok(())
    }

    fn is_transfer_instruction(
        message: &VersionedMessage,
        instruction: &CompiledInstruction,
        meta: Option<&UiTransactionStatusMeta>,
    ) -> Result<bool, AggError> {
        if !Self::is_system_program(message, instruction, meta).map_err(Self::lookup_error)? {
            return Ok(false);
        }
        match Self::transfer_lamports(&instruction.data) {
            Ok(lamports) => Ok(lamports.is_some()),
            Err(()) => Err(AggError::InvalidTransferInstruction),
        }
    }

    fn decode_transfer_instruction(
        message: &VersionedMessage,
        instruction: &CompiledInstruction,
        meta: Option<&UiTransactionStatusMeta>,
    ) -> Result<Instruction, AggError> {
        if !Self::is_system_program(message, instruction, meta).map_err(Self::lookup_error)? {
            return Err(AggError::InvalidTransferInstruction);
        }
        let Some(lamports) = Self::transfer_lamports(&instruction.data)
            .map_err(|()| AggError::InvalidTransferInstruction)?
        else {
            return Err(AggError::InvalidTransferInstruction);
        };
        let from = Self::account_at(
            message,
            meta,
            Self::operand_index(instruction, 0).map_err(Self::lookup_error)?,
        )
        .map_err(Self::lookup_error)?;
        let to = Self::account_at(
            message,
            meta,
            Self::operand_index(instruction, 1).map_err(Self::lookup_error)?,
        )
        .map_err(Self::lookup_error)?;
        let amount = lamports as f64 / 1_000_000_000.0;

        debug!(
            "Transfer: {} SOL from {} to {}",
            amount,
            from.to_string(),
            to.to_string()
        );
        Ok(Instruction::transfer(from, to, amount))
    }

    fn is_system_program(
        message: &VersionedMessage,
        instruction: &CompiledInstruction,
        meta: Option<&UiTransactionStatusMeta>,
    ) -> Result<bool, AccountLookup> {
        let program_id = Self::account_at(message, meta, instruction.program_id_index as usize)?;
        Ok(program_id == system_program::id())
    }

    fn operand_index(
        instruction: &CompiledInstruction,
        operand: usize,
    ) -> Result<usize, AccountLookup> {
        instruction
            .accounts
            .get(operand)
            .copied()
            .map(usize::from)
            .ok_or(AccountLookup::OutOfRange)
    }

    fn account_at(
        message: &VersionedMessage,
        meta: Option<&UiTransactionStatusMeta>,
        index: usize,
    ) -> Result<Pubkey, AccountLookup> {
        let static_keys = message.static_account_keys();
        if let Some(key) = static_keys.get(index) {
            return Ok(*key);
        }
        let Some(meta) = meta else {
            return Err(AccountLookup::OutOfRange);
        };
        let loaded =
            Self::loaded_account_keys(message, meta).map_err(|()| AccountLookup::Metadata)?;
        loaded
            .get(index - static_keys.len())
            .copied()
            .ok_or(AccountLookup::OutOfRange)
    }

    fn lookup_error(error: AccountLookup) -> AggError {
        match error {
            AccountLookup::OutOfRange => AggError::InvalidTransferInstruction,
            AccountLookup::Metadata => AggError::InvalidBalanceMetadata,
        }
    }

    /// `Ok(None)` means the data is not a System `Transfer`.
    /// A complete `Transfer` discriminant with a malformed payload is an error.
    fn transfer_lamports(data: &[u8]) -> Result<Option<u64>, ()> {
        if data.len() < 4 {
            return Ok(None);
        }
        let mut discriminant_bytes = [0u8; 4];
        discriminant_bytes.copy_from_slice(&data[..4]);
        if u32::from_le_bytes(discriminant_bytes) != SYSTEM_TRANSFER_DISCRIMINANT {
            return Ok(None);
        }
        if data.len() != SYSTEM_TRANSFER_DATA_LEN {
            return Err(());
        }
        let mut lamports = [0u8; 8];
        lamports.copy_from_slice(&data[4..12]);
        Ok(Some(u64::from_le_bytes(lamports)))
    }

    /// Records a post-balance for every resolved account.
    /// Legacy accounts are the static keys. V0 accounts continue with loaded
    /// writable addresses and then loaded readonly addresses.
    fn record_observed_balances(
        block: &mut Block,
        message: &VersionedMessage,
        meta: &UiTransactionStatusMeta,
    ) -> Result<(), ()> {
        let mut accounts = message.static_account_keys().to_vec();
        accounts.extend(Self::loaded_account_keys(message, meta)?);
        if meta.post_balances.len() < accounts.len() {
            return Err(());
        }
        for (key, balance) in accounts.iter().zip(meta.post_balances.iter()) {
            block.insert_account(key.to_string(), *balance);
        }
        Ok(())
    }

    fn loaded_account_keys(
        message: &VersionedMessage,
        meta: &UiTransactionStatusMeta,
    ) -> Result<Vec<Pubkey>, ()> {
        let (writable_count, readonly_count) = message
            .address_table_lookups()
            .map(|lookups| {
                lookups
                    .iter()
                    .fold((0usize, 0usize), |(writable, readonly), lookup| {
                        (
                            writable.saturating_add(lookup.writable_indexes.len()),
                            readonly.saturating_add(lookup.readonly_indexes.len()),
                        )
                    })
            })
            .unwrap_or((0, 0));
        let loaded = match &meta.loaded_addresses {
            OptionSerializer::Some(addresses) => Some(addresses),
            OptionSerializer::None | OptionSerializer::Skip => None,
        };
        let Some(loaded) = loaded else {
            if writable_count == 0 && readonly_count == 0 {
                return Ok(Vec::new());
            }
            return Err(());
        };
        if loaded.writable.len() != writable_count || loaded.readonly.len() != readonly_count {
            return Err(());
        }
        let mut keys = Self::parse_loaded_pubkeys(&loaded.writable)?;
        keys.extend(Self::parse_loaded_pubkeys(&loaded.readonly)?);
        Ok(keys)
    }

    fn parse_loaded_pubkeys(keys: &[String]) -> Result<Vec<Pubkey>, ()> {
        keys.iter()
            .map(|key| Pubkey::from_str(key).map_err(|_| ()))
            .collect()
    }

}

#[cfg(test)]
mod tests {
    use super::Parser;
    use crate::error::AggError;
    use crate::util::{Block, Instruction, ProtocolMessage};
    use solana_program::hash::Hash;
    use solana_program::instruction::CompiledInstruction;
    use solana_program::message::v0::{self, MessageAddressTableLookup};
    use solana_program::message::{Message, MessageHeader, VersionedMessage};
    use solana_program::pubkey::Pubkey;
    use solana_program::system_instruction::{self, SystemInstruction};
    use solana_program::system_program;
    use solana_sdk::signature::{Keypair, Signature, Signer};
    use solana_sdk::transaction::{Transaction, TransactionVersion, VersionedTransaction};
    use solana_transaction_status::option_serializer::OptionSerializer;
    use solana_transaction_status::{
        EncodableWithMeta, EncodedTransactionWithStatusMeta, TransactionStatusMeta,
        UiLoadedAddresses, UiTransactionEncoding, UiTransactionStatusMeta,
    };
    use tokio::sync::mpsc::unbounded_channel;

    const LAMPORTS: u64 = 2_500_000_000;

    fn account(byte: u8) -> Pubkey {
        Pubkey::from([byte; 32])
    }

    fn message(keys: Vec<Pubkey>) -> VersionedMessage {
        VersionedMessage::Legacy(Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: keys,
            recent_blockhash: Hash::new_from_array([0; 32]),
            instructions: vec![],
        })
    }

    fn transfer_keys() -> (Pubkey, Pubkey, VersionedMessage) {
        let from = account(3);
        let to = account(4);
        let accounts = message(vec![from, to, system_program::id()]);
        (from, to, accounts)
    }

    fn transfer_instruction(accounts: Vec<u8>, data: Vec<u8>) -> CompiledInstruction {
        CompiledInstruction {
            program_id_index: 2,
            accounts,
            data,
        }
    }

    fn valid_transfer_instruction() -> CompiledInstruction {
        CompiledInstruction::new(
            2,
            &SystemInstruction::Transfer { lamports: LAMPORTS },
            vec![0, 1],
        )
    }

    fn assert_rejected(message: &VersionedMessage, instruction: &CompiledInstruction) {
        assert!(matches!(
            Parser::decode_transfer_instruction(message, instruction, None),
            Err(AggError::InvalidTransferInstruction)
        ));
    }

    #[test]
    fn valid_system_transfer_decodes() {
        let (from, to, message) = transfer_keys();
        let instruction = valid_transfer_instruction();
        assert_eq!(instruction.data.len(), 12);
        assert_eq!(&instruction.data[..4], &[2, 0, 0, 0]);
        assert!(Parser::is_transfer_instruction(&message, &instruction, None).unwrap());
        let decoded = Parser::decode_transfer_instruction(&message, &instruction, None).unwrap();
        let expected = Instruction::transfer(from, to, LAMPORTS as f64 / 1_000_000_000.0);
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
    }

    #[test]
    fn empty_instruction_data_is_not_a_transfer() {
        let (_from, _to, message) = transfer_keys();
        let instruction = transfer_instruction(vec![0, 1], vec![]);
        assert!(!Parser::is_transfer_instruction(&message, &instruction, None).unwrap());
        assert_rejected(&message, &instruction);
    }

    #[test]
    fn truncated_discriminant_is_not_a_transfer() {
        let (_from, _to, message) = transfer_keys();
        let instruction = transfer_instruction(vec![0, 1], vec![2, 0, 0]);
        assert!(!Parser::is_transfer_instruction(&message, &instruction, None).unwrap());
        assert_rejected(&message, &instruction);
    }

    #[test]
    fn truncated_transfer_payload_is_rejected() {
        let (_from, _to, message) = transfer_keys();
        let mut instruction = valid_transfer_instruction();
        instruction.data.truncate(7);
        assert!(matches!(
            Parser::is_transfer_instruction(&message, &instruction, None),
            Err(AggError::InvalidTransferInstruction)
        ));
        assert_rejected(&message, &instruction);
    }

    #[test]
    fn low_byte_two_with_different_discriminant_is_not_a_transfer() {
        let (_from, _to, message) = transfer_keys();
        let mut instruction = valid_transfer_instruction();
        instruction.data[1] = 1;
        assert_eq!(instruction.data[0], 2);
        assert!(!Parser::is_transfer_instruction(&message, &instruction, None).unwrap());
        assert_rejected(&message, &instruction);
    }

    #[test]
    fn missing_source_operand_is_rejected() {
        let (_from, _to, message) = transfer_keys();
        let mut instruction = valid_transfer_instruction();
        instruction.accounts.clear();
        assert_rejected(&message, &instruction);
    }

    #[test]
    fn missing_destination_operand_is_rejected() {
        let (_from, _to, message) = transfer_keys();
        let mut instruction = valid_transfer_instruction();
        instruction.accounts = vec![0];
        assert_rejected(&message, &instruction);
    }

    #[test]
    fn invalid_source_account_index_is_rejected() {
        let (_from, _to, message) = transfer_keys();
        let mut instruction = valid_transfer_instruction();
        instruction.accounts = vec![9, 1];
        assert_rejected(&message, &instruction);
    }

    #[test]
    fn invalid_destination_account_index_is_rejected() {
        let (_from, _to, message) = transfer_keys();
        let mut instruction = valid_transfer_instruction();
        instruction.accounts = vec![0, 9];
        assert_rejected(&message, &instruction);
    }

    #[test]
    fn valid_transfer_preserves_existing_decoded_output() {
        let (from, to, message) = transfer_keys();
        let decoded =
            Parser::decode_transfer_instruction(&message, &valid_transfer_instruction(), None)
                .unwrap();
        let value = serde_json::to_value(&decoded).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "Transfer": [
                    from.to_string(),
                    to.to_string(),
                    LAMPORTS as f64 / 1_000_000_000.0
                ]
            })
        );
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(future)
    }

    fn balances_meta(post_balances: Vec<u64>) -> UiTransactionStatusMeta {
        serde_json::from_value(serde_json::json!({
            "err": null,
            "status": {"Ok": null},
            "fee": 5000,
            "preBalances": post_balances,
            "postBalances": post_balances,
        }))
        .expect("balance metadata")
    }

    fn encoded_accounts(
        keys: Vec<Pubkey>,
        post_balances: Option<Vec<u64>>,
    ) -> EncodedTransactionWithStatusMeta {
        encoded_with_signature(keys, post_balances, 9)
    }

    fn encoded_with_signature(
        keys: Vec<Pubkey>,
        post_balances: Option<Vec<u64>>,
        signature_byte: u8,
    ) -> EncodedTransactionWithStatusMeta {
        let signatures = if keys.is_empty() {
            Vec::new()
        } else {
            vec![Signature::from([signature_byte; 64])]
        };
        let readonly_unsigned = keys.len().saturating_sub(1) as u8;
        let versioned = VersionedTransaction {
            signatures,
            message: VersionedMessage::Legacy(Message {
                header: MessageHeader {
                    num_required_signatures: u8::from(!keys.is_empty()),
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: readonly_unsigned,
                },
                account_keys: keys,
                recent_blockhash: Hash::new_from_array([1; 32]),
                instructions: vec![],
            }),
        };
        EncodedTransactionWithStatusMeta {
            transaction: versioned.encode_with_meta(
                UiTransactionEncoding::Base64,
                &TransactionStatusMeta::default(),
            ),
            meta: post_balances.map(balances_meta),
            version: Some(TransactionVersion::LEGACY),
        }
    }

    fn parse_accounts(transaction: EncodedTransactionWithStatusMeta) -> Block {
        let (sender, mut receiver) = unbounded_channel();
        block_on(Parser::invoke(ProtocolMessage::new_chuck(
            1,
            0,
            1,
            vec![transaction],
            sender,
        )))
        .expect("parse accounts");
        match receiver.try_recv() {
            Ok(ProtocolMessage::ParsedBlock(_, _, _, block)) => block,
            Ok(other) => panic!("expected a parsed block, got {other:?}"),
            Err(_) => panic!("parser succeeded without emitting a block"),
        }
    }

    fn assert_balance_error(transaction: EncodedTransactionWithStatusMeta) {
        let (sender, mut receiver) = unbounded_channel();
        let error = block_on(Parser::invoke(ProtocolMessage::new_chuck(
            1,
            0,
            1,
            vec![transaction],
            sender,
        )))
        .expect_err("malformed balance metadata");
        assert!(matches!(error, AggError::InvalidBalanceMetadata));
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn zero_static_account_keys_record_nothing() {
        let mut block = Block::default();
        let message = VersionedMessage::Legacy(Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![],
            recent_blockhash: Hash::new_from_array([0; 32]),
            instructions: vec![],
        });
        Parser::record_observed_balances(&mut block, &message, &balances_meta(vec![]))
            .expect("empty account list");
        assert!(block.get_account_map().is_none());
    }

    #[test]
    fn one_static_account_key_records_its_balance() {
        let key = account(8);
        let block = parse_accounts(encoded_accounts(vec![key], Some(vec![42])));
        assert_eq!(block.get_account_balance(&key.to_string()), Some(42));
        assert_eq!(block.get_account_map().unwrap().len(), 1);
    }

    #[test]
    fn two_static_account_keys_record_both_balances() {
        let first = account(8);
        let second = account(9);
        let block = parse_accounts(encoded_accounts(vec![first, second], Some(vec![11, 22])));
        assert_eq!(block.get_account_balance(&first.to_string()), Some(11));
        assert_eq!(block.get_account_balance(&second.to_string()), Some(22));
        assert_eq!(block.get_account_map().unwrap().len(), 2);
    }

    #[test]
    fn three_static_account_keys_record_every_balance() {
        let first = account(8);
        let second = account(9);
        let third = account(10);
        let block = parse_accounts(encoded_accounts(
            vec![first, second, third],
            Some(vec![11, 22, 33]),
        ));
        assert_eq!(block.get_account_balance(&first.to_string()), Some(11));
        assert_eq!(block.get_account_balance(&second.to_string()), Some(22));
        assert_eq!(block.get_account_balance(&third.to_string()), Some(33));
        assert_eq!(block.get_account_map().unwrap().len(), 3);
    }

    #[test]
    fn empty_post_balances_are_rejected() {
        assert_balance_error(encoded_accounts(vec![account(8), account(9)], Some(vec![])));
    }

    #[test]
    fn one_post_balance_is_rejected_for_two_accounts() {
        assert_balance_error(encoded_accounts(
            vec![account(8), account(9)],
            Some(vec![11]),
        ));
    }

    #[test]
    fn two_post_balances_are_recorded() {
        let first = account(8);
        let second = account(9);
        let block = parse_accounts(encoded_accounts(vec![first, second], Some(vec![70, 80])));
        assert_eq!(block.get_account_balance(&first.to_string()), Some(70));
        assert_eq!(block.get_account_balance(&second.to_string()), Some(80));
    }

    #[test]
    fn post_balances_shorter_than_static_accounts_are_rejected() {
        assert_balance_error(encoded_accounts(
            vec![account(8), account(9), account(10)],
            Some(vec![11, 22]),
        ));
    }

    #[test]
    fn absent_metadata_records_no_balances() {
        let key = account(8);
        let block = parse_accounts(encoded_accounts(vec![key], None));
        assert!(block.get_account_map().is_none());
        assert!(block
            .get_tx_details(&Signature::from([9; 64]).to_string())
            .is_some());
    }

    #[test]
    fn signed_transfer_preserves_existing_balance_observations() {
        let payer = Keypair::new();
        let recipient = account(12);
        let instruction = system_instruction::transfer(&payer.pubkey(), &recipient, 1_500);
        let message = Message::new(&[instruction], Some(&payer.pubkey()));
        let mut transaction = Transaction::new_unsigned(message);
        transaction
            .try_sign(&[&payer], Hash::new_from_array([7; 32]))
            .expect("sign transfer");
        let versioned = VersionedTransaction::from(transaction);
        let post_balances = vec![1_000_000, 1_500, 1];
        let encoded = EncodedTransactionWithStatusMeta {
            transaction: versioned.encode_with_meta(
                UiTransactionEncoding::Base64,
                &TransactionStatusMeta::default(),
            ),
            meta: Some(balances_meta(post_balances.clone())),
            version: Some(TransactionVersion::LEGACY),
        };
        let block = parse_accounts(encoded);
        let static_keys = versioned.message.static_account_keys();
        assert_eq!(
            block.get_account_balance(&static_keys[0].to_string()),
            Some(post_balances[0])
        );
        assert_eq!(
            block.get_account_balance(&static_keys[1].to_string()),
            Some(post_balances[1])
        );
        assert_eq!(
            block.get_account_balance(&static_keys[2].to_string()),
            Some(post_balances[2])
        );
        assert_eq!(block.get_account_map().unwrap().len(), static_keys.len());
        assert!(block
            .get_tx_details(&versioned.signatures[0].to_string())
            .is_some());
    }

    #[test]
    fn five_static_accounts_preserve_exact_lamport_values() {
        let keys = vec![
            account(21),
            account(22),
            account(23),
            account(24),
            account(25),
        ];
        let balances = vec![1, 42, u64::MAX, 7, 9_223_372_036_854_775_807];
        let block = parse_accounts(encoded_accounts(keys.clone(), Some(balances.clone())));
        assert_eq!(block.get_account_map().unwrap().len(), 5);
        for (key, balance) in keys.iter().zip(balances.iter()) {
            assert_eq!(block.get_account_balance(&key.to_string()), Some(*balance));
        }
    }

    #[test]
    fn fee_payer_sender_and_recipient_balances_follow_their_indexes() {
        let fee_payer = Keypair::new();
        let sender = Keypair::new();
        let recipient = account(30);
        let instruction = system_instruction::transfer(&sender.pubkey(), &recipient, 50);
        let message = Message::new(&[instruction], Some(&fee_payer.pubkey()));
        let mut transaction = Transaction::new_unsigned(message);
        transaction
            .try_sign(&[&fee_payer, &sender], Hash::new_from_array([4; 32]))
            .expect("sign fee payer and sender");
        let versioned = VersionedTransaction::from(transaction);
        let static_keys = versioned.message.static_account_keys();
        let payer_index = static_keys
            .iter()
            .position(|key| key == &fee_payer.pubkey())
            .unwrap();
        let sender_index = static_keys
            .iter()
            .position(|key| key == &sender.pubkey())
            .unwrap();
        let recipient_index = static_keys
            .iter()
            .position(|key| key == &recipient)
            .unwrap();
        assert_ne!(payer_index, sender_index);
        assert_ne!(sender_index, recipient_index);
        assert_ne!(payer_index, recipient_index);
        let post_balances: Vec<u64> = (0..static_keys.len())
            .map(|index| 1_000_000_000_000 + index as u64)
            .collect();
        let encoded = EncodedTransactionWithStatusMeta {
            transaction: versioned.encode_with_meta(
                UiTransactionEncoding::Base64,
                &TransactionStatusMeta::default(),
            ),
            meta: Some(balances_meta(post_balances.clone())),
            version: Some(TransactionVersion::LEGACY),
        };
        let block = parse_accounts(encoded);
        for (index, key) in static_keys.iter().enumerate() {
            assert_eq!(
                block.get_account_balance(&key.to_string()),
                Some(post_balances[index])
            );
        }
        assert!(block
            .get_tx_details(&versioned.signatures[0].to_string())
            .is_some());
    }

    #[test]
    fn repeated_account_keeps_the_later_transaction_balance() {
        let key = account(40);
        let first = encoded_with_signature(vec![key], Some(vec![10]), 4);
        let second = encoded_with_signature(vec![key], Some(vec![99]), 5);
        let (sender, mut receiver) = unbounded_channel();
        block_on(Parser::invoke(ProtocolMessage::new_chuck(
            3,
            0,
            1,
            vec![first, second],
            sender,
        )))
        .expect("parse ordered transactions");
        let block = match receiver.try_recv().expect("parsed block") {
            ProtocolMessage::ParsedBlock(_, _, _, block) => block,
            other => panic!("expected a parsed block, got {other:?}"),
        };
        assert_eq!(block.get_account_balance(&key.to_string()), Some(99));
        assert!(block
            .get_tx_details(&Signature::from([4; 64]).to_string())
            .is_some());
        assert!(block
            .get_tx_details(&Signature::from([5; 64]).to_string())
            .is_some());
    }

    fn v0_encoded(
        static_keys: Vec<Pubkey>,
        writable: Vec<Pubkey>,
        readonly: Vec<Pubkey>,
        post_balances: Vec<u64>,
        signature_byte: u8,
        loaded: Option<Option<UiLoadedAddresses>>,
    ) -> EncodedTransactionWithStatusMeta {
        let address_table_lookups = if writable.is_empty() && readonly.is_empty() {
            Vec::new()
        } else {
            vec![MessageAddressTableLookup {
                account_key: account(99),
                writable_indexes: (0..writable.len() as u8).collect(),
                readonly_indexes: (0..readonly.len() as u8).collect(),
            }]
        };
        let versioned = VersionedTransaction {
            signatures: vec![Signature::from([signature_byte; 64])],
            message: VersionedMessage::V0(v0::Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: static_keys.len().saturating_sub(1) as u8,
                },
                account_keys: static_keys,
                recent_blockhash: Hash::new_from_array([2; 32]),
                instructions: vec![],
                address_table_lookups,
            }),
        };
        let mut meta = balances_meta(post_balances);
        meta.loaded_addresses = match loaded {
            None => OptionSerializer::Skip,
            Some(None) => OptionSerializer::None,
            Some(Some(addresses)) => OptionSerializer::Some(addresses),
        };
        EncodedTransactionWithStatusMeta {
            transaction: versioned.encode_with_meta(
                UiTransactionEncoding::Base64,
                &TransactionStatusMeta::default(),
            ),
            meta: Some(meta),
            version: Some(TransactionVersion::Number(0)),
        }
    }

    fn loaded(writable: &[Pubkey], readonly: &[Pubkey]) -> UiLoadedAddresses {
        UiLoadedAddresses {
            writable: writable.iter().map(ToString::to_string).collect(),
            readonly: readonly.iter().map(ToString::to_string).collect(),
        }
    }

    #[test]
    fn legacy_transaction_records_static_balances_only() {
        let first = account(1);
        let second = account(2);
        let block = parse_accounts(encoded_accounts(vec![first, second], Some(vec![4, 5])));
        assert_eq!(block.get_account_balance(&first.to_string()), Some(4));
        assert_eq!(block.get_account_balance(&second.to_string()), Some(5));
        assert_eq!(block.get_account_map().unwrap().len(), 2);
    }

    #[test]
    fn v0_without_loaded_addresses_records_static_balances() {
        let first = account(1);
        let second = account(2);
        let block = parse_accounts(v0_encoded(
            vec![first, second],
            vec![],
            vec![],
            vec![4, 5],
            11,
            None,
        ));
        assert_eq!(block.get_account_balance(&first.to_string()), Some(4));
        assert_eq!(block.get_account_balance(&second.to_string()), Some(5));
        assert_eq!(block.get_account_map().unwrap().len(), 2);
        assert!(block
            .get_tx_details(&Signature::from([11; 64]).to_string())
            .is_some());
    }

    #[test]
    fn v0_loaded_writable_address_is_indexed() {
        let payer = account(1);
        let writable = account(2);
        let block = parse_accounts(v0_encoded(
            vec![payer],
            vec![writable],
            vec![],
            vec![10, u64::MAX],
            12,
            Some(Some(loaded(&[writable], &[]))),
        ));
        assert_eq!(block.get_account_balance(&payer.to_string()), Some(10));
        assert_eq!(
            block.get_account_balance(&writable.to_string()),
            Some(u64::MAX)
        );
    }

    #[test]
    fn v0_loaded_readonly_address_is_indexed() {
        let payer = account(1);
        let readonly = account(3);
        let block = parse_accounts(v0_encoded(
            vec![payer],
            vec![],
            vec![readonly],
            vec![10, 77],
            13,
            Some(Some(loaded(&[], &[readonly]))),
        ));
        assert_eq!(block.get_account_balance(&readonly.to_string()), Some(77));
    }

    #[test]
    fn v0_loaded_addresses_follow_static_writable_readonly_order() {
        let payer = account(1);
        let writable_a = account(2);
        let writable_b = account(3);
        let readonly_a = account(4);
        let readonly_b = account(5);
        let balances = vec![11, 22, 33, 44, u64::MAX];
        let block = parse_accounts(v0_encoded(
            vec![payer],
            vec![writable_a, writable_b],
            vec![readonly_a, readonly_b],
            balances.clone(),
            14,
            Some(Some(loaded(
                &[writable_a, writable_b],
                &[readonly_a, readonly_b],
            ))),
        ));
        let keys = [payer, writable_a, writable_b, readonly_a, readonly_b];
        for (key, balance) in keys.iter().zip(balances.iter()) {
            assert_eq!(block.get_account_balance(&key.to_string()), Some(*balance));
        }
        assert_eq!(block.get_account_map().unwrap().len(), 5);
        assert!(block
            .get_tx_details(&Signature::from([14; 64]).to_string())
            .is_some());
    }

    #[test]
    fn missing_loaded_address_metadata_is_rejected() {
        assert_balance_error(v0_encoded(
            vec![account(1)],
            vec![account(2)],
            vec![],
            vec![1, 2],
            15,
            None,
        ));
        assert_balance_error(v0_encoded(
            vec![account(1)],
            vec![account(2)],
            vec![],
            vec![1, 2],
            16,
            Some(None),
        ));
    }

    #[test]
    fn loaded_address_count_mismatch_is_rejected() {
        let payer = account(1);
        let writable = account(2);
        let mut encoded = v0_encoded(
            vec![payer],
            vec![writable],
            vec![],
            vec![1, 2],
            17,
            Some(Some(loaded(&[writable, account(3)], &[]))),
        );
        encoded.meta.as_mut().unwrap().post_balances = vec![1, 2, 3];
        assert_balance_error(encoded);
    }

    #[test]
    fn missing_post_balance_for_loaded_account_is_rejected() {
        assert_balance_error(v0_encoded(
            vec![account(1)],
            vec![account(2)],
            vec![account(3)],
            vec![1, 2],
            18,
            Some(Some(loaded(&[account(2)], &[account(3)]))),
        ));
    }

    #[test]
    fn malformed_loaded_public_key_is_rejected() {
        let mut addresses = loaded(&[account(2)], &[]);
        addresses.writable = vec!["not-a-solana-public-key".to_string()];
        assert_balance_error(v0_encoded(
            vec![account(1)],
            vec![account(2)],
            vec![],
            vec![1, 2],
            19,
            Some(Some(addresses)),
        ));
    }

    #[test]
    fn later_loaded_account_observation_replaces_the_earlier_balance() {
        let payer = account(1);
        let loaded_account = account(2);
        let first = v0_encoded(
            vec![payer],
            vec![loaded_account],
            vec![],
            vec![5, 10],
            20,
            Some(Some(loaded(&[loaded_account], &[]))),
        );
        let second = v0_encoded(
            vec![payer],
            vec![loaded_account],
            vec![],
            vec![6, 99],
            21,
            Some(Some(loaded(&[loaded_account], &[]))),
        );
        let (sender, mut receiver) = unbounded_channel();
        block_on(Parser::invoke(ProtocolMessage::new_chuck(
            9,
            0,
            1,
            vec![first, second],
            sender,
        )))
        .expect("parse v0 transactions");
        let block = match receiver.try_recv().expect("parsed block") {
            ProtocolMessage::ParsedBlock(_, _, _, block) => block,
            other => panic!("expected a parsed block, got {other:?}"),
        };
        assert_eq!(block.get_account_balance(&payer.to_string()), Some(6));
        assert_eq!(
            block.get_account_balance(&loaded_account.to_string()),
            Some(99)
        );
        assert!(block
            .get_tx_details(&Signature::from([20; 64]).to_string())
            .is_some());
        assert!(block
            .get_tx_details(&Signature::from([21; 64]).to_string())
            .is_some());
    }

    struct SignedV0Transfer {
        lamports: u64,
        static_keys: Vec<Pubkey>,
        writable: Vec<Pubkey>,
        readonly: Vec<Pubkey>,
        account_indexes: Vec<u8>,
        program_index: u8,
        readonly_unsigned: u8,
        post_balances: Vec<u64>,
        loaded_addresses: Option<Option<UiLoadedAddresses>>,
    }

    fn signed_v0_transfer(
        source: &Keypair,
        fixture: SignedV0Transfer,
    ) -> (VersionedTransaction, EncodedTransactionWithStatusMeta) {
        let address_table_lookups = if fixture.writable.is_empty() && fixture.readonly.is_empty() {
            Vec::new()
        } else {
            vec![MessageAddressTableLookup {
                account_key: account(99),
                writable_indexes: (0..fixture.writable.len() as u8).collect(),
                readonly_indexes: (0..fixture.readonly.len() as u8).collect(),
            }]
        };
        let message = VersionedMessage::V0(v0::Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: fixture.readonly_unsigned,
            },
            account_keys: fixture.static_keys,
            recent_blockhash: Hash::new_from_array([8; 32]),
            instructions: vec![CompiledInstruction::new(
                fixture.program_index,
                &SystemInstruction::Transfer {
                    lamports: fixture.lamports,
                },
                fixture.account_indexes,
            )],
            address_table_lookups,
        });
        let versioned =
            VersionedTransaction::try_new(message, &[source]).expect("sign v0 transfer");
        let mut meta = balances_meta(fixture.post_balances);
        meta.loaded_addresses = match fixture.loaded_addresses {
            None => OptionSerializer::Skip,
            Some(None) => OptionSerializer::None,
            Some(Some(addresses)) => OptionSerializer::Some(addresses),
        };
        let encoded = EncodedTransactionWithStatusMeta {
            transaction: versioned.encode_with_meta(
                UiTransactionEncoding::Base64,
                &TransactionStatusMeta::default(),
            ),
            meta: Some(meta),
            version: Some(TransactionVersion::Number(0)),
        };
        (versioned, encoded)
    }

    fn loaded_destination_transfer(
        source: &Keypair,
        destination: Pubkey,
        lamports: u64,
        post_balances: Vec<u64>,
        loaded_addresses: Option<Option<UiLoadedAddresses>>,
    ) -> (VersionedTransaction, EncodedTransactionWithStatusMeta) {
        signed_v0_transfer(
            source,
            SignedV0Transfer {
                lamports,
                static_keys: vec![source.pubkey(), system_program::id()],
                writable: vec![destination],
                readonly: vec![],
                account_indexes: vec![0, 2],
                program_index: 1,
                readonly_unsigned: 1,
                post_balances,
                loaded_addresses,
            },
        )
    }

    fn transfer_json(block: &Block, signature: &str) -> serde_json::Value {
        let record = block
            .get_tx_details(signature)
            .expect("indexed transaction");
        serde_json::to_value(record).expect("transaction json")
    }

    #[test]
    fn legacy_transfer_resolves_static_accounts() {
        let source = Keypair::new();
        let destination = account(12);
        let lamports = 1_500_000_000;
        let instruction = system_instruction::transfer(&source.pubkey(), &destination, lamports);
        let message = Message::new(&[instruction], Some(&source.pubkey()));
        let mut transaction = Transaction::new_unsigned(message);
        transaction
            .try_sign(&[&source], Hash::new_from_array([7; 32]))
            .expect("sign legacy transfer");
        let versioned = VersionedTransaction::from(transaction);
        let static_keys = versioned.message.static_account_keys().to_vec();
        let encoded = EncodedTransactionWithStatusMeta {
            transaction: versioned.encode_with_meta(
                UiTransactionEncoding::Base64,
                &TransactionStatusMeta::default(),
            ),
            meta: Some(balances_meta(vec![9, 8, 1])),
            version: Some(TransactionVersion::LEGACY),
        };
        let signature = versioned.signatures[0].to_string();
        let block = parse_accounts(encoded);
        let value = transfer_json(&block, &signature);
        assert_eq!(
            value["instruction"][0],
            serde_json::to_value(Instruction::transfer(
                static_keys[0],
                static_keys[1],
                lamports as f64 / 1_000_000_000.0
            ))
            .unwrap()
        );
        assert_eq!(block.get_account_map().unwrap().len(), static_keys.len());
    }

    #[test]
    fn v0_transfer_resolves_static_accounts() {
        let source = Keypair::new();
        let destination = account(12);
        let lamports = 2_000_000_000;
        let (versioned, encoded) = signed_v0_transfer(
            &source,
            SignedV0Transfer {
                lamports,
                static_keys: vec![source.pubkey(), destination, system_program::id()],
                writable: vec![],
                readonly: vec![],
                account_indexes: vec![0, 1],
                program_index: 2,
                readonly_unsigned: 1,
                post_balances: vec![40, 50, 1],
                loaded_addresses: None,
            },
        );
        let block = parse_accounts(encoded);
        let value = transfer_json(&block, &versioned.signatures[0].to_string());
        assert_eq!(
            value["instruction"][0],
            serde_json::to_value(Instruction::transfer(
                source.pubkey(),
                destination,
                lamports as f64 / 1_000_000_000.0
            ))
            .unwrap()
        );
        assert_eq!(
            block.get_account_balance(&destination.to_string()),
            Some(50)
        );
    }

    #[test]
    fn v0_transfer_resolves_loaded_writable_destination() {
        let source = Keypair::new();
        let destination = account(21);
        let lamports = 3_500_000_000;
        let balances = vec![70, 1, u64::MAX];
        let (versioned, encoded) = loaded_destination_transfer(
            &source,
            destination,
            lamports,
            balances.clone(),
            Some(Some(loaded(&[destination], &[]))),
        );
        assert_eq!(versioned.message.static_account_keys().len(), 2);
        let block = parse_accounts(encoded);
        let signature = versioned.signatures[0].to_string();
        let value = transfer_json(&block, &signature);
        assert_eq!(
            value["instruction"][0],
            serde_json::to_value(Instruction::transfer(
                source.pubkey(),
                destination,
                lamports as f64 / 1_000_000_000.0
            ))
            .unwrap()
        );
        assert_eq!(
            block.get_account_balance(&source.pubkey().to_string()),
            Some(balances[0])
        );
        assert_eq!(
            block.get_account_balance(&system_program::id().to_string()),
            Some(balances[1])
        );
        assert_eq!(
            block.get_account_balance(&destination.to_string()),
            Some(balances[2])
        );
        assert!(block.get_tx_details(&signature).is_some());
    }

    #[test]
    fn missing_loaded_addresses_reject_a_loaded_transfer_operand() {
        let source = Keypair::new();
        let destination = account(21);
        let (_versioned, encoded) =
            loaded_destination_transfer(&source, destination, 10, vec![1, 1, 1], None);
        assert!(matches!(
            encoded.meta.as_ref().unwrap().loaded_addresses,
            OptionSerializer::Skip
        ));
        assert_balance_error(encoded);

        let source = Keypair::new();
        let destination = account(21);
        let (_versioned, explicit_none) =
            loaded_destination_transfer(&source, destination, 10, vec![1, 1, 1], Some(None));
        assert_balance_error(explicit_none);
    }

    #[test]
    fn loaded_address_count_mismatch_rejects_a_transfer() {
        let destination = account(21);
        let source = Keypair::new();
        let (_versioned, encoded) = loaded_destination_transfer(
            &source,
            destination,
            10,
            vec![1, 1, 1],
            Some(Some(loaded(&[destination, account(22)], &[]))),
        );
        assert_balance_error(encoded);
    }

    #[test]
    fn malformed_loaded_address_rejects_a_transfer() {
        let destination = account(21);
        let source = Keypair::new();
        let mut addresses = loaded(&[destination], &[]);
        addresses.writable = vec!["not-a-solana-public-key".to_string()];
        let (_versioned, encoded) = loaded_destination_transfer(
            &source,
            destination,
            10,
            vec![1, 1, 1],
            Some(Some(addresses)),
        );
        assert_balance_error(encoded);
    }

    fn resolved_transfer_message() -> (VersionedMessage, UiTransactionStatusMeta) {
        let source = Keypair::new();
        let destination = account(21);
        let (versioned, encoded) = loaded_destination_transfer(
            &source,
            destination,
            10,
            vec![1, 1, 1],
            Some(Some(loaded(&[destination], &[]))),
        );
        (versioned.message, encoded.meta.expect("loaded metadata"))
    }

    #[test]
    fn invalid_transfer_source_index_is_rejected() {
        let (message, meta) = resolved_transfer_message();
        let mut instruction = message.instructions()[0].clone();
        instruction.accounts = vec![9, 2];
        assert!(matches!(
            Parser::decode_transfer_instruction(&message, &instruction, Some(&meta)),
            Err(AggError::InvalidTransferInstruction)
        ));
    }

    #[test]
    fn invalid_transfer_destination_index_is_rejected() {
        let (message, meta) = resolved_transfer_message();
        let mut instruction = message.instructions()[0].clone();
        instruction.accounts = vec![0, 9];
        assert!(matches!(
            Parser::decode_transfer_instruction(&message, &instruction, Some(&meta)),
            Err(AggError::InvalidTransferInstruction)
        ));
    }

    #[test]
    fn invalid_transfer_program_index_is_rejected() {
        let (message, meta) = resolved_transfer_message();
        let mut instruction = message.instructions()[0].clone();
        instruction.program_id_index = 9;
        assert!(matches!(
            Parser::decode_transfer_instruction(&message, &instruction, Some(&meta)),
            Err(AggError::InvalidTransferInstruction)
        ));
    }
}
