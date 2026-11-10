use crate::error::AggError;
use crate::util::{Block, Instruction, ProtocolMessage, TxRecord};
use log::debug;
use solana_program::instruction::CompiledInstruction;
use solana_program::message::VersionedMessage;
use solana_program::pubkey::Pubkey;
use solana_program::system_program;

const SYSTEM_TRANSFER_DISCRIMINANT: u32 = 2;
const SYSTEM_TRANSFER_DATA_LEN: usize = 12;

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
                for (_, instruction) in message.instructions().iter().enumerate() {
                    if Self::is_transfer_instruction(message, instruction)? {
                        instructions.push(Self::decode_transfer_instruction(message, instruction)?);
                    }
                }
                if let Some(meta) = tx.meta.clone() {
                    Self::record_observed_balances(
                        &mut partial_block,
                        message.static_account_keys(),
                        &meta.post_balances,
                    )
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
    ) -> Result<bool, AggError> {
        let Some(is_system_program) = Self::is_system_program(message, instruction) else {
            return Err(AggError::InvalidTransferInstruction);
        };
        if !is_system_program {
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
    ) -> Result<Instruction, AggError> {
        let Some(is_system_program) = Self::is_system_program(message, instruction) else {
            return Err(AggError::InvalidTransferInstruction);
        };
        if !is_system_program {
            return Err(AggError::InvalidTransferInstruction);
        }
        let Some(lamports) = Self::transfer_lamports(&instruction.data)
            .map_err(|()| AggError::InvalidTransferInstruction)?
        else {
            return Err(AggError::InvalidTransferInstruction);
        };
        let from = Self::transfer_account(message, instruction, 0)
            .ok_or(AggError::InvalidTransferInstruction)?;
        let to = Self::transfer_account(message, instruction, 1)
            .ok_or(AggError::InvalidTransferInstruction)?;
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
    ) -> Option<bool> {
        let program_id = message
            .static_account_keys()
            .get(instruction.program_id_index as usize)?;
        Some(*program_id == system_program::id())
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

    /// Records every static-account balance.
    /// `post_balances` must cover every static account key.
    fn record_observed_balances(
        block: &mut Block,
        static_keys: &[Pubkey],
        post_balances: &[u64],
    ) -> Result<(), ()> {
        if post_balances.len() < static_keys.len() {
            return Err(());
        }
        for (key, balance) in static_keys.iter().zip(post_balances.iter()) {
            block.insert_account(key.to_string(), *balance);
        }
        Ok(())
    }

    fn transfer_account(
        message: &VersionedMessage,
        instruction: &CompiledInstruction,
        operand: usize,
    ) -> Option<Pubkey> {
        let index = instruction.accounts.get(operand).copied()?;
        message.static_account_keys().get(index as usize).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::Parser;
    use crate::error::AggError;
    use crate::util::{Block, Instruction, ProtocolMessage};
    use solana_program::hash::Hash;
    use solana_program::instruction::CompiledInstruction;
    use solana_program::message::{Message, MessageHeader, VersionedMessage};
    use solana_program::pubkey::Pubkey;
    use solana_program::system_instruction::{self, SystemInstruction};
    use solana_program::system_program;
    use solana_sdk::signature::{Keypair, Signature, Signer};
    use solana_sdk::transaction::{Transaction, TransactionVersion, VersionedTransaction};
    use solana_transaction_status::{
        EncodableWithMeta, EncodedTransactionWithStatusMeta, TransactionStatusMeta,
        UiTransactionEncoding, UiTransactionStatusMeta,
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
            Parser::decode_transfer_instruction(message, instruction),
            Err(AggError::InvalidTransferInstruction)
        ));
    }

    #[test]
    fn valid_system_transfer_decodes() {
        let (from, to, message) = transfer_keys();
        let instruction = valid_transfer_instruction();
        assert_eq!(instruction.data.len(), 12);
        assert_eq!(&instruction.data[..4], &[2, 0, 0, 0]);
        assert!(Parser::is_transfer_instruction(&message, &instruction).unwrap());
        let decoded = Parser::decode_transfer_instruction(&message, &instruction).unwrap();
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
        assert!(!Parser::is_transfer_instruction(&message, &instruction).unwrap());
        assert_rejected(&message, &instruction);
    }

    #[test]
    fn truncated_discriminant_is_not_a_transfer() {
        let (_from, _to, message) = transfer_keys();
        let instruction = transfer_instruction(vec![0, 1], vec![2, 0, 0]);
        assert!(!Parser::is_transfer_instruction(&message, &instruction).unwrap());
        assert_rejected(&message, &instruction);
    }

    #[test]
    fn truncated_transfer_payload_is_rejected() {
        let (_from, _to, message) = transfer_keys();
        let mut instruction = valid_transfer_instruction();
        instruction.data.truncate(7);
        assert!(matches!(
            Parser::is_transfer_instruction(&message, &instruction),
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
        assert!(!Parser::is_transfer_instruction(&message, &instruction).unwrap());
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
            Parser::decode_transfer_instruction(&message, &valid_transfer_instruction()).unwrap();
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
        Parser::record_observed_balances(&mut block, &[], &[]).expect("empty account list");
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
}
