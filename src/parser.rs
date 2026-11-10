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
                    let sender_account = message.static_account_keys()[0];
                    let sender_balance = meta.post_balances[0];
                    let receiver_account = message.static_account_keys()[1];
                    let receiver_balance = meta.post_balances[1];
                    partial_block.insert_account(sender_account.to_string(), sender_balance);
                    partial_block.insert_account(receiver_account.to_string(), receiver_balance);
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
    use crate::util::Instruction;
    use solana_program::hash::Hash;
    use solana_program::instruction::CompiledInstruction;
    use solana_program::message::{Message, MessageHeader, VersionedMessage};
    use solana_program::pubkey::Pubkey;
    use solana_program::system_instruction::SystemInstruction;
    use solana_program::system_program;

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
}
