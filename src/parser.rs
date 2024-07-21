use crate::error::AggError;
use crate::util::{Block, Instruction, ProtocolMessage, TxRecord};
use log::debug;
use solana_program::instruction::CompiledInstruction;
use solana_program::message::VersionedMessage;
use solana_program::pubkey::Pubkey;
use std::str::FromStr;

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
                if let Some(transaction) = tx.transaction.decode() {
                    let message = &transaction.message;
                    for (_, instruction) in message.instructions().iter().enumerate() {
                        if Self::is_transfer_instruction(&message, instruction)? {
                            instructions
                                .push(Self::decode_transfer_instruction(&message, instruction)?);
                        }
                    }
                    if let Some(meta) = tx.meta.clone() {
                        let sender_account = message.static_account_keys()[0];
                        let sender_balance = meta.post_balances[0];
                        let receiver_account = message.static_account_keys()[1];
                        let receiver_balance = meta.post_balances[1];
                        partial_block.insert_account(sender_account.to_string(), sender_balance);
                        partial_block
                            .insert_account(receiver_account.to_string(), receiver_balance);
                    }
                    partial_block.push_transaction(
                        transaction.message.hash(),
                        TxRecord::new(instructions, tx.meta.clone()),
                    );
                }
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
        // Check if the program ID is the System Program
        let program_id = message.static_account_keys()[instruction.program_id_index as usize];
        let system_program_id = Pubkey::from_str("11111111111111111111111111111111")?;
        Ok(program_id == system_program_id && instruction.data[0] == 2) // 2 is the index for transfer instruction
    }

    fn decode_transfer_instruction(
        message: &VersionedMessage,
        instruction: &CompiledInstruction,
    ) -> Result<Instruction, AggError> {
        let accounts = &instruction.accounts;
        let default_key = Pubkey::from([1; 32]);
        let from = message
            .static_account_keys()
            .get(accounts[0] as usize)
            .unwrap_or(&default_key);
        let to = message
            .static_account_keys()
            .get(accounts[1] as usize)
            .unwrap_or(&default_key);

        let amount = u64::from_le_bytes(instruction.data[4..12].try_into()?);
        let amount = amount as f64 / 1_000_000_000.0;

        debug!(
            "Transfer: {} SOL from {} to {}",
            amount,
            from.to_string(),
            to.to_string()
        );
        Ok(Instruction::transfer(*from, *to, amount))
    }
}
