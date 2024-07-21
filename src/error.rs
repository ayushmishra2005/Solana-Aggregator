use crate::util::ProtocolMessage;
use solana_client::client_error::ClientError;
use solana_program::pubkey::ParsePubkeyError;
use std::array::TryFromSliceError;
use std::fmt::{Debug, Display, Formatter};
use std::io::Error;
use tokio::sync::mpsc::error::SendError;
pub enum AggError {
    ClientError(ClientError),
    UnableToParsePublicKey(ParsePubkeyError),
    ConversionError(TryFromSliceError),
    MpscChannelError(SendError<ProtocolMessage>),
    OneshotChannelError,
    DbError(rocksdb::Error),
    JsonError(serde_json::Error),
    ServerError(std::io::Error),
    BlockNotFound,
    NoBlockFinalised,
    TxNotFound,
}

impl Display for AggError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let err_mgs = match self {
            AggError::ClientError(err) => format!("Client Error: {}", err),
            AggError::UnableToParsePublicKey(err) => format!("Unable to parse public key: {}", err),
            AggError::ConversionError(err) => format!("Conversion Error: {}", err),
            AggError::MpscChannelError(err) => format!("Mpsc Channel Error: {}", err),
            AggError::OneshotChannelError => "Oneshot Channel Error".to_string(),
            AggError::DbError(err) => format!("Db Error: {}", err),
            AggError::JsonError(err) => format!("Json Error: {}", err),
            AggError::BlockNotFound => "Block Not Found".to_string(),
            AggError::NoBlockFinalised => "No Block Finalised".to_string(),
            AggError::TxNotFound => "Transaction Not Found".to_string(),
            AggError::ServerError(err) => format!("Server Error {}", err),
        };
        write!(f, "{}", err_mgs)
    }
}

impl Debug for AggError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let err_mgs = match self {
            AggError::ClientError(err) => format!("Client Error: {:?}", err),
            AggError::UnableToParsePublicKey(err) => {
                format!("Unable to parse public key: {:?}", err)
            }
            AggError::ConversionError(err) => format!("Conversion Error: {:?}", err),
            AggError::MpscChannelError(err) => format!("Mpsc Channel Error: {:?}", err),
            AggError::OneshotChannelError => "Oneshot Channel Error".to_string(),
            AggError::DbError(err) => format!("Db Error: {:?}", err),
            AggError::JsonError(err) => format!("Json Error: {:?}", err),
            AggError::BlockNotFound => "Block Not Found".to_string(),
            AggError::NoBlockFinalised => "No Block Finalised".to_string(),
            AggError::TxNotFound => "Transaction Not Found".to_string(),
            AggError::ServerError(err) => format!("Server Error {:?}", err),
        };
        write!(f, "{}", err_mgs)
    }
}

impl From<ClientError> for AggError {
    fn from(err: ClientError) -> Self {
        Self::ClientError(err)
    }
}

impl From<ParsePubkeyError> for AggError {
    fn from(err: ParsePubkeyError) -> Self {
        Self::UnableToParsePublicKey(err)
    }
}

impl From<TryFromSliceError> for AggError {
    fn from(err: TryFromSliceError) -> Self {
        Self::ConversionError(err)
    }
}

impl From<SendError<ProtocolMessage>> for AggError {
    fn from(err: SendError<ProtocolMessage>) -> Self {
        Self::MpscChannelError(err)
    }
}

impl From<rocksdb::Error> for AggError {
    fn from(err: rocksdb::Error) -> Self {
        Self::DbError(err)
    }
}

impl From<serde_json::Error> for AggError {
    fn from(err: serde_json::Error) -> Self {
        Self::JsonError(err)
    }
}

impl From<std::io::Error> for AggError {
    fn from(value: Error) -> Self {
        Self::ServerError(value)
    }
}
