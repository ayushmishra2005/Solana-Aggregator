use crate::util::ProtocolMessage;
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::rpc_request::RpcError;
use solana_pubkey::ParsePubkeyError;
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
    AccountNotFound,
    InvalidRequest,
    InvalidTransactionSignature,
    InvalidTransferInstruction,
    InvalidBalanceMetadata,
    InvalidChunk,
    ConflictingChunk,
    ConflictingBlock,
    WriteFailed,
    IncompatibleStorage(String),
}

impl Display for AggError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let err_mgs = match self {
            AggError::ClientError(err) => format_client_error(err),
            AggError::UnableToParsePublicKey(err) => format!("Unable to parse public key: {}", err),
            AggError::ConversionError(err) => format!("Conversion Error: {}", err),
            AggError::MpscChannelError(err) => format!("Mpsc Channel Error: {}", err),
            AggError::OneshotChannelError => "Oneshot Channel Error".to_string(),
            AggError::DbError(err) => format!("Db Error: {}", err),
            AggError::JsonError(err) => format!("Json Error: {}", err),
            AggError::BlockNotFound => "Block Not Found".to_string(),
            AggError::NoBlockFinalised => "No Block Finalised".to_string(),
            AggError::TxNotFound => "Transaction Not Found".to_string(),
            AggError::AccountNotFound => "Account observation not found".to_string(),
            AggError::InvalidRequest => "Invalid request".to_string(),
            AggError::InvalidTransactionSignature => "Invalid transaction signature".to_string(),
            AggError::InvalidTransferInstruction => {
                "Invalid system transfer instruction".to_string()
            }
            AggError::InvalidBalanceMetadata => "Invalid balance metadata".to_string(),
            AggError::InvalidChunk => "Invalid block chunk".to_string(),
            AggError::ConflictingChunk => "Conflicting block chunk".to_string(),
            AggError::ConflictingBlock => "Stored block does not match this block".to_string(),
            AggError::WriteFailed => "Block write failed".to_string(),
            AggError::IncompatibleStorage(message) => message.clone(),
            AggError::ServerError(err) => format!("Server Error {}", err),
        };
        write!(f, "{}", err_mgs)
    }
}

impl Debug for AggError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let err_mgs = match self {
            AggError::ClientError(err) => format_client_error(err),
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
            AggError::AccountNotFound => "Account observation not found".to_string(),
            AggError::InvalidRequest => "Invalid request".to_string(),
            AggError::InvalidTransactionSignature => "Invalid transaction signature".to_string(),
            AggError::InvalidTransferInstruction => {
                "Invalid system transfer instruction".to_string()
            }
            AggError::InvalidBalanceMetadata => "Invalid balance metadata".to_string(),
            AggError::InvalidChunk => "Invalid block chunk".to_string(),
            AggError::ConflictingChunk => "Conflicting block chunk".to_string(),
            AggError::ConflictingBlock => "Stored block does not match this block".to_string(),
            AggError::WriteFailed => "Block write failed".to_string(),
            AggError::IncompatibleStorage(message) => message.clone(),
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

fn format_client_error(err: &ClientError) -> String {
    format!("Client Error: {}", rpc_failure_class(err))
}

/// Classifies a transport or RPC failure without copying the upstream error text.
/// That text includes the complete RPC URL, so punctuation in a credential can
/// survive any attempt to redact the rendered string.
fn rpc_failure_class(err: &ClientError) -> String {
    match err.kind() {
        ClientErrorKind::RpcError(RpcError::RpcResponseError { code, .. }) => {
            format!("RPC error {code}")
        }
        ClientErrorKind::RpcError(_) => "RPC error".to_string(),
        ClientErrorKind::Reqwest(request) => request_class(request).to_string(),
        ClientErrorKind::Io(io) => io_class(io).to_string(),
        _ => fallback_class(err).to_string(),
    }
}

fn request_class(err: &solana_client::client_error::reqwest::Error) -> &'static str {
    if err.is_timeout() || source_is_timeout(err) {
        "timeout"
    } else if err.status()
        == Some(solana_client::client_error::reqwest::StatusCode::TOO_MANY_REQUESTS)
    {
        "rate limited (429)"
    } else if err.is_connect() || source_is_connection(err) {
        "connection error"
    } else {
        "transport error"
    }
}

fn io_class(err: &std::io::Error) -> &'static str {
    if err.kind() == std::io::ErrorKind::TimedOut {
        "timeout"
    } else if is_connection_kind(err.kind()) {
        "connection error"
    } else {
        "transport error"
    }
}

fn fallback_class(err: &ClientError) -> &'static str {
    if source_is_timeout(err) {
        "timeout"
    } else if source_is_connection(err) {
        "connection error"
    } else {
        "transport error"
    }
}

fn source_is_timeout(err: &dyn std::error::Error) -> bool {
    source_kind(err).is_some_and(|kind| kind == std::io::ErrorKind::TimedOut)
}

fn source_is_connection(err: &dyn std::error::Error) -> bool {
    source_kind(err).is_some_and(is_connection_kind)
}

fn source_kind(err: &dyn std::error::Error) -> Option<std::io::ErrorKind> {
    let mut source = std::error::Error::source(err);
    while let Some(next) = source {
        if let Some(io) = next.downcast_ref::<std::io::Error>() {
            return Some(io.kind());
        }
        source = next.source();
    }
    None
}

fn is_connection_kind(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::BrokenPipe
    )
}

#[cfg(test)]
mod tests {
    use super::AggError;
    use solana_client::rpc_client::RpcClient;
    use std::time::Duration;

    fn assert_classified_without_secrets(
        err: solana_client::client_error::ClientError,
        secrets: &[&str],
    ) {
        let raw = format!("{err} {err:?}");
        assert!(
            secrets.iter().any(|secret| raw.contains(secret)),
            "the RPC client did not expose a credential, so the classifier was not exercised"
        );
        let agg = AggError::from(err);
        let outputs = [
            agg.to_string(),
            format!("{agg:?}"),
            format!("subscriber failed to start: {agg}"),
            format!("Error from subscriber client {agg}"),
            format!("Failed to fetch latest slot {agg:?}"),
            format!("Failed to fetch block at slot 9 after retry exhaustion: {agg:?}"),
            format!("Retrying getBlock for slot 9: {agg:?}"),
            format!("Error reading RPC genesis hash {agg}"),
            format!("Fatal indexer failure: {agg}"),
        ];
        for output in &outputs {
            for secret in secrets {
                assert!(
                    !output.contains(secret),
                    "application output leaked a credential"
                );
            }
            assert!(output.contains("Client Error:"), "{output}");
            assert!(
                output.contains("connection error")
                    || output.contains("timeout")
                    || output.contains("rate limited")
                    || output.contains("RPC error")
                    || output.contains("transport error"),
                "{output}"
            );
        }
    }

    #[test]
    fn synthetic_rpc_credential_is_hidden_in_application_errors() {
        const SECRET: &str = "synthetic-test-credential";
        let url = format!("http://127.0.0.1:1/?api-key={SECRET}");
        let client = RpcClient::new_with_timeout(url, Duration::from_secs(2));
        let err = client
            .get_slot()
            .expect_err("RPC call to a closed local port should fail");
        assert_classified_without_secrets(err, &[SECRET, "api-key="]);
    }

    #[test]
    fn punctuated_url_credentials_stay_out_of_startup_and_rpc_errors() {
        const USER_SECRET: &str = "user)secret";
        const PASS_SECRET: &str = "pass'secret";
        const PATH_SECRET: &str = "path)secret";
        const QUERY_SECRET: &str = "query'secret";
        const FRAGMENT_SECRET: &str = "frag)secret";
        const ENCODED_SECRET: &str = "%27%29encoded";
        let url = format!(
            "http://{USER_SECRET}:{PASS_SECRET}@127.0.0.1:1/{PATH_SECRET}?api-key={QUERY_SECRET}&raw={ENCODED_SECRET}#{FRAGMENT_SECRET}"
        );
        let client = RpcClient::new_with_timeout(url, Duration::from_secs(2));
        let err = client.get_slot().expect_err("closed local port");
        assert_classified_without_secrets(
            err,
            &[
                USER_SECRET,
                PASS_SECRET,
                PATH_SECRET,
                QUERY_SECRET,
                FRAGMENT_SECRET,
                ENCODED_SECRET,
            ],
        );
    }
}
