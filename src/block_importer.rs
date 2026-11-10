use crate::error::AggError;
use crate::parser::Parser;
use crate::util::ProtocolMessage;
use log::{error, warn};
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::RpcBlockConfig;
use solana_client::rpc_custom_error::JSON_RPC_SERVER_ERROR_SLOT_SKIPPED;
use solana_client::rpc_request::RpcError;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{UiConfirmedBlock, UiTransactionEncoding};
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug)]
enum BlockFetch {
    Fetched(UiConfirmedBlock),
    Skipped(u64),
    Failed(AggError),
}

pub struct Subscriber {
    latest_slot: u64,
    chain_url: String,
    rpc_client: RpcClient,
    rpc_block_config: RpcBlockConfig,
    unbounded_sender: UnboundedSender<ProtocolMessage>,
}

impl Subscriber {

    /// This function initializes the subscriber client
    ///
    /// # Arguments
    ///
    /// * `chain_url` - A string slice that holds the chain url
    /// * `message_sender` - A UnboundedSender<ProtocolMessage> that holds the message sender
    ///
    /// # Returns
    ///
    /// * `Result<Self, AggError>` - A Result that holds the Subscriber client or an error
    pub fn initialize(
        chain_url: String,
        message_sender: UnboundedSender<ProtocolMessage>,
    ) -> Result<Self, AggError> {
        let rpc_client = RpcClient::new(&chain_url);
        let rpc_block_config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            transaction_details: None,
            rewards: None,
            commitment: Some(CommitmentConfig::finalized()),
            max_supported_transaction_version: Some(0),
        };
        let latest_slot = rpc_client.get_slot_with_commitment(CommitmentConfig::finalized())?;
        Ok(Self {
            latest_slot,
            chain_url,
            rpc_client,
            rpc_block_config,
            unbounded_sender: message_sender,
        })
    }

    fn fetch_latest_slot(&self) -> Result<u64, AggError> {
        let slot = self
            .rpc_client
            .get_slot_with_commitment(CommitmentConfig::finalized())?;
        Ok(slot)
    }

    /// This function runs the subscriber client
    pub async fn run(&mut self) {
        loop {
            match self.fetch_latest_slot() {
                Ok(fetched_slot) => {
                    if self.latest_slot < fetched_slot {
                        self.latest_slot = self.latest_slot.saturating_add(1);
                        let sender_clone = self.unbounded_sender.clone();
                        let chain_url = self.chain_url.clone();
                        let rpc_block_config = self.rpc_block_config.clone();
                        let latest_slot = self.latest_slot;
                        tokio::spawn(async move {
                            BlockFetcher::invoke(ProtocolMessage::fetch_block(
                                chain_url,
                                rpc_block_config,
                                latest_slot,
                                sender_clone,
                            ))
                            .await;
                        });
                    }
                }
                Err(err) => {
                    error!(target: "subscriber", "Failed to fetch latest slot {:?}", err);
                }
            }
        }
    }
}

struct BlockFetcher;

impl BlockFetcher {

    /// This function invokes the block fetcher
    ///
    /// # Arguments
    ///
    /// * `message` - A ProtocolMessage that holds the message
    async fn invoke(message: ProtocolMessage) {
        match message {
            ProtocolMessage::FetchBlock(chain_url, rpc_block_config, latest_slot, sender) => {
                let client =
                    RpcClient::new_with_timeout(chain_url, std::time::Duration::from_secs(30));
                let _outcome = Self::request_block(&client, latest_slot, rpc_block_config, sender);
            }
            _ => {}
        }
    }

    fn request_block(
        client: &RpcClient,
        slot: u64,
        rpc_block_config: RpcBlockConfig,
        sender: UnboundedSender<ProtocolMessage>,
    ) -> BlockFetch {
        let outcome =
            Self::classify_get_block(slot, client.get_block_with_config(slot, rpc_block_config));
        Self::dispatch_block_fetch(&outcome, &sender);
        outcome
    }

    fn classify_get_block(slot: u64, result: Result<UiConfirmedBlock, ClientError>) -> BlockFetch {
        match result {
            Ok(block) => BlockFetch::Fetched(block),
            Err(err) if Self::is_explicit_slot_skip(&err) => BlockFetch::Skipped(slot),
            Err(err) => BlockFetch::Failed(AggError::from(err)),
        }
    }

    /// Solana JSON-RPC code -32007 (`SlotSkipped`) is the only explicit skip.
    fn is_explicit_slot_skip(err: &ClientError) -> bool {
        matches!(
            err.kind(),
            ClientErrorKind::RpcError(RpcError::RpcResponseError { code, .. })
                if *code == JSON_RPC_SERVER_ERROR_SLOT_SKIPPED
        )
    }

    fn dispatch_block_fetch(outcome: &BlockFetch, sender: &UnboundedSender<ProtocolMessage>) {
        match outcome {
            BlockFetch::Fetched(block) => {
                if let Some(block_no) = block.block_height {
                    if let Some(txs) = block.transactions.as_deref() {
                        let chunks = txs.chunks(10);
                        let len_of_chunks = chunks.len() as u64;
                        for (index, chunk) in chunks.enumerate() {
                            let sender_clone = sender.clone();
                            let chunk_clone = chunk.to_vec();
                            tokio::spawn(async move {
                                if let Err(error) = Parser::invoke(ProtocolMessage::new_chuck(
                                    block_no,
                                    index as u64,
                                    len_of_chunks,
                                    chunk_clone,
                                    sender_clone,
                                ))
                                .await
                                {
                                    error!(target: "subscriber", "Error from Parser {}", error);
                                }
                            });
                        }
                    }
                } else {
                    warn!(target: "subscriber", "Block Number not available");
                }
            }
            BlockFetch::Skipped(slot) => {
                warn!(target: "subscriber", "Slot {slot} was skipped");
            }
            BlockFetch::Failed(err) => {
                error!(target: "subscriber", "Failed to fetch block {:?}", err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BlockFetcher;
    use solana_client::client_error::Result as RpcResult;
    use solana_client::rpc_client::{RpcClient, RpcClientConfig};
    use solana_client::rpc_config::RpcBlockConfig;
    use solana_client::rpc_request::RpcRequest;
    use solana_client::rpc_sender::{RpcSender, RpcTransportStats};
    use solana_sdk::commitment_config::CommitmentConfig;
    use solana_transaction_status::UiTransactionEncoding;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    struct RecordingSender {
        calls: Arc<Mutex<Vec<(RpcRequest, serde_json::Value)>>>,
    }

    impl RpcSender for RecordingSender {
        fn send<'life0, 'async_trait>(
            &'life0 self,
            request: RpcRequest,
            params: serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = RpcResult<serde_json::Value>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            self.calls
                .lock()
                .expect("rpc recorder")
                .push((request, params));
            Box::pin(async { Ok(serde_json::Value::Null) })
        }

        fn get_transport_stats(&self) -> RpcTransportStats {
            RpcTransportStats::default()
        }

        fn url(&self) -> String {
            "recording-sender".to_string()
        }
    }

    fn rpc_slot_requested(slot: u64) -> u64 {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let client = RpcClient::new_sender(
            RecordingSender {
                calls: Arc::clone(&calls),
            },
            RpcClientConfig::with_commitment(CommitmentConfig::finalized()),
        );
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            transaction_details: None,
            rewards: None,
            commitment: Some(CommitmentConfig::finalized()),
            max_supported_transaction_version: Some(0),
        };

        BlockFetcher::request_block(&client, slot, config, sender);

        let calls = calls.lock().expect("rpc recorder");
        assert_eq!(calls.len(), 1, "slot {slot} should issue one RPC request");
        assert_eq!(calls[0].0, RpcRequest::GetBlock);
        let params = calls[0].1.as_array().expect("getBlock params");
        assert_eq!(
            params
                .get(1)
                .and_then(|config| config.get("commitment"))
                .and_then(serde_json::Value::as_str),
            Some("finalized"),
            "block fetch must keep the finalized commitment"
        );
        params[0].as_u64().expect("getBlock slot")
    }

    #[test]
    fn fetch_block_requests_slot_100() {
        assert_eq!(rpc_slot_requested(100), 100);
    }

    #[test]
    fn fetch_block_requests_slot_500() {
        assert_eq!(rpc_slot_requested(500), 500);
    }

    #[test]
    fn fetch_block_requests_slot_1000() {
        assert_eq!(rpc_slot_requested(1000), 1000);
    }

    const RPC_SECRET: &str = "synthetic-secret";

    struct FetchLogCapture;

    static FETCH_LOG: Mutex<Option<String>> = Mutex::new(None);
    static FETCH_LOGGER: FetchLogCapture = FetchLogCapture;

    impl log::Log for FetchLogCapture {
        fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
            metadata.target() == "subscriber"
        }

        fn log(&self, record: &log::Record<'_>) {
            if record.target() != "subscriber" {
                return;
            }
            let mut slot = FETCH_LOG
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(logs) = slot.as_mut() {
                logs.push_str(&record.args().to_string());
                logs.push('\n');
            }
        }

        fn flush(&self) {}
    }

    fn capture_fetch_logs(log_error: impl FnOnce()) -> String {
        static INSTALL: std::sync::Once = std::sync::Once::new();
        static GUARD: Mutex<()> = Mutex::new(());
        let _guard = GUARD
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        INSTALL.call_once(|| {
            let _ = log::set_logger(&FETCH_LOGGER);
            log::set_max_level(log::LevelFilter::Error);
        });
        {
            let mut slot = FETCH_LOG
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *slot = Some(String::new());
        }
        log_error();
        let mut slot = FETCH_LOG
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        slot.take().unwrap_or_default()
    }

    #[test]
    fn fetch_block_error_log_hides_rpc_credential() {
        let secret_query = format!("api-key={RPC_SECRET}");
        let url = format!("http://127.0.0.1:1/?{secret_query}");
        let client = RpcClient::new_with_timeout(url, std::time::Duration::from_secs(2));
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            transaction_details: None,
            rewards: None,
            commitment: Some(CommitmentConfig::finalized()),
            max_supported_transaction_version: Some(0),
        };
        let raw = client
            .get_block_with_config(100, config)
            .expect_err("closed local port should fail getBlock");
        let raw_display = raw.to_string();
        let raw_debug = format!("{raw:?}");
        assert!(
            raw_display.contains(RPC_SECRET) || raw_debug.contains(RPC_SECRET),
            "getBlock ClientError did not include the synthetic credential, so log redaction was not exercised. display={}",
            raw_display.replace(RPC_SECRET, "<secret>")
        );

        let agg = crate::error::AggError::from(raw);
        let display = agg.to_string();
        let debug = format!("{agg:?}");
        for output in [&display, &debug] {
            assert!(!output.contains(RPC_SECRET), "{output}");
            assert!(!output.contains(&secret_query), "{output}");
            assert!(output.contains("Client Error:"), "{output}");
            assert!(output.contains("127.0.0.1"), "{output}");
        }

        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        let logged = capture_fetch_logs(|| {
            BlockFetcher::request_block(&client, 100, config, sender);
        });
        assert!(
            logged.contains("Failed to fetch block"),
            "fetch error was not logged: {logged}"
        );
        assert!(logged.contains("Client Error:"), "{logged}");
        assert!(logged.contains("127.0.0.1"), "{logged}");
        assert!(!logged.contains(RPC_SECRET), "{logged}");
        assert!(!logged.contains(&secret_query), "{logged}");
    }

    use super::BlockFetch;
    use crate::util::ProtocolMessage;
    use solana_client::client_error::ClientError;
    use solana_client::rpc_custom_error::{
        JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
        JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET,
        JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED, JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
    };
    use solana_client::rpc_request::RpcResponseErrorData;
    use solana_program::hash::Hash;
    use solana_program::message::{MessageHeader, VersionedMessage};
    use solana_program::pubkey::Pubkey;
    use solana_sdk::signature::Signature;
    use solana_sdk::transaction::VersionedTransaction;
    use solana_transaction_status::{
        EncodableWithMeta, EncodedTransactionWithStatusMeta, TransactionStatusMeta,
        UiConfirmedBlock,
    };
    use std::io::{Read, Write};
    use std::time::Duration;

    type RecordedCalls = Arc<Mutex<Vec<(RpcRequest, serde_json::Value)>>>;

    struct ScriptedSender {
        response: Mutex<Option<RpcResult<serde_json::Value>>>,
        calls: RecordedCalls,
    }

    impl RpcSender for ScriptedSender {
        fn send<'life0, 'async_trait>(
            &'life0 self,
            request: RpcRequest,
            params: serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = RpcResult<serde_json::Value>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            self.calls
                .lock()
                .expect("rpc recorder")
                .push((request, params));
            let response = self
                .response
                .lock()
                .expect("rpc response")
                .take()
                .expect("scripted response");
            Box::pin(async move { response })
        }

        fn get_transport_stats(&self) -> RpcTransportStats {
            RpcTransportStats::default()
        }

        fn url(&self) -> String {
            "scripted-sender".to_string()
        }
    }

    fn block_config() -> RpcBlockConfig {
        RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            transaction_details: None,
            rewards: None,
            commitment: Some(CommitmentConfig::finalized()),
            max_supported_transaction_version: Some(0),
        }
    }

    fn scripted_client(response: RpcResult<serde_json::Value>) -> (RpcClient, RecordedCalls) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let client = RpcClient::new_sender(
            ScriptedSender {
                response: Mutex::new(Some(response)),
                calls: Arc::clone(&calls),
            },
            RpcClientConfig::with_commitment(CommitmentConfig::finalized()),
        );
        (client, calls)
    }

    fn rpc_failure(code: i64, message: String) -> ClientError {
        solana_client::rpc_request::RpcError::RpcResponseError {
            code,
            message,
            data: RpcResponseErrorData::Empty,
        }
        .into()
    }

    fn fetch_scripted(
        slot: u64,
        response: RpcResult<serde_json::Value>,
    ) -> (
        BlockFetch,
        Vec<(RpcRequest, serde_json::Value)>,
        tokio::sync::mpsc::UnboundedReceiver<ProtocolMessage>,
    ) {
        let (client, calls) = scripted_client(response);
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let outcome = BlockFetcher::request_block(&client, slot, block_config(), sender);
        let calls = calls.lock().expect("rpc recorder").clone();
        (outcome, calls, receiver)
    }

    fn assert_finalized_get_block(calls: &[(RpcRequest, serde_json::Value)], slot: u64) {
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, RpcRequest::GetBlock);
        let params = calls[0].1.as_array().expect("getBlock params");
        assert_eq!(params[0].as_u64(), Some(slot));
        assert_eq!(
            params
                .get(1)
                .and_then(|config| config.get("commitment"))
                .and_then(serde_json::Value::as_str),
            Some("finalized")
        );
    }

    fn sample_transaction() -> (EncodedTransactionWithStatusMeta, String, String) {
        let account = Pubkey::from([4u8; 32]);
        let signature = Signature::from([8u8; 64]);
        let versioned = VersionedTransaction {
            signatures: vec![signature],
            message: VersionedMessage::Legacy(solana_program::message::Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![account],
                recent_blockhash: Hash::new_from_array([1; 32]),
                instructions: vec![],
            }),
        };
        let encoded = EncodedTransactionWithStatusMeta {
            transaction: versioned.encode_with_meta(
                UiTransactionEncoding::Base64,
                &TransactionStatusMeta::default(),
            ),
            meta: Some(
                serde_json::from_value(serde_json::json!({
                    "err": null,
                    "status": {"Ok": null},
                    "fee": 5000,
                    "preBalances": [10],
                    "postBalances": [10],
                }))
                .expect("transaction metadata"),
            ),
            version: Some(solana_sdk::transaction::TransactionVersion::LEGACY),
        };
        (encoded, signature.to_string(), account.to_string())
    }

    fn confirmed_block(transaction: EncodedTransactionWithStatusMeta) -> serde_json::Value {
        serde_json::to_value(UiConfirmedBlock {
            previous_blockhash: "11111111111111111111111111111111".to_string(),
            blockhash: "11111111111111111111111111111111".to_string(),
            parent_slot: 99,
            transactions: Some(vec![transaction]),
            signatures: None,
            rewards: None,
            num_reward_partitions: None,
            block_time: Some(0),
            block_height: Some(42),
        })
        .expect("confirmed block")
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(future)
    }

    #[test]
    fn finalized_get_block_is_fetched_and_processed() {
        let (transaction, signature, account) = sample_transaction();
        let parsed = block_on(async {
            let (outcome, calls, mut receiver) =
                fetch_scripted(100, Ok(confirmed_block(transaction)));
            assert!(
                matches!(outcome, BlockFetch::Fetched(block) if block.block_height == Some(42))
            );
            assert_finalized_get_block(&calls, 100);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                if let Ok(message) = receiver.try_recv() {
                    return message;
                }
                if tokio::time::Instant::now() >= deadline {
                    panic!("fetched block did not enter the parser pipeline");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        match parsed {
            ProtocolMessage::ParsedBlock(block_no, total_chunks, chunk_no, block) => {
                assert_eq!(block_no, 42);
                assert_eq!(total_chunks, 1);
                assert_eq!(chunk_no, 0);
                assert!(block.get_tx_details(&signature).is_some());
                assert_eq!(block.get_account_balance(&account), Some(10));
            }
            other => panic!("expected a parsed block, got {other:?}"),
        }
    }

    #[test]
    fn explicit_slot_skipped_is_not_a_fetched_block() {
        let slot = 500;
        let (outcome, calls, mut receiver) = fetch_scripted(
            slot,
            Err(rpc_failure(
                JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
                format!(
                    "Slot {slot} was skipped, or missing due to ledger jump to recent snapshot"
                ),
            )),
        );
        assert!(matches!(outcome, BlockFetch::Skipped(skipped) if skipped == slot));
        assert_finalized_get_block(&calls, slot);
        assert!(receiver.try_recv().is_err());
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        let (client, _) = scripted_client(Err(rpc_failure(
            JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
            format!("Slot {slot} was skipped, or missing due to ledger jump to recent snapshot"),
        )));
        let logged = capture_fetch_logs(|| {
            log::set_max_level(log::LevelFilter::Warn);
            BlockFetcher::request_block(&client, slot, block_config(), sender);
            log::set_max_level(log::LevelFilter::Error);
        });
        let skipped_line = logged
            .lines()
            .find(|line| line.contains(&format!("Slot {slot} was skipped")))
            .unwrap_or_else(|| panic!("skipped slot was not logged: {logged}"));
        assert!(!skipped_line.contains("Failed to fetch block"));
    }

    #[test]
    fn unavailable_block_is_a_failure() {
        for (code, message) in [
            (
                JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
                "Block not available for slot 100",
            ),
            (
                JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET,
                "Block status not yet available for slot 100",
            ),
            (
                JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED,
                "Slot 100 was skipped, or missing in long-term storage",
            ),
        ] {
            let (outcome, calls, mut receiver) =
                fetch_scripted(100, Err(rpc_failure(code, message.to_string())));
            assert!(
                matches!(outcome, BlockFetch::Failed(_)),
                "code {code} was classified as {outcome:?}"
            );
            assert_finalized_get_block(&calls, 100);
            assert!(receiver.try_recv().is_err());
        }
    }

    #[test]
    fn unknown_rpc_failure_is_not_a_skipped_slot() {
        let (outcome, _calls, mut receiver) =
            fetch_scripted(100, Err(rpc_failure(-32603, "Internal error".to_string())));
        assert!(matches!(outcome, BlockFetch::Failed(_)));
        assert!(receiver.try_recv().is_err());
    }

    fn local_http(status_line: &'static str, hang: bool) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("local http");
        let addr = listener.local_addr().expect("local address");
        std::thread::spawn(move || {
            for _ in 0..8 {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                if hang {
                    std::thread::sleep(Duration::from_secs(2));
                    continue;
                }
                let response = format!(
                    "HTTP/1.1 {status_line}\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        format!("http://{addr}/?api-key={RPC_SECRET}")
    }

    fn assert_failed_without_block_or_secret(outcome: BlockFetch, logged: &str) {
        match outcome {
            BlockFetch::Failed(err) => {
                let rendered = format!("{err:?}");
                assert!(!rendered.contains(RPC_SECRET), "{rendered}");
                assert!(!rendered.contains("api-key="), "{rendered}");
            }
            other => panic!("expected a failed fetch, got {other:?}"),
        }
        assert!(logged.contains("Failed to fetch block"), "{logged}");
        assert!(!logged.contains(RPC_SECRET), "{logged}");
        assert!(!logged.contains("api-key="), "{logged}");
        assert!(!logged.contains("was skipped"), "{logged}");
    }

    #[test]
    fn rate_limited_get_block_is_a_failure() {
        let url = local_http("429 Too Many Requests", false);
        let client = RpcClient::new_with_timeout(url, Duration::from_secs(5));
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut outcome = None;
        let logged = capture_fetch_logs(|| {
            outcome = Some(BlockFetcher::request_block(
                &client,
                100,
                block_config(),
                sender,
            ));
        });
        let outcome = outcome.expect("rate-limited fetch");
        assert_failed_without_block_or_secret(outcome, &logged);
        assert!(logged.contains("429"), "{logged}");
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn timed_out_get_block_is_a_failure() {
        let url = local_http("200 OK", true);
        let client = RpcClient::new_with_timeout(url, Duration::from_millis(200));
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut outcome = None;
        let logged = capture_fetch_logs(|| {
            outcome = Some(BlockFetcher::request_block(
                &client,
                100,
                block_config(),
                sender,
            ));
        });
        let outcome = outcome.expect("timed out fetch");
        assert_failed_without_block_or_secret(outcome, &logged);
        let lower = logged.to_ascii_lowercase();
        assert!(lower.contains("timedout"), "{logged}");
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn transport_failure_is_not_indexed() {
        let client = RpcClient::new_with_timeout(
            format!("http://127.0.0.1:1/?api-key={RPC_SECRET}"),
            Duration::from_secs(2),
        );
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut outcome = None;
        let logged = capture_fetch_logs(|| {
            outcome = Some(BlockFetcher::request_block(
                &client,
                100,
                block_config(),
                sender,
            ));
        });
        let outcome = outcome.expect("transport failure");
        assert_failed_without_block_or_secret(outcome, &logged);
        assert!(logged.contains("127.0.0.1"), "{logged}");
        assert!(receiver.try_recv().is_err());
    }
}
