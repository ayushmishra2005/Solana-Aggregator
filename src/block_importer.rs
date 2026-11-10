use crate::error::AggError;
use crate::parser::Parser;
use crate::util::ProtocolMessage;
use log::{error, warn};
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::RpcBlockConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::UiTransactionEncoding;
use tokio::sync::mpsc::UnboundedSender;

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
                Self::request_block(&client, latest_slot, rpc_block_config, sender);
            }
            _ => {}
        }
    }

    fn request_block(
        client: &RpcClient,
        slot: u64,
        rpc_block_config: RpcBlockConfig,
        sender: UnboundedSender<ProtocolMessage>,
    ) {
        match client.get_block_with_config(slot, rpc_block_config) {
            Ok(block) => {
                if let Some(block_no) = block.block_height {
                    if let Some(txs) = block.transactions {
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
            Err(err) => {
                error!(
                    target: "subscriber",
                    "Failed to fetch block {:?}",
                    AggError::from(err)
                );
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
}
