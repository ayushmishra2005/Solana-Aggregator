use crate::error::AggError;
use crate::parser::Parser;
use crate::util::{ProtocolMessage, PARSER_CONCURRENCY};
use log::{error, warn};
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcBlockConfig;
use solana_client::rpc_custom_error::{
    JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
    JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET, JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
};
use solana_client::rpc_request::RpcError;
use solana_commitment_config::CommitmentConfig;
use solana_transaction_status::{
    EncodedTransactionWithStatusMeta, UiConfirmedBlock, UiTransactionEncoding,
};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

const MAX_GET_BLOCK_ATTEMPTS: u32 = 3;
const INITIAL_GET_BLOCK_BACKOFF: Duration = Duration::from_millis(200);
const MAX_GET_BLOCK_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Debug)]
enum BlockFetch {
    Fetched(UiConfirmedBlock),
    Skipped(u64),
    Failed { error: AggError, retryable: bool },
}

#[derive(Debug)]
enum SlotProgress {
    CaughtUp,
    Advanced,
    Halted(String),
    Retry { slot: u64, reason: String },
    SlotQueryFailed,
}

fn finalized_block_config() -> RpcBlockConfig {
    RpcBlockConfig {
        encoding: Some(UiTransactionEncoding::Base64),
        transaction_details: None,
        rewards: None,
        commitment: Some(CommitmentConfig::finalized()),
        max_supported_transaction_version: Some(1),
    }
}

struct ParseJob {
    fetch_slot: u64,
    block_no: u64,
    chunk_index: u64,
    total_chunks: u64,
    transactions: Vec<EncodedTransactionWithStatusMeta>,
    handler_sender: Sender<ProtocolMessage>,
    outcome_sender: Option<Sender<ProtocolMessage>>,
}

#[cfg(test)]
struct ParserGate {
    hold: std::sync::atomic::AtomicBool,
    in_flight: std::sync::atomic::AtomicUsize,
    max_in_flight: std::sync::atomic::AtomicUsize,
    completed: std::sync::atomic::AtomicUsize,
    release: tokio::sync::Notify,
}

#[cfg(test)]
static PARSER_GATE: ParserGate = ParserGate {
    hold: std::sync::atomic::AtomicBool::new(false),
    in_flight: std::sync::atomic::AtomicUsize::new(0),
    max_in_flight: std::sync::atomic::AtomicUsize::new(0),
    completed: std::sync::atomic::AtomicUsize::new(0),
    release: tokio::sync::Notify::const_new(),
};

fn parser_channel() -> (Sender<ParseJob>, tokio::sync::mpsc::Receiver<ParseJob>) {
    tokio::sync::mpsc::channel(PARSER_CONCURRENCY)
}

fn spawn_parser_workers(
    receiver: tokio::sync::mpsc::Receiver<ParseJob>,
    stopping: Arc<std::sync::atomic::AtomicBool>,
) -> Vec<JoinHandle<()>> {
    let receiver = Arc::new(Mutex::new(receiver));
    let mut workers = Vec::with_capacity(PARSER_CONCURRENCY);
    for _ in 0..PARSER_CONCURRENCY {
        let receiver = Arc::clone(&receiver);
        let stopping = Arc::clone(&stopping);
        workers.push(tokio::spawn(async move {
            loop {
                let job = {
                    let mut queue = receiver.lock().await;
                    queue.recv().await
                };
                let Some(job) = job else {
                    log::info!(target: "subscriber", "Parser worker stopped");
                    break;
                };
                #[cfg(test)]
                if PARSER_GATE.hold.load(std::sync::atomic::Ordering::SeqCst) {
                    let current = PARSER_GATE
                        .in_flight
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                        + 1;
                    PARSER_GATE
                        .max_in_flight
                        .fetch_max(current, std::sync::atomic::Ordering::SeqCst);
                    PARSER_GATE.release.notified().await;
                    PARSER_GATE
                        .in_flight
                        .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                }
                run_parse_job(job, &stopping).await;
                #[cfg(test)]
                {
                    PARSER_GATE
                        .completed
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }));
    }
    workers
}

#[cfg(test)]
fn start_parser_workers() -> Sender<ParseJob> {
    let (sender, receiver) = parser_channel();
    spawn_parser_workers(
        receiver,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    );
    sender
}

async fn run_parse_job(job: ParseJob, stopping: &std::sync::atomic::AtomicBool) {
    let ParseJob {
        fetch_slot,
        block_no,
        chunk_index,
        total_chunks,
        transactions,
        handler_sender,
        outcome_sender,
    } = job;
    if let Err(error) = Parser::invoke(ProtocolMessage::new_chuck(
        block_no,
        chunk_index,
        total_chunks,
        transactions,
        handler_sender.clone(),
    ))
    .await
    {
        if stopping.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        error!(target: "subscriber", "Error from Parser {}", error);
        let reported = handler_sender
            .send(ProtocolMessage::ChunkFailed(
                block_no,
                fetch_slot,
                error.to_string(),
            ))
            .await
            .is_ok();
        if !reported {
            if let Some(outcome) = outcome_sender {
                let _ = outcome
                    .send(ProtocolMessage::BlockProcessingResult(
                        fetch_slot,
                        Err(error.to_string()),
                    ))
                    .await;
            }
        }
    }
}

pub struct Subscriber {
    latest_slot: u64,
    chain_url: String,
    rpc_client: RpcClient,
    rpc_block_config: RpcBlockConfig,
    handler_sender: Sender<ProtocolMessage>,
    parser_jobs: Sender<ParseJob>,
    parser_workers: Vec<JoinHandle<()>>,
    parser_inbox: Option<tokio::sync::mpsc::Receiver<ParseJob>>,
    stopping: Arc<std::sync::atomic::AtomicBool>,
    runtime: Option<Arc<crate::lifecycle::Lifecycle>>,
    outcome_tx: Option<Sender<ProtocolMessage>>,
    outcome_rx: Option<tokio::sync::mpsc::Receiver<ProtocolMessage>>,
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
    pub async fn initialize(
        chain_url: String,
        message_sender: Sender<ProtocolMessage>,
        stopping: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Self, AggError> {
        let rpc_client = RpcClient::new(chain_url.clone());
        log::info!(target: "subscriber", "Solana RPC client initialized");
        let rpc_block_config = finalized_block_config();
        let latest_slot = rpc_client
            .get_slot_with_commitment(CommitmentConfig::finalized())
            .await?;
        let (parser_jobs, parser_inbox) = parser_channel();
        let parser_workers = spawn_parser_workers(parser_inbox, Arc::clone(&stopping));
        Ok(Self {
            latest_slot,
            chain_url,
            rpc_client,
            rpc_block_config,
            handler_sender: message_sender,
            parser_jobs,
            parser_workers,
            parser_inbox: None,
            stopping,
            runtime: None,
            outcome_tx: None,
            outcome_rx: None,
        })
    }

    pub fn resume_from_checkpoint(&mut self, slot: u64) {
        self.latest_slot = slot;
    }

    pub fn completed_slot(&self) -> u64 {
        self.latest_slot
    }

    pub fn set_block_outcomes(
        &mut self,
        sender: Sender<ProtocolMessage>,
        receiver: tokio::sync::mpsc::Receiver<ProtocolMessage>,
    ) {
        self.outcome_tx = Some(sender);
        self.outcome_rx = Some(receiver);
    }

    pub fn attach_runtime(&mut self, runtime: Arc<crate::lifecycle::Lifecycle>) {
        self.runtime = Some(runtime);
    }

    pub async fn shutdown_workers(&mut self, budget: std::time::Duration) {
        let (replacement, inbox) = parser_channel();
        drop(inbox);
        drop(std::mem::replace(&mut self.parser_jobs, replacement));
        let workers = std::mem::take(&mut self.parser_workers);
        let deadline = tokio::time::Instant::now() + budget;
        let mut unresolved = Vec::new();
        for mut worker in workers {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if tokio::time::timeout(remaining, &mut worker).await.is_err() && !worker.is_finished()
            {
                unresolved.push(worker);
                continue;
            }
        }
        if unresolved.is_empty() {
            log::info!(target: "subscriber", "Parser workers stopped");
            return;
        }
        log::error!(
            target: "subscriber",
            "Shutdown deadline exceeded with {} parser workers still running",
            unresolved.len()
        );
        for worker in unresolved {
            worker.abort();
        }
    }

    async fn fetch_latest_slot(&self) -> Result<u64, AggError> {
        let slot = self
            .rpc_client
            .get_slot_with_commitment(CommitmentConfig::finalized())
            .await?;
        if let Some(runtime) = &self.runtime {
            runtime.note_finalized_tip(slot);
        }
        Ok(slot)
    }

    /// Runs until the finalized tip is caught up, or returns the fatal reason.
    /// A returned error means the in-memory cursor did not advance.
    #[cfg(test)]
    pub async fn run(&mut self) -> Result<(), String> {
        let (_sender, receiver) = tokio::sync::watch::channel(false);
        self.run_with_shutdown(receiver).await
    }

    pub async fn run_with_shutdown(
        &mut self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), String> {
        log::info!(
            target: "subscriber",
            "Indexing started from checkpoint slot {}",
            self.latest_slot
        );
        loop {
            if *shutdown.borrow() || self.stopping.load(std::sync::atomic::Ordering::SeqCst) {
                log::info!(target: "subscriber", "Subscriber stopped scheduling new slots");
                return Ok(());
            }
            match self.advance_once().await {
                SlotProgress::Halted(reason) => {
                    if self.stopping.load(std::sync::atomic::Ordering::SeqCst) {
                        log::info!(
                            target: "subscriber",
                            "Subscriber channel closed during shutdown at slot {}",
                            self.latest_slot
                        );
                        return Ok(());
                    }
                    error!(target: "subscriber", "{reason}");
                    return Err(reason);
                }
                SlotProgress::CaughtUp => {
                    if let Some(runtime) = &self.runtime {
                        runtime.set_phase(crate::lifecycle::ServicePhase::CaughtUp);
                    }
                }
                SlotProgress::Advanced => {
                    if let Some(runtime) = &self.runtime {
                        runtime.set_phase(crate::lifecycle::ServicePhase::Indexing);
                    }
                }
                SlotProgress::Retry { slot, reason } => {
                    warn!(
                        target: "subscriber",
                        "getBlock for slot {slot} is still unavailable after {MAX_GET_BLOCK_ATTEMPTS} attempts; retrying the same slot without moving the checkpoint: {reason}"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(MAX_GET_BLOCK_BACKOFF) => {}
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                log::info!(target: "subscriber", "Subscriber stopped scheduling new slots");
                                return Ok(());
                            }
                        }
                    }
                }
                SlotProgress::SlotQueryFailed => {
                    tokio::select! {
                        _ = tokio::time::sleep(MAX_GET_BLOCK_BACKOFF) => {}
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                log::info!(target: "subscriber", "Subscriber stopped scheduling new slots");
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }

    /// Fetches the next finalized slot and advances `latest_slot` only after
    /// a fetched block or an authoritative skip. A failed fetch leaves the cursor
    /// on the last progressed slot.
    async fn advance_once(&mut self) -> SlotProgress {
        if let Some(inbox) = self.parser_inbox.take() {
            self.parser_workers
                .extend(spawn_parser_workers(inbox, Arc::clone(&self.stopping)));
        }
        let fetched_slot = match self.fetch_latest_slot().await {
            Ok(slot) => slot,
            Err(err) => {
                error!(target: "subscriber", "Failed to fetch latest slot {:?}", err);
                return SlotProgress::SlotQueryFailed;
            }
        };
        if self.latest_slot >= fetched_slot {
            return SlotProgress::CaughtUp;
        }
        let slot = self.latest_slot.saturating_add(1);
        let outcome = BlockFetcher::invoke(
            &self.rpc_client,
            ProtocolMessage::fetch_block(
                self.chain_url.clone(),
                self.rpc_block_config,
                slot,
                self.handler_sender.clone(),
            ),
            self.outcome_tx.as_ref(),
            &self.parser_jobs,
        )
        .await;
        match outcome {
            BlockFetch::Skipped(_) => {
                if self
                    .handler_sender
                    .send(ProtocolMessage::RecordSkippedSlot(slot))
                    .await
                    .is_err()
                {
                    return SlotProgress::Halted(format!(
                        "Stopping slot progression at {slot}: database channel closed"
                    ));
                }
                match self.wait_for_block(slot).await {
                    Ok(()) => {
                        self.latest_slot = slot;
                        SlotProgress::Advanced
                    }
                    Err(err) => SlotProgress::Halted(format!(
                        "Stopping slot progression at {slot} after skipped-slot checkpoint failed: {err}"
                    )),
                }
            }
            BlockFetch::Fetched(_) => match self.wait_for_block(slot).await {
                Ok(()) => {
                    self.latest_slot = slot;
                    SlotProgress::Advanced
                }
                Err(err) => SlotProgress::Halted(format!(
                    "Stopping slot progression at {slot} after block processing failed: {err}"
                )),
            },
            BlockFetch::Failed { error, retryable } if retryable => SlotProgress::Retry {
                slot,
                reason: error.to_string(),
            },
            BlockFetch::Failed { error, .. } => SlotProgress::Halted(format!(
                "Stopping slot progression at {slot} after getBlock failed: {error:?}"
            )),
        }
    }

    async fn wait_for_block(&mut self, fetch_slot: u64) -> Result<(), String> {
        let held_sender = self.outcome_tx.take();
        let result = self.read_block_result(fetch_slot).await;
        self.outcome_tx = held_sender;
        result
    }

    async fn read_block_result(&mut self, fetch_slot: u64) -> Result<(), String> {
        let Some(receiver) = self.outcome_rx.as_mut() else {
            return Err("block outcome channel is not connected".to_string());
        };
        loop {
            match receiver.recv().await {
                None => return Err("block outcome channel closed".to_string()),
                Some(ProtocolMessage::BlockProcessingResult(slot, Ok(())))
                    if slot == fetch_slot =>
                {
                    return Ok(());
                }
                Some(ProtocolMessage::BlockProcessingResult(slot, Err(err)))
                    if slot == fetch_slot =>
                {
                    return Err(err);
                }
                Some(ProtocolMessage::BlockProcessingResult(_, _)) => {}
                Some(_) => {}
            }
        }
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        for worker in self.parser_workers.drain(..) {
            worker.abort();
        }
    }
}

struct BlockFetcher;

impl BlockFetcher {
    async fn invoke(
        client: &RpcClient,
        message: ProtocolMessage,
        outcome_sender: Option<&Sender<ProtocolMessage>>,
        parser_jobs: &Sender<ParseJob>,
    ) -> BlockFetch {
        match message {
            ProtocolMessage::FetchBlock(_chain_url, rpc_block_config, slot, sender) => {
                Self::request_block(
                    client,
                    slot,
                    rpc_block_config,
                    sender,
                    outcome_sender,
                    parser_jobs,
                )
                .await
            }
            _ => BlockFetch::Failed {
                error: AggError::ClientError(ClientError::from(ClientErrorKind::Custom(
                    "block fetcher received an unexpected message".to_string(),
                ))),
                retryable: false,
            },
        }
    }

    async fn request_block(
        client: &RpcClient,
        slot: u64,
        rpc_block_config: RpcBlockConfig,
        sender: Sender<ProtocolMessage>,
        outcome_sender: Option<&Sender<ProtocolMessage>>,
        parser_jobs: &Sender<ParseJob>,
    ) -> BlockFetch {
        Self::request_block_retrying(
            client,
            slot,
            rpc_block_config,
            sender,
            outcome_sender,
            parser_jobs,
            Self::pause,
        )
        .await
    }

    async fn request_block_retrying<P, F>(
        client: &RpcClient,
        slot: u64,
        rpc_block_config: RpcBlockConfig,
        sender: Sender<ProtocolMessage>,
        outcome_sender: Option<&Sender<ProtocolMessage>>,
        parser_jobs: &Sender<ParseJob>,
        mut pause: P,
    ) -> BlockFetch
    where
        P: FnMut(Duration) -> F,
        F: Future<Output = ()>,
    {
        for attempt in 1..=MAX_GET_BLOCK_ATTEMPTS {
            match client.get_block_with_config(slot, rpc_block_config).await {
                Ok(block) => {
                    let outcome = BlockFetch::Fetched(block);
                    Self::dispatch_block_fetch(
                        slot,
                        &outcome,
                        &sender,
                        outcome_sender,
                        parser_jobs,
                    )
                    .await;
                    return outcome;
                }
                Err(err) if Self::is_explicit_slot_skip(&err) => {
                    let outcome = BlockFetch::Skipped(slot);
                    Self::dispatch_block_fetch(
                        slot,
                        &outcome,
                        &sender,
                        outcome_sender,
                        parser_jobs,
                    )
                    .await;
                    return outcome;
                }
                Err(err)
                    if attempt < MAX_GET_BLOCK_ATTEMPTS && Self::is_retryable_block_fetch(&err) =>
                {
                    let delay = Self::backoff_before_next_attempt(attempt);
                    let sanitized = AggError::from(err);
                    warn!(
                        target: "subscriber",
                        "Retrying getBlock for slot {slot} after attempt {attempt} of {MAX_GET_BLOCK_ATTEMPTS} in {delay:?}: {sanitized:?}"
                    );
                    pause(delay).await;
                }
                Err(err) => {
                    let retryable = Self::is_transport_retry(&err);
                    let outcome = BlockFetch::Failed {
                        error: AggError::from(err),
                        retryable,
                    };
                    Self::dispatch_block_fetch(
                        slot,
                        &outcome,
                        &sender,
                        outcome_sender,
                        parser_jobs,
                    )
                    .await;
                    return outcome;
                }
            }
        }
        unreachable!("getBlock retry loop returns on every attempt");
    }

    async fn pause(delay: Duration) {
        tokio::time::sleep(delay).await;
    }

    /// Delay before the next attempt after `failed_attempt` (1-based).
    /// Attempt 1 waits 200ms, attempt 2 waits 400ms, later waits cap at 1s.
    fn backoff_before_next_attempt(failed_attempt: u32) -> Duration {
        let shift = failed_attempt.saturating_sub(1).min(16);
        let multiplier = 1u64 << shift;
        let millis = INITIAL_GET_BLOCK_BACKOFF.as_millis() as u64;
        Duration::from_millis(
            millis
                .saturating_mul(multiplier)
                .min(MAX_GET_BLOCK_BACKOFF.as_millis() as u64),
        )
    }

    fn is_retryable_block_fetch(err: &ClientError) -> bool {
        match err.kind() {
            ClientErrorKind::RpcError(RpcError::RpcResponseError { code, .. }) => {
                *code == JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE
                    || *code == JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET
            }
            _ => Self::is_transport_retry(err),
        }
    }

    /// Timeouts, connection failures, and HTTP 429 are retried after the
    /// three-attempt budget. A concrete "block not available" answer is not.
    fn is_transport_retry(err: &ClientError) -> bool {
        match err.kind() {
            ClientErrorKind::Reqwest(err) => Self::reqwest_is_transient(err),
            ClientErrorKind::Io(err) => Self::io_is_transient(err),
            _ => false,
        }
    }

    fn reqwest_is_transient(err: &solana_client::client_error::reqwest::Error) -> bool {
        err.is_timeout()
            || err.is_connect()
            || err.status()
                == Some(solana_client::client_error::reqwest::StatusCode::TOO_MANY_REQUESTS)
            || Self::error_source_is_transient_io(std::error::Error::source(err))
    }

    fn error_source_is_transient_io(source: Option<&(dyn std::error::Error + 'static)>) -> bool {
        let mut source = source;
        while let Some(err) = source {
            if let Some(io) = err.downcast_ref::<std::io::Error>() {
                return Self::io_is_transient(io);
            }
            source = err.source();
        }
        false
    }

    fn io_is_transient(err: &std::io::Error) -> bool {
        matches!(
            err.kind(),
            std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::Interrupted
                | std::io::ErrorKind::UnexpectedEof
        )
    }

    /// Solana JSON-RPC code -32007 (`SlotSkipped`) is the only explicit skip.
    fn is_explicit_slot_skip(err: &ClientError) -> bool {
        matches!(
            err.kind(),
            ClientErrorKind::RpcError(RpcError::RpcResponseError { code, .. })
                if *code == JSON_RPC_SERVER_ERROR_SLOT_SKIPPED
        )
    }

    async fn dispatch_block_fetch(
        fetch_slot: u64,
        outcome: &BlockFetch,
        sender: &Sender<ProtocolMessage>,
        outcome_sender: Option<&Sender<ProtocolMessage>>,
        parser_jobs: &Sender<ParseJob>,
    ) {
        match outcome {
            BlockFetch::Fetched(block) => {
                if let Some(block_no) = block.block_height {
                    if let Some(txs) = block.transactions.as_deref() {
                        let chunks = txs.chunks(10);
                        let len_of_chunks = chunks.len() as u64;
                        if len_of_chunks == 0 {
                            if sender
                                .send(ProtocolMessage::StoreEmptyBlock(block_no, fetch_slot))
                                .await
                                .is_err()
                            {
                                Self::report_processing(
                                    outcome_sender,
                                    fetch_slot,
                                    Err("database channel closed".to_string()),
                                )
                                .await;
                            }
                            return;
                        }
                        if sender
                            .send(ProtocolMessage::ExpectBlock(
                                fetch_slot,
                                block_no,
                                len_of_chunks,
                            ))
                            .await
                            .is_err()
                        {
                            Self::report_processing(
                                outcome_sender,
                                fetch_slot,
                                Err("database channel closed".to_string()),
                            )
                            .await;
                            return;
                        }
                        for (index, chunk) in chunks.enumerate() {
                            if parser_jobs
                                .send(ParseJob {
                                    fetch_slot,
                                    block_no,
                                    chunk_index: index as u64,
                                    total_chunks: len_of_chunks,
                                    transactions: chunk.to_vec(),
                                    handler_sender: sender.clone(),
                                    outcome_sender: outcome_sender.cloned(),
                                })
                                .await
                                .is_err()
                            {
                                Self::report_processing(
                                    outcome_sender,
                                    fetch_slot,
                                    Err("parser workers stopped".to_string()),
                                )
                                .await;
                                return;
                            }
                        }
                    } else {
                        Self::report_processing(
                            outcome_sender,
                            fetch_slot,
                            Err("fetched block did not include transactions".to_string()),
                        )
                        .await;
                    }
                } else {
                    warn!(target: "subscriber", "Block Number not available");
                    Self::report_processing(
                        outcome_sender,
                        fetch_slot,
                        Err("fetched block has no block height".to_string()),
                    )
                    .await;
                }
            }
            BlockFetch::Skipped(slot) => {
                warn!(target: "subscriber", "Slot {slot} was skipped");
            }
            BlockFetch::Failed { error, retryable } => {
                error!(
                    target: "subscriber",
                    "Failed to fetch block at slot {fetch_slot} after retry exhaustion: {error:?}"
                );
                if *retryable {
                    warn!(
                        target: "subscriber",
                        "Slot {fetch_slot} stays uncheckpointed and will be retried"
                    );
                }
            }
        }
    }

    async fn report_processing(
        outcome_sender: Option<&Sender<ProtocolMessage>>,
        fetch_slot: u64,
        result: Result<(), String>,
    ) {
        let Some(sender) = outcome_sender else {
            return;
        };
        let _ = sender
            .send(ProtocolMessage::BlockProcessingResult(fetch_slot, result))
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::BlockFetcher;
    use solana_client::client_error::Result as RpcResult;
    use solana_client::nonblocking::rpc_client::RpcClient;
    use solana_client::rpc_client::RpcClientConfig;
    use solana_client::rpc_config::RpcBlockConfig;
    use solana_client::rpc_request::RpcRequest;
    use solana_client::rpc_sender::{RpcSender, RpcTransportStats};
    use solana_commitment_config::CommitmentConfig;
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
        let (sender, _receiver) = tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
        let config = super::finalized_block_config();

        block_on(async {
            let jobs = super::start_parser_workers();
            BlockFetcher::request_block(&client, slot, config, sender, None, &jobs).await
        });

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
        assert_eq!(
            params
                .get(1)
                .and_then(|config| config.get("maxSupportedTransactionVersion"))
                .and_then(serde_json::Value::as_u64),
            Some(1),
            "getBlock must request transaction version 1 as a JSON integer"
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
            max_supported_transaction_version: Some(1),
        };
        let raw = block_on(client.get_block_with_config(100, config))
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

        let (sender, _receiver) = tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
        let logged = capture_fetch_logs(|| {
            log::set_max_level(log::LevelFilter::Warn);
            block_on(async {
                let jobs = super::start_parser_workers();
                BlockFetcher::request_block_retrying(
                    &client,
                    100,
                    config,
                    sender,
                    None,
                    &jobs,
                    |_delay| futures_util::future::ready(()),
                )
                .await;
            });
            log::set_max_level(log::LevelFilter::Error);
        });
        assert!(
            logged.contains("Retrying getBlock"),
            "retry was not logged: {logged}"
        );
        assert!(
            logged.contains("Failed to fetch block"),
            "fetch error was not logged: {logged}"
        );
        assert!(logged.contains("Client Error:"), "{logged}");
        assert!(logged.contains("127.0.0.1"), "{logged}");
        assert!(!logged.contains(RPC_SECRET), "{logged}");
        assert!(!logged.contains(&secret_query), "{logged}");
    }

    use super::{BlockFetch, SlotProgress, Subscriber};
    use crate::util::ProtocolMessage;
    use solana_client::client_error::ClientError;
    use solana_client::rpc_custom_error::{
        JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
        JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET,
        JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED, JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
        JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION,
    };
    use solana_client::rpc_request::RpcResponseErrorData;
    use solana_hash::Hash;
    use solana_message::{MessageHeader, VersionedMessage};
    use solana_pubkey::Pubkey;
    use solana_signature::Signature;
    use solana_transaction::versioned::{TransactionVersion, VersionedTransaction};
    use solana_transaction_status::{
        EncodableWithMeta, EncodedTransactionWithStatusMeta, TransactionStatusMeta,
        UiConfirmedBlock,
    };
    use std::collections::VecDeque;
    use std::io::{Read, Write};
    use std::time::Duration;

    type RecordedCalls = Arc<Mutex<Vec<(RpcRequest, serde_json::Value)>>>;

    struct ScriptedSender {
        responses: Mutex<VecDeque<RpcResult<serde_json::Value>>>,
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
                .responses
                .lock()
                .expect("rpc response")
                .pop_front()
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
        super::finalized_block_config()
    }

    fn scripted_client(responses: Vec<RpcResult<serde_json::Value>>) -> (RpcClient, RecordedCalls) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let client = RpcClient::new_sender(
            ScriptedSender {
                responses: Mutex::new(VecDeque::from(responses)),
                calls: Arc::clone(&calls),
            },
            RpcClientConfig::with_commitment(CommitmentConfig::finalized()),
        );
        (client, calls)
    }

    struct FetchTrial {
        outcome: BlockFetch,
        calls: Vec<(RpcRequest, serde_json::Value)>,
        receiver: tokio::sync::mpsc::Receiver<ProtocolMessage>,
        delays: Vec<Duration>,
    }

    async fn fetch_responses(
        slot: u64,
        responses: Vec<RpcResult<serde_json::Value>>,
    ) -> FetchTrial {
        let (client, calls) = scripted_client(responses);
        let (sender, receiver) = tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
        let delays = Arc::new(Mutex::new(Vec::new()));
        let delay_log = Arc::clone(&delays);
        let jobs = super::start_parser_workers();
        let outcome = BlockFetcher::request_block_retrying(
            &client,
            slot,
            block_config(),
            sender,
            None,
            &jobs,
            move |delay| {
                delay_log.lock().expect("backoff log").push(delay);
                futures_util::future::ready(())
            },
        )
        .await;
        let calls = calls.lock().expect("rpc recorder").clone();
        let delays = delays.lock().expect("backoff log").clone();
        FetchTrial {
            outcome,
            calls,
            receiver,
            delays,
        }
    }

    async fn fetch_scripted(slot: u64, response: RpcResult<serde_json::Value>) -> FetchTrial {
        fetch_responses(slot, vec![response]).await
    }

    fn rpc_failure(code: i64, message: String) -> ClientError {
        solana_client::rpc_request::RpcError::RpcResponseError {
            code,
            message,
            data: RpcResponseErrorData::Empty,
        }
        .into()
    }

    fn assert_finalized_attempts(
        calls: &[(RpcRequest, serde_json::Value)],
        slot: u64,
        attempts: usize,
    ) {
        assert_eq!(calls.len(), attempts);
        for call in calls {
            assert_eq!(call.0, RpcRequest::GetBlock);
            let params = call.1.as_array().expect("getBlock params");
            assert_eq!(params[0].as_u64(), Some(slot));
            assert_eq!(
                params
                    .get(1)
                    .and_then(|config| config.get("commitment"))
                    .and_then(serde_json::Value::as_str),
                Some("finalized")
            );
        }
    }

    fn assert_finalized_get_block(calls: &[(RpcRequest, serde_json::Value)], slot: u64) {
        assert_finalized_attempts(calls, slot, 1);
    }

    fn sample_transaction() -> (EncodedTransactionWithStatusMeta, String, String) {
        let account = Pubkey::from([4u8; 32]);
        let signature = Signature::from([8u8; 64]);
        let versioned = VersionedTransaction {
            signatures: vec![signature],
            message: VersionedMessage::Legacy(solana_message::Message {
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
            version: Some(TransactionVersion::LEGACY),
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
            let mut trial = fetch_scripted(100, Ok(confirmed_block(transaction))).await;
            assert!(
                matches!(trial.outcome, BlockFetch::Fetched(block) if block.block_height == Some(42))
            );
            assert!(trial.delays.is_empty());
            assert_finalized_get_block(&trial.calls, 100);
            wait_for_one_parsed(&mut trial.receiver).await
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
        let mut trial = block_on(fetch_scripted(
            slot,
            Err(rpc_failure(
                JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
                format!(
                    "Slot {slot} was skipped, or missing due to ledger jump to recent snapshot"
                ),
            )),
        ));
        assert!(matches!(trial.outcome, BlockFetch::Skipped(skipped) if skipped == slot));
        assert!(trial.delays.is_empty());
        assert_finalized_get_block(&trial.calls, slot);
        assert!(trial.receiver.try_recv().is_err());
        let (sender, _receiver) = tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
        let (client, _) = scripted_client(vec![Err(rpc_failure(
            JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
            format!("Slot {slot} was skipped, or missing due to ledger jump to recent snapshot"),
        ))]);
        let logged = capture_fetch_logs(|| {
            log::set_max_level(log::LevelFilter::Warn);
            block_on(async {
                let jobs = super::start_parser_workers();
                BlockFetcher::request_block(&client, slot, block_config(), sender, None, &jobs)
                    .await
            });
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
        for code in [
            JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
            JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET,
        ] {
            let mut responses = Vec::new();
            for _ in 0..3 {
                responses.push(Err(rpc_failure(code, format!("transient {code}"))));
            }
            let mut trial = block_on(fetch_responses(100, responses));
            assert!(matches!(trial.outcome, BlockFetch::Failed { .. }));
            assert_finalized_attempts(&trial.calls, 100, 3);
            assert_eq!(
                trial.delays,
                vec![Duration::from_millis(200), Duration::from_millis(400)]
            );
            assert!(trial.receiver.try_recv().is_err());
        }

        let mut trial = block_on(fetch_scripted(
            100,
            Err(rpc_failure(
                JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED,
                "Slot 100 was skipped, or missing in long-term storage".to_string(),
            )),
        ));
        assert!(matches!(trial.outcome, BlockFetch::Failed { .. }));
        assert!(trial.delays.is_empty());
        assert_finalized_get_block(&trial.calls, 100);
        assert!(trial.receiver.try_recv().is_err());
    }

    #[test]
    fn unknown_rpc_failure_is_not_a_skipped_slot() {
        let mut trial = block_on(fetch_scripted(
            100,
            Err(rpc_failure(-32602, "Invalid params".to_string())),
        ));
        assert!(matches!(trial.outcome, BlockFetch::Failed { .. }));
        assert!(trial.delays.is_empty());
        assert_finalized_get_block(&trial.calls, 100);
        assert!(trial.receiver.try_recv().is_err());
    }

    fn local_http(status_line: &'static str, hang: bool) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("local http");
        let addr = listener.local_addr().expect("local address");
        std::thread::spawn(move || {
            for _ in 0..24 {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                if hang {
                    std::thread::spawn(move || {
                        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                        let mut buf = [0u8; 1024];
                        let _ = stream.read(&mut buf);
                        std::thread::sleep(Duration::from_secs(2));
                    });
                    continue;
                }
                let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
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
            BlockFetch::Failed { error: err, .. } => {
                let rendered = format!("{err:?}");
                assert!(!rendered.contains(RPC_SECRET), "{rendered}");
                assert!(!rendered.contains("api-key="), "{rendered}");
            }
            other => panic!("expected a failed fetch, got {other:?}"),
        }
        assert!(logged.contains("Failed to fetch block"), "{logged}");
        assert!(!logged.contains(RPC_SECRET), "{logged}");
        assert!(!logged.contains("api-key="), "{logged}");
        assert!(
            logged
                .lines()
                .filter(|line| {
                    line.contains("Failed to fetch block") || line.contains("Retrying getBlock")
                })
                .all(|line| !line.contains("was skipped")),
            "{logged}"
        );
    }

    #[test]
    fn rate_limited_get_block_is_a_failure() {
        let url = local_http("429 Too Many Requests", false);
        let client = RpcClient::new_with_timeout(url, Duration::from_secs(5));
        let (sender, mut receiver) = tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
        let mut outcome = None;
        let logged = capture_fetch_logs(|| {
            log::set_max_level(log::LevelFilter::Warn);
            outcome = Some(block_on(async {
                let jobs = super::start_parser_workers();
                BlockFetcher::request_block_retrying(
                    &client,
                    100,
                    block_config(),
                    sender,
                    None,
                    &jobs,
                    |_delay| futures_util::future::ready(()),
                )
                .await
            }));
            log::set_max_level(log::LevelFilter::Error);
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
        let (sender, mut receiver) = tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
        let mut outcome = None;
        let logged = capture_fetch_logs(|| {
            outcome = Some(block_on(async {
                let jobs = super::start_parser_workers();
                BlockFetcher::request_block_retrying(
                    &client,
                    100,
                    block_config(),
                    sender,
                    None,
                    &jobs,
                    |_delay| futures_util::future::ready(()),
                )
                .await
            }));
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
        let (sender, mut receiver) = tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
        let mut outcome = None;
        let logged = capture_fetch_logs(|| {
            log::set_max_level(log::LevelFilter::Warn);
            outcome = Some(block_on(async {
                let jobs = super::start_parser_workers();
                BlockFetcher::request_block_retrying(
                    &client,
                    100,
                    block_config(),
                    sender,
                    None,
                    &jobs,
                    |_delay| futures_util::future::ready(()),
                )
                .await
            }));
            log::set_max_level(log::LevelFilter::Error);
        });
        let outcome = outcome.expect("transport failure");
        assert!(
            logged.contains("Retrying getBlock"),
            "transport failure was not retried: {logged}"
        );
        assert_failed_without_block_or_secret(outcome, &logged);
        assert!(logged.contains("127.0.0.1"), "{logged}");
        assert!(receiver.try_recv().is_err());
    }

    fn live_get_block_error(url: String, timeout: Duration) -> ClientError {
        let client = RpcClient::new_with_timeout(url, timeout);
        block_on(client.get_block_with_config(100, block_config()))
            .expect_err("live getBlock failure")
    }

    async fn wait_for_one_parsed(
        receiver: &mut tokio::sync::mpsc::Receiver<ProtocolMessage>,
    ) -> ProtocolMessage {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut parsed = None;
        loop {
            match receiver.try_recv() {
                Ok(message @ ProtocolMessage::ParsedBlock(_, _, _, _)) => {
                    if parsed.is_some() {
                        panic!("block was processed more than once");
                    }
                    parsed = Some(message);
                }
                Ok(_) => {}
                Err(_) if parsed.is_some() => return parsed.expect("parsed block"),
                Err(_) if tokio::time::Instant::now() >= deadline => {
                    panic!("fetched block did not enter the parser pipeline");
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    }

    #[test]
    fn get_block_backoff_is_capped_at_one_second() {
        assert_eq!(
            BlockFetcher::backoff_before_next_attempt(1),
            Duration::from_millis(200)
        );
        assert_eq!(
            BlockFetcher::backoff_before_next_attempt(2),
            Duration::from_millis(400)
        );
        assert_eq!(
            BlockFetcher::backoff_before_next_attempt(3),
            Duration::from_millis(800)
        );
        assert_eq!(
            BlockFetcher::backoff_before_next_attempt(4),
            Duration::from_secs(1)
        );
        assert_eq!(
            BlockFetcher::backoff_before_next_attempt(10),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn retryable_get_block_failures_then_succeed_once() {
        let (transaction, signature, account) = sample_transaction();
        let parsed = block_on(async {
            let mut trial = fetch_responses(
                77,
                vec![
                    Err(rpc_failure(
                        JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
                        "Block not available for slot 77".to_string(),
                    )),
                    Err(rpc_failure(
                        JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET,
                        "Block status not yet available for slot 77".to_string(),
                    )),
                    Ok(confirmed_block(transaction)),
                ],
            )
            .await;
            assert!(matches!(trial.outcome, BlockFetch::Fetched(_)));
            assert_finalized_attempts(&trial.calls, 77, 3);
            assert_eq!(
                trial.delays,
                vec![Duration::from_millis(200), Duration::from_millis(400)]
            );
            wait_for_one_parsed(&mut trial.receiver).await
        });
        match parsed {
            ProtocolMessage::ParsedBlock(block_no, total_chunks, chunk_no, block) => {
                assert_eq!(block_no, 42);
                assert_eq!(total_chunks, 1);
                assert_eq!(chunk_no, 0);
                assert!(block.get_tx_details(&signature).is_some());
                assert_eq!(block.get_account_balance(&account), Some(10));
            }
            other => panic!("expected one parsed block, got {other:?}"),
        }
    }

    #[test]
    fn skipped_slot_after_a_transient_failure_stops_retrying() {
        let slot = 77;
        let mut trial = block_on(fetch_responses(
            slot,
            vec![
                Err(rpc_failure(
                    JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
                    format!("Block not available for slot {slot}"),
                )),
                Err(rpc_failure(
                    JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
                    format!(
                        "Slot {slot} was skipped, or missing due to ledger jump to recent snapshot"
                    ),
                )),
            ],
        ));
        assert!(matches!(trial.outcome, BlockFetch::Skipped(skipped) if skipped == slot));
        assert_finalized_attempts(&trial.calls, slot, 2);
        assert_eq!(trial.delays, vec![Duration::from_millis(200)]);
        assert!(trial.receiver.try_recv().is_err());
    }

    #[test]
    fn rate_limit_timeout_and_transport_errors_are_retried() {
        let rate_limit = live_get_block_error(
            local_http("429 Too Many Requests", false),
            Duration::from_secs(5),
        );
        assert!(matches!(
            rate_limit.kind(),
            solana_client::client_error::ClientErrorKind::Reqwest(err)
                if err.status()
                    == Some(solana_client::client_error::reqwest::StatusCode::TOO_MANY_REQUESTS)
        ));
        assert!(BlockFetcher::is_retryable_block_fetch(&rate_limit));

        let timeout = live_get_block_error(local_http("200 OK", true), Duration::from_millis(200));
        assert!(matches!(
            timeout.kind(),
            solana_client::client_error::ClientErrorKind::Reqwest(err) if err.is_timeout()
        ));
        assert!(BlockFetcher::is_retryable_block_fetch(&timeout));

        let transport = live_get_block_error(
            format!("http://127.0.0.1:1/?api-key={RPC_SECRET}"),
            Duration::from_secs(2),
        );
        assert!(BlockFetcher::is_retryable_block_fetch(&transport));

        block_on(async {
            let (transaction, _, _) = sample_transaction();
            let mut rate_trial =
                fetch_responses(100, vec![Err(rate_limit), Ok(confirmed_block(transaction))]).await;
            assert!(matches!(rate_trial.outcome, BlockFetch::Fetched(_)));
            assert_finalized_attempts(&rate_trial.calls, 100, 2);
            assert_eq!(rate_trial.delays, vec![Duration::from_millis(200)]);
            assert!(matches!(
                wait_for_one_parsed(&mut rate_trial.receiver).await,
                ProtocolMessage::ParsedBlock(_, 1, 0, _)
            ));

            let (transaction, _, _) = sample_transaction();
            let mut timeout_trial =
                fetch_responses(100, vec![Err(timeout), Ok(confirmed_block(transaction))]).await;
            assert!(matches!(timeout_trial.outcome, BlockFetch::Fetched(_)));
            assert_eq!(timeout_trial.delays, vec![Duration::from_millis(200)]);
            assert!(matches!(
                wait_for_one_parsed(&mut timeout_trial.receiver).await,
                ProtocolMessage::ParsedBlock(_, 1, 0, _)
            ));
        });

        let transports = vec![
            transport,
            live_get_block_error(
                format!("http://127.0.0.1:1/?api-key={RPC_SECRET}"),
                Duration::from_secs(2),
            ),
            live_get_block_error(
                format!("http://127.0.0.1:1/?api-key={RPC_SECRET}"),
                Duration::from_secs(2),
            ),
        ];
        let mut exhausted = block_on(fetch_responses(
            100,
            transports.into_iter().map(Err).collect(),
        ));
        assert!(matches!(exhausted.outcome, BlockFetch::Failed { .. }));
        assert_finalized_attempts(&exhausted.calls, 100, 3);
        assert_eq!(
            exhausted.delays,
            vec![Duration::from_millis(200), Duration::from_millis(400)]
        );
        assert!(exhausted.receiver.try_recv().is_err());
    }

    struct ProgressionRpc {
        tip: u64,
        blocks: Mutex<VecDeque<RpcResult<serde_json::Value>>>,
        calls: RecordedCalls,
    }

    impl RpcSender for ProgressionRpc {
        fn send<'life0, 'async_trait>(
            &'life0 self,
            request: RpcRequest,
            params: serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = RpcResult<serde_json::Value>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            let response = match &request {
                RpcRequest::GetSlot => Ok(serde_json::json!(self.tip)),
                RpcRequest::GetBlock => self
                    .blocks
                    .lock()
                    .expect("block script")
                    .pop_front()
                    .expect("unexpected getBlock"),
                _ => panic!("unexpected rpc request"),
            };
            self.calls
                .lock()
                .expect("rpc recorder")
                .push((request, params));
            Box::pin(async move { response })
        }

        fn get_transport_stats(&self) -> RpcTransportStats {
            RpcTransportStats::default()
        }

        fn url(&self) -> String {
            format!("http://127.0.0.1/?api-key={RPC_SECRET}")
        }
    }

    fn progression_subscriber(
        cursor: u64,
        tip: u64,
        blocks: Vec<RpcResult<serde_json::Value>>,
    ) -> (
        Subscriber,
        RecordedCalls,
        tokio::sync::mpsc::Receiver<ProtocolMessage>,
    ) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let rpc = ProgressionRpc {
            tip,
            blocks: Mutex::new(VecDeque::from(blocks)),
            calls: Arc::clone(&calls),
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
        let (parser_jobs, parser_inbox) = super::parser_channel();
        let subscriber = Subscriber {
            latest_slot: cursor,
            chain_url: format!("http://127.0.0.1/?api-key={RPC_SECRET}"),
            rpc_client: RpcClient::new_sender(
                rpc,
                RpcClientConfig::with_commitment(CommitmentConfig::finalized()),
            ),
            rpc_block_config: block_config(),
            handler_sender: sender,
            parser_jobs,
            parser_workers: Vec::new(),
            parser_inbox: Some(parser_inbox),
            stopping: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            runtime: None,
            outcome_tx: None,
            outcome_rx: None,
        };
        (subscriber, calls, receiver)
    }

    fn block_requests(calls: &[(RpcRequest, serde_json::Value)]) -> Vec<u64> {
        calls
            .iter()
            .filter(|(request, _)| *request == RpcRequest::GetBlock)
            .map(|(_, params)| {
                let params = params.as_array().expect("getBlock params");
                assert_eq!(
                    params
                        .get(1)
                        .and_then(|config| config.get("commitment"))
                        .and_then(serde_json::Value::as_str),
                    Some("finalized")
                );
                params[0].as_u64().expect("getBlock slot")
            })
            .collect()
    }

    #[test]
    fn subscriber_progresses_only_through_fetched_and_skipped_slots() {
        let (transaction, signature, _) = sample_transaction();
        let secret_failure = format!("http://127.0.0.1/?api-key={RPC_SECRET}");
        block_on(async {
            let (mut subscriber, calls, mut receiver) = progression_subscriber(
                100,
                110,
                vec![
                    Ok(confirmed_block(transaction)),
                    Err(rpc_failure(
                        JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
                        "Slot 102 was skipped, or missing due to ledger jump to recent snapshot"
                            .to_string(),
                    )),
                    Err(rpc_failure(-32602, secret_failure.clone())),
                ],
            );
            let (outcome_tx, outcome_rx) =
                tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
            subscriber.set_block_outcomes(outcome_tx.clone(), outcome_rx);
            let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
            let mut seen_tx = Some(seen_tx);
            tokio::spawn(async move {
                while let Some(message) = receiver.recv().await {
                    match message {
                        ProtocolMessage::ParsedBlock(_, _, _, _) => {
                            outcome_tx
                                .try_send(ProtocolMessage::BlockProcessingResult(101, Ok(())))
                                .expect("outcome");
                            if let Some(sender) = seen_tx.take() {
                                let _ = sender.send(message);
                            }
                        }
                        ProtocolMessage::RecordSkippedSlot(slot) => {
                            outcome_tx
                                .try_send(ProtocolMessage::BlockProcessingResult(slot, Ok(())))
                                .expect("skipped slot outcome");
                        }
                        _ => {}
                    }
                }
            });
            assert!(matches!(
                subscriber.advance_once().await,
                SlotProgress::Advanced
            ));
            assert_eq!(subscriber.latest_slot, 101);
            match seen_rx.await.expect("parsed block") {
                ProtocolMessage::ParsedBlock(block_no, total_chunks, chunk_no, block) => {
                    assert_eq!((block_no, total_chunks, chunk_no), (42, 1, 0));
                    assert!(block.get_tx_details(&signature).is_some());
                }
                other => panic!("expected one parsed block, got {other:?}"),
            }

            assert!(matches!(
                subscriber.advance_once().await,
                SlotProgress::Advanced
            ));
            assert_eq!(subscriber.latest_slot, 102);
            assert_eq!(
                block_requests(&calls.lock().expect("calls")),
                vec![101, 102]
            );

            assert!(matches!(
                subscriber.advance_once().await,
                SlotProgress::Halted(_)
            ));
            assert_eq!(subscriber.latest_slot, 102);
            assert_eq!(
                block_requests(&calls.lock().expect("calls")),
                vec![101, 102, 103]
            );
        });
    }

    #[test]
    fn unsupported_transaction_version_stops_without_skipping() {
        block_on(async {
            let (mut subscriber, calls, mut receiver) = progression_subscriber(
                100,
                110,
                vec![Err(rpc_failure(
                    JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION,
                    "Transaction version (2) is not supported".to_string(),
                ))],
            );
            let progress = subscriber.advance_once().await;
            match progress {
                SlotProgress::Halted(reason) => {
                    assert!(reason.contains("at 101"), "{reason}");
                    assert!(reason.contains("not supported"), "{reason}");
                    assert!(!reason.contains("was skipped"), "{reason}");
                }
                other => panic!("unsupported version must halt, got {other:?}"),
            }
            assert_eq!(subscriber.latest_slot, 100);
            assert_eq!(block_requests(&calls.lock().expect("calls")), vec![101]);
            assert!(receiver.try_recv().is_err());
        });
    }

    #[test]
    fn subscriber_stops_when_a_slot_remains_unresolved() {
        let secret = format!("http://127.0.0.1/?api-key={RPC_SECRET}");
        let (mut subscriber, calls, mut receiver) =
            progression_subscriber(100, 110, vec![Err(rpc_failure(-32602, secret.clone()))]);
        let logged = capture_fetch_logs(|| {
            block_on(async {
                let result = tokio::time::timeout(Duration::from_secs(2), subscriber.run())
                    .await
                    .expect("subscriber stopped after the failed slot");
                let error = result.expect_err("failed fetch is fatal");
                assert!(error.contains("at 101"), "{error}");
                assert!(error.contains("getBlock failed"), "{error}");
                assert!(!error.contains(RPC_SECRET), "{error}");
                assert!(!error.contains("api-key="), "{error}");
            });
        });
        assert_eq!(subscriber.latest_slot, 100);
        assert_eq!(block_requests(&calls.lock().expect("calls")), vec![101]);
        assert!(receiver.try_recv().is_err());
        assert!(
            logged.contains("Stopping slot progression at 101"),
            "{logged}"
        );
        assert!(logged.contains("Failed to fetch block"), "{logged}");
        assert!(!logged.contains(RPC_SECRET), "{logged}");
        assert!(!logged.contains("api-key="), "{logged}");
    }

    #[test]
    fn retry_exhaustion_does_not_advance_or_fetch_later_slots() {
        let mut failures = Vec::new();
        for _ in 0..3 {
            failures.push(Err(rpc_failure(
                JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
                "Block not available for slot 51".to_string(),
            )));
        }
        let (mut subscriber, calls, mut receiver) = progression_subscriber(50, 80, failures);
        block_on(async {
            let result = tokio::time::timeout(Duration::from_secs(3), subscriber.run())
                .await
                .expect("subscriber stopped after retry exhaustion");
            let error = result.expect_err("exhausted retries are fatal");
            assert!(error.contains("at 51"), "{error}");
        });
        assert_eq!(subscriber.latest_slot, 50);
        assert_eq!(
            block_requests(&calls.lock().expect("calls")),
            vec![51, 51, 51]
        );
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn transport_timeout_retries_the_same_slot_without_advancing() {
        fn timed_out() -> solana_client::client_error::ClientError {
            solana_client::client_error::ClientError::from(
                solana_client::client_error::ClientErrorKind::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timed out",
                )),
            )
        }
        block_on(async {
            let (mut subscriber, calls, _receiver) = progression_subscriber(
                50,
                80,
                vec![
                    Err(timed_out()),
                    Err(timed_out()),
                    Err(timed_out()),
                    Err(timed_out()),
                    Err(timed_out()),
                    Err(timed_out()),
                ],
            );
            assert!(matches!(
                subscriber.advance_once().await,
                SlotProgress::Retry { slot: 51, .. }
            ));
            assert!(matches!(
                subscriber.advance_once().await,
                SlotProgress::Retry { slot: 51, .. }
            ));
            assert_eq!(subscriber.latest_slot, 50);
            assert_eq!(
                block_requests(&calls.lock().expect("calls")),
                vec![51, 51, 51, 51, 51, 51]
            );
        });
    }

    #[test]
    fn subscriber_does_not_fetch_when_the_cursor_is_caught_up() {
        let (mut subscriber, calls, mut receiver) = progression_subscriber(40, 40, Vec::new());
        let progress = block_on(subscriber.advance_once());
        assert!(matches!(progress, SlotProgress::CaughtUp));
        assert_eq!(subscriber.latest_slot, 40);
        assert!(block_requests(&calls.lock().expect("calls")).is_empty());
        assert!(receiver.try_recv().is_err());
        let recorded = calls.lock().expect("calls");
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, RpcRequest::GetSlot);
        let params = recorded[0].1.as_array().expect("getSlot params");
        assert_eq!(params[0]["commitment"], "finalized");
    }

    #[test]
    fn repeated_block_fetches_reuse_one_rpc_client() {
        block_on(async {
            let (client, calls) = scripted_client(vec![
                Err(rpc_failure(-32602, "first".to_string())),
                Err(rpc_failure(-32602, "second".to_string())),
            ]);
            let (sender, _receiver) =
                tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
            let jobs = super::start_parser_workers();
            for _ in 0..2 {
                let outcome = BlockFetcher::request_block(
                    &client,
                    7,
                    block_config(),
                    sender.clone(),
                    None,
                    &jobs,
                )
                .await;
                assert!(matches!(outcome, BlockFetch::Failed { .. }));
            }
            assert_eq!(block_requests(&calls.lock().expect("calls")), vec![7, 7]);
        });
    }

    #[test]
    fn parser_concurrency_stays_within_four_workers() {
        block_on(async {
            use std::sync::atomic::Ordering;
            super::PARSER_GATE.hold.store(true, Ordering::SeqCst);
            super::PARSER_GATE.in_flight.store(0, Ordering::SeqCst);
            super::PARSER_GATE.max_in_flight.store(0, Ordering::SeqCst);
            super::PARSER_GATE.completed.store(0, Ordering::SeqCst);
            let (mut subscriber, _calls, _receiver) = progression_subscriber(1, 1, Vec::new());
            let _ = subscriber.advance_once().await;
            let jobs = subscriber.parser_jobs.clone();
            let handler = subscriber.handler_sender.clone();
            let mut queued = Vec::new();
            for index in 0..9 {
                let jobs = jobs.clone();
                let handler_sender = handler.clone();
                queued.push(tokio::spawn(async move {
                    jobs.send(super::ParseJob {
                        fetch_slot: 2,
                        block_no: 2,
                        chunk_index: index,
                        total_chunks: 9,
                        transactions: Vec::new(),
                        handler_sender,
                        outcome_sender: None,
                    })
                    .await
                    .expect("queue parse job");
                }));
            }
            let started = tokio::time::timeout(Duration::from_secs(2), async {
                while super::PARSER_GATE.in_flight.load(Ordering::SeqCst)
                    < super::PARSER_CONCURRENCY
                {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            assert!(started.is_ok(), "parser workers did not start");
            assert_eq!(
                super::PARSER_GATE.max_in_flight.load(Ordering::SeqCst),
                super::PARSER_CONCURRENCY
            );
            let queued_immediately = queued.iter().filter(|task| task.is_finished()).count();
            assert!(
                queued_immediately < 9,
                "the extra chunk was accepted without waiting"
            );
            super::PARSER_GATE.hold.store(false, Ordering::SeqCst);
            super::PARSER_GATE.release.notify_waiters();
            for task in queued {
                task.await.expect("parse job queued");
            }
            let finished = tokio::time::timeout(Duration::from_secs(2), async {
                while super::PARSER_GATE.completed.load(Ordering::SeqCst) < 9 {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            assert!(finished.is_ok(), "parser jobs did not finish");
            assert_eq!(
                super::PARSER_GATE.max_in_flight.load(Ordering::SeqCst),
                super::PARSER_CONCURRENCY
            );
        });
    }

    #[test]
    fn closed_outcome_channel_does_not_count_as_success() {
        let (transaction, _, _) = sample_transaction();
        block_on(async {
            let (mut subscriber, calls, _receiver) =
                progression_subscriber(100, 110, vec![Ok(confirmed_block(transaction))]);
            let (outcome_sender, outcome_receiver) =
                tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
            drop(outcome_sender);
            subscriber.outcome_rx = Some(outcome_receiver);
            assert!(matches!(
                subscriber.advance_once().await,
                SlotProgress::Halted(_)
            ));
            assert_eq!(subscriber.latest_slot, 100);
            assert_eq!(block_requests(&calls.lock().expect("calls")), vec![101]);
        });
    }

    #[test]
    fn stale_slot_acknowledgment_does_not_advance_the_cursor() {
        let (first, _, _) = sample_transaction();
        let (second, _, _) = sample_transaction();
        block_on(async {
            let (mut subscriber, calls, mut receiver) = progression_subscriber(
                100,
                110,
                vec![Ok(confirmed_block(first)), Ok(confirmed_block(second))],
            );
            let (outcome_sender, outcome_receiver) =
                tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
            subscriber.set_block_outcomes(outcome_sender.clone(), outcome_receiver);
            tokio::spawn(async move {
                let mut parsed = 0;
                while let Some(message) = receiver.recv().await {
                    if let ProtocolMessage::ParsedBlock(_, _, _, _) = message {
                        parsed += 1;
                        let acknowledged_slot = 101;
                        outcome_sender
                            .try_send(ProtocolMessage::BlockProcessingResult(
                                acknowledged_slot,
                                Ok(()),
                            ))
                            .expect("outcome");
                        if parsed == 2 {
                            break;
                        }
                    }
                }
            });
            assert!(matches!(
                subscriber.advance_once().await,
                SlotProgress::Advanced
            ));
            assert_eq!(subscriber.latest_slot, 101);
            let second =
                tokio::time::timeout(Duration::from_millis(300), subscriber.advance_once()).await;
            assert!(
                second.is_err(),
                "stale slot 101 acknowledgment completed slot 102"
            );
            assert_eq!(subscriber.latest_slot, 101);
            assert_eq!(
                block_requests(&calls.lock().expect("calls")),
                vec![101, 102]
            );
        });
    }

    #[test]
    fn parser_failure_stops_subscriber_progression() {
        let (mut transaction, _, _) = sample_transaction();
        transaction.transaction = solana_transaction_status::EncodedTransaction::Binary(
            String::new(),
            solana_transaction_status::TransactionBinaryEncoding::Base64,
        );
        block_on(async {
            let (handler_sender, handler_receiver) =
                tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
            let (db_sender, mut db_receiver) =
                tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
            let (outcome_sender, outcome_receiver) =
                tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
            let mut handler = crate::handler::Handler::initialize(handler_receiver, db_sender);
            handler.set_block_outcomes(outcome_sender.clone());
            tokio::spawn(async move {
                handler.run().await;
            });
            let (mut subscriber, calls, _receiver) =
                progression_subscriber(100, 110, vec![Ok(confirmed_block(transaction))]);
            subscriber.handler_sender = handler_sender;
            subscriber.set_block_outcomes(outcome_sender, outcome_receiver);
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(2), subscriber.advance_once()).await,
                Ok(SlotProgress::Halted(_))
            ));
            assert_eq!(subscriber.latest_slot, 100);
            assert_eq!(block_requests(&calls.lock().expect("calls")), vec![101]);
            assert!(db_receiver.try_recv().is_err());
        });
    }

    #[test]
    fn skipped_slot_checkpoint_failure_stops_progression() {
        block_on(async {
            let (mut subscriber, calls, mut receiver) = progression_subscriber(
                100,
                110,
                vec![Err(rpc_failure(
                    JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
                    "Slot 101 was skipped".to_string(),
                ))],
            );
            let (outcome_sender, outcome_receiver) =
                tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
            subscriber.set_block_outcomes(outcome_sender.clone(), outcome_receiver);
            tokio::spawn(async move {
                while let Some(message) = receiver.recv().await {
                    if let ProtocolMessage::RecordSkippedSlot(slot) = message {
                        outcome_sender
                            .try_send(ProtocolMessage::BlockProcessingResult(
                                slot,
                                Err("checkpoint write failed".to_string()),
                            ))
                            .expect("outcome");
                        break;
                    }
                }
            });
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(2), subscriber.advance_once()).await,
                Ok(SlotProgress::Halted(_))
            ));
            assert_eq!(subscriber.latest_slot, 100);
            assert_eq!(block_requests(&calls.lock().expect("calls")), vec![101]);
        });
    }

    #[test]
    fn subscriber_resumes_from_checkpoint_instead_of_the_live_tip() {
        block_on(async {
            let (mut subscriber, calls, _receiver) = progression_subscriber(
                500,
                500,
                vec![Err(rpc_failure(-32602, "unavailable".to_string()))],
            );
            subscriber.resume_from_checkpoint(20);
            assert!(matches!(
                subscriber.advance_once().await,
                SlotProgress::Halted(_)
            ));
            assert_eq!(subscriber.latest_slot, 20);
            assert_eq!(block_requests(&calls.lock().expect("calls")), vec![21]);
        });
    }

    #[test]
    fn empty_fetched_block_waits_for_durable_acknowledgment() {
        block_on(async {
            let value = serde_json::to_value(UiConfirmedBlock {
                previous_blockhash: "11111111111111111111111111111111".to_string(),
                blockhash: "11111111111111111111111111111111".to_string(),
                parent_slot: 99,
                transactions: Some(vec![]),
                signatures: None,
                rewards: None,
                num_reward_partitions: None,
                block_time: Some(0),
                block_height: Some(7),
            })
            .expect("empty block");
            let (mut subscriber, calls, mut receiver) =
                progression_subscriber(100, 110, vec![Ok(value)]);
            let (outcome_sender, outcome_receiver) =
                tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
            subscriber.set_block_outcomes(outcome_sender.clone(), outcome_receiver);
            tokio::spawn(async move {
                while let Some(message) = receiver.recv().await {
                    if let ProtocolMessage::StoreEmptyBlock(height, slot) = message {
                        assert_eq!((height, slot), (7, 101));
                        outcome_sender
                            .try_send(ProtocolMessage::BlockProcessingResult(slot, Ok(())))
                            .expect("outcome");
                        break;
                    }
                }
            });
            let result =
                tokio::time::timeout(Duration::from_secs(2), subscriber.advance_once()).await;
            assert!(matches!(result, Ok(SlotProgress::Advanced)));
            assert_eq!(subscriber.latest_slot, 101);
            assert_eq!(block_requests(&calls.lock().expect("calls")), vec![101]);
        });
    }

    #[test]
    fn missing_block_height_does_not_advance() {
        block_on(async {
            let value = serde_json::to_value(UiConfirmedBlock {
                previous_blockhash: "11111111111111111111111111111111".to_string(),
                blockhash: "11111111111111111111111111111111".to_string(),
                parent_slot: 99,
                transactions: Some(vec![]),
                signatures: None,
                rewards: None,
                num_reward_partitions: None,
                block_time: Some(0),
                block_height: None,
            })
            .expect("missing height");
            let (mut subscriber, _calls, mut receiver) =
                progression_subscriber(100, 110, vec![Ok(value)]);
            assert!(matches!(
                subscriber.advance_once().await,
                SlotProgress::Halted(_)
            ));
            assert_eq!(subscriber.latest_slot, 100);
            assert!(receiver.try_recv().is_err());
        });
    }

    struct HoldingSender {
        started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        release: Arc<tokio::sync::Notify>,
    }

    impl RpcSender for HoldingSender {
        fn send<'life0, 'async_trait>(
            &'life0 self,
            request: RpcRequest,
            _params: serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = RpcResult<serde_json::Value>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            let release = Arc::clone(&self.release);
            let started = request == RpcRequest::GetSlot;
            let notify = if started {
                self.started.lock().expect("started").take()
            } else {
                None
            };
            Box::pin(async move {
                if let Some(sender) = notify {
                    let _ = sender.send(());
                    release.notified().await;
                }
                if started {
                    Ok(serde_json::json!(40))
                } else {
                    Err(rpc_failure(
                        JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
                        "skipped".to_string(),
                    ))
                }
            })
        }

        fn get_transport_stats(&self) -> RpcTransportStats {
            RpcTransportStats::default()
        }

        fn url(&self) -> String {
            "holding-sender".to_string()
        }
    }

    #[test]
    fn shutdown_with_no_active_block_does_not_fetch_or_move_the_cursor() {
        block_on(async {
            let (mut subscriber, calls, _receiver) = progression_subscriber(40, 80, Vec::new());
            let (_shutdown, receiver) = tokio::sync::watch::channel(true);
            let result = subscriber.run_with_shutdown(receiver).await;
            assert!(result.is_ok());
            assert_eq!(subscriber.completed_slot(), 40);
            assert!(calls.lock().expect("calls").is_empty());
        });
    }

    #[test]
    fn shutdown_while_waiting_for_rpc_leaves_the_cursor() {
        block_on(async {
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let release = Arc::new(tokio::sync::Notify::new());
            let rpc = HoldingSender {
                started: std::sync::Mutex::new(Some(started_tx)),
                release: Arc::clone(&release),
            };
            let (sender, _receiver) =
                tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
            let mut subscriber = Subscriber {
                latest_slot: 40,
                chain_url: "http://127.0.0.1".to_string(),
                rpc_client: RpcClient::new_sender(
                    rpc,
                    RpcClientConfig::with_commitment(CommitmentConfig::finalized()),
                ),
                rpc_block_config: block_config(),
                handler_sender: sender,
                parser_jobs: super::parser_channel().0,
                parser_workers: Vec::new(),
                parser_inbox: Some(super::parser_channel().1),
                stopping: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                runtime: None,
                outcome_tx: None,
                outcome_rx: None,
            };
            let (_shutdown, shutdown) = tokio::sync::watch::channel(false);
            let mut run = Box::pin(subscriber.run_with_shutdown(shutdown));
            tokio::select! {
                _ = started_rx => {}
                _ = &mut run => panic!("subscriber returned while RPC was held"),
            }
            drop(run);
            assert_eq!(subscriber.completed_slot(), 40);
            release.notify_waiters();
        });
    }

    #[test]
    fn shutdown_during_active_processing_leaves_the_previous_cursor() {
        block_on(async {
            let (transaction, _, _) = sample_transaction();
            let (mut subscriber, _calls, mut receiver) =
                progression_subscriber(100, 110, vec![Ok(confirmed_block(transaction))]);
            let (outcome_tx, outcome_rx) =
                tokio::sync::mpsc::channel(crate::util::TEST_CHANNEL_CAPACITY);
            subscriber.set_block_outcomes(outcome_tx, outcome_rx);
            let (_shutdown, shutdown) = tokio::sync::watch::channel(false);
            let mut run = Box::pin(subscriber.run_with_shutdown(shutdown));
            tokio::select! {
                message = receiver.recv() => {
                    assert!(message.is_some(), "indexing did not start");
                }
                _ = &mut run => panic!("subscriber finished before the block was acknowledged"),
            }
            drop(run);
            assert_eq!(subscriber.completed_slot(), 100);
        });
    }

    struct SkipAfterNotify {
        started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    impl RpcSender for SkipAfterNotify {
        fn send<'life0, 'async_trait>(
            &'life0 self,
            request: RpcRequest,
            _params: serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = RpcResult<serde_json::Value>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            let notify = if request == RpcRequest::GetBlock {
                self.started.lock().expect("started").take()
            } else {
                None
            };
            Box::pin(async move {
                if let Some(sender) = notify {
                    let _ = sender.send(());
                }
                if request == RpcRequest::GetSlot {
                    Ok(serde_json::json!(110))
                } else {
                    Err(rpc_failure(
                        JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
                        "skipped".to_string(),
                    ))
                }
            })
        }

        fn get_transport_stats(&self) -> RpcTransportStats {
            RpcTransportStats::default()
        }

        fn url(&self) -> String {
            "skip-after-notify".to_string()
        }
    }

    #[test]
    fn shutdown_while_the_handler_channel_is_full_leaves_the_cursor() {
        block_on(async {
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (sender, _backlog) = tokio::sync::mpsc::channel(1);
            sender
                .try_send(ProtocolMessage::RecordSkippedSlot(0))
                .expect("fill the only slot");
            let (jobs, inbox) = super::parser_channel();
            let mut subscriber = Subscriber {
                latest_slot: 100,
                chain_url: "http://127.0.0.1".to_string(),
                rpc_client: RpcClient::new_sender(
                    SkipAfterNotify {
                        started: std::sync::Mutex::new(Some(started_tx)),
                    },
                    RpcClientConfig::with_commitment(CommitmentConfig::finalized()),
                ),
                rpc_block_config: block_config(),
                handler_sender: sender,
                parser_jobs: jobs,
                parser_workers: Vec::new(),
                parser_inbox: Some(inbox),
                stopping: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                runtime: None,
                outcome_tx: None,
                outcome_rx: None,
            };
            let (_shutdown, shutdown) = tokio::sync::watch::channel(false);
            let mut run = Box::pin(subscriber.run_with_shutdown(shutdown));
            tokio::select! {
                _ = started_rx => {}
                _ = &mut run => panic!("subscriber finished while the handler channel was full"),
            }
            drop(run);
            assert_eq!(subscriber.completed_slot(), 100);
        });
    }

    struct ArmShutdown {
        flag: Arc<std::sync::atomic::AtomicBool>,
    }

    impl RpcSender for ArmShutdown {
        fn send<'life0, 'async_trait>(
            &'life0 self,
            request: RpcRequest,
            _params: serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = RpcResult<serde_json::Value>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            let flag = Arc::clone(&self.flag);
            let is_slot = request == RpcRequest::GetSlot;
            Box::pin(async move {
                if is_slot {
                    Ok(serde_json::json!(110))
                } else {
                    flag.store(true, std::sync::atomic::Ordering::SeqCst);
                    Err(rpc_failure(
                        JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
                        "skipped".to_string(),
                    ))
                }
            })
        }

        fn get_transport_stats(&self) -> RpcTransportStats {
            RpcTransportStats::default()
        }

        fn url(&self) -> String {
            "arm-shutdown".to_string()
        }
    }

    #[test]
    fn unexpected_channel_closure_is_fatal_and_shutdown_closure_is_not() {
        let closed = block_on(async {
            let (mut subscriber, _calls, receiver) = progression_subscriber(
                100,
                110,
                vec![Err(rpc_failure(
                    JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
                    "skipped".to_string(),
                ))],
            );
            drop(receiver);
            let (sender, closed_receiver) = tokio::sync::mpsc::channel(1);
            drop(closed_receiver);
            subscriber.handler_sender = sender;
            subscriber
                .run()
                .await
                .expect_err("closed channel while indexing")
        });
        assert!(closed.contains("database channel closed"), "{closed}");

        let logged = capture_fetch_logs(|| {
            log::set_max_level(log::LevelFilter::Info);
            block_on(async {
                let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let (sender, closed_receiver) = tokio::sync::mpsc::channel(1);
                drop(closed_receiver);
                let (jobs, inbox) = super::parser_channel();
                let mut subscriber = Subscriber {
                    latest_slot: 100,
                    chain_url: "http://127.0.0.1".to_string(),
                    rpc_client: RpcClient::new_sender(
                        ArmShutdown {
                            flag: Arc::clone(&flag),
                        },
                        RpcClientConfig::with_commitment(CommitmentConfig::finalized()),
                    ),
                    rpc_block_config: block_config(),
                    handler_sender: sender,
                    parser_jobs: jobs,
                    parser_workers: Vec::new(),
                    parser_inbox: Some(inbox),
                    stopping: Arc::clone(&flag),
                    runtime: None,
                    outcome_tx: None,
                    outcome_rx: None,
                };
                let result = subscriber.run().await;
                assert!(result.is_ok(), "{result:?}");
                assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
                let (job_sender, job_receiver) = tokio::sync::mpsc::channel(1);
                drop(job_receiver);
                super::run_parse_job(
                    super::ParseJob {
                        fetch_slot: 101,
                        block_no: 1,
                        chunk_index: 0,
                        total_chunks: 1,
                        transactions: Vec::new(),
                        handler_sender: job_sender,
                        outcome_sender: None,
                    },
                    &flag,
                )
                .await;
            });
            log::set_max_level(log::LevelFilter::Error);
        });
        assert!(!logged.contains("Mpsc Channel Error"), "{logged}");
        assert!(!logged.contains("Stopping slot progression"), "{logged}");
    }
}
