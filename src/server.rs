use crate::error::AggError;
use crate::lifecycle::{HealthBody, Lifecycle, StatusBody};
use crate::util::{
    Channel, ProtocolMessage, QueryFailureKind, QueryParams, MAX_API_BLOCK_RANGE,
    REPLY_CHANNEL_CAPACITY,
};
use actix_web::http::StatusCode;
use actix_web::{get, middleware, web, App, HttpResponse, HttpServer, Responder};
use serde::Serialize;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;

const DATABASE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct ApiState {
    sender: Sender<ProtocolMessage>,
    db_timeout: Duration,
    lifecycle: Option<Arc<Lifecycle>>,
}

impl ApiState {
    fn production(sender: Sender<ProtocolMessage>, lifecycle: Arc<Lifecycle>) -> Self {
        Self {
            sender,
            db_timeout: DATABASE_TIMEOUT,
            lifecycle: Some(lifecycle),
        }
    }

    #[cfg(test)]
    fn with_timeout(sender: Sender<ProtocolMessage>, db_timeout: Duration) -> Self {
        Self {
            sender,
            db_timeout,
            lifecycle: None,
        }
    }

    #[cfg(test)]
    fn with_lifecycle(sender: Sender<ProtocolMessage>, lifecycle: Arc<Lifecycle>) -> Self {
        Self {
            sender,
            db_timeout: DATABASE_TIMEOUT,
            lifecycle: Some(lifecycle),
        }
    }
}

pub(crate) struct AggServer;

impl AggServer {

    /// This function runs the server
    ///
    /// # Arguments
    ///
    /// * `handler_sender` - A Sender<ProtocolMessage> that holds the handler sender
    /// * `port_no` - A string slice that holds the port number
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    pub async fn run(
        handler_sender: Sender<ProtocolMessage>,
        port_no: String,
        lifecycle: Arc<Lifecycle>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), AggError> {
        let state = ApiState::production(handler_sender, lifecycle);
        let server = HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(state.clone()))
                .wrap(middleware::Logger::default())
                .configure(configure)
        })
        .disable_signals()
        .bind(format!("127.0.0.1:{port_no}"))?
        .shutdown_timeout(5)
        .run();
        let handle = server.handle();
        // The server has to keep running on its own task. stop() waits for that
        // task to observe the command, so it cannot be driven by the same future.
        let mut server_task = AbortOnDrop(Some(tokio::spawn(server)));
        tokio::select! {
            result = server_task.0.as_mut().unwrap() => {
                server_stopped(result)?;
            }
            _ = shutdown.wait_for(|stopped| *stopped) => {
                log::info!(target: "server", "HTTP server stopping");
                handle.stop(false).await;
                let task = server_task.0.take().expect("HTTP server task");
                server_stopped(task.await)?;
                log::info!(target: "server", "HTTP server stopped");
            }
        }
        Ok(())
    }
}

struct AbortOnDrop<T>(Option<tokio::task::JoinHandle<T>>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

fn server_stopped(
    result: Result<Result<(), std::io::Error>, tokio::task::JoinError>,
) -> Result<(), AggError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error.into()),
        Err(_) => Err(std::io::Error::other("HTTP server task stopped unexpectedly").into()),
    }
}

pub(crate) fn configure(config: &mut web::ServiceConfig) {
    config
        .service(get_tx_details)
        .service(get_block_details)
        .service(get_latest_block)
        .service(get_block_range)
        .service(get_account_balance)
        .service(get_health)
        .service(get_status);
}

#[get("/health")]
async fn get_health(state: web::Data<ApiState>) -> impl Responder {
    match state.lifecycle.as_ref() {
        Some(lifecycle) if lifecycle.is_healthy() => {
            HttpResponse::Ok().json(HealthBody { status: "healthy" })
        }
        _ => HttpResponse::ServiceUnavailable().json(HealthBody {
            status: "unhealthy",
        }),
    }
}

#[get("/status")]
async fn get_status(state: web::Data<ApiState>) -> impl Responder {
    let Some(lifecycle) = &state.lifecycle else {
        return HttpResponse::ServiceUnavailable().json(StatusBody {
            state: "failed",
            last_processed_slot: None,
            last_indexed_block_height: None,
            last_observed_finalized_slot: None,
            last_successful_processing_time: None,
            error: Some("Indexer status is unavailable".to_string()),
        });
    };
    HttpResponse::Ok().json(StatusBody::from(lifecycle.snapshot()))
}

#[derive(Serialize)]
struct ApiErrorBody {
    error: &'static str,
}

fn api_error(status: StatusCode, message: &'static str) -> HttpResponse {
    HttpResponse::build(status).json(ApiErrorBody { error: message })
}

fn status_for(kind: QueryFailureKind) -> StatusCode {
    match kind {
        QueryFailureKind::BadRequest => StatusCode::BAD_REQUEST,
        QueryFailureKind::NotFound => StatusCode::NOT_FOUND,
        QueryFailureKind::NotReady | QueryFailureKind::Unavailable => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        QueryFailureKind::Storage => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn parse_block_number(value: &str) -> Result<u64, &'static str> {
    if value.is_empty() || value.starts_with('+') || value.starts_with('-') {
        return Err("Invalid block number");
    }
    value.parse::<u64>().map_err(|_| "Invalid block number")
}

fn parse_block_range(start: &str, end: &str) -> Result<(u64, u64), &'static str> {
    let start = parse_block_number(start)?;
    let end = parse_block_number(end)?;
    if start > end {
        return Err("Block range start is greater than end");
    }
    let length = end
        .checked_sub(start)
        .and_then(|span| span.checked_add(1))
        .ok_or("Block range exceeds 100 blocks")?;
    if length > MAX_API_BLOCK_RANGE {
        return Err("Block range exceeds 100 blocks");
    }
    Ok((start, end))
}

fn parse_signature(value: &str) -> Result<String, &'static str> {
    Signature::from_str(value)
        .map(|signature| signature.to_string())
        .map_err(|_| "Invalid transaction signature")
}

fn parse_address(value: &str) -> Result<String, &'static str> {
    Pubkey::from_str(value)
        .map(|address| address.to_string())
        .map_err(|_| "Invalid account address")
}

fn parse_optional_block(value: Option<String>) -> Result<Option<u64>, &'static str> {
    match value {
        None => Ok(None),
        Some(raw) => parse_block_number(&raw)
            .map(Some)
            .map_err(|_| "Invalid block_no query parameter"),
    }
}

async fn wait_for_database(
    outbound: &Sender<ProtocolMessage>,
    message: ProtocolMessage,
    mut inbound: Receiver<ProtocolMessage>,
    timeout: Duration,
) -> Result<ProtocolMessage, HttpResponse> {
    if outbound.send(message).await.is_err() {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Database worker is unavailable",
        ));
    }
    match tokio::time::timeout(timeout, inbound.recv()).await {
        Ok(Some(reply)) => Ok(reply),
        Ok(None) => Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Database worker is unavailable",
        )),
        Err(_) => Err(api_error(
            StatusCode::GATEWAY_TIMEOUT,
            "Database request timed out",
        )),
    }
}

fn response_for(reply: ProtocolMessage) -> HttpResponse {
    match reply {
        ProtocolMessage::TxDetails(tx) => HttpResponse::Ok().json(tx),
        ProtocolMessage::BlockDetails(block) => HttpResponse::Ok().json(block),
        ProtocolMessage::LatestBlockDetails(block_no, block) => {
            HttpResponse::Ok().json((block_no, block))
        }
        ProtocolMessage::BlockRangeDetails(blocks) => HttpResponse::Ok().json(blocks),
        ProtocolMessage::AccountBalance(balance) => HttpResponse::Ok().json(balance),
        ProtocolMessage::QueryError(kind, message) => api_error(status_for(kind), message),
        _ => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Unexpected database response",
        ),
    }
}

#[get("/tx_details/{tx_id}")]
async fn get_tx_details(tx_id: web::Path<String>, state: web::Data<ApiState>) -> impl Responder {
    let signature = match parse_signature(tx_id.into_inner().as_str()) {
        Ok(signature) => signature,
        Err(message) => return api_error(StatusCode::BAD_REQUEST, message),
    };
    let channel = Channel::new(REPLY_CHANNEL_CAPACITY);
    let reply = match wait_for_database(
        &state.sender,
        ProtocolMessage::FetchTransactionDetails(signature, channel.sender()),
        channel.receiver,
        state.db_timeout,
    )
    .await
    {
        Ok(reply) => reply,
        Err(response) => return response,
    };
    response_for(reply)
}

#[get("/block_details/{block_no}")]
async fn get_block_details(
    block_no: web::Path<String>,
    state: web::Data<ApiState>,
) -> impl Responder {
    let block_no = match parse_block_number(block_no.into_inner().as_str()) {
        Ok(block_no) => block_no,
        Err(message) => return api_error(StatusCode::BAD_REQUEST, message),
    };
    let channel = Channel::new(REPLY_CHANNEL_CAPACITY);
    let reply = match wait_for_database(
        &state.sender,
        ProtocolMessage::FetchBlockDetails(block_no.to_string(), channel.sender()),
        channel.receiver,
        state.db_timeout,
    )
    .await
    {
        Ok(reply) => reply,
        Err(response) => return response,
    };
    response_for(reply)
}

#[get("/latest_block")]
async fn get_latest_block(state: web::Data<ApiState>) -> impl Responder {
    let channel = Channel::new(REPLY_CHANNEL_CAPACITY);
    let reply = match wait_for_database(
        &state.sender,
        ProtocolMessage::FetchLatestBlock(channel.sender()),
        channel.receiver,
        state.db_timeout,
    )
    .await
    {
        Ok(reply) => reply,
        Err(response) => return response,
    };
    response_for(reply)
}

#[get("/block_range/{start}/{end}")]
async fn get_block_range(
    range: web::Path<(String, String)>,
    state: web::Data<ApiState>,
) -> impl Responder {
    let (start, end) = range.into_inner();
    let (start, end) = match parse_block_range(&start, &end) {
        Ok(bounds) => bounds,
        Err(message) => return api_error(StatusCode::BAD_REQUEST, message),
    };
    let channel = Channel::new(REPLY_CHANNEL_CAPACITY);
    let reply = match wait_for_database(
        &state.sender,
        ProtocolMessage::FetchBlockRange(start, end, channel.sender()),
        channel.receiver,
        state.db_timeout,
    )
    .await
    {
        Ok(reply) => reply,
        Err(response) => return response,
    };
    response_for(reply)
}

#[get("/account_balance/{account_id}")]
async fn get_account_balance(
    account_id: web::Path<String>,
    query: web::Query<QueryParams>,
    state: web::Data<ApiState>,
) -> impl Responder {
    let address = match parse_address(account_id.into_inner().as_str()) {
        Ok(address) => address,
        Err(message) => return api_error(StatusCode::BAD_REQUEST, message),
    };
    let block_no = match parse_optional_block(query.into_inner().block_no) {
        Ok(block_no) => block_no,
        Err(message) => return api_error(StatusCode::BAD_REQUEST, message),
    };
    let channel = Channel::new(REPLY_CHANNEL_CAPACITY);
    let reply = match wait_for_database(
        &state.sender,
        ProtocolMessage::FetchAccountBalance(address, block_no, channel.sender()),
        channel.receiver,
        state.db_timeout,
    )
    .await
    {
        Ok(reply) => reply,
        Err(response) => return response,
    };
    response_for(reply)
}

#[cfg(test)]
mod tests {
    use super::{configure, ApiState};
    use crate::db_handler::RocksDb;
    use crate::handler::Handler;
    use crate::util::{Block, ProtocolMessage, TxRecord};
    use actix_web::{test, web, App};
    use serde_json::Value;
    use solana_pubkey::Pubkey;
    use solana_signature::Signature;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc::Sender;
    use tokio::task::JoinHandle;

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: std::path::PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "solana-agg-api-{}-{}-{}",
                std::process::id(),
                nanos,
                sequence
            ));
            std::fs::create_dir_all(&path).expect("temp dir");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    struct Running {
        _dir: TempDir,
        path: String,
        observed: String,
        zero: String,
        signature: String,
        sender: Sender<ProtocolMessage>,
        db_task: JoinHandle<()>,
        handler_task: JoinHandle<()>,
    }

    impl Drop for Running {
        fn drop(&mut self) {
            self.db_task.abort();
            self.handler_task.abort();
        }
    }

    async fn indexed() -> Running {
        let dir = TempDir::new();
        let path = dir.path.to_string_lossy().to_string();
        let (db_sender, db_receiver) = crate::util::test_channel();
        let (handler_sender, handler_receiver) = crate::util::test_channel();
        let mut database = RocksDb::initialize(path.clone(), db_receiver).expect("database");
        database
            .establish_initial_checkpoint(1)
            .expect("checkpoint");
        let observed = Pubkey::from([3; 32]).to_string();
        let zero = Pubkey::from([4; 32]).to_string();
        let signature = Signature::from([8; 64]);
        let mut block = Block::default();
        block.push_transaction(signature, TxRecord::default());
        block.insert_account(observed.clone(), 42);
        block.insert_account(zero.clone(), 0);
        database.persist_test_block(7, 2, block).expect("block 7");
        let mut next = Block::default();
        next.insert_account(observed.clone(), 42);
        database.persist_test_block(8, 3, next).expect("block 8");
        let db_task = tokio::spawn(async move {
            database.run().await;
        });
        let mut handler = Handler::initialize(handler_receiver, db_sender);
        let handler_task = tokio::spawn(async move {
            handler.run().await;
        });
        Running {
            _dir: dir,
            path,
            observed,
            zero,
            signature: signature.to_string(),
            sender: handler_sender,
            db_task,
            handler_task,
        }
    }

    async fn call(sender: Sender<ProtocolMessage>, uri: &str) -> (u16, Value) {
        call_with_timeout(sender, uri, Duration::from_secs(5)).await
    }

    async fn call_with_timeout(
        sender: Sender<ProtocolMessage>,
        uri: &str,
        timeout: Duration,
    ) -> (u16, Value) {
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(ApiState::with_timeout(sender, timeout)))
                .configure(configure),
        )
        .await;
        let request = test::TestRequest::get().uri(uri).to_request();
        let response = test::call_service(&app, request).await;
        let status = response.status().as_u16();
        let body = test::read_body_json(response).await;
        (status, body)
    }

    async fn empty_service() -> (
        Sender<ProtocolMessage>,
        JoinHandle<()>,
        JoinHandle<()>,
        TempDir,
    ) {
        let dir = TempDir::new();
        let (db_sender, db_receiver) = crate::util::test_channel();
        let (handler_sender, handler_receiver) = crate::util::test_channel();
        let database =
            RocksDb::initialize(dir.path.to_string_lossy().to_string(), db_receiver).expect("db");
        let db_task = tokio::spawn(async move { database.run().await });
        let mut handler = Handler::initialize(handler_receiver, db_sender);
        let handler_task = tokio::spawn(async move { handler.run().await });
        (handler_sender, db_task, handler_task, dir)
    }

    #[actix_web::test]
    async fn latest_block_without_an_index_is_not_ready() {
        let (sender, db_task, handler_task, _dir) = empty_service().await;
        let (status, body) = call(sender, "/latest_block").await;
        assert_eq!(status, 503);
        assert_eq!(body["error"], "No block has been indexed yet");
        db_task.abort();
        handler_task.abort();
    }

    #[actix_web::test]
    async fn indexed_block_transaction_and_balance_responses_keep_their_shape() {
        let running = indexed().await;
        let (status, latest) = call(running.sender.clone(), "/latest_block").await;
        assert_eq!(status, 200);
        assert_eq!(latest[0], 8);
        assert!(latest[1]["tx_map"].is_object());
        assert_eq!(latest[1]["account_map"][&running.observed], 42);

        let (status, block) = call(running.sender.clone(), "/block_details/7").await;
        assert_eq!(status, 200);
        assert!(block["tx_map"][&running.signature].is_object());
        assert_eq!(block["account_map"][&running.zero], 0);
        assert_eq!(block["account_map"][&running.observed], 42);

        let (status, tx) = call(
            running.sender.clone(),
            &format!("/tx_details/{}", running.signature),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(tx["instruction"], serde_json::json!([]));
        assert!(tx["metadata"].is_null());

        let (status, balance) = call(
            running.sender.clone(),
            &format!("/account_balance/{}", running.observed),
        )
        .await;
        assert_eq!((status, balance), (200, serde_json::json!(42)));
        let (status, zero) = call(
            running.sender.clone(),
            &format!("/account_balance/{}?block_no=7", running.zero),
        )
        .await;
        assert_eq!((status, zero), (200, serde_json::json!(0)));
    }

    #[actix_web::test]
    async fn missing_and_invalid_queries_use_controlled_statuses() {
        let running = indexed().await;
        let unknown_signature = Signature::from([9; 64]).to_string();
        let unknown_address = Pubkey::from([5; 32]).to_string();
        let cases = [
            ("/block_details/99", 404u16, "Block not found"),
            ("/block_details/nope", 400, "Invalid block number"),
            ("/block_details/-1", 400, "Invalid block number"),
            (
                "/tx_details/not-a-signature",
                400,
                "Invalid transaction signature",
            ),
            (
                "/account_balance/not-an-address",
                400,
                "Invalid account address",
            ),
            (
                "/account_balance/11111111111111111111111111111111?block_no=abc",
                400,
                "Invalid block_no query parameter",
            ),
            (
                "/block_range/8/7",
                400,
                "Block range start is greater than end",
            ),
            ("/block_range/1/101", 400, "Block range exceeds 100 blocks"),
            (
                "/block_range/0/18446744073709551615",
                400,
                "Block range exceeds 100 blocks",
            ),
            ("/block_range/-1/2", 400, "Invalid block number"),
            ("/block_range/20/20", 200, ""),
        ];
        for (uri, expected, message) in cases {
            let (status, body) = call(running.sender.clone(), uri).await;
            assert_eq!(status, expected, "{uri} -> {body}");
            if expected == 200 {
                assert_eq!(body, serde_json::json!({}));
            } else {
                assert_eq!(body["error"], message, "{uri}");
                assert!(!body.to_string().contains(&running.path), "{body}");
            }
        }
        let (status, body) = call(
            running.sender.clone(),
            &format!("/tx_details/{unknown_signature}"),
        )
        .await;
        assert_eq!(status, 404);
        assert_eq!(body["error"], "Transaction not found");
        let (status, body) = call(
            running.sender.clone(),
            &format!("/account_balance/{unknown_address}?block_no=7"),
        )
        .await;
        assert_eq!(status, 404);
        assert_eq!(body["error"], "Account observation not found");
    }

    #[actix_web::test]
    async fn empty_database_balance_request_returns_instead_of_hanging() {
        let (sender, db_task, handler_task, _dir) = empty_service().await;
        let (status, body) =
            call(sender, "/account_balance/11111111111111111111111111111111").await;
        assert_eq!(status, 503);
        assert_eq!(body["error"], "No block has been indexed yet");
        db_task.abort();
        handler_task.abort();
    }

    #[actix_web::test]
    async fn valid_block_range_returns_only_stored_blocks() {
        let running = indexed().await;
        let (status, body) = call(running.sender.clone(), "/block_range/7/8").await;
        assert_eq!(status, 200);
        assert!(body["7"]["tx_map"][&running.signature].is_object());
        assert_eq!(body["8"]["account_map"][&running.observed], 42);
        assert!(body.get("9").is_none());
    }

    #[actix_web::test]
    async fn closed_handler_channel_is_service_unavailable() {
        let (sender, receiver) = crate::util::test_channel();
        drop(receiver);
        let (status, body) = call(sender, "/latest_block").await;
        assert_eq!(status, 503);
        assert_eq!(body["error"], "Database worker is unavailable");
    }

    #[actix_web::test]
    async fn closed_database_worker_is_service_unavailable() {
        let (handler_sender, handler_receiver) = crate::util::test_channel();
        let (db_sender, db_receiver) = crate::util::test_channel();
        drop(db_receiver);
        let mut handler = Handler::initialize(handler_receiver, db_sender);
        let handler_task = tokio::spawn(async move { handler.run().await });
        let (status, body) =
            call_with_timeout(handler_sender, "/latest_block", Duration::from_secs(1)).await;
        assert_eq!(status, 503);
        assert_eq!(body["error"], "Database worker is unavailable");
        handler_task.abort();
    }

    #[actix_web::test]
    async fn unanswered_database_request_times_out() {
        let (sender, _receiver) = crate::util::test_channel();
        let (status, body) =
            call_with_timeout(sender, "/latest_block", Duration::from_millis(50)).await;
        assert_eq!(status, 504);
        assert_eq!(body["error"], "Database request timed out");
    }

    #[actix_web::test]
    async fn corrupt_block_is_an_internal_error_without_a_database_path() {
        let dir = TempDir::new();
        let path = dir.path.to_string_lossy().to_string();
        let (db_sender, db_receiver) = crate::util::test_channel();
        let (handler_sender, handler_receiver) = crate::util::test_channel();
        let database = RocksDb::initialize(path.clone(), db_receiver).expect("db");
        database.corrupt_block_record(4).expect("corrupt");
        let db_task = tokio::spawn(async move { database.run().await });
        let mut handler = Handler::initialize(handler_receiver, db_sender);
        let handler_task = tokio::spawn(async move { handler.run().await });
        let (status, body) = call(handler_sender, "/block_details/4").await;
        assert_eq!(status, 500);
        assert_eq!(body["error"], "Internal storage error");
        let rendered = body.to_string();
        assert!(!rendered.contains(&path), "{rendered}");
        assert!(!rendered.contains("not-json"), "{rendered}");
        db_task.abort();
        handler_task.abort();
    }

    async fn call_state(state: ApiState, uri: &str) -> (u16, Value) {
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .configure(configure),
        )
        .await;
        let response =
            test::call_service(&app, test::TestRequest::get().uri(uri).to_request()).await;
        let status = response.status().as_u16();
        let body = test::read_body_json(response).await;
        (status, body)
    }

    #[actix_web::test]
    async fn health_is_ok_while_indexing_and_unavailable_when_a_worker_fails() {
        let (lifecycle, _shutdown) = crate::lifecycle::Lifecycle::new();
        lifecycle.set_phase(crate::lifecycle::ServicePhase::Indexing);
        let (sender, _receiver) = crate::util::test_channel();
        let (status, body) = call_state(
            ApiState::with_lifecycle(sender.clone(), std::sync::Arc::clone(&lifecycle)),
            "/health",
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body, serde_json::json!({"status": "healthy"}));

        lifecycle.set_phase(crate::lifecycle::ServicePhase::CaughtUp);
        let (status, body) = call_state(
            ApiState::with_lifecycle(sender.clone(), std::sync::Arc::clone(&lifecycle)),
            "/health",
        )
        .await;
        assert_eq!((status, body["status"].as_str()), (200, Some("healthy")));

        lifecycle.begin_shutdown();
        let (status, body) = call_state(
            ApiState::with_lifecycle(sender.clone(), std::sync::Arc::clone(&lifecycle)),
            "/health",
        )
        .await;
        assert_eq!(status, 503);
        assert_eq!(body["status"], "unhealthy");

        let (failed, _shutdown) = crate::lifecycle::Lifecycle::new();
        failed.fail("handler stopped unexpectedly".to_string());
        let (status, body) = call_state(ApiState::with_lifecycle(sender, failed), "/health").await;
        assert_eq!(status, 503);
        assert_eq!(body["status"], "unhealthy");
    }

    #[actix_web::test]
    async fn status_reports_the_durable_slot_without_inventing_a_block_or_leaking_secrets() {
        let (lifecycle, _shutdown) = crate::lifecycle::Lifecycle::new();
        lifecycle.note_checkpoint(80);
        let (sender, _receiver) = crate::util::test_channel();
        let (_status, fresh) = call_state(
            ApiState::with_lifecycle(sender.clone(), std::sync::Arc::clone(&lifecycle)),
            "/status",
        )
        .await;
        assert_eq!(fresh["state"], "starting");
        assert_eq!(fresh["last_processed_slot"], 80);
        assert!(fresh["last_indexed_block_height"].is_null());
        assert!(fresh["error"].is_null());

        lifecycle.set_phase(crate::lifecycle::ServicePhase::Indexing);
        lifecycle.note_checkpoint(11);
        lifecycle.note_block_height(4);
        lifecycle.note_finalized_tip(20);
        let (_status, indexed) = call_state(
            ApiState::with_lifecycle(sender.clone(), std::sync::Arc::clone(&lifecycle)),
            "/status",
        )
        .await;
        assert_eq!(indexed["state"], "indexing");
        assert_eq!(indexed["last_processed_slot"], 11);
        assert_eq!(indexed["last_indexed_block_height"], 4);
        assert_eq!(indexed["last_observed_finalized_slot"], 20);
        assert_ne!(
            indexed["last_processed_slot"],
            indexed["last_indexed_block_height"]
        );

        let (skipped, _shutdown) = crate::lifecycle::Lifecycle::new();
        skipped.note_checkpoint(12);
        skipped.set_phase(crate::lifecycle::ServicePhase::Indexing);
        let (_status, body) = call_state(
            ApiState::with_lifecycle(sender, std::sync::Arc::clone(&skipped)),
            "/status",
        )
        .await;
        assert_eq!(body["last_processed_slot"], 12);
        assert!(body["last_indexed_block_height"].is_null());
        let rendered = body.to_string();
        assert!(!rendered.contains("api-key"));
        assert!(!rendered.contains("super-secret"));
        assert!(!rendered.contains("/tmp"));
        assert!(!rendered.contains("RocksDB"));
    }
}
