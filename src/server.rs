use crate::error::AggError;
use crate::util::{Channel, ProtocolMessage, QueryParams};
use actix_web::{get, middleware, web, App, HttpResponse, HttpServer, Responder};
use tokio::sync::mpsc::UnboundedSender;

pub(crate) struct AggServer;

impl AggServer {

    /// This function runs the server
    ///
    /// # Arguments
    ///
    /// * `handler_sender` - A UnboundedSender<ProtocolMessage> that holds the handler sender
    /// * `port_no` - A string slice that holds the port number
    ///
    /// # Returns
    ///
    /// * `Result<(), AggError>` - A Result that holds the result or an error
    pub async fn run(
        handler_sender: UnboundedSender<ProtocolMessage>,
        port_no: String,
    ) -> Result<(), AggError> {
        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(handler_sender.clone()))
                .wrap(middleware::Logger::default())
                .service(get_tx_details)
                .service(get_block_details)
                .service(get_latest_block)
                .service(get_block_range)
                .service(get_account_balance)
        })
        .bind(format!("127.0.0.1:{port_no}"))?
        .run()
        .await?;
        Ok(())
    }
}

#[get("/tx_details/{tx_id}")]
async fn get_tx_details(
    tx_id: web::Path<String>,
    sender: web::Data<UnboundedSender<ProtocolMessage>>,
) -> impl Responder {
    let mut channel = Channel::<ProtocolMessage>::new();
    if let Err(error) = sender.send(ProtocolMessage::FetchTransactionDetails(
        tx_id.into_inner(),
        channel.sender(),
    )) {
        return HttpResponse::InternalServerError().json(error.to_string());
    }
    match channel.receiver.recv().await {
        Some(ProtocolMessage::TxDetails(tx)) => HttpResponse::Ok().json(tx),
        Some(ProtocolMessage::Error(err)) => HttpResponse::InternalServerError().json(err),
        _ => HttpResponse::InternalServerError().finish(),
    }
}

#[get("/block_details/{block_no}")]
async fn get_block_details(
    block_no: web::Path<String>,
    sender: web::Data<UnboundedSender<ProtocolMessage>>,
) -> impl Responder {
    let mut channel = Channel::<ProtocolMessage>::new();
    if let Err(error) = sender.send(ProtocolMessage::FetchBlockDetails(
        block_no.into_inner(),
        channel.sender(),
    )) {
        return HttpResponse::InternalServerError().json(error.to_string());
    }
    match channel.receiver.recv().await {
        Some(ProtocolMessage::BlockDetails(block)) => HttpResponse::Ok().json(block),
        _ => HttpResponse::InternalServerError().finish(),
    }
}

#[get("/latest_block")]
async fn get_latest_block(sender: web::Data<UnboundedSender<ProtocolMessage>>) -> impl Responder {
    let mut channel = Channel::<ProtocolMessage>::new();
    if let Err(error) = sender.send(ProtocolMessage::FetchLatestBlock(channel.sender())) {
        return HttpResponse::InternalServerError().json(error.to_string());
    }
    match channel.receiver.recv().await {
        Some(ProtocolMessage::LatestBlockDetails(block_no, block)) => {
            HttpResponse::Ok().json((block_no, block))
        }
        _ => HttpResponse::InternalServerError().finish(),
    }
}

#[get("/block_range/{start}/{end}")]
async fn get_block_range(
    range: web::Path<(u64, u64)>,
    sender: web::Data<UnboundedSender<ProtocolMessage>>,
) -> impl Responder {
    let mut channel = Channel::<ProtocolMessage>::new();
    let (start, end) = range.into_inner();
    if let Err(err) = sender.send(ProtocolMessage::FetchBlockRange(
        start,
        end,
        channel.sender(),
    )) {
        return HttpResponse::InternalServerError().json(err.to_string());
    }
    match channel.receiver.recv().await {
        Some(ProtocolMessage::BlockRangeDetails(blocks)) => HttpResponse::Ok().json(blocks),
        _ => HttpResponse::InternalServerError().finish(),
    }
}

#[get("/account_balance/{account_id}")]
async fn get_account_balance(
    account_id: web::Path<String>,
    query: web::Query<QueryParams>,
    sender: web::Data<UnboundedSender<ProtocolMessage>>,
) -> impl Responder {
    let mut channel = Channel::<ProtocolMessage>::new();
    if let Err(error) = sender.send(ProtocolMessage::FetchAccountBalance(
        account_id.into_inner(),
        query.into_inner().block_no,
        channel.sender(),
    )) {
        return HttpResponse::InternalServerError().json(error.to_string());
    }
    match channel.receiver.recv().await {
        Some(ProtocolMessage::AccountBalance(balance)) => HttpResponse::Ok().json(balance),
        _ => HttpResponse::InternalServerError().finish(),
    }
}

// Curl Requests
// curl -X GET "http://127.0.0.1:8080/tx_details/1234" -H "accept: application/json" -d ""
// curl -X GET "http://127.0.0.1:9944/tx_details/9944" -H "accept: application/json" -d ""
//curl -X GET "http://127.0.0.1:9944/tx_details/5erfSq9i9UasLUt366TRPAK1W3peaJn42kKQa8F9qGDd" -H "accept: application/json" -d ""
//curl -X GET "http://127.0.0.1:9944/latest_block" -H "accept: application/json" -d ""
//curl -X GET "http://127.0.0.1:9944/block_range/301265452/301265544" -H "accept: application/json" -d ""
//curl -X GET "http://127.0.0.1:9944/account_balance/vgcDar2pryHvMgPkKaZfh8pQy4BJxv7SpwUG7zinWjG" -H "accept: application/json" -d ""
