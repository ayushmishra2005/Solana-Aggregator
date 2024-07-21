use crate::block_importer::Subscriber;
use crate::db_handler::RocksDb;
use crate::error::AggError;
use crate::handler::Handler;
use crate::util::ProtocolMessage;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub struct SourceChain(String);
pub struct NoSourceChain;
pub struct DbPath(String);
pub struct NoDbPath;
pub struct DbSender(UnboundedSender<ProtocolMessage>);
pub struct NoDbSender;
pub struct DbReceiver(UnboundedReceiver<ProtocolMessage>);
pub struct NoDbReceiver;
pub struct HandlerSender(UnboundedSender<ProtocolMessage>);
pub struct NoHandlerSender;
pub struct HandlerReceiver(UnboundedReceiver<ProtocolMessage>);
pub struct NoHandlerReceiver;

pub struct Builder<ChainUrl, DBPath, DBSender, DBReceiver, RouterSender, RouterReceiver> {
    chain_url: ChainUrl,
    db_path: DBPath,
    db_sender: DBSender,
    db_receiver: DBReceiver,
    router_sender: RouterSender,
    router_receiver: RouterReceiver,
}

impl Default
    for Builder<
        NoSourceChain,
        NoDbPath,
        NoDbSender,
        NoDbReceiver,
        NoHandlerSender,
        NoHandlerReceiver,
    >
{
    fn default() -> Self {
        Builder {
            chain_url: NoSourceChain,
            db_path: NoDbPath,
            db_sender: NoDbSender,
            db_receiver: NoDbReceiver,
            router_sender: NoHandlerSender,
            router_receiver: NoHandlerReceiver,
        }
    }
}

impl<ChainUrl, DBPath, DBSender, DBReceiver, RouterSender, RouterReceiver>
    Builder<ChainUrl, DBPath, DBSender, DBReceiver, RouterSender, RouterReceiver>
{
    /// This function sets the chain url
    ///
    /// # Arguments
    ///
    /// * `chain_url` - A string slice that holds the chain url
    ///
    /// # Returns
    ///
    /// * `Builder<...>` - A Builder that holds the chain url
    pub fn chain_url(
        self,
        chain_url: String,
    ) -> Builder<SourceChain, DBPath, DBSender, DBReceiver, RouterSender, RouterReceiver> {
        Builder {
            chain_url: SourceChain(chain_url),
            db_path: self.db_path,
            db_sender: self.db_sender,
            db_receiver: self.db_receiver,
            router_sender: self.router_sender,
            router_receiver: self.router_receiver,
        }
    }

    /// This function sets the db path
    ///
    /// # Arguments
    ///
    /// * `db_path` - A string slice that holds the db path
    ///
    /// # Returns
    ///
    /// * `Builder<...>` - A Builder that holds the db path
    pub fn db_path(
        self,
        db_path: String,
    ) -> Builder<ChainUrl, DbPath, DBSender, DBReceiver, RouterSender, RouterReceiver> {
        Builder {
            chain_url: self.chain_url,
            db_path: DbPath(db_path),
            db_sender: self.db_sender,
            db_receiver: self.db_receiver,
            router_sender: self.router_sender,
            router_receiver: self.router_receiver,
        }
    }

    /// This function sets the db sender
    ///
    /// # Arguments
    ///
    /// * `db_sender` - A UnboundedSender<ProtocolMessage> that holds the db sender
    ///
    /// # Returns
    ///
    /// * `Builder<...>` - A Builder that holds the db sender
    pub fn db_sender(
        self,
        db_sender: UnboundedSender<ProtocolMessage>,
    ) -> Builder<ChainUrl, DBPath, DbSender, DBReceiver, RouterSender, RouterReceiver> {
        Builder {
            chain_url: self.chain_url,
            db_path: self.db_path,
            db_sender: DbSender(db_sender),
            db_receiver: self.db_receiver,
            router_sender: self.router_sender,
            router_receiver: self.router_receiver,
        }
    }

    /// This function sets the db receiver
    ///
    /// # Arguments
    ///
    /// * `db_receiver` - A UnboundedReceiver<ProtocolMessage> that holds the db receiver
    pub fn db_receiver(
        self,
        db_receiver: UnboundedReceiver<ProtocolMessage>,
    ) -> Builder<ChainUrl, DBPath, DBSender, DbReceiver, RouterSender, RouterReceiver> {
        Builder {
            chain_url: self.chain_url,
            db_path: self.db_path,
            db_sender: self.db_sender,
            db_receiver: DbReceiver(db_receiver),
            router_sender: self.router_sender,
            router_receiver: self.router_receiver,
        }
    }

    /// This function sets the router sender
    ///
    /// # Arguments
    ///
    /// * `router_sender` - A UnboundedSender<ProtocolMessage> that holds the router sender
    ///
    /// # Returns
    ///
    /// * `Builder<...>` - A Builder that holds the router sender
    pub fn router_sender(
        self,
        router_sender: UnboundedSender<ProtocolMessage>,
    ) -> Builder<ChainUrl, DBPath, DBSender, DBReceiver, HandlerSender, RouterReceiver> {
        Builder {
            chain_url: self.chain_url,
            db_path: self.db_path,
            db_sender: self.db_sender,
            db_receiver: self.db_receiver,
            router_sender: HandlerSender(router_sender),
            router_receiver: self.router_receiver,
        }
    }

    /// This function sets the router receiver
    ///
    /// # Arguments
    ///
    /// * `router_receiver` - A UnboundedReceiver<ProtocolMessage> that holds the router receiver
    ///
    /// # Returns
    ///
    /// * `Builder<...>` - A Builder that holds the router receiver
    pub fn router_receiver(
        self,
        router_receiver: UnboundedReceiver<ProtocolMessage>,
    ) -> Builder<ChainUrl, DBPath, DBSender, DBReceiver, RouterSender, HandlerReceiver> {
        Builder {
            chain_url: self.chain_url,
            db_path: self.db_path,
            db_sender: self.db_sender,
            db_receiver: self.db_receiver,
            router_sender: self.router_sender,
            router_receiver: HandlerReceiver(router_receiver),
        }
    }
}

impl Builder<SourceChain, NoDbPath, NoDbSender, NoDbReceiver, HandlerSender, NoHandlerReceiver> {
    pub fn build(self) -> Result<Subscriber, AggError> {
        Subscriber::initialize(self.chain_url.0, self.router_sender.0)
    }
}

impl Builder<NoSourceChain, DbPath, NoDbSender, DbReceiver, NoHandlerSender, NoHandlerReceiver> {
    pub fn build(self) -> Result<RocksDb, AggError> {
        RocksDb::initialize(self.db_path.0, self.db_receiver.0)
    }
}

impl Builder<NoSourceChain, NoDbPath, DbSender, NoDbReceiver, NoHandlerSender, HandlerReceiver> {
    pub fn build(self) -> Handler {
        Handler::initialize(self.router_receiver.0, self.db_sender.0)
    }
}
