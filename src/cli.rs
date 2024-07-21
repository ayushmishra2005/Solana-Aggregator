use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct Cli {
    #[structopt(
        short = "s",
        long = "chain-url",
        default_value = "https://devnet.helius-rpc.com/?api-key=5f5e0b82-0a0e-4638-b095-8077e2d8c9b8"
    )]
    pub chain_url: String,

    #[structopt(
        short = "d",
        long = "db-url",
        default_value = "/Users/krishnasingh/Workspace/Official/solana-agg/db"
    )]
    pub db_path: String,

    #[structopt(short = "", long = "port-no", default_value = "9944")]
    pub port_no: String,
}
