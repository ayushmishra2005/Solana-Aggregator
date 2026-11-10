use structopt::StructOpt;

const DEFAULT_CHAIN_URL: &str = "https://api.devnet.solana.com";

#[derive(Debug, StructOpt)]
pub struct Cli {
    #[structopt(
        short = "s",
        long = "chain-url",
        env = "SOLANA_RPC_URL",
        hide_env_values = true,
        default_value = DEFAULT_CHAIN_URL
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

#[cfg(test)]
mod tests {
    use super::{Cli, DEFAULT_CHAIN_URL};
    use std::ffi::OsString;
    use std::sync::{Mutex, MutexGuard};
    use structopt::StructOpt;

    const ENV_KEY: &str = "SOLANA_RPC_URL";
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        previous: Option<OsString>,
    }

    impl EnvGuard {
        fn set(value: &str) -> Self {
            let previous = std::env::var_os(ENV_KEY);
            // SAFETY: CLI tests hold ENV_LOCK across the read and restore of this
            // variable. StructOpt reads it while the guard is alive.
            unsafe { std::env::set_var(ENV_KEY, value) };
            Self { previous }
        }

        fn unset() -> Self {
            let previous = std::env::var_os(ENV_KEY);
            // SAFETY: same critical section as `set`.
            unsafe { std::env::remove_var(ENV_KEY) };
            Self { previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: the matching constructor ran while ENV_LOCK was held, and
            // Drop runs before that guard is released.
            unsafe {
                match self.previous.take() {
                    Some(value) => std::env::set_var(ENV_KEY, value),
                    None => std::env::remove_var(ENV_KEY),
                }
            }
        }
    }

    fn lock_env() -> MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn parse(args: &[&str]) -> Cli {
        Cli::from_iter_safe(args).unwrap_or_else(|err| panic!("failed to parse {args:?}: {err}"))
    }

    #[test]
    fn default_rpc_url_contains_no_credential() {
        let _lock = lock_env();
        let _env = EnvGuard::unset();
        let cli = parse(&["solana-agg"]);

        assert_eq!(cli.chain_url, DEFAULT_CHAIN_URL);
        assert_eq!(cli.chain_url, "https://api.devnet.solana.com");
        let lower = cli.chain_url.to_ascii_lowercase();
        assert!(!lower.contains("api-key"));
        assert!(!lower.contains("api_key"));
        assert!(!lower.contains("apikey"));
        assert!(!lower.contains("helius"));
        assert!(!cli.chain_url.contains('?'));
        assert!(!cli.chain_url.contains('@'));
    }

    #[test]
    fn solana_rpc_url_configures_the_endpoint() {
        let _lock = lock_env();
        let _env = EnvGuard::set("https://private.example/rpc?api-key=synthetic-env-credential");
        let cli = parse(&["solana-agg"]);

        assert_eq!(
            cli.chain_url,
            "https://private.example/rpc?api-key=synthetic-env-credential"
        );

        let help = Cli::from_iter_safe(["solana-agg", "--help"]).expect_err("help is an error");
        let help = help.to_string();
        assert!(help.contains("SOLANA_RPC_URL"));
        assert!(help.contains(DEFAULT_CHAIN_URL));
        assert!(!help.contains("synthetic-env-credential"));
    }

    #[test]
    fn chain_url_flag_overrides_environment() {
        let _lock = lock_env();
        let _env = EnvGuard::set("https://from-env.example/?api-key=synthetic-env-credential");

        let long = parse(&["solana-agg", "--chain-url", "https://from-flag.example"]);
        assert_eq!(long.chain_url, "https://from-flag.example");

        let short = parse(&["solana-agg", "-s", "https://from-short-flag.example"]);
        assert_eq!(short.chain_url, "https://from-short-flag.example");
    }

    #[test]
    fn existing_cli_flags_remain_functional() {
        let _lock = lock_env();
        let _env = EnvGuard::unset();
        let defaults = parse(&["solana-agg"]);
        assert_eq!(
            defaults.db_path,
            "/Users/krishnasingh/Workspace/Official/solana-agg/db"
        );
        assert_eq!(defaults.port_no, "9944");

        let configured = parse(&[
            "solana-agg",
            "-s",
            "https://flags.example",
            "-d",
            "/tmp/solana-agg-db",
            "--port-no",
            "9999",
        ]);
        assert_eq!(configured.chain_url, "https://flags.example");
        assert_eq!(configured.db_path, "/tmp/solana-agg-db");
        assert_eq!(configured.port_no, "9999");

        let long_db = parse(&["solana-agg", "--db-url", "/var/tmp/solana-agg"]);
        assert_eq!(long_db.db_path, "/var/tmp/solana-agg");
        assert_eq!(long_db.chain_url, DEFAULT_CHAIN_URL);
        assert_eq!(long_db.port_no, "9944");
    }
}
