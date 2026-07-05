//! Headless BTC -> XMR swap executor.
//!
//! This is the CLI/automation counterpart to the eigenwallet GUI's swap flow.
//! The 4.x `swap` CLI dropped the interactive `buy-xmr` subcommand because maker
//! selection moved into the Tauri frontend; this example restores a non-interactive
//! path by building the same headless [`Context`] the GUI uses and calling
//! [`buy_xmr_with_seller`], which auto-selects the maker given on `--seller`.
//!
//! Usage:
//!   buy_xmr_cli \
//!       --seller /onion3/<b32>:9939/p2p/12D3Koo... \
//!       --receive-address 4... \
//!       [--change-address bc1...] \
//!       [--data-dir <dir>]        # reuse an existing (funded) internal wallet
//!       [--electrum-rpc ssl://host:port] \
//!       [--no-tor] [--testnet]
//!
//! It swaps whatever spendable BTC sits in the internal wallet (clamped to the
//! maker's min/max), then blocks until the atomic swap completes.
#![allow(unused_crate_dependencies)]

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context as _, Result, bail};
use libp2p::Multiaddr;
use uuid::Uuid;

use swap::cli::api::request::buy_xmr_with_seller;
use swap::cli::api::tauri_bindings::MoneroNodeConfig;
use swap::cli::api::{Context, ContextBuilder};
use swap::cli::command::Bitcoin;
use swap::monero::MoneroAddressPool;
use swap_env::defaults::default_rendezvous_points;
use swap_p2p::libp2p_ext::{MultiAddrExt, MultiAddrVecExt};

const HELP: &str = "\
buy_xmr_cli -- headless BTC->XMR atomic swap with a chosen maker

REQUIRED:
    --seller <MULTIADDR>          Maker address incl. /p2p/<peer-id>
    --receive-address <XMR_ADDR>  Where to receive the XMR

OPTIONAL:
    --change-address <BTC_ADDR>   BTC change address (default: internal wallet)
    --data-dir <DIR>              Data dir to reuse (e.g. the eigenwallet GUI's,
                                  so the funded internal wallet is shared)
    --electrum-rpc <URL>          Electrum RPC URL (repeatable; default: built-in)
    --no-tor                      Do not route libp2p over Tor (default: Tor on)
    --tor-socks-port <PORT>       Route onion dials through an EXTERNAL Tor daemon's
                                  SOCKS5 proxy on 127.0.0.1:<PORT> (e.g. Tor Browser =
                                  9150, tor service = 9050) instead of the embedded arti
                                  client. Far more reliable for onion services. Keep Tor
                                  Browser / tor running while the swap executes.
    --testnet                     Use testnet / stagenet defaults
    -h, --help                    Show this help
";

struct Args {
    seller: Multiaddr,
    receive_address: String,
    change_address: Option<String>,
    data_dir: Option<PathBuf>,
    electrum: Vec<String>,
    tor: bool,
    tor_socks_port: Option<u16>,
    testnet: bool,
}

fn parse_args() -> Result<Args> {
    let mut seller: Option<Multiaddr> = None;
    let mut receive_address: Option<String> = None;
    let mut change_address: Option<String> = None;
    let mut data_dir: Option<PathBuf> = None;
    let mut electrum: Vec<String> = Vec::new();
    let mut tor = true;
    let mut tor_socks_port: Option<u16> = None;
    let mut testnet = false;

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--seller" => {
                let v = it.next().context("--seller requires a value")?;
                seller = Some(v.parse().context("invalid --seller multiaddr")?);
            }
            "--receive-address" => {
                receive_address = Some(it.next().context("--receive-address requires a value")?);
            }
            "--change-address" => {
                change_address = Some(it.next().context("--change-address requires a value")?);
            }
            "--data-dir" => {
                data_dir = Some(PathBuf::from(
                    it.next().context("--data-dir requires a value")?,
                ));
            }
            "--electrum-rpc" => {
                electrum.push(it.next().context("--electrum-rpc requires a value")?);
            }
            "--no-tor" => tor = false,
            "--tor-socks-port" => {
                let v = it.next().context("--tor-socks-port requires a value")?;
                tor_socks_port = Some(v.parse().context("invalid --tor-socks-port")?);
            }
            "--testnet" => testnet = true,
            "-h" | "--help" => {
                print!("{HELP}");
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}\n\n{HELP}"),
        }
    }

    Ok(Args {
        seller: seller.context("--seller is required")?,
        receive_address: receive_address.context("--receive-address is required")?,
        change_address,
        data_dir,
        electrum,
        tor,
        tor_socks_port,
        testnet,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    // Same startup step the `swap` binary performs (swap/src/bin/swap.rs): rustls 0.23
    // can't auto-select a crypto provider when several are in the dependency graph, so
    // arti/tor would panic without this. Must run before any TLS is used.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install default rustls provider");

    let args = parse_args()?;

    // Route onion dials through an external Tor daemon's SOCKS5 proxy instead of the
    // embedded arti client (libp2p-tor reads this env var in its transport `dial`).
    // Set here, at startup, before the Context builds the swarm and any dial happens —
    // no other thread reads the env yet, so this is sound despite `set_var` being
    // `unsafe` in the 2024 edition.
    if let Some(port) = args.tor_socks_port {
        unsafe {
            std::env::set_var("SWAP_TOR_SOCKS_PORT", port.to_string());
        }
        tracing::info!(port, "Routing onion dials via external Tor SOCKS5 proxy");
    }

    // The seller multiaddr must carry the peer id we auto-select.
    let seller_peer_id = args
        .seller
        .extract_peer_id()
        .context("--seller must contain a /p2p/<peer-id> component")?;

    // Single-recipient Monero pool (100% to the given address).
    let monero_address =
        monero_address::MoneroAddress::from_str_with_unchecked_network(&args.receive_address)
            .context("invalid --receive-address")?;
    let monero_receive_pool = MoneroAddressPool::from(monero_address);

    // Optional BTC change address (network is checked inside buy_xmr_with_seller).
    let bitcoin_change_address = match args.change_address.as_deref() {
        Some(s) => Some(
            bitcoin::Address::from_str(s).context("invalid --change-address")?,
        ),
        None => None,
    };

    // Default rendezvous points -> (peer_id, [addr]) for discovery. The seller is
    // ALSO dialed directly inside buy_xmr_with_seller, so this is belt-and-suspenders.
    let rendezvous_points = default_rendezvous_points().extract_peer_addresses();

    // Build the same headless Context the GUI builds. No Tauri handle => the BTC
    // lock is auto-approved and maker selection is driven by our peer-id filter.
    let context = Arc::new(Context::new_without_tauri_handle());
    ContextBuilder::new(args.testnet)
        .with_bitcoin(Bitcoin {
            bitcoin_electrum_rpc_urls: args.electrum.clone(),
            bitcoin_target_block: None,
        })
        .with_monero(MoneroNodeConfig::Pool)
        .with_tor(args.tor)
        .with_data_dir(args.data_dir.clone())
        .with_rendezvous_points(rendezvous_points)
        .with_json(false)
        .build(context.clone())
        .await
        .context("failed to initialize swap context")?;

    let swap_id = Uuid::new_v4();
    tracing::info!(%swap_id, seller = %args.seller, "Starting headless BTC->XMR swap");

    buy_xmr_with_seller(
        args.seller,
        seller_peer_id,
        bitcoin_change_address,
        monero_receive_pool,
        swap_id,
        context.clone(),
    )
    .await
    .context("buy_xmr_with_seller failed")?;

    // buy_xmr_with_seller runs the swap as a background task; block until it ends.
    context
        .tasks
        .wait_for_tasks()
        .await
        .context("swap task failed")?;

    tracing::info!(%swap_id, "swap finished");
    Ok(())
}
