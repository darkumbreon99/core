#![allow(unused_crate_dependencies)]

use anyhow::Result;
use arti_client::{TorClient, config::TorClientConfigBuilder};
use futures::StreamExt;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::core::upgrade::Version;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{PeerId, SwarmBuilder, Transport, identity, yamux};
use libp2p::{dns, tcp};
use libp2p::{identify, noise, ping};
use libp2p_tor::{AddressConversion, TorTransport};
use std::sync::Arc;
use std::time::Duration;
use swap_p2p::libp2p_ext::MultiAddrExt;
use swap_p2p::protocols::{quotes_cached, rendezvous};
use tor_rtcompat::tokio::TokioRustlsRuntime;

const USE_TOR: bool = true;

#[derive(NetworkBehaviour)]
struct Behaviour {
    rendezvous: rendezvous::discovery::Behaviour,
    ping: ping::Behaviour,
    quote: quotes_cached::Behaviour,
}

fn create_transport(
    identity: &identity::Keypair,
    tor_client: Option<Arc<TorClient<TokioRustlsRuntime>>>,
) -> Result<Boxed<(PeerId, StreamMuxerBox)>> {
    let auth_upgrade = noise::Config::new(identity)?;
    let multiplex_upgrade = yamux::Config::default();

    if let Some(tor_client) = tor_client {
        let transport = TorTransport::from_client(tor_client, AddressConversion::IpAndDns)
            .upgrade(Version::V1)
            .authenticate(auth_upgrade)
            .multiplex(multiplex_upgrade)
            .timeout(Duration::from_secs(60))
            .map(|(peer, muxer), _| (peer, StreamMuxerBox::new(muxer)))
            .boxed();
        Ok(transport)
    } else {
        // TCP with system DNS
        let tcp = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true));
        let tcp_dns = dns::tokio::Transport::system(tcp)?;
        let transport = tcp_dns
            .upgrade(Version::V1)
            .authenticate(auth_upgrade)
            .multiplex(multiplex_upgrade)
            .timeout(Duration::from_secs(60))
            .map(|(peer, muxer), _| (peer, StreamMuxerBox::new(muxer)))
            .boxed();
        Ok(transport)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "info,swap_p2p=trace,fetch_quotes=trace,libp2p_request_response=trace,libp2p_swarm=debug",
        ))
        .try_init();

    let identity = identity::Keypair::generate_ed25519();

    let tor_client_opt = if USE_TOR {
        let config = TorClientConfigBuilder::default().build()?;
        let runtime = TokioRustlsRuntime::current()?;
        let tor_client = TorClient::with_runtime(runtime)
            .config(config)
            .create_bootstrapped()
            .await?;
        Some(tor_client)
    } else {
        None
    };

    let rendezvous_nodes = swap_env::defaults::default_rendezvous_points();
    let rendezvous_nodes_peer_ids = rendezvous_nodes
        .iter()
        .map(|addr| {
            addr.extract_peer_id()
                .expect("Rendezvous node address must contain peer ID")
        })
        .collect();

    let namespace = rendezvous::XmrBtcNamespace::Mainnet;

    let behaviour = Behaviour {
        rendezvous: rendezvous::discovery::Behaviour::new(
            identity.clone(),
            rendezvous_nodes_peer_ids,
            namespace.into(),
        ),
        ping: ping::Behaviour::new(ping::Config::new().with_interval(Duration::from_secs(1))),
        quote: quotes_cached::Behaviour::new(identify::Config::new(
            "fetch_quotes/1.0.0".to_string(),
            identity.public(),
        )),
    };

    let transport = create_transport(&identity, tor_client_opt)?;

    let mut swarm = SwarmBuilder::with_existing_identity(identity)
        .with_tokio()
        .with_other_transport(|_| transport)?
        .with_behaviour(|_| behaviour)?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    for rendezvous_node_addr in rendezvous_nodes {
        swarm.add_peer_address(
            rendezvous_node_addr
                .extract_peer_id()
                .expect("Rendezvous node address must contain peer ID"),
            rendezvous_node_addr,
        );
    }

    // Discover for a fixed window, then print the fullest cached-quotes snapshot.
    // emit_cached_quotes fires per QuoteReceived, so early snapshots are sparse; we
    // keep the latest cumulative one (formatted to strings to dodge type plumbing)
    // and print it once at the deadline. FETCH_QUOTES_SECS overrides the window.
    let window = std::env::var("FETCH_QUOTES_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(90);
    let deadline = tokio::time::sleep(Duration::from_secs(window));
    tokio::pin!(deadline);
    let mut latest_lines: Vec<String> = Vec::new();

    loop {
        tokio::select! {
            _ = &mut deadline => {
                use std::io::Write;
                let mut out = std::io::stdout().lock();
                for line in &latest_lines {
                    let _ = writeln!(out, "{line}");
                }
                let _ = out.flush();
                std::process::exit(0);
            }
            event = swarm.select_next_some() => {
                if let libp2p::swarm::SwarmEvent::Behaviour(BehaviourEvent::Quote(
                    quotes_cached::Event::CachedQuotes { quotes },
                )) = event
                {
                    // price is BTC per XMR; format matches eigen_scan.py's parser.
                    latest_lines = quotes
                        .iter()
                        .map(|(peer, addr, quote, agent_version)| {
                            format!(
                                "price={:.8} BTC min_quantity={:.8} BTC max_quantity={:.8} BTC address={} peer_id={} version={}",
                                quote.price.to_btc(),
                                quote.min_quantity.to_btc(),
                                quote.max_quantity.to_btc(),
                                addr,
                                peer,
                                agent_version.as_ref().map(|v| v.to_string()).unwrap_or_default(),
                            )
                        })
                        .collect();
                }
            }
        }
    }
}
