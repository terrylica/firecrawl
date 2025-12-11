#![feature(ip)]

mod config;
mod consumer;
mod dispatcher;
mod models;
mod signature;

use anyhow::Result;
use tokio::signal;
use tokio::sync::broadcast;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_span_list(false),
        )
        .try_init()?;

    info!("Starting webhook dispatcher");

    let config = config::Config::from_env()?;
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    let consumer_handle = tokio::spawn(async move { consumer::run(config, shutdown_rx).await });

    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Received SIGINT (Ctrl+C)");
        }
        _ = async {
            #[cfg(unix)]
            {
                match signal::unix::signal(signal::unix::SignalKind::terminate()) {
                    Ok(mut sig) => sig.recv().await,
                    Err(e) => {
                        error!(error = %e, "Failed to install SIGTERM handler");
                        None
                    }
                }
            }
            #[cfg(not(unix))]
            {
                std::future::pending::<()>().await
            }
        } => {
            info!("Received SIGTERM");
        }
    }
    info!("Shutdown signal received");

    let _ = shutdown_tx.send(());

    match consumer_handle.await {
        Ok(Ok(())) => info!("Consumer shutdown successfully"),
        Ok(Err(e)) => error!(error = %e, "Consumer exited with error"),
        Err(e) => error!(error = %e, "Consumer panicked"),
    }

    Ok(())
}
