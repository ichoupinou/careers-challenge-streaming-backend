use std::net::SocketAddr;
use std::sync::Arc;

use stream_service::{api, app};
use stream_service::clock::SystemClock;
use stream_service::config::Config;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    init_tracing(config.log_json);
    tracing::info!(?config, "starting");

    let service = app::start(config.clone(), Arc::new(SystemClock))?;
    let state = service.app.clone();

    let snapshotter = service.snapshotter.clone();
    let every = config.snapshot_interval;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            let s = snapshotter.clone();
            match tokio::task::spawn_blocking(move || s.run_once()).await {
                Ok(Ok(Some(info))) => tracing::info!(?info, "snapshot written"),
                Ok(Ok(None)) => {}
                Ok(Err(e)) => tracing::error!(error = %e, "snapshot failed"),
                Err(e) => tracing::error!(error = %e, "snapshot task panicked"),
            }
        }
    });

    let housekeeping = state.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tick.tick().await;
            housekeeping.prune();
        }
    });

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "listening");
    let closer = state.clone();
    axum::serve(listener, api::router(state))
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            // End open SSE streams so in-flight connections can finish.
            closer.begin_shutdown();
        })
        .await?;
    // Drain queued events into the WAL, fsync, and write a final snapshot.
    tokio::task::spawn_blocking(move || service.shutdown()).await?;
    tracing::info!("stopped");
    Ok(())
}

fn init_tracing(json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
    tracing::info!("shutdown signal received");
}
