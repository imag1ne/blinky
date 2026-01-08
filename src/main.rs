use tokio::signal;

use blinky::config;
use blinky::shortcode::base58::Base58Shortcode;
use blinky::startup;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load configuration with graceful error handling
    let config = match config::AppConfig::from_env() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Failed to load configuration: {}", e);
            eprintln!("Please check your environment variables and configuration.");
            eprintln!("Required: DATABASE_URL");
            std::process::exit(1);
        }
    };

    // Initialize logging
    let _guard = match startup::init_logging(&config) {
        Ok(guard) => Some(guard),
        Err(e) => {
            eprintln!("Failed to initialize logging: {}", e);
            eprintln!("Continuing with default logging configuration...");
            // Note: We continue execution even if logging fails - the service can still run
            None
        }
    };

    // Initialize shortcode generator
    let shortcode_generator = Base58Shortcode;
    Base58Shortcode::warm_up(); // Warm up the RNG to avoid blocking later

    // Initialize storage (PostgreSQL + optional Redis cache)
    let storage = startup::init_storage(&config).await?;

    // Initialize queue and worker (optional, requires cached storage)
    let worker = startup::init_queue_and_worker(&config, &storage).await;

    // Build application state and router
    let queue = worker.as_ref().map(|w| w.queue.clone());
    let router = startup::build_app_state(&config, storage, shortcode_generator, queue);

    // Apply middleware (CORS)
    let router = blinky::cors::apply_cors(router, &config)?;

    // Start server
    let addr = config.to_socket_addr().await?;
    startup::start_server(router, addr, shutdown_signal()).await?;

    // Shutdown worker if it exists
    if let Some(worker) = worker {
        startup::shutdown_worker(worker).await;
    }

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! { _ = ctrl_c => {}, _ = terminate => {}, }
    tracing::info!("Shutdown signal received");
}
