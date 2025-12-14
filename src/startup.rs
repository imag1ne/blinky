use std::{net::SocketAddr, time::Duration};

use axum::Router;
use tokio::task::JoinHandle;
use tracing_appender::non_blocking::WorkerGuard;

use crate::{
    analytics::{ClickEventQueue, ClickEventWorker, worker::WorkerShutdownHandle},
    config::{AppConfig, LogFormat},
    error::BlinkyError,
    logging::try_init_subscriber,
    shortcode::GenerateShortcode,
    state::AppState,
    storage::{cached::CachedStorage, postgresql::PgStorage},
};

/// Runtime context for the background worker
pub struct WorkerRuntime {
    pub shutdown_handle: WorkerShutdownHandle,
    pub join_handle: JoinHandle<()>,
    pub queue: ClickEventQueue,
}

/// Storage implementation - either cached or direct DB
pub enum StorageImpl {
    Cached(CachedStorage<PgStorage>),
    Direct(PgStorage),
}

/// Initialize logging subsystem
///
/// Returns Ok(()) on success, or Err if logging failed to initialize.
/// The application can continue running even if logging fails.
pub fn init_logging(config: &AppConfig) -> Result<WorkerGuard, BlinkyError> {
    let (stdout, guard) = tracing_appender::non_blocking(std::io::stdout());

    // Note: _guard is dropped here, but that's OK because we're using non_blocking
    // which maintains the writer internally
    match config.log_format {
        LogFormat::Json => {
            let subscriber =
                crate::logging::build_json_subscriber("blinky", &config.rust_log, stdout);
            try_init_subscriber(subscriber)?;
        }
        LogFormat::Plain => {
            let subscriber = crate::logging::build_plain_subscriber(&config.rust_log, stdout);
            try_init_subscriber(subscriber)?;
        }
    }

    Ok(guard)
}

/// Initialize storage layer (PostgreSQL + optional Redis cache)
///
/// Returns the storage implementation. If Redis cache is configured but fails to
/// initialize, falls back to direct PostgreSQL access.
pub async fn init_storage(config: &AppConfig) -> Result<StorageImpl, BlinkyError> {
    // Always initialize PostgreSQL first
    let pg_storage = PgStorage::try_from_database_url_with_pool_size(
        config.database_url.as_str(),
        config.db_pool_size,
    )
    .await?;

    // Try to initialize Redis cache if configured
    if let Some(cache_url) = &config.redis_cache_url {
        tracing::info!(cache_url = %cache_url, "Initializing Redis cache");

        match CachedStorage::try_from_redis_url(
            pg_storage.clone(),
            cache_url.as_str(),
            config.cache_ttl_secs,
        )
        .await
        {
            Ok(cached_storage) => {
                tracing::info!("Redis cache initialized successfully");
                Ok(StorageImpl::Cached(cached_storage))
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to initialize Redis cache, falling back to DB-only mode"
                );
                Ok(StorageImpl::Direct(pg_storage))
            }
        }
    } else {
        tracing::info!("Redis cache not configured, using DB-only mode");
        Ok(StorageImpl::Direct(pg_storage))
    }
}

/// Initialize click event queue and worker
///
/// Returns None if:
/// - Queue URL is not configured
/// - Queue initialization fails
///
/// This function gracefully handles failures - if the queue can't be initialized,
/// the application continues without async analytics.
pub async fn init_queue_and_worker(
    config: &AppConfig,
    storage: &StorageImpl,
) -> Option<WorkerRuntime> {
    // Check if queue URL is configured
    let queue_url = config.redis_queue_url.as_ref()?;

    tracing::info!(queue_url = %queue_url, "Initializing Redis queue");

    // Try to initialize queue
    let queue = match ClickEventQueue::try_from_redis_url(
        queue_url.as_str(),
        config.queue_stream_key.clone(),
        config.queue_dlq_stream_key.clone(),
    )
    .await
    {
        Ok(q) => {
            let q = q.with_max_retries(config.queue_max_retries);
            tracing::info!(
                max_retries = %config.queue_max_retries,
                "Redis queue initialized successfully"
            );
            q
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "Failed to initialize queue, analytics will be disabled"
            );
            return None;
        }
    };

    // Configure and start worker
    let block_timeout_ms = config.queue_block_timeout_ms.min(usize::MAX as u64) as usize;
    let min_idle_time_ms = config.queue_min_idle_time_ms.min(usize::MAX as u64) as usize;

    let worker = ClickEventWorker::new(queue.clone(), config.queue_worker_id.clone())
        .with_consumer_group(config.queue_consumer_group.clone())
        .with_batch_size(config.queue_batch_size)
        .with_poll_interval(Duration::from_millis(config.queue_poll_interval_ms))
        .with_block_timeout_ms(block_timeout_ms)
        .with_min_idle_time_ms(min_idle_time_ms);

    tracing::info!(
        consumer_group = %config.queue_consumer_group,
        worker_id = %config.queue_worker_id,
        min_idle_time_ms = %min_idle_time_ms,
        "Click event worker configured"
    );

    let shutdown_handle = worker.shutdown_handle();

    // Start worker with appropriate storage type
    let join_handle = match storage {
        StorageImpl::Cached(cached) => {
            let storage_for_worker = cached.clone();
            tokio::spawn(async move {
                worker.run(storage_for_worker).await;
            })
        }
        StorageImpl::Direct(pg) => {
            let storage_for_worker = pg.clone();
            tokio::spawn(async move {
                worker.run(storage_for_worker).await;
            })
        }
    };

    tracing::info!("Click event worker started");

    Some(WorkerRuntime {
        shutdown_handle,
        join_handle,
        queue,
    })
}

/// Build application state based on storage implementation and optional queue
pub fn build_app_state<Gen>(
    config: &AppConfig,
    storage: StorageImpl,
    generator: Gen,
    queue: Option<ClickEventQueue>,
) -> Router
where
    Gen: GenerateShortcode + Clone + Send + Sync + 'static,
{
    match storage {
        StorageImpl::Cached(cached_storage) => {
            let mut state = AppState::new(
                config.base_url.clone(),
                cached_storage,
                generator,
                config.shortcode_max_retries,
            );

            // Add queue to state if available
            if let Some(q) = queue {
                state = state.with_click_queue(q);
            }

            crate::routes::build_router(state)
        }
        StorageImpl::Direct(pg_storage) => {
            let mut state = AppState::new(
                config.base_url.clone(),
                pg_storage,
                generator,
                config.shortcode_max_retries,
            );

            if let Some(q) = queue {
                state = state.with_click_queue(q);
            }

            crate::routes::build_router(state)
        }
    }
}

/// Gracefully shutdown the worker
///
/// This function:
/// 1. Logs queue stats before shutdown
/// 2. Signals worker to stop
/// 3. Waits up to 30 seconds for graceful shutdown
/// 4. Logs final queue stats
/// 5. Warns if messages remain unprocessed
pub async fn shutdown_worker(worker: WorkerRuntime) {
    tracing::info!("Shutting down click event worker");

    // Get queue stats before shutdown for observability
    if let Ok(stats) = worker.queue.stats().await {
        tracing::info!(
            pending_messages = %stats.total_messages,
            dlq_messages = %stats.dlq_messages,
            "Worker shutdown initiated with queue stats"
        );
    }

    worker.shutdown_handle.shutdown();

    match tokio::time::timeout(Duration::from_secs(30), worker.join_handle).await {
        Ok(Ok(_)) => {
            tracing::info!("Click event worker stopped gracefully");

            // Log final queue stats after shutdown
            if let Ok(final_stats) = worker.queue.stats().await {
                if final_stats.total_messages > 0 {
                    tracing::warn!(
                        unprocessed_messages = %final_stats.total_messages,
                        "Worker shutdown complete with unprocessed messages remaining"
                    );
                } else {
                    tracing::info!("Worker shutdown complete, all messages processed");
                }
            }
        }
        Ok(Err(e)) => {
            tracing::error!(
                error = %e,
                "Click event worker task panicked during shutdown"
            );
        }
        Err(_) => {
            tracing::error!("Click event worker shutdown timed out after 30 seconds");

            // Log queue state on timeout for debugging
            if let Ok(timeout_stats) = worker.queue.stats().await {
                tracing::error!(
                    pending_messages = %timeout_stats.total_messages,
                    dlq_messages = %timeout_stats.dlq_messages,
                    "Worker shutdown timed out, messages may be lost or reprocessed on restart"
                );
            }
        }
    }
}

/// Start the HTTP server with graceful shutdown
pub async fn start_server<F>(
    router: Router,
    addr: SocketAddr,
    shutdown_signal: F,
) -> Result<(), BlinkyError>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tracing::info!(%addr, "Starting server");

    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        BlinkyError::InternalServerError(format!("Failed to bind to {}: {}", addr, e))
    })?;

    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal)
    .await
    .map_err(|e| BlinkyError::InternalServerError(format!("Server error: {}", e)))?;

    Ok(())
}
