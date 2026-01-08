use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use redis::{AsyncCommands, ExistenceCheck, SetExpiry, SetOptions};
use tokio::time::sleep;

use crate::{
    analytics::queue::{ClickEvent, ClickEventQueue},
    storage::Storage,
};

/// Background worker for processing click events from the queue
pub struct ClickEventWorker {
    queue: ClickEventQueue,
    consumer_group: String,
    consumer_name: String,
    batch_size: usize,
    poll_interval: Duration,
    block_timeout_ms: usize,
    min_idle_time_ms: usize,
    shutdown: Arc<AtomicBool>,
}

impl ClickEventWorker {
    const DEFAULT_CONSUMER_GROUP: &'static str = "click-processors";
    const DEFAULT_BATCH_SIZE: usize = 100;
    const DEFAULT_POLL_INTERVAL_MS: u64 = 1000;
    const DEFAULT_BLOCK_TIMEOUT_MS: usize = 1000;
    const DEFAULT_MIN_IDLE_TIME_MS: usize = 60000; // 60 seconds

    pub fn new(queue: ClickEventQueue, worker_id: String) -> Self {
        Self {
            queue,
            consumer_group: Self::DEFAULT_CONSUMER_GROUP.to_string(),
            consumer_name: worker_id,
            batch_size: Self::DEFAULT_BATCH_SIZE,
            poll_interval: Duration::from_millis(Self::DEFAULT_POLL_INTERVAL_MS),
            block_timeout_ms: Self::DEFAULT_BLOCK_TIMEOUT_MS,
            min_idle_time_ms: Self::DEFAULT_MIN_IDLE_TIME_MS,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn with_consumer_group(mut self, group: String) -> Self {
        self.consumer_group = group;
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub fn with_block_timeout_ms(mut self, block_timeout_ms: usize) -> Self {
        self.block_timeout_ms = block_timeout_ms;
        self
    }

    pub fn with_min_idle_time_ms(mut self, min_idle_time_ms: usize) -> Self {
        self.min_idle_time_ms = min_idle_time_ms;
        self
    }

    /// Get a handle for graceful shutdown
    pub fn shutdown_handle(&self) -> WorkerShutdownHandle {
        WorkerShutdownHandle {
            shutdown: self.shutdown.clone(),
        }
    }

    /// Start processing events (runs forever until shutdown)
    pub async fn run<S>(&self, storage: S)
    where
        S: Storage + Clone + Send + 'static,
    {
        tracing::info!(
            consumer_group = %self.consumer_group,
            consumer_name = %self.consumer_name,
            batch_size = %self.batch_size,
            "Click event worker started"
        );

        let mut consecutive_errors = 0;
        let max_consecutive_errors = 10;

        while !self.shutdown.load(Ordering::Acquire) {
            match self.process_batch(storage.clone()).await {
                Ok(processed) => {
                    consecutive_errors = 0; // Reset on success

                    if processed == 0 {
                        // No events, sleep before next poll
                        sleep(self.poll_interval).await;
                    } else {
                        tracing::debug!(processed = %processed, "Processed click events");
                        // Continue immediately if we processed a full batch
                        if processed < self.batch_size {
                            sleep(self.poll_interval).await;
                        }
                    }
                }
                Err(e) => {
                    consecutive_errors += 1;
                    tracing::error!(
                        error = %e,
                        consecutive_errors = %consecutive_errors,
                        "Failed to process batch"
                    );

                    if consecutive_errors >= max_consecutive_errors {
                        tracing::error!("Too many consecutive errors, worker shutting down");
                        break;
                    }

                    // Exponential backoff on errors
                    let backoff = Duration::from_secs(2_u64.pow(consecutive_errors.min(5)));
                    sleep(backoff).await;
                }
            }
        }

        tracing::info!(
            consumer_name = %self.consumer_name,
            "Click event worker stopped"
        );
    }

    /// Process a batch of events
    async fn process_batch<S>(&self, storage: S) -> Result<usize, anyhow::Error>
    where
        S: Storage,
    {
        // Read pending events from stream (blocking with timeout)
        // Uses the worker's configured min_idle_time_ms for auto-claiming
        let events = self
            .queue
            .read_pending_with_min_idle(
                &self.consumer_group,
                &self.consumer_name,
                self.batch_size,
                self.block_timeout_ms,
                self.min_idle_time_ms,
            )
            .await?;

        if events.is_empty() {
            return Ok(0);
        }

        let mut processed = 0;

        for (message_id, event) in events {
            match self.process_event(&storage, &event).await {
                Ok(()) => {
                    // Acknowledge successful processing
                    if let Err(e) = self.queue.ack(&self.consumer_group, &message_id).await {
                        tracing::warn!(
                            error = %e,
                            message_id = %message_id,
                            "Failed to ack message, will be reprocessed"
                        );
                    }
                    processed += 1;
                }
                Err(e) => {
                    self.handle_processing_error(&message_id, &event, e).await;
                }
            }
        }

        Ok(processed)
    }

    /// Handle errors during event processing with retry logic and DLQ
    async fn handle_processing_error(
        &self,
        message_id: &str,
        event: &ClickEvent,
        error: anyhow::Error,
    ) {
        let error_msg = error.to_string();

        // Increment retry count
        let retry_count = match self.queue.increment_retry_count(message_id).await {
            Ok(count) => count,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    message_id = %message_id,
                    "Failed to increment retry count"
                );
                return;
            }
        };

        let max_retries = self.queue.max_retries();

        if retry_count >= max_retries {
            // Max retries exceeded - move to DLQ
            self.move_event_to_dlq(message_id, event, &error_msg, retry_count, max_retries)
                .await;
        } else {
            // Will retry
            self.log_retry_attempt(message_id, event, &error_msg, retry_count, max_retries);
        }
    }

    /// Move an event to the dead letter queue after max retries exceeded
    async fn move_event_to_dlq(
        &self,
        message_id: &str,
        event: &ClickEvent,
        error_msg: &str,
        retry_count: u32,
        max_retries: u32,
    ) {
        tracing::error!(
            error = %error_msg,
            short_code = %event.short_code,
            message_id = %message_id,
            retry_count = %retry_count,
            max_retries = %max_retries,
            "Max retries exceeded, moving to DLQ"
        );

        if let Err(dlq_err) = self
            .queue
            .move_to_dlq(&self.consumer_group, message_id, event, error_msg)
            .await
        {
            tracing::error!(
                error = %dlq_err,
                message_id = %message_id,
                "Failed to move message to DLQ"
            );
        }
    }

    /// Log a retry attempt
    fn log_retry_attempt(
        &self,
        message_id: &str,
        event: &ClickEvent,
        error_msg: &str,
        retry_count: u32,
        max_retries: u32,
    ) {
        tracing::warn!(
            error = %error_msg,
            short_code = %event.short_code,
            message_id = %message_id,
            retry_count = %retry_count,
            max_retries = %max_retries,
            "Failed to process click event, will retry"
        );
    }

    /// Process a single click event with deduplication
    async fn process_event<S>(&self, storage: &S, event: &ClickEvent) -> Result<(), anyhow::Error>
    where
        S: Storage,
    {
        // Check if this event was already processed (deduplication)
        // Uses event_id as dedup key - simple and universally unique
        // Redis SET NX with 24-hour TTL prevents duplicate processing
        let dedup_key = format!("dedup:click:{}", event.dedup_key());

        let mut conn = self.queue.clone_redis_pool().get().await?;

        // Try to set the dedup key (SET NX = set if not exists)
        // Returns None if key was set (first time seeing this event)
        // Returns Some(old_value) if key already exists (duplicate event)
        let opts = SetOptions::default()
            .conditional_set(ExistenceCheck::NX) // Only set if not exists
            .with_expiration(SetExpiry::EX(86400)); // 24 hours TTL

        let result: Result<Option<String>, _> = conn.set_options(&dedup_key, "1", opts).await;

        let is_duplicate = match result {
            Ok(Some(_)) => false, // Key was set successfully (first occurrence)
            Ok(None) => true,     // Key already existed (duplicate)
            Err(e) => {
                tracing::error!(
                    error = %e,
                    event_id = %event.event_id,
                    "Failed to check dedup key"
                );
                return Err(anyhow::anyhow!("Failed to check dedup key: {}", e));
            }
        };

        if is_duplicate {
            tracing::debug!(
                event_id = %event.event_id,
                short_code = %event.short_code,
                "Duplicate click event detected, skipping"
            );
            return Ok(()); // Already processed, skip further processing
        }

        // First occurrence - process the click
        storage
            .increment_click(&event.short_code)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to increment click: {}", e))?;

        tracing::debug!(
            event_id = %event.event_id,
            short_code = %event.short_code,
            timestamp_ms = %event.timestamp_ms,
            "Click event processed"
        );

        Ok(())
    }
}

/// Handle for gracefully shutting down the worker
#[derive(Clone)]
pub struct WorkerShutdownHandle {
    shutdown: Arc<AtomicBool>,
}

impl WorkerShutdownHandle {
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}
