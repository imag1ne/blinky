use std::{
    marker::PhantomData,
    time::{SystemTime, UNIX_EPOCH},
};

use deadpool_redis::Pool as RedisPool;
use redis::{
    AsyncCommands,
    streams::{
        StreamAutoClaimOptions, StreamAutoClaimReply, StreamId, StreamMaxlen, StreamReadOptions,
        StreamReadReply,
    },
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{error::BlinkyError, storage::create_redis_pool};

/// Get current timestamp in milliseconds since UNIX epoch
fn current_timestamp_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as u64,
        Err(e) => {
            // System clock is before UNIX epoch - this should be extremely rare
            // but can happen with misconfigured clocks or time travel
            tracing::error!(
                error = %e,
                "System clock error: time is before UNIX epoch. Using fallback timestamp."
            );
            // Return a sentinel value that indicates an error condition
            // This is better than panicking - the event will still be processed
            // but with an obviously incorrect timestamp that can be detected
            0
        }
    }
}

/// Errors that can occur when deserializing stream entries
#[derive(Debug)]
enum DeserializationError {
    /// Event payload is missing from stream entry
    MissingPayload { message_id: String },
    /// Event payload contains invalid UTF-8
    InvalidUtf8 { message_id: String, error: String },
    /// Event payload is not valid JSON for type T
    InvalidJson { message_id: String, error: String },
    /// Event value is not a BulkString (unexpected Redis type)
    UnexpectedValueType { message_id: String },
}

impl DeserializationError {
    /// Get the message ID associated with this error
    fn message_id(&self) -> &str {
        match self {
            DeserializationError::MissingPayload { message_id } => message_id,
            DeserializationError::InvalidUtf8 { message_id, .. } => message_id,
            DeserializationError::InvalidJson { message_id, .. } => message_id,
            DeserializationError::UnexpectedValueType { message_id } => message_id,
        }
    }
}

/// Dead letter metadata for failed messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterMetadata<T> {
    /// Original event that failed
    pub event: T,
    /// Number of processing attempts
    pub retry_count: u32,
    /// Last error message
    pub last_error: String,
    /// Timestamp when moved to DLQ (milliseconds since epoch)
    pub dlq_timestamp_ms: u64,
    /// Original Redis Stream message ID
    pub original_message_id: String,
}

/// Click event to be queued for async processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickEvent {
    /// Unique event identifier (UUIDv7)
    /// Generated once when event is created, never changes
    /// Used for deduplication
    pub event_id: String,

    /// The short code that was clicked
    pub short_code: String,

    /// Timestamp when the event was created (milliseconds since epoch)
    pub timestamp_ms: u64,

    /// Optional: IP address or client ID for analytics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
}

impl ClickEvent {
    pub fn new(short_code: String) -> Self {
        let timestamp_ms = current_timestamp_ms();
        let event_id = uuid::Uuid::now_v7().to_string();

        Self {
            event_id,
            short_code,
            timestamp_ms,
            client_id: None,
        }
    }

    pub fn with_client_id(mut self, client_id: String) -> Self {
        self.client_id = Some(client_id);
        self
    }

    /// Get the deduplication key (same as event_id)
    pub fn dedup_key(&self) -> &str {
        &self.event_id
    }
}

/// Generic Redis-based queue using Redis Streams
///
/// Redis Streams provide:
/// - At-least-once delivery
/// - Consumer groups for multiple workers
/// - Automatic retry of failed messages
/// - Message persistence
///
/// Type parameter `T` must implement `Serialize` and `DeserializeOwned` for JSON serialization.
#[derive(Clone)]
pub struct EventQueue<T> {
    redis: RedisPool,
    stream_key: String,
    dlq_stream_key: String,
    max_retries: u32,
    approx_max_stream_length: Option<usize>,
    _phantom: PhantomData<T>,
}

impl<T> EventQueue<T>
where
    T: Serialize + DeserializeOwned,
{
    const DEFAULT_MAX_RETRIES: u32 = 3;
    const DEFAULT_APPROX_MAX_STREAM_LENGTH: usize = 20_000;
    const RETRY_COUNT_KEY_PREFIX: &'static str = "retry:count:";
    const RETRY_COUNT_TTL_SECONDS: i64 = 86400; // 24 hours
    const DEFAULT_MIN_IDLE_TIME_MS: usize = 60000; // 60 seconds

    pub fn new(redis: RedisPool, stream_key: String, dlq_stream_key: String) -> Self {
        Self {
            redis,
            stream_key,
            dlq_stream_key,
            max_retries: Self::DEFAULT_MAX_RETRIES,
            approx_max_stream_length: Some(Self::DEFAULT_APPROX_MAX_STREAM_LENGTH),
            _phantom: PhantomData,
        }
    }

    /// Create a queue from a Redis URL with custom stream keys
    pub async fn try_from_redis_url(
        redis_url: &str,
        stream_key: String,
        dlq_stream_key: String,
    ) -> Result<Self, BlinkyError> {
        let pool = create_redis_pool(redis_url)?;
        Ok(Self::new(pool, stream_key, dlq_stream_key))
    }

    pub fn with_stream_key(mut self, stream_key: String) -> Self {
        self.stream_key = stream_key;
        self
    }

    pub fn with_dlq_stream_key(mut self, dlq_stream_key: String) -> Self {
        self.dlq_stream_key = dlq_stream_key;
        self
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Configure approximate maximum stream length for XADD trimming.
    /// Set to `None` to disable trimming (not recommended in production).
    pub fn with_max_stream_length(mut self, max_length: Option<usize>) -> Self {
        if matches!(max_length, Some(0)) {
            tracing::warn!(
                stream = %self.stream_key,
                "Received max stream length of zero; disabling trimming"
            );
        }
        self.approx_max_stream_length =
            max_length.and_then(|len| if len == 0 { None } else { Some(len) });

        self
    }

    /// Clone the Redis pool (for deduplication or other Redis operations)
    pub fn clone_redis_pool(&self) -> RedisPool {
        self.redis.clone()
    }

    /// Get max retries configuration
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Get configured approximate maximum stream length (if any)
    pub fn max_stream_length(&self) -> Option<usize> {
        self.approx_max_stream_length
    }

    /// Get a Redis connection from the pool
    ///
    /// Helper method to reduce boilerplate and ensure consistent error handling
    async fn get_connection(&self) -> Result<deadpool_redis::Connection, BlinkyError> {
        self.redis.get().await.map_err(|e| {
            tracing::error!(error = %e, "Failed to get Redis connection");
            BlinkyError::QueueError(e.into())
        })
    }

    /// Generate the retry count key for a message
    fn retry_key(&self, message_id: &str) -> String {
        format!("{}{}", Self::RETRY_COUNT_KEY_PREFIX, message_id)
    }

    /// Enqueue an event to the stream
    /// Returns the message ID assigned by Redis
    pub async fn enqueue(&self, event: T) -> Result<String, BlinkyError> {
        let mut conn = self.get_connection().await?;

        // Serialize event to JSON
        let event_json =
            serde_json::to_string(&event).map_err(|e| BlinkyError::QueueError(e.into()))?;

        let message_id: String = match self.approx_max_stream_length {
            Some(max_len) => {
                conn.xadd_maxlen(
                    &self.stream_key,
                    StreamMaxlen::Approx(max_len),
                    "*",
                    &[("event", event_json.as_str())],
                )
                .await
            }
            None => {
                conn.xadd(
                    &self.stream_key,
                    "*", // Auto-generate ID
                    &[("event", event_json.as_str())],
                )
                .await
            }
        }
        .map_err(|e: redis::RedisError| {
            tracing::error!(error = %e, stream = %self.stream_key, "Failed to add event to stream");
            BlinkyError::QueueError(e.into())
        })?;

        tracing::debug!(
            message_id = %message_id,
            stream = %self.stream_key,
            "Event enqueued"
        );

        Ok(message_id)
    }

    /// Read pending events from the stream (for worker processing)
    ///
    /// This is a convenience wrapper that uses a sensible default (60 seconds) for `min_idle_time_ms`.
    /// For more control, use `read_pending_with_min_idle`.
    pub async fn read_pending(
        &self,
        consumer_group: &str,
        consumer_name: &str,
        count: usize,
        block_ms: usize,
    ) -> Result<Vec<(String, T)>, BlinkyError> {
        self.read_pending_with_min_idle(
            consumer_group,
            consumer_name,
            count,
            block_ms,
            Self::DEFAULT_MIN_IDLE_TIME_MS,
        )
        .await
    }

    /// Deserialize a stream entry into an event
    ///
    /// This is a pure function that extracts and deserializes the event payload
    /// from a Redis Stream entry. It handles all error cases (missing payload,
    /// invalid UTF-8, malformed JSON, unexpected value types).
    ///
    /// Returns Ok((message_id, event)) on success, or Err with details on failure.
    fn deserialize_stream_entry(
        &self,
        stream_id: StreamId,
    ) -> Result<(String, T), DeserializationError> {
        let message_id = stream_id.id.clone();

        // Extract event JSON from the stream entry
        let event_json =
            stream_id
                .map
                .get("event")
                .ok_or_else(|| DeserializationError::MissingPayload {
                    message_id: message_id.clone(),
                })?;

        // Ensure it's a BulkString (bytes)
        let data = match event_json {
            redis::Value::BulkString(data) => data,
            _ => {
                return Err(DeserializationError::UnexpectedValueType {
                    message_id: message_id.clone(),
                });
            }
        };

        // Convert bytes to UTF-8 string
        let json_str =
            String::from_utf8(data.clone()).map_err(|e| DeserializationError::InvalidUtf8 {
                message_id: message_id.clone(),
                error: e.to_string(),
            })?;

        // Deserialize JSON to event type T
        let event = serde_json::from_str::<T>(&json_str).map_err(|e| {
            DeserializationError::InvalidJson {
                message_id: message_id.clone(),
                error: e.to_string(),
            }
        })?;

        Ok((message_id, event))
    }

    /// Acknowledge a malformed message to prevent it from blocking the queue
    ///
    /// Logs the error and attempts to ACK the message. If ACK fails, logs a warning
    /// but does not propagate the error (the message will be retried by another worker).
    async fn ack_malformed_message(
        &self,
        conn: &mut deadpool_redis::Connection,
        consumer_group: &str,
        error: &DeserializationError,
    ) {
        let message_id = error.message_id();

        // Log the specific error type
        match error {
            DeserializationError::MissingPayload { .. } => {
                tracing::warn!(
                    message_id = %message_id,
                    "Missing event payload in stream entry, will ack to skip"
                );
            }
            DeserializationError::InvalidUtf8 { error: err, .. } => {
                tracing::warn!(
                    error = %err,
                    message_id = %message_id,
                    "Invalid UTF-8 in event, will ack to skip"
                );
            }
            DeserializationError::InvalidJson { error: err, .. } => {
                tracing::warn!(
                    error = %err,
                    message_id = %message_id,
                    "Failed to deserialize event, will ack to skip"
                );
            }
            DeserializationError::UnexpectedValueType { .. } => {
                tracing::warn!(
                    message_id = %message_id,
                    "Unexpected value type in stream entry, will ack to skip"
                );
            }
        }

        // Attempt to ACK the malformed message
        let ack_result: Result<usize, redis::RedisError> = conn
            .xack(&self.stream_key, consumer_group, &[message_id])
            .await;

        Self::log_ack_result(ack_result, message_id, "malformed message");
    }

    /// Read pending events with configurable min_idle_time_ms for autoclaim.
    pub async fn read_pending_with_min_idle(
        &self,
        consumer_group: &str,
        consumer_name: &str,
        count: usize,
        block_ms: usize,
        min_idle_time_ms: usize,
    ) -> Result<Vec<(String, T)>, BlinkyError> {
        let mut conn = self.get_connection().await?;

        // Ensure consumer group exists (XGROUP CREATE)
        // Ignore error if group already exists
        let _: Result<(), redis::RedisError> = conn
            .xgroup_create_mkstream(&self.stream_key, consumer_group, "0")
            .await;

        let mut events = Vec::new();

        // Step 1: XAUTOCLAIM - Reclaim old pending messages from crashed/slow workers
        // XAUTOCLAIM stream group consumer min-idle-time start [COUNT count]
        let autoclaim_opts = StreamAutoClaimOptions::default().count(count);

        let autoclaim_result: Result<StreamAutoClaimReply, redis::RedisError> = conn
            .xautoclaim_options(
                &self.stream_key,
                consumer_group,
                consumer_name,
                min_idle_time_ms,
                "0-0", // Start from beginning of pending list
                autoclaim_opts,
            )
            .await;

        // Parse autoclaimed messages if successful
        match autoclaim_result {
            Ok(reply) => {
                for stream_id in reply.claimed {
                    match self.deserialize_stream_entry(stream_id) {
                        Ok((message_id, event)) => {
                            tracing::info!(
                                message_id = %message_id,
                                "Auto-claimed pending message from failed worker"
                            );
                            events.push((message_id, event));
                        }
                        Err(e) => {
                            self.ack_malformed_message(&mut conn, consumer_group, &e)
                                .await;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "XAUTOCLAIM failed, continuing to XREADGROUP");
            }
        }

        // If we got enough events from autoclaim, return early
        if events.len() >= count {
            return Ok(events);
        }

        // Step 2: XREADGROUP - Read new messages
        // XREADGROUP GROUP group consumer BLOCK ms COUNT count STREAMS stream >
        let remaining_count = count - events.len();
        let results: StreamReadReply = conn
            .xread_options(
                &[&self.stream_key],
                &[">"], // Read only new messages
                &StreamReadOptions::default()
                    .group(consumer_group, consumer_name)
                    .count(remaining_count)
                    .block(block_ms),
            )
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "Failed to read from stream");
                BlinkyError::QueueError(e.into())
            })?;

        for stream_key in results.keys {
            for stream_id in stream_key.ids {
                match self.deserialize_stream_entry(stream_id) {
                    Ok((message_id, event)) => {
                        tracing::debug!(
                            message_id = %message_id,
                            "Read event from stream"
                        );
                        events.push((message_id, event));
                    }
                    Err(e) => {
                        self.ack_malformed_message(&mut conn, consumer_group, &e)
                            .await;
                    }
                }
            }
        }

        Ok(events)
    }

    /// Acknowledge successful processing of a message
    /// This removes it from the pending list
    pub async fn ack(&self, consumer_group: &str, message_id: &str) -> Result<(), BlinkyError> {
        let mut conn = self.get_connection().await?;

        let _: usize = conn
            .xack(&self.stream_key, consumer_group, &[message_id])
            .await
            .map_err(|e| {
                tracing::error!(error = %e, message_id = %message_id, "Failed to ack message");
                BlinkyError::QueueError(e.into())
            })?;

        tracing::debug!(message_id = %message_id, "Message acknowledged");
        Ok(())
    }

    fn log_ack_result(
        result: Result<usize, redis::RedisError>,
        message_id: &str,
        ack_context: &str,
    ) {
        match result {
            Ok(0) => tracing::warn!(
                message_id = %message_id,
                ack_context = %ack_context,
                "Ack reported zero messages; entry may remain pending"
            ),
            Ok(_) => {}
            Err(e) => tracing::error!(
                error = %e,
                message_id = %message_id,
                ack_context = %ack_context,
                "Failed to ack stream entry"
            ),
        }
    }

    /// Increment retry count for a message
    /// Returns the new retry count
    pub async fn increment_retry_count(&self, message_id: &str) -> Result<u32, BlinkyError> {
        let mut conn = self.get_connection().await?;

        let retry_key = self.retry_key(message_id);

        // Increment retry count with 24-hour TTL
        let count: u32 = conn.incr(&retry_key, 1).await.map_err(|e| {
            tracing::error!(error = %e, message_id = %message_id, "Failed to increment retry count");
            BlinkyError::QueueError(e.into())
        })?;

        // Set TTL on first increment (when count == 1)
        if count == 1 {
            let _: () = conn.expire(&retry_key, Self::RETRY_COUNT_TTL_SECONDS).await.map_err(|e| {
                tracing::warn!(error = %e, message_id = %message_id, "Failed to set retry count TTL");
                BlinkyError::QueueError(e.into())
            })?;
        }

        tracing::debug!(
            message_id = %message_id,
            retry_count = %count,
            "Incremented retry count"
        );

        Ok(count)
    }

    /// Get current retry count for a message
    pub async fn get_retry_count(&self, message_id: &str) -> Result<u32, BlinkyError> {
        let mut conn = self.get_connection().await?;

        let retry_key = self.retry_key(message_id);
        let count: Option<u32> = conn
            .get(&retry_key)
            .await
            .map_err(|e| BlinkyError::QueueError(e.into()))?;

        Ok(count.unwrap_or(0))
    }

    /// Move a message to the dead letter queue
    pub async fn move_to_dlq(
        &self,
        consumer_group: &str,
        message_id: &str,
        event: &T,
        error_msg: &str,
    ) -> Result<(), BlinkyError>
    where
        T: Clone,
    {
        let retry_count = self.get_retry_count(message_id).await?;

        let dlq_metadata = DeadLetterMetadata {
            event: event.clone(),
            retry_count,
            last_error: error_msg.to_string(),
            dlq_timestamp_ms: current_timestamp_ms(),
            original_message_id: message_id.to_string(),
        };

        let mut conn = self.get_connection().await?;

        // Serialize DLQ metadata
        let dlq_json =
            serde_json::to_string(&dlq_metadata).map_err(|e| BlinkyError::QueueError(e.into()))?;

        // Add to DLQ stream
        let dlq_message_id: String = conn
            .xadd(
                &self.dlq_stream_key,
                "*",
                &[("metadata", dlq_json.as_str())],
            )
            .await
            .map_err(|e| {
                tracing::error!(error = %e, dlq_stream = %self.dlq_stream_key, "Failed to add to DLQ");
                BlinkyError::QueueError(e.into())
            })?;

        tracing::warn!(
            message_id = %message_id,
            dlq_message_id = %dlq_message_id,
            retry_count = %retry_count,
            error = %error_msg,
            "Message moved to DLQ"
        );

        // ACK the original message to remove from pending
        self.ack(consumer_group, message_id).await?;

        // Clean up retry count (best effort - don't fail if cleanup fails)
        let retry_key = self.retry_key(message_id);
        if let Err(e) = conn.del::<_, ()>(&retry_key).await {
            tracing::warn!(
                error = %e,
                message_id = %message_id,
                "Failed to delete retry count key (non-critical)"
            );
        }

        Ok(())
    }

    /// Get queue statistics
    pub async fn stats(&self) -> Result<QueueStats, BlinkyError> {
        let mut conn = self.get_connection().await?;

        // Get stream lengths
        let main_length: usize = conn.xlen(&self.stream_key).await.unwrap_or(0);
        let dlq_length: usize = conn.xlen(&self.dlq_stream_key).await.unwrap_or(0);

        Ok(QueueStats {
            stream_key: self.stream_key.clone(),
            total_messages: main_length,
            dlq_messages: dlq_length,
        })
    }
}

#[derive(Debug, Clone)]
pub struct QueueStats {
    pub stream_key: String,
    pub total_messages: usize,
    pub dlq_messages: usize,
}

/// Type alias for ClickEvent queue - maintains backward compatibility
pub type ClickEventQueue = EventQueue<ClickEvent>;

impl ClickEventQueue {
    /// Convenience constructor for ClickEvent queue with default stream keys
    pub fn new_click_queue(redis: RedisPool) -> Self {
        Self::new(
            redis,
            "clicks:stream".to_string(),
            "clicks:stream:dlq".to_string(),
        )
    }

    /// Convenience constructor from Redis URL with default stream keys
    pub async fn try_from_redis_url_click(redis_url: &str) -> Result<Self, BlinkyError> {
        Self::try_from_redis_url(
            redis_url,
            "clicks:stream".to_string(),
            "clicks:stream:dlq".to_string(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_click_event_creation() {
        let event = ClickEvent::new("abc123".to_string());
        assert_eq!(event.short_code, "abc123");
        assert!(!event.event_id.is_empty());
        assert_eq!(event.dedup_key(), &event.event_id);
        assert!(event.client_id.is_none());
    }

    #[test]
    fn test_click_event_with_client_id() {
        let event = ClickEvent::new("abc123".to_string()).with_client_id("192.168.1.1".to_string());
        assert_eq!(event.short_code, "abc123");
        assert_eq!(event.client_id, Some("192.168.1.1".to_string()));
    }

    #[test]
    fn test_dead_letter_metadata_serialization() {
        let event = ClickEvent::new("test".to_string());
        let metadata = DeadLetterMetadata {
            event: event.clone(),
            retry_count: 3,
            last_error: "Test error".to_string(),
            dlq_timestamp_ms: 1234567890,
            original_message_id: "1234-5".to_string(),
        };

        // Test serialization
        let json = serde_json::to_string(&metadata).unwrap();
        assert!(json.contains("Test error"));
        assert!(json.contains("\"retry_count\":3"));

        // Test deserialization
        let deserialized: DeadLetterMetadata<ClickEvent> = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.retry_count, 3);
        assert_eq!(deserialized.last_error, "Test error");
        assert_eq!(deserialized.original_message_id, "1234-5");
    }

    #[test]
    fn test_queue_configuration() {
        use crate::storage::create_redis_pool;

        let pool = create_redis_pool("redis://localhost:6379").unwrap();
        let queue = ClickEventQueue::new_click_queue(pool.clone());

        assert_eq!(queue.stream_key, "clicks:stream");
        assert_eq!(queue.dlq_stream_key, "clicks:stream:dlq");
        assert_eq!(queue.max_retries, 3);
        assert_eq!(
            queue.max_stream_length(),
            Some(EventQueue::<ClickEvent>::DEFAULT_APPROX_MAX_STREAM_LENGTH)
        );

        let custom_queue = ClickEventQueue::new(
            pool.clone(),
            "custom:stream".to_string(),
            "custom:dlq".to_string(),
        )
        .with_max_retries(5)
        .with_max_stream_length(Some(5_000));

        assert_eq!(custom_queue.stream_key, "custom:stream");
        assert_eq!(custom_queue.dlq_stream_key, "custom:dlq");
        assert_eq!(custom_queue.max_retries(), 5);
        assert_eq!(custom_queue.max_stream_length(), Some(5_000));

        let unbounded_queue = ClickEventQueue::new_click_queue(pool).with_max_stream_length(None);
        assert_eq!(unbounded_queue.max_stream_length(), None);
    }
}
