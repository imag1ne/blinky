use thiserror::Error;

/// Errors that can occur during queue operations
#[derive(Error, Debug)]
pub enum QueueError {
    /// Failed to get a connection from the Redis pool
    #[error("Redis connection pool error: {0}")]
    ConnectionPool(#[from] deadpool_redis::PoolError),

    /// Redis operation failed (XADD, XREAD, etc.)
    #[error("Redis operation failed: {0}")]
    RedisOperation(#[from] redis::RedisError),

    /// Failed to serialize event data to JSON
    #[error("Failed to serialize event: {0}")]
    Serialization(#[from] serde_json::Error),
}
