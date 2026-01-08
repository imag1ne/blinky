pub mod cached;
pub mod postgresql;

use chrono::{DateTime, Utc};
use deadpool_redis::{Config as RedisConfig, Pool as RedisPool, Runtime};
use url::Url;

use crate::error::BlinkyError;

#[trait_variant::make(Send)]
pub trait Storage {
    /// Insert a new short URL
    async fn insert_url(&self, short_code: &str, original_url: &Url) -> Result<(), BlinkyError>;

    /// Get the original URL for a short code (cacheable, read-only)
    async fn get_url(&self, short_code: &str) -> Result<Option<Url>, BlinkyError>;

    /// Increment click counter by short code (fire-and-forget safe, used in background worker)
    async fn increment_click(&self, short_code: &str) -> Result<(), BlinkyError>;

    /// Get statistics for a short code
    async fn get_stats(&self, short_code: &str) -> Result<Option<Stats>, BlinkyError>;

    /// Check if a short code exists (used by shortcode generator)
    async fn code_exists(&self, short_code: &str) -> Result<bool, BlinkyError>;
}

pub struct Stats {
    pub short_code_id: i64,
    pub click_count: i64,
    pub last_clicked_at: Option<DateTime<Utc>>,
}

/// Create a Redis connection pool from a URL string
pub fn create_redis_pool(redis_url: &str) -> Result<RedisPool, BlinkyError> {
    let cfg = RedisConfig::from_url(redis_url);
    let pool = cfg
        .create_pool(Some(Runtime::Tokio1))
        .map_err(|e| BlinkyError::CacheError(format!("Failed to create Redis pool: {}", e)))?;

    Ok(pool)
}
