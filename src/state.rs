use std::sync::Arc;
use tokio::sync::Semaphore;
use url::Url;

use crate::{
    analytics::ClickEventQueue, shortcode::base58::Base58Shortcode, storage::postgresql::PgStorage,
};

/// Application state shared across all request handlers
///
/// Generic over both the shortcode generator and storage implementation
/// to support different backends.
#[derive(Clone)]
pub struct AppState<Gen = Base58Shortcode, Store = PgStorage> {
    /// Base URL for generating shortened links.
    pub base_url: Url,

    /// Storage backend for URL mappings and analytics
    /// Can be PgStorage for direct database access, or CachedStorage for Redis-backed caching
    pub store: Store,

    /// Shortcode generator implementation
    /// Default is Base58Shortcode which generates 7-character random codes
    pub shortcode_generator: Gen,

    /// Optional: Click event queue for async analytics processing
    /// When configured, click events are queued to Redis Streams for background processing
    /// When None, clicks are incremented directly in the database
    pub click_queue: Option<ClickEventQueue>,

    /// Maximum attempts to generate a unique shortcode before giving up
    /// Protects against infinite loops when the shortcode space is nearly exhausted
    pub shortcode_max_retries: u32,

    /// Semaphore to limit concurrent fallback increment tasks (prevents DoS)
    /// When the queue is unavailable, direct DB increments are used as fallback
    /// This semaphore prevents unbounded task spawning that could exhaust resources
    pub fallback_increment_semaphore: Arc<Semaphore>,
}

impl<Gen, Store> AppState<Gen, Store>
where
    Gen: Clone,
    Store: Clone,
{
    /// Maximum number of concurrent fallback increment tasks
    ///
    /// This limit protects against resource exhaustion when the Redis queue is unavailable
    /// and all click increments fall back to direct database writes.
    ///
    /// ## Tuning Guidelines
    ///
    /// The limit should account for:
    /// - Database connection pool size (default: 20)
    /// - Expected request rate during Redis outages
    /// - Average database query latency
    ///
    /// **Formula:** `limit ≈ expected_req_per_sec × avg_query_latency_sec`
    ///
    /// **Examples:**
    /// - 1,000 req/s × 0.1s latency = 100 concurrent tasks → use 100-200
    /// - 5,000 req/s × 0.1s latency = 500 concurrent tasks → use 500-1000
    /// - 10,000 req/s × 0.1s latency = 1000 concurrent tasks → use 1000-2000
    ///
    /// **Current value (1000):** Supports ~10,000 req/s with 100ms DB latency
    /// or ~5,000 req/s with 200ms DB latency
    ///
    /// **Note:** The actual bottleneck is the database connection pool.
    /// If this limit is hit frequently, consider:
    /// 1. Increasing DB pool size (default: 20, see `db_pool_size` config)
    /// 2. Using read replicas for analytics
    /// 3. Fixing the Redis queue infrastructure instead
    const FALLBACK_INCREMENT_LIMIT: usize = 1000;

    pub fn new(
        base_url: Url,
        store: Store,
        shortcode_generator: Gen,
        shortcode_max_retries: u32,
    ) -> Self {
        Self {
            base_url,
            store,
            shortcode_generator,
            click_queue: None,
            shortcode_max_retries,
            fallback_increment_semaphore: Arc::new(Semaphore::new(Self::FALLBACK_INCREMENT_LIMIT)),
        }
    }

    pub fn with_click_queue(mut self, queue: ClickEventQueue) -> Self {
        self.click_queue = Some(queue);
        self
    }
}
