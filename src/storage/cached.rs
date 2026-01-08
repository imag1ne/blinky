use crate::error::BlinkyError;
use crate::state::AppState;
use crate::storage::{Stats, Storage, create_redis_pool};
use axum::extract::FromRef;
use deadpool_redis::Pool as RedisPool;
use redis::AsyncCommands;
use std::time::Duration;
use url::Url;

/// CachedStorage wraps a base storage implementation with Redis caching.
/// It uses the cache-aside pattern: check cache first, on miss fetch from DB and populate cache.
#[derive(Clone)]
pub struct CachedStorage<S> {
    /// The underlying storage (e.g., PgStorage)
    db: S,
    /// Redis connection pool
    cache: RedisPool,
    /// Time-to-live for cached entries
    ttl: Duration,
}

impl<S> CachedStorage<S>
where
    S: Storage + Clone,
{
    pub fn new(db: S, cache: RedisPool, ttl: Duration) -> Self {
        Self { db, cache, ttl }
    }

    /// Create a Redis connection pool from a Redis URL
    pub async fn try_from_redis_url(
        db: S,
        redis_url: &str,
        ttl: Duration,
    ) -> Result<Self, BlinkyError> {
        let pool = create_redis_pool(redis_url)?;
        Ok(Self::new(db, pool, ttl))
    }

    /// Get a value from cache
    async fn get_from_cache(&self, key: &str) -> Result<Option<String>, BlinkyError> {
        let mut conn = self.cache.get().await.map_err(|e| {
            tracing::warn!(error = %e, "Failed to get Redis connection");
            BlinkyError::CacheError(e.to_string())
        })?;

        let value: Option<String> = conn.get(key).await.map_err(|e| {
            tracing::warn!(error = %e, key = %key, "Failed to get from cache");
            BlinkyError::CacheError(e.to_string())
        })?;

        if value.is_some() {
            tracing::debug!(key = %key, "Cache hit");
        } else {
            tracing::debug!(key = %key, "Cache miss");
        }

        Ok(value)
    }

    /// Set a value in cache with TTL
    async fn set_cache(&self, key: &str, value: &str) -> Result<(), BlinkyError> {
        let mut conn = self.cache.get().await.map_err(|e| {
            tracing::warn!(error = %e, "Failed to get Redis connection for set");
            BlinkyError::CacheError(e.to_string())
        })?;

        let ttl_secs = self.ttl.as_secs();
        let _: () = conn.set_ex(key, value, ttl_secs).await.map_err(|e| {
            tracing::warn!(error = %e, key = %key, "Failed to set cache");
            BlinkyError::CacheError(e.to_string())
        })?;

        tracing::debug!(key = %key, ttl_secs = %ttl_secs, "Cache set");
        Ok(())
    }

    /// Delete a value from cache
    async fn delete_cache(&self, key: &str) -> Result<(), BlinkyError> {
        let mut conn = self.cache.get().await.map_err(|e| {
            tracing::warn!(error = %e, "Failed to get Redis connection for delete");
            BlinkyError::CacheError(e.to_string())
        })?;

        let _: usize = conn.del(key).await.map_err(|e| {
            tracing::warn!(error = %e, key = %key, "Failed to delete from cache");
            BlinkyError::CacheError(e.to_string())
        })?;

        tracing::debug!(key = %key, "Cache deleted");
        Ok(())
    }

    /// Generate cache key for short code lookups
    fn cache_key_for_short_code(short_code: &str) -> String {
        format!("sc:{}", short_code)
    }
}

impl<S> Storage for CachedStorage<S>
where
    S: Storage + Clone + Sync,
{
    async fn insert_url(&self, short_code: &str, original_url: &Url) -> Result<(), BlinkyError> {
        // Write to DB first
        self.db.insert_url(short_code, original_url).await?;

        // Populate cache immediately (write-through pattern)
        let cache_key = Self::cache_key_for_short_code(short_code);
        if let Err(e) = self.set_cache(&cache_key, original_url.as_str()).await {
            // Log error but don't fail the request - cache population is optional
            tracing::warn!(error = %e, short_code = %short_code, "Failed to populate cache on insert, continuing");
        }

        Ok(())
    }

    async fn get_url(&self, short_code: &str) -> Result<Option<Url>, BlinkyError> {
        let cache_key = Self::cache_key_for_short_code(short_code);

        // Try cache first
        match self.get_from_cache(&cache_key).await {
            Ok(Some(cached_url)) => {
                // Cache hit - parse and return immediately
                match Url::parse(&cached_url) {
                    Ok(url) => {
                        tracing::debug!(short_code = %short_code, "Cache hit for URL");
                        return Ok(Some(url));
                    }
                    Err(e) => {
                        // Invalid URL in cache - delete it and fall through to DB
                        tracing::warn!(error = %e, short_code = %short_code, "Invalid URL in cache, deleting");
                        let _ = self.delete_cache(&cache_key).await;
                    }
                }
            }
            Ok(None) => {
                // Cache miss - fall through to DB
                tracing::debug!(short_code = %short_code, "Cache miss for URL");
            }
            Err(e) => {
                // Cache error - log and fall through to DB (graceful degradation)
                tracing::warn!(error = %e, short_code = %short_code, "Cache error, falling back to DB");
            }
        }

        // Cache miss or error - fetch from DB
        let url = self.db.get_url(short_code).await?;

        // Populate cache if found (fire and forget - don't fail on cache errors)
        if let Some(ref url_value) = url
            && let Err(e) = self.set_cache(&cache_key, url_value.as_str()).await
        {
            tracing::warn!(error = %e, short_code = %short_code, "Failed to populate cache after DB fetch");
        }

        Ok(url)
    }

    async fn increment_click(&self, short_code: &str) -> Result<(), BlinkyError> {
        // Analytics are not cached - pass through to DB
        self.db.increment_click(short_code).await
    }

    async fn get_stats(&self, short_code: &str) -> Result<Option<Stats>, BlinkyError> {
        // Stats are not cached (they change frequently) - pass through to DB
        self.db.get_stats(short_code).await
    }

    async fn code_exists(&self, short_code: &str) -> Result<bool, BlinkyError> {
        let cache_key = Self::cache_key_for_short_code(short_code);

        // Check cache first
        match self.get_from_cache(&cache_key).await {
            Ok(Some(_)) => return Ok(true), // Cache hit means it exists
            Ok(None) => {
                // Cache miss - check DB
            }
            Err(e) => {
                // Cache error - fall through to DB
                tracing::warn!(error = %e, short_code = %short_code, "Cache error in code_exists, falling back to DB");
            }
        }

        // Check DB
        self.db.code_exists(short_code).await
    }
}

impl<Gen, S> FromRef<AppState<Gen, CachedStorage<S>>> for CachedStorage<S>
where
    Gen: Clone,
    S: Storage + Clone,
{
    fn from_ref(state: &AppState<Gen, CachedStorage<S>>) -> Self {
        state.store.clone()
    }
}
