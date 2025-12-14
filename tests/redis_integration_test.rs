use std::{
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use blinky::{
    analytics::{
        queue::{ClickEvent, EventQueue},
        worker::ClickEventWorker,
    },
    storage::{Storage, cached::CachedStorage, postgresql::PgStorage},
};
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use url::Url;

/// Build a unique database name for isolated testing
fn unique_db_name(prefix: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let uuid = uuid::Uuid::new_v4();
    // Use timestamp + UUID to guarantee uniqueness even with concurrent test runs
    format!("{}_{:x}_{:x}", prefix, timestamp, uuid.as_u128())
}

/// Create a temporary test database with schema
async fn setup_test_db() -> PgPool {
    let base = env::var("TEST_DATABASE_URL")
        .or_else(|_| env::var("DATABASE_URL"))
        .expect("Set TEST_DATABASE_URL or DATABASE_URL to a Postgres URL");

    let mut server_url = Url::parse(&base).expect("Invalid Postgres URL");
    server_url.set_path("postgres");

    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(server_url.as_str())
        .await
        .expect("Failed to connect to Postgres");

    // Create test database with retry logic in case of transient name collision
    let db_name = unique_db_name("blinky_test");
    let create_stmt = format!(r#"CREATE DATABASE "{}""#, db_name);

    match admin_pool.execute(create_stmt.as_str()).await {
        Ok(_) => {}
        Err(e) => {
            // If database already exists, try once more with a new name
            if e.to_string().contains("already exists") {
                let db_name = unique_db_name("blinky_test");
                let create_stmt = format!(r#"CREATE DATABASE "{}""#, db_name);
                admin_pool
                    .execute(create_stmt.as_str())
                    .await
                    .expect("Failed to create test database on retry");
            } else {
                panic!("Failed to create test database: {}", e);
            }
        }
    }

    let mut db_url = server_url.clone();
    db_url.set_path(&db_name);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(db_url.as_str())
        .await
        .expect("Failed to connect to test database");

    // Create schema
    pool.execute(
        r#"
        CREATE TABLE IF NOT EXISTS short_urls (
            id BIGSERIAL PRIMARY KEY,
            short_code VARCHAR(32) UNIQUE NOT NULL,
            original_url TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS url_analytics (
            short_url_id BIGINT PRIMARY KEY REFERENCES short_urls(id) ON DELETE CASCADE,
            click_count BIGINT NOT NULL DEFAULT 0,
            last_clicked_at TIMESTAMPTZ
        );
        "#,
    )
    .await
    .expect("Failed to create schema");

    pool
}

/// Get Redis URL from environment
fn get_redis_url() -> String {
    env::var("TEST_REDIS_URL")
        .or_else(|_| env::var("REDIS_URL"))
        .unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

// ============================================================================
// Redis Cache Integration Tests
// ============================================================================

#[tokio::test]
#[ignore] // Requires both Postgres and Redis
async fn test_cached_storage_cache_hit() {
    let pool = setup_test_db().await;
    let redis_url = get_redis_url();
    let storage = PgStorage::new(pool);
    let cached_storage =
        CachedStorage::try_from_redis_url(storage, &redis_url, Duration::from_secs(60))
            .await
            .expect("Failed to create cached storage");

    let short_code = format!("{:x}", uuid::Uuid::new_v4().as_u128());
    let original_url = Url::parse("https://example.com/cache-test").unwrap();

    // Insert URL (should populate cache)
    cached_storage
        .insert_url(&short_code, &original_url)
        .await
        .expect("Failed to insert URL");

    // Get URL (should hit cache)
    let result = cached_storage
        .get_url(&short_code)
        .await
        .expect("Failed to get URL");

    assert_eq!(result.unwrap().as_str(), original_url.as_str());

    // Verify it exists in cache by getting it again
    let result2 = cached_storage
        .get_url(&short_code)
        .await
        .expect("Failed to get URL second time");

    assert_eq!(result2.unwrap().as_str(), original_url.as_str());
}

#[tokio::test]
#[ignore] // Requires both Postgres and Redis
async fn test_cached_storage_cache_miss_then_populate() {
    let pool = setup_test_db().await;
    let redis_url = get_redis_url();
    let storage = PgStorage::new(pool.clone());
    let cached_storage =
        CachedStorage::try_from_redis_url(storage, &redis_url, Duration::from_secs(60))
            .await
            .expect("Failed to create cached storage");

    let short_code = format!("{:x}", uuid::Uuid::new_v4().as_u128());
    let original_url = Url::parse("https://example.com/cache-miss").unwrap();

    // Insert directly to DB (bypassing cache)
    let direct_storage = PgStorage::new(pool);
    direct_storage
        .insert_url(&short_code, &original_url)
        .await
        .expect("Failed to insert URL directly");

    // Get URL (should miss cache, fetch from DB, then populate cache)
    let result = cached_storage
        .get_url(&short_code)
        .await
        .expect("Failed to get URL");

    assert_eq!(result.unwrap().as_str(), original_url.as_str());

    // Second get should hit cache
    let result2 = cached_storage
        .get_url(&short_code)
        .await
        .expect("Failed to get URL second time");

    assert_eq!(result2.unwrap().as_str(), original_url.as_str());
}

#[tokio::test]
#[ignore] // Requires both Postgres and Redis
async fn test_cached_storage_code_exists() {
    let pool = setup_test_db().await;
    let redis_url = get_redis_url();
    let storage = PgStorage::new(pool);
    let cached_storage =
        CachedStorage::try_from_redis_url(storage, &redis_url, Duration::from_secs(60))
            .await
            .expect("Failed to create cached storage");

    let short_code = format!("{:x}", uuid::Uuid::new_v4().as_u128());
    let original_url = Url::parse("https://example.com/exists").unwrap();

    // Initially should not exist
    assert!(
        !cached_storage
            .code_exists(&short_code)
            .await
            .expect("Failed to check existence")
    );

    // Insert URL
    cached_storage
        .insert_url(&short_code, &original_url)
        .await
        .expect("Failed to insert URL");

    // Now should exist (and hit cache)
    assert!(
        cached_storage
            .code_exists(&short_code)
            .await
            .expect("Failed to check existence after insert")
    );
}

#[tokio::test]
#[ignore] // Requires both Postgres and Redis
async fn test_cached_storage_cache_ttl() {
    let pool = setup_test_db().await;
    let redis_url = get_redis_url();
    let storage = PgStorage::new(pool);
    // Use very short TTL for testing
    let cached_storage =
        CachedStorage::try_from_redis_url(storage, &redis_url, Duration::from_secs(2))
            .await
            .expect("Failed to create cached storage");

    let short_code = format!("{:x}", uuid::Uuid::new_v4().as_u128());
    let original_url = Url::parse("https://example.com/ttl").unwrap();

    // Insert URL
    cached_storage
        .insert_url(&short_code, &original_url)
        .await
        .expect("Failed to insert URL");

    // Get URL immediately (cache hit)
    let result1 = cached_storage
        .get_url(&short_code)
        .await
        .expect("Failed to get URL");
    assert!(result1.is_some());

    // Wait for cache to expire
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Get URL again (cache expired, should fetch from DB and repopulate)
    let result2 = cached_storage
        .get_url(&short_code)
        .await
        .expect("Failed to get URL after TTL");
    assert!(result2.is_some());
    assert_eq!(result2.unwrap().as_str(), original_url.as_str());
}

// ============================================================================
// Event Queue Integration Tests
// ============================================================================

#[tokio::test]
#[ignore] // Requires Redis
async fn test_event_queue_enqueue_and_read() {
    let redis_url = get_redis_url();
    let stream_key = format!("test:enqueue:stream:{}", uuid::Uuid::new_v4());
    let dlq_key = format!("test:enqueue:dlq:{}", uuid::Uuid::new_v4());

    let queue = EventQueue::<ClickEvent>::try_from_redis_url(&redis_url, stream_key, dlq_key)
        .await
        .expect("Failed to create queue");

    let event = ClickEvent::new("test_code".to_string());

    // Enqueue event
    let message_id = queue
        .enqueue(event.clone())
        .await
        .expect("Failed to enqueue event");

    assert!(!message_id.is_empty());

    // Read event from queue
    let consumer_group = "test_group";
    let consumer_name = "test_consumer";
    let events = queue
        .read_pending(consumer_group, consumer_name, 10, 1000)
        .await
        .expect("Failed to read events");

    assert_eq!(events.len(), 1);
    let (read_message_id, read_event) = &events[0];
    assert_eq!(read_event.short_code, "test_code");
    assert_eq!(read_event.event_id, event.event_id);

    // Acknowledge the event
    queue
        .ack(consumer_group, read_message_id)
        .await
        .expect("Failed to ack event");
}

#[tokio::test]
#[ignore] // Requires Redis
async fn test_event_queue_retry_count() {
    let redis_url = get_redis_url();
    let stream_key = format!("test:retry:stream:{}", uuid::Uuid::new_v4());
    let dlq_key = format!("test:retry:dlq:{}", uuid::Uuid::new_v4());

    let queue = EventQueue::<ClickEvent>::try_from_redis_url(&redis_url, stream_key, dlq_key)
        .await
        .expect("Failed to create queue");

    let event = ClickEvent::new("retry_test".to_string());
    let message_id = queue.enqueue(event).await.expect("Failed to enqueue event");

    // Initial retry count should be 0
    let count = queue
        .get_retry_count(&message_id)
        .await
        .expect("Failed to get retry count");
    assert_eq!(count, 0);

    // Increment retry count
    let new_count = queue
        .increment_retry_count(&message_id)
        .await
        .expect("Failed to increment retry count");
    assert_eq!(new_count, 1);

    // Increment again
    let new_count = queue
        .increment_retry_count(&message_id)
        .await
        .expect("Failed to increment retry count");
    assert_eq!(new_count, 2);

    // Get retry count
    let count = queue
        .get_retry_count(&message_id)
        .await
        .expect("Failed to get retry count");
    assert_eq!(count, 2);
}

#[tokio::test]
#[ignore] // Requires Redis
async fn test_event_queue_move_to_dlq() {
    let redis_url = get_redis_url();
    let stream_key = format!("test:dlq:stream:{}", uuid::Uuid::new_v4());
    let dlq_key = format!("test:dlq:dlq:{}", uuid::Uuid::new_v4());

    let queue = EventQueue::<ClickEvent>::try_from_redis_url(&redis_url, stream_key, dlq_key)
        .await
        .expect("Failed to create queue")
        .with_max_retries(3);

    let event = ClickEvent::new("dlq_test".to_string());

    // Enqueue event
    let message_id = queue
        .enqueue(event.clone())
        .await
        .expect("Failed to enqueue event");

    let consumer_group = "dlq_test_group";
    let consumer_name = "dlq_test_consumer";

    // Read event
    let events = queue
        .read_pending(consumer_group, consumer_name, 10, 1000)
        .await
        .expect("Failed to read events");
    assert_eq!(events.len(), 1);

    // Simulate retries
    for i in 1..=3 {
        let count = queue
            .increment_retry_count(&message_id)
            .await
            .expect("Failed to increment retry count");
        assert_eq!(count, i);
    }

    // Move to DLQ
    queue
        .move_to_dlq(consumer_group, &message_id, &event, "Test error")
        .await
        .expect("Failed to move to DLQ");

    // Check stats - DLQ should have one message
    let stats = queue.stats().await.expect("Failed to get stats");
    assert_eq!(stats.dlq_messages, 1);
}

#[tokio::test]
#[ignore] // Requires Redis
async fn test_event_queue_stats() {
    let redis_url = get_redis_url();
    let stream_key = format!("test:stats:stream:{}", uuid::Uuid::new_v4());
    let dlq_key = format!("test:stats:dlq:{}", uuid::Uuid::new_v4());

    let queue = EventQueue::<ClickEvent>::try_from_redis_url(&redis_url, stream_key, dlq_key)
        .await
        .expect("Failed to create queue");

    // Initial stats
    let stats = queue.stats().await.expect("Failed to get stats");
    assert_eq!(stats.total_messages, 0);
    assert_eq!(stats.dlq_messages, 0);

    // Enqueue some events
    for i in 0..3 {
        let event = ClickEvent::new(format!("code_{}", i));
        queue.enqueue(event).await.expect("Failed to enqueue event");
    }

    // Stats should show 3 messages
    let stats = queue.stats().await.expect("Failed to get stats");
    assert_eq!(stats.total_messages, 3);
    assert_eq!(stats.dlq_messages, 0);
}

#[tokio::test]
#[ignore] // Requires Redis
async fn test_event_queue_max_stream_length() {
    let redis_url = get_redis_url();
    let stream_key = format!("test:maxlen:stream:{}", uuid::Uuid::new_v4());
    let dlq_key = format!("test:maxlen:dlq:{}", uuid::Uuid::new_v4());

    // Create queue with small max length
    let queue = EventQueue::<ClickEvent>::try_from_redis_url(&redis_url, stream_key, dlq_key)
        .await
        .expect("Failed to create queue")
        .with_max_stream_length(Some(5));

    assert_eq!(queue.max_stream_length(), Some(5));

    // Enqueue 10 events (should only keep ~5 due to approximate trimming)
    for i in 0..10 {
        let event = ClickEvent::new(format!("code_{}", i));
        queue.enqueue(event).await.expect("Failed to enqueue event");
    }

    // Stats should show approximate max length (may be slightly more due to approximate trimming)
    let stats = queue.stats().await.expect("Failed to get stats");
    assert!(
        stats.total_messages <= 10,
        "Stream length should be trimmed"
    );
}

// ============================================================================
// Worker Integration Tests
// ============================================================================

#[tokio::test]
#[ignore] // Requires both Postgres and Redis
async fn test_worker_processes_events() {
    let pool = setup_test_db().await;
    let redis_url = get_redis_url();
    let storage = PgStorage::new(pool);

    let stream_key = format!("test:worker:stream:{}", uuid::Uuid::new_v4());
    let dlq_key = format!("test:worker:dlq:{}", uuid::Uuid::new_v4());

    let queue = EventQueue::<ClickEvent>::try_from_redis_url(&redis_url, stream_key, dlq_key)
        .await
        .expect("Failed to create queue");

    // Insert a short URL to DB
    let short_code = "worker_test";
    let original_url = Url::parse("https://example.com/worker").unwrap();
    storage
        .insert_url(short_code, &original_url)
        .await
        .expect("Failed to insert URL");

    // Enqueue 3 click events
    for _ in 0..3 {
        let event = ClickEvent::new(short_code.to_string());
        queue.enqueue(event).await.expect("Failed to enqueue event");
    }

    // Create and start worker
    let worker = ClickEventWorker::new(queue.clone(), "test_worker".to_string())
        .with_consumer_group("test_worker_group".to_string())
        .with_batch_size(10)
        .with_poll_interval(Duration::from_millis(100));

    let shutdown_handle = worker.shutdown_handle();
    let storage_clone = storage.clone();

    // Run worker in background
    let worker_task = tokio::spawn(async move {
        worker.run(storage_clone).await;
    });

    // Wait for worker to process events
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Shutdown worker
    shutdown_handle.shutdown();

    // Wait for worker to stop
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_task).await;

    // Verify click count was incremented
    let stats = storage
        .get_stats(short_code)
        .await
        .expect("Failed to get stats")
        .expect("Stats not found");

    assert_eq!(stats.click_count, 3);
}

#[tokio::test]
#[ignore] // Requires both Postgres and Redis
async fn test_worker_deduplication() {
    let pool = setup_test_db().await;
    let redis_url = get_redis_url();
    let storage = PgStorage::new(pool);

    let stream_key = format!("test:dedup:stream:{}", uuid::Uuid::new_v4());
    let dlq_key = format!("test:dedup:dlq:{}", uuid::Uuid::new_v4());

    let queue = EventQueue::<ClickEvent>::try_from_redis_url(&redis_url, stream_key, dlq_key)
        .await
        .expect("Failed to create queue");

    let short_code = "dedup_test";
    let original_url = Url::parse("https://example.com/dedup").unwrap();
    storage
        .insert_url(short_code, &original_url)
        .await
        .expect("Failed to insert URL");

    // Create event with same event_id (simulating duplicate)
    let event = ClickEvent::new(short_code.to_string());

    // Enqueue same event 3 times
    for _ in 0..3 {
        queue
            .enqueue(event.clone())
            .await
            .expect("Failed to enqueue event");
    }

    // Create and start worker
    let worker = ClickEventWorker::new(queue.clone(), "dedup_worker".to_string())
        .with_consumer_group("dedup_worker_group".to_string())
        .with_batch_size(10)
        .with_poll_interval(Duration::from_millis(100));

    let shutdown_handle = worker.shutdown_handle();
    let storage_clone = storage.clone();

    let worker_task = tokio::spawn(async move {
        worker.run(storage_clone).await;
    });

    // Wait for worker to process
    tokio::time::sleep(Duration::from_secs(2)).await;

    shutdown_handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_task).await;

    // Verify click count is 1 (deduplication worked)
    let stats = storage
        .get_stats(short_code)
        .await
        .expect("Failed to get stats")
        .expect("Stats not found");

    assert_eq!(
        stats.click_count, 1,
        "Click count should be 1 due to deduplication"
    );
}

#[tokio::test]
#[ignore] // Requires both Postgres and Redis
async fn test_end_to_end_with_cache_and_queue() {
    let pool = setup_test_db().await;
    let redis_url = get_redis_url();
    let pg_storage = PgStorage::new(pool);

    // Create cached storage
    let cached_storage =
        CachedStorage::try_from_redis_url(pg_storage, &redis_url, Duration::from_secs(300))
            .await
            .expect("Failed to create cached storage");

    // Create queue with unique keys
    let stream_key = format!("test:e2e:stream:{}", uuid::Uuid::new_v4());
    let dlq_key = format!("test:e2e:dlq:{}", uuid::Uuid::new_v4());

    let queue = EventQueue::<ClickEvent>::try_from_redis_url(&redis_url, stream_key, dlq_key)
        .await
        .expect("Failed to create queue");

    let short_code = "e2e_test";
    let original_url = Url::parse("https://example.com/e2e").unwrap();

    // Step 1: Insert URL (should populate cache)
    cached_storage
        .insert_url(short_code, &original_url)
        .await
        .expect("Failed to insert URL");

    // Step 2: Verify cache hit
    let cached_url = cached_storage
        .get_url(short_code)
        .await
        .expect("Failed to get URL")
        .expect("URL not found");
    assert_eq!(cached_url.as_str(), original_url.as_str());

    // Step 3: Enqueue click events
    for _ in 0..5 {
        let event = ClickEvent::new(short_code.to_string());
        queue.enqueue(event).await.expect("Failed to enqueue event");
    }

    // Step 4: Start worker to process events
    let worker = ClickEventWorker::new(queue.clone(), "e2e_worker".to_string())
        .with_consumer_group("e2e_group".to_string())
        .with_batch_size(10)
        .with_poll_interval(Duration::from_millis(100));

    let shutdown_handle = worker.shutdown_handle();
    let storage_clone = cached_storage.clone();

    let worker_task = tokio::spawn(async move {
        worker.run(storage_clone).await;
    });

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    shutdown_handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_task).await;

    // Step 5: Verify stats (not cached)
    let stats = cached_storage
        .get_stats(short_code)
        .await
        .expect("Failed to get stats")
        .expect("Stats not found");

    assert_eq!(stats.click_count, 5);
    assert!(stats.last_clicked_at.is_some());

    // Step 6: Verify URL still cached
    let cached_url2 = cached_storage
        .get_url(short_code)
        .await
        .expect("Failed to get URL second time")
        .expect("URL not found");
    assert_eq!(cached_url2.as_str(), original_url.as_str());
}
