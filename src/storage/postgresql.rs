use axum::extract::FromRef;
use sqlx::{PgPool, postgres::PgPoolOptions};
use url::Url;

use crate::error::BlinkyError;
use crate::state::AppState;
use crate::storage::{Stats, Storage};

#[derive(Clone)]
pub struct PgStorage {
    pool: PgPool,
}

impl PgStorage {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn try_from_database_url(db_url: &str) -> Result<Self, BlinkyError> {
        Self::try_from_database_url_with_pool_size(db_url, 20).await
    }

    pub async fn try_from_database_url_with_pool_size(
        db_url: &str,
        pool_size: u32,
    ) -> Result<Self, BlinkyError> {
        let pool = create_connection_pool(db_url, pool_size).await?;
        Ok(Self::new(pool))
    }
}

impl Storage for PgStorage {
    async fn insert_url(&self, short_code: &str, original_url: &Url) -> Result<(), BlinkyError> {
        // Start a transaction
        let mut tx = self.pool.begin().await?;
        let url_str = original_url.as_str();
        // Insert the new short URL
        let short_url_id = sqlx::query_scalar!(
            r#"
            INSERT INTO short_urls (short_code, original_url)
            VALUES ($1, $2)
            RETURNING id
            "#,
            short_code,
            url_str
        )
        .fetch_one(&mut *tx)
        .await?;

        // Insert initial analytics record
        sqlx::query!(
            r#"
            INSERT INTO url_analytics (short_url_id)
            VALUES ($1)
            "#,
            short_url_id
        )
        .execute(&mut *tx)
        .await?;

        // Commit the transaction
        tx.commit().await?;

        Ok(())
    }

    async fn get_url(&self, short_code: &str) -> Result<Option<Url>, BlinkyError> {
        let original_url = sqlx::query_scalar!(
            r#"
            SELECT original_url
            FROM short_urls
            WHERE short_code = $1
            "#,
            short_code
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(match original_url {
            Some(url_str) => Some(Url::parse(&url_str)?),
            None => None,
        })
    }

    async fn increment_click(&self, short_code: &str) -> Result<(), BlinkyError> {
        let res = sqlx::query!(
            r#"
            UPDATE url_analytics AS ua
            SET click_count = ua.click_count + 1,
                last_clicked_at = NOW()
            FROM short_urls AS su
            WHERE ua.short_url_id = su.id
              AND su.short_code = $1
            "#,
            short_code
        )
        .execute(&self.pool)
        .await?;

        if res.rows_affected() == 0 {
            return Err(BlinkyError::SqlxError(sqlx::Error::RowNotFound));
        }

        Ok(())
    }

    async fn get_stats(&self, short_code: &str) -> Result<Option<Stats>, BlinkyError> {
        let stats = sqlx::query_as!(
            Stats,
            r#"
            SELECT
                su.id AS short_code_id,
                ua.click_count AS click_count,
                ua.last_clicked_at AS last_clicked_at
            FROM short_urls AS su
            INNER JOIN
                url_analytics AS ua
                ON su.id = ua.short_url_id
            WHERE su.short_code = $1
            "#,
            short_code
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(stats)
    }

    async fn code_exists(&self, short_code: &str) -> Result<bool, BlinkyError> {
        let exists = sqlx::query_scalar!(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM short_urls
                WHERE short_code = $1
            ) AS "exists!"
            "#,
            short_code
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(exists)
    }
}

async fn create_connection_pool(url: &str, pool_size: u32) -> sqlx::Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(pool_size)
        .acquire_timeout(std::time::Duration::from_secs(2))
        .connect(url)
        .await
}

impl<Gen> FromRef<AppState<Gen, PgStorage>> for PgStorage {
    fn from_ref(state: &AppState<Gen, PgStorage>) -> Self {
        state.store.clone()
    }
}

// rust
#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
    use std::{
        env,
        time::{SystemTime, UNIX_EPOCH},
    };

    // Build a unique DB name using timestamp + UUID to avoid collisions
    fn unique_db_name(prefix: &str) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let uuid = uuid::Uuid::new_v4();
        format!("{}_{:x}_{:x}", prefix, timestamp, uuid.as_u128())
    }

    // Create a new temporary database, return a pool connected to it.
    async fn setup_db() -> PgPool {
        // Base server URL, e.g. postgres://user:pass@localhost:5432/postgres
        let base = env::var("TEST_DATABASE_URL")
            .or_else(|_| env::var("DATABASE_URL"))
            .expect("Set `TEST_DATABASE_URL` or `DATABASE_URL` to a Postgres URL with create DB privileges");

        let mut server_url = Url::parse(&base).expect("Invalid Postgres URL");
        // Connect to `postgres` maintenance DB to be able to CREATE DATABASE
        server_url.set_path("postgres");

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(server_url.as_str())
            .await
            .expect("Failed to connect to Postgres (admin)");

        let db_name = unique_db_name("blinky_test");
        // Quote identifier safely
        let create_stmt = format!(r#"CREATE DATABASE "{}""#, db_name);
        admin_pool
            .execute(create_stmt.as_str())
            .await
            .expect("Failed to create test database");

        // Now connect to the freshly created DB
        let mut db_url = server_url.clone();
        db_url.set_path(&db_name);

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(db_url.as_str())
            .await
            .expect("Failed to connect to test database");

        // Bootstrap minimal schema required by PgStorage
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

        // Note: we do not drop the DB at the end to keep the test simple.
        // You can manually drop it if desired.
        pool
    }

    fn make_storage(pool: &PgPool) -> PgStorage {
        PgStorage::new(pool.clone())
    }

    #[tokio::test]
    #[ignore] // Ignore by default to avoid requiring a DB for all tests
    async fn insert_and_get_url_roundtrip() {
        let pool = setup_db().await;
        let storage = make_storage(&pool);

        let code = "abc123";
        let original = Url::parse("https://example.com/path?x=1").unwrap();

        storage.insert_url(code, &original).await.unwrap();

        let fetched = storage.get_url(code).await.unwrap();
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().as_str(), original.as_str());

        // Nonexistent code
        let missing = storage.get_url("nope").await.unwrap();
        assert!(missing.is_none());
    }

    #[tokio::test]
    #[ignore] // Ignore by default to avoid requiring a DB for all tests
    async fn code_exists_true_and_false() {
        let pool = setup_db().await;
        let storage = make_storage(&pool);

        let code = "exists1";
        let url = Url::parse("https://rust-lang.org").unwrap();

        assert!(!storage.code_exists(code).await.unwrap());
        storage.insert_url(code, &url).await.unwrap();
        assert!(storage.code_exists(code).await.unwrap());
        assert!(!storage.code_exists("missing").await.unwrap());
    }

    #[tokio::test]
    #[ignore] // Ignore by default to avoid requiring a DB for all tests
    async fn get_stats_and_increment_click() {
        let pool = setup_db().await;
        let storage = make_storage(&pool);

        let code = "clickme";
        let url = Url::parse("https://example.org").unwrap();
        storage.insert_url(code, &url).await.unwrap();

        // Initially, stats should exist with 0 clicks and no last click
        let stats = storage.get_stats(code).await.unwrap();
        let stats = stats.expect("stats should exist after insert");
        assert_eq!(stats.click_count, 0);
        assert!(stats.last_clicked_at.is_none());

        // Increment once
        storage.increment_click(code).await.unwrap();

        let stats2 = storage.get_stats(code).await.unwrap().unwrap();
        assert_eq!(stats2.click_count, 1);
        assert!(stats2.last_clicked_at.is_some());

        // Increment twice more
        storage.increment_click(code).await.unwrap();
        storage.increment_click(code).await.unwrap();
        let stats3 = storage.get_stats(code).await.unwrap().unwrap();
        assert_eq!(stats3.click_count, 3);
        assert!(stats3.last_clicked_at.is_some());
    }

    #[tokio::test]
    #[ignore] // Ignore by default to avoid requiring a DB for all tests
    async fn increment_click_unknown_code_returns_err() {
        let pool = setup_db().await;
        let storage = make_storage(&pool);

        // Use a code that surely doesn't exist
        let res = storage.increment_click("nonexistent_code_xyz123").await;
        assert!(
            res.is_err(),
            "expected error when incrementing missing short_code"
        );
    }

    #[tokio::test]
    #[ignore] // Ignore by default to avoid requiring a DB for all tests
    async fn insert_url_unique_short_code_violates() {
        let pool = setup_db().await;
        let storage = make_storage(&pool);

        let code = "dup";
        let url1 = Url::parse("https://a.example").unwrap();
        let url2 = Url::parse("https://b.example").unwrap();

        storage.insert_url(code, &url1).await.unwrap();
        let err = storage
            .insert_url(code, &url2)
            .await
            .expect_err("should fail on duplicate short_code");
        // Do not rely on internal error shape; just ensure it fails.
        let _ = err;
    }
}
