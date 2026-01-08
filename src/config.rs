use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use anyhow::Context;
use config::{self, Config, Environment};
use dotenvy::dotenv;
use serde::Deserialize;
use serde_with::serde_as;
use url::Url;

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct AppConfig {
    // --- Server ---
    /// APP_HOST - bind host (default `0.0.0.0`)
    #[serde(default = "default_app_host")]
    pub app_host: String,
    /// APP_PORT - bind port (default `8080`)
    #[serde(default = "default_app_port")]
    pub app_port: u16,
    /// BASE_URL - external base URL for generated links (default `http://localhost:8080`)
    #[serde(default = "default_base_url")]
    pub base_url: Url,

    // --- Database ---
    /// DATABASE_URL - Postgres DSN (required)
    pub database_url: Url,
    /// DB_POOL_SIZE - Maximum number of database connections in the pool (default `20`)
    #[serde(default = "default_db_pool_size")]
    pub db_pool_size: u32,

    // --- Redis / Cache ---
    /// REDIS_CACHE_URL - optional Redis connection string for caching
    #[serde(default)]
    pub redis_cache_url: Option<Url>,
    /// CACHE_TTL_SECS - time-to-live for cached entries (default `3600`)
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_cache_ttl_secs")]
    pub cache_ttl_secs: Duration,

    // --- Redis / Queue ---
    /// REDIS_QUEUE_URL - optional Redis connection string for queue
    #[serde(default)]
    pub redis_queue_url: Option<Url>,

    // --- Queue / DLQ settings ---
    /// QUEUE_MAX_RETRIES - maximum retry attempts before moving to DLQ (default `3`)
    #[serde(default = "default_queue_max_retries")]
    pub queue_max_retries: u32,
    /// QUEUE_STREAM_KEY - Redis stream key for primary queue
    #[serde(default = "default_queue_stream_key")]
    pub queue_stream_key: String,
    /// QUEUE_DLQ_STREAM_KEY - Redis stream key for dead-letter queue
    #[serde(default = "default_queue_dlq_stream_key")]
    pub queue_dlq_stream_key: String,
    /// QUEUE_CONSUMER_GROUP - Redis consumer group name
    #[serde(default = "default_queue_consumer_group")]
    pub queue_consumer_group: String,
    /// QUEUE_WORKER_ID - identifier for the worker consumer
    #[serde(default = "default_queue_worker_id")]
    pub queue_worker_id: String,
    /// QUEUE_BATCH_SIZE - number of messages fetched per poll
    #[serde(default = "default_queue_batch_size")]
    pub queue_batch_size: usize,
    /// QUEUE_POLL_INTERVAL_MS - delay between empty polls in milliseconds
    #[serde(default = "default_queue_poll_interval_ms")]
    pub queue_poll_interval_ms: u64,
    /// QUEUE_BLOCK_TIMEOUT_MS - block timeout for XREADGROUP in milliseconds
    #[serde(default = "default_queue_block_timeout_ms")]
    pub queue_block_timeout_ms: u64,
    /// QUEUE_MIN_IDLE_TIME_MS - min idle time before claiming pending messages
    #[serde(default = "default_queue_min_idle_time_ms")]
    pub queue_min_idle_time_ms: u64,

    // --- Shortcode generation ---
    /// SHORTCODE_MAX_RETRIES - maximum attempts to generate unique shortcode (default `5`)
    #[serde(default = "default_shortcode_max_retries")]
    pub shortcode_max_retries: u32,

    // --- CORS ---
    /// CORS_ENABLED - enable CORS middleware (default `false`)
    #[serde(default)]
    pub cors_enabled: bool,
    /// CORS_ALLOWED_ORIGINS - list of allowed CORS origins (default `[]`)
    #[serde(default)]
    pub cors_allowed_origins: Vec<String>,

    // --- Logging / OpenAPI ---
    /// LOG_FORMAT - log format (default `json`)
    #[serde(default)]
    pub log_format: LogFormat,
    /// RUST_LOG - Rust log level (default `"info"`)
    #[serde(default = "default_rust_log")]
    pub rust_log: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Plain,
    #[default]
    Json,
}

fn default_app_host() -> String {
    "0.0.0.0".to_string()
}

fn default_base_url() -> Url {
    Url::parse("http://localhost:8080").unwrap()
}

const fn default_app_port() -> u16 {
    8080
}

const fn default_cache_ttl_secs() -> Duration {
    Duration::from_secs(3600)
}

const fn default_db_pool_size() -> u32 {
    20
}

const fn default_shortcode_max_retries() -> u32 {
    5
}

fn default_rust_log() -> String {
    "info".to_string()
}

const fn default_queue_max_retries() -> u32 {
    3
}

fn default_queue_stream_key() -> String {
    "clicks:stream".to_string()
}

fn default_queue_dlq_stream_key() -> String {
    "clicks:stream:dlq".to_string()
}

fn default_queue_consumer_group() -> String {
    "click-processors".to_string()
}

fn default_queue_worker_id() -> String {
    "worker-1".to_string()
}

const fn default_queue_batch_size() -> usize {
    100
}

const fn default_queue_poll_interval_ms() -> u64 {
    1_000
}

const fn default_queue_block_timeout_ms() -> u64 {
    1_000
}

const fn default_queue_min_idle_time_ms() -> u64 {
    60_000
}

impl AppConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        // Load .env file if present
        dotenv().ok();

        // Build config from environment variables
        let settings = Config::builder()
            .add_source(
                Environment::default()
                    .try_parsing(true)
                    .ignore_empty(true)
                    .list_separator(",")
                    .with_list_parse_key("cors_allowed_origins"),
            )
            .build()?;

        // Deserialize into AppConfig
        let cfg = settings.try_deserialize::<AppConfig>()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate feature-dependent requirements and cross-field invariants.
    pub fn validate(&self) -> anyhow::Result<()> {
        // Validate database URL scheme
        match self.database_url.scheme() {
            "postgres" | "postgresql" => {}
            other => anyhow::bail!("Unsupported DATABASE_URL scheme: {}", other),
        }

        // Validate Redis Cache URL scheme
        if let Some(redis) = &self.redis_cache_url {
            match redis.scheme() {
                "redis" | "rediss" => {}
                other => anyhow::bail!("Unsupported REDIS_CACHE_URL scheme: {}", other),
            }
        }

        // Validate Redis Queue URL scheme
        if let Some(redis) = &self.redis_queue_url {
            match redis.scheme() {
                "redis" | "rediss" => {}
                other => anyhow::bail!("Unsupported REDIS_QUEUE_URL scheme: {}", other),
            }
        }

        // BASE_URL must be an absolute http(s) URL
        if self.base_url.scheme() != "http" && self.base_url.scheme() != "https" {
            anyhow::bail!("BASE_URL must start with `http://` or `https://`");
        }

        // Cache TTL invariant
        if self.redis_cache_url.is_some() && self.cache_ttl_secs.is_zero() {
            anyhow::bail!("CACHE_TTL_SECS must be > 0 when REDIS_CACHE_URL is set");
        }

        // CORS origins must be "*" or valid URLs
        for origin in &self.cors_allowed_origins {
            if origin != "*" {
                url::Url::parse(origin)
                    .with_context(|| format!("Invalid CORS origin: {}", origin))?;
            }
        }

        // Queue max retries must be at least 1
        if self.queue_max_retries == 0 {
            anyhow::bail!("QUEUE_MAX_RETRIES must be > 0");
        }

        if self.queue_stream_key.trim().is_empty() {
            anyhow::bail!("QUEUE_STREAM_KEY must not be empty");
        }

        if self.queue_dlq_stream_key.trim().is_empty() {
            anyhow::bail!("QUEUE_DLQ_STREAM_KEY must not be empty");
        }

        if self.queue_consumer_group.trim().is_empty() {
            anyhow::bail!("QUEUE_CONSUMER_GROUP must not be empty");
        }

        if self.queue_worker_id.trim().is_empty() {
            anyhow::bail!("QUEUE_WORKER_ID must not be empty");
        }

        if self.queue_batch_size == 0 {
            anyhow::bail!("QUEUE_BATCH_SIZE must be > 0");
        }

        if self.queue_poll_interval_ms == 0 {
            anyhow::bail!("QUEUE_POLL_INTERVAL_MS must be > 0");
        }

        if self.queue_block_timeout_ms == 0 {
            anyhow::bail!("QUEUE_BLOCK_TIMEOUT_MS must be > 0");
        }

        if self.queue_min_idle_time_ms == 0 {
            anyhow::bail!("QUEUE_MIN_IDLE_TIME_MS must be > 0");
        }

        Ok(())
    }

    /// Borrow DATABASE_URL as &str for drivers expecting a string.
    pub fn database_url_str(&self) -> &str {
        self.database_url.as_str()
    }

    /// Build a SocketAddr for listening from APP_HOST and APP_PORT.
    pub async fn to_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        // If APP_HOST is an IP literal, use it directly.
        if let Ok(ip) = self.app_host.parse::<IpAddr>() {
            return Ok(SocketAddr::new(ip, self.app_port));
        }

        // Otherwise resolve the hostname.
        tokio::net::lookup_host((self.app_host.as_str(), self.app_port))
            .await
            .with_context(|| format!("Failed to resolve {}:{}", self.app_host, self.app_port))?
            .next()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "No addresses resolved for {}:{}",
                    self.app_host,
                    self.app_port
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::{File, FileFormat};

    fn cfg_from_toml(toml: &str) -> anyhow::Result<AppConfig> {
        let settings = Config::builder()
            .add_source(File::from_str(toml, FileFormat::Toml))
            .build()?;
        let cfg: AppConfig = settings.try_deserialize()?;
        cfg.validate()?;
        Ok(cfg)
    }

    #[test]
    fn minimal_defaults_ok() -> anyhow::Result<()> {
        let toml = r#"
            database_url = "postgres://user:pass@localhost:5432/db"
        "#;
        let cfg = cfg_from_toml(toml)?;
        assert_eq!(cfg.app_host, "0.0.0.0");
        assert_eq!(cfg.app_port, 8080);
        assert_eq!(cfg.base_url.as_str(), "http://localhost:8080/");
        assert_eq!(cfg.cache_ttl_secs, Duration::from_secs(3600));
        assert!(matches!(cfg.log_format, LogFormat::Json));
        assert_eq!(cfg.queue_max_retries, 3);
        assert_eq!(cfg.queue_stream_key, "clicks:stream");
        assert_eq!(cfg.queue_dlq_stream_key, "clicks:stream:dlq");
        assert_eq!(cfg.queue_consumer_group, "click-processors");
        assert_eq!(cfg.queue_worker_id, "worker-1");
        assert_eq!(cfg.queue_batch_size, 100);
        assert_eq!(cfg.queue_poll_interval_ms, 1_000);
        assert_eq!(cfg.queue_block_timeout_ms, 1_000);
        assert_eq!(cfg.queue_min_idle_time_ms, 60_000);
        Ok(())
    }

    #[test]
    fn overrides_parse_ok() -> anyhow::Result<()> {
        let toml = r#"
            app_host = "127.0.0.1"
            app_port = 34567
            base_url = "https://example.com"
            database_url = "postgres://u:p@h:5432/db"
            redis_cache_url = "redis://localhost:6379"
            redis_queue_url = "redis://localhost:6380"
            cache_ttl_secs = 42
            queue_max_retries = 5
            queue_stream_key = "analytics:stream"
            queue_dlq_stream_key = "analytics:stream:dlq"
            queue_consumer_group = "analytics-group"
            queue_worker_id = "worker-42"
            queue_batch_size = 25
            queue_poll_interval_ms = 250
            queue_block_timeout_ms = 1500
            queue_min_idle_time_ms = 90000
            cors_allowed_origins = ["https://a.example", "https://b.example"]
            log_format = "plain"
            rust_log = "debug"
            openapi_enabled = true
        "#;
        let cfg = cfg_from_toml(toml)?;
        assert_eq!(cfg.app_host, "127.0.0.1");
        assert_eq!(cfg.app_port, 34567);
        assert_eq!(cfg.base_url.as_str(), "https://example.com/");
        assert_eq!(
            cfg.redis_cache_url.as_ref().unwrap().as_str(),
            "redis://localhost:6379"
        );
        assert_eq!(
            cfg.redis_queue_url.as_ref().unwrap().as_str(),
            "redis://localhost:6380"
        );
        assert_eq!(cfg.cache_ttl_secs, Duration::from_secs(42));
        assert_eq!(cfg.queue_max_retries, 5);
        assert_eq!(cfg.queue_stream_key, "analytics:stream");
        assert_eq!(cfg.queue_dlq_stream_key, "analytics:stream:dlq");
        assert_eq!(cfg.queue_consumer_group, "analytics-group");
        assert_eq!(cfg.queue_worker_id, "worker-42");
        assert_eq!(cfg.queue_batch_size, 25);
        assert_eq!(cfg.queue_poll_interval_ms, 250);
        assert_eq!(cfg.queue_block_timeout_ms, 1_500);
        assert_eq!(cfg.queue_min_idle_time_ms, 90_000);
        assert_eq!(cfg.cors_allowed_origins.len(), 2);
        assert!(matches!(cfg.log_format, LogFormat::Plain));
        assert_eq!(cfg.rust_log, "debug");

        Ok(())
    }

    #[test]
    fn invalid_base_url_rejected() {
        let toml = r#"
            base_url = "ftp://localhost"
            database_url = "postgres://u:p@h:5432/db"
        "#;
        let err = cfg_from_toml(toml).unwrap_err();
        assert!(format!("{err:#}").contains("BASE_URL must start"));
    }

    #[test]
    fn invalid_db_scheme_rejected() {
        let toml = r#"
            database_url = "mysql://user:pass@localhost/db"
        "#;
        let err = cfg_from_toml(toml).unwrap_err();
        assert!(format!("{err:#}").contains("Unsupported DATABASE_URL scheme"));
    }

    #[test]
    fn invalid_redis_cache_scheme_rejected() {
        let toml = r#"
            database_url = "postgres://u:p@h:5432/db"
            redis_cache_url = "amqp://localhost:5672"
        "#;
        let err = cfg_from_toml(toml).unwrap_err();
        assert!(format!("{err:#}").contains("Unsupported REDIS_CACHE_URL scheme"));
    }

    #[test]
    fn invalid_redis_queue_scheme_rejected() {
        let toml = r#"
            database_url = "postgres://u:p@h:5432/db"
            redis_queue_url = "amqp://localhost:5672"
        "#;
        let err = cfg_from_toml(toml).unwrap_err();
        assert!(format!("{err:#}").contains("Unsupported REDIS_QUEUE_URL scheme"));
    }

    #[test]
    fn cache_ttl_zero_with_redis_cache_rejected() {
        let toml = r#"
            database_url = "postgres://u:p@h:5432/db"
            redis_cache_url = "redis://localhost:6379"
            cache_ttl_secs = 0
        "#;
        let err = cfg_from_toml(toml).unwrap_err();
        assert!(format!("{err:#}").contains("CACHE_TTL_SECS must be > 0"));
    }

    #[test]
    fn missing_database_url_rejected() {
        let err = cfg_from_toml("").unwrap_err();
        assert!(format!("{err:#}").contains("missing field `database_url`"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn to_socket_addr_ip_ok() {
        let toml = r#"
            database_url = "postgres://u:p@h:5432/db"
            app_host = "127.0.0.1"
            app_port = 18081
        "#;
        let cfg = cfg_from_toml(toml).unwrap();
        let addr = cfg.to_socket_addr().await.unwrap();
        assert_eq!(addr.port(), 18081);
        assert!(addr.ip().is_ipv4());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn to_socket_addr_hostname_ok() {
        let toml = r#"
            database_url = "postgres://u:p@h:5432/db"
            app_host = "localhost"
            app_port = 18082
        "#;
        let cfg = cfg_from_toml(toml).unwrap();
        let addr = cfg.to_socket_addr().await.unwrap();
        assert_eq!(addr.port(), 18082);
        assert!(addr.ip().is_ipv4() || addr.ip().is_ipv6());
    }
}
