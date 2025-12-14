use axum::{
    Router,
    http::{HeaderValue, Method, header::CONTENT_TYPE},
};
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::config::AppConfig;
use crate::error::BlinkyError;

/// Apply CORS middleware to the router based on configuration
///
/// If `cors_enabled` is false, CORS middleware is not applied.
/// If `cors_allowed_origins` is empty, CORS is not applied.
/// If it contains "*", all origins are allowed.
/// Otherwise, only specified origins are allowed.
pub fn apply_cors<S>(router: Router<S>, config: &AppConfig) -> Result<Router<S>, BlinkyError>
where
    S: Clone + Send + Sync + 'static,
{
    // Skip CORS if disabled
    if !config.cors_enabled {
        tracing::info!("CORS disabled via configuration");
        return Ok(router);
    }

    // Skip CORS if no origins configured
    if config.cors_allowed_origins.is_empty() {
        tracing::info!("CORS not configured, allowing all origins by default");
        return Ok(router);
    }

    let cors_layer = build_cors_layer(config)?;

    tracing::info!(
        origins = ?config.cors_allowed_origins,
        "CORS enabled with configured origins"
    );

    Ok(router.layer(cors_layer))
}

fn build_cors_layer(config: &AppConfig) -> Result<CorsLayer, BlinkyError> {
    let mut cors = CorsLayer::new()
        // Allow common HTTP methods for URL shortener
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        // Allow common headers
        .allow_headers([CONTENT_TYPE]);

    // Configure allowed origins
    let use_wildcard = config.cors_allowed_origins.contains(&"*".to_string());

    if use_wildcard {
        // Allow any origin - credentials NOT supported with wildcard
        tracing::warn!(
            "CORS configured with wildcard (*) - credentials are disabled. \
             For authenticated requests, specify explicit origins instead."
        );
        cors = cors.allow_origin(AllowOrigin::any());
        // Note: allow_credentials(false) is the default, so we don't set it
    } else {
        // Parse and validate origins
        let origins: Result<Vec<_>, _> = config
            .cors_allowed_origins
            .iter()
            .map(|origin| {
                origin.parse::<HeaderValue>().map_err(|e| {
                    BlinkyError::InternalServerError(format!(
                        "Invalid CORS origin '{}': {}",
                        origin, e
                    ))
                })
            })
            .collect();

        cors = cors
            .allow_origin(AllowOrigin::list(origins?))
            .allow_credentials(true);
    }

    Ok(cors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Request, StatusCode};
    use axum::{Router, routing::get};
    use std::time::Duration;
    use tower::ServiceExt;
    use url::Url;

    fn test_config(origins: Vec<String>) -> AppConfig {
        AppConfig {
            app_host: "127.0.0.1".into(),
            app_port: 8080,
            base_url: Url::parse("http://example.com").unwrap(),
            database_url: Url::parse("postgres://example@localhost/db").unwrap(),
            db_pool_size: 20,
            redis_cache_url: None,
            redis_queue_url: None,
            cache_ttl_secs: Duration::from_secs(3600),
            shortcode_max_retries: 5,
            queue_max_retries: 3,
            queue_stream_key: "clicks:stream".into(),
            queue_dlq_stream_key: "clicks:stream:dlq".into(),
            queue_consumer_group: "click-processors".into(),
            queue_worker_id: "worker-1".into(),
            queue_batch_size: 100,
            queue_poll_interval_ms: 1_000,
            queue_block_timeout_ms: 1_000,
            queue_min_idle_time_ms: 60_000,
            cors_enabled: true,
            cors_allowed_origins: origins,
            log_format: crate::config::LogFormat::Json,
            rust_log: "info".into(),
        }
    }

    #[tokio::test]
    async fn cors_disabled_skips_middleware() {
        let mut config = test_config(vec!["https://example.com".to_string()]);
        config.cors_enabled = false;
        let router = Router::new().route("/", get(|| async { "ok" }));
        let app = apply_cors(router, &config).unwrap();

        let request = Request::builder()
            .uri("/")
            .header("Origin", "https://example.com")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // No CORS headers should be present when disabled
        assert!(
            !response
                .headers()
                .contains_key("access-control-allow-origin")
        );
    }

    #[tokio::test]
    async fn cors_with_specific_origin_allows_request() {
        let config = test_config(vec!["https://example.com".to_string()]);
        let router = Router::new().route("/", get(|| async { "ok" }));
        let app = apply_cors(router, &config).unwrap();

        let request = Request::builder()
            .uri("/")
            .header("Origin", "https://example.com")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Check CORS headers
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .unwrap(),
            "https://example.com"
        );
    }

    #[tokio::test]
    async fn cors_with_wildcard_allows_any_origin() {
        let config = test_config(vec!["*".to_string()]);
        let router = Router::new().route("/", get(|| async { "ok" }));
        let app = apply_cors(router, &config).unwrap();

        let request = Request::builder()
            .uri("/")
            .header("Origin", "https://random-origin.com")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .contains_key("access-control-allow-origin")
        );
    }

    #[tokio::test]
    async fn cors_not_configured_allows_request() {
        let config = test_config(vec![]);
        let router = Router::new().route("/", get(|| async { "ok" }));
        let app = apply_cors(router, &config).unwrap();

        let request = Request::builder()
            .uri("/")
            .header("Origin", "https://example.com")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cors_handles_preflight_options_request() {
        let config = test_config(vec!["https://example.com".to_string()]);
        let router = Router::new().route("/", get(|| async { "ok" }));
        let app = apply_cors(router, &config).unwrap();

        let request = Request::builder()
            .method("OPTIONS")
            .uri("/")
            .header("Origin", "https://example.com")
            .header("Access-Control-Request-Method", "POST")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert!(response.status().is_success() || response.status() == StatusCode::NO_CONTENT);
        assert!(
            response
                .headers()
                .contains_key("access-control-allow-methods")
        );
    }

    #[tokio::test]
    async fn cors_allows_multiple_origins() {
        let config = test_config(vec![
            "https://example.com".to_string(),
            "https://app.example.com".to_string(),
        ]);
        let router = Router::new().route("/", get(|| async { "ok" }));
        let app = apply_cors(router, &config).unwrap();

        // First origin
        let request1 = Request::builder()
            .uri("/")
            .header("Origin", "https://example.com")
            .body(axum::body::Body::empty())
            .unwrap();

        let response1 = app.clone().oneshot(request1).await.unwrap();
        assert_eq!(response1.status(), StatusCode::OK);

        // Second origin
        let request2 = Request::builder()
            .uri("/")
            .header("Origin", "https://app.example.com")
            .body(axum::body::Body::empty())
            .unwrap();

        let response2 = app.oneshot(request2).await.unwrap();
        assert_eq!(response2.status(), StatusCode::OK);
    }
}
