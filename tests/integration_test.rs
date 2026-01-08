use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use blinky::{
    routes,
    shortcode::base58::Base58Shortcode,
    state::AppState,
    storage::{Storage, postgresql::PgStorage},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};
use tower::ServiceExt;
use url::Url;

/// Build a unique database name for isolated testing
fn unique_db_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}_{}", prefix, nanos)
}

/// Create a temporary test database with schema
async fn setup_test_db() -> PgPool {
    // Base server URL for creating test databases
    let base = env::var("TEST_DATABASE_URL")
        .or_else(|_| env::var("DATABASE_URL"))
        .expect("Set TEST_DATABASE_URL or DATABASE_URL to a Postgres URL");

    let mut server_url = Url::parse(&base).expect("Invalid Postgres URL");
    server_url.set_path("postgres");

    // Connect to postgres maintenance DB
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(server_url.as_str())
        .await
        .expect("Failed to connect to Postgres");

    // Create test database
    let db_name = unique_db_name("blinky_integration_test");
    let create_stmt = format!(r#"CREATE DATABASE "{}""#, db_name);
    admin_pool
        .execute(create_stmt.as_str())
        .await
        .expect("Failed to create test database");

    // Connect to the new database
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

/// Build test application with in-memory state
fn build_test_app(storage: PgStorage) -> axum::Router {
    let state = AppState::new(
        Url::parse("http://localhost:8080").unwrap(),
        storage,
        Base58Shortcode,
        5,
    );

    axum::Router::new()
        .route(
            "/health_check",
            axum::routing::get(routes::health::health_check),
        )
        .route(
            "/shorten",
            axum::routing::post(routes::shorten::shorten_url::<Base58Shortcode, PgStorage>),
        )
        .route(
            "/{code}",
            axum::routing::get(routes::redirect::redirect::<Base58Shortcode, PgStorage>),
        )
        .route(
            "/stats/{code}",
            axum::routing::get(routes::stats::stats::<PgStorage>),
        )
        .with_state(state)
}

#[tokio::test]
#[ignore] // Requires database
async fn test_shorten_and_redirect_flow() {
    // Setup
    let pool = setup_test_db().await;
    let storage = PgStorage::new(pool);
    let app = build_test_app(storage);

    // Step 1: Shorten a URL
    let shorten_request = Request::builder()
        .method("POST")
        .uri("/shorten")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "url": "https://example.com/test-page" }).to_string(),
        ))
        .unwrap();

    let shorten_response = app.clone().oneshot(shorten_request).await.unwrap();

    assert_eq!(shorten_response.status(), StatusCode::CREATED);
    assert!(shorten_response.headers().contains_key("location"));

    // Parse response body
    let body_bytes = shorten_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let shorten_body: Value = serde_json::from_slice(&body_bytes).unwrap();
    let short_url = shorten_body["short_url"].as_str().unwrap();

    // Extract shortcode from short_url (e.g., "http://localhost:8080/abc123" -> "abc123")
    let shortcode = short_url.split('/').next_back().unwrap();
    assert_eq!(shortcode.len(), 7); // Base58 shortcode length

    // Step 2: Redirect using the shortcode
    let redirect_request = Request::builder()
        .method("GET")
        .uri(format!("/{}", shortcode))
        .body(Body::empty())
        .unwrap();

    let redirect_response = app.clone().oneshot(redirect_request).await.unwrap();

    assert_eq!(redirect_response.status(), StatusCode::MOVED_PERMANENTLY);

    let location_header = redirect_response
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap();

    assert_eq!(location_header, "https://example.com/test-page");
}

#[tokio::test]
#[ignore] // Requires database
async fn test_get_stats() {
    // Setup
    let pool = setup_test_db().await;
    let storage = PgStorage::new(pool);
    let app = build_test_app(storage.clone());

    // Step 1: Create a short URL directly
    let test_url = Url::parse("https://example.com/stats-test").unwrap();
    let shortcode = "test123";
    storage.insert_url(shortcode, &test_url).await.unwrap();

    // Step 2: Increment click count
    storage.increment_click(shortcode).await.unwrap();
    storage.increment_click(shortcode).await.unwrap();

    // Step 3: Get stats
    let stats_request = Request::builder()
        .method("GET")
        .uri(format!("/stats/{}", shortcode))
        .body(Body::empty())
        .unwrap();

    let stats_response = app.oneshot(stats_request).await.unwrap();

    assert_eq!(stats_response.status(), StatusCode::OK);

    // Parse response
    let body_bytes = stats_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let stats_body: Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(stats_body["click_count"], 2);
    assert!(stats_body["last_clicked_at"].is_string());
}

#[tokio::test]
#[ignore] // Requires database
async fn test_not_found() {
    let pool = setup_test_db().await;
    let storage = PgStorage::new(pool);
    let app = build_test_app(storage);

    // Try to access a non-existent shortcode
    let request = Request::builder()
        .method("GET")
        .uri("/nonexistent")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(body["code"], "not_found");
    assert_eq!(body["error"], "Resource not found");
    assert!(body["details"].is_string());
}

#[tokio::test]
#[ignore] // Requires database
async fn test_invalid_url_rejected() {
    let pool = setup_test_db().await;
    let storage = PgStorage::new(pool);
    let app = build_test_app(storage);

    // Try to shorten an invalid URL
    let request = Request::builder()
        .method("POST")
        .uri("/shorten")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "url": "not-a-valid-url" }).to_string()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(body["code"], "bad_request");
}

#[tokio::test]
#[ignore] // Requires database
async fn test_localhost_url_rejected() {
    let pool = setup_test_db().await;
    let storage = PgStorage::new(pool);
    let app = build_test_app(storage);

    // Try to shorten localhost URL (security measure)
    let request = Request::builder()
        .method("POST")
        .uri("/shorten")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "url": "http://localhost:3000/admin" }).to_string(),
        ))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(body["code"], "bad_request");
    assert!(body["error"].as_str().unwrap().contains("Localhost"));
}
