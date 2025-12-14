use axum::{
    Router,
    extract::FromRef,
    routing::{get, post},
};

use crate::shortcode::GenerateShortcode;
use crate::state::AppState;
use crate::storage::Storage;

pub mod health;
pub mod redirect;
pub mod shorten;
pub mod stats;

/// Build router with all routes configured
pub fn build_router<Gen, Store>(state: AppState<Gen, Store>) -> Router
where
    Gen: GenerateShortcode + Clone + Send + Sync + 'static,
    Store: Storage + Clone + Send + Sync + 'static,
    Store: FromRef<AppState<Gen, Store>>,
{
    Router::new()
        .route("/health_check", get(health::health_check))
        .route("/shorten", post(shorten::shorten_url::<Gen, Store>))
        .route("/{code}", get(redirect::redirect::<Gen, Store>))
        .route("/stats/{code}", get(stats::stats::<Store>))
        .with_state(state)
}
