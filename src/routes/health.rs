use axum::{Json, response::IntoResponse};
use serde::Serialize;

#[derive(Serialize)]
pub struct Health {
    status: &'static str,
}

pub async fn health_check() -> impl IntoResponse {
    Json(Health { status: "ok" })
}
