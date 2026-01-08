use axum::{
    Json,
    extract::{Path, State},
    response::IntoResponse,
};
use chrono::{DateTime, Utc};

use crate::{error::ApiError, storage::Storage};

pub async fn stats<Store>(
    Path(shortcode): Path<String>,
    State(store): State<Store>,
) -> Result<impl IntoResponse, ApiError>
where
    Store: Storage + Send + Sync + 'static,
{
    let stats = store
        .get_stats(&shortcode)
        .await?
        .ok_or_else(|| ApiError::NotFound)?;

    let body = Json(StatsResponse {
        shortcode,
        click_count: stats.click_count,
        last_clicked_at: stats.last_clicked_at,
    });

    Ok(body)
}

#[derive(serde::Serialize)]
pub struct StatsResponse {
    pub shortcode: String,
    pub click_count: i64,
    pub last_clicked_at: Option<DateTime<Utc>>,
}
