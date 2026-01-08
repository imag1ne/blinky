use axum::{
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::IntoResponse,
};

use crate::{analytics::ClickEvent, error::ApiError, state::AppState, storage::Storage};

pub async fn redirect<Gen, Store>(
    Path(shortcode): Path<String>,
    State(state): State<AppState<Gen, Store>>,
) -> Result<impl IntoResponse, ApiError>
where
    Gen: Clone + Send + Sync + 'static,
    Store: Storage + Clone + Send + Sync + 'static,
{
    // Fetch URL
    let original_url = state
        .store
        .get_url(&shortcode)
        .await?
        .ok_or_else(|| ApiError::NotFound)?;

    // Helper to spawn bounded background task for click increment
    let spawn_fallback_increment = || {
        if let Ok(permit) = state
            .fallback_increment_semaphore
            .clone()
            .try_acquire_owned()
        {
            let store = state.store.clone();
            let shortcode = shortcode.clone();
            tokio::spawn(async move {
                let _permit = permit; // Hold permit until task completes
                if let Err(e) = store.increment_click(&shortcode).await {
                    tracing::error!(
                        error = %e,
                        shortcode = %shortcode,
                        "Failed to increment click counter in background"
                    );
                }
            });
        } else {
            tracing::warn!(
                shortcode = %shortcode,
                "Too many fallback increment tasks in flight, dropping click event"
            );
        }
    };

    // Enqueue click event for async processing
    if let Some(ref queue) = state.click_queue {
        let event = ClickEvent::new(shortcode.clone());
        if let Err(e) = queue.enqueue(event).await {
            // Log error but don't fail the redirect
            tracing::warn!(
                error = %e,
                shortcode = %shortcode,
                "Failed to enqueue click event, falling back to direct increment"
            );
            spawn_fallback_increment();
        }
    } else {
        // No queue configured - use direct DB increment
        spawn_fallback_increment();
    }

    // Return redirect immediately
    Ok((
        StatusCode::MOVED_PERMANENTLY,
        [(
            header::LOCATION,
            HeaderValue::from_str(original_url.as_str())?,
        )],
    ))
}
