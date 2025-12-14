use axum::{
    extract::{FromRequest, Json, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use url::{Host, Url};

use crate::{error::ApiError, shortcode::GenerateShortcode, state::AppState, storage::Storage};

#[derive(Deserialize)]
pub struct ShortenRequest {
    pub url: String,
}

pub struct ValidatedShortenRequest {
    url: Url,
}

#[derive(Serialize)]
pub struct ShortenResponse {
    pub short_url: String,
}

pub async fn shorten_url<Gen, Store>(
    State(state): State<AppState<Gen, Store>>,
    request_body: ValidatedShortenRequest,
) -> Result<impl IntoResponse, ApiError>
where
    Gen: GenerateShortcode + Send + Sync + 'static,
    Store: Storage + Send + Sync + 'static,
{
    let shortcode = {
        let mut attempts = 0;
        loop {
            let sc = state.shortcode_generator.generate_shortcode();

            match state.store.insert_url(&sc, &request_body.url).await {
                Ok(_) => break sc,
                Err(e) if e.is_unique_violation() => {
                    attempts += 1;
                    if attempts >= state.shortcode_max_retries {
                        return Err(ApiError::excessive_shortcode_collisions());
                    }
                }
                Err(e) => return Err(ApiError::from(e)),
            }
        }
    };

    let short_url = state.base_url.join(&shortcode)?;

    let mut headers = HeaderMap::with_capacity(1);
    headers.insert(header::LOCATION, HeaderValue::from_str(short_url.as_str())?);

    let body = Json(ShortenResponse {
        short_url: short_url.to_string(),
    });

    Ok((StatusCode::CREATED, headers, body))
}

impl<S> FromRequest<S> for ValidatedShortenRequest
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Json(body) = Json::<ShortenRequest>::from_request(req, state).await?;
        let url = Url::parse(body.url.trim()).map_err(|_| ApiError::BadRequest("Invalid URL"))?;
        validate_url(&url)?;
        Ok(ValidatedShortenRequest { url })
    }
}

fn validate_url(url: &Url) -> Result<(), ApiError> {
    matches!(url.scheme(), "http" | "https")
        .then_some(())
        .ok_or_else(|| ApiError::BadRequest("Only http and https are allowed"))?;

    match url.host() {
        Some(Host::Domain(d)) => {
            let d = d.to_ascii_lowercase();
            if d == "localhost" || d.ends_with(".local") {
                return Err(ApiError::BadRequest("Localhost URLs are not allowed"));
            }
        }
        Some(Host::Ipv4(ip)) => {
            if ip.is_loopback() || ip.is_private() || ip.is_link_local() {
                return Err(ApiError::BadRequest(
                    "Private/loopback IP URLs are not allowed",
                ));
            }
        }
        Some(Host::Ipv6(ip)) => {
            if ip.is_loopback() || ip.is_unspecified() || ip.is_unique_local() || ip.is_multicast()
            {
                return Err(ApiError::BadRequest(
                    "Private/loopback IP URLs are not allowed",
                ));
            }
        }
        None => return Err(ApiError::BadRequest("Invalid URL")),
    }

    Ok(())
}
