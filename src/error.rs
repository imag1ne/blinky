use axum::{
    Json,
    extract::rejection::JsonRejection,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;
use tracing::{error, warn};

#[derive(Error, Debug)]
pub enum BlinkyError {
    #[error("Failed to set global logger")]
    SetGlobalLogger(#[from] tracing_log::log_tracer::SetLoggerError),
    #[error("Failed to set global subscriber")]
    SetGlobalSubscriber(#[from] tracing::subscriber::SetGlobalDefaultError),

    #[error("Database error: {0}")]
    SqlxError(#[from] sqlx::Error),
    #[error("Failed to parse URL: {0}")]
    UrlParseError(#[from] url::ParseError),
    #[error("Excessive shortcode collisions")]
    ExcessiveShortcodeCollisions,

    #[error("Cache error: {0}")]
    CacheError(String),
    #[error("Queue error: {0}")]
    QueueError(#[from] crate::analytics::QueueError),
    #[error("Internal server error: {0}")]
    InternalServerError(String),

    #[error("Rate limit RPS and burst must be > 0 when rate limiting is enabled")]
    InvalidRateLimitConfig,
}

impl BlinkyError {
    pub fn is_unique_violation(&self) -> bool {
        if let BlinkyError::SqlxError(sqlx::Error::Database(db_err)) = self {
            return db_err.is_unique_violation();
        }

        false
    }
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(&'static str),
    #[error("Not found")]
    NotFound,
    #[error("Conflict")]
    Conflict,
    #[error("Too many requests")]
    TooManyRequests,
    #[error("Service unavailable")]
    ServiceUnavailable,
    #[error("Invalid JSON")]
    InvalidJson(#[from] JsonRejection),
    #[error("Invalid header value")]
    ResponseHeaderError(#[from] header::InvalidHeaderValue),
    #[error(transparent)]
    Internal(BlinkyError),
}

impl ApiError {
    pub fn excessive_shortcode_collisions() -> Self {
        ApiError::from(BlinkyError::ExcessiveShortcodeCollisions)
    }
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    error: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<String>,
}

impl From<url::ParseError> for ApiError {
    fn from(err: url::ParseError) -> Self {
        ApiError::from(BlinkyError::from(err))
    }
}

impl From<BlinkyError> for ApiError {
    fn from(e: BlinkyError) -> Self {
        if e.is_unique_violation() {
            ApiError::Conflict
        } else {
            ApiError::Internal(e)
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // Choose status + public message; log internals.
        //
        // Logging policy:
        // - error!: Unexpected conditions that might indicate bugs
        // - warn!: Expected failures (client errors, transient issues)
        // - No log: Normal validation failures
        let (status, body) = match &self {
            // Client errors - no logging needed (expected failures)
            ApiError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                ErrorBody {
                    code: "bad_request",
                    error: msg,
                    details: None,
                },
            ),
            ApiError::NotFound => (
                StatusCode::NOT_FOUND,
                ErrorBody {
                    code: "not_found",
                    error: "Resource not found",
                    details: Some("The requested short URL does not exist".to_string()),
                },
            ),
            ApiError::Conflict => (
                StatusCode::CONFLICT,
                ErrorBody {
                    code: "conflict",
                    error: "Resource already exists",
                    details: Some("A short URL with this code already exists".to_string()),
                },
            ),
            ApiError::TooManyRequests => (
                StatusCode::TOO_MANY_REQUESTS,
                ErrorBody {
                    code: "too_many_requests",
                    error: "Rate limit exceeded",
                    details: Some("Please wait before making additional requests".to_string()),
                },
            ),
            ApiError::InvalidJson(_) => (
                StatusCode::BAD_REQUEST,
                ErrorBody {
                    code: "invalid_json",
                    error: "Invalid JSON in request body",
                    details: None,
                },
            ),

            // Unexpected internal errors - log as error!
            ApiError::ResponseHeaderError(e) => {
                error!(error = ?e, "Failed to construct response header");

                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorBody {
                        code: "internal_error",
                        error: "Internal server error",
                        details: None,
                    },
                )
            }

            // Service unavailable errors - log as warn! (transient issues)
            ApiError::ServiceUnavailable => {
                warn!("Service unavailable");

                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    ErrorBody {
                        code: "service_unavailable",
                        error: "Service temporarily unavailable",
                        details: Some("Please try again later".to_string()),
                    },
                )
            }

            // Internal errors with specific handling
            ApiError::Internal(e) => match e {
                // Expected transient failures - log as warn!
                BlinkyError::SqlxError(sqlx::Error::PoolTimedOut) => {
                    warn!("Database pool timeout - server overloaded");

                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        ErrorBody {
                            code: "service_unavailable",
                            error: "Service temporarily unavailable",
                            details: Some("DB error, please try again later".to_string()),
                        },
                    )
                }
                BlinkyError::ExcessiveShortcodeCollisions => {
                    warn!("Excessive shortcode collisions - possible capacity issue");

                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        ErrorBody {
                            code: "service_unavailable",
                            error: "Service temporarily unavailable",
                            details: Some(
                                "Unable to generate unique shortcode, please try again later"
                                    .to_string(),
                            ),
                        },
                    )
                }
                // Unexpected errors - log as error!
                other => {
                    error!(error = ?other, "Unexpected internal error");

                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        ErrorBody {
                            code: "internal_error",
                            error: "Internal server error",
                            details: None,
                        },
                    )
                }
            },
        };

        (status, Json(body)).into_response()
    }
}
