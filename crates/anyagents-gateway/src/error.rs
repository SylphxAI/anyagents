use axum::body::Body;
use axum::http::{Response, StatusCode, header};
use axum::response::IntoResponse;

/// A gateway failure, mapped to an HTTP response for the harness.
#[derive(Debug, thiserror::Error)]
pub enum Fail {
    #[error("unsupported feature: {0}")]
    Unsupported(String),
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("unknown or missing gateway token")]
    Unauthorized,
    #[error("provider request failed")]
    Upstream,
}

impl Fail {
    pub(crate) fn unsupported(feature: impl Into<String>) -> Self {
        Self::Unsupported(feature.into())
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }

    pub fn unauthorized() -> Self {
        Self::Unauthorized
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::Unsupported(_) | Self::Invalid(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Upstream => StatusCode::BAD_GATEWAY,
        }
    }

    pub(crate) fn error_type(&self) -> &'static str {
        match self {
            Self::Unauthorized => "authentication_error",
            Self::Upstream => "api_error",
            Self::Unsupported(_) | Self::Invalid(_) => "invalid_request_error",
        }
    }
}

impl IntoResponse for Fail {
    fn into_response(self) -> Response<Body> {
        let body = serde_json::json!({
            "error": {
                "type": self.error_type(),
                "message": self.to_string(),
            }
        });
        let bytes = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
        Response::builder()
            .status(self.status())
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(bytes))
            .expect("error response")
    }
}

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;

    use super::Fail;

    #[test]
    fn unauthorized_is_401() {
        let response = Fail::Unauthorized.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
    }
}
