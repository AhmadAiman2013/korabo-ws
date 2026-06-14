use thiserror::Error;

#[derive(Debug, Error)]
pub enum WsError {
    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("DynamoDB error: {0}")]
    DynamoDB(String),

    #[error("Connection gone: {0}")]
    ConnectionGone(String),

    #[error("Management API error: {0}")]
    ManagementApi(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl WsError {
    /// HTTP status code for $connect rejection. Route handler errors
    /// should always return 200 and push the error via management API instead.
    pub fn status_code(&self) -> u16 {
        match self {
            WsError::Unauthorized(_) => 401,
            WsError::Forbidden(_) => 403,
            WsError::BadRequest(_) => 400,
            WsError::NotFound(_) => 404,
            _ => 500,
        }
    }
}
