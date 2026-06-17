use aws_lambda_events::apigw::ApiGatewayWebsocketProxyRequest;
use jwt::{extract_claims_without_bearer, JwtPublicKey};
use claims::Claims;
use crate::errors::WsError;

pub fn extract_claims(
    event: &ApiGatewayWebsocketProxyRequest,
    jwt: &JwtPublicKey,
) -> Result<Claims, WsError> {
    let token = event
        .query_string_parameters
        .first("token")
        .ok_or(WsError::Unauthorized("no token query".to_string()))?;

    extract_claims_without_bearer(token, jwt).map_err(|e| WsError::Unauthorized(e.to_string()))
}