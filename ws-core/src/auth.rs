use aws_lambda_events::apigw::ApiGatewayWebsocketProxyRequest;
use jwt::{extract_claims as extract_jwt_claims, JwtPublicKey};
use claims::Claims;
use crate::errors::WsError;

pub fn extract_claims(
    event: &ApiGatewayWebsocketProxyRequest,
    jwt: &JwtPublicKey,
) -> Result<Claims, WsError> {
    let token = event
        .query_string_parameters
        .first("token")
        .ok_or(WsError::Unauthorized)?;

    extract_jwt_claims(token, jwt).map_err(|_| WsError::Unauthorized)
}