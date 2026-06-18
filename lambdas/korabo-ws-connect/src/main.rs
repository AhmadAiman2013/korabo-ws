use aws_config::BehaviorVersion;
use aws_lambda_events::apigw::ApiGatewayWebsocketProxyRequest;
use aws_sdk_dynamodb::Client as DynamoClient;
use jwt::JwtPublicKey;
use lambda_runtime::tracing::{error, init_default_subscriber};
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde_json::{json, Value};
use std::env::var;
use std::sync::Arc;
use ws_core::connection::put_connection;
use ws_core::{auth::extract_claims};

struct State {
    dynamo: DynamoClient,
    jwt: JwtPublicKey,
    connections_table: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_default_subscriber();

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let dynamo = DynamoClient::new(&config);

    let connections_table = String::from("korabo_ws_connections");

    let jwt = JwtPublicKey::from_jwks_file(
        var("JWT_ISSUER").expect("JWT_ISSUER must be set"),
        var("JWT_AUDIENCE").expect("JWT_AUDIENCE must be set"),
    )
    .expect("Failed to load JWKS");

    let state = Arc::new(State {
        dynamo,
        jwt,
        connections_table,
    });

    run(service_fn(
        |event: LambdaEvent<ApiGatewayWebsocketProxyRequest>| {
            let s = state.clone();
            async move { handler(event, s).await }
        },
    ))
    .await
}

async fn handler(
    event: LambdaEvent<ApiGatewayWebsocketProxyRequest>,
    state: Arc<State>,
) -> Result<Value, Error> {
    let connection_id = event
        .payload
        .request_context
        .connection_id
        .as_deref()
        .unwrap_or("unknown");

    // 1. Validate JWT — reject connection with 401 if invalid.
    let claims = match extract_claims(&event.payload, &state.jwt) {
        Ok(c) => c,
        Err(e) => {
            error!(connection_id, error = %e, "JWT validation failed on $connect");
            return Ok(json!({ "statusCode": 401 }));
        }
    };

    let user_id = &claims.sub;

    // 2.0 Persist connection record
    if let Err(e) = put_connection(
        &state.dynamo,
        &state.connections_table,
        connection_id,
        user_id,
    )
    .await
    {
        error!(connection_id, user_id, error = %e, "Failed to store connection");
        return Ok(json!({ "statusCode": 500 }));
    }

    Ok(json!({"statusCode" : 200}))
}
