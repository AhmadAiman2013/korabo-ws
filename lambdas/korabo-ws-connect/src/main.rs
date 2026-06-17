use aws_config::BehaviorVersion;
use aws_lambda_events::apigw::ApiGatewayWebsocketProxyRequest;
use aws_sdk_apigatewaymanagement::config::Builder as ApigwBuilder;
use aws_sdk_dynamodb::Client as DynamoClient;
use jwt::JwtPublicKey;
use lambda_runtime::tracing::{error, init_default_subscriber};
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde_json::{json, Value};
use std::env::var;
use std::sync::Arc;
use ws_core::connection::put_connection;
use ws_core::notification::get_unread_count;
use ws_core::presence::get_last_seen;
use ws_core::types::ServerPush;
use ws_core::{auth::extract_claims, management::ManagementClient};

struct State {
    dynamo: DynamoClient,
    apigw: ManagementClient,
    jwt: JwtPublicKey,
    connections_table: String,
    presence_table: String,
    notifications_table: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_default_subscriber();

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let dynamo = DynamoClient::new(&config);

    let endpoint = var("WS_GATEWAY").expect("WS_GATEWAY must be set");
    let apigw_config = ApigwBuilder::from(&config).endpoint_url(endpoint).build();
    let apigw = ManagementClient::new(aws_sdk_apigatewaymanagement::Client::from_conf(
        apigw_config,
    ));

    let connections_table = String::from("korabo_ws_connections");
    let presence_table = String::from("korabo_ws_user_presence");
    let notifications_table = String::from("korabo_ws_notifications");

    let jwt = JwtPublicKey::from_jwks_file(
        var("JWT_ISSUER").expect("JWT_ISSUER must be set"),
        var("JWT_AUDIENCE").expect("JWT_AUDIENCE must be set"),
    )
    .expect("Failed to load JWKS");

    let state = Arc::new(State {
        dynamo,
        apigw,
        jwt,
        connections_table,
        presence_table,
        notifications_table,
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

    // 3.0 Fetch last_seen_at and unread_count
    let last_seen_at = get_last_seen(&state.dynamo, &state.presence_table, user_id)
        .await
        .unwrap_or(None);

    let unread_notification_count =
        get_unread_count(&state.dynamo, &state.notifications_table, user_id)
            .await
            .unwrap_or(0);

    // 4.o Push `connected` event
    let push = ServerPush::Connected {
        last_seen_at,
        unread_notification_count,
    };

    if let Err(e) = state.apigw.post_to_connection(connection_id, &push).await {
        error!(connection_id, error = %e, "Failed to push `connected` event");
    }

    Ok(json!({"statusCode" : 200}))
}
