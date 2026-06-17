use aws_config::BehaviorVersion;
use aws_lambda_events::apigw::ApiGatewayWebsocketProxyRequest;
use aws_sdk_dynamodb::Client as DynamoClient;
use lambda_runtime::tracing::init_default_subscriber;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};
use ws_core::connection::{delete_connection, get_connection};
use ws_core::presence::update_last_seen;
use ws_core::subscription::delete_connection_subscriptions;

struct State {
    dynamo: DynamoClient,
    connections_table: String,
    subscriptions_table: String,
    presence_table: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_default_subscriber();

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let dynamo = DynamoClient::new(&config);

    let connections_table = String::from("korabo_ws_connections");
    let presence_table = String::from("korabo_ws_user_presence");
    let subscriptions_table = String::from("korabo_ws_chat_subscription");

    let state = Arc::new(State {
        dynamo,
        connections_table,
        subscriptions_table,
        presence_table,
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

    info!(connection_id, "WebSocket $disconnect");

    let user_id = match get_connection(&state.dynamo, &state.connections_table, connection_id).await
    {
        Ok(conn) => conn.user_id,
        Err(e) => {
            error!(connection_id, error = %e, "Connection record not found on disconnect");
            return Ok(json!({ "statusCode": 200 }));
        }
    };

    if let Err(e) = delete_connection(&state.dynamo, &state.connections_table, connection_id).await
    {
        error!(connection_id, error = %e, "Failed to delete connection record");
    }

    if let Err(e) =
        delete_connection_subscriptions(&state.dynamo, &state.subscriptions_table, connection_id)
            .await
    {
        error!(connection_id, error = %e, "Failed to clean up chat subscriptions");
    }

    if let Err(e) = update_last_seen(&state.dynamo, &state.presence_table, &user_id).await {
        error!(user_id, error = %e, "Failed to update user presence");
    }

    Ok(json!({ "statusCode": 200 }))
}
