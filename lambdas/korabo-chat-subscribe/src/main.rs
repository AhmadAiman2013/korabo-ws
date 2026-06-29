use aws_config::BehaviorVersion;
use aws_lambda_events::apigw::ApiGatewayWebsocketProxyRequest;
use aws_sdk_apigatewaymanagement::config::Builder as ApigwBuilder;
use aws_sdk_dynamodb::Client as DynamoClient;
use lambda_runtime::tracing::{error, init_default_subscriber};
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde_json::{json, Value};
use std::env::var;
use std::sync::Arc;
use ws_core::management::ManagementClient;
use ws_core::membership::require_connection_and_membership;
use ws_core::subscription::{delete_subscription, put_subscription};
use ws_core::types::{ClientMessage, ServerPush};

struct State {
    dynamo: DynamoClient,
    apigw: ManagementClient,
    connections_table: String,
    subscriptions_table: String,
    members_table: String,
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
    let subscriptions_table = String::from("korabo_ws_chat_subscription");
    let members_table = String::from("korabo_group_members");

    let state = Arc::new(State {
        dynamo,
        apigw,
        connections_table,
        subscriptions_table,
        members_table,
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

    let body = event.payload.body.unwrap_or("unknown".to_string());
    let msg: ClientMessage = match serde_json::from_str(&body) {
        Ok(m) => m,
        Err(e) => {
            let _ = state
                .apigw
                .post_to_connection(
                    connection_id,
                    &ServerPush::Error {
                        code: "BAD_REQUEST".into(),
                        message: format!("Invalid message body: {}", e),
                    },
                )
                .await;
            return Ok(json!({"statusCode" : 200}));
        }
    };

    match msg {
        ClientMessage::ChatJoin { group_id } => {
            let conn = match require_connection_and_membership(
                &state.dynamo,
                &state.connections_table,
                &state.members_table,
                &state.apigw,
                connection_id,
                &group_id,
            )
            .await
            {
                Ok(c) => c,
                Err(_) => return Ok(json!({"statusCode" : 200})),
            };

            if let Err(e) = put_subscription(
                &state.dynamo,
                &state.subscriptions_table,
                &group_id,
                connection_id,
                &conn.user_id,
            )
            .await
            {
                error!(connection_id, group_id, error = %e, "Failed to put subscription");
            }

            let _ = state
                .apigw
                .post_to_connection(
                    connection_id,
                    &ServerPush::Ack {
                        action: "chat.join".into(),
                    },
                )
                .await;
        }

        ClientMessage::ChatLeave { group_id } => {
            if let Err(e) = delete_subscription(
                &state.dynamo,
                &state.subscriptions_table,
                &group_id,
                connection_id,
            )
            .await
            {
                error!(connection_id, group_id, error = %e, "Failed to delete subscription");
            }

            let _ = state
                .apigw
                .post_to_connection(
                    connection_id,
                    &ServerPush::Ack {
                        action: "chat.leave".into(),
                    },
                )
                .await;
        }
    }

    Ok(json!({"statusCode" : 200}))
}
