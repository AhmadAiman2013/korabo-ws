use aws_config::BehaviorVersion;
use aws_lambda_events::apigw::ApiGatewayWebsocketProxyRequest;
use aws_sdk_apigatewaymanagement::config::Builder as ApigwBuilder;
use aws_sdk_dynamodb::Client as DynamoClient;
use lambda_runtime::tracing::init_default_subscriber;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde_json::{json, Value};
use std::env::var;
use std::sync::Arc;
use tracing::error;
use ws_core::chat::put_chat_message;
use ws_core::connection::get_connection;
use ws_core::management::ManagementClient;
use ws_core::subscription::{get_group_subscribers, is_connection_subscribed};
use ws_core::types::{ClientMessage, ServerPush};

struct State {
    dynamo: DynamoClient,
    apigw: ManagementClient,
    connections_table: String,
    subscriptions_table: String,
    messages_table: String,
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
    let messages_table = String::from("korabo_ws_chat_messages");

    let state = Arc::new(State {
        dynamo,
        apigw,
        connections_table,
        subscriptions_table,
        messages_table,
    });

    run(service_fn(
        |event: LambdaEvent<ApiGatewayWebsocketProxyRequest>| {
            let s = state.clone();
            async move { handler(event, s).await }
        },
    ))
    .await
}

// handler
async fn handler(
    event: LambdaEvent<ApiGatewayWebsocketProxyRequest>,
    state: Arc<State>,
) -> Result<Value, Error> {
    // small helper for the common OK response
    let ok_resp = || json!({ "statusCode": 200 });

    let connection_id = event
        .payload
        .request_context
        .connection_id
        .as_deref()
        .unwrap_or("unknown");

    let conn = match get_connection(&state.dynamo, &state.connections_table, connection_id).await {
        Ok(c) => c,
        Err(e) => {
            error!(connection_id, error = %e, "Connection record not found");
            return Ok(ok_resp());
        }
    };

    let body = event.payload.body.unwrap_or_default();
    let (group_id, content) = match serde_json::from_str::<ClientMessage>(&body) {
        Ok(ClientMessage::ChatSend { group_id, content }) => (group_id, content),
        Ok(_) => return Ok(ok_resp()),
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
            return Ok(ok_resp());
        }
    };

    // 1. ensure the user is member of group

    let is_subscribed = match is_connection_subscribed(
        &state.dynamo,
        &state.subscriptions_table,
        &group_id,
        &connection_id,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => {
            error!(connection_id = %connection_id, group_id = %group_id, error = %e, "Failed to determine group membership");

            let _ = state
                .apigw
                .post_to_connection(
                    connection_id,
                    &ServerPush::Error {
                        code: "INTERNAL_ERROR".into(),
                        message: "Unable to verify group membership".into(),
                    },
                )
                .await;
            return Ok(ok_resp());
        }
    };

    if !is_subscribed {

        let _ = state
            .apigw
            .post_to_connection(
                connection_id,
                &ServerPush::Error {
                    code: "UNAUTHORIZED".into(),
                    message: format!("You are not a member of group {}", group_id),
                },
            )
            .await;
        return Ok(ok_resp());
    }

    // 2. Persist message.
    let record = match put_chat_message(
        &state.dynamo,
        &state.messages_table,
        &group_id,
        &conn.user_id,
        &content,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            error!(connection_id, group_id, error = %e, "Failed to persist message");
            let _ = state
                .apigw
                .post_to_connection(
                    connection_id,
                    &ServerPush::Error {
                        code: "INTERNAL_ERROR".into(),
                        message: "Failed to send message".into(),
                    },
                )
                .await;
            return Ok(ok_resp());
        }
    };

    // 3. Build the push payload once.
    // sort_key == message_id from the client's perspective.
    let push = ServerPush::ChatMessage {
        group_id: record.group_id.clone(),
        message_id: record.sort_key.clone(),
        sender_id: record.sender_id.clone(),
        content: record.content.clone(),
        created_at: record.created_at.clone(),
    };

    // 4. Fan out to all connections currently subscribed to this group.
    let subscribers = get_group_subscribers(&state.dynamo, &state.subscriptions_table, &group_id)
        .await
        .unwrap_or_else(|e| {
            error!(group_id, error = %e, "Failed to fetch group subscribers");
            vec![]
        });

    for conn_id in &subscribers {
        // push_or_ignore_gone returns false for stale entries — in production
        // you could batch-delete them here, but for now we rely on TTL.
        state.apigw.push_or_ignore_gone(conn_id, &push).await;
    }

    Ok(ok_resp())
}
