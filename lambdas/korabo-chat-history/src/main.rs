use std::env::var;
use std::sync::Arc;
use aws_config::BehaviorVersion;
use aws_lambda_events::apigw::ApiGatewayWebsocketProxyRequest;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_apigatewaymanagement::config::Builder as ApigwBuilder;
use lambda_runtime::tracing::init_default_subscriber;
use serde_json::{json, Value};
use tracing::error;
use ws_core::chat::get_chat_history;
use ws_core::management::ManagementClient;
use ws_core::membership::require_connection_and_membership;
use ws_core::presence::get_last_seen;
use ws_core::types::{ClientMessage, ServerPush};

struct State {
    dynamo: DynamoClient,
    apigw: ManagementClient,
    connections_table: String,
    messages_table: String,
    members_table: String,
    presence_table: String,
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
    let messages_table = String::from("korabo_ws_chat_messages");
    let members_table = String::from("korabo_group_members");
    let presence_table = String::from("korabo_ws_user_presence");

    let state = Arc::new(State {
        dynamo,
        apigw,
        connections_table,
        messages_table,
        members_table,
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
    // small helper for the common OK response
    let ok_resp = || json!({ "statusCode": 200 });

    let connection_id = event
        .payload
        .request_context
        .connection_id
        .as_deref()
        .unwrap_or("unknown");

    let body = event.payload.body.unwrap_or_default();
    let group_id = match serde_json::from_str::<ClientMessage>(&body) {
        Ok(ClientMessage::ChatHistory { group_id}) => group_id,
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
        Err(_) => {
            error!("Failed to assert connection and membership");
            let _ = state
                .apigw
                .post_to_connection(
                    connection_id,
                    &ServerPush::Error {
                        code: "INTERNAL_ERROR".into(),
                        message: "Failed to assert connection and membership".into(),
                    },
                )
                .await;
            return Ok(json!({ "statusCode": 200 }));
        }
    };

    let last_seen_at = get_last_seen(&state.dynamo, &state.presence_table, &*conn.user_id)
        .await
        .unwrap_or(None);

    let messages = match get_chat_history(&state.dynamo, &state.messages_table, &group_id, last_seen_at)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            error!(group_id, error = %e, "Failed to fetch chat history");
            let _ = state
                .apigw
                .post_to_connection(
                    connection_id,
                    &ServerPush::Error {
                        code: "INTERNAL_ERROR".into(),
                        message: "Failed to fetch history".into(),
                    },
                )
                .await;
            return Ok(json!({ "statusCode": 200 }));
        }
    };

    let _ = state
        .apigw
        .post_to_connection(
            connection_id,
            &ServerPush::ChatHistory {
                group_id,
                messages,
            },
        )
        .await;

    Ok(json!({ "statusCode": 200 }))
}