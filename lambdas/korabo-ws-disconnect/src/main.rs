use std::env::var;
use aws_config::BehaviorVersion;
use aws_lambda_events::apigw::ApiGatewayWebsocketProxyRequest;
use aws_sdk_apigatewaymanagement::config::Builder as ApigwBuilder;
use aws_sdk_dynamodb::Client as DynamoClient;
use lambda_runtime::tracing::init_default_subscriber;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};
use ws_core::connection::{delete_connection, get_connection};
use ws_core::management::ManagementClient;
use ws_core::presence::update_last_seen;
use ws_core::subscription::{delete_connection_subscriptions, get_connection_groups, get_group_subscribers};
use ws_core::types::ServerPush;

struct State {
    dynamo: DynamoClient,
    apigw: ManagementClient,
    connections_table: String,
    subscriptions_table: String,
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
    let presence_table = String::from("korabo_ws_user_presence");
    let subscriptions_table = String::from("korabo_ws_chat_subscription");

    let state = Arc::new(State {
        dynamo,
        apigw,
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

    let group_ids = get_connection_groups(&state.dynamo, &state.subscriptions_table, connection_id).await?;

    for id in group_ids {
        let subscribers = get_group_subscribers(&state.dynamo, &state.subscriptions_table, &id)
            .await
            .unwrap_or_else(|e| {
                error!(id, error = %e, "Failed to fetch group subscribers");
                vec![]
            });


        for conn_id in &subscribers {
            state.
                apigw
                .push_or_ignore_gone(
                    conn_id,
                    &ServerPush::ChatOnlinePresence {
                        group_id: id.clone(),
                        user_id: user_id.clone(),
                        status: "offline".into(),
                    },
                )
                .await;
        }
    }



    Ok(json!({ "statusCode": 200 }))
}
