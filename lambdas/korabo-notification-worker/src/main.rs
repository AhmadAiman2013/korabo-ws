use aws_config::BehaviorVersion;
use aws_lambda_events::sqs::SqsEvent;
use aws_sdk_apigatewaymanagement::config::Builder as ApigwBuilder;
use aws_sdk_dynamodb::Client as DynamoClient;
use lambda_runtime::tracing::init_default_subscriber;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde_json::from_str;
use std::env::var;
use std::sync::Arc;
use tracing::error;
use ws_core::connection::get_user_connections;
use ws_core::errors::WsError;
use ws_core::management::ManagementClient;
use ws_core::notification::put_notification;
use ws_core::types::{ServerPush, SqsNotificationEvent};

struct State {
    dynamo: DynamoClient,
    apigw: ManagementClient,
    connections_table: String,
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
    let notifications_table = String::from("korabo_ws_notifications");

    let state = Arc::new(State {
        dynamo,
        apigw,
        connections_table,
        notifications_table,
    });

    run(service_fn(|event: LambdaEvent<SqsEvent>| {
        let s = state.clone();
        async move { handler(event, s).await }
    }))
    .await
}

async fn handler(event: LambdaEvent<SqsEvent>, state: Arc<State>) -> Result<(), Error> {
    for record in event.payload.records {
        let body = match record.body {
            Some(b) => b,
            None => {
                error!(
                    "Received SQS record with no body, message_id: {:?}",
                    record.message_id
                );
                continue;
            }
        };

        let noti_event: SqsNotificationEvent = match from_str(&body) {
            Ok(e) => e,
            Err(err) => {
                error!("Failed to deserialize message: {:?} body: {}", err, body);
                continue;
            }
        };

        if let Err(err) = process_event(&noti_event, &state).await {
            error!(
                "Failed to process message_id {:?}: {}",
                record.message_id, err
            );
            return Err(err.into());
        }
    }
    Ok(())
}

async fn process_event(evt: &SqsNotificationEvent, state: &State) -> Result<(), WsError> {
    let user_ids: Vec<String> = evt.targeting.user_ids.clone();

    let exclude: Vec<&str> = evt
        .targeting
        .exclude_user_ids
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|s| s.as_str())
        .collect();

    for user_id in &user_ids {
        if exclude.contains(&user_id.as_str()) {
            continue;
        }

        // 1. Persist notification record (also sets sparse GSI attribute).
        let record = put_notification(
            &state.dynamo,
            &state.notifications_table,
            user_id,
            &evt.event_type,
            &evt.actor_id,
            evt.payload.clone(),
        )
        .await?;

        // 2. Push to all the user's active connections (multi-tab support).
        let connections = get_user_connections(&state.dynamo, &&state.connections_table, user_id)
            .await
            .unwrap_or_default();

        let push = ServerPush::Notification {
            notification_id: record.notification_id,
            notification_type: record.notification_type,
            actor_id: record.actor_id,
            payload: record.payload,
            created_at: record.created_at,
        };

        for conn_id in &connections {
            state.apigw.push_or_ignore_gone(conn_id, &push).await;
        }
    }

    Ok(())
}
