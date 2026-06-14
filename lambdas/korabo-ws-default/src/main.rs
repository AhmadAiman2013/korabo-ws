use aws_lambda_events::apigw::ApiGatewayWebsocketProxyRequest;
use lambda_runtime::tracing::{init_default_subscriber, warn};
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde_json::{json, Value};

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_default_subscriber();

    run(service_fn(handler)).await
}

async fn handler(event: LambdaEvent<ApiGatewayWebsocketProxyRequest>) -> Result<Value, Error> {
    let connection_id = event
        .payload
        .request_context
        .connection_id
        .as_deref()
        .unwrap_or("unknown");

    let route = event
        .payload
        .request_context
        .route_key
        .as_deref()
        .unwrap_or("unknown");

    warn!(connection_id, route, "Unmatched WebSocket route ($default)");

    Ok(json!({"statusCode" : 200}))
}
