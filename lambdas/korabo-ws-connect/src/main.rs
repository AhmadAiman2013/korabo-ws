use std::env::var;
use std::sync::Arc;
use aws_config::BehaviorVersion;
use aws_lambda_events::apigw::ApiGatewayWebsocketProxyRequest;
use aws_sdk_apigatewaymanagement::config::Builder as ApigwBuilder;
use aws_sdk_dynamodb::Client as DynamoClient;
use jwt::JwtPublicKey;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use lambda_runtime::tracing::{init_default_subscriber, warn};
use serde_json::{json, Value};
use ws_core::{
    auth::extract_claims,
    management::ManagementClient
};

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

    let endpoint = var("WS_GATEWAY")
        .expect("WS_GATEWAY must be set");
    let apigw_config = ApigwBuilder::from(&config)
        .endpoint_url(endpoint)
        .build();
    let apigw = ManagementClient::new(
        aws_sdk_apigatewaymanagement::Client::from_conf(apigw_config),
    );

    let connections_table = String::from("korabo_ws_connections");
    let presence_table = String::from("korabo_ws_user_presence");
    let notifications_table = String::from("korabo_ws_notifications");

    let jwt = JwtPublicKey::from_jwks_file(
        var("JWT_ISSUER").expect("JWT_ISSUER must be set"),
        var("JWT_AUDIENCE").expect("JWT_AUDIENCE must be set"),
    )
        .expect("Failed to load JWKS");

    let state = Arc::new( State{
        dynamo,
        apigw,
        jwt,
        connections_table,
        presence_table,
        notifications_table
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
    _event: LambdaEvent<ApiGatewayWebsocketProxyRequest>,
    _state: Arc<State>
) -> Result<Value, Error> {



    Ok(json!({ "statusCode": 200 }))

}