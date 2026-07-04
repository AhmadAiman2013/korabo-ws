use std::env::var;
use aws_config::BehaviorVersion;
use aws_lambda_events::http::header::{AUTHORIZATION, CONTENT_TYPE};
use aws_lambda_events::http::{Method, StatusCode};
use lambda_http::tracing::init_default_subscriber;
use lambda_http::{run, Error};
use aws_sdk_dynamodb::Client as DynamoClient;
use axum::{Json, Router};
use axum::extract::State;
use axum::routing::get;
use jwt::{AuthClaims, JwtPublicKey};
use serde::Deserialize;
use serde_json::{json, Value};
use tower_http::cors::CorsLayer;
use ws_core::chat::get_chat_history_page;
use ws_core::errors::ResponseError;
use ws_core::membership::assert_group_member;
use ws_core::utils::health_check;

#[derive(Clone)]
struct AppState {
    dynamo: DynamoClient,
    jwt: JwtPublicKey,
    messages_table: String,
    members_table: String,
}

impl AsRef<JwtPublicKey> for AppState {
    fn as_ref(&self) -> &JwtPublicKey {
        &self.jwt
    }
}

#[derive(Deserialize, Debug)]
struct ChatMessageHistoryRequest {
    group_id: String,
    cursor: Option<String>,
    limit: Option<i32>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_default_subscriber();

    let jwt = JwtPublicKey::from_jwks_file(
        var("JWT_ISSUER").expect("JWT_ISSUER must be set"),
        var("JWT_AUDIENCE").expect("JWT_AUDIENCE must be set"),
    )
        .expect("Failed to load JWKS");

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;

    let dynamo = DynamoClient::new(&config);

    let messages_table = String::from("korabo_ws_chat_messages");
    let members_table = String::from("korabo_group_members");

    let origins = [
        "https://d3h6bl8rffsevw.cloudfront.net".parse()?,
        "http://localhost:4200".parse()?,
    ];

    let cors = CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([CONTENT_TYPE, AUTHORIZATION]);


    let state = AppState {
        dynamo,
        jwt,
        messages_table,
        members_table,
    };

    let app = Router::new()
        .nest(
            "/chat",
            Router::new()
                .route("/health", get(health_check))
                .route("/chat-history", get(chat_message_history))
                .with_state(state),
        )
        .layer(cors);

    run(app).await
}

async fn chat_message_history(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Json(body): Json<ChatMessageHistoryRequest>,
) -> Result<(StatusCode, Json<Value>), ResponseError> {
    let user_id = &claims.sub;
    let ChatMessageHistoryRequest { group_id, cursor, limit } = body;

    assert_group_member(&state.dynamo, &state.members_table, &group_id, &user_id).await?;

    let default_limit = limit.unwrap_or(20);

    let (messages, next_cursor) = get_chat_history_page(
        &state.dynamo,
        &state.messages_table,
        &group_id,
        cursor,
        Some(default_limit),
    )
        .await?;

    Ok((
        StatusCode::OK,
        Json(json!({
            "body": {
                "group_id": group_id,
                "messages": messages,
                "next_cursor": next_cursor,
            }
        })),
    ))
}

