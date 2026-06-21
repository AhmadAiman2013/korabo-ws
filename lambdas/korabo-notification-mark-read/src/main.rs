use std::env::var;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client;
use axum::extract::State;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Method, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use jwt::{AuthClaims, JwtPublicKey};
use lambda_http::tracing::init_default_subscriber;
use lambda_http::{run, Error};
use serde::Deserialize;
use serde_json::{json, Value};
use tower_http::cors::CorsLayer;
use ws_core::errors::ResponseError;
use ws_core::notification::mark_notifications_read;

#[derive(Clone)]
struct AppState {
    dynamo: Client,
    jwt: JwtPublicKey,
    notification_table: String,
}

impl AsRef<JwtPublicKey> for AppState {
    fn as_ref(&self) -> &JwtPublicKey {
        &self.jwt
    }
}

#[derive(Deserialize, Debug)]
struct NotificationReadRequest {
    notification_ids: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_default_subscriber();

    let jwt = JwtPublicKey::from_jwks_file(
        var("JWT_ISSUER").expect("JWT_ISSUER must be set"),
        var("JWT_AUDIENCE").expect("JWT_AUDIENCE must be set"),
    ).expect("Failed to load JWKS");

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;

    let dynamo = Client::new(&config);

    let notification_table = String::from("korabo_ws_notifications");

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
        notification_table,
    };

    let app = Router::new()
        .nest(
            "/notification",
            Router::new()
                .route("/health", get(health_check))
                .route("/mark-read", post(mark_noti_read))
                .with_state(state),
        )
        .layer(cors);

    run(app).await
}

async fn mark_noti_read(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Json(body): Json<NotificationReadRequest>,
) -> Result<(StatusCode, Json<Value>), ResponseError> {
    let user_id = &claims.sub;
    let NotificationReadRequest { notification_ids } = body;

    mark_notifications_read(
        &state.dynamo,
        &state.notification_table,
        user_id,
        &*notification_ids,
    )
    .await?;

    Ok((StatusCode::CREATED, Json(json!({
        "message": "Notification was marked as read",
    }))))
}

async fn health_check() -> Json<Value> {
    let health = true;
    match health {
        true => Json(json!({ "status": "healthy" })),
        false => Json(json!({ "status": "unhealthy" })),
    }
}
