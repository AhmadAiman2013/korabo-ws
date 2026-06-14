use crate::errors::WsError;
use crate::utils::{now_rfc3339, ttl_hours};
use aws_sdk_dynamodb::Client;
use serde::{Deserialize, Serialize};
use serde_dynamo::to_item;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WsConnection {
    pub connection_id: String,
    pub user_id: String,
    pub connected_at: String,
    pub ttl: i64,
}

pub async fn put_connection(
    dynamo: &Client,
    table: &str,
    connection_id: &str,
    user_id: &str,
) -> Result<(), WsError> {
    let conn = WsConnection {
        connection_id: connection_id.to_owned(),
        user_id: user_id.to_owned(),
        connected_at: now_rfc3339(),
        ttl: ttl_hours(24),
    };

    let item = to_item(conn).map_err(|e| WsError::Serialization(e.to_string()))?;

    dynamo
        .put_item()
        .table_name(table)
        .set_item(Some(item))
        .send()
        .await
        .map_err(|e| WsError::DynamoDB(e.to_string()))?;

    Ok(())
}
