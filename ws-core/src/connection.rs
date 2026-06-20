use crate::errors::WsError;
use crate::utils::{now_rfc3339, return_collection_ids, ttl_hours};
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use serde::{Deserialize, Serialize};
use serde_dynamo::{from_item, to_item};

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

pub async fn get_connection(
    dynamo: &Client,
    table: &str,
    connection_id: &str,
) -> Result<WsConnection, WsError> {
    let resp = dynamo
        .get_item()
        .table_name(table)
        .key(
            "connection_id",
            AttributeValue::S(connection_id.to_string()),
        )
        .send()
        .await
        .map_err(|e| WsError::DynamoDB(e.to_string()))?;

    resp.item()
        .map(|item| from_item(item.clone()).map_err(|e| WsError::Serialization(e.to_string())))
        .unwrap_or_else(|| {
            Err(WsError::NotFound(format!(
                "connection {} not found",
                connection_id
            )))
        })
}

pub async fn delete_connection(
    dynamo: &Client,
    table: &str,
    connection_id: &str,
) -> Result<(), WsError> {
    dynamo
        .delete_item()
        .table_name(table)
        .key(
            "connection_id",
            AttributeValue::S(connection_id.to_string()),
        )
        .send()
        .await
        .map_err(|e| WsError::DynamoDB(e.to_string()))?;

    Ok(())
}

pub async fn get_user_connections(
    dynamo: &Client,
    table: &str,
    user_id: &str,
) -> Result<Vec<String>, WsError> {
    let resp = dynamo
        .query()
        .table_name(table)
        .index_name("user_id-index")
        .key_condition_expression("user_id = :uid")
        .expression_attribute_values(":uid", AttributeValue::S(user_id.to_string()))
        .projection_expression("connection_id")
        .send()
        .await
        .map_err(|e| WsError::DynamoDB(e.to_string()))?;

    let ids = return_collection_ids(resp);

    Ok(ids)
}
