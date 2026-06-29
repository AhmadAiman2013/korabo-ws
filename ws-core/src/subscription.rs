use crate::errors::WsError;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use serde::{Deserialize, Serialize};
use serde_dynamo::to_item;
use crate::utils::{now_rfc3339, ttl_hours};

#[derive(Serialize, Deserialize, Debug)]
pub struct ChatSubscription {
    pub group_id: String,
    pub connection_id: String,
    pub user_id: String,
    pub subscribed_at: String,
    pub ttl: i64,
}

pub async fn put_subscription(
    dynamo: &Client,
    table: &str,
    group_id: &str,
    connection_id: &str,
    user_id: &str,
) -> Result<(), WsError> {
    let sub = ChatSubscription {
        group_id: group_id.to_string(),
        connection_id: connection_id.to_string(),
        user_id: user_id.to_string(),
        subscribed_at: now_rfc3339(),
        ttl: ttl_hours(24),
    };
    
    let item = to_item(sub).map_err(|e| WsError::Serialization(e.to_string()))?;
    
    dynamo
        .put_item()
        .table_name(table)
        .set_item(Some(item))
        .send()
        .await
        .map_err(|e| WsError::DynamoDB(e.to_string()))?;
    
    Ok(())
}

pub async fn delete_subscription(
    dynamo: &Client,
    table: &str,
    group_id: &str,
    connection_id: &str,
) -> Result<(), WsError> {
    dynamo
        .delete_item()
        .table_name(table)
        .key("group_id", AttributeValue::S(group_id.to_string()))
        .key("connection_id", AttributeValue::S(connection_id.to_string()))
        .send()
    .await
        .map_err(|e| WsError::DynamoDB(e.to_string()))?;
    
    Ok(())
}

/// Delete all subscriptions for a closing connection.
/// Queries the connection_id-index GSI, then batch-deletes each row.
/// Called from ws-disconnect.
pub async fn delete_connection_subscriptions(
    dynamo: &Client,
    table: &str,
    connection_id: &str,
) -> Result<(), WsError> {
    let resp = dynamo
        .query()
        .table_name(table)
        .index_name("connection_id-index")
        .key_condition_expression("connection_id = :cid")
        .expression_attribute_values(":cid", AttributeValue::S(connection_id.to_string()))
        .projection_expression("group_id, connection_id")
        .send()
        .await
        .map_err(|e| WsError::DynamoDB(e.to_string()))?;

    for item in resp.items() {
        let gid = item
            .get("group_id")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();

        let cid = item
            .get("connection_id")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();

        if gid.is_empty() || cid.is_empty() {
            continue;
        }

        dynamo
            .delete_item()
            .table_name(table)
            .key("group_id", AttributeValue::S(gid))
            .key("connection_id", AttributeValue::S(cid))
            .send()
            .await
            .map_err(|e1| WsError::DynamoDB(e1.to_string()))?;
    }

    Ok(())
}
