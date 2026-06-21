use crate::errors::WsError;
use crate::types::NotificationRecord;
use crate::utils::now_rfc3339;
use aws_sdk_dynamodb::types::{AttributeValue, Select};
use aws_sdk_dynamodb::Client;
use serde_dynamo::to_item;
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

pub async fn get_unread_count(dynamo: &Client, table: &str, user_id: &str) -> Result<i64, WsError> {
    let resp = dynamo
        .query()
        .table_name(table)
        .index_name("unread-index")
        .key_condition_expression("unread_user_id = :uid")
        .expression_attribute_values(":uid", AttributeValue::S(user_id.to_owned()))
        .select(Select::Count)
        .send()
        .await
        .map_err(|e| WsError::DynamoDB(e.to_string()))?;

    Ok(resp.count() as i64)
}

/// Write a new notification and return the record so the caller can push it
/// over WebSocket immediately.
///
/// The `unread_user_id` sparse GSI attribute is set here and REMOVED by
/// `mark_notifications_read`, which causes the item to drop out of the
/// unread-index automatically.
pub async fn put_notification(
    dynamo: &Client,
    table: &str,
    user_id: &str,
    notification_type: &str,
    actor_id: &str,
    payload: Value,
) -> Result<NotificationRecord, WsError> {
    let created_at = now_rfc3339();
    let notification_id = Uuid::new_v4().to_string();

    let sort_key = format!("{}#{}", created_at, notification_id);

    let record = NotificationRecord {
        user_id: user_id.to_string(),
        sort_key: sort_key.clone(),
        notification_id: sort_key.clone(),
        notification_type: notification_type.to_string(),
        actor_id: actor_id.to_string(),
        payload,
        is_read: false,
        read_at: None,
        created_at,
    };

    let mut item: HashMap<String, AttributeValue> =
        to_item(&record).map_err(|e| WsError::Serialization(e.to_string()))?;

    // Sparse GSI attribute — present only when the notification is unread.
    // DynamoDB will include this item in unread-index as long as this attribute exists.
    item.insert(
        "unread_user_id".to_string(),
        AttributeValue::S(user_id.to_string()),
    );

    dynamo
        .put_item()
        .table_name(table)
        .set_item(Some(item))
        .send()
        .await
        .map_err(|e| WsError::DynamoDB(e.to_string()))?;

    Ok(record)
}

/// Mark a batch of notifications as read.
///
/// `notification_ids` are the sort_key values returned in pushes and history
/// (they are equal to the `notification_id` field on `NotificationRecord`).
///
/// The UPDATE removes `unread_user_id`, which drops the item from the sparse
/// unread-index GSI so it no longer appears in unread queries.
pub async fn mark_notifications_read(
    dynamo: &Client,
    table: &str,
    user_id: &str,
    notification_id: &[String],
) -> Result<(), WsError> {
    let read_at = now_rfc3339();

    for notification_id in notification_id {
        dynamo
            .update_item()
            .table_name(table)
            .key("user_id", AttributeValue::S(user_id.to_string()))
            .key("sort_key", AttributeValue::S(notification_id.to_owned()))
            .update_expression("SET is_read = :r, read_at = :t REMOVE unread_user_id")
            .expression_attribute_values(":r", AttributeValue::Bool(true))
            .expression_attribute_values(":t", AttributeValue::S(read_at.to_owned()))
            .condition_expression("attribute_exists(sort_key)")
            .send()
            .await
            .map_err(|e| WsError::DynamoDB(e.to_string()))?;
    }

    Ok(())
}
