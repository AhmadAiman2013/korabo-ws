use std::collections::HashMap;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use serde_dynamo::to_item;
use uuid::Uuid;
use crate::errors::WsError;
use crate::types::ChatMessageRecord;
use crate::utils::now_rfc3339;

pub async fn put_chat_message(
    dynamo: &Client,
    table: &str,
    group_id: &str,
    sender_id: &str,
    content: &str,
) -> Result<ChatMessageRecord, WsError> {
    let created_at = now_rfc3339();
    let message_id = Uuid::new_v4().to_string();
    // RFC3339 prefix keeps messages in chronological order under the same group_id PK.
    let sort_key = format!("{}#{}", created_at, message_id);

    let record = ChatMessageRecord {
        group_id: group_id.to_string(),
        sort_key: sort_key.clone(),
        message_id: message_id.clone(),
        sender_id: sender_id.to_string(),
        content: content.to_string(),
        message_type: "TEXT".to_string(),
        created_at,
    };

    let item: HashMap<String, AttributeValue> =
        to_item(&record).map_err(|e| WsError::Serialization(e.to_string()))?;

    dynamo
        .put_item()
        .table_name(table)
        .set_item(Some(item))
        .send()
        .await
        .map_err(|e| WsError::DynamoDB(e.to_string()))?;

    Ok(record)
}