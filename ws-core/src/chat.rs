use std::collections::HashMap;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use serde_dynamo::aws_sdk_dynamodb_1::from_item;
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

/// Fetch chat history for a group.
///
/// `since` is an RFC 3339 string — only messages with sort_key >= since are returned.
/// Pass `last_seen_at` from the `connected` push to get only missed messages.
pub async fn get_chat_history(
    dynamo: &Client,
    table: &str,
    group_id: &str,
    since: Option<String>,
) -> Result<Vec<ChatMessageRecord>, WsError> {
    // Simple query: return all messages for the group (optionally since a given time)
    let (key_cond, expr_values) = match &since {
        Some(s) => (
            "group_id = :gid AND sort_key >= :since".to_string(),
            vec![
                (":gid", AttributeValue::S(group_id.to_string())),
                (":since", AttributeValue::S(s.clone())),
            ],
        ),
        None => (
            "group_id = :gid".to_string(),
            vec![(":gid", AttributeValue::S(group_id.to_string()))],
        ),
    };

    let mut req = dynamo
        .query()
        .table_name(table)
        .key_condition_expression(key_cond)
        .scan_index_forward(true); // oldest → newest

    for (k, v) in expr_values {
        req = req.expression_attribute_values(k, v);
    }

    let resp = req
        .send()
        .await
        .map_err(|e| WsError::DynamoDB(e.to_string()))?;

    let records = resp
        .items()
        .iter()
        .filter_map(|item| from_item::<ChatMessageRecord>(item.clone()).ok())
        .collect();

    Ok(records)
}

/// Fetch a single page of chat history for a group.
///
/// `cursor` is an optional last-seen `sort_key` from a previous page. The query
/// will return messages with `sort_key > cursor`. `limit` controls the maximum
/// number of items returned (DynamoDB Query `Limit`). Returns the messages and
/// an optional next_cursor (the last returned message's `sort_key`) when more
/// items are available.
pub async fn get_chat_history_page(
    dynamo: &Client,
    table: &str,
    group_id: &str,
    cursor: Option<String>,
    limit: Option<i32>,
) -> Result<(Vec<ChatMessageRecord>, Option<String>), WsError> {
    // Build key condition expression depending on whether a cursor is present.
    let (key_cond, mut expr_values) = match &cursor {
        Some(c) => (
            "group_id = :gid AND sort_key > :cursor".to_string(),
            vec![
                (":gid", AttributeValue::S(group_id.to_string())),
                (":cursor", AttributeValue::S(c.clone())),
            ],
        ),
        None => (
            "group_id = :gid".to_string(),
            vec![(":gid", AttributeValue::S(group_id.to_string()))],
        ),
    };

    let mut req = dynamo
        .query()
        .table_name(table)
        .key_condition_expression(key_cond)
        .scan_index_forward(true); // oldest → newest

    for (k, v) in expr_values.drain(..) {
        req = req.expression_attribute_values(k, v);
    }

    if let Some(l) = limit {
        req = req.limit(l);
    }

    let resp = req
        .send()
        .await
        .map_err(|e| WsError::DynamoDB(e.to_string()))?;

    let records: Vec<ChatMessageRecord> = resp
        .items()
        .iter()
        .filter_map(|item| from_item::<ChatMessageRecord>(item.clone()).ok())
        .collect();

    // If DynamoDB returned a LastEvaluatedKey it means there are more items.
    // Use the last returned record's sort_key as an opaque cursor for the next page.
    let next_cursor = if resp.last_evaluated_key().is_some() {
        records.last().map(|r| r.sort_key.clone())
    } else {
        None
    };

    Ok((records, next_cursor))
}

