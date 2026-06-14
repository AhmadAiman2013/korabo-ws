use crate::errors::WsError;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;

pub async fn get_last_seen(
    dynamo: &Client,
    table: &str,
    user_id: &str,
) -> Result<Option<String>, WsError> {
    let resp = dynamo
        .get_item()
        .table_name(table)
        .key("user_id", AttributeValue::S(user_id.to_string()))
        .projection_expression("last_seen_at")
        .send()
        .await
        .map_err(|e| WsError::DynamoDB(e.to_string()))?;

    Ok(resp
        .item()
        .and_then(|item| item.get("last_seen_at"))
        .and_then(|v| v.as_s().ok())
        .cloned())
}
