use crate::errors::WsError;
use aws_sdk_dynamodb::types::{AttributeValue, Select};
use aws_sdk_dynamodb::Client;

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
