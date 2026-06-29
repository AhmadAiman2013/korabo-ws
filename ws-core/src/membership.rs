use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use lambda_http::tracing::error;
use crate::connection::{get_connection, WsConnection};
use crate::errors::WsError;
use crate::management::ManagementClient;
use crate::types::ServerPush;

pub fn group_pk(group_id: &str) -> AttributeValue {
    AttributeValue::S(format!("GROUP#{group_id}"))
}

pub fn member_sk(user_id: &str) -> AttributeValue {
    AttributeValue::S(format!("MEMBER#{user_id}"))
}

async fn assert_group_member(
    dynamo: &Client,
    members_table: &str,
    group_id: &str,
    user_id: &str,
) -> Result<(), WsError> {
    let resp = dynamo
        .get_item()
        .table_name(members_table)
        .key("PK", group_pk(group_id))
        .key("SK", member_sk(user_id))
        .projection_expression("#s")
        .expression_attribute_names("#s", "status")
        .consistent_read(false)
        .send()
        .await
        .map_err(|e| WsError::DynamoDB(e.to_string()))?;
    
    match resp.item { 
        Some(item) => {
            let status = item
                .get("status")
                .and_then(|v| v.as_s().ok())
                .map(|s| s.as_str());
            
            if status == Some("active") {
                Ok(())
            } else { 
                Err(WsError::Forbidden(format!(
                    "user {} is not an active member of group {}",
                    user_id, group_id
                )))
            }
        }
        None => Err(WsError::Forbidden(format!(
            "user {} is not a member of group {}",
            user_id, group_id
        ))),
    }
}


pub async fn require_connection_and_membership(
    dynamo: &Client,
    connections_table: &str,
    members_table: &str,
    apigw: &ManagementClient,
    connection_id: &str,
    group_id: &str,
) -> Result<WsConnection, ()> {
    let conn = match get_connection(dynamo, connections_table, connection_id).await {
        Ok(c) => c,
        Err(e) => {
            error!("Connection record not found: {}, error: {}", connection_id, e);
            return Err(());
        }
    };
    
    // Membership check
    if let Err(e) = assert_group_member(dynamo, members_table, group_id, &conn.user_id).await {
        let _ = apigw
            .post_to_connection(
                connection_id,
                &ServerPush::Error {
                    code: "FORBIDDEN".into(),
                    message: e.to_string(),
                },
            )
            .await;
        return Err(());
    }
    
    Ok(conn)
}