use crate::errors::WsError;
use crate::types::ServerPush;
use aws_sdk_apigatewaymanagement::operation::post_to_connection::PostToConnectionError;
use aws_sdk_apigatewaymanagement::primitives::Blob;
use aws_sdk_apigatewaymanagement::Client;

pub struct ManagementClient {
    inner: Client,
}

impl ManagementClient {
    pub fn new(client: Client) -> Self {
        Self { inner: client }
    }

    /// Push a message to a specific WebSocket connection.
    /// Returns `WsError::ConnectionGone` (410) when the connection no longer exists —
    pub async fn post_to_connection(
        &self,
        connection_id: &str,
        push: &ServerPush,
    ) -> Result<(), WsError> {
        let body = serde_json::to_vec(push).map_err(|e| WsError::Serialization(e.to_string()))?;

        self.inner
            .post_to_connection()
            .connection_id(connection_id)
            .data(Blob::new(body))
            .send()
            .await
            .map_err(|e1| match e1.into_service_error() {
                PostToConnectionError::GoneException(_) => {
                    WsError::ConnectionGone(connection_id.to_string())
                }
                other => WsError::ManagementApi(other.to_string()),
            })?;

        Ok(())
    }

    /// Push to a connection and silently discard Gone errors (stale connection_ids).
    /// Returns `true` if the push succeeded, `false` if the connection was gone.
    /// Other errors are logged and treated as transient failures (also returns false).
    pub async fn push_or_ignore_gone(
        &self,
        connection_id: &str,
        push: &ServerPush,
    ) -> bool {
        match self.post_to_connection(connection_id, push) .await {
            Ok(_) => true,
            Err(WsError::ConnectionGone(_)) => {
                false
            }
            Err(_) => {
                false
            }
        }
    }
}
