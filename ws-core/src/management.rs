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
                other => WsError::ManagementApi(format!("{other:?}")),
            })?;

        Ok(())
    }
}
