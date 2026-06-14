use aws_sdk_apigatewaymanagement::Client;

pub struct ManagementClient {
    inner: Client,
}

impl ManagementClient {
    pub fn new(client: Client) -> Self {
        Self { inner: client }
    }
}