use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub enum ServerPush {
    
    #[serde(rename = "connected")]
    Connected {
        last_seen_at: Option<String>,
        unread_notification_count: i64,
    },
    
    #[serde(rename = "error")]
    Error { code: String, message: String }
}