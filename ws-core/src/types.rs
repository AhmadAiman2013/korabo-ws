use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Debug, Clone)]
pub enum ServerPush {
    
    #[serde(rename = "connected")]
    Connected {
        last_seen_at: Option<String>,
        unread_notification_count: i64,
    },

    /// A real-time notification pushed after SQS consumption.
    #[serde(rename = "notification")]
    Notification {
        /// Equals the DynamoDB sort_key — use this in `notif.read`.
        notification_id: String,
        notification_type: String,
        actor_id: String,
        payload: Value,
        created_at: String,
    },
    
    #[serde(rename = "error")]
    Error { code: String, message: String }
}

// DynamoDB
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NotificationRecord {
    pub user_id: String,
    /// `{rfc3339}#{uuid}` — used as the client-facing notification_id for mark-read.
    pub sort_key: String,
    pub notification_id: String,
    pub notification_type: String,
    pub actor_id: String,
    pub payload: Value,
    pub is_read: bool,
    pub read_at: Option<String>,
    pub created_at: String,
}

// ── SQS event published by other modules ─────────────────────────────────────
#[derive(Serialize, Deserialize, Debug)]
pub struct SqsNotificationEvent {
    pub event_id: String,
    pub event_type: String,
    pub actor_id: String,
    pub targeting: NotificationTargeting,
    pub payload: Value,
    pub created_at: String,
}

#[derive(Deserialize, Debug)]
pub struct NotificationTargeting {
    pub user_ids: Vec<String>,
    pub group_id: Option<String>,
    pub exclude_user_ids: Option<Vec<String>>,
}

