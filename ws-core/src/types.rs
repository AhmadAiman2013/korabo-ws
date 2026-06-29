use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Debug)]
#[serde(tag = "action")]
pub enum ClientMessage {

    #[serde(rename = "chat.join")]
    ChatJoin { group_id: String },

    #[serde(rename = "chat.leave")]
    ChatLeave { group_id: String },
}

#[derive(Serialize, Debug, Clone)]
pub enum ServerPush {
    
    #[serde(rename = "connected")]
    Connected {
        last_seen_at: Option<String>,
        unread_notification_count: i64,
    },

    #[serde(rename = "chat.message")]
    ChatMessage {
        group_id: String,
        message_id: String,
        sender_id: String,
        content: String,
        created_at: String,
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

    #[serde(rename = "ack")]
    Ack { action: String },
    
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

#[derive(Serialize, Deserialize, Debug)]
pub struct NotificationTargeting {
    pub user_ids: Vec<String>,
    pub group_id: Option<String>,
    pub exclude_user_ids: Option<Vec<String>>,
}

