use aws_sdk_dynamodb::operation::query::QueryOutput;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

pub fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| OffsetDateTime::now_utc().to_string())
}

pub fn ttl_hours(hours: i64) -> i64 {
    (OffsetDateTime::now_utc() + time::Duration::hours(hours)).unix_timestamp()
}

pub fn return_collection_ids(response: QueryOutput) -> Vec<String> {
    response
        .items()
        .iter()
        .filter_map(|item| {
            item.get("connection_id")
                .and_then(|v| v.as_s().ok())
                .cloned()
        })
        .collect()
}
