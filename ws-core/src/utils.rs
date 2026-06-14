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