use std::time::{Duration, SystemTime};

use super::*;

/// Version, flags, channel, level, time.
const HEADER_BYTES: usize = 1 + 1 + 2 + 1 + 8;
const AT: Duration = Duration::from_nanos(1_790_000_000_123_456_789);

#[test]
fn round_trips_and_truncates_to_the_frame() {
    let fields = [
        ("status", Value::U64(200)),
        ("path", Value::Str("/orders/7")),
        ("delta", Value::I64(-3)),
        ("ratio", Value::F64(0.5)),
        ("cached", Value::Bool(true)),
    ];
    let record = Record {
        level: Level::Warn,
        target: "App\\Jobs\\SettleRound::handle",
        message: "settled ✓",
        fields: &fields,
    };
    let at = SystemTime::UNIX_EPOCH + AT;

    let whole =
        decode(&encode(ChannelId(4), &record, at, 4096)).expect("decodes");
    assert_eq!(whole.channel, ChannelId(4));
    assert_eq!(whole.level, Level::Warn);
    assert_eq!(whole.time, AT.as_nanos() as i64);
    assert_eq!(whole.message, "settled ✓");
    assert_eq!(whole.fields.len(), 5);
    assert_eq!(whole.fields[0], ("status".to_owned(), OwnedValue::U64(200)));
    assert!(!whole.truncated);

    // A frame that holds the header, the target and part of the message:
    // cut on a char boundary, no fields, marked.
    let tight = HEADER_BYTES + 2 + record.target.len() + 4 + 2 + 9;
    let cut =
        decode(&encode(ChannelId(4), &record, at, tight)).expect("decodes");
    assert_eq!(cut.message, "settled ");
    assert!(cut.fields.is_empty());
    assert!(cut.truncated);
}
