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

    let whole = decode(&encode(ChannelId(4), &record, at, 4096).unwrap())
        .expect("decodes");
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
    let cut = decode(&encode(ChannelId(4), &record, at, tight).unwrap())
        .expect("decodes");
    assert_eq!(cut.message, "settled ");
    assert!(cut.fields.is_empty());
    assert!(cut.truncated);
}

#[test]
fn audit_logging_encoder_budgets_targets_and_the_empty_envelope() {
    for target in ["x".repeat(65_535), "🦀".repeat(20_000)] {
        for capacity in [0, 20, 21, 22, 24, 62, 126, 65_472] {
            let record = Record {
                level: Level::Error,
                target: &target,
                message: "body ✓",
                fields: &[],
            };
            let encoded =
                encode(ChannelId(1), &record, SystemTime::UNIX_EPOCH, capacity);
            if capacity < 21 {
                assert!(encoded.is_none());
                continue;
            }
            let encoded = encoded.unwrap();
            assert!(encoded.len() <= capacity);
            let decoded = decode(&encoded).unwrap();
            assert!(target.starts_with(&decoded.target));
            assert!(record.message.starts_with(&decoded.message));
            assert!(decoded.truncated);
            // The default geometry can never need a 33rd part.
            if capacity == 65_472 {
                assert!(encoded.len().div_ceil(2046) <= 32);
            }
        }
    }
    let empty =
        Record { level: Level::Info, target: "", message: "", fields: &[] };
    let encoded =
        encode(ChannelId(1), &empty, SystemTime::UNIX_EPOCH, 21).unwrap();
    assert_eq!(encoded.len(), 21);
    assert!(!decode(&encoded).unwrap().truncated);
}
