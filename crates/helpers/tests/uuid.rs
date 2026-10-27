use std::cell::Cell;

use serde::de::value::{Error, SeqDeserializer};

#[test]
fn audit_uuid_sequences_reject_at_the_first_excess_element() {
    for length in [0, 15, 16, 17, 65_536] {
        let consumed = Cell::new(0);
        let elements = (0..length).map(|_| {
            consumed.set(consumed.get() + 1);
            42u8
        });
        let input = SeqDeserializer::<_, Error>::new(elements);
        let result = super::deserialize(input);
        assert_eq!(result.is_ok(), length == 16);
        assert_eq!(consumed.get(), length.min(17));
        if length == 16 {
            assert_eq!(result.unwrap().as_bytes(), &[42u8; 16]);
        }
    }
}

#[test]
fn audit_uuid_binary_form_preserves_exact_length() {
    for length in [0, 15, 16, 17] {
        let input = vec![42u8; length];
        let result =
            super::deserialize(serde::de::value::BorrowedBytesDeserializer::<
                Error,
            >::new(&input));
        assert_eq!(result.is_ok(), length == 16);
    }
}
