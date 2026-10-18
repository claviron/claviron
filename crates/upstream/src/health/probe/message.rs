//! Streaming, case-insensitive legacy library diagnostic recognition.
//! Scan arbitrarily long/chunked Display output without truncating or allocating.
use std::fmt::{self, Write};
// Fixed diagnostic phrase and its KMP prefix table; neither is a timer/limit.
const EARLY_EOF: &[u8] = b"early eof";
const PREFIX: [usize; 9] = [0, 0, 0, 0, 0, 0, 1, 0, 0];
#[derive(Default)]
struct Matcher {
    matched: usize,
    found: bool,
}
impl Write for Matcher {
    fn write_str(
        &mut self,
        value: &str,
    ) -> fmt::Result {
        for byte in value.bytes().map(|byte| byte.to_ascii_lowercase()) {
            if self.found {
                break;
            }
            while self.matched != 0 && EARLY_EOF[self.matched] != byte {
                self.matched = PREFIX[self.matched - 1];
            }
            if EARLY_EOF[self.matched] == byte {
                self.matched += 1;
            }
            if self.matched == EARLY_EOF.len() {
                self.found = true;
            }
        }
        Ok(())
    }
}
pub(super) fn has_early_eof(value: &impl fmt::Display) -> bool {
    let mut matcher = Matcher::default();
    let _ = write!(&mut matcher, "{value}");
    matcher.found
}
