use std::fmt::Display;

/// Produce a concise, single-line error message suitable for user-facing summaries.
pub fn concise_error<E: Display>(err: &E) -> String {
    let mut s = err.to_string();
    const LIM: usize = 300;
    if s.len() > LIM {
        s.truncate(LIM);
        s.push('…');
    }
    s
}
