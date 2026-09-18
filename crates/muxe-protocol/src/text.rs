//! Small UTF-8-safe text utilities for bounded protocol diagnostics.

/// Truncates `text` in place to at most `cap` bytes without splitting a UTF-8 character.
///
/// This does not allocate. When truncation is necessary, the retained text may be shorter than
/// `cap` so that it ends on a UTF-8 character boundary.
pub fn truncate_utf8(text: &mut String, cap: usize) {
    if text.len() <= cap {
        return;
    }

    let mut end = cap;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
}

#[cfg(test)]
mod tests {
    use super::truncate_utf8;

    #[test]
    fn truncates_at_a_character_boundary_without_allocating_a_copy() {
        let mut text = "abécd".to_owned();
        truncate_utf8(&mut text, 3);
        assert_eq!(text, "ab");

        truncate_utf8(&mut text, 8);
        assert_eq!(text, "ab");
    }
}
