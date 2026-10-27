use super::*;

#[test]
fn cell_revision_preserves_scalar_counts_and_full_width_text_waits() {
    let cells = Cells::new(Arc::new(
        claviron_orbit::Fleet::join("revision", 1).unwrap(),
    ))
    .unwrap();
    let scalar = cells.allocate(7_i64).unwrap();
    let handle = Cell::Int(scalar.clone());
    let before = handle.version().unwrap();
    scalar.store(8).unwrap();
    assert_eq!(handle.wait_changed(before).unwrap(), 1);
    let text = cells.allocate_text("before").unwrap();
    let handle = Cell::Text(text.clone());
    let before = handle.version().unwrap();
    text.store("after").unwrap();
    assert_eq!(handle.wait_changed(before).unwrap(), text.version().unwrap());
    // A full-width opaque token is valid for text, including its high bit.
    assert_eq!(
        handle.wait_changed(1_u64 << 63).unwrap(),
        text.version().unwrap()
    );
}
