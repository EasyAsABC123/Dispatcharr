use super::*;

fn packet(seed: u8, pid: u16) -> Vec<u8> {
    let mut value = vec![seed % 251; 188];
    value[..6].copy_from_slice(&[0x47, (pid >> 8) as u8, pid as u8, 0x30 | (seed % 16), 1, 0]);
    value
}

#[test]
fn source_pending_pid_is_stamped_before_first_chunk_disarms_marker() {
    let mut input = InputAccumulator::new(376);
    input.marker.arm();
    input.mark_chunk = true;
    let frame = [packet(5, 256), packet(6, 256), packet(7, 258)].concat();
    let mut published = Vec::new();
    for window in frame.chunks(376) {
        if let Some((data, _)) = assemble_input_window(
            &mut input.assembler,
            &mut input.marker,
            &mut input.mark_chunk,
            &mut input.chunk,
            window,
            376,
        ) {
            published.push(data);
        }
    }
    assert_eq!(published.len(), 1);
    assert_eq!(published[0][5], 128);
    assert_eq!(published[0][193], 0);
    assert_eq!(input.chunk.len(), 188);
    // Actual Original second mark flushes PID258 with discontinuity set.
    let captured: serde_json::Value =
        serde_json::from_str(include_str!("../datasets/source-core-mark-3.json")).unwrap();
    let row = captured["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["label"] == "second_mark_flushes_previously_stamped_pending_PID")
        .unwrap();
    assert_eq!(
        row["chunks"][3]["hex"].as_str().unwrap(),
        input
            .chunk
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    assert_eq!(
        input.chunk[5], 128,
        "pending PID from the same upstream input call must retain Source stamping"
    );
}
