//! Original raw-input five-second byte reporting rule; upstream credit retained.
use std::sync::atomic::{AtomicU64, Ordering};
pub(crate) fn report(total: u64, now: f64, last: &AtomicU64, reported: &AtomicU64) -> bool {
    let previous = f64::from_bits(last.load(Ordering::Relaxed));
    if now - previous >= 5.0 {
        reported.store(total, Ordering::Release);
        last.store(now.to_bits(), Ordering::Relaxed);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_raw_input_counter_clock_vectors() {
        let data: serde_json::Value =
            serde_json::from_str(include_str!("../datasets/source-byte-reporting.json")).unwrap();
        assert!(
            data["read_count"].as_u64().unwrap() >= 3200,
            "actual Source oracle must be captured"
        );
        for case in data["cases"].as_array().unwrap() {
            let last = AtomicU64::new(case["start"].as_f64().unwrap().to_bits());
            let reported = AtomicU64::new(0);
            let mut total = 0;
            for read in case["reads"].as_array().unwrap() {
                total += read["size"].as_u64().unwrap();
                report(total, read["at"].as_f64().unwrap(), &last, &reported);
                let value = reported.load(Ordering::Acquire);
                assert_eq!(value, read["reported"].as_u64().unwrap_or(0), "{read}");
                assert_eq!(total - value, read["pending"].as_u64().unwrap(), "{read}");
                assert_eq!(
                    f64::from_bits(last.load(Ordering::Relaxed)),
                    read["last"].as_f64().unwrap(),
                    "{read}"
                );
            }
        }
    }
}
