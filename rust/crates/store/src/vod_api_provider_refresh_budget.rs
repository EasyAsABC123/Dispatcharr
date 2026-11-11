//! Private count-only SQL encoding admission. This is a subdivision of the
//! caller's existing work reservation, not another memory pool or allowance.
//! Original Dispatcharr authors retain application and algorithm credit.
use super::vod_api_owned_read::ReadError;
use serde_json::Value;
pub(super) fn encoded_bytes(value: &Value, maximum: usize) -> Result<usize, ReadError> {
    struct Count {
        used: usize,
        maximum: usize,
    }
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.used = self
                .used
                .checked_add(bytes.len())
                .filter(|n| *n <= self.maximum)
                .ok_or_else(|| std::io::Error::other("refresh SQL JSON argument capacity"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Count { used: 0, maximum };
    serde_json::to_writer(&mut counter, value).map_err(|_| ReadError::Capacity)?;
    Ok(counter.used)
}
