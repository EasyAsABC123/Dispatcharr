//! JSON-compatible subset of the original Python msgpack wire encoding.
//! Explicit scalar encoding avoids serde_json arbitrary_precision's private
//! Number wrapper. Decode allocations are checked before trusting wire lengths.
use dispatcharr_contracts::UpdateEventError;
use serde_json::{Map, Number, Value};

const DEPTH: usize = 64;
const NODES: usize = 100_000;
type Result<T> = std::result::Result<T, UpdateEventError>;

pub fn encode(value: &Value, maximum: usize) -> Result<Vec<u8>> {
    let mut encoder = Encoder {
        data: Vec::new(),
        maximum,
        nodes: 0,
    };
    encoder.value(value, 0)?;
    Ok(encoder.data)
}
pub fn decode(data: &[u8], maximum: usize) -> Result<Value> {
    if data.len() > maximum {
        return Err(UpdateEventError::InvalidMessage);
    }
    let mut decoder = Decoder {
        data,
        offset: 0,
        nodes: 0,
    };
    let value = decoder.value(0)?;
    if decoder.offset != data.len() {
        return Err(UpdateEventError::InvalidMessage);
    }
    Ok(value)
}

struct Encoder {
    data: Vec<u8>,
    maximum: usize,
    nodes: usize,
}
impl Encoder {
    fn bytes(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > self.maximum.saturating_sub(self.data.len()) {
            return Err(UpdateEventError::InvalidMessage);
        }
        self.data.extend_from_slice(bytes);
        Ok(())
    }
    fn head(&mut self, marker: u8, bytes: &[u8]) -> Result<()> {
        self.bytes(&[marker])?;
        self.bytes(bytes)
    }
    fn unsigned(&mut self, value: u64) -> Result<()> {
        match value {
            0..=127 => self.bytes(&[value as u8]),
            128..=255 => self.head(0xcc, &[value as u8]),
            256..=65535 => self.head(0xcd, &(value as u16).to_be_bytes()),
            65536..=4294967295 => self.head(0xce, &(value as u32).to_be_bytes()),
            _ => self.head(0xcf, &value.to_be_bytes()),
        }
    }
    fn signed(&mut self, value: i64) -> Result<()> {
        match value {
            0.. => self.unsigned(value as u64),
            -32..=-1 => self.bytes(&[value as i8 as u8]),
            -128..=-33 => self.head(0xd0, &[value as i8 as u8]),
            -32768..=-129 => self.head(0xd1, &(value as i16).to_be_bytes()),
            -2147483648..=-32769 => self.head(0xd2, &(value as i32).to_be_bytes()),
            _ => self.head(0xd3, &value.to_be_bytes()),
        }
    }
    fn string(&mut self, value: &str) -> Result<()> {
        let length = value.len();
        if length <= 31 {
            self.bytes(&[0xa0 | length as u8])?;
        } else if length <= 255 {
            self.head(0xd9, &[length as u8])?;
        } else if length <= 65535 {
            self.head(0xda, &(length as u16).to_be_bytes())?;
        } else {
            self.head(
                0xdb,
                &u32::try_from(length)
                    .map_err(|_| UpdateEventError::InvalidMessage)?
                    .to_be_bytes(),
            )?;
        }
        self.bytes(value.as_bytes())
    }
    fn container(&mut self, length: usize, map: bool) -> Result<()> {
        let count = length
            .checked_mul(if map { 2 } else { 1 })
            .ok_or(UpdateEventError::InvalidMessage)?;
        if count > NODES.saturating_sub(self.nodes) {
            return Err(UpdateEventError::InvalidMessage);
        }
        if length <= 15 {
            self.bytes(&[(if map { 0x80 } else { 0x90 }) | length as u8])
        } else if length <= 65535 {
            self.head(
                if map { 0xde } else { 0xdc },
                &(length as u16).to_be_bytes(),
            )
        } else {
            self.head(
                if map { 0xdf } else { 0xdd },
                &u32::try_from(length)
                    .map_err(|_| UpdateEventError::InvalidMessage)?
                    .to_be_bytes(),
            )
        }
    }
    fn value(&mut self, value: &Value, depth: usize) -> Result<()> {
        self.nodes += 1;
        if depth > DEPTH || self.nodes > NODES {
            return Err(UpdateEventError::InvalidMessage);
        }
        match value {
            Value::Null => self.bytes(&[0xc0]),
            Value::Bool(value) => self.bytes(&[if *value { 0xc3 } else { 0xc2 }]),
            Value::Number(value) => {
                if let Some(value) = value.as_u64() {
                    return self.unsigned(value);
                }
                if let Some(value) = value.as_i64() {
                    return self.signed(value);
                }
                // Python msgpack refuses integers outside its64-bit range;
                // converting them to floats would silently change the payload.
                if !value.is_f64() {
                    return Err(UpdateEventError::InvalidMessage);
                }
                let value = value
                    .as_f64()
                    .filter(|value| value.is_finite())
                    .ok_or(UpdateEventError::InvalidMessage)?;
                self.head(0xcb, &value.to_be_bytes())
            }
            Value::String(value) => self.string(value),
            Value::Array(values) => {
                self.container(values.len(), false)?;
                for value in values {
                    self.value(value, depth + 1)?;
                }
                Ok(())
            }
            Value::Object(values) => {
                self.container(values.len(), true)?;
                for (key, value) in values {
                    self.nodes += 1;
                    self.string(key)?;
                    self.value(value, depth + 1)?;
                }
                Ok(())
            }
        }
    }
}

struct Decoder<'a> {
    data: &'a [u8],
    offset: usize,
    nodes: usize,
}
impl Decoder<'_> {
    fn take(&mut self, length: usize) -> Result<&[u8]> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.data.len())
            .ok_or(UpdateEventError::InvalidMessage)?;
        let result = &self.data[self.offset..end];
        self.offset = end;
        Ok(result)
    }
    fn number<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| UpdateEventError::InvalidMessage)
    }
    fn string(&mut self, length: usize) -> Result<Value> {
        let text = std::str::from_utf8(self.take(length)?)
            .map_err(|_| UpdateEventError::InvalidMessage)?;
        Ok(Value::String(text.to_owned()))
    }
    fn container(&mut self, length: usize, map: bool, depth: usize) -> Result<Value> {
        let count = length
            .checked_mul(if map { 2 } else { 1 })
            .ok_or(UpdateEventError::InvalidMessage)?;
        if count > NODES.saturating_sub(self.nodes) || count > self.data.len() - self.offset {
            return Err(UpdateEventError::InvalidMessage);
        }
        if map {
            let mut values = Map::new();
            for _ in 0..length {
                let Value::String(key) = self.value(depth + 1)? else {
                    return Err(UpdateEventError::InvalidMessage);
                };
                values.insert(key, self.value(depth + 1)?);
            }
            Ok(Value::Object(values))
        } else {
            let mut values = Vec::with_capacity(length);
            for _ in 0..length {
                values.push(self.value(depth + 1)?);
            }
            Ok(Value::Array(values))
        }
    }
    fn float(value: f64) -> Result<Value> {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or(UpdateEventError::InvalidMessage)
    }
    fn value(&mut self, depth: usize) -> Result<Value> {
        self.nodes += 1;
        if depth > DEPTH || self.nodes > NODES {
            return Err(UpdateEventError::InvalidMessage);
        }
        let marker = self.take(1)?[0];
        Ok(match marker {
            0x00..=0x7f => Value::from(marker),
            0xe0..=0xff => Value::from(marker as i8),
            0xa0..=0xbf => return self.string((marker & 31) as usize),
            0x90..=0x9f => return self.container((marker & 15) as usize, false, depth),
            0x80..=0x8f => return self.container((marker & 15) as usize, true, depth),
            0xc0 => Value::Null,
            0xc2 => Value::Bool(false),
            0xc3 => Value::Bool(true),
            0xcc => Value::from(self.take(1)?[0]),
            0xcd => Value::from(u16::from_be_bytes(self.number()?)),
            0xce => Value::from(u32::from_be_bytes(self.number()?)),
            0xcf => Value::from(u64::from_be_bytes(self.number()?)),
            0xd0 => Value::from(self.take(1)?[0] as i8),
            0xd1 => Value::from(i16::from_be_bytes(self.number()?)),
            0xd2 => Value::from(i32::from_be_bytes(self.number()?)),
            0xd3 => Value::from(i64::from_be_bytes(self.number()?)),
            0xca => return Self::float(f32::from_be_bytes(self.number()?) as f64),
            0xcb => return Self::float(f64::from_be_bytes(self.number()?)),
            0xd9 => {
                let length = self.take(1)?[0] as usize;
                return self.string(length);
            }
            0xda => {
                let length = u16::from_be_bytes(self.number()?) as usize;
                return self.string(length);
            }
            0xdb => {
                let length = u32::from_be_bytes(self.number()?) as usize;
                return self.string(length);
            }
            0xdc | 0xde => {
                let length = u16::from_be_bytes(self.number()?) as usize;
                return self.container(length, marker == 0xde, depth);
            }
            0xdd | 0xdf => {
                let length = u32::from_be_bytes(self.number()?) as usize;
                return self.container(length, marker == 0xdf, depth);
            }
            _ => return Err(UpdateEventError::InvalidMessage),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn scalar_wire_values_do_not_use_json_number_wrapper() {
        for (value, bytes) in [
            (json!(128), vec![0xcc, 128]),
            (json!(-33), vec![0xd0, 223]),
            (json!(65536), vec![0xce, 0, 1, 0, 0]),
            (json!(1.5), vec![0xcb, 63, 248, 0, 0, 0, 0, 0, 0]),
        ] {
            assert_eq!(encode(&value, 1024).unwrap(), bytes);
            assert_eq!(decode(&bytes, 1024).unwrap(), value);
        }
        let envelope = json!({"type":"update","data":{"type":"channel_stats","success":true,"stats":"{}"},"__asgi_channel__":["specific.fixture!one"]});
        assert_eq!(
            decode(&encode(&envelope, 4096).unwrap(), 4096).unwrap(),
            envelope
        );
    }
    #[test]
    fn hostile_lengths_depth_and_trailing_data_are_bounded_before_allocation() {
        for bytes in [
            vec![0xdd, 255, 255, 255, 255],
            vec![0xdf, 255, 255, 255, 255],
            vec![0xdb, 255, 255, 255, 255],
            vec![0xc0, 0xc0],
            vec![0xa1, 255],
            vec![0x91; 66],
        ] {
            assert_eq!(decode(&bytes, 4096), Err(UpdateEventError::InvalidMessage));
        }
        assert!(encode(&json!("a".repeat(100)), 16).is_err());
        assert!(encode(
            &serde_json::from_str::<Value>("18446744073709551616").unwrap(),
            1024
        )
        .is_err());
    }
}
