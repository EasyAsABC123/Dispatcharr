//! Restricted Django cache pickle codec for JSON primitives only.
//! No imports, classes, reducers, extensions or persistent references are run.
//! Original Dispatcharr authors retain application credit (ATTRIBUTION.md).
use num_bigint::BigInt;
use serde_json::{Map, Number, Value};
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashSet},
    rc::Rc,
};
const MAX_BYTES: usize = 1024 * 1024;
const MAX_NODES: usize = 16384;
const MAX_DEPTH: usize = 64;
#[derive(Clone, Copy)]
struct Limits {
    bytes: usize,
    nodes: usize,
    depth: usize,
}
const DEFAULT_LIMITS: Limits = Limits {
    bytes: MAX_BYTES,
    nodes: MAX_NODES,
    depth: MAX_DEPTH,
};
type Node = Rc<RefCell<Primitive>>;
enum Primitive {
    Scalar(Value),
    List(Vec<Node>),
    Dict(BTreeMap<String, Node>),
}
enum Item {
    Mark,
    Node(Node),
}
struct Reader<'a> {
    data: &'a [u8],
    limits: Limits,
    at: usize,
    stack: Vec<Item>,
    memo: Vec<Option<Node>>,
    nodes: usize,
    work: usize,
}
impl<'a> Reader<'a> {
    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(n)?;
        let data = self.data.get(self.at..end)?;
        self.at = end;
        Some(data)
    }
    fn byte(&mut self) -> Option<u8> {
        Some(self.bytes(1)?[0])
    }
    fn length(&mut self, n: usize) -> Option<usize> {
        let mut bytes = [0u8; 8];
        bytes[..n].copy_from_slice(self.bytes(n)?);
        usize::try_from(u64::from_le_bytes(bytes))
            .ok()
            .filter(|n| *n <= self.limits.bytes)
    }
    fn top(&self) -> Option<Node> {
        match self.stack.last()? {
            Item::Node(node) => Some(node.clone()),
            _ => None,
        }
    }
    fn push(&mut self, value: Primitive) -> Option<()> {
        self.nodes += 1;
        if self.nodes > self.limits.nodes {
            return None;
        }
        self.stack.push(Item::Node(Rc::new(RefCell::new(value))));
        Some(())
    }
    fn marked(&mut self) -> Option<Vec<Node>> {
        let mark = self
            .stack
            .iter()
            .rposition(|value| matches!(value, Item::Mark))?;
        let rest = self.stack.split_off(mark);
        rest.into_iter()
            .skip(1)
            .map(|item| match item {
                Item::Node(node) => Some(node),
                _ => None,
            })
            .collect()
    }
    fn dict_items(&mut self, items: Vec<Node>) -> Option<()> {
        if !items.len().is_multiple_of(2) {
            return None;
        }
        let top = self.top()?;
        for pair in items.chunks_exact(2) {
            if reaches(&pair[1], &top, &mut self.work) {
                return None;
            }
        }
        let additions = items
            .chunks_exact(2)
            .map(|pair| {
                let Primitive::Scalar(Value::String(key)) = &*pair[0].borrow() else {
                    return None;
                };
                Some((key.clone(), pair[1].clone()))
            })
            .collect::<Option<Vec<_>>>()?;
        let mut target = top.borrow_mut();
        let Primitive::Dict(entries) = &mut *target else {
            return None;
        };
        for (key, value) in additions {
            entries.insert(key, value);
        }
        Some(())
    }
    fn list_items(&mut self, items: Vec<Node>) -> Option<()> {
        let top = self.top()?;
        for item in &items {
            if reaches(item, &top, &mut self.work) {
                return None;
            }
        }
        let mut target = top.borrow_mut();
        let Primitive::List(entries) = &mut *target else {
            return None;
        };
        entries.extend(items);
        Some(())
    }
    fn memo(&mut self, index: usize) -> Option<()> {
        if index >= self.limits.nodes {
            return None;
        }
        let top = self.top()?;
        self.memo
            .resize_with(self.memo.len().max(index + 1), || None);
        self.memo[index] = Some(top);
        Some(())
    }
    fn read(mut self) -> Option<Value> {
        for _ in 0..65536 {
            if self.stack.len() > self.limits.nodes {
                return None;
            }
            match self.byte()? {
                0x80 => {
                    if ![4, 5].contains(&self.byte()?) {
                        return None;
                    }
                }
                0x95 => {
                    let length = self.length(8)?;
                    if length > self.data.len().checked_sub(self.at)? {
                        return None;
                    }
                }
                b'.' => {
                    if self.at != self.data.len() || self.stack.len() != 1 {
                        return None;
                    }
                    let node = self.top()?;
                    return convert(&node, 0, &mut HashSet::new(), &mut 0, self.limits);
                }
                b'(' => self.stack.push(Item::Mark),
                b'N' => self.push(Primitive::Scalar(Value::Null))?,
                0x88 => self.push(Primitive::Scalar(Value::Bool(true)))?,
                0x89 => self.push(Primitive::Scalar(Value::Bool(false)))?,
                b'K' => {
                    let n = self.byte()?;
                    self.push(Primitive::Scalar(n.into()))?
                }
                b'M' => {
                    let n = self.length(2)?;
                    self.push(Primitive::Scalar(n.into()))?
                }
                b'J' => {
                    let n = i32::from_le_bytes(self.bytes(4)?.try_into().ok()?);
                    self.push(Primitive::Scalar(n.into()))?
                }
                0x8a | 0x8b => {
                    let width = if self.data[self.at - 1] == 0x8a { 1 } else { 4 };
                    let len = self.length(width)?;
                    if len > 1800 {
                        return None;
                    }
                    let n = BigInt::from_signed_bytes_le(self.bytes(len)?).to_string();
                    if n.trim_start_matches('-').len() > 4300 {
                        return None;
                    }
                    self.push(Primitive::Scalar(serde_json::from_str(&n).ok()?))?
                }
                b'G' => {
                    let n = f64::from_be_bytes(self.bytes(8)?.try_into().ok()?);
                    self.push(Primitive::Scalar(Number::from_f64(n)?.into()))?
                }
                0x8c | b'X' | 0x8d => {
                    let width = match self.data[self.at - 1] {
                        0x8c => 1,
                        b'X' => 4,
                        _ => 8,
                    };
                    let len = self.length(width)?;
                    let text = std::str::from_utf8(self.bytes(len)?).ok()?.to_owned();
                    self.push(Primitive::Scalar(text.into()))?
                }
                b']' => self.push(Primitive::List(vec![]))?,
                b'}' => self.push(Primitive::Dict(BTreeMap::new()))?,
                b'a' => {
                    let Item::Node(node) = self.stack.pop()? else {
                        return None;
                    };
                    self.list_items(vec![node])?
                }
                b'e' => {
                    let items = self.marked()?;
                    self.list_items(items)?
                }
                b's' => {
                    let Item::Node(value) = self.stack.pop()? else {
                        return None;
                    };
                    let Item::Node(key) = self.stack.pop()? else {
                        return None;
                    };
                    self.dict_items(vec![key, value])?
                }
                b'u' => {
                    let items = self.marked()?;
                    self.dict_items(items)?
                }
                0x94 => self.memo(self.memo.len())?,
                b'q' | b'r' => {
                    let width = if self.data[self.at - 1] == b'q' { 1 } else { 4 };
                    let index = self.length(width)?;
                    self.memo(index)?
                }
                b'h' | b'j' => {
                    let width = if self.data[self.at - 1] == b'h' { 1 } else { 4 };
                    let index = self.length(width)?;
                    let node = self.memo.get(index)?.as_ref()?.clone();
                    self.stack.push(Item::Node(node))
                }
                _ => return None,
            }
        }
        None
    }
}
fn reaches(node: &Node, target: &Node, work: &mut usize) -> bool {
    let mut pending = vec![node.clone()];
    let mut visited = HashSet::new();
    while let Some(node) = pending.pop() {
        *work += 1;
        if *work > 65536 {
            return true;
        }
        if Rc::ptr_eq(&node, target) {
            return true;
        }
        if !visited.insert(Rc::as_ptr(&node) as usize) {
            continue;
        }
        if visited.len() > MAX_NODES {
            return true;
        }
        match &*node.borrow() {
            Primitive::Scalar(_) => {}
            Primitive::List(items) => pending.extend(items.iter().cloned()),
            Primitive::Dict(items) => pending.extend(items.values().cloned()),
        }
    }
    false
}
fn convert(
    node: &Node,
    depth: usize,
    path: &mut HashSet<usize>,
    budget: &mut usize,
    limits: Limits,
) -> Option<Value> {
    if depth > limits.depth {
        return None;
    }
    let id = Rc::as_ptr(node) as usize;
    if !path.insert(id) {
        return None;
    }
    *budget = budget.checked_add(1)?;
    if *budget > limits.nodes {
        return None;
    }
    let value = match &*node.borrow() {
        Primitive::Scalar(value) => {
            *budget = budget.checked_add(match value {
                Value::String(value) => value.len(),
                _ => 1,
            })?;
            if *budget > limits.bytes {
                return None;
            }
            value.clone()
        }
        Primitive::List(items) => Value::Array(
            items
                .iter()
                .map(|item| convert(item, depth + 1, path, budget, limits))
                .collect::<Option<_>>()?,
        ),
        Primitive::Dict(items) => {
            let mut map = Map::new();
            for (key, item) in items {
                *budget = budget.checked_add(key.len())?;
                if *budget > limits.bytes {
                    return None;
                }
                map.insert(key.clone(), convert(item, depth + 1, path, budget, limits)?);
            }
            Value::Object(map)
        }
    };
    path.remove(&id);
    Some(value)
}
pub(crate) fn decode(data: &[u8]) -> Option<Value> {
    decode_limited(data, DEFAULT_LIMITS)
}
/// Opt-in bounded cache reader. The ordinary codec keeps its historical limits.
/// Before decoding, bound raw, graph nodes and expanded primitive work; reject
/// amplification instead of building a tree and subtracting it afterwards.
pub(crate) fn decode_settings(data: &[u8]) -> Option<Value> {
    decode_limited(
        data,
        Limits {
            bytes: 65536,
            nodes: 2048,
            depth: 32,
        },
    )
}
fn decode_limited(data: &[u8], limits: Limits) -> Option<Value> {
    if data.len() > limits.bytes {
        return None;
    }
    Reader {
        data,
        limits,
        at: 0,
        stack: vec![],
        memo: vec![],
        nodes: 0,
        work: 0,
    }
    .read()
}
pub(crate) fn encode(value: &Value) -> Option<Vec<u8>> {
    fn emit(value: &Value, out: &mut Vec<u8>, depth: usize, nodes: &mut usize) -> Option<()> {
        if depth > MAX_DEPTH || *nodes > MAX_NODES || out.len() > MAX_BYTES {
            return None;
        }
        *nodes += 1;
        match value {
            Value::Null => out.push(b'N'),
            Value::Bool(v) => out.push(if *v { 0x88 } else { 0x89 }),
            Value::String(v) => {
                if v.len() > MAX_BYTES {
                    return None;
                }
                out.push(b'X');
                out.extend(u32::try_from(v.len()).ok()?.to_le_bytes());
                out.extend(v.as_bytes())
            }
            Value::Number(n) if n.is_f64() => {
                out.push(b'G');
                out.extend(n.as_f64()?.to_be_bytes())
            }
            Value::Number(n) => {
                let decimal = n.to_string();
                if decimal.trim_start_matches('-').len() > 4300 {
                    return None;
                }
                let n = decimal.parse::<BigInt>().ok()?.to_signed_bytes_le();
                out.push(0x8b);
                out.extend(u32::try_from(n.len()).ok()?.to_le_bytes());
                out.extend(n)
            }
            Value::Array(items) => {
                out.extend(b"](");
                for item in items {
                    emit(item, out, depth + 1, nodes)?
                }
                out.push(b'e')
            }
            Value::Object(items) => {
                out.extend(b"}(");
                for (key, item) in items {
                    emit(&Value::String(key.clone()), out, depth + 1, nodes)?;
                    emit(item, out, depth + 1, nodes)?
                }
                out.push(b'u')
            }
        }
        if out.len() > MAX_BYTES {
            return None;
        }
        Some(())
    }
    let mut out = vec![0x80, 4];
    emit(value, &mut out, 0, &mut 0)?;
    out.push(b'.');
    Some(out)
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn pinned_boolean_vectors_and_rejected_executable_opcodes() {
        assert_eq!(decode(&[0x80, 4, 0x88, b'.']), Some(json!(true)));
        assert_eq!(decode(&[0x80, 4, 0x89, b'.']), Some(json!(false)));
        assert!(decode(b"cos\nsystem\n.").is_none());
        assert!(decode(&[0x80, 4, b'}', 0x94, b'h', 0, b'N', b's', b'.']).is_none());
        assert!(decode(&[0x80, 4, b']', 0x94, b'h', 0, b'a', b'.']).is_none());
    }
    #[test]
    fn source_protocol4_dictionary_vectors_include_memo_aliases() {
        let vectors: Vec<Value> =
            serde_json::from_str(include_str!("../datasets/django-pickle-protocol4.json")).unwrap();
        for vector in vectors {
            let hex = vector["hex"].as_str().unwrap();
            let bytes: Vec<u8> = hex
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            assert_eq!(decode(&bytes), Some(vector["value"].clone()));
        }
    }
    #[test]
    fn primitive_round_trip_keeps_large_integer_and_float_types() {
        let value:Value=serde_json::from_str(r#"{"M3U_EPG":"::/0","nested":[null,true,1,1.0,-123456789012345678901234567890],"unicode":"東京"}"#).unwrap();
        assert_eq!(decode(&encode(&value).unwrap()), Some(value));
    }
}

#[cfg(test)]
mod settings_limits_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn settings_reader_has_narrow_limits_without_changing_ordinary_codec() {
        let small = json!({"buffering_timeout":31});
        let raw = encode(&small).unwrap();
        assert_eq!(decode_settings(&raw), Some(small));
        let ordinary = json!({"literal":"x".repeat(70000)});
        let raw = encode(&ordinary).unwrap();
        assert_eq!(decode(&raw), Some(ordinary));
        assert!(decode_settings(&raw).is_none());
        assert!(decode_settings(&vec![0; 65537]).is_none());
    }
}
