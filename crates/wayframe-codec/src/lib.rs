//! Bounded protocol records, independent of listeners and policy contexts.
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::Value;
use std::fmt;
/// serde_json normally keeps the last duplicate key. Policy and execution parsers
/// must never disagree on duplicate `method`, `name` or authorization metadata.
struct UniqueValue(Value);
impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Unique;
        impl<'de> Visitor<'de> for Unique {
            type Value = UniqueValue;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("JSON without duplicate keys")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Bool(v)))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| UniqueValue(Value::Number(n)))
                    .ok_or_else(|| E::custom("non-finite number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(v.into())))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(v)))
            }
            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(v) = a.next_element::<UniqueValue>()? {
                    values.push(v.0);
                }
                Ok(UniqueValue(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
                let mut values = serde_json::Map::new();
                while let Some((k, v)) = a.next_entry::<String, UniqueValue>()? {
                    if values.insert(k, v.0).is_some() {
                        return Err(de::Error::custom("duplicate JSON key"));
                    }
                }
                Ok(UniqueValue(Value::Object(values)))
            }
        }
        d.deserialize_any(Unique)
    }
}
pub fn parse_unique(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    let value = serde_json::from_slice::<UniqueValue>(bytes)?.0;
    let mut stack = vec![(&value, 0usize)];
    let mut nodes = 0usize;
    while let Some((node, depth)) = stack.pop() {
        nodes += 1;
        if depth > 64 || nodes > 4096 {
            return Err(<serde_json::Error as de::Error>::custom(
                "JSON complexity limit",
            ));
        }
        match node {
            Value::Array(values) => stack.extend(values.iter().map(|v| (v, depth + 1))),
            Value::Object(values) => stack.extend(values.values().map(|v| (v, depth + 1))),
            _ => {}
        }
    }
    Ok(value)
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Event {
    pub event: Option<String>,
    pub id: Option<String>,
    pub retry: Option<String>,
    pub data: String,
}
impl Event {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = String::new();
        for (field, value) in [
            ("event", &self.event),
            ("id", &self.id),
            ("retry", &self.retry),
        ] {
            if let Some(value) = value {
                out.push_str(field);
                out.push_str(": ");
                out.push_str(value);
                out.push('\n');
            }
        }
        if self.data.is_empty() && out.is_empty() {
            out.push_str(":\n");
        }
        if !self.data.is_empty() {
            for line in self.data.split('\n') {
                out.push_str("data: ");
                out.push_str(line);
                out.push('\n');
            }
        }
        out.push('\n');
        out.into_bytes()
    }
}
#[derive(Debug)]
pub struct DecodeError(pub &'static str);
impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for DecodeError {}
/// Incremental UTF-8 SSE record decoder with a per-event memory ceiling.
pub struct Sse {
    pending: Vec<u8>,
    event: Event,
    maximum: usize,
    event_bytes: usize,
}
impl Sse {
    pub fn new(maximum: usize) -> Result<Self, DecodeError> {
        if maximum == 0 || maximum > 64 * 1024 * 1024 {
            return Err(DecodeError("invalid event budget"));
        }
        Ok(Self {
            pending: vec![],
            event: Event::default(),
            maximum,
            event_bytes: 0,
        })
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Event>, DecodeError> {
        if self.pending.len().saturating_add(bytes.len()) > self.maximum {
            return Err(DecodeError("SSE pending limit"));
        }
        self.pending.extend_from_slice(bytes);
        let mut events = vec![];
        while let Some(end) = self.pending.iter().position(|b| matches!(b, b'\r' | b'\n')) {
            if self.pending[end] == b'\r' && end + 1 == self.pending.len() {
                break;
            }
            let width = if self.pending[end] == b'\r' && self.pending.get(end + 1) == Some(&b'\n') {
                2
            } else {
                1
            };
            self.event_bytes += end + width;
            if self.event_bytes > self.maximum {
                return Err(DecodeError("SSE event limit"));
            }
            let line = std::str::from_utf8(&self.pending[..end])
                .map_err(|_| DecodeError("invalid SSE UTF-8"))?;
            if line.is_empty() {
                if self.event.data.ends_with('\n') {
                    self.event.data.pop();
                }
                events.push(std::mem::take(&mut self.event));
                self.event_bytes = 0;
            } else if !line.starts_with(':') {
                let (field, value) = line.split_once(':').unwrap_or((line, ""));
                let value = value.strip_prefix(' ').unwrap_or(value);
                match field {
                    "data" => {
                        self.event.data.push_str(value);
                        self.event.data.push('\n');
                    }
                    "event" => self.event.event = Some(value.into()),
                    "id" if !value.contains('\0') => self.event.id = Some(value.into()),
                    "retry" if value.bytes().all(|b| b.is_ascii_digit()) => {
                        self.event.retry = Some(value.into())
                    }
                    _ => {}
                }
            }
            self.pending.drain(..end + width);
        }
        Ok(events)
    }
    pub fn finish(&self) -> Result<(), DecodeError> {
        if !self.pending.is_empty() || self.event_bytes != 0 {
            return Err(DecodeError("truncated SSE record"));
        }
        Ok(())
    }
}
#[cfg(test)]
mod codec_tests {
    use super::*;
    #[test]
    fn fragmented_utf8_and_crlf_preserve_record_metadata() {
        let mut decoder = Sse::new(1024).unwrap();
        let text = "id: 1\r\nevent: message\r\ndata: {\"text\":\"é\"}\r\n\r\n";
        let mut events = vec![];
        for b in text.as_bytes() {
            events.extend(decoder.push(&[*b]).unwrap());
        }
        decoder.finish().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id.as_deref(), Some("1"));
        assert_eq!(
            parse_unique(events[0].data.as_bytes()).unwrap()["text"],
            "é"
        );
    }
    #[test]
    fn bounds_and_truncation_are_failures() {
        let mut decoder = Sse::new(8).unwrap();
        assert!(decoder.push(b"data: too long").is_err());
        let mut decoder = Sse::new(1024).unwrap();
        decoder.push(b"data: {}\n").unwrap();
        assert!(decoder.finish().is_err());
    }
}

#[cfg(feature = "http")]
pub mod metadata;
