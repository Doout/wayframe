//! Optional MCP envelope inspection and tool authorization, not an MCP server SDK.
//! Versioned against 2026-07-28 with opt-in 2025-11-25 compatibility.
//! The bounded JSON body is authoritative; mirrored headers are never trusted alone.
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::Value;
use std::{collections::HashMap, fmt};
use wayframe_core::{Block, Decision, Execution, Phase, Reason};
use wayframe_http::{Operation, RequestContext};

pub const MODERN_VERSION: &str = "2026-07-28";
pub const LEGACY_VERSION: &str = "2025-11-25";
const META_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
const META_CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";

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
fn bad(code: &'static str) -> Decision {
    Decision::deny(Reason::InvalidInput, code)
}
fn header<'a>(ctx: &'a RequestContext, name: &str) -> Result<Option<&'a str>, ()> {
    let mut values = ctx.headers.get_all(name).iter();
    let value = values
        .next()
        .map(|v| v.to_str().map_err(|_| ()))
        .transpose()?;
    if values.next().is_some() {
        return Err(());
    }
    Ok(value)
}
fn header_name(value: &str) -> Option<String> {
    if let Some(encoded) = value
        .strip_prefix("=?base64?")
        .and_then(|s| s.strip_suffix("?="))
    {
        if encoded.len() > 4096 {
            return None;
        }
        String::from_utf8(STANDARD.decode(encoded).ok()?).ok()
    } else if value.trim() == value
        && value
            .bytes()
            .all(|b| (0x20..=0x7e).contains(&b) || b == b'\t')
    {
        Some(value.into())
    } else {
        None
    }
}
fn valid_id(v: &Value) -> bool {
    match v {
        Value::String(s) => s.len() <= 256,
        Value::Number(n) => n.is_i64() || n.is_u64(),
        _ => false,
    }
}
/// A bounded inspection block. Legacy mode is an explicit application decision.
pub struct McpGuard {
    id: String,
    legacy: bool,
    sessions: bool,
    maximum: usize,
}
impl McpGuard {
    pub fn new(id: String, maximum: usize) -> Result<Self, String> {
        if !(1024..=1024 * 1024).contains(&maximum) {
            return Err("invalid MCP envelope limit".into());
        }
        Ok(Self {
            id,
            legacy: false,
            sessions: false,
            maximum,
        })
    }
    pub fn allow_legacy(mut self, enabled: bool) -> Self {
        self.legacy = enabled;
        self
    }
    /// Enable handshake-era GET/DELETE forwarding. The application must enforce session ownership.
    pub fn allow_sessions(mut self, enabled: bool) -> Self {
        self.sessions = enabled;
        self
    }
    fn inspect(&self, ctx: &mut RequestContext) -> Decision {
        ctx.operation = None;
        if matches!(ctx.method.as_str(), "GET" | "DELETE")
            && header(ctx, "mcp-protocol-version") != Ok(Some(LEGACY_VERSION))
        {
            return Decision::deny(Reason::MethodNotAllowed, "mcp_method_not_supported");
        }
        if self.sessions && self.legacy && matches!(ctx.method.as_str(), "GET" | "DELETE") {
            if header(ctx, "mcp-protocol-version") != Ok(Some(LEGACY_VERSION))
                || !matches!(header(ctx, "mcp-session-id"), Ok(Some(id)) if !id.is_empty() && id.len() <= 512 && id.bytes().all(|b| (0x21..=0x7e).contains(&b)))
            {
                return bad("mcp_session_required");
            }
            if ctx.method.as_str() == "GET"
                && !matches!(header(ctx, "accept"), Ok(Some(accept)) if accept.split(',').any(|part| part.trim() == "text/event-stream"))
            {
                return bad("mcp_accept_required");
            }
            if ctx.body.as_ref().is_some_and(|b| !b.is_empty()) {
                return bad("mcp_unexpected_body");
            }
            ctx.operation = Some(Operation {
                protocol: "mcp",
                action: format!("transport/{}", ctx.method.as_str().to_ascii_lowercase()),
                target: None,
            });
            return Decision::Continue;
        }
        let Some(body) = &ctx.body else {
            return bad("mcp_body_inspection_required");
        };
        if body.len() > self.maximum {
            return Decision::deny(Reason::PayloadTooLarge, "mcp_envelope_limit");
        }
        if ctx.method.as_str() != "POST" {
            return bad("mcp_post_required");
        }
        let media = match header(ctx, "content-type") {
            Ok(Some(v)) => v,
            _ => return bad("mcp_content_type"),
        };
        if !media
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case("application/json")
        {
            return bad("mcp_content_type");
        }
        let accept = match header(ctx, "accept") {
            Ok(Some(v)) => v,
            _ => return bad("mcp_accept_required"),
        };
        let acceptable = |wanted: &str| {
            accept.split(',').any(|part| {
                let mut fields = part.trim().split(';');
                fields
                    .next()
                    .unwrap_or("")
                    .trim()
                    .eq_ignore_ascii_case(wanted)
                    && !fields.any(|p| {
                        p.trim().strip_prefix("q=").is_some_and(|q| {
                            q.parse::<f32>()
                                .map_or(true, |n| !n.is_finite() || n <= 0.0 || n > 1.0)
                        })
                    })
            })
        };
        if !acceptable("application/json") || !acceptable("text/event-stream") {
            return bad("mcp_accept_required");
        }
        let value = match parse_unique(body) {
            Ok(v) => v,
            Err(_) => return bad("mcp_invalid_json"),
        };
        let Some(object) = value.as_object() else {
            return bad("mcp_batch_not_supported");
        };
        if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return bad("mcp_invalid_envelope");
        }
        if object.contains_key("result") || object.contains_key("error") {
            return bad("mcp_client_response_not_supported");
        }
        let Some(method) = object
            .get("method")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 128)
        else {
            return bad("mcp_invalid_method");
        };
        let id = object.get("id");
        if id.is_some_and(|v| !valid_id(v)) {
            return bad("mcp_invalid_id");
        }
        let empty = serde_json::Map::new();
        let params = match object.get("params") {
            None => &empty,
            Some(Value::Object(p)) => p,
            _ => return bad("mcp_invalid_params"),
        };
        if params.get("_meta").is_some_and(|m| !m.is_object()) {
            return bad("mcp_invalid_metadata");
        }
        let version = match header(ctx, "mcp-protocol-version") {
            Ok(v) => v,
            Err(_) => return bad("mcp_duplicate_header"),
        };
        let body_version = params
            .get("_meta")
            .and_then(Value::as_object)
            .and_then(|m| m.get(META_VERSION));
        let modern = body_version.is_some() || version == Some(MODERN_VERSION);
        if modern {
            let Some(meta) = params.get("_meta").and_then(Value::as_object) else {
                return bad("mcp_required_metadata");
            };
            let Some(body_version) = meta.get(META_VERSION).and_then(Value::as_str) else {
                return bad("mcp_required_metadata");
            };
            if !meta.get(META_CAPABILITIES).is_some_and(Value::is_object) {
                return bad("mcp_required_metadata");
            }
            if body_version != MODERN_VERSION || version.is_some_and(|v| v != MODERN_VERSION) {
                return bad("mcp_unsupported_version");
            }
            if version != Some(body_version) {
                return bad("mcp_header_mismatch");
            }
            if header(ctx, "mcp-method") != Ok(Some(method)) {
                return bad("mcp_header_mismatch");
            }
            // This example advertises no extensions defining client notifications.
            if id.is_none() {
                return bad("mcp_notification_not_supported");
            }
        } else {
            if !self.legacy {
                return bad("mcp_legacy_disabled");
            }
            if method == "initialize" {
                if params.get("protocolVersion").and_then(Value::as_str) != Some(LEGACY_VERSION)
                    || !params.get("capabilities").is_some_and(Value::is_object)
                    || !params.get("clientInfo").is_some_and(Value::is_object)
                {
                    return bad("mcp_invalid_initialize");
                }
                if version.is_some_and(|v| v != LEGACY_VERSION) {
                    return bad("mcp_unsupported_version");
                }
            } else if version != Some(LEGACY_VERSION) {
                return bad("mcp_unsupported_version");
            }
            if id.is_none()
                && !matches!(
                    method,
                    "notifications/initialized" | "notifications/cancelled"
                )
            {
                return bad("mcp_notification_not_supported");
            }
            if method == "notifications/cancelled" && !params.get("requestId").is_some_and(valid_id)
            {
                return bad("mcp_invalid_cancellation");
            }
            if id.is_some() && method.starts_with("notifications/") {
                return bad("mcp_invalid_notification");
            }
        }
        let target = match method {
            "tools/call" | "prompts/get" => params.get("name").and_then(Value::as_str),
            "resources/read" => params.get("uri").and_then(Value::as_str),
            _ => None,
        };
        if matches!(method, "tools/call" | "prompts/get" | "resources/read")
            && !target.is_some_and(|s| !s.is_empty() && s.len() <= 1024)
        {
            return bad("mcp_invalid_target");
        }
        if method == "tools/call" && params.get("arguments").is_some_and(|v| !v.is_object()) {
            return bad("mcp_invalid_arguments");
        }
        if modern {
            let mirrored = match header(ctx, "mcp-name") {
                Ok(v) => v,
                Err(_) => return bad("mcp_header_mismatch"),
            };
            match (target, mirrored) {
                (Some(expected), Some(actual))
                    if header_name(actual).as_deref() == Some(expected) => {}
                (None, None) => {}
                _ => return bad("mcp_header_mismatch"),
            }
        }
        ctx.operation = Some(Operation {
            protocol: "mcp",
            action: method.into(),
            target: target.map(str::to_owned),
        });
        Decision::Continue
    }
}
impl Block<RequestContext> for McpGuard {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Inspect
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        Execution::Immediate(self.inspect(ctx))
    }
}

/// Explicit tool-to-permission mapping; neither annotations nor clientInfo authorize anything.
pub struct ToolPermissions {
    id: String,
    required: HashMap<String, String>,
}
impl ToolPermissions {
    pub fn new(id: String, required: HashMap<String, String>) -> Result<Self, String> {
        if required.is_empty()
            || required.len() > 1024
            || required
                .iter()
                .any(|(t, p)| t.is_empty() || t.len() > 1024 || p.is_empty() || p.len() > 256)
        {
            return Err("invalid tool permission map".into());
        }
        Ok(Self { id, required })
    }
}
impl Block<RequestContext> for ToolPermissions {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Authorize
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        let denied = || Decision::deny(Reason::Forbidden, "tool_not_permitted");
        let decision = match (&ctx.identity, &ctx.operation) {
            (None, _) => Decision::deny(Reason::Unauthenticated, "unauthorized"),
            (_, None) => bad("mcp_not_inspected"),
            (Some(identity), Some(op)) if op.protocol == "mcp" => match op.action.as_str() {
                "server/discover"
                | "initialize"
                | "notifications/initialized"
                | "notifications/cancelled"
                | "transport/get"
                | "transport/delete"
                | "ping"
                | "tools/list" => Decision::Continue,
                "tools/call" => match op.target.as_ref().and_then(|name| self.required.get(name)) {
                    Some(permission) if identity.permissions.contains(permission) => {
                        Decision::Continue
                    }
                    _ => denied(),
                },
                _ => denied(),
            },
            _ => denied(),
        };
        Execution::Immediate(decision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;
    use wayframe_http::Identity;
    fn envelope(target: &str) -> Value {
        json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
            "name":target,"arguments":{},"_meta":{(META_VERSION):MODERN_VERSION,(META_CAPABILITIES):{}}}})
    }
    fn context(value: Value) -> RequestContext {
        let mut c = RequestContext::new(
            http::Method::POST,
            "/mcp".into(),
            "127.0.0.1".parse().unwrap(),
            tokio::time::Instant::now() + Duration::from_secs(1),
        );
        for (key, value) in [
            ("content-type", "application/json"),
            ("accept", "application/json, text/event-stream"),
            ("mcp-protocol-version", MODERN_VERSION),
            ("mcp-method", "tools/call"),
            ("mcp-name", "add"),
        ] {
            c.headers.insert(
                http::header::HeaderName::from_static(key),
                value.parse().unwrap(),
            );
        }
        c.body = Some(bytes::Bytes::from(serde_json::to_vec(&value).unwrap()));
        c
    }
    fn inspect(c: &mut RequestContext) -> Decision {
        McpGuard::new("mcp".into(), 32768).unwrap().inspect(c)
    }
    #[test]
    fn session_methods_require_opt_in_and_valid_protocol_metadata() {
        let mut c = context(envelope("add"));
        c.method = http::Method::GET;
        c.body = Some(bytes::Bytes::new());
        assert_eq!(
            inspect(&mut c),
            Decision::deny(Reason::MethodNotAllowed, "mcp_method_not_supported")
        );
        c.headers
            .insert("mcp-protocol-version", LEGACY_VERSION.parse().unwrap());
        c.headers
            .insert("mcp-session-id", "owned-session".parse().unwrap());
        let guard = McpGuard::new("mcp".into(), 32768)
            .unwrap()
            .allow_legacy(true)
            .allow_sessions(true);
        assert_eq!(guard.inspect(&mut c), Decision::Continue);
        assert_eq!(c.operation.as_ref().unwrap().action, "transport/get");
        c.headers
            .append("mcp-session-id", "another-session".parse().unwrap());
        assert_eq!(guard.inspect(&mut c), bad("mcp_session_required"));
        assert!(c.operation.is_none());
    }
    #[test]
    fn cancellation_cannot_omit_its_request_id() {
        let mut c = context(
            serde_json::json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{}}),
        );
        c.headers
            .insert("mcp-protocol-version", LEGACY_VERSION.parse().unwrap());
        let guard = McpGuard::new("mcp".into(), 32768)
            .unwrap()
            .allow_legacy(true);
        assert_eq!(guard.inspect(&mut c), bad("mcp_invalid_cancellation"));
        c.body = Some(bytes::Bytes::from(serde_json::to_vec(&serde_json::json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":12}})).unwrap()));
        assert_eq!(guard.inspect(&mut c), Decision::Continue);
    }
    #[test]
    fn duplicate_keys_are_rejected_at_every_depth() {
        for bytes in [
            br#"{"method":"a","method":"b"}"#.as_slice(),
            br#"{"params":{"name":"a","name":"b"}}"#.as_slice(),
        ] {
            assert!(parse_unique(bytes).is_err());
        }
    }
    #[test]
    fn mirrored_headers_cannot_hide_a_different_tool() {
        let mut c = context(envelope("admin_preview"));
        assert_eq!(inspect(&mut c), bad("mcp_header_mismatch"));
        assert!(c.operation.is_none());
    }
    #[test]
    fn valid_request_populates_verified_operation() {
        let mut c = context(envelope("add"));
        assert_eq!(inspect(&mut c), Decision::Continue);
        assert_eq!(c.operation.unwrap().target.as_deref(), Some("add"));
    }
    #[test]
    fn unsupported_version_and_null_ids_fail_closed() {
        let mut value = envelope("add");
        value["id"] = Value::Null;
        assert_eq!(inspect(&mut context(value)), bad("mcp_invalid_id"));
        let mut c = context(envelope("add"));
        c.headers
            .insert("mcp-protocol-version", "2099-01-01".parse().unwrap());
        assert_eq!(inspect(&mut c), bad("mcp_unsupported_version"));
    }
    #[test]
    fn duplicate_metadata_headers_are_rejected() {
        let mut c = context(envelope("add"));
        c.headers
            .append("mcp-name", "admin_preview".parse().unwrap());
        assert_eq!(inspect(&mut c), bad("mcp_header_mismatch"));
    }
    #[test]
    fn base64_mirrored_name_is_decoded_before_comparison() {
        let mut c = context(envelope("add"));
        c.headers
            .insert("mcp-name", "=?base64?YWRk?=".parse().unwrap());
        assert_eq!(inspect(&mut c), Decision::Continue);
    }
    #[test]
    fn modern_requests_need_capabilities() {
        let mut v = envelope("add");
        v["params"]["_meta"]
            .as_object_mut()
            .unwrap()
            .remove(META_CAPABILITIES);
        assert_eq!(inspect(&mut context(v)), bad("mcp_required_metadata"));
    }
    #[tokio::test]
    async fn permissions_come_from_authenticated_identity() {
        let mut c = context(envelope("add"));
        assert_eq!(inspect(&mut c), Decision::Continue);
        let p = ToolPermissions::new(
            "tools".into(),
            HashMap::from([("add".into(), "tools:read".into())]),
        )
        .unwrap();
        c.identity = Some(Identity::new("reader", "demo"));
        assert!(matches!(
            p.execute(&mut c).resolve().await,
            Decision::Deny {
                reason: Reason::Forbidden,
                ..
            }
        ));
        c.identity = Some(Identity::new("reader", "demo").with_permissions(["tools:read"]));
        assert_eq!(p.execute(&mut c).resolve().await, Decision::Continue);
        c.operation.as_mut().unwrap().target = Some("unknown".into());
        assert!(matches!(
            p.execute(&mut c).resolve().await,
            Decision::Deny {
                reason: Reason::Forbidden,
                ..
            }
        ));
    }
}
