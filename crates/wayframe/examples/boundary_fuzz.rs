//! Reproducible bounded mutation harness. Prints its seed so failures can be replayed.
use std::time::{Duration, Instant};
use wayframe::{
    codec::Sse,
    http::{stream::canonical_path, RequestContext},
    mcp::{parse_unique, McpGuard, MODERN_VERSION},
    prelude::*,
};
fn next(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let count = std::env::var("WAYFRAME_FUZZ_RUNS")
        .ok()
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(10000usize);
    if !(1000..=1000000).contains(&count) {
        return Err("invalid fuzz iterations".into());
    }
    let mut seed = 0x5a17_cafe_b00b_u64;
    let initial = seed;
    let guard = McpGuard::new("mcp".into(), 4096)?;
    let corpus = [
        br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"add","arguments":{}}}"#
            .as_slice(),
        br#"{"id":1,"id":2}"#.as_slice(),
        b"data: {}\r\n\r\n",
        b"/%252fadmin",
    ];
    for _ in 0..count {
        let mut input = corpus[(next(&mut seed) as usize) % corpus.len()].to_vec();
        for _ in 0..(next(&mut seed) % 8) {
            let at = (next(&mut seed) as usize) % (input.len() + 1);
            match next(&mut seed) % 3 {
                0 if at < input.len() => {
                    input[at] = next(&mut seed) as u8;
                }
                1 if at < input.len() => {
                    input.remove(at);
                }
                _ => input.insert(at, next(&mut seed) as u8),
            }
        }
        let _ = parse_unique(&input);
        if let Ok(path) = std::str::from_utf8(&input) {
            if let Some(path) = canonical_path(path) {
                assert_eq!(canonical_path(&path).as_deref(), Some(path.as_str()));
            }
        }
        let mut decoder = Sse::new(4096).map_err(|e| e.to_string())?;
        let width = ((next(&mut seed) % 16) + 1) as usize;
        for piece in input.chunks(width) {
            if decoder.push(piece).is_err() {
                break;
            }
        }
        let _ = decoder.finish();
        let mut ctx = RequestContext::new(
            "POST".parse()?,
            "/mcp".into(),
            "127.0.0.1".parse()?,
            (Instant::now() + Duration::from_secs(1)).into(),
        );
        ctx.body = Some(input.into());
        for (key, value) in [
            ("content-type", "application/json"),
            ("accept", "application/json, text/event-stream"),
            ("mcp-protocol-version", MODERN_VERSION),
            ("mcp-method", "tools/call"),
            ("mcp-name", "add"),
        ] {
            ctx.headers.insert(
                http::header::HeaderName::from_bytes(key.as_bytes())?,
                value.parse()?,
            );
        }
        let decision = match guard.execute(&mut ctx) {
            Execution::Immediate(decision) => decision,
            Execution::Deferred(_) => unreachable!("native inspector"),
        };
        if decision != Decision::Continue {
            assert!(ctx.operation.is_none());
        }
    }
    println!(
        "{}",
        serde_json::json!({"seed":initial,"iterations":count,"status":"passed","scope":"JSON duplicate keys, canonical paths, fragmented SSE, MCP envelopes"})
    );
    Ok(())
}
