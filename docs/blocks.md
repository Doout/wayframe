# Authoring blocks

Implement `wayframe::Context` for your work item and `Block<YourContext>` for a native control.
The complete non-HTTP example is `crates/wayframe/examples/typed_message.rs`.

```rust
use wayframe::prelude::*;
use std::time::Instant;

struct Message { deadline: Instant, value: String }
impl Context for Message {
    fn deadline(&self) -> Instant { self.deadline }
}
struct Nonempty;
impl Block<Message> for Nonempty {
    fn id(&self) -> &str { "nonempty" }
    fn phase(&self) -> Phase { Phase::Inspect }
    fn execute<'a>(&'a self, message: &'a mut Message) -> Execution<'a> {
        Execution::Immediate(if message.value.is_empty() {
            Decision::deny(Reason::InvalidInput, "empty_message")
        } else { Decision::Continue })
    }
}
```

Construct the block once, wrap it in `Arc`, and supply it to `Pipeline::compile`. Maintain state
in the block, not in global mutable variables. A context is owned by one evaluation at a time;
block objects may be called by many evaluations concurrently. State must be explicitly synchronized.

For asynchronous checks return `Execution::Deferred(Box::pin(async move { ... }))`. Work must yield
cooperatively, observe cancellation and respect the existing deadline. The executor cancels the
future on timeout; do not detach side-effectful operations and assume the deadline stops them.
Native blocks cannot extend the executor's captured deadline or force later blocks to run after deny.

The v0.1 native interface is a Rust source API, not a stable binary ABI. Blocks compile into the
application. No arbitrary `.so`/`.dll` loading occurs. An embedded Python/Java process is not a
native block; use an explicit authenticated external policy bridge with OS resource boundaries.

HTTP-specific block authors use `wayframe::http::RequestContext`. Authenticated `Identity` contains
subject, tenant and granted permissions. It is populated by trusted authentication, not by client
headers or MCP `_meta.clientInfo`. `Operation` is populated by protocol inspection before
operation-level authorization. Optional `body` contains an already bounded envelope for inspection;
this version does not expose a general streaming-transform host API.

The plugin equivalent is the separately versioned WIT contract in `wit/policy.wit`, not this Rust
trait object. SDK language bindings are generated from WIT. The first guest sample is Rust; no
multilanguage SDK compatibility claim has been tested yet.
