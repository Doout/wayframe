//! Wayframe is a framework, not a gateway executable.
//!
//! Start with typed native blocks and a pipeline. Opt into HTTP, controls,
//! transport adapters, MCP inspection, and Wasm only when the application needs them.
#[cfg(feature = "auth")]
pub use wayframe_auth as auth;
#[cfg(feature = "controls")]
pub use wayframe_blocks as controls;
#[cfg(feature = "cache")]
pub use wayframe_cache as cache;
#[cfg(feature = "codec")]
pub use wayframe_codec as codec;
pub use wayframe_core::Task;
pub use wayframe_core::{Block, Context, Decision, Execution, Phase, Plan, PlanError, Reason};
#[cfg(feature = "flow")]
pub use wayframe_flow as flow;
#[cfg(feature = "http")]
pub use wayframe_http as http;
#[cfg(feature = "lb")]
pub use wayframe_lb as lb;
#[cfg(feature = "listeners")]
pub use wayframe_listeners as listeners;
#[cfg(feature = "management")]
pub use wayframe_management as management;
#[cfg(feature = "mcp")]
pub use wayframe_mcp as mcp;
#[cfg(feature = "observability")]
pub use wayframe_observability as observability;
#[cfg(feature = "plugins")]
pub use wayframe_plugins as plugins;
#[cfg(feature = "resilience")]
pub use wayframe_resilience as resilience;
#[cfg(feature = "routing")]
pub use wayframe_routing as routing;
pub use wayframe_runtime::{Generation, Live, Pipeline, PublishError};
#[cfg(feature = "transport")]
pub use wayframe_transport as transport;
#[cfg(feature = "wasm")]
pub use wayframe_wasm as wasm;
pub mod prelude {
    pub use crate::{Block, Context, Decision, Execution, Phase, Pipeline, Reason};
}

#[cfg(feature = "policies")]
pub use wayframe_policies as policies;
