//! Wayframe is a framework, not a gateway executable.
//!
//! Start with typed native blocks and a pipeline. Opt into HTTP, controls,
//! transport adapters, MCP inspection, and Wasm only when the application needs them.
#[cfg(feature = "controls")]
pub use wayframe_blocks as controls;
pub use wayframe_core::{Block, Context, Decision, Execution, Phase, Plan, PlanError, Reason};
#[cfg(feature = "http")]
pub use wayframe_http as http;
#[cfg(feature = "lb")]
pub use wayframe_lb as lb;
#[cfg(feature = "mcp")]
pub use wayframe_mcp as mcp;
pub use wayframe_runtime::{Generation, Live, Pipeline, PublishError};
#[cfg(feature = "transport")]
pub use wayframe_transport as transport;
#[cfg(feature = "wasm")]
pub use wayframe_wasm as wasm;
pub mod prelude {
    pub use crate::{Block, Context, Decision, Execution, Phase, Pipeline, Reason};
}
