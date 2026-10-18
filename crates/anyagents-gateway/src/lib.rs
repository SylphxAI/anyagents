//! Loopback protocol gateway between harness APIs and model providers.
//!
//! A route carries the API key stored for that model provider. The gateway
//! never accepts, stores or forwards a vendor subscription credential.

mod canonical;
mod error;
mod route;
mod router;
mod sse;
mod stream;
mod usage;

pub use canonical::{
    Completion, Request, StopReason, encode_request, encode_response, parse_request, parse_response,
};
pub use error::Fail;
pub use route::{MemoryRoutes, Protocol, ProviderRoute, RouteResolver, RouteTable};
pub use router::router;
pub use usage::{MemoryUsage, UsageEvent, UsageSink};
