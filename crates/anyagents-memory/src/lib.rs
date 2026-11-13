//! Retrieval over raw agent history beyond lexical search: dense candidates
//! from a local multilingual embedder, fused with BM25 by reciprocal rank and
//! reordered by a local cross-encoder. Every result cites the exact raw bytes
//! it came from; raw history stays the only source of truth.
//!
//! The kernel links only [`protocol`], [`fusion`] and [`text`]. The model
//! runtime, the dense index and the offline evaluation are the `service`
//! feature, run as its own process (`anyagents memory-service`).
pub mod fusion;
pub mod protocol;
pub mod text;

#[cfg(feature = "service")]
pub mod dense;
#[cfg(feature = "service")]
pub mod embed;
#[cfg(feature = "service")]
pub mod eval;
#[cfg(feature = "service")]
pub mod model;
#[cfg(feature = "service")]
pub mod pins;
#[cfg(feature = "service")]
pub mod runtime;
#[cfg(feature = "service")]
pub mod service;
