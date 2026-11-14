//! Retrieval over raw agent history beyond lexical search: dense candidates
//! from an embedder, fused with BM25 by reciprocal rank and reordered by a
//! cross-encoder. Both run locally on pinned models by default; either can be
//! a remote model bound to an AnyAgents profile (`providers`). Every result
//! cites the exact raw bytes it came from; raw history stays the only source
//! of truth.
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
pub mod providers;
#[cfg(feature = "service")]
pub mod runtime;
#[cfg(feature = "service")]
pub mod service;

/// A temporary directory under the physical temporary root. Raw reads refuse
/// a path through a symbolic link, and macOS keeps its temporary directory
/// behind one (`/var` is `/private/var`).
#[cfg(test)]
pub(crate) fn test_tempdir() -> tempfile::TempDir {
    tempfile::tempdir_in(
        std::env::temp_dir()
            .canonicalize()
            .expect("physical temporary root"),
    )
    .expect("owned temporary fixture")
}
