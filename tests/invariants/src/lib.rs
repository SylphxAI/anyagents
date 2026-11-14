//! Delivery guarantees. `tests/guarantees.rs` drives the mailbox and a
//! scripted in-memory harness through crash points; [`conformance`] runs the
//! same delivery scenarios against every real harness adapter over a scripted
//! vendor process (`tests/conformance.rs`).

pub mod conformance;
