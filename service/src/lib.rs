//! Real-time streaming backend for the Teton challenge.
//!
//! Single process, event-sourced: accepted events are appended to a WAL,
//! applied in WAL order to in-memory state, and periodically snapshotted.
//! Every aggregate is a function of the *set* of accepted events keyed by
//! `ts`, so late and out-of-order events need no special fix-up logic.

pub mod api;
pub mod clock;
pub mod config;
pub mod event;
