//! Introspect command tests
//! 
//! Tests for the introspect command functionality, organized by object type.

pub mod tables;
pub mod views;
pub mod functions;
pub mod triggers;
pub mod types;
pub mod sequences;
pub mod extensions;
pub mod domains;
pub mod policies;
pub mod rules;
pub mod event_triggers;
pub mod materialized_views;
pub mod procedures;
pub mod collations;

/// Integration tests for the introspect command
pub mod integration; 