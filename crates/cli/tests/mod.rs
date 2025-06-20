//! CLI command tests
//! 
//! This module contains comprehensive tests for all CLI commands.
//! Each command has its own test module with isolated tests for different scenarios.

pub mod introspect;
pub mod init;
pub mod diff;
pub mod migrate;
pub mod validate;
pub mod inspect;

/// Common test utilities and helpers
pub mod common;

/// Test fixtures and mock data
pub mod fixtures; 