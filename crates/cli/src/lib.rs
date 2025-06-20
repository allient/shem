//! Shem CLI library
//! 
//! This library provides the core functionality for the Shem CLI tool.

pub mod commands;
pub mod config;

// Re-export main types for convenience
pub use config::Config; 