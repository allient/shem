//! Shem CLI library
//! 
//! This library provides the core functionality for the Shem CLI tool.

pub mod commands;
pub mod config;
pub mod cli_util;

// Re-export main types for convenience
pub use config::Config;
pub use cli_util::{TestEnv, db, cli::{self, run_shem_command_in_dir, assert_command_success}};
