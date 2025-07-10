//! Integration tests for the Shem CLI
//!
//! This file contains the main integration tests that can be run with `cargo test`.

mod common;
mod fixtures;
mod introspect;

use anyhow::Result;
use common::{TestEnv, cli, db};
use fixtures::sql;

