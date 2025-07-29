use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub mod error;
pub mod schema;
pub mod traits;

pub use error::{Error, Result};
