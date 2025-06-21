use serde::{Deserialize, Serialize};

/// Policy types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PolicyCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

/// Check option types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CheckOption {
    Local,
    Cascaded,
}

/// Collation provider
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CollationProvider {
    Libc,
    Icu,
    Builtin,
}

/// Rule event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RuleEvent {
    Select,
    Update,
    Insert,
    Delete,
} 