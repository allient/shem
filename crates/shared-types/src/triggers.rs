use serde::{Deserialize, Serialize};

/// Trigger types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TriggerWhen {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// Event trigger types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EventTriggerEvent {
    DdlCommandStart,
    DdlCommandEnd,
    TableRewrite,
    SqlDrop,
} 