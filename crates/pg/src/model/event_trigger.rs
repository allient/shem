use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventTrigger {
    pub oid: u32,
    pub name: String,
    pub owner: String,
    /// The full, raw "CREATE EVENT TRIGGER..." statement.
    pub definition: String,
    pub comment: Option<String>,
    pub is_from_extension: bool,
}
