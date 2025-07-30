use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use shem_postgres::model::*;

pub enum Definition {
    Role(models::Role),
    Schema(models::NamedSchema),
    Relation(models::Relation),
    Type(models::Type),
    // ... etc.
}