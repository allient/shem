use serde::{Deserialize, Serialize};
use super::expressions::Expression;

/// Table constraint types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TableConstraint {
    PrimaryKey {
        columns: Vec<String>,
        name: Option<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        references: ForeignKeyReference,
        name: Option<String>,
    },
    Unique {
        columns: Vec<String>,
        name: Option<String>,
    },
    Check {
        expression: Expression,
        name: Option<String>,
    },
    Exclusion {
        elements: Vec<ExclusionElement>,
        using: String,
        name: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForeignKeyReference {
    pub table: String,
    pub columns: Vec<String>,
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExclusionElement {
    pub expression: Expression,
    pub operator: String,
    pub order: Option<SortOrder>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
} 