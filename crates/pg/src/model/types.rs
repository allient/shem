use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Attribute {
    pub name: String,
    pub type_name: String,         // Fully formatted type
    pub collation: Option<String>, // Fully qualified collation name
}

// Common information for all types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypeInfo {
    pub oid: u32,
    pub name: String,
    pub schema: String,
    pub owner: String,
    pub acl: Option<String>,
    pub comment: Option<String>,
    pub is_user_defined: bool,
    pub is_from_extension: bool,
    pub array_type_oid: Option<u32>,
}

// The specific struct for a pseudo-type.
// It might not even need any fields beyond the common ones.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PseudoType {
    pub info: TypeInfo,
    // You could add specific flags here if needed, but it's often unnecessary.
    // For example, is_polymorphic: bool,
}

// Base Type ('b')
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaseType {
    pub info: TypeInfo,
    pub internal_length: i16,
    pub is_passed_by_value: bool,
    pub alignment: char,
    pub storage: char,
    pub category: char,
    pub is_preferred: bool,
    pub default_value: Option<String>,
    pub element_type_oid: Option<u32>, // 0 if not an array type
    pub delimiter: char,
    pub is_collatable: bool,
    // I/O functions are critical for CREATE TYPE
    pub input_fn: String,
    pub output_fn: String,
    pub receive_fn: Option<String>,
    pub send_fn: Option<String>,
    pub typmod_in_fn: Option<String>,
    pub typmod_out_fn: Option<String>,
    pub analyze_fn: Option<String>,
}

// Composite Type ('c')
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompositeType {
    pub info: TypeInfo,
    pub attributes: Vec<Attribute>,
    pub class_oid: u32, // OID of the backing pg_class entry
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Type {
    Base(BaseType),
    Composite(CompositeType),
    Domain(Domain),
    Enum(EnumType),
    Range(RangeType),
    Pseudo(PseudoType), // For things like 'any', 'void', etc. // Array and Multirange types are properties of other types, not distinct kinds.
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DomainConstraint {
    pub oid: u32,
    pub name: String,
    pub definition: String, // The full CHECK (...) text
    pub not_valid: bool,
}

// Domain Type ('d')
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Domain {
    pub info: TypeInfo,
    pub base_type: String,         // Fully formatted base type
    pub collation: Option<String>, // Fully qualified collation name
    pub not_null: bool,
    pub default: Option<String>,
    pub constraints: Vec<DomainConstraint>,
}

// Enum Type ('e')
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnumType {
    pub info: TypeInfo,
    pub values: Vec<EnumValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnumValue {
    pub oid: u32,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RangeType {
    /// Common information shared by all types (OID, name, schema, etc.).
    pub info: TypeInfo,
    pub subtype: String,
    pub subtype_opclass: String,
    pub collation: Option<String>,
    pub canonical_fn: Option<String>,
    pub subtype_diff_fn: Option<String>,
    pub multirange_type_oid: Option<u32>,
}
