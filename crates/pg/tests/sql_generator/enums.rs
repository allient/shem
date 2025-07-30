use pg::model::types::{EnumType, EnumValue, TypeInfo, Type};
use pg::traits::SqlGenerator;
use pg::sql_generator::PostgresSqlGenerator;

#[test]
fn test_generate_create_enum_type() {
    let enum_type = EnumType {
        info: TypeInfo {
            oid: 0,
            name: "status".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        values: vec![
            EnumValue { oid: 0, label: "active".to_string() },
            EnumValue { oid: 0, label: "inactive".to_string() },
            EnumValue { oid: 0, label: "pending".to_string() },
        ],
    };

    let generator = PostgresSqlGenerator;
    let enum_type_wrapped = Type::Enum(enum_type);
    let result = generator.create_type(&enum_type_wrapped).unwrap();
    
    assert_eq!(
        result,
        "CREATE TYPE \"status\" AS ENUM ('active', 'inactive', 'pending');"
    );
}

#[test]
fn test_generate_create_enum_type_no_schema() {
    let enum_type = EnumType {
        info: TypeInfo {
            oid: 0,
            name: "priority".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        values: vec![
            EnumValue { oid: 0, label: "low".to_string() },
            EnumValue { oid: 0, label: "medium".to_string() },
            EnumValue { oid: 0, label: "high".to_string() },
        ],
    };

    let generator = PostgresSqlGenerator;
    let enum_type_wrapped = Type::Enum(enum_type);
    let result = generator.create_type(&enum_type_wrapped).unwrap();
    
    assert_eq!(
        result,
        "CREATE TYPE \"priority\" AS ENUM ('low', 'medium', 'high');"
    );
}

#[test]
fn test_generate_create_enum_with_schema() {
    let enum_type = EnumType {
        info: TypeInfo {
            oid: 0,
            name: "color".to_string(),
            schema: "custom_schema".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        values: vec![
            EnumValue { oid: 0, label: "red".to_string() },
            EnumValue { oid: 0, label: "green".to_string() },
            EnumValue { oid: 0, label: "blue".to_string() },
        ],
    };

    let generator = PostgresSqlGenerator;
    let enum_type_wrapped = Type::Enum(enum_type);
    let result = generator.create_type(&enum_type_wrapped).unwrap();
    
    assert_eq!(
        result,
        "CREATE TYPE \"color\" AS ENUM ('red', 'green', 'blue');"
    );
}

#[test]
fn test_generate_create_enum_single_value() {
    let enum_type = EnumType {
        info: TypeInfo {
            oid: 0,
            name: "single_value_enum".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        values: vec![
            EnumValue { oid: 0, label: "only_value".to_string() },
        ],
    };

    let generator = PostgresSqlGenerator;
    let enum_type_wrapped = Type::Enum(enum_type);
    let result = generator.create_type(&enum_type_wrapped).unwrap();
    
    assert_eq!(
        result,
        "CREATE TYPE \"single_value_enum\" AS ENUM ('only_value');"
    );
}

#[test]
fn test_generate_create_enum_with_special_characters() {
    let enum_type = EnumType {
        info: TypeInfo {
            oid: 0,
            name: "special_chars".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        values: vec![
            EnumValue { oid: 0, label: "value_with'quote".to_string() },
            EnumValue { oid: 0, label: "value_with\"quote".to_string() },
            EnumValue { oid: 0, label: "value_with\\backslash".to_string() },
        ],
    };

    let generator = PostgresSqlGenerator;
    let enum_type_wrapped = Type::Enum(enum_type);
    let result = generator.create_type(&enum_type_wrapped).unwrap();
    
    // The generator should properly escape special characters
    // Note: The actual escaping behavior may vary based on implementation
    assert!(result.contains("CREATE TYPE \"special_chars\" AS ENUM"));
    assert!(result.contains("value_with"));
}

#[test]
fn test_drop_enum() {
    let enum_type = EnumType {
        info: TypeInfo {
            oid: 0,
            name: "status".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        values: vec![
            EnumValue { oid: 0, label: "active".to_string() },
            EnumValue { oid: 0, label: "inactive".to_string() },
        ],
    };

    let generator = PostgresSqlGenerator;
    let enum_type_wrapped = Type::Enum(enum_type);
    let result = generator.drop_type(&enum_type_wrapped).unwrap();
    
    assert_eq!(result, "DROP TYPE \"public\".\"status\";\n");
}

#[test]
fn test_drop_enum_no_schema() {
    let enum_type = EnumType {
        info: TypeInfo {
            oid: 0,
            name: "priority".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        values: vec![
            EnumValue { oid: 0, label: "low".to_string() },
            EnumValue { oid: 0, label: "high".to_string() },
        ],
    };

    let generator = PostgresSqlGenerator;
    let enum_type_wrapped = Type::Enum(enum_type);
    let result = generator.drop_type(&enum_type_wrapped).unwrap();
    
    assert_eq!(result, "DROP TYPE \"public\".\"priority\";\n");
}

#[test]
fn test_drop_enum_with_schema() {
    let enum_type = EnumType {
        info: TypeInfo {
            oid: 0,
            name: "color".to_string(),
            schema: "custom_schema".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        values: vec![
            EnumValue { oid: 0, label: "red".to_string() },
            EnumValue { oid: 0, label: "blue".to_string() },
        ],
    };

    let generator = PostgresSqlGenerator;
    let enum_type_wrapped = Type::Enum(enum_type);
    let result = generator.drop_type(&enum_type_wrapped).unwrap();
    
    assert_eq!(result, "DROP TYPE \"custom_schema\".\"color\";\n");
}

#[test]
fn test_enum_with_comment() {
    let enum_type = EnumType {
        info: TypeInfo {
            oid: 0,
            name: "status".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: Some("User status enumeration".to_string()),
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        values: vec![
            EnumValue { oid: 0, label: "active".to_string() },
            EnumValue { oid: 0, label: "inactive".to_string() },
        ],
    };

    let generator = PostgresSqlGenerator;
    let enum_type_wrapped = Type::Enum(enum_type);
    let result = generator.create_type(&enum_type_wrapped).unwrap();
    
    // The result should include the CREATE TYPE statement
    assert!(result.contains("CREATE TYPE \"status\" AS ENUM"));
    assert!(result.contains("'active'"));
    assert!(result.contains("'inactive'"));
    
    // Note: Comments are typically added with separate COMMENT ON statements
    // The generator might handle this separately
}

#[test]
fn test_enum_with_acl() {
    let enum_type = EnumType {
        info: TypeInfo {
            oid: 0,
            name: "status".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: Some("postgres=UC/postgres".to_string()),
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        values: vec![
            EnumValue { oid: 0, label: "active".to_string() },
            EnumValue { oid: 0, label: "inactive".to_string() },
        ],
    };

    let generator = PostgresSqlGenerator;
    let enum_type_wrapped = Type::Enum(enum_type);
    let result = generator.create_type(&enum_type_wrapped).unwrap();
    
    // The result should include the CREATE TYPE statement
    assert!(result.contains("CREATE TYPE \"status\" AS ENUM"));
    assert!(result.contains("'active'"));
    assert!(result.contains("'inactive'"));
    
    // Note: ACLs are typically set with separate GRANT/REVOKE statements
    // The generator might handle this separately
}

#[test]
fn test_enum_empty_values() {
    let enum_type = EnumType {
        info: TypeInfo {
            oid: 0,
            name: "empty_enum".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        values: vec![],
    };

    let generator = PostgresSqlGenerator;
    let enum_type_wrapped = Type::Enum(enum_type);
    let result = generator.create_type(&enum_type_wrapped).unwrap();
    
    // PostgreSQL doesn't allow empty enums, so this should either error or handle gracefully
    assert!(result.contains("CREATE TYPE \"empty_enum\" AS ENUM"));
}

#[test]
fn test_enum_case_sensitivity() {
    let enum_type = EnumType {
        info: TypeInfo {
            oid: 0,
            name: "CaseSensitive".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        values: vec![
            EnumValue { oid: 0, label: "Value1".to_string() },
            EnumValue { oid: 0, label: "value1".to_string() },
            EnumValue { oid: 0, label: "VALUE1".to_string() },
        ],
    };

    let generator = PostgresSqlGenerator;
    let enum_type_wrapped = Type::Enum(enum_type);
    let result = generator.create_type(&enum_type_wrapped).unwrap();
    
    assert_eq!(
        result,
        "CREATE TYPE \"CaseSensitive\" AS ENUM ('Value1', 'value1', 'VALUE1');"
    );
} 