use postgres::PostgresSqlGenerator;
use shem_core::schema::{Type, TypeInfo, Domain, BaseType, CompositeType, RangeType, PseudoType};
use shem_core::traits::SqlGenerator;

#[test]
fn test_create_domain() {
    let generator = PostgresSqlGenerator;
    
    let domain = Domain {
        info: TypeInfo {
            oid: 0,
            name: "email_address".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: Some("Email address domain with validation".to_string()),
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        base_type: "text".to_string(),
        collation: None,
        not_null: false,
        default: None,
        constraints: vec![
            shem_core::schema::DomainConstraint {
                oid: 0,
                name: "email_check".to_string(),
                definition: "CHECK (VALUE ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')".to_string(),
                not_valid: false,
            }
        ],
    };

    let result = generator.create_type(&Type::Domain(domain)).unwrap();
    assert!(result.contains("CREATE DOMAIN"));
    assert!(result.contains("email_address"));
    assert!(result.contains("CHECK"));
}

#[test]
fn test_drop_domain() {
    let generator = PostgresSqlGenerator;
    
    let domain = Domain {
        info: TypeInfo {
            oid: 0,
            name: "my_domain".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        base_type: "integer".to_string(),
        collation: None,
        not_null: false,
        default: None,
        constraints: vec![],
    };

    let sql = generator.drop_type(&Type::Domain(domain)).unwrap();
    assert!(sql.contains("DROP DOMAIN"));
    assert!(sql.contains("my_domain"));
}

#[test]
fn test_create_base_type() {
    let generator = PostgresSqlGenerator;
    
    let base_type = BaseType {
        info: TypeInfo {
            oid: 0,
            name: "custom_int".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        internal_length: 4,
        is_passed_by_value: false,
        alignment: 'i',
        storage: 'p',
        category: 'N',
        is_preferred: false,
        default_value: Some("0".to_string()),
        element_type_oid: Some(23), // integer
        delimiter: ',',
        is_collatable: false,
        input_fn: "int4in".to_string(),
        output_fn: "int4out".to_string(),
        receive_fn: Some("int4recv".to_string()),
        send_fn: Some("int4send".to_string()),
        typmod_in_fn: None,
        typmod_out_fn: None,
        analyze_fn: None,
    };

    let result = generator.create_type(&Type::Base(base_type)).unwrap();
    assert!(result.contains("CREATE TYPE"));
    assert!(result.contains("custom_int"));
}

#[test]
fn test_create_composite_type() {
    let generator = PostgresSqlGenerator;
    
    let composite_type = CompositeType {
        info: TypeInfo {
            oid: 0,
            name: "point".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        attributes: vec![
            shem_core::schema::Attribute {
                name: "x".to_string(),
                type_name: "integer".to_string(),
                collation: None,
            },
            shem_core::schema::Attribute {
                name: "y".to_string(),
                type_name: "integer".to_string(),
                collation: None,
            },
        ],
        class_oid: 0,
    };

    let result = generator.create_type(&Type::Composite(composite_type)).unwrap();
    assert!(result.contains("CREATE TYPE"));
    assert!(result.contains("point"));
    assert!(result.contains("x"));
    assert!(result.contains("y"));
}

#[test]
fn test_create_range_type() {
    let generator = PostgresSqlGenerator;
    
    let range_type = RangeType {
        info: TypeInfo {
            oid: 0,
            name: "int_range".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        subtype: "integer".to_string(),
        subtype_opclass: "int4_ops".to_string(),
        collation: None,
        canonical_fn: None,
        subtype_diff_fn: None,
        multirange_type_oid: None,
    };

    let result = generator.create_type(&Type::Range(range_type)).unwrap();
    assert!(result.contains("CREATE TYPE"));
    assert!(result.contains("int_range"));
    assert!(result.contains("integer"));
}

#[test]
fn test_create_pseudo_type() {
    let generator = PostgresSqlGenerator;
    
    let pseudo_type = PseudoType {
        info: TypeInfo {
            oid: 0,
            name: "any".to_string(),
            schema: "pg_catalog".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: false,
            is_from_extension: false,
            array_type_oid: None,
        },
    };

    let result = generator.create_type(&Type::Pseudo(pseudo_type)).unwrap();
    assert!(result.contains("CREATE TYPE"));
    assert!(result.contains("any"));
}

#[test]
fn test_drop_base_type() {
    let generator = PostgresSqlGenerator;
    
    let base_type = BaseType {
        info: TypeInfo {
            oid: 0,
            name: "custom_int".to_string(),
            schema: "public".to_string(),
            owner: "".to_string(),
            acl: None,
            comment: None,
            is_user_defined: true,
            is_from_extension: false,
            array_type_oid: None,
        },
        internal_length: 4,
        is_passed_by_value: false,
        alignment: 'i',
        storage: 'p',
        category: 'N',
        is_preferred: false,
        default_value: Some("0".to_string()),
        element_type_oid: Some(23),
        delimiter: ',',
        is_collatable: false,
        input_fn: "int4in".to_string(),
        output_fn: "int4out".to_string(),
        receive_fn: Some("int4recv".to_string()),
        send_fn: Some("int4send".to_string()),
        typmod_in_fn: None,
        typmod_out_fn: None,
        analyze_fn: None,
    };

    let result = generator.drop_type(&Type::Base(base_type)).unwrap();
    assert!(result.contains("DROP TYPE"));
    assert!(result.contains("custom_int"));
    assert!(result.contains("CASCADE"));
} 