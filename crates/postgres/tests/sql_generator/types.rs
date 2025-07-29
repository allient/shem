use shem_core::schema::{Domain, DomainConstraint, Sequence, BaseType, ArrayType, MultirangeType};
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;



#[test]
fn test_create_domain() {
    let domain = Domain {
        oid: 0,
        name: "email_address".to_string(),
        schema: "public".to_string(),
        owner: "".to_string(),
        base_type: "text".to_string(),
        collation: None,
        not_null: true,
        default: Some("'noreply@example.com'".to_string()),
        constraints: vec![
            DomainConstraint {
                oid: 0,
                name: "valid_email".to_string(),
                definition: "VALUE ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'".to_string(),
                not_valid: false,
                r#type: shem_core::schema::DomainConstraintType::Check,
            },
        ],
        acl: None,
        comment: Some("Email address domain with validation".to_string()),
        is_user_defined: true,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_domain(&domain).unwrap();
    
    assert!(result.contains("CREATE DOMAIN email_address AS text"));
    assert!(result.contains("CHECK (VALUE ~"));
    assert!(result.contains("DEFAULT 'noreply@example.com'"));
    assert!(result.contains("NOT NULL"));
}

#[test]
fn test_drop_domain() {
    let dom = Domain {
        oid: 0,
        name: "my_domain".to_string(),
        schema: "public".to_string(),
        owner: "".to_string(),
        base_type: "text".to_string(),
        collation: None,
        not_null: false,
        default: None,
        constraints: vec![],
        acl: None,
        comment: None,
        is_user_defined: true,
        is_from_extension: false,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_domain(&dom).unwrap();
    assert_eq!(sql, "DROP DOMAIN IF EXISTS my_domain CASCADE;");
}

#[test]
fn test_create_sequence() {
    let sequence = Sequence {
        name: "user_id_seq".to_string(),
        schema: Some("public".to_string()),
        data_type: "bigint".to_string(),
        start: 1,
        increment: 1,
        min_value: Some(1),
        max_value: Some(9223372036854775807),
        cache: 1,
        cycle: false,
        owned_by: Some("users.id".to_string()),
        comment: Some("User ID sequence".to_string()),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_sequence(&sequence).unwrap();
    
    assert!(result.contains("CREATE SEQUENCE user_id_seq"));
    assert!(result.contains("AS bigint"));
    assert!(result.contains("START 1"));
    assert!(result.contains("INCREMENT 1"));
    assert!(result.contains("MINVALUE 1"));
    assert!(result.contains("MAXVALUE 9223372036854775807"));
    assert!(result.contains("CACHE 1"));
    assert!(result.contains("OWNED BY users.id"));
}

#[test]
fn test_drop_sequence() {
    let seq = Sequence {
        name: "my_seq".to_string(),
        schema: None,
        data_type: "integer".to_string(),
        start: 1,
        increment: 1,
        min_value: Some(1),
        max_value: Some(2147483647),
        cache: 1,
        cycle: false,
        owned_by: None,
        comment: None,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_sequence(&seq).unwrap();
    assert_eq!(sql, "DROP SEQUENCE IF EXISTS my_seq CASCADE;");
}

#[test]
fn test_alter_sequence() {
    let old_sequence = Sequence {
        name: "user_id_seq".to_string(),
        schema: Some("public".to_string()),
        data_type: "integer".to_string(),
        start: 1,
        increment: 1,
        min_value: Some(1),
        max_value: Some(2147483647),
        cache: 1,
        cycle: false,
        owned_by: Some("users.id".to_string()),
        comment: None,
    };

    let new_sequence = Sequence {
        name: "user_id_seq".to_string(),
        schema: Some("public".to_string()),
        data_type: "bigint".to_string(),
        start: 1000,
        increment: 2,
        min_value: Some(1000),
        max_value: Some(9223372036854775807),
        cache: 10,
        cycle: true,
        owned_by: Some("users.id".to_string()),
        comment: Some("Updated user ID sequence".to_string()),
    };

    let generator = PostgresSqlGenerator;
    let (up_statements, down_statements) = generator.alter_sequence(&old_sequence, &new_sequence).unwrap();
    
    assert!(!up_statements.is_empty());
    assert!(!down_statements.is_empty());
    
    let up_sql = up_statements.join("; ");
    assert!(up_sql.contains("ALTER SEQUENCE user_id_seq"));
    assert!(up_sql.contains("RESTART WITH 1000"));
    assert!(up_sql.contains("INCREMENT BY 2"));
    assert!(up_sql.contains("SET MINVALUE 1000"));
    assert!(up_sql.contains("SET MAXVALUE 9223372036854775807"));
    assert!(up_sql.contains("CACHE 10"));
    assert!(up_sql.contains("CYCLE"));
}

#[test]
fn test_generate_create_base_type() {
    let type_def = BaseType {
        name: "custom_int".to_string(),
        schema: None,
        internal_length: None,
        is_passed_by_value: false,
        alignment: "int4".to_string(),
        storage: "plain".to_string(),
        category: Some("N".to_string()),
        preferred: false,
        default: Some("0".to_string()),
        element: Some("integer".to_string()),
        delimiter: Some(",".to_string()),
        collatable: false,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_base_type(&type_def).unwrap();
    
    assert_eq!(result, "CREATE TYPE custom_int AS (ALIGNMENT = int4, STORAGE = plain, CATEGORY = 'N', DEFAULT = 0, ELEMENT = integer, DELIMITER = ',');");
}

#[test]
fn test_generate_create_base_type_default() {
    let type_def = BaseType {
        name: "custom_int".to_string(),
        schema: None,
        internal_length: None,
        is_passed_by_value: false,
        alignment: "int4".to_string(),
        storage: "plain".to_string(),
        category: Some("N".to_string()),
        preferred: false,
        default: None,
        element: Some("integer".to_string()),
        delimiter: Some(",".to_string()),
        collatable: false,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_base_type(&type_def).unwrap();
    
    assert_eq!(result, "CREATE TYPE custom_int AS (ALIGNMENT = int4, STORAGE = plain, CATEGORY = 'N', ELEMENT = integer, DELIMITER = ',');");
}

#[test]
fn test_generate_create_array_type() {
    let type_def = ArrayType {
        name: "int_array".to_string(),
        schema: None,
        element_type: "integer".to_string(),
        element_schema: None,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_array_type(&type_def).unwrap();
    
    assert_eq!(result, "CREATE TYPE int_array AS ARRAY OF integer;");
}

#[test]
fn test_generate_create_multirange_type() {
    let type_def = MultirangeType {
        name: "int_multirange".to_string(),
        schema: None,
        range_type: "int4range".to_string(),
        range_schema: None,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_multirange_type(&type_def).unwrap();
    
    assert_eq!(result, "CREATE TYPE int_multirange AS MULTIRANGE OF int4range;");
}

#[test]
fn test_drop_base_type() {
    let base_type = BaseType {
        name: "custom_int".to_string(),
        schema: Some("public".to_string()),
        internal_length: None,
        is_passed_by_value: false,
        alignment: "int4".to_string(),
        storage: "plain".to_string(),
        category: Some("N".to_string()),
        preferred: false,
        default: Some("0".to_string()),
        element: Some("integer".to_string()),
        delimiter: Some(",".to_string()),
        collatable: false,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_base_type(&base_type).unwrap();
    
    assert_eq!(result, "DROP TYPE IF EXISTS public.custom_int CASCADE;");
}

#[test]
fn test_drop_array_type() {
    let array_type = ArrayType {
        name: "int_array".to_string(),
        schema: Some("public".to_string()),
        element_type: "integer".to_string(),
        element_schema: None,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_array_type(&array_type).unwrap();
    
    assert_eq!(result, "DROP TYPE IF EXISTS public.int_array CASCADE;");
}

#[test]
fn test_drop_multirange_type() {
    let multirange_type = MultirangeType {
        name: "int_multirange".to_string(),
        schema: Some("public".to_string()),
        range_type: "int4range".to_string(),
        range_schema: None,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_multirange_type(&multirange_type).unwrap();
    
    assert_eq!(result, "DROP TYPE IF EXISTS public.int_multirange CASCADE;");
} 