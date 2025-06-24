use shem_core::schema::Sequence;
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;

#[test]
fn test_create_sequence_basic() {
    let sequence = Sequence {
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
    let result = generator.create_sequence(&sequence).unwrap();
    
    assert!(result.contains("CREATE SEQUENCE my_seq"));
    assert!(result.contains("AS integer"));
    assert!(result.contains("START 1"));
    assert!(result.contains("INCREMENT 1"));
    assert!(result.contains("MINVALUE 1"));
    assert!(result.contains("MAXVALUE 2147483647"));
    assert!(result.contains("CACHE 1"));
    assert!(!result.contains("CYCLE"));
}

#[test]
fn test_create_sequence_with_owned_by() {
    let sequence = Sequence {
        name: "user_id_seq".to_string(),
        schema: Some("public".to_string()),
        data_type: "bigint".to_string(),
        start: 1000,
        increment: 5,
        min_value: Some(1000),
        max_value: Some(9223372036854775807),
        cache: 10,
        cycle: true,
        owned_by: Some("users.id".to_string()),
        comment: Some("User ID sequence".to_string()),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_sequence(&sequence).unwrap();
    
    assert!(result.contains("CREATE SEQUENCE user_id_seq"));
    assert!(result.contains("AS bigint"));
    assert!(result.contains("START 1000"));
    assert!(result.contains("INCREMENT 5"));
    assert!(result.contains("MINVALUE 1000"));
    assert!(result.contains("MAXVALUE 9223372036854775807"));
    assert!(result.contains("CACHE 10"));
    assert!(result.contains("CYCLE"));
    assert!(result.contains("OWNED BY users.id"));
}

#[test]
fn test_create_sequence_no_limits() {
    let sequence = Sequence {
        name: "unlimited_seq".to_string(),
        schema: None,
        data_type: "bigint".to_string(),
        start: 1,
        increment: 1,
        min_value: None,
        max_value: None,
        cache: 1,
        cycle: false,
        owned_by: None,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_sequence(&sequence).unwrap();
    
    assert!(result.contains("CREATE SEQUENCE unlimited_seq"));
    assert!(result.contains("AS bigint"));
    assert!(result.contains("START 1"));
    assert!(result.contains("INCREMENT 1"));
    assert!(!result.contains("MINVALUE"));
    assert!(!result.contains("MAXVALUE"));
    assert!(result.contains("CACHE 1"));
    assert!(!result.contains("CYCLE"));
}

#[test]
fn test_drop_sequence() {
    let sequence = Sequence {
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
    let result = generator.drop_sequence(&sequence).unwrap();
    
    assert_eq!(result, "DROP SEQUENCE IF EXISTS my_seq CASCADE;");
}

#[test]
fn test_drop_sequence_with_schema() {
    let sequence = Sequence {
        name: "my_seq".to_string(),
        schema: Some("public".to_string()),
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
    let result = generator.drop_sequence(&sequence).unwrap();
    
    assert_eq!(result, "DROP SEQUENCE IF EXISTS public.my_seq CASCADE;");
}

#[test]
fn test_alter_sequence_change_all_properties() {
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
fn test_alter_sequence_no_changes() {
    let sequence = Sequence {
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

    let generator = PostgresSqlGenerator;
    let (up_statements, down_statements) = generator.alter_sequence(&sequence, &sequence).unwrap();
    
    assert!(up_statements.is_empty());
    assert!(down_statements.is_empty());
}

#[test]
fn test_alter_sequence_remove_limits() {
    let old_sequence = Sequence {
        name: "limited_seq".to_string(),
        schema: None,
        data_type: "integer".to_string(),
        start: 1,
        increment: 1,
        min_value: Some(1),
        max_value: Some(1000),
        cache: 1,
        cycle: false,
        owned_by: None,
        comment: None,
    };

    let new_sequence = Sequence {
        name: "limited_seq".to_string(),
        schema: None,
        data_type: "integer".to_string(),
        start: 1,
        increment: 1,
        min_value: None,
        max_value: None,
        cache: 1,
        cycle: false,
        owned_by: None,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let (up_statements, _) = generator.alter_sequence(&old_sequence, &new_sequence).unwrap();
    
    assert!(!up_statements.is_empty());
    let up_sql = up_statements.join("; ");
    assert!(up_sql.contains("ALTER SEQUENCE limited_seq"));
    assert!(up_sql.contains("SET NO MINVALUE"));
    assert!(up_sql.contains("SET NO MAXVALUE"));
} 