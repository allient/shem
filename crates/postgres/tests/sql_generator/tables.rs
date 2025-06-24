use shem_core::schema::{Table, Column, Constraint, ConstraintKind, Identity, GeneratedColumn};
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;

#[test]
fn test_generate_create_table() {
    let table = Table {
        name: "users".to_string(),
        schema: None,
        columns: vec![
            Column {
                name: "id".to_string(),
                type_name: "SERIAL".to_string(),
                nullable: false,
                default: None,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "default".to_string(),
                type_name: "VARCHAR(100)".to_string(),
                nullable: false,
                default: None,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "email".to_string(),
                type_name: "VARCHAR(255)".to_string(),
                nullable: true,
                default: None,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
        ],
        constraints: vec![
            Constraint {
                name: "users_pkey".to_string(),
                kind: ConstraintKind::PrimaryKey,
                definition: "PRIMARY KEY (id)".to_string(),
                deferrable: false,
                initially_deferred: false,
            },
            Constraint {
                name: "users_email_key".to_string(),
                kind: ConstraintKind::Unique,
                definition: "UNIQUE (email)".to_string(),
                deferrable: false,
                initially_deferred: false,
            },
        ],
        indexes: vec![],
        comment: None,
        tablespace: None,
        inherits: vec![],
        partition_by: None,
        storage_parameters: std::collections::HashMap::new(),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_table(&table).unwrap();
    
    assert!(result.contains("CREATE TABLE users"));
    assert!(result.contains("id SERIAL NOT NULL"));
    assert!(result.contains("\"default\" VARCHAR(100) NOT NULL"));
    assert!(result.contains("email VARCHAR(255)"));
    assert!(result.contains("PRIMARY KEY (id)"));
    assert!(result.contains("UNIQUE (email)"));
}

#[test]
fn test_generate_alter_table() {
    use shem_core::schema::{Table, Column, Constraint, ConstraintKind, Identity, GeneratedColumn};
    
    // Old table with some columns and constraints
    let old_table = Table {
        name: "users".to_string(),
        schema: None,
        columns: vec![
            Column {
                name: "id".to_string(),
                type_name: "INTEGER".to_string(),
                nullable: false,
                default: Some("1".to_string()),
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "name".to_string(),
                type_name: "VARCHAR(50)".to_string(),
                nullable: false,
                default: None,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "email".to_string(),
                type_name: "TEXT".to_string(),
                nullable: true,
                default: None,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "to_drop".to_string(),
                type_name: "TEXT".to_string(),
                nullable: false,
                default: None,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
        ],
        constraints: vec![
            Constraint {
                name: "to_drop_constraint".to_string(),
                kind: ConstraintKind::Unique,
                definition: "UNIQUE (email)".to_string(),
                deferrable: false,
                initially_deferred: false,
            },
        ],
        indexes: vec![],
        comment: None,
        tablespace: None,
        inherits: vec![],
        partition_by: None,
        storage_parameters: std::collections::HashMap::new(),
    };

    // New table with modified columns and constraints
    let new_table = Table {
        name: "users".to_string(),
        schema: None,
        columns: vec![
            Column {
                name: "id".to_string(),
                type_name: "BIGINT".to_string(),
                nullable: false,
                default: None,
                identity: Some(Identity {
                    always: true,
                    start: 1,
                    increment: 1,
                    min_value: Some(1),
                    max_value: Some(9223372036854775807),
                    cache: Some(1),
                    cycle: false,
                }),
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "name".to_string(),
                type_name: "VARCHAR(100)".to_string(),
                nullable: true,
                default: Some("'Unknown'".to_string()),
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "email".to_string(),
                type_name: "TEXT".to_string(),
                nullable: true,
                default: None,
                identity: None,
                generated: Some(GeneratedColumn {
                    expression: "LOWER(email)".to_string(),
                    stored: true,
                }),
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "new_column".to_string(),
                type_name: "TIMESTAMP".to_string(),
                nullable: false,
                default: Some("NOW()".to_string()),
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
        ],
        constraints: vec![
            Constraint {
                name: "new_constraint".to_string(),
                kind: ConstraintKind::Check,
                definition: "CHECK (LENGTH(name) > 0)".to_string(),
                deferrable: false,
                initially_deferred: false,
            },
        ],
        indexes: vec![],
        comment: None,
        tablespace: None,
        inherits: vec![],
        partition_by: None,
        storage_parameters: std::collections::HashMap::new(),
    };

    let generator = PostgresSqlGenerator;
    let (up_statements, down_statements) = generator.generate_alter_table(&old_table, &new_table).unwrap();
    
    assert!(!up_statements.is_empty());
    assert!(!down_statements.is_empty());
    
    let up_sql = up_statements.join("; ");
    assert!(up_sql.contains("ALTER TABLE users DROP COLUMN to_drop"));
    assert!(up_sql.contains("ALTER TABLE users ADD COLUMN new_column TIMESTAMP NOT NULL DEFAULT NOW()"));
    assert!(up_sql.contains("ALTER TABLE users ALTER COLUMN name TYPE VARCHAR(100)"));
    assert!(up_sql.contains("ALTER TABLE users ALTER COLUMN name DROP NOT NULL"));
    assert!(up_sql.contains("ALTER TABLE users ALTER COLUMN name SET DEFAULT 'Unknown'"));
    assert!(up_sql.contains("ALTER TABLE users ALTER COLUMN id TYPE BIGINT"));
    assert!(up_sql.contains("ALTER TABLE users ALTER COLUMN id DROP DEFAULT"));
    assert!(up_sql.contains("ALTER TABLE users ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY"));
    assert!(up_sql.contains("ALTER TABLE users ALTER COLUMN email SET GENERATED ALWAYS AS (LOWER(email)) STORED"));
    assert!(up_sql.contains("ALTER TABLE users DROP CONSTRAINT to_drop_constraint"));
    assert!(up_sql.contains("ALTER TABLE users ADD CONSTRAINT new_constraint CHECK (LENGTH(name) > 0)"));
} 