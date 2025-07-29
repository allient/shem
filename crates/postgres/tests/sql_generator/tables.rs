use postgres::PostgresSqlGenerator;
use shem_core::schema::{Column, Constraint, ConstraintKind, Table, ColumnStorage, ReplicaIdentity, Identity, Generated, IdentityGeneration};
use shem_core::traits::SqlGenerator;

#[test]
fn test_generate_create_table() {
    let table = Table {
        oid: 0,
        name: "users".to_string(),
        schema: "public".to_string(),
        owner: "".to_string(),
        columns: vec![
            Column {
                name: "id".to_string(),
                type_name: "SERIAL".to_string(),
                is_not_null: true,
                has_default: false,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: ColumnStorage::Plain,
                compression: None,
                acl: None,
                is_dropped: false,
                is_local: true,
                stats_target: None,
                fdw_options: std::collections::HashMap::new(),
            },
            Column {
                name: "default".to_string(),
                type_name: "VARCHAR(100)".to_string(),
                is_not_null: true,
                has_default: false,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: ColumnStorage::Plain,
                compression: None,
                acl: None,
                is_dropped: false,
                is_local: true,
                stats_target: None,
                fdw_options: std::collections::HashMap::new(),
            },
            Column {
                name: "email".to_string(),
                type_name: "VARCHAR(255)".to_string(),
                is_not_null: false,
                has_default: false,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: ColumnStorage::Plain,
                compression: None,
                acl: None,
                is_dropped: false,
                is_local: true,
                stats_target: None,
                fdw_options: std::collections::HashMap::new(),
            },
        ],
        constraints: vec![
            Constraint {
                oid: 0,
                name: "users_pkey".to_string(),
                table_oid: 0,
                definition: "PRIMARY KEY (id)".to_string(),
                r#type: shem_core::schema::ConstraintType::PrimaryKey,
                foreign_table_oid: None,
                foreign_key_columns: Vec::new(),
                primary_key_columns: Vec::new(),
                on_update: shem_core::schema::ReferentialAction::NoAction,
                on_delete: shem_core::schema::ReferentialAction::NoAction,
                is_deferrable: false,
                is_initially_deferred: false,
                is_not_valid: false,
            },
            Constraint {
                oid: 0,
                name: "users_email_key".to_string(),
                table_oid: 0,
                definition: "UNIQUE (email)".to_string(),
                r#type: shem_core::schema::ConstraintType::Unique,
                foreign_table_oid: None,
                foreign_key_columns: Vec::new(),
                primary_key_columns: Vec::new(),
                on_update: shem_core::schema::ReferentialAction::NoAction,
                on_delete: shem_core::schema::ReferentialAction::NoAction,
                is_deferrable: false,
                is_initially_deferred: false,
                is_not_valid: false,
            },
        ],
        indexes: vec![],
        triggers: vec![],
        comment: None,
        tablespace: None,
        inherits: vec![],
        partition_key: None,
        replica_identity: ReplicaIdentity::Default,
        acl: None,
        is_user_defined: true,
        is_from_extension: false,
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
    use shem_core::schema::{Column, Constraint, ConstraintKind, GeneratedColumn, Identity, Table};

    // Old table with some columns and constraints
    let old_table = Table {
        oid: 0,
        name: "users".to_string(),
        schema: "public".to_string(),
        owner: "".to_string(),
        columns: vec![
            Column {
                name: "id".to_string(),
                type_name: "INTEGER".to_string(),
                is_not_null: true,
                has_default: true,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: ColumnStorage::Plain,
                compression: None,
                acl: None,
                is_dropped: false,
                is_local: true,
                stats_target: None,
                fdw_options: std::collections::HashMap::new(),
            },
            Column {
                name: "name".to_string(),
                type_name: "VARCHAR(50)".to_string(),
                is_not_null: true,
                has_default: false,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: ColumnStorage::Plain,
                compression: None,
                acl: None,
                is_dropped: false,
                is_local: true,
                stats_target: None,
                fdw_options: std::collections::HashMap::new(),
            },
            Column {
                name: "email".to_string(),
                type_name: "TEXT".to_string(),
                is_not_null: false,
                has_default: false,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: ColumnStorage::Plain,
                compression: None,
                acl: None,
                is_dropped: false,
                is_local: true,
                stats_target: None,
                fdw_options: std::collections::HashMap::new(),
            },
            Column {
                name: "to_drop".to_string(),
                type_name: "TEXT".to_string(),
                is_not_null: true,
                has_default: false,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: ColumnStorage::Plain,
                compression: None,
                acl: None,
                is_dropped: false,
                is_local: true,
                stats_target: None,
                fdw_options: std::collections::HashMap::new(),
            },
        ],
        constraints: vec![Constraint {
            oid: 0,
            name: "to_drop_constraint".to_string(),
            table_oid: 0,
            definition: "UNIQUE (email)".to_string(),
            r#type: shem_core::schema::ConstraintType::Unique,
            foreign_table_oid: None,
            foreign_key_columns: Vec::new(),
            primary_key_columns: Vec::new(),
            on_update: shem_core::schema::ReferentialAction::NoAction,
            on_delete: shem_core::schema::ReferentialAction::NoAction,
            is_deferrable: false,
            is_initially_deferred: false,
            is_not_valid: false,
        }],
        indexes: vec![],
        triggers: vec![],
        comment: None,
        tablespace: None,
        inherits: vec![],
        partition_key: None,
        replica_identity: ReplicaIdentity::Default,
        acl: None,
        is_user_defined: true,
        is_from_extension: false,
    };

    // New table with modified columns and constraints
    let new_table = Table {
        oid: 0,
        name: "users".to_string(),
        schema: "public".to_string(),
        owner: "".to_string(),
        columns: vec![
            Column {
                name: "id".to_string(),
                type_name: "BIGINT".to_string(),
                is_not_null: false,
                has_default: false,
                identity: Some(Identity {
                    generation: IdentityGeneration::Always,
                }),
                generated: None,
                comment: None,
                collation: None,
                storage: ColumnStorage::Plain,
                compression: None,
                acl: None,
                is_dropped: false,
                is_local: true,
                stats_target: None,
                fdw_options: std::collections::HashMap::new(),
            },
            Column {
                name: "name".to_string(),
                type_name: "VARCHAR(100)".to_string(),
                is_not_null: false,
                has_default: false,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: ColumnStorage::Plain,
                compression: None,
                acl: None,
                is_dropped: false,
                is_local: true,
                stats_target: None,
                fdw_options: std::collections::HashMap::new(),
            },
            Column {
                name: "email".to_string(),
                type_name: "TEXT".to_string(),
                is_not_null: true,
                has_default: false,
                identity: None,
                generated: Some(Generated {
                    expression: "LOWER(email)".to_string(),
                }),
                comment: None,
                collation: None,
                storage: ColumnStorage::Plain,
                compression: None,
                acl: None,
                is_dropped: false,
                is_local: true,
                stats_target: None,
                fdw_options: std::collections::HashMap::new(),
            },
            Column {
                name: "new_column".to_string(),
                type_name: "TIMESTAMP".to_string(),
                is_not_null: false,
                has_default: false,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: ColumnStorage::Plain,
                compression: None,
                acl: None,
                is_dropped: false,
                is_local: true,
                stats_target: None,
                fdw_options: std::collections::HashMap::new(),
            },
        ],
        constraints: vec![Constraint {
            oid: 0,
            name: "new_constraint".to_string(),
            table_oid: 0,
            definition: "CHECK (LENGTH(name) > 0)".to_string(),
            r#type: shem_core::schema::ConstraintType::Check,
            foreign_table_oid: None,
            foreign_key_columns: Vec::new(),
            primary_key_columns: Vec::new(),
            on_update: shem_core::schema::ReferentialAction::NoAction,
            on_delete: shem_core::schema::ReferentialAction::NoAction,
            is_deferrable: false,
            is_initially_deferred: false,
            is_not_valid: false,
        }],
        indexes: vec![],
        triggers: vec![],
        comment: None,
        tablespace: None,
        inherits: vec![],
        partition_key: None,
        replica_identity: ReplicaIdentity::Default,
        acl: None,
        is_user_defined: true,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let (up_statements, down_statements) = generator
        .generate_alter_table(&old_table, &new_table)
        .unwrap();

    assert!(!up_statements.is_empty());
    assert!(!down_statements.is_empty());

    let up_sql = up_statements.join("; ");
    assert!(up_sql.contains("ALTER TABLE users DROP COLUMN to_drop"));
    assert!(
        up_sql.contains("ALTER TABLE users ADD COLUMN new_column TIMESTAMP")
    );
    assert!(up_sql.contains("ALTER TABLE users ALTER COLUMN name TYPE VARCHAR(100)"));
    assert!(up_sql.contains("ALTER TABLE users ALTER COLUMN name DROP NOT NULL"));
    assert!(up_sql.contains("ALTER TABLE users ALTER COLUMN id TYPE BIGINT"));
    assert!(up_sql.contains("ALTER TABLE users ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY"));
    assert!(up_sql.contains(
        "ALTER TABLE users ALTER COLUMN email SET GENERATED ALWAYS AS (LOWER(email)) STORED"
    ));
    assert!(up_sql.contains("ALTER TABLE users DROP CONSTRAINT to_drop_constraint"));
    assert!(
        up_sql.contains("ALTER TABLE users ADD CONSTRAINT new_constraint CHECK (LENGTH(name) > 0)")
    );
}
