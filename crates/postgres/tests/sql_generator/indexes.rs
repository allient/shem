use shem_core::schema::{Index, IndexMethod, IndexColumn, SortOrder};
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;

#[test]
fn test_create_index() {
    let index = Index {
        name: "idx_users_email".to_string(),
        columns: vec![
            IndexColumn {
                name: "email".to_string(),
                expression: None,
                order: SortOrder::Ascending,
                nulls_first: false,
                opclass: None,
            },
        ],
        unique: false,
        method: IndexMethod::Btree,
        where_clause: Some("email IS NOT NULL".to_string()),
        tablespace: None,
        storage_parameters: std::collections::HashMap::new(),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_index(&index).unwrap();
    
    assert!(result.contains("CREATE INDEX idx_users_email ON table_name"));
    assert!(result.contains("USING btree"));
    assert!(result.contains("(email)"));
    assert!(result.contains("WHERE email IS NOT NULL"));
}

#[test]
fn test_create_unique_index() {
    let index = Index {
        name: "idx_users_email_unique".to_string(),
        columns: vec![
            IndexColumn {
                name: "email".to_string(),
                expression: None,
                order: SortOrder::Ascending,
                nulls_first: false,
                opclass: None,
            },
        ],
        unique: true,
        method: IndexMethod::Btree,
        where_clause: None,
        tablespace: None,
        storage_parameters: std::collections::HashMap::new(),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_index(&index).unwrap();
    
    assert!(result.contains("CREATE UNIQUE INDEX idx_users_email_unique ON table_name"));
    assert!(result.contains("USING btree"));
    assert!(result.contains("(email)"));
}

#[test]
fn test_create_index_multiple_columns() {
    let index = Index {
        name: "idx_users_name_email".to_string(),
        columns: vec![
            IndexColumn {
                name: "name".to_string(),
                expression: None,
                order: SortOrder::Ascending,
                nulls_first: false,
                opclass: None,
            },
            IndexColumn {
                name: "email".to_string(),
                expression: None,
                order: SortOrder::Descending,
                nulls_first: true,
                opclass: Some("text_ops".to_string()),
            },
        ],
        unique: false,
        method: IndexMethod::Btree,
        where_clause: None,
        tablespace: Some("fast_space".to_string()),
        storage_parameters: std::collections::HashMap::new(),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_index(&index).unwrap();
    
    assert!(result.contains("CREATE INDEX idx_users_name_email ON table_name"));
    assert!(result.contains("USING btree"));
    assert!(result.contains("(name, email DESC NULLS FIRST text_ops)"));
    assert!(result.contains("TABLESPACE fast_space"));
}

#[test]
fn test_create_index_different_methods() {
    let methods = vec![
        (IndexMethod::Hash, "hash"),
        (IndexMethod::Gin, "gin"),
        (IndexMethod::Gist, "gist"),
        (IndexMethod::Spgist, "spgist"),
        (IndexMethod::Brin, "brin"),
    ];

    for (method, expected) in methods {
        let index = Index {
            name: format!("idx_test_{}", expected),
            columns: vec![
                IndexColumn {
                    name: "test_column".to_string(),
                    expression: None,
                    order: SortOrder::Ascending,
                    nulls_first: false,
                    opclass: None,
                },
            ],
            unique: false,
            method,
            where_clause: None,
            tablespace: None,
            storage_parameters: std::collections::HashMap::new(),
        };

        let generator = PostgresSqlGenerator;
        let result = generator.create_index(&index).unwrap();
        
        assert!(result.contains(&format!("USING {}", expected)));
    }
}

#[test]
fn test_create_index_with_reserved_keyword() {
    let index = Index {
        name: "order".to_string(), // Reserved keyword
        columns: vec![
            IndexColumn {
                name: "user_id".to_string(),
                expression: None,
                order: SortOrder::Ascending,
                nulls_first: false,
                opclass: None,
            },
        ],
        unique: false,
        method: IndexMethod::Btree,
        where_clause: None,
        tablespace: None,
        storage_parameters: std::collections::HashMap::new(),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_index(&index).unwrap();
    
    assert!(result.contains("CREATE INDEX \"order\" ON table_name"));
    assert!(result.contains("USING btree"));
    assert!(result.contains("(user_id)"));
}

#[test]
fn test_drop_index() {
    let index = Index {
        name: "my_index".to_string(),
        columns: vec![
            IndexColumn {
                name: "my_column".to_string(),
                expression: None,
                order: SortOrder::Ascending,
                nulls_first: false,
                opclass: None,
            },
        ],
        unique: false,
        method: IndexMethod::Btree,
        where_clause: None,
        tablespace: None,
        storage_parameters: std::collections::HashMap::new(),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_index(&index).unwrap();
    
    assert_eq!(result, "DROP INDEX IF EXISTS my_index CASCADE;");
}

#[test]
fn test_create_index_partial() {
    let index = Index {
        name: "idx_active_users".to_string(),
        columns: vec![
            IndexColumn {
                name: "email".to_string(),
                expression: None,
                order: SortOrder::Ascending,
                nulls_first: false,
                opclass: None,
            },
        ],
        unique: false,
        method: IndexMethod::Btree,
        where_clause: Some("active = true AND deleted_at IS NULL".to_string()),
        tablespace: None,
        storage_parameters: std::collections::HashMap::new(),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_index(&index).unwrap();
    
    assert!(result.contains("CREATE INDEX idx_active_users ON table_name"));
    assert!(result.contains("USING btree"));
    assert!(result.contains("(email)"));
    assert!(result.contains("WHERE active = true AND deleted_at IS NULL"));
} 