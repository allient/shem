use shem_core::schema::{View, MaterializedView, CheckOption};
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;

#[test]
fn test_create_view() {
    let view = View {
        name: "user_summary".to_string(),
        schema: Some("public".to_string()),
        definition: "SELECT id, name, email FROM users WHERE active = true".to_string(),
        check_option: CheckOption::Local,
        comment: Some("Active users summary view".to_string()),
        security_barrier: false,
        columns: vec!["id".to_string(), "name".to_string(), "email".to_string()],
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_view(&view).unwrap();
    
    assert!(result.contains("CREATE VIEW user_summary AS"));
    assert!(result.contains("SELECT id, name, email FROM users WHERE active = true"));
    assert!(result.contains("WITH LOCAL CHECK OPTION"));
}

#[test]
fn test_drop_view() {
    let view = View {
        name: "my_view".to_string(),
        schema: None,
        definition: "SELECT * FROM my_table".to_string(),
        check_option: CheckOption::None,
        comment: None,
        security_barrier: false,
        columns: vec![],
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_view(&view).unwrap();
    assert_eq!(sql, "DROP VIEW IF EXISTS my_view CASCADE;");
}

#[test]
fn test_create_materialized_view_with_data() {
    let view = MaterializedView {
        name: "my_view".to_string(),
        schema: None,
        definition: "SELECT * FROM big_table".to_string(),
        check_option: CheckOption::None,
        comment: None,
        tablespace: None,
        storage_parameters: std::collections::HashMap::new(),
        indexes: vec![],
        populate_with_data: true,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.create_materialized_view(&view).unwrap();
    assert_eq!(sql, "CREATE MATERIALIZED VIEW my_view AS SELECT * FROM big_table\nWITH DATA;");
}

#[test]
fn test_create_materialized_view_with_no_data() {
    let view = MaterializedView {
        name: "my_view".to_string(),
        schema: None,
        definition: "SELECT * FROM big_table".to_string(),
        check_option: CheckOption::None,
        comment: None,
        tablespace: None,
        storage_parameters: std::collections::HashMap::new(),
        indexes: vec![],
        populate_with_data: false,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.create_materialized_view(&view).unwrap();
    assert_eq!(sql, "CREATE MATERIALIZED VIEW my_view AS SELECT * FROM big_table\nWITH NO DATA;");
}

#[test]
fn test_create_materialized_view_with_reserved_keyword() {
    let view = MaterializedView {
        name: "order".to_string(), // Reserved keyword
        schema: None,
        definition: "SELECT * FROM big_table".to_string(),
        check_option: CheckOption::None,
        comment: None,
        tablespace: None,
        storage_parameters: std::collections::HashMap::new(),
        indexes: vec![],
        populate_with_data: true,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.create_materialized_view(&view).unwrap();
    assert_eq!(sql, "CREATE MATERIALIZED VIEW \"order\" AS SELECT * FROM big_table\nWITH DATA;");
}

#[test]
fn test_drop_materialized_view() {
    let view = MaterializedView {
        name: "mat_view".to_string(),
        schema: None,
        definition: "SELECT * FROM my_table".to_string(),
        check_option: CheckOption::None,
        comment: None,
        tablespace: None,
        storage_parameters: std::collections::HashMap::new(),
        indexes: vec![],
        populate_with_data: true,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_materialized_view(&view).unwrap();
    assert_eq!(sql, "DROP MATERIALIZED VIEW IF EXISTS mat_view CASCADE;");
} 