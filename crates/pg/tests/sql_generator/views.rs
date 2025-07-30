use pg::model::relation::{View, MaterializedView, CheckOption};
use pg::traits::SqlGenerator;
use pg::sql_generator::PostgresSqlGenerator;

#[test]
fn test_create_view() {
    let view = View {
        oid: 0,
        name: "user_summary".to_string(),
        schema: "public".to_string(),
        owner: "".to_string(),
        definition: "SELECT id, name, email FROM users WHERE active = true".to_string(),
        columns: vec![],
        check_option: CheckOption::Local,
        options: std::collections::HashMap::new(),
        acl: None,
        comment: Some("Active users summary view".to_string()),
        is_user_defined: true,
        is_from_extension: false,
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
        oid: 0,
        name: "my_view".to_string(),
        schema: "public".to_string(),
        owner: "".to_string(),
        definition: "SELECT * FROM my_table".to_string(),
        columns: vec![],
        check_option: CheckOption::None,
        options: std::collections::HashMap::new(),
        acl: None,
        comment: None,
        is_user_defined: true,
        is_from_extension: false,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_view(&view).unwrap();
    assert_eq!(sql, "DROP VIEW IF EXISTS my_view CASCADE;");
}

#[test]
fn test_create_materialized_view_with_data() {
    let view = MaterializedView {
        oid: 0,
        name: "my_view".to_string(),
        schema: "public".to_string(),
        owner: "postgres".to_string(),
        definition: "SELECT * FROM big_table".to_string(),
        columns: vec![],
        is_populated: true,
        options: std::collections::HashMap::new(),
        tablespace: None,
        acl: None,
        comment: None,
        indexes: vec![],
        is_user_defined: true,
        is_from_extension: false,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.create_materialized_view(&view).unwrap();
    assert_eq!(sql, "CREATE MATERIALIZED VIEW my_view AS SELECT * FROM big_table\nWITH DATA;");
}

#[test]
fn test_create_materialized_view_with_no_data() {
    let view = MaterializedView {
        oid: 0,
        name: "my_view".to_string(),
        schema: "public".to_string(),
        owner: "postgres".to_string(),
        definition: "SELECT * FROM big_table".to_string(),
        columns: vec![],
        is_populated: false,
        options: std::collections::HashMap::new(),
        tablespace: None,
        acl: None,
        comment: None,
        indexes: vec![],
        is_user_defined: true,
        is_from_extension: false,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.create_materialized_view(&view).unwrap();
    assert_eq!(sql, "CREATE MATERIALIZED VIEW my_view AS SELECT * FROM big_table\nWITH NO DATA;");
}

#[test]
fn test_create_materialized_view_with_reserved_keyword() {
    let view = MaterializedView {
        oid: 0,
        name: "order".to_string(), // Reserved keyword
        schema: "public".to_string(),
        owner: "postgres".to_string(),
        definition: "SELECT * FROM big_table".to_string(),
        columns: vec![],
        is_populated: true,
        options: std::collections::HashMap::new(),
        tablespace: None,
        acl: None,
        comment: None,
        indexes: vec![],
        is_user_defined: true,
        is_from_extension: false,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.create_materialized_view(&view).unwrap();
    assert_eq!(sql, "CREATE MATERIALIZED VIEW \"order\" AS SELECT * FROM big_table\nWITH DATA;");
}

#[test]
fn test_drop_materialized_view() {
    let view = MaterializedView {
        oid: 0,
        name: "mat_view".to_string(),
        schema: "public".to_string(),
        owner: "postgres".to_string(),
        definition: "SELECT * FROM my_table".to_string(),
        columns: vec![],
        is_populated: true,
        options: std::collections::HashMap::new(),
        tablespace: None,
        acl: None,
        comment: None,
        indexes: vec![],
        is_user_defined: true,
        is_from_extension: false,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_materialized_view(&view).unwrap();
    assert_eq!(sql, "DROP MATERIALIZED VIEW IF EXISTS mat_view CASCADE;");
} 