use shem_core::schema::Tablespace;
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;
use std::collections::HashMap;

#[test]
fn test_create_tablespace_basic() {
    let tablespace = Tablespace {
        name: "ts1".to_string(),
        location: "/data/ts1".to_string(),
        owner: "postgres".to_string(),
        options: HashMap::new(),
        comment: None,
    };
    let sql = PostgresSqlGenerator.create_tablespace(&tablespace).unwrap();
    assert!(sql.contains("CREATE TABLESPACE \"ts1\" OWNER \"postgres\" LOCATION '/data/ts1'"));
}

#[test]
fn test_create_tablespace_with_options_and_comment() {
    let mut options = HashMap::new();
    options.insert("random_page_cost".to_string(), "2.0".to_string());
    let tablespace = Tablespace {
        name: "ts2".to_string(),
        location: "/data/ts2".to_string(),
        owner: "postgres".to_string(),
        options,
        comment: Some("My tablespace".to_string()),
    };
    let sql = PostgresSqlGenerator.create_tablespace(&tablespace).unwrap();
    assert!(sql.contains("WITH (random_page_cost = 2.0)"));
    assert!(sql.contains("COMMENT ON TABLESPACE \"ts2\" IS 'My tablespace';"));
}

#[test]
fn test_drop_tablespace() {
    let tablespace = Tablespace {
        name: "ts1".to_string(),
        location: "/data/ts1".to_string(),
        owner: "postgres".to_string(),
        options: HashMap::new(),
        comment: None,
    };
    let sql = PostgresSqlGenerator.drop_tablespace(&tablespace).unwrap();
    assert_eq!(sql, "DROP TABLESPACE IF EXISTS \"ts1\" CASCADE;");
} 