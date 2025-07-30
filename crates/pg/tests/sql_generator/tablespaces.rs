use pg::model::global::Tablespace;
use pg::traits::SqlGenerator;
use pg::sql_generator::PostgresSqlGenerator;
use std::collections::HashMap;

#[test]
fn test_create_tablespace_basic() {
    let tablespace = Tablespace {
        oid: 0,
        name: "ts1".to_string(),
        location: "/data/ts1".to_string(),
        owner: "postgres".to_string(),
        options: HashMap::new(),
        acl: None,
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
        oid: 0,
        name: "ts2".to_string(),
        location: "/data/ts2".to_string(),
        owner: "postgres".to_string(),
        options,
        acl: None,
        comment: Some("My tablespace".to_string()),
    };
    let sql = PostgresSqlGenerator.create_tablespace(&tablespace).unwrap();
    assert!(sql.contains("WITH (random_page_cost = 2.0)"));
    assert!(sql.contains("COMMENT ON TABLESPACE \"ts2\" IS 'My tablespace';"));
}

#[test]
fn test_drop_tablespace() {
    let tablespace = Tablespace {
        oid: 0,
        name: "ts1".to_string(),
        location: "/data/ts1".to_string(),
        owner: "postgres".to_string(),
        options: HashMap::new(),
        acl: None,
        comment: None,
    };
    let sql = PostgresSqlGenerator.drop_tablespace(&tablespace).unwrap();
    assert_eq!(sql, "DROP TABLESPACE IF EXISTS \"ts1\" CASCADE;");
} 