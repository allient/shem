use shem_core::schema::Role;
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;

#[test]
fn test_create_role_basic() {
    let role = Role {
        name: "test_user".to_string(),
        superuser: false,
        createdb: false,
        createrole: false,
        inherit: true,
        login: true,
        replication: false,
        connection_limit: None,
        password: None,
        valid_until: None,
        member_of: vec![],
    };
    let sql = PostgresSqlGenerator.create_role(&role).unwrap();
    assert_eq!(sql, "CREATE ROLE \"test_user\" INHERIT LOGIN;");
}

#[test]
fn test_create_role_with_options() {
    let role = Role {
        name: "admin".to_string(),
        superuser: true,
        createdb: true,
        createrole: true,
        inherit: false,
        login: true,
        replication: true,
        connection_limit: Some(10),
        password: Some("secret".to_string()),
        valid_until: Some("2025-01-01".to_string()),
        member_of: vec!["group1".to_string(), "group2".to_string()],
    };
    let sql = PostgresSqlGenerator.create_role(&role).unwrap();
    assert!(sql.contains("SUPERUSER"));
    assert!(sql.contains("CREATEDB"));
    assert!(sql.contains("CREATEROLE"));
    assert!(sql.contains("LOGIN"));
    assert!(sql.contains("REPLICATION"));
    assert!(sql.contains("CONNECTION LIMIT 10"));
    assert!(sql.contains("PASSWORD 'secret'"));
    assert!(sql.contains("VALID UNTIL '2025-01-01'"));
    assert!(sql.contains("IN ROLE group1, group2"));
}

#[test]
fn test_drop_role() {
    let role = Role {
        name: "test_user".to_string(),
        superuser: false,
        createdb: false,
        createrole: false,
        inherit: true,
        login: true,
        replication: false,
        connection_limit: None,
        password: None,
        valid_until: None,
        member_of: vec![],
    };
    let sql = PostgresSqlGenerator.drop_role(&role).unwrap();
    assert_eq!(sql, "DROP ROLE IF EXISTS \"test_user\" CASCADE;");
} 