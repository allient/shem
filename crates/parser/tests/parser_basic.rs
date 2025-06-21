use parser::{parse_sql, parse_schema, Statement};
use shared_types::PolicyCommand;

#[test]
fn test_parse_table() {
    let sql = r#"
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateTable(table) => {
            assert_eq!(table.name, "users");
            assert_eq!(table.columns.len(), 4);
        }
        _ => panic!("Expected CreateTable statement"),
    }
}

#[test]
fn test_parse_policy() {
    let sql = r#"
        CREATE POLICY select_policy ON sample_data
            FOR SELECT USING (bool_val = TRUE);
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreatePolicy(policy) => {
            assert_eq!(policy.name, "select_policy");
            assert_eq!(policy.table, "sample_data");
            assert_eq!(policy.command, PolicyCommand::Select);
        }
        _ => panic!("Expected CreatePolicy statement"),
    }
}

#[test]
fn test_parse_function() {
    let sql = r#"
        CREATE FUNCTION add(a integer, b integer) RETURNS integer AS $$
        BEGIN
            RETURN a + b;
        END;
        $$ LANGUAGE plpgsql;
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateFunction(func) => {
            assert_eq!(func.name, "add");
            assert_eq!(func.parameters.len(), 2);
            assert_eq!(func.language, "plpgsql");
        }
        _ => panic!("Expected CreateFunction statement"),
    }
}

#[test]
fn test_parse_schema_multiple_objects() {
    let sql = r#"
        CREATE TABLE users (id SERIAL PRIMARY KEY);
        CREATE POLICY p1 ON users FOR SELECT USING (id > 0);
    "#;
    let schema = parse_schema(sql).unwrap();
    assert_eq!(schema.tables.len(), 1);
    assert_eq!(schema.policies.len(), 1);
    assert_eq!(schema.tables[0].name, "users");
    assert_eq!(schema.policies[0].name, "p1");
} 