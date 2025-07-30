use shem_core::schema::Trigger;
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;

#[test]
fn test_create_trigger_basic() {
    let trigger = Trigger {
        oid: 1,
        name: "test_trigger".to_string(),
        table_oid: 1,
        table_name: "test_table".to_string(),
        schema: "public".to_string(),
        definition: "CREATE TRIGGER test_trigger BEFORE INSERT ON test_table FOR EACH ROW EXECUTE FUNCTION test_function".to_string(),
        is_constraint: false,
        comment: None,
        is_from_extension: false,
    };
    let sql = PostgresSqlGenerator.create_trigger(&trigger).unwrap();
    println!("Generated SQL: {}", sql);
    assert!(sql.contains("CREATE TRIGGER \"test_trigger\""));
    assert!(sql.contains("BEFORE INSERT"));
    assert!(sql.contains("ON \"test_table\""));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));
}

#[test]
fn test_create_trigger_with_arguments() {
    let trigger = Trigger {
        oid: 1,
        name: "test_trigger".to_string(),
        table_oid: 1,
        table_name: "test_table".to_string(),
        schema: "public".to_string(),
        definition: "CREATE TRIGGER test_trigger AFTER UPDATE ON test_table FOR EACH ROW EXECUTE FUNCTION test_function(arg1, arg2)".to_string(),
        is_constraint: false,
        comment: None,
        is_from_extension: false,
    };
    let sql = PostgresSqlGenerator.create_trigger(&trigger).unwrap();
    assert!(sql.contains("CREATE TRIGGER \"test_trigger\""));
    assert!(sql.contains("AFTER UPDATE"));
    assert!(sql.contains("ON \"test_table\""));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function(arg1, arg2)"));
}

#[test]
fn test_create_trigger_with_condition() {
    let trigger = Trigger {
        oid: 1,
        name: "test_trigger".to_string(),
        table_oid: 1,
        table_name: "test_table".to_string(),
        schema: "public".to_string(),
        definition: "CREATE TRIGGER test_trigger BEFORE INSERT ON test_table FOR EACH ROW WHEN (NEW.id > 0) EXECUTE FUNCTION test_function".to_string(),
        is_constraint: false,
        comment: None,
        is_from_extension: false,
    };
    let sql = PostgresSqlGenerator.create_trigger(&trigger).unwrap();
    assert!(sql.contains("CREATE TRIGGER \"test_trigger\""));
    assert!(sql.contains("BEFORE INSERT"));
    assert!(sql.contains("ON \"test_table\""));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("WHEN (NEW.id > 0)"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));
}

#[test]
fn test_create_trigger_for_each_statement() {
    let trigger = Trigger {
        oid: 1,
        name: "test_trigger".to_string(),
        table_oid: 1,
        table_name: "test_table".to_string(),
        schema: "public".to_string(),
        definition: "CREATE TRIGGER test_trigger AFTER DELETE ON test_table FOR EACH STATEMENT EXECUTE FUNCTION test_function".to_string(),
        is_constraint: false,
        comment: None,
        is_from_extension: false,
    };
    let sql = PostgresSqlGenerator.create_trigger(&trigger).unwrap();
    assert!(sql.contains("CREATE TRIGGER \"test_trigger\""));
    assert!(sql.contains("AFTER DELETE"));
    assert!(sql.contains("ON \"test_table\""));
    assert!(sql.contains("FOR EACH STATEMENT"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));
}

#[test]
fn test_create_trigger_multiple_events() {
    let trigger = Trigger {
        oid: 1,
        name: "test_trigger".to_string(),
        table_oid: 1,
        table_name: "test_table".to_string(),
        schema: "public".to_string(),
        definition: "CREATE TRIGGER test_trigger BEFORE INSERT OR UPDATE ON test_table FOR EACH ROW EXECUTE FUNCTION test_function".to_string(),
        is_constraint: false,
        comment: None,
        is_from_extension: false,
    };
    let sql = PostgresSqlGenerator.create_trigger(&trigger).unwrap();
    assert!(sql.contains("CREATE TRIGGER \"test_trigger\""));
    assert!(sql.contains("BEFORE INSERT OR UPDATE"));
    assert!(sql.contains("ON \"test_table\""));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));
}

#[test]
fn test_create_trigger_with_schema() {
    let trigger = Trigger {
        oid: 1,
        name: "test_trigger".to_string(),
        table_oid: 1,
        table_name: "test_table".to_string(),
        schema: "test_schema".to_string(),
        definition: "CREATE TRIGGER test_trigger BEFORE INSERT ON test_schema.test_table FOR EACH ROW EXECUTE FUNCTION test_function".to_string(),
        is_constraint: false,
        comment: None,
        is_from_extension: false,
    };
    let sql = PostgresSqlGenerator.create_trigger(&trigger).unwrap();
    assert!(sql.contains("CREATE TRIGGER \"test_trigger\""));
    assert!(sql.contains("BEFORE INSERT"));
    assert!(sql.contains("ON \"test_schema\".\"test_table\""));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));
}

#[test]
fn test_create_trigger_with_comment() {
    let trigger = Trigger {
        oid: 1,
        name: "test_trigger".to_string(),
        table_oid: 1,
        table_name: "test_table".to_string(),
        schema: "public".to_string(),
        definition: "CREATE TRIGGER test_trigger BEFORE INSERT ON test_table FOR EACH ROW EXECUTE FUNCTION test_function".to_string(),
        is_constraint: false,
        comment: Some("This is a test trigger".to_string()),
        is_from_extension: false,
    };
    let sql = PostgresSqlGenerator.create_trigger(&trigger).unwrap();
    assert!(sql.contains("CREATE TRIGGER \"test_trigger\""));
    assert!(sql.contains("BEFORE INSERT"));
    assert!(sql.contains("ON \"test_table\""));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));
}

#[test]
fn test_drop_trigger() {
    let trigger = Trigger {
        oid: 1,
        name: "test_trigger".to_string(),
        table_oid: 1,
        table_name: "test_table".to_string(),
        schema: "public".to_string(),
        definition: "CREATE TRIGGER test_trigger BEFORE INSERT ON test_table FOR EACH ROW EXECUTE FUNCTION test_function".to_string(),
        is_constraint: false,
        comment: None,
        is_from_extension: false,
    };
    let sql = PostgresSqlGenerator.drop_trigger(&trigger).unwrap();
    assert!(sql.contains("DROP TRIGGER IF EXISTS \"test_trigger\""));
    assert!(sql.contains("ON \"test_table\""));
    assert!(sql.contains("CASCADE"));
}

#[test]
fn test_drop_trigger_with_schema() {
    let trigger = Trigger {
        oid: 1,
        name: "test_trigger".to_string(),
        table_oid: 1,
        table_name: "test_table".to_string(),
        schema: "test_schema".to_string(),
        definition: "CREATE TRIGGER test_trigger BEFORE INSERT ON test_schema.test_table FOR EACH ROW EXECUTE FUNCTION test_function".to_string(),
        is_constraint: false,
        comment: None,
        is_from_extension: false,
    };
    let sql = PostgresSqlGenerator.drop_trigger(&trigger).unwrap();
    assert!(sql.contains("DROP TRIGGER IF EXISTS \"test_trigger\""));
    assert!(sql.contains("ON \"test_schema\".\"test_table\""));
    assert!(sql.contains("CASCADE"));
} 