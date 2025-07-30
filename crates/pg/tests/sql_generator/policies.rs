use shem_core::schema::{Policy, PolicyCommand};
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;

#[test]
fn test_create_policy() {
    let policy = Policy {
        oid: 0,
        name: Some("user_access_policy".to_string()),
        table_oid: 0,
        table_name: "users".to_string(),
        schema: "public".to_string(),
        command: PolicyCommand::All,
        permissive: true,
        roles: vec!["PUBLIC".to_string()],
        using: Some("user_id = current_user_id()".to_string()),
        check: Some("user_id = current_user_id()".to_string()),
        is_user_defined: true,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_policy(&policy).unwrap();
    
    assert!(result.contains("CREATE POLICY user_access_policy ON users"));
    assert!(result.contains("FOR ALL"));
    assert!(result.contains("TO PUBLIC"));
    assert!(result.contains("USING (user_id = current_user_id())"));
    assert!(result.contains("WITH CHECK (user_id = current_user_id())"));
}

#[test]
fn test_create_policy_select_only() {
    let policy = Policy {
        oid: 0,
        name: Some("read_policy".to_string()),
        table_oid: 0,
        table_name: "users".to_string(),
        schema: "public".to_string(),
        command: PolicyCommand::Select,
        permissive: true,
        roles: vec!["PUBLIC".to_string()],
        using: Some("active = true".to_string()),
        check: None,
        is_user_defined: true,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_policy(&policy).unwrap();
    
    assert!(result.contains("CREATE POLICY read_policy ON users"));
    assert!(result.contains("FOR SELECT"));
    assert!(result.contains("TO PUBLIC"));
    assert!(result.contains("USING (active = true)"));
    assert!(!result.contains("WITH CHECK"));
}

#[test]
fn test_create_policy_insert_only() {
    let policy = Policy {
        oid: 0,
        name: Some("insert_policy".to_string()),
        table_oid: 0,
        table_name: "users".to_string(),
        schema: "public".to_string(),
        command: PolicyCommand::Insert,
        permissive: true,
        roles: vec!["PUBLIC".to_string()],
        using: None,
        check: Some("email IS NOT NULL".to_string()),
        is_user_defined: true,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_policy(&policy).unwrap();
    
    assert!(result.contains("CREATE POLICY insert_policy ON users"));
    assert!(result.contains("FOR INSERT"));
    assert!(result.contains("TO PUBLIC"));
    assert!(!result.contains("USING"));
    assert!(result.contains("WITH CHECK (email IS NOT NULL)"));
}

#[test]
fn test_create_policy_update_only() {
    let policy = Policy {
        oid: 0,
        name: Some("update_policy".to_string()),
        table_oid: 0,
        table_name: "users".to_string(),
        schema: "public".to_string(),
        command: PolicyCommand::Update,
        permissive: true,
        roles: vec!["PUBLIC".to_string()],
        using: Some("user_id = current_user_id()".to_string()),
        check: Some("user_id = current_user_id()".to_string()),
        is_user_defined: true,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_policy(&policy).unwrap();
    
    assert!(result.contains("CREATE POLICY update_policy ON users"));
    assert!(result.contains("FOR UPDATE"));
    assert!(result.contains("TO PUBLIC"));
    assert!(result.contains("USING (user_id = current_user_id())"));
    assert!(result.contains("WITH CHECK (user_id = current_user_id())"));
}

#[test]
fn test_create_policy_delete_only() {
    let policy = Policy {
        oid: 0,
        name: Some("delete_policy".to_string()),
        table_oid: 0,
        table_name: "users".to_string(),
        schema: "public".to_string(),
        command: PolicyCommand::Delete,
        permissive: true,
        roles: vec!["PUBLIC".to_string()],
        using: Some("user_id = current_user_id()".to_string()),
        check: None,
        is_user_defined: true,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_policy(&policy).unwrap();
    
    assert!(result.contains("CREATE POLICY delete_policy ON users"));
    assert!(result.contains("FOR DELETE"));
    assert!(result.contains("TO PUBLIC"));
    assert!(result.contains("USING (user_id = current_user_id())"));
    assert!(!result.contains("WITH CHECK"));
}

#[test]
fn test_create_policy_specific_roles() {
    let policy = Policy {
        oid: 0,
        name: Some("admin_policy".to_string()),
        table_oid: 0,
        table_name: "users".to_string(),
        schema: "public".to_string(),
        command: PolicyCommand::All,
        permissive: true,
        roles: vec!["admin".to_string(), "superuser".to_string()],
        using: Some("true".to_string()),
        check: Some("true".to_string()),
        is_user_defined: true,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_policy(&policy).unwrap();
    
    assert!(result.contains("CREATE POLICY admin_policy ON users"));
    assert!(result.contains("FOR ALL"));
    assert!(result.contains("TO admin, superuser"));
    assert!(result.contains("USING (true)"));
    assert!(result.contains("WITH CHECK (true)"));
}

#[test]
fn test_create_policy_with_reserved_keyword() {
    let policy = Policy {
        oid: 0,
        name: Some("order".to_string()),
        table_oid: 0,
        table_name: "orders".to_string(),
        schema: "public".to_string(),
        command: PolicyCommand::Select,
        permissive: true,
        roles: vec!["PUBLIC".to_string()],
        using: Some("user_id = current_user_id()".to_string()),
        check: None,
        is_user_defined: true,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_policy(&policy).unwrap();
    
    assert!(result.contains("CREATE POLICY \"order\" ON orders"));
    assert!(result.contains("FOR SELECT"));
    assert!(result.contains("TO PUBLIC"));
    assert!(result.contains("USING (user_id = current_user_id())"));
}

#[test]
fn test_drop_policy() {
    let policy = Policy {
        oid: 0,
        name: Some("my_policy".to_string()),
        table_oid: 0,
        table_name: "my_table".to_string(),
        schema: "public".to_string(),
        command: PolicyCommand::Select,
        permissive: true,
        roles: vec!["PUBLIC".to_string()],
        using: Some("true".to_string()),
        check: None,
        is_user_defined: true,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_policy(&policy).unwrap();
    
    assert_eq!(result, "DROP POLICY IF EXISTS my_policy ON my_table CASCADE;");
}

#[test]
fn test_drop_policy_with_schema() {
    let policy = Policy {
        oid: 0,
        name: Some("my_policy".to_string()),
        table_oid: 0,
        table_name: "my_table".to_string(),
        schema: "public".to_string(),
        command: PolicyCommand::Select,
        permissive: true,
        roles: vec!["PUBLIC".to_string()],
        using: Some("true".to_string()),
        check: None,
        is_user_defined: true,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_policy(&policy).unwrap();
    
    assert_eq!(result, "DROP POLICY IF EXISTS my_policy ON my_table CASCADE;");
} 