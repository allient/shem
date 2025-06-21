use parser::{parse_sql, parse_schema, Statement};
use shared_types::*;

#[test]
fn test_parse_create_schema() {
    let sql = r#"
        CREATE SCHEMA test_schema AUTHORIZATION test_user;
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateSchema(schema) => {
            assert_eq!(schema.name, "test_schema");
            assert_eq!(schema.owner, Some("test_user".to_string()));
        }
        _ => panic!("Expected CreateSchema statement"),
    }
}

#[test]
fn test_parse_create_table_complex() {
    let sql = r#"
        CREATE TABLE complex_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email TEXT UNIQUE,
            age INTEGER CHECK (age > 0),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            data JSONB,
            tags TEXT[]
        ) INHERITS (parent_table) WITH (fillfactor = 90);
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateTable(table) => {
            assert_eq!(table.name, "complex_table");
            assert_eq!(table.columns.len(), 7);
            assert_eq!(table.inherits, vec!["parent_table"]);
        }
        _ => panic!("Expected CreateTable statement"),
    }
}

#[test]
fn test_parse_create_view() {
    let sql = r#"
        CREATE VIEW user_view AS 
        SELECT id, name, email FROM users WHERE active = true;
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateView(view) => {
            assert_eq!(view.name, "user_view");
            assert!(view.query.contains("SELECT id, name, email FROM users"));
        }
        _ => panic!("Expected CreateView statement"),
    }
}

#[test]
fn test_parse_create_materialized_view() {
    let sql = r#"
        CREATE MATERIALIZED VIEW mat_view AS 
        SELECT COUNT(*) as count FROM users;
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateMaterializedView(view) => {
            assert_eq!(view.name, "mat_view");
            assert!(view.query.contains("SELECT COUNT(*)"));
        }
        _ => panic!("Expected CreateMaterializedView statement"),
    }
}

#[test]
fn test_parse_create_function_complex() {
    let sql = r#"
        CREATE FUNCTION complex_func(
            param1 INTEGER,
            param2 TEXT DEFAULT 'default',
            param3 OUT BOOLEAN
        ) RETURNS INTEGER 
        LANGUAGE plpgsql
        STABLE
        SECURITY DEFINER
        AS $$
        BEGIN
            param3 := param1 > 0;
            RETURN param1 * 2;
        END;
        $$;
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateFunction(func) => {
            assert_eq!(func.name, "complex_func");
            assert_eq!(func.parameters.len(), 3);
            assert_eq!(func.language, "plpgsql");
        }
        _ => panic!("Expected CreateFunction statement"),
    }
}

#[test]
fn test_parse_create_procedure() {
    let sql = r#"
        CREATE PROCEDURE test_proc(param1 INTEGER)
        LANGUAGE plpgsql
        AS $$
        BEGIN
            INSERT INTO logs VALUES (param1, NOW());
        END;
        $$;
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateProcedure(proc) => {
            assert_eq!(proc.name, "test_proc");
            assert_eq!(proc.parameters.len(), 1);
        }
        _ => panic!("Expected CreateProcedure statement"),
    }
}

#[test]
fn test_parse_create_enum() {
    let sql = r#"
        CREATE TYPE status_enum AS ENUM ('active', 'inactive', 'pending');
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateEnum(enum_type) => {
            assert_eq!(enum_type.name, "status_enum");
            assert_eq!(enum_type.values, vec!["active", "inactive", "pending"]);
        }
        _ => panic!("Expected CreateEnum statement"),
    }
}

#[test]
fn test_parse_create_type() {
    let sql = r#"
        CREATE TYPE address_type AS (
            street TEXT,
            city TEXT,
            zip_code VARCHAR(10)
        );
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateType(type_def) => {
            assert_eq!(type_def.name, "address_type");
            assert_eq!(type_def.attributes.len(), 3);
        }
        _ => panic!("Expected CreateType statement"),
    }
}

#[test]
fn test_parse_create_domain() {
    let sql = r#"
        CREATE DOMAIN email_domain AS TEXT 
        CHECK (VALUE ~ '^[^@]+@[^@]+\.[^@]+$');
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateDomain(domain) => {
            assert_eq!(domain.name, "email_domain");
            assert_eq!(domain.data_type, DataType::Text);
        }
        _ => panic!("Expected CreateDomain statement"),
    }
}

#[test]
fn test_parse_create_sequence() {
    let sql = r#"
        CREATE SEQUENCE user_id_seq 
        START WITH 1000 
        INCREMENT BY 1 
        MINVALUE 1000 
        MAXVALUE 999999 
        CACHE 20 
        CYCLE;
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateSequence(seq) => {
            assert_eq!(seq.name, "user_id_seq");
        }
        _ => panic!("Expected CreateSequence statement"),
    }
}

#[test]
fn test_parse_create_extension() {
    let sql = r#"
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA extensions;
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateExtension(ext) => {
            assert_eq!(ext.name, "uuid-ossp");
        }
        _ => panic!("Expected CreateExtension statement"),
    }
}

#[test]
fn test_parse_create_trigger() {
    let sql = r#"
        CREATE TRIGGER audit_trigger 
        AFTER INSERT OR UPDATE ON users 
        FOR EACH ROW 
        EXECUTE FUNCTION audit_function();
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateTrigger(trigger) => {
            assert_eq!(trigger.name, "audit_trigger");
            assert_eq!(trigger.table, "users");
            assert_eq!(trigger.function, "audit_function");
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_parse_create_policy_complex() {
    let sql = r#"
        CREATE POLICY user_policy ON users 
        FOR ALL 
        TO authenticated_users 
        USING (owner_id = current_user_id()) 
        WITH CHECK (status = 'active');
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreatePolicy(policy) => {
            assert_eq!(policy.name, "user_policy");
            assert_eq!(policy.table, "users");
            assert_eq!(policy.command, PolicyCommand::All);
        }
        _ => panic!("Expected CreatePolicy statement"),
    }
}

#[test]
fn test_parse_create_server() {
    let sql = r#"
        CREATE SERVER foreign_server 
        FOREIGN DATA WRAPPER postgres_fdw 
        OPTIONS (host 'remote.example.com', port '5432');
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateServer(server) => {
            assert_eq!(server.name, "foreign_server");
            assert_eq!(server.foreign_data_wrapper, "postgres_fdw");
        }
        _ => panic!("Expected CreateServer statement"),
    }
}

#[test]
fn test_parse_create_collation() {
    let sql = r#"
        CREATE COLLATION german_phonebook (provider = icu, locale = 'de-u-co-phonebk');
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateCollation(collation) => {
            assert_eq!(collation.name, "german_phonebook");
        }
        _ => panic!("Expected CreateCollation statement"),
    }
}

#[test]
fn test_parse_create_rule() {
    let sql = r#"
        CREATE RULE log_rule AS ON INSERT TO users 
        DO ALSO INSERT INTO user_log VALUES (NEW.*);
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateRule(rule) => {
            assert_eq!(rule.name, "log_rule");
            assert_eq!(rule.table, "users");
        }
        _ => panic!("Expected CreateRule statement"),
    }
}

#[test]
fn test_parse_create_range_type() {
    let sql = r#"
        CREATE TYPE float_range AS RANGE (subtype = float8);
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateRangeType(range_type) => {
            assert_eq!(range_type.name, "float_range");
            assert_eq!(range_type.subtype, "float8");
        }
        _ => panic!("Expected CreateRangeType statement"),
    }
}

#[test]
fn test_parse_create_publication() {
    let sql = r#"
        CREATE PUBLICATION my_pub FOR TABLE users, posts;
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreatePublication(publication) => {
            assert_eq!(publication.name, "my_pub");
            assert_eq!(publication.tables, vec!["users", "posts"]);
        }
        _ => panic!("Expected CreatePublication statement"),
    }
}

#[test]
fn test_parse_create_subscription() {
    let sql = r#"
        CREATE SUBSCRIPTION my_sub 
        CONNECTION 'host=remote.example.com port=5432 dbname=mydb' 
        PUBLICATION my_pub;
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateSubscription(sub) => {
            assert_eq!(sub.name, "my_sub");
            assert!(sub.connection.contains("host=remote.example.com"));
        }
        _ => panic!("Expected CreateSubscription statement"),
    }
}

#[test]
fn test_parse_create_role() {
    let sql = r#"
        CREATE ROLE test_role 
        LOGIN 
        PASSWORD 'secret123' 
        CREATEDB 
        CREATEROLE;
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateRole(role) => {
            assert_eq!(role.name, "test_role");
            assert!(role.login);
            assert!(role.createdb);
            assert!(role.createrole);
        }
        _ => panic!("Expected CreateRole statement"),
    }
}

#[test]
fn test_parse_create_tablespace() {
    let sql = r#"
        CREATE TABLESPACE fastspace LOCATION '/ssd1/postgresql/data';
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateTablespace(tablespace) => {
            assert_eq!(tablespace.name, "fastspace");
            assert_eq!(tablespace.location, "/ssd1/postgresql/data");
        }
        _ => panic!("Expected CreateTablespace statement"),
    }
}

#[test]
fn test_parse_create_foreign_table() {
    let sql = r#"
        CREATE FOREIGN TABLE foreign_users (
            id INTEGER,
            name TEXT
        ) SERVER foreign_server;
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateForeignTable(table) => {
            assert_eq!(table.name, "foreign_users");
            assert_eq!(table.server, "foreign_server");
            assert_eq!(table.columns.len(), 2);
        }
        _ => panic!("Expected CreateForeignTable statement"),
    }
}

#[test]
fn test_parse_create_foreign_data_wrapper() {
    let sql = r#"
        CREATE FOREIGN DATA WRAPPER postgres_fdw 
        HANDLER postgres_fdw_handler 
        VALIDATOR postgres_fdw_validator;
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateForeignDataWrapper(wrapper) => {
            assert_eq!(wrapper.name, "postgres_fdw");
            assert_eq!(wrapper.handler, Some("postgres_fdw_handler".to_string()));
            assert_eq!(wrapper.validator, Some("postgres_fdw_validator".to_string()));
        }
        _ => panic!("Expected CreateForeignDataWrapper statement"),
    }
}

#[test]
fn test_parse_alter_table() {
    let sql = r#"
        ALTER TABLE users 
        ADD COLUMN last_login TIMESTAMPTZ,
        ALTER COLUMN email SET NOT NULL,
        ADD CONSTRAINT unique_email UNIQUE (email);
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::AlterTable(alter) => {
            assert_eq!(alter.name, "users");
            assert_eq!(alter.actions.len(), 3);
        }
        _ => panic!("Expected AlterTable statement"),
    }
}

#[test]
fn test_parse_drop_object() {
    let sql = r#"
        DROP TABLE users CASCADE;
    "#;
    let stmts = parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::DropObject(drop) => {
            assert_eq!(drop.name, "users");
            assert!(drop.cascade);
        }
        _ => panic!("Expected DropObject statement"),
    }
}

#[test]
fn test_parse_complex_schema() {
    let sql = r#"
        -- Create schema
        CREATE SCHEMA app_schema;
        
        -- Create tables
        CREATE TABLE app_schema.users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE
        );
        
        CREATE TABLE app_schema.posts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES app_schema.users(id),
            title TEXT NOT NULL,
            content TEXT
        );
        
        -- Create functions
        CREATE FUNCTION app_schema.get_user_posts(user_id INTEGER)
        RETURNS TABLE(id INTEGER, title TEXT)
        LANGUAGE sql
        AS $$
            SELECT id, title FROM app_schema.posts WHERE user_id = $1;
        $$;
        
        -- Create policies
        CREATE POLICY user_policy ON app_schema.users
        FOR SELECT USING (id = current_user_id());
        
        -- Create triggers
        CREATE TRIGGER audit_users
        AFTER INSERT OR UPDATE ON app_schema.users
        FOR EACH ROW EXECUTE FUNCTION audit_function();
    "#;
    
    let schema = parse_schema(sql).unwrap();
    assert_eq!(schema.named_schemas.len(), 1);
    assert_eq!(schema.tables.len(), 2);
    assert_eq!(schema.functions.len(), 1);
    assert_eq!(schema.policies.len(), 1);
    assert_eq!(schema.triggers.len(), 1);
    
    // Verify specific objects
    assert_eq!(schema.named_schemas[0].name, "app_schema");
    assert_eq!(schema.tables[0].name, "users");
    assert_eq!(schema.tables[1].name, "posts");
    assert_eq!(schema.functions[0].name, "get_user_posts");
    assert_eq!(schema.policies[0].name, "user_policy");
    assert_eq!(schema.triggers[0].name, "audit_users");
} 