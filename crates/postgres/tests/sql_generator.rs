use shem_core::{Column, Type, TypeKind, SqlGenerator, Table, Constraint};
use shem_postgres::PostgresSqlGenerator;

#[test]
fn test_generate_create_enum_type() {
    let type_def = Type {
        name: "status".to_string(),
        schema: Some("public".to_string()),
        kind: TypeKind::Enum {
            values: vec!["active".to_string(), "inactive".to_string(), "pending".to_string()],
        },
        comment: None,
        definition: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_type(&type_def).unwrap();
    
    assert_eq!(
        result,
        "CREATE TYPE public.status AS ENUM ('active', 'inactive', 'pending');"
    );
}

#[test]
fn test_generate_create_enum_type_no_schema() {
    let type_def = Type {
        name: "priority".to_string(),
        schema: None,
        kind: TypeKind::Enum {
            values: vec!["low".to_string(), "medium".to_string(), "high".to_string()],
        },
        comment: None,
        definition: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_type(&type_def).unwrap();
    
    assert_eq!(
        result,
        "CREATE TYPE priority AS ENUM ('low', 'medium', 'high');"
    );
}

#[test]
fn test_generate_create_composite_type() {
    let type_def = Type {
        name: "address".to_string(),
        schema: None,
        kind: TypeKind::Composite {
            attributes: vec![
                Column {
                    name: "street".to_string(),
                    type_name: "text".to_string(),
                    nullable: true,
                    default: None,
                    identity: None,
                    generated: None,
                    comment: None,
                    collation: None,
                    storage: None,
                    compression: None,
                },
                Column {
                    name: "city".to_string(),
                    type_name: "text".to_string(),
                    nullable: true,
                    default: None,
                    identity: None,
                    generated: None,
                    comment: None,
                    collation: None,
                    storage: None,
                    compression: None,
                },
                Column {
                    name: "zip_code".to_string(),
                    type_name: "varchar(10)".to_string(),
                    nullable: true,
                    default: None,
                    identity: None,
                    generated: None,
                    comment: None,
                    collation: None,
                    storage: None,
                    compression: None,
                },
            ],
        },
        comment: None,
        definition: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_type(&type_def).unwrap();
    
    assert_eq!(
        result,
        "CREATE TYPE address AS (street text, city text, zip_code varchar(10));"
    );
}

#[test]
fn test_generate_create_composite_type_with_schema() {
    let type_def = Type {
        name: "point".to_string(),
        schema: Some("geometry".to_string()),
        kind: TypeKind::Composite {
            attributes: vec![
                Column {
                    name: "x".to_string(),
                    type_name: "double precision".to_string(),
                    nullable: true,
                    default: None,
                    identity: None,
                    generated: None,
                    comment: None,
                    collation: None,
                    storage: None,
                    compression: None,
                },
                Column {
                    name: "y".to_string(),
                    type_name: "double precision".to_string(),
                    nullable: true,
                    default: None,
                    identity: None,
                    generated: None,
                    comment: None,
                    collation: None,
                    storage: None,
                    compression: None,
                },
            ],
        },
        comment: None,
        definition: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_type(&type_def).unwrap();
    
    assert_eq!(
        result,
        "CREATE TYPE geometry.point AS (x double precision, y double precision);"
    );
}

#[test]
fn test_generate_create_range_type() {
    let type_def = Type {
        name: "int_range".to_string(),
        schema: None,
        kind: TypeKind::Range,
        comment: None,
        definition: Some("integer".to_string()),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_type(&type_def).unwrap();
    
    assert_eq!(result, "CREATE TYPE int_range AS RANGE (SUBTYPE = integer);");
}

#[test]
fn test_generate_create_range_type_default() {
    let type_def = Type {
        name: "int_range".to_string(),
        schema: None,
        kind: TypeKind::Range,
        comment: None,
        definition: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_type(&type_def).unwrap();
    
    assert_eq!(result, "CREATE TYPE int_range AS RANGE (SUBTYPE = integer);");
}

#[test]
fn test_generate_create_range_type_with_schema() {
    let type_def = Type {
        name: "date_range".to_string(),
        schema: Some("custom".to_string()),
        kind: TypeKind::Range,
        comment: None,
        definition: Some("date".to_string()),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_type(&type_def).unwrap();
    
    assert_eq!(result, "CREATE TYPE custom.date_range AS RANGE (SUBTYPE = date);");
}

#[test]
fn test_generate_create_base_type() {
    let type_def = Type {
        name: "custom_int".to_string(),
        schema: None,
        kind: TypeKind::Base,
        comment: None,
        definition: Some("integer".to_string()),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_type(&type_def).unwrap();
    
    assert_eq!(result, "CREATE TYPE custom_int AS (integer);");
}

#[test]
fn test_generate_create_base_type_default() {
    let type_def = Type {
        name: "custom_int".to_string(),
        schema: None,
        kind: TypeKind::Base,
        comment: None,
        definition: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_type(&type_def).unwrap();
    
    assert_eq!(result, "CREATE TYPE custom_int AS (integer);");
}

#[test]
fn test_generate_create_array_type() {
    let type_def = Type {
        name: "int_array".to_string(),
        schema: None,
        kind: TypeKind::Array,
        comment: None,
        definition: Some("integer[]".to_string()),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_type(&type_def).unwrap();
    
    assert_eq!(result, "CREATE TYPE int_array AS (integer[]);");
}

#[test]
fn test_generate_create_array_type_default() {
    let type_def = Type {
        name: "int_array".to_string(),
        schema: None,
        kind: TypeKind::Array,
        comment: None,
        definition: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_type(&type_def).unwrap();
    
    assert_eq!(result, "CREATE TYPE int_array AS (integer[]);");
}

#[test]
fn test_generate_create_multirange_type() {
    let type_def = Type {
        name: "int_multirange".to_string(),
        schema: None,
        kind: TypeKind::Multirange,
        comment: None,
        definition: Some("int4multirange".to_string()),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_type(&type_def).unwrap();
    
    assert_eq!(result, "CREATE TYPE int_multirange AS (int4multirange);");
}

#[test]
fn test_generate_create_multirange_type_default() {
    let type_def = Type {
        name: "int_multirange".to_string(),
        schema: None,
        kind: TypeKind::Multirange,
        comment: None,
        definition: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_type(&type_def).unwrap();
    
    assert_eq!(result, "CREATE TYPE int_multirange AS (int4multirange);");
}

#[test]
fn test_generate_create_domain_type_error() {
    let type_def = Type {
        name: "email".to_string(),
        schema: None,
        kind: TypeKind::Domain,
        comment: None,
        definition: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_type(&type_def);
    
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Domain types should be created using create_domain method"));
}

#[test]
fn test_drop_view() {
    use shem_core::View;
    let view = View {
        name: "my_view".to_string(),
        schema: Some("public".to_string()),
        definition: "SELECT 1".to_string(),
        check_option: shem_core::CheckOption::None,
        comment: None,
        security_barrier: false,
        columns: vec![],
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_view(&view).unwrap();
    assert_eq!(sql, "DROP VIEW IF EXISTS public.my_view CASCADE;");
}

#[test]
fn test_drop_materialized_view() {
    use shem_core::MaterializedView;
    let view = MaterializedView {
        name: "mat_view".to_string(),
        schema: None,
        definition: "SELECT 1".to_string(),
        check_option: shem_core::CheckOption::None,
        comment: None,
        tablespace: None,
        storage_parameters: Default::default(),
        indexes: vec![],
        populate_with_data: true,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_materialized_view(&view).unwrap();
    assert_eq!(sql, "DROP MATERIALIZED VIEW IF EXISTS mat_view CASCADE;");
}

#[test]
fn test_drop_function() {
    use shem_core::{Function, Parameter, ReturnType, ReturnKind};
    let func = Function {
        name: "my_func".to_string(),
        schema: Some("public".to_string()),
        parameters: vec![Parameter {
            name: "a".to_string(),
            type_name: "int".to_string(),
            mode: shem_core::ParameterMode::In,
            default: None,
        }],
        returns: ReturnType { kind: ReturnKind::Scalar, type_name: "int".to_string(), is_set: false },
        language: "sql".to_string(),
        definition: "SELECT 1".to_string(),
        comment: None,
        volatility: shem_core::Volatility::Immutable,
        strict: false,
        security_definer: false,
        parallel_safety: shem_core::ParallelSafety::Safe,
        cost: None,
        rows: None,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_function(&func).unwrap();
    assert_eq!(sql, "DROP FUNCTION IF EXISTS public.my_func(int) CASCADE;");
}

#[test]
fn test_drop_procedure() {
    use shem_core::{Procedure, Parameter};
    let proc = Procedure {
        name: "my_proc".to_string(),
        schema: None,
        parameters: vec![],
        language: "plpgsql".to_string(),
        definition: "BEGIN END;".to_string(),
        comment: None,
        security_definer: false,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_procedure(&proc).unwrap();
    assert_eq!(sql, "DROP PROCEDURE IF EXISTS my_proc() CASCADE;");
}

#[test]
fn test_drop_type() {
    use shem_core::Type;
    let typ = Type {
        name: "my_type".to_string(),
        schema: None,
        kind: shem_core::TypeKind::Base,
        comment: None,
        definition: None,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_type(&typ).unwrap();
    assert_eq!(sql, "DROP TYPE IF EXISTS my_type CASCADE;");
}

#[test]
fn test_drop_domain() {
    use shem_core::Domain;
    let dom = Domain {
        name: "my_domain".to_string(),
        schema: None,
        base_type: "text".to_string(),
        constraints: vec![],
        default: None,
        not_null: false,
        comment: None,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_domain(&dom).unwrap();
    assert_eq!(sql, "DROP DOMAIN IF EXISTS my_domain CASCADE;");
}

#[test]
fn test_drop_sequence() {
    use shem_core::Sequence;
    let seq = Sequence {
        name: "my_seq".to_string(),
        schema: None,
        data_type: "bigint".to_string(),
        start: 1,
        increment: 1,
        min_value: None,
        max_value: None,
        cache: 1,
        cycle: false,
        owned_by: None,
        comment: None,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_sequence(&seq).unwrap();
    assert_eq!(sql, "DROP SEQUENCE IF EXISTS my_seq CASCADE;");
}

#[test]
fn test_alter_extension() {
    use shem_core::Extension;
    let ext = Extension {
        name: "uuid-ossp".to_string(),
        schema: None,
        version: "1.1".to_string(),
        cascade: false,
        comment: None,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.alter_extension(&ext).unwrap();
    assert_eq!(sql, "ALTER EXTENSION \"uuid-ossp\" UPDATE TO '1.1';");
}

#[test]
fn test_drop_extension() {
    use shem_core::Extension;
    let ext = Extension {
        name: "uuid-ossp".to_string(),
        schema: None,
        version: "1.1".to_string(),
        cascade: true,
        comment: None,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_extension(&ext).unwrap();
    assert_eq!(sql, "DROP EXTENSION IF EXISTS \"uuid-ossp\" CASCADE;");
}

#[test]
fn test_drop_trigger() {
    use shem_core::{Trigger, TriggerTiming, TriggerEvent, TriggerLevel};
    let trig = Trigger {
        name: "my_trigger".to_string(),
        table: "my_table".to_string(),
        schema: None,
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Insert],
        function: "my_func".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: None,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_trigger(&trig).unwrap();
    assert_eq!(sql, "DROP TRIGGER IF EXISTS my_trigger ON my_table CASCADE;");
}

#[test]
fn test_drop_policy() {
    use shem_core::Policy;
    let pol = Policy {
        name: "my_policy".to_string(),
        table: "my_table".to_string(),
        schema: None,
        command: shem_core::PolicyCommand::All,
        permissive: true,
        roles: vec![],
        using: None,
        check: None,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_policy(&pol).unwrap();
    assert_eq!(sql, "DROP POLICY IF EXISTS my_policy ON my_table;");
}

#[test]
fn test_drop_server() {
    use shem_core::Server;
    let srv = Server {
        name: "my_server".to_string(),
        foreign_data_wrapper: "fdw".to_string(),
        options: Default::default(),
        version: None,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_server(&srv).unwrap();
    assert_eq!(sql, "DROP SERVER IF EXISTS my_server CASCADE;");
}

#[test]
fn test_create_index_and_drop_index() {
    use shem_core::{Index, IndexColumn, IndexMethod, SortOrder};
    let idx = Index {
        name: "my_idx".to_string(),
        columns: vec![IndexColumn {
            name: "col1".to_string(),
            expression: None,
            order: SortOrder::Ascending,
            nulls_first: false,
            opclass: None,
        }],
        unique: false,
        method: IndexMethod::Btree,
        where_clause: None,
        tablespace: None,
        storage_parameters: Default::default(),
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.create_index(&idx).unwrap();
    assert!(sql.starts_with("CREATE INDEX my_idx ON table_name USING btree (col1)"));
    let drop_sql = generator.drop_index(&idx).unwrap();
    assert_eq!(drop_sql, "DROP INDEX IF EXISTS my_idx CASCADE;");
}

#[test]
fn test_create_collation_and_drop_collation() {
    use shem_core::{Collation, CollationProvider};
    let coll = Collation {
        name: "my_collation".to_string(),
        schema: None,
        locale: Some("en_US.utf8".to_string()),
        lc_collate: None,
        lc_ctype: None,
        provider: CollationProvider::Libc,
        deterministic: true,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.create_collation(&coll).unwrap();
    assert!(sql.starts_with("CREATE COLLATION my_collation (LOCALE = 'en_US.utf8') PROVIDER libc DETERMINISTIC;"));
    let drop_sql = generator.drop_collation(&coll).unwrap();
    assert_eq!(drop_sql, "DROP COLLATION IF EXISTS my_collation CASCADE;");
}

#[test]
fn test_create_rule_and_drop_rule() {
    use shem_core::{Rule, RuleEvent};
    let rule = Rule {
        name: "my_rule".to_string(),
        table: "my_table".to_string(),
        schema: None,
        event: RuleEvent::Select,
        instead: false,
        condition: None,
        actions: vec!["NOTHING".to_string()],
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.create_rule(&rule).unwrap();
    assert!(sql.starts_with("CREATE RULE my_rule AS ON SELECT"));
    let drop_sql = generator.drop_rule(&rule).unwrap();
    assert_eq!(drop_sql, "DROP RULE IF EXISTS my_rule ON my_table CASCADE;");
}

#[test]
fn test_create_event_trigger_and_drop_event_trigger() {
    use shem_core::{EventTrigger, EventTriggerEvent};
    let trig = EventTrigger {
        name: "my_evt_trig".to_string(),
        event: EventTriggerEvent::DdlCommandStart,
        function: "my_func".to_string(),
        enabled: true,
        tags: vec![],
        condition: None,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.create_event_trigger(&trig).unwrap();
    assert!(sql.starts_with("CREATE EVENT TRIGGER my_evt_trig ON DDL_COMMAND_START"));
    let drop_sql = generator.drop_event_trigger(&trig).unwrap();
    assert_eq!(drop_sql, "DROP EVENT TRIGGER IF EXISTS my_evt_trig CASCADE;");
}

#[test]
fn test_create_constraint_trigger_and_drop_constraint_trigger() {
    use shem_core::{ConstraintTrigger, TriggerTiming, TriggerEvent};
    let trig = ConstraintTrigger {
        name: "my_constr_trig".to_string(),
        table: "my_table".to_string(),
        schema: None,
        function: "my_func".to_string(),
        timing: TriggerTiming::After,
        events: vec![TriggerEvent::Insert],
        arguments: vec![],
        constraint_name: "my_constraint".to_string(),
        deferrable: true,
        initially_deferred: false,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.create_constraint_trigger(&trig).unwrap();
    assert!(sql.starts_with("CREATE CONSTRAINT TRIGGER my_constr_trig AFTER INSERT ON my_table"));
    let drop_sql = generator.drop_constraint_trigger(&trig).unwrap();
    assert_eq!(drop_sql, "DROP TRIGGER IF EXISTS my_constr_trig ON my_table CASCADE;");
}

#[test]
fn test_comment_on() {
    let generator = PostgresSqlGenerator;
    let sql = generator.comment_on("TABLE", "my_table", "This is a comment").unwrap();
    assert_eq!(sql, "COMMENT ON TABLE my_table IS 'This is a comment';");
}

#[test]
fn test_grant_and_revoke_privileges() {
    let generator = PostgresSqlGenerator;
    let grant_sql = generator.grant_privileges(&["SELECT".to_string(), "UPDATE".to_string()], "my_table", &["user1".to_string(), "user2".to_string()]).unwrap();
    assert_eq!(grant_sql, "GRANT SELECT, UPDATE ON my_table TO user1, user2;");
    let revoke_sql = generator.revoke_privileges(&["SELECT".to_string()], "my_table", &["user1".to_string()]).unwrap();
    assert_eq!(revoke_sql, "REVOKE SELECT ON my_table FROM user1;");
}

#[test]
fn test_generate_create_table() {
    let table = Table {
        name: "users".to_string(),
        schema: None,
        columns: vec![
            Column {
                name: "id".to_string(),
                type_name: "SERIAL".to_string(),
                nullable: false,
                default: None,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "default".to_string(),
                type_name: "VARCHAR(100)".to_string(),
                nullable: false,
                default: None,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "email".to_string(),
                type_name: "VARCHAR(255)".to_string(),
                nullable: true,
                default: None,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
        ],
        constraints: vec![
            Constraint {
                name: "users_pkey".to_string(),
                kind: shem_core::ConstraintKind::PrimaryKey,
                definition: "PRIMARY KEY (id)".to_string(),
                deferrable: false,
                initially_deferred: false,
            },
            Constraint {
                name: "users_email_key".to_string(),
                kind: shem_core::ConstraintKind::Unique,
                definition: "UNIQUE (email)".to_string(),
                deferrable: false,
                initially_deferred: false,
            },
        ],
        indexes: vec![],
        comment: None,
        tablespace: None,
        inherits: vec![],
        partition_by: None,
        storage_parameters: std::collections::HashMap::new(),
    };

    let generator = PostgresSqlGenerator;
    let result = generator.generate_create_table(&table).unwrap();
    
    // The debug print should show in the test output
    assert!(result.contains("CREATE TABLE users"));
    assert!(result.contains("id SERIAL NOT NULL"));
    assert!(result.contains("\"default\" VARCHAR(100) NOT NULL"));
    assert!(result.contains("email VARCHAR(255)"));
    assert!(result.contains("PRIMARY KEY (id)"));
    assert!(result.contains("UNIQUE (email)"));
}

#[test]
fn test_create_materialized_view_with_data() {
    use shem_core::MaterializedView;
    let view = MaterializedView {
        name: "my_view".to_string(),
        schema: None,
        definition: "SELECT * FROM big_table".to_string(),
        check_option: shem_core::CheckOption::None,
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
    use shem_core::MaterializedView;
    let view = MaterializedView {
        name: "my_view".to_string(),
        schema: None,
        definition: "SELECT * FROM big_table".to_string(),
        check_option: shem_core::CheckOption::None,
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
    use shem_core::MaterializedView;
    let view = MaterializedView {
        name: "order".to_string(), // Reserved keyword
        schema: None,
        definition: "SELECT * FROM big_table".to_string(),
        check_option: shem_core::CheckOption::None,
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
fn test_generate_alter_table() {
    use shem_core::{Table, Column, Constraint, ConstraintKind, Identity, GeneratedColumn};
    
    // Old table with some columns and constraints
    let old_table = Table {
        name: "users".to_string(),
        schema: None,
        columns: vec![
            Column {
                name: "id".to_string(),
                type_name: "INTEGER".to_string(),
                nullable: false,
                default: Some("1".to_string()),
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "name".to_string(),
                type_name: "VARCHAR(50)".to_string(),
                nullable: false,
                default: None,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "email".to_string(),
                type_name: "VARCHAR(100)".to_string(),
                nullable: true,
                default: None,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "to_drop".to_string(),
                type_name: "TEXT".to_string(),
                nullable: false,
                default: None,
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
        ],
        constraints: vec![
            Constraint {
                name: "users_pkey".to_string(),
                kind: ConstraintKind::PrimaryKey,
                definition: "PRIMARY KEY (id)".to_string(),
                deferrable: false,
                initially_deferred: false,
            },
            Constraint {
                name: "to_drop_constraint".to_string(),
                kind: ConstraintKind::Unique,
                definition: "UNIQUE (email)".to_string(),
                deferrable: false,
                initially_deferred: false,
            },
        ],
        indexes: vec![],
        comment: None,
        tablespace: None,
        inherits: vec![],
        partition_by: None,
        storage_parameters: std::collections::HashMap::new(),
    };

    // New table with modified columns and constraints
    let new_table = Table {
        name: "users".to_string(),
        schema: None,
        columns: vec![
            Column {
                name: "id".to_string(),
                type_name: "BIGINT".to_string(), // Type changed
                nullable: false,
                default: None, // Default removed
                identity: Some(Identity { // Identity added
                    always: true,
                    start: 1,
                    increment: 1,
                    min_value: None,
                    max_value: None,
                    cache: None,
                    cycle: false,
                }),
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "name".to_string(),
                type_name: "VARCHAR(100)".to_string(), // Type changed
                nullable: true, // Nullability changed
                default: Some("'Unknown'".to_string()), // Default added
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "email".to_string(),
                type_name: "VARCHAR(100)".to_string(),
                nullable: true,
                default: None,
                identity: None,
                generated: Some(GeneratedColumn { // Generated column added
                    expression: "LOWER(email)".to_string(),
                    stored: true,
                }),
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
            Column {
                name: "new_column".to_string(), // New column
                type_name: "TIMESTAMP".to_string(),
                nullable: false,
                default: Some("NOW()".to_string()),
                identity: None,
                generated: None,
                comment: None,
                collation: None,
                storage: None,
                compression: None,
            },
        ],
        constraints: vec![
            Constraint {
                name: "users_pkey".to_string(),
                kind: ConstraintKind::PrimaryKey,
                definition: "PRIMARY KEY (id)".to_string(), // Same
                deferrable: false,
                initially_deferred: false,
            },
            Constraint {
                name: "new_constraint".to_string(), // New constraint
                kind: ConstraintKind::Check,
                definition: "CHECK (LENGTH(name) > 0)".to_string(),
                deferrable: false,
                initially_deferred: false,
            },
        ],
        indexes: vec![],
        comment: None,
        tablespace: None,
        inherits: vec![],
        partition_by: None,
        storage_parameters: std::collections::HashMap::new(),
    };

    let generator = PostgresSqlGenerator;
    let (up_statements, down_statements) = generator.generate_alter_table(&old_table, &new_table).unwrap();

    // Verify up statements (migration to new table)
    assert!(up_statements.iter().any(|s| s.contains("DROP COLUMN to_drop")));
    assert!(up_statements.iter().any(|s| s.contains("ADD COLUMN new_column TIMESTAMP")));
    assert!(up_statements.iter().any(|s| s.contains("ALTER COLUMN id TYPE BIGINT")));
    assert!(up_statements.iter().any(|s| s.contains("ALTER COLUMN id DROP DEFAULT")));
    assert!(up_statements.iter().any(|s| s.contains("ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY")));
    assert!(up_statements.iter().any(|s| s.contains("ALTER COLUMN name TYPE VARCHAR(100)")));
    assert!(up_statements.iter().any(|s| s.contains("ALTER COLUMN name DROP NOT NULL")));
    assert!(up_statements.iter().any(|s| s.contains("ALTER COLUMN name SET DEFAULT 'Unknown'")));
    assert!(up_statements.iter().any(|s| s.contains("ALTER COLUMN email SET GENERATED ALWAYS AS (LOWER(email)) STORED")));
    assert!(up_statements.iter().any(|s| s.contains("DROP CONSTRAINT to_drop_constraint")));
    assert!(up_statements.iter().any(|s| s.contains("ADD CONSTRAINT new_constraint CHECK (LENGTH(name) > 0)")));

    // Verify down statements (rollback to old table)
    assert!(down_statements.iter().any(|s| s.contains("ADD COLUMN to_drop TEXT NOT NULL")));
    assert!(down_statements.iter().any(|s| s.contains("DROP COLUMN new_column")));
    assert!(down_statements.iter().any(|s| s.contains("ALTER COLUMN id TYPE INTEGER")));
    assert!(down_statements.iter().any(|s| s.contains("ALTER COLUMN id SET DEFAULT 1")));
    assert!(down_statements.iter().any(|s| s.contains("ALTER COLUMN id DROP IDENTITY")));
    assert!(down_statements.iter().any(|s| s.contains("ALTER COLUMN name TYPE VARCHAR(50)")));
    assert!(down_statements.iter().any(|s| s.contains("ALTER COLUMN name SET NOT NULL")));
    assert!(down_statements.iter().any(|s| s.contains("ALTER COLUMN name DROP DEFAULT")));
    assert!(down_statements.iter().any(|s| s.contains("ALTER COLUMN email DROP EXPRESSION")));
    assert!(down_statements.iter().any(|s| s.contains("ADD CONSTRAINT to_drop_constraint UNIQUE (email)")));
    assert!(down_statements.iter().any(|s| s.contains("DROP CONSTRAINT new_constraint")));

    println!("Up statements: {:?}", up_statements);
    println!("Down statements: {:?}", down_statements);
}

#[test]
fn test_alter_enum() {
    use shem_core::EnumType;
    
    // Old enum with some values
    let old_enum = EnumType {
        name: "status".to_string(),
        schema: Some("public".to_string()),
        values: vec![
            "pending".to_string(),
            "active".to_string(),
            "inactive".to_string(),
        ],
        comment: None,
    };

    // New enum with added values and some removed
    let new_enum = EnumType {
        name: "status".to_string(),
        schema: Some("public".to_string()),
        values: vec![
            "pending".to_string(),
            "active".to_string(),
            "completed".to_string(), // Added
            "cancelled".to_string(), // Added
        ],
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let (up_statements, down_statements) = generator.alter_enum(&old_enum, &new_enum).unwrap();

    println!("Up statements: {:?}", up_statements);
    println!("Down statements: {:?}", down_statements);

    // Verify up statements (migration to new enum)
    assert!(up_statements.iter().any(|s| s.contains("ALTER TYPE public.status ADD VALUE 'completed'")));
    assert!(up_statements.iter().any(|s| s.contains("ALTER TYPE public.status ADD VALUE 'cancelled'")));
    assert!(up_statements.iter().any(|s| s.contains("WARNING: Cannot remove enum values: inactive")));

    // Verify down statements (rollback limitations)
    assert!(down_statements.iter().any(|s| s.contains("WARNING: Cannot remove enum value 'completed'")));
    assert!(down_statements.iter().any(|s| s.contains("WARNING: Cannot remove enum value 'cancelled'")));
    assert!(down_statements.iter().any(|s| s.contains("WARNING: Cannot restore enum value 'inactive'")));
}

#[test]
fn test_alter_enum_no_changes() {
    use shem_core::EnumType;
    
    // Same enum (no changes)
    let enum_type = EnumType {
        name: "status".to_string(),
        schema: None,
        values: vec![
            "pending".to_string(),
            "active".to_string(),
        ],
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let (up_statements, down_statements) = generator.alter_enum(&enum_type, &enum_type).unwrap();

    // Should return empty vectors when no changes
    assert!(up_statements.is_empty());
    assert!(down_statements.is_empty());
} 