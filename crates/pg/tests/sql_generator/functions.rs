use pg::model::routine::{Function, Procedure, Routine};
use pg::traits::SqlGenerator;
use pg::sql_generator::PostgresSqlGenerator;

#[test]
fn test_create_function() {
    let function = Function {
        oid: 0,
        name: "calculate_total".to_string(),
        schema: "public".to_string(),
        owner: "postgres".to_string(),
        definition: "CREATE OR REPLACE FUNCTION public.calculate_total(IN price numeric, IN tax_rate numeric DEFAULT 0.1) RETURNS numeric LANGUAGE sql AS $function$ SELECT price * (1 + tax_rate) $function$".to_string(),
        identity_arguments: "price numeric, tax_rate numeric".to_string(),
        acl: None,
        comment: Some("Calculate total with tax".to_string()),
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let routine = Routine::Function(function);
    let result = generator.create_routine(&routine).unwrap();
    
    assert!(result.contains("CREATE OR REPLACE FUNCTION public.calculate_total"));
    assert!(result.contains("IN price numeric"));
    assert!(result.contains("IN tax_rate numeric"));
    assert!(result.contains("RETURNS numeric"));
    assert!(result.contains("LANGUAGE sql"));
    assert!(result.contains("SELECT price * (1 + tax_rate)"));
}

#[test]
fn test_drop_function() {
    let func = Function {
        oid: 0,
        name: "my_func".to_string(),
        schema: "public".to_string(),
        owner: "postgres".to_string(),
        definition: "CREATE OR REPLACE FUNCTION public.my_func(param1 integer) RETURNS integer LANGUAGE sql AS $function$ SELECT param1 * 2 $function$".to_string(),
        identity_arguments: "param1 integer".to_string(),
        acl: None,
        comment: None,
        is_from_extension: false,
    };
    let generator = PostgresSqlGenerator;
    let routine = Routine::Function(func);
    let sql = generator.drop_routine(&routine).unwrap();
    assert_eq!(sql, "DROP FUNCTION IF EXISTS \"my_func\"(param1 integer) CASCADE;");
}

#[test]
fn test_create_procedure() {
    let procedure = Procedure {
        oid: 0,
        name: "update_user_status".to_string(),
        schema: "public".to_string(),
        owner: "postgres".to_string(),
        definition: "CREATE OR REPLACE PROCEDURE public.update_user_status(IN user_id integer, IN new_status text) LANGUAGE plpgsql AS $procedure$ BEGIN UPDATE users SET status = new_status WHERE id = user_id; END; $procedure$".to_string(),
        identity_arguments: "user_id integer, new_status text".to_string(),
        acl: None,
        comment: Some("Update user status procedure".to_string()),
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let routine = Routine::Procedure(procedure);
    let result = generator.create_routine(&routine).unwrap();
    
    assert!(result.contains("CREATE OR REPLACE PROCEDURE public.update_user_status"));
    assert!(result.contains("IN user_id integer"));
    assert!(result.contains("IN new_status text"));
    assert!(result.contains("LANGUAGE plpgsql"));
    assert!(result.contains("BEGIN UPDATE users SET status = new_status WHERE id = user_id; END;"));
}

#[test]
fn test_drop_procedure() {
    let proc = Procedure {
        oid: 0,
        name: "my_proc".to_string(),
        schema: "public".to_string(),
        owner: "postgres".to_string(),
        definition: "CREATE OR REPLACE PROCEDURE public.my_proc(param1 integer) LANGUAGE plpgsql AS $procedure$ BEGIN END; $procedure$".to_string(),
        identity_arguments: "param1 integer".to_string(),
        acl: None,
        comment: None,
        is_from_extension: false,
    };
    let generator = PostgresSqlGenerator;
    let routine = Routine::Procedure(proc);
    let sql = generator.drop_routine(&routine).unwrap();
    assert_eq!(sql, "DROP PROCEDURE IF EXISTS \"my_proc\"(param1 integer) CASCADE;");
} 