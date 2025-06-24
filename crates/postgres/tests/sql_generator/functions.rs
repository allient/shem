use shem_core::schema::{Function, Procedure, Parameter, ReturnType, ReturnKind, Volatility, ParallelSafety, ParameterMode};
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;

#[test]
fn test_create_function() {
    let function = Function {
        name: "calculate_total".to_string(),
        schema: Some("public".to_string()),
        parameters: vec![
            Parameter {
                name: "price".to_string(),
                type_name: "numeric".to_string(),
                mode: ParameterMode::In,
                default: None,
            },
            Parameter {
                name: "tax_rate".to_string(),
                type_name: "numeric".to_string(),
                mode: ParameterMode::In,
                default: Some("0.1".to_string()),
            },
        ],
        returns: ReturnType {
            kind: ReturnKind::Scalar,
            type_name: "numeric".to_string(),
            is_set: false,
        },
        language: "sql".to_string(),
        definition: "SELECT price * (1 + tax_rate)".to_string(),
        comment: Some("Calculate total with tax".to_string()),
        volatility: Volatility::Immutable,
        strict: false,
        security_definer: false,
        parallel_safety: ParallelSafety::Safe,
        cost: Some(1.0),
        rows: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_function(&function).unwrap();
    
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
        name: "my_func".to_string(),
        schema: None,
        parameters: vec![
            Parameter {
                name: "param1".to_string(),
                type_name: "integer".to_string(),
                mode: ParameterMode::In,
                default: None,
            },
        ],
        returns: ReturnType {
            kind: ReturnKind::Scalar,
            type_name: "integer".to_string(),
            is_set: false,
        },
        language: "sql".to_string(),
        definition: "SELECT param1 * 2".to_string(),
        comment: None,
        volatility: Volatility::Immutable,
        strict: false,
        security_definer: false,
        parallel_safety: ParallelSafety::Safe,
        cost: None,
        rows: None,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_function(&func).unwrap();
    assert_eq!(sql, "DROP FUNCTION IF EXISTS my_func(integer) CASCADE;");
}

#[test]
fn test_create_procedure() {
    let procedure = Procedure {
        name: "update_user_status".to_string(),
        schema: Some("public".to_string()),
        parameters: vec![
            Parameter {
                name: "user_id".to_string(),
                type_name: "integer".to_string(),
                mode: ParameterMode::In,
                default: None,
            },
            Parameter {
                name: "new_status".to_string(),
                type_name: "text".to_string(),
                mode: ParameterMode::In,
                default: None,
            },
        ],
        language: "plpgsql".to_string(),
        definition: "BEGIN UPDATE users SET status = new_status WHERE id = user_id; END;".to_string(),
        comment: Some("Update user status procedure".to_string()),
        security_definer: true,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_procedure(&procedure).unwrap();
    
    assert!(result.contains("CREATE OR REPLACE PROCEDURE public.update_user_status"));
    assert!(result.contains("IN user_id integer"));
    assert!(result.contains("IN new_status text"));
    assert!(result.contains("LANGUAGE plpgsql"));
    assert!(result.contains("BEGIN UPDATE users SET status = new_status WHERE id = user_id; END;"));
}

#[test]
fn test_drop_procedure() {
    let proc = Procedure {
        name: "my_proc".to_string(),
        schema: None,
        parameters: vec![
            Parameter {
                name: "param1".to_string(),
                type_name: "integer".to_string(),
                mode: ParameterMode::In,
                default: None,
            },
        ],
        language: "plpgsql".to_string(),
        definition: "BEGIN END;".to_string(),
        comment: None,
        security_definer: false,
    };
    let generator = PostgresSqlGenerator;
    let sql = generator.drop_procedure(&proc).unwrap();
    assert_eq!(sql, "DROP PROCEDURE IF EXISTS my_proc(integer) CASCADE;");
} 