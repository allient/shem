use anyhow::Result;
use std::path::Path;

pub mod ast;
mod visitor;

pub use ast::*;
pub use visitor::*;

/// Parse SQL file into AST
pub fn parse_file(path: &Path) -> Result<Vec<Statement>> {
    let content = std::fs::read_to_string(path)?;
    parse_sql(&content)
}

/// Parse SQL string into AST
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let result = pg_query::parse(sql)?;
    let statements = visitor::parse_statements(&result)?;
    Ok(statements)
}

/// Parse SQL into schema definition
pub fn parse_schema(sql: &str) -> Result<SchemaDefinition> {
    let statements = parse_sql(sql)?;
    let mut schema = SchemaDefinition::new();

    for stmt in statements {
        match stmt {
            Statement::CreateTable(create) => schema.tables.push(create),
            Statement::CreateView(create) => schema.views.push(create),
            Statement::CreateMaterializedView(create) => schema.materialized_views.push(create),
            Statement::CreateFunction(create) => schema.functions.push(create),
            Statement::CreateProcedure(create) => schema.procedures.push(create),
            Statement::CreateEnum(create) => schema.enums.push(create),
            Statement::CreateType(create) => schema.types.push(create),
            Statement::CreateDomain(create) => schema.domains.push(create),
            Statement::CreateSequence(create) => schema.sequences.push(create),
            Statement::CreateExtension(create) => schema.extensions.push(create),
            Statement::CreateTrigger(create) => schema.triggers.push(create),
            Statement::CreatePolicy(create) => schema.policies.push(create),
            Statement::CreateServer(create) => schema.servers.push(create),
            _ => continue,
        }
    }

    Ok(schema)
}