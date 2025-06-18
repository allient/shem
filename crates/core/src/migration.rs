use std::path::Path;
use chrono::Utc;
use serde::{Serialize, Deserialize};
use crate::{Schema, Result, Error};
use crate::schema::{
    Table, View, MaterializedView, Function, Procedure,
    Type, Domain, Sequence, Extension, Trigger, Policy, Server,
    TriggerEvent, TriggerTiming, CheckOption, ParameterMode, ReturnType,
    TypeKind, ReturnKind, SortOrder,
};


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Migration {
    pub version: String,
    pub description: String,
    pub statements: Vec<String>,
    pub rollback_statements: Vec<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Generate migration from schema diff
pub fn generate_migration(from: &Schema, to: &Schema) -> Result<Migration> {
    let mut statements = Vec::new();
    let mut rollback_statements = Vec::new();

    // Handle tables
    for (name, table) in &to.tables {
        if !from.tables.contains_key(name) {
            statements.push(generate_create_table(table)?);
            rollback_statements.push(generate_drop_table(table)?);
        } else {
            let old_table = &from.tables[name];
            let (up, down) = generate_alter_table(old_table, table)?;
            statements.extend(up);
            rollback_statements.extend(down);
        }
    }

    // Handle views
    for (name, view) in &to.views {
        if !from.views.contains_key(name) {
            statements.push(generate_create_view(view)?);
            rollback_statements.push(format!("DROP VIEW IF EXISTS {}", name));
        }
    }

    // Handle materialized views
    for (name, view) in &to.materialized_views {
        if !from.materialized_views.contains_key(name) {
            statements.push(generate_create_materialized_view(view)?);
            rollback_statements.push(format!("DROP MATERIALIZED VIEW IF EXISTS {}", name));
        }
    }

    // Handle functions
    for (name, func) in &to.functions {
        if !from.functions.contains_key(name) {
            statements.push(generate_create_function(func)?);
            rollback_statements.push(format!("DROP FUNCTION IF EXISTS {}", name));
        }
    }

    // Handle procedures
    for (name, proc) in &to.procedures {
        if !from.procedures.contains_key(name) {
            statements.push(generate_create_procedure(proc)?);
            rollback_statements.push(format!("DROP PROCEDURE IF EXISTS {}", name));
        }
    }

    // Handle types
    for (name, type_def) in &to.types {
        if !from.types.contains_key(name) {
            statements.push(generate_create_type(type_def)?);
            rollback_statements.push(format!("DROP TYPE IF EXISTS {}", name));
        }
    }

    // Handle domains
    for (name, domain) in &to.domains {
        if !from.domains.contains_key(name) {
            statements.push(generate_create_domain(domain)?);
            rollback_statements.push(format!("DROP DOMAIN IF EXISTS {}", name));
        }
    }

    // Handle sequences
    for (name, seq) in &to.sequences {
        if !from.sequences.contains_key(name) {
            statements.push(generate_create_sequence(seq)?);
            rollback_statements.push(format!("DROP SEQUENCE IF EXISTS {}", name));
        } else {
            let old_seq = &from.sequences[name];
            let (up, down) = generate_alter_sequence(old_seq, seq)?;
            statements.extend(up);
            rollback_statements.extend(down);
        }
    }

    // Handle extensions
    for (name, ext) in &to.extensions {
        if !from.extensions.contains_key(name) {
            statements.push(generate_create_extension(ext)?);
            rollback_statements.push(format!("DROP EXTENSION IF EXISTS {}", name));
        }
    }

    // Handle triggers
    for (name, trigger) in &to.triggers {
        if !from.triggers.contains_key(name) {
            statements.push(generate_create_trigger(trigger)?);
            rollback_statements.push(format!("DROP TRIGGER IF EXISTS {} ON {}", name, trigger.table));
        }
    }

    // Handle policies
    for (name, policy) in &to.policies {
        if !from.policies.contains_key(name) {
            statements.push(generate_create_policy(policy)?);
            rollback_statements.push(format!("DROP POLICY IF EXISTS {} ON {}", name, policy.table));
        }
    }

    // Handle servers
    for (name, server) in &to.servers {
        if !from.servers.contains_key(name) {
            statements.push(generate_create_server(server)?);
            rollback_statements.push(format!("DROP SERVER IF EXISTS {}", name));
        }
    }

    Ok(Migration {
        version: chrono::Utc::now().format("%Y%m%d%H%M%S").to_string(),
        description: "Generated migration".to_string(),
        statements,
        rollback_statements,
        created_at: Utc::now(),
    })
}

// Helper functions for generating SQL statements

fn generate_create_table(table: &Table) -> Result<String> {
    let mut sql = format!("CREATE TABLE {} (", table.name);
    let mut columns = Vec::new();

    // Add columns
    for (i, col) in table.columns.iter().enumerate() {
        let mut col_def = format!("{} {}", col.name, col.type_name);
        if !col.nullable {
            col_def.push_str(" NOT NULL");
        }
        if let Some(default) = &col.default {
            col_def.push_str(&format!(" DEFAULT {}", default));
        }
        if let Some(identity) = &col.identity {
            col_def.push_str(if identity.always { " GENERATED ALWAYS AS IDENTITY" } else { " GENERATED BY DEFAULT AS IDENTITY" });
        }
        if let Some(generated) = &col.generated {
            col_def.push_str(&format!(" GENERATED ALWAYS AS ({}) STORED", generated.expression));
        }
        columns.push(col_def);
    }

    // Add constraints
    for constraint in &table.constraints {
        let sql = match constraint.kind {
            crate::ConstraintKind::PrimaryKey => {
                format!("PRIMARY KEY {}", &constraint.definition["PRIMARY KEY".len()..].trim())
            }
            crate::ConstraintKind::Unique => {
                format!("UNIQUE {}", &constraint.definition["UNIQUE".len()..].trim())
            }
            crate::ConstraintKind::ForeignKey => {
                // Not implemented: you can add more parsing here
                format!("-- FOREIGN KEY: {:?}", constraint)
            }
            crate::ConstraintKind::Check => {
                // Not implemented: you can add more parsing here
                format!("-- CHECK: {:?}", constraint)
            }
            crate::ConstraintKind::Exclusion => {
                // Not implemented: you can add more parsing here
                format!("-- EXCLUSION: {:?}", constraint)
            }
        };
        columns.push(sql);
    }

    sql.push_str(&columns.join(",\n    "));
    sql.push_str("\n);");

    Ok(sql)
}

fn generate_drop_table(table: &Table) -> Result<String> {
    Ok(format!("DROP TABLE IF EXISTS {} CASCADE;", table.name))
}

fn generate_alter_table(old: &Table, new: &Table) -> Result<(Vec<String>, Vec<String>)> {
    let mut up_statements = Vec::new();
    let mut down_statements = Vec::new();
    
    // Handle column changes
    let old_columns: std::collections::HashMap<_, _> = old.columns.iter()
        .map(|c| (&c.name, c))
        .collect();
    let new_columns: std::collections::HashMap<_, _> = new.columns.iter()
        .map(|c| (&c.name, c))
        .collect();
    
    // Add new columns
    for (name, new_col) in &new_columns {
        if !old_columns.contains_key(name) {
            let mut col_def = format!("{} {}", name, new_col.type_name);
            if !new_col.nullable {
                col_def.push_str(" NOT NULL");
            }
            if let Some(default) = &new_col.default {
                col_def.push_str(&format!(" DEFAULT {}", default));
            }
            if let Some(identity) = &new_col.identity {
                col_def.push_str(if identity.always { " GENERATED ALWAYS AS IDENTITY" } else { " GENERATED BY DEFAULT AS IDENTITY" });
            }
            if let Some(generated) = &new_col.generated {
                col_def.push_str(&format!(" GENERATED ALWAYS AS ({}) STORED", generated.expression));
            }
            
            up_statements.push(format!("ALTER TABLE {} ADD COLUMN {};", new.name, col_def));
            down_statements.push(format!("ALTER TABLE {} DROP COLUMN {};", old.name, name));
        }
    }
    
    // Drop removed columns
    for (name, _) in &old_columns {
        if !new_columns.contains_key(name) {
            up_statements.push(format!("ALTER TABLE {} DROP COLUMN {};", new.name, name));
            // For down migration, we need to recreate the column with its original definition
            if let Some(old_col) = old_columns.get(name) {
                let mut col_def = format!("{} {}", name, old_col.type_name);
                if !old_col.nullable {
                    col_def.push_str(" NOT NULL");
                }
                if let Some(default) = &old_col.default {
                    col_def.push_str(&format!(" DEFAULT {}", default));
                }
                if let Some(identity) = &old_col.identity {
                    col_def.push_str(if identity.always { " GENERATED ALWAYS AS IDENTITY" } else { " GENERATED BY DEFAULT AS IDENTITY" });
                }
                if let Some(generated) = &old_col.generated {
                    col_def.push_str(&format!(" GENERATED ALWAYS AS ({}) STORED", generated.expression));
                }
                down_statements.push(format!("ALTER TABLE {} ADD COLUMN {};", old.name, col_def));
            }
        }
    }
    
    // Modify existing columns
    for (name, new_col) in &new_columns {
        if let Some(old_col) = old_columns.get(name) {
            // Check for type changes
            if old_col.type_name != new_col.type_name {
                up_statements.push(format!(
                    "ALTER TABLE {} ALTER COLUMN {} TYPE {};",
                    new.name, name, new_col.type_name
                ));
                down_statements.push(format!(
                    "ALTER TABLE {} ALTER COLUMN {} TYPE {};",
                    old.name, name, old_col.type_name
                ));
            }
            
            // Check for nullability changes
            if old_col.nullable != new_col.nullable {
                if new_col.nullable {
                    up_statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL;",
                        new.name, name
                    ));
                    down_statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;",
                        old.name, name
                    ));
                } else {
                    up_statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;",
                        new.name, name
                    ));
                    down_statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL;",
                        old.name, name
                    ));
                }
            }
            
            // Check for default changes
            if old_col.default != new_col.default {
                if let Some(default) = &new_col.default {
                    up_statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {};",
                        new.name, name, default
                    ));
                } else {
                    up_statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                        new.name, name
                    ));
                }
                
                if let Some(default) = &old_col.default {
                    down_statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {};",
                        old.name, name, default
                    ));
                } else {
                    down_statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                        old.name, name
                    ));
                }
            }
        }
    }
    
    // Handle constraint changes
    let old_constraints: std::collections::HashMap<_, _> = old.constraints.iter()
        .map(|c| (&c.name, c))
        .collect();
    let new_constraints: std::collections::HashMap<_, _> = new.constraints.iter()
        .map(|c| (&c.name, c))
        .collect();
    
    // Add new constraints
    for (name, new_constraint) in &new_constraints {
        if !old_constraints.contains_key(name) {
            up_statements.push(format!(
                "ALTER TABLE {} ADD CONSTRAINT {} {};",
                new.name, name, new_constraint.definition
            ));
            down_statements.push(format!(
                "ALTER TABLE {} DROP CONSTRAINT {};",
                old.name, name
            ));
        }
    }
    
    // Drop removed constraints
    for (name, _) in &old_constraints {
        if !new_constraints.contains_key(name) {
            up_statements.push(format!(
                "ALTER TABLE {} DROP CONSTRAINT {};",
                new.name, name
            ));
            if let Some(old_constraint) = old_constraints.get(name) {
                down_statements.push(format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} {};",
                    old.name, name, old_constraint.definition
                ));
            }
        }
    }
    
    // Handle index changes
    let old_indexes: std::collections::HashMap<_, _> = old.indexes.iter()
        .map(|i| (&i.name, i))
        .collect();
    let new_indexes: std::collections::HashMap<_, _> = new.indexes.iter()
        .map(|i| (&i.name, i))
        .collect();
    
    // Add new indexes
    for (name, new_index) in &new_indexes {
        if !old_indexes.contains_key(name) {
            let columns: Vec<String> = new_index.columns.iter()
                .map(|c| {
                    let mut col = c.name.clone();
                    col.push_str(match c.order {
                        SortOrder::Ascending => " ASC",
                        SortOrder::Descending => " DESC",
                    });
                    if c.nulls_first {
                        col.push_str(" NULLS FIRST");
                    }
                    col
                })
                .collect();
            
            let unique = if new_index.unique { "UNIQUE " } else { "" };
            up_statements.push(format!(
                "CREATE {}INDEX {} ON {} USING {} ({});",
                unique, name, new.name, new_index.method, columns.join(", ")
            ));
            down_statements.push(format!("DROP INDEX {};", name));
        }
    }
    
    // Drop removed indexes
    for (name, _) in &old_indexes {
        if !new_indexes.contains_key(name) {
            up_statements.push(format!("DROP INDEX {};", name));
            if let Some(old_index) = old_indexes.get(name) {
                let columns: Vec<String> = old_index.columns.iter()
                    .map(|c| {
                        let mut col = c.name.clone();
                        col.push_str(match c.order {
                            SortOrder::Ascending => " ASC",
                            SortOrder::Descending => " DESC",
                        });
                        if c.nulls_first {
                            col.push_str(" NULLS FIRST");
                        }
                        col
                    })
                    .collect();
                
                let unique = if old_index.unique { "UNIQUE " } else { "" };
                down_statements.push(format!(
                    "CREATE {}INDEX {} ON {} USING {} ({});",
                    unique, name, old.name, old_index.method, columns.join(", ")
                ));
            }
        }
    }
    
    Ok((up_statements, down_statements))
}

fn generate_create_view(view: &View) -> Result<String> {
    let mut sql = format!("CREATE VIEW {} AS {}", view.name, view.definition);
    
    match view.check_option {
        CheckOption::Local => sql.push_str(" WITH LOCAL CHECK OPTION"),
        CheckOption::Cascaded => sql.push_str(" WITH CASCADED CHECK OPTION"),
        CheckOption::None => {}
    }
    
    sql.push(';');
    Ok(sql)
}

fn generate_create_materialized_view(view: &MaterializedView) -> Result<String> {
    let mut sql = format!("CREATE MATERIALIZED VIEW {} AS {}", view.name, view.definition);
    
    // Materialized views don't have check options
    sql.push(';');
    Ok(sql)
}

fn generate_create_function(func: &Function) -> Result<String> {
    let mut sql = format!("CREATE OR REPLACE FUNCTION {} (", func.name);
    
    // Add parameters
    let params: Vec<String> = func.parameters.iter().map(|param| {
        let mut param_def = String::new();
        if !param.name.is_empty() {
            param_def.push_str(&param.name);
            param_def.push(' ');
        }
        param_def.push_str(&param.type_name);
        if let Some(default) = &param.default {
            param_def.push_str(&format!(" DEFAULT {}", default));
        }
        param_def.push_str(match param.mode {
            ParameterMode::In => " IN",
            ParameterMode::Out => " OUT",
            ParameterMode::InOut => " INOUT",
            ParameterMode::Variadic => " VARIADIC",
        });
        param_def
    }).collect();
    
    sql.push_str(&params.join(", "));
    sql.push_str(") ");
    
    // Add return type
    match func.returns.kind {
        ReturnKind::Scalar => {
            sql.push_str(&format!("RETURNS {}", func.returns.type_name));
        }
        ReturnKind::Table => {
            sql.push_str(&format!("RETURNS TABLE ({})", func.returns.type_name));
        }
        ReturnKind::SetOf => {
            sql.push_str(&format!("RETURNS SETOF {}", func.returns.type_name));
        }
    }
    
    // Add language and definition
    sql.push_str(&format!(" LANGUAGE {} AS $$", func.language));
    sql.push_str(&func.definition);
    sql.push_str("$$;");
    
    Ok(sql)
}

fn generate_create_procedure(proc: &Procedure) -> Result<String> {
    let mut sql = format!("CREATE OR REPLACE PROCEDURE {} (", proc.name);
    
    // Add parameters
    let params: Vec<String> = proc.parameters.iter().map(|param| {
        let mut param_def = String::new();
        if !param.name.is_empty() {
            param_def.push_str(&param.name);
            param_def.push(' ');
        }
        param_def.push_str(&param.type_name);
        if let Some(default) = &param.default {
            param_def.push_str(&format!(" DEFAULT {}", default));
        }
        param_def.push_str(match param.mode {
            ParameterMode::In => " IN",
            ParameterMode::Out => " OUT",
            ParameterMode::InOut => " INOUT",
            ParameterMode::Variadic => " VARIADIC",
        });
        param_def
    }).collect();
    
    sql.push_str(&params.join(", "));
    sql.push_str(") ");
    
    // Add language and definition
    sql.push_str(&format!("LANGUAGE {} AS $$", proc.language));
    sql.push_str(&proc.definition);
    sql.push_str("$$;");
    
    Ok(sql)
}

fn generate_create_type(type_def: &Type) -> Result<String> {
    match &type_def.kind {
        TypeKind::Enum => {
            // For enums, we need to get the values from somewhere else
            // For now, we'll return an error since we don't have the enum values
            Err(Error::Schema("Enum values are required to create an enum type".into()))
        }
        TypeKind::Composite => {
            // For composite types, we need the attributes
            // For now, we'll return an error since we don't have the attributes
            Err(Error::Schema("Composite type attributes are required to create a composite type".into()))
        }
        TypeKind::Range => {
            // For range types, we need the subtype
            // For now, we'll return an error since we don't have the subtype
            Err(Error::Schema("Range subtype is required to create a range type".into()))
        }
        TypeKind::Base => {
            // For base types, we need the input/output functions
            // For now, we'll return an error since we don't have the functions
            Err(Error::Schema("Input/output functions are required to create a base type".into()))
        }
        TypeKind::Domain => {
            Err(Error::Schema("Domain type should be handled by generate_create_domain".into()))
        }
    }
}

fn generate_create_domain(domain: &Domain) -> Result<String> {
    let mut sql = format!("CREATE DOMAIN {} AS {}", domain.name, domain.base_type);
    
    // Add constraints
    for constraint in &domain.constraints {
        sql.push_str(&format!(" CHECK ({})", constraint));
    }
    
    sql.push(';');
    Ok(sql)
}

fn generate_create_sequence(seq: &Sequence) -> Result<String> {
    let mut sql = format!("CREATE SEQUENCE {}", seq.name);
    
    // Add schema if specified
    if let Some(schema) = &seq.schema {
        sql = format!("CREATE SEQUENCE {}.{}", schema, seq.name);
    }
    
    // Add sequence options
    sql.push_str(&format!(" START WITH {}", seq.start));
    sql.push_str(&format!(" INCREMENT BY {}", seq.increment));
    
    // Add min/max values if specified
    if let Some(min) = seq.min_value {
        sql.push_str(&format!(" MINVALUE {}", min));
    } else {
        sql.push_str(" NO MINVALUE");
    }
    
    if let Some(max) = seq.max_value {
        sql.push_str(&format!(" MAXVALUE {}", max));
    } else {
        sql.push_str(" NO MAXVALUE");
    }
    
    // Add cache and cycle options
    sql.push_str(&format!(" CACHE {}", seq.cache));
    sql.push_str(if seq.cycle { " CYCLE" } else { " NO CYCLE" });
    
    sql.push(';');
    Ok(sql)
}

fn generate_alter_sequence(old: &Sequence, new: &Sequence) -> Result<(Vec<String>, Vec<String>)> {
    let mut up_statements = Vec::new();
    let mut down_statements = Vec::new();

    // Handle start value changes
    if old.start != new.start {
        up_statements.push(format!(
            "ALTER SEQUENCE {} RESTART WITH {};",
            new.name, new.start
        ));
        down_statements.push(format!(
            "ALTER SEQUENCE {} RESTART WITH {};",
            old.name, old.start
        ));
    }

    // Handle increment changes
    if old.increment != new.increment {
        up_statements.push(format!(
            "ALTER SEQUENCE {} INCREMENT BY {};",
            new.name, new.increment
        ));
        down_statements.push(format!(
            "ALTER SEQUENCE {} INCREMENT BY {};",
            old.name, old.increment
        ));
    }

    // Handle min value changes
    if old.min_value != new.min_value {
        if let Some(min) = new.min_value {
            up_statements.push(format!(
                "ALTER SEQUENCE {} MINVALUE {};",
                new.name, min
            ));
        } else {
            up_statements.push(format!(
                "ALTER SEQUENCE {} NO MINVALUE;",
                new.name
            ));
        }
        if let Some(min) = old.min_value {
            down_statements.push(format!(
                "ALTER SEQUENCE {} MINVALUE {};",
                old.name, min
            ));
        } else {
            down_statements.push(format!(
                "ALTER SEQUENCE {} NO MINVALUE;",
                old.name
            ));
        }
    }

    // Handle max value changes
    if old.max_value != new.max_value {
        if let Some(max) = new.max_value {
            up_statements.push(format!(
                "ALTER SEQUENCE {} MAXVALUE {};",
                new.name, max
            ));
        } else {
            up_statements.push(format!(
                "ALTER SEQUENCE {} NO MAXVALUE;",
                new.name
            ));
        }
        if let Some(max) = old.max_value {
            down_statements.push(format!(
                "ALTER SEQUENCE {} MAXVALUE {};",
                old.name, max
            ));
        } else {
            down_statements.push(format!(
                "ALTER SEQUENCE {} NO MAXVALUE;",
                old.name
            ));
        }
    }

    // Handle cache changes
    if old.cache != new.cache {
        up_statements.push(format!(
            "ALTER SEQUENCE {} CACHE {};",
            new.name, new.cache
        ));
        down_statements.push(format!(
            "ALTER SEQUENCE {} CACHE {};",
            old.name, old.cache
        ));
    }

    // Handle cycle changes
    if old.cycle != new.cycle {
        if new.cycle {
            up_statements.push(format!(
                "ALTER SEQUENCE {} CYCLE;",
                new.name
            ));
            down_statements.push(format!(
                "ALTER SEQUENCE {} NO CYCLE;",
                old.name
            ));
        } else {
            up_statements.push(format!(
                "ALTER SEQUENCE {} NO CYCLE;",
                new.name
            ));
            down_statements.push(format!(
                "ALTER SEQUENCE {} CYCLE;",
                old.name
            ));
        }
    }

    Ok((up_statements, down_statements))
}

fn generate_create_extension(ext: &Extension) -> Result<String> {
    let mut sql = format!("CREATE EXTENSION IF NOT EXISTS {}", ext.name);
    
    if let Some(schema) = &ext.schema {
        sql.push_str(&format!(" SCHEMA {}", schema));
    }
    
    if !ext.version.is_empty() {
        sql.push_str(&format!(" VERSION '{}'", ext.version));
    }
    
    sql.push(';');
    Ok(sql)
}

fn generate_create_trigger(trigger: &Trigger) -> Result<String> {
    let events: Vec<&str> = trigger.events.iter().map(|e| match e {
        TriggerEvent::Insert => "INSERT",
        TriggerEvent::Update => "UPDATE",
        TriggerEvent::Delete => "DELETE",
        TriggerEvent::Truncate => "TRUNCATE",
    }).collect();

    let timing = match trigger.timing {
        TriggerTiming::Before => "BEFORE",
        TriggerTiming::After => "AFTER",
        TriggerTiming::InsteadOf => "INSTEAD OF",
    };

    let events_str = events.join(" OR ");
    let function = &trigger.function;

    let args = if !trigger.arguments.is_empty() {
        format!("({})", trigger.arguments.join(", "))
    } else {
        String::new()
    };

    Ok(format!(
        "CREATE TRIGGER {} {} {} ON {} FOR EACH ROW EXECUTE FUNCTION {}{};",
        trigger.name, timing, events_str, trigger.table, function, args
    ))
}

fn generate_create_policy(policy: &Policy) -> Result<String> {
    let mut sql = format!(
        "CREATE POLICY {} ON {} AS {}",
        policy.name,
        policy.table,
        if policy.permissive { "PERMISSIVE" } else { "RESTRICTIVE" }
    );

    if !policy.roles.is_empty() {
        sql.push_str(&format!(
            " FOR {}",
            policy.roles.join(", ")
        ));
    }

    if let Some(using) = &policy.using {
        sql.push_str(&format!(" USING ({})", using));
    }

    if let Some(check) = &policy.check {
        sql.push_str(&format!(" WITH CHECK ({})", check));
    }

    sql.push(';');
    Ok(sql)
}

fn generate_create_server(server: &Server) -> Result<String> {
    let mut sql = format!(
        "CREATE SERVER {} FOREIGN DATA WRAPPER {}",
        server.name,
        server.foreign_data_wrapper
    );

    if !server.options.is_empty() {
        let options: Vec<String> = server.options
            .iter()
            .map(|(k, v)| format!("{} '{}'", k, v))
            .collect();
        sql.push_str(&format!(" OPTIONS ({})", options.join(", ")));
    }

    sql.push(';');
    Ok(sql)
}

/// Write migration to file
pub fn write_migration(path: &Path, migration: &Migration) -> Result<()> {
    let content = format!(
        "-- Migration: {}\n\
         -- Generated: {}\n\
         -- Up Migration\n\
         {}\n\
         \n\
         -- Down Migration\n\
         {}",
        migration.description,
        migration.created_at,
        migration.statements.join("\n"),
        migration.rollback_statements.join("\n")
    );
    
    std::fs::write(path, content)?;
        
    Ok(())
}

// Helper to extract columns from debug string for PRIMARY KEY/UNIQUE
fn extract_columns_from_debug(def: &str) -> Option<String> {
    // Looks for columns: ["id", "other"]
    let start = def.find("[\"")?;
    let end = def[start..].find(']')? + start;
    let cols_str = &def[start+2..end];
    let cols: Vec<_> = cols_str.split("\", \"").collect();
    Some(cols.join(", "))
} 