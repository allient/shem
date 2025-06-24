/*!
 * PostgreSQL SQL Generator
 *
 * This module provides the `PostgresSqlGenerator` struct, which implements the `SqlGenerator` trait.
 * Its purpose is to generate valid PostgreSQL SQL statements for all schema objects, including:
 *   - CREATE, ALTER, and DROP for tables, types, enums, domains, functions, procedures, views,
 *     materialized views, indexes, triggers, policies, extensions, servers, collations, rules,
 *     event triggers, constraint triggers, and more.
 *   - COMMENT ON statements for documentation.
 *   - GRANT and REVOKE statements for privileges.
 *
 * The SQL generator is used for schema migration, introspection/export, reverse generation,
 * and automation of database changes, allowing tools to programmatically manage PostgreSQL schemas.
 */
use shem_core::{
    Collation, ConstraintTrigger, Domain, EventTrigger, Extension, Function, Index,
    IndexMethod, MaterializedView, Policy, Procedure, Rule, Sequence, Server,
    Table, Trigger, View,
    schema::{CheckOption, CollationProvider, EventTriggerEvent, ParameterMode, PolicyCommand, RuleEvent, SortOrder, TriggerEvent, TriggerLevel, TriggerTiming, BaseType, ArrayType, MultirangeType},
    traits::SqlGenerator,
};
use shem_core::{EnumType, Result};

/// PostgreSQL SQL generator
#[derive(Debug, Clone)]
pub struct PostgresSqlGenerator;

impl PostgresSqlGenerator {
    /// Quote an identifier to handle reserved keywords and preserve case sensitivity
    fn quote_identifier(identifier: &str) -> String {
        // Check if quoting is needed
        let needs_quoting = identifier.chars().any(|c| !c.is_alphanumeric() && c != '_')
            || {
                // Check if it's a reserved keyword (simplified list)
                let lower = identifier.to_lowercase();
                matches!(
                    lower.as_str(),
                    "all"
                        | "analyse"
                        | "analyze"
                        | "and"
                        | "any"
                        | "array"
                        | "as"
                        | "asc"
                        | "asymmetric"
                        | "authorization"
                        | "binary"
                        | "both"
                        | "case"
                        | "cast"
                        | "check"
                        | "collate"
                        | "column"
                        | "constraint"
                        | "create"
                        | "cross"
                        | "current_date"
                        | "current_role"
                        | "current_time"
                        | "current_timestamp"
                        | "current_user"
                        | "default"
                        | "deferrable"
                        | "desc"
                        | "distinct"
                        | "do"
                        | "else"
                        | "end"
                        | "except"
                        | "false"
                        | "for"
                        | "foreign"
                        | "freeze"
                        | "from"
                        | "full"
                        | "grant"
                        | "group"
                        | "having"
                        | "in"
                        | "initially"
                        | "inner"
                        | "intersect"
                        | "into"
                        | "is"
                        | "isnull"
                        | "join"
                        | "leading"
                        | "left"
                        | "like"
                        | "limit"
                        | "localtime"
                        | "localtimestamp"
                        | "natural"
                        | "not"
                        | "notnull"
                        | "null"
                        | "offset"
                        | "on"
                        | "only"
                        | "or"
                        | "order"
                        | "outer"
                        | "overlaps"
                        | "placing"
                        | "primary"
                        | "references"
                        | "right"
                        | "select"
                        | "session_user"
                        | "similar"
                        | "some"
                        | "symmetric"
                        | "table"
                        | "then"
                        | "to"
                        | "trailing"
                        | "true"
                        | "union"
                        | "unique"
                        | "user"
                        | "using"
                        | "when"
                        | "where"
                        | "with"
                )
            }
            || {
                // Check if it starts with a number
                identifier.chars().next().map_or(false, |c| c.is_numeric())
            };

        if needs_quoting {
            format!("\"{}\"", identifier.replace("\"", "\"\""))
        } else {
            identifier.to_string()
        }
    }

    fn is_reserved_keyword(name: &str) -> bool {
        // Add more reserved keywords as needed
        matches!(name.to_ascii_lowercase().as_str(), "order")
    }
}

impl SqlGenerator for PostgresSqlGenerator {
    fn generate_create_table(&self, table: &Table) -> Result<String> {
        let table_name = Self::quote_identifier(&table.name);
        let mut sql = format!("CREATE TABLE {} (\n    ", table_name);
        let mut columns = Vec::new();

        // Add columns
        for column in &table.columns {
            let column_name = Self::quote_identifier(&column.name);
            let mut col_def = format!("{} {}", column_name, column.type_name);
            if !column.nullable {
                col_def.push_str(" NOT NULL");
            }
            if let Some(default) = &column.default {
                col_def.push_str(&format!(" DEFAULT {}", default));
            }
            if let Some(identity) = &column.identity {
                col_def.push_str(if identity.always {
                    " GENERATED ALWAYS AS IDENTITY"
                } else {
                    " GENERATED BY DEFAULT AS IDENTITY"
                });
            }
            if let Some(generated) = &column.generated {
                col_def.push_str(&format!(
                    " GENERATED ALWAYS AS ({}) STORED",
                    generated.expression
                ));
            }
            columns.push(col_def);
        }

        // Add constraints
        for constraint in &table.constraints {
            columns.push(constraint.definition.clone());
        }

        sql.push_str(&columns.join(",\n    "));
        sql.push_str("\n);");

        Ok(sql)
    }

    fn generate_alter_table(&self, old: &Table, new: &Table) -> Result<(Vec<String>, Vec<String>)> {
        let mut up_statements = Vec::new();
        let mut down_statements = Vec::new();

        let old_table_name = Self::quote_identifier(&old.name);
        let new_table_name = Self::quote_identifier(&new.name);

        // Handle column changes
        let old_columns: std::collections::HashMap<&str, &shem_core::Column> =
            old.columns.iter().map(|c| (c.name.as_str(), c)).collect();
        let new_columns: std::collections::HashMap<&str, &shem_core::Column> =
            new.columns.iter().map(|c| (c.name.as_str(), c)).collect();

        // Find dropped columns (in old but not in new)
        for (col_name, old_col) in &old_columns {
            if !new_columns.contains_key(col_name) {
                let column_name = Self::quote_identifier(col_name);
                up_statements.push(format!(
                    "ALTER TABLE {} DROP COLUMN {}",
                    new_table_name, column_name
                ));
                // Down migration: add the column back
                let mut col_def = format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    old_table_name, column_name, old_col.type_name
                );
                if !old_col.nullable {
                    col_def.push_str(" NOT NULL");
                }
                if let Some(default) = &old_col.default {
                    col_def.push_str(&format!(" DEFAULT {}", default));
                }
                if let Some(identity) = &old_col.identity {
                    col_def.push_str(if identity.always {
                        " GENERATED ALWAYS AS IDENTITY"
                    } else {
                        " GENERATED BY DEFAULT AS IDENTITY"
                    });
                }
                if let Some(generated) = &old_col.generated {
                    col_def.push_str(&format!(
                        " GENERATED ALWAYS AS ({}) STORED",
                        generated.expression
                    ));
                }
                down_statements.push(col_def);
            }
        }

        // Find added columns (in new but not in old)
        for (col_name, new_col) in &new_columns {
            if !old_columns.contains_key(col_name) {
                let column_name = Self::quote_identifier(col_name);
                let mut col_def = format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    new_table_name, column_name, new_col.type_name
                );
                if !new_col.nullable {
                    col_def.push_str(" NOT NULL");
                }
                if let Some(default) = &new_col.default {
                    col_def.push_str(&format!(" DEFAULT {}", default));
                }
                if let Some(identity) = &new_col.identity {
                    col_def.push_str(if identity.always {
                        " GENERATED ALWAYS AS IDENTITY"
                    } else {
                        " GENERATED BY DEFAULT AS IDENTITY"
                    });
                }
                if let Some(generated) = &new_col.generated {
                    col_def.push_str(&format!(
                        " GENERATED ALWAYS AS ({}) STORED",
                        generated.expression
                    ));
                }
                up_statements.push(col_def);
                down_statements.push(format!(
                    "ALTER TABLE {} DROP COLUMN {}",
                    old_table_name, column_name
                ));
            }
        }

        // Find modified columns (in both old and new but different)
        for (col_name, new_col) in &new_columns {
            if let Some(old_col) = old_columns.get(col_name) {
                let column_name = Self::quote_identifier(col_name);

                // Check for type changes
                if old_col.type_name != new_col.type_name {
                    up_statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                        new_table_name, column_name, new_col.type_name
                    ));
                    down_statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                        old_table_name, column_name, old_col.type_name
                    ));
                }

                // Check for nullability changes
                if old_col.nullable != new_col.nullable {
                    if new_col.nullable {
                        up_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                            new_table_name, column_name
                        ));
                        down_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                            old_table_name, column_name
                        ));
                    } else {
                        up_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                            new_table_name, column_name
                        ));
                        down_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                            old_table_name, column_name
                        ));
                    }
                }

                // Check for default value changes
                if old_col.default != new_col.default {
                    match &new_col.default {
                        Some(default) => {
                            up_statements.push(format!(
                                "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                                new_table_name, column_name, default
                            ));
                        }
                        None => {
                            up_statements.push(format!(
                                "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                                new_table_name, column_name
                            ));
                        }
                    }
                    match &old_col.default {
                        Some(default) => {
                            down_statements.push(format!(
                                "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                                old_table_name, column_name, default
                            ));
                        }
                        None => {
                            down_statements.push(format!(
                                "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                                old_table_name, column_name
                            ));
                        }
                    }
                }

                // Check for identity changes
                if old_col.identity != new_col.identity {
                    // Drop old identity if it exists
                    if old_col.identity.is_some() {
                        up_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP IDENTITY",
                            new_table_name, column_name
                        ));
                    }
                    // Add new identity if it exists
                    if let Some(identity) = &new_col.identity {
                        up_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} ADD GENERATED {} AS IDENTITY",
                            new_table_name,
                            column_name,
                            if identity.always {
                                "ALWAYS"
                            } else {
                                "BY DEFAULT"
                            }
                        ));
                    }

                    // Down migration: restore old identity
                    if new_col.identity.is_some() {
                        down_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP IDENTITY",
                            old_table_name, column_name
                        ));
                    }
                    if let Some(identity) = &old_col.identity {
                        down_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} ADD GENERATED {} AS IDENTITY",
                            old_table_name,
                            column_name,
                            if identity.always {
                                "ALWAYS"
                            } else {
                                "BY DEFAULT"
                            }
                        ));
                    }
                }

                // Check for generated column changes
                if old_col.generated != new_col.generated {
                    // Drop old generated column if it exists
                    if old_col.generated.is_some() {
                        up_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP EXPRESSION",
                            new_table_name, column_name
                        ));
                    }
                    // Add new generated column if it exists
                    if let Some(generated) = &new_col.generated {
                        up_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET GENERATED ALWAYS AS ({}) STORED",
                            new_table_name, column_name, generated.expression
                        ));
                    }

                    // Down migration: restore old generated column
                    if new_col.generated.is_some() {
                        down_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP EXPRESSION",
                            old_table_name, column_name
                        ));
                    }
                    if let Some(generated) = &old_col.generated {
                        down_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET GENERATED ALWAYS AS ({}) STORED",
                            old_table_name, column_name, generated.expression
                        ));
                    }
                }
            }
        }

        // Handle constraint changes
        let old_constraints: std::collections::HashMap<&str, &shem_core::Constraint> = old
            .constraints
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect();
        let new_constraints: std::collections::HashMap<&str, &shem_core::Constraint> = new
            .constraints
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect();

        // Find dropped constraints (in old but not in new)
        for (constraint_name, old_constraint) in &old_constraints {
            if !new_constraints.contains_key(constraint_name) {
                up_statements.push(format!(
                    "ALTER TABLE {} DROP CONSTRAINT {}",
                    new_table_name, constraint_name
                ));
                down_statements.push(format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} {}",
                    old_table_name, constraint_name, old_constraint.definition
                ));
            }
        }

        // Find added constraints (in new but not in old)
        for (constraint_name, new_constraint) in &new_constraints {
            if !old_constraints.contains_key(constraint_name) {
                up_statements.push(format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} {}",
                    new_table_name, constraint_name, new_constraint.definition
                ));
                down_statements.push(format!(
                    "ALTER TABLE {} DROP CONSTRAINT {}",
                    old_table_name, constraint_name
                ));
            }
        }

        // Find modified constraints (in both old and new but different)
        for (constraint_name, new_constraint) in &new_constraints {
            if let Some(old_constraint) = old_constraints.get(constraint_name) {
                if old_constraint.definition != new_constraint.definition {
                    // Drop and recreate the constraint
                    up_statements.push(format!(
                        "ALTER TABLE {} DROP CONSTRAINT {}",
                        new_table_name, constraint_name
                    ));
                    up_statements.push(format!(
                        "ALTER TABLE {} ADD CONSTRAINT {} {}",
                        new_table_name, constraint_name, new_constraint.definition
                    ));

                    down_statements.push(format!(
                        "ALTER TABLE {} DROP CONSTRAINT {}",
                        old_table_name, constraint_name
                    ));
                    down_statements.push(format!(
                        "ALTER TABLE {} ADD CONSTRAINT {} {}",
                        old_table_name, constraint_name, old_constraint.definition
                    ));
                }
            }
        }

        Ok((up_statements, down_statements))
    }

    fn generate_create_enum(&self, enum_type: &EnumType) -> Result<String> {
        let enum_name = Self::quote_identifier(&enum_type.name);
        let values = enum_type
            .values
            .iter()
            .map(|v| format!("'{}'", v))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("CREATE TYPE {} AS ENUM ({});", enum_name, values);
        Ok(sql)
    }

    fn generate_drop_table(&self, table: &Table) -> Result<String> {
        let table_name = Self::quote_identifier(&table.name);
        Ok(format!("DROP TABLE IF EXISTS {} CASCADE;", table_name))
    }

    fn create_view(&self, view: &View) -> Result<String> {
        let view_name = Self::quote_identifier(&view.name);
        let mut sql = format!("CREATE VIEW {} AS {}", view_name, view.definition);
        match view.check_option {
            CheckOption::None => {}
            CheckOption::Local => sql.push_str(" WITH LOCAL CHECK OPTION"),
            CheckOption::Cascaded => sql.push_str(" WITH CASCADED CHECK OPTION"),
        }
        sql.push(';');
        Ok(sql)
    }

    fn create_materialized_view(&self, view: &MaterializedView) -> Result<String> {
        let view_name = Self::quote_identifier(&view.name);

        // Use the populate_with_data field to determine WITH DATA vs WITH NO DATA
        let with_clause = if view.populate_with_data {
            "WITH DATA"
        } else {
            "WITH NO DATA"
        };

        Ok(format!(
            "CREATE MATERIALIZED VIEW {} AS {}\n{};",
            view_name, view.definition, with_clause
        ))
    }

    fn create_function(&self, function: &Function) -> Result<String> {
        let function_name = Self::quote_identifier(&function.name);
        let schema = function.schema.as_deref().unwrap_or("public");
        let language = function.language.to_lowercase();
        let body = function.definition.trim();

        let params = function
            .parameters
            .iter()
            .map(|p| {
                let mode = match p.mode {
                    ParameterMode::In => "IN ",
                    ParameterMode::Out => "OUT ",
                    ParameterMode::InOut => "INOUT ",
                    ParameterMode::Variadic => "VARIADIC ",
                };
                format!("{}{} {}", mode, p.name, p.type_name)
            })
            .collect::<Vec<_>>()
            .join(", ");

        let returns = format!("RETURNS {}", function.returns.type_name);

        Ok(format!(
            "CREATE OR REPLACE FUNCTION {}.{}({}) {} LANGUAGE {} AS $function$\n{}\n$function$;",
            schema, function_name, params, returns, language, body
        ))
    }

    fn create_procedure(&self, procedure: &Procedure) -> Result<String> {
        let procedure_name = Self::quote_identifier(&procedure.name);
        let params = procedure
            .parameters
            .iter()
            .map(|p| {
                let mode = match p.mode {
                    ParameterMode::In => "IN",
                    ParameterMode::Out => "OUT",
                    ParameterMode::InOut => "INOUT",
                    ParameterMode::Variadic => "VARIADIC",
                };
                format!("{} {} {}", mode, p.name, p.type_name)
            })
            .collect::<Vec<_>>()
            .join(", ");

        let language = procedure.language.to_lowercase();
        let body = procedure.definition.trim();
        let schema = procedure.schema.as_deref().unwrap_or("public");

        Ok(format!(
            "CREATE OR REPLACE PROCEDURE {}.{}({}) LANGUAGE {} AS $procedure$ {} $procedure$;",
            schema, procedure_name, params, language, body
        ))
    }

    fn create_enum(&self, enum_type: &EnumType) -> Result<String> {
        let enum_name = match &enum_type.schema {
            Some(schema) => format!("{}.{}", schema, Self::quote_identifier(&enum_type.name)),
            None => Self::quote_identifier(&enum_type.name),
        };

        let values = enum_type
            .values
            .iter()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");

        Ok(format!("CREATE TYPE {} AS ENUM ({});", enum_name, values))
    }

    fn alter_enum(&self, old: &EnumType, new: &EnumType) -> Result<(Vec<String>, Vec<String>)> {
        let mut up_statements = Vec::new();
        let mut down_statements = Vec::new();

        // Get the enum name with schema
        let enum_name = match &new.schema {
            Some(schema) => format!("{}.{}", schema, Self::quote_identifier(&new.name)),
            None => Self::quote_identifier(&new.name),
        };

        // Find values that are in new but not in old (added values)
        let old_values: std::collections::HashSet<&str> =
            old.values.iter().map(|v| v.as_str()).collect();
        let new_values: std::collections::HashSet<&str> =
            new.values.iter().map(|v| v.as_str()).collect();

        // Add new values using ALTER TYPE ... ADD VALUE
        for value in &new.values {
            if !old_values.contains(value.as_str()) {
                let escaped_value = format!("'{}'", value.replace('\'', "''"));
                up_statements.push(format!(
                    "ALTER TYPE {} ADD VALUE {};",
                    enum_name, escaped_value
                ));

                // Note: PostgreSQL doesn't support DROP VALUE, so we can't rollback added values
                // We'll add a comment to indicate this limitation
                down_statements.push(format!(
                    "-- WARNING: Cannot remove enum value '{}' - PostgreSQL limitation",
                    value
                ));
            }
        }

        // Check for removed values (PostgreSQL limitation)
        let removed_values: Vec<&str> = old_values.difference(&new_values).copied().collect();
        if !removed_values.is_empty() {
            // Add warnings about removed values
            up_statements.push(format!(
                "-- WARNING: Cannot remove enum values: {} - PostgreSQL limitation",
                removed_values.join(", ")
            ));

            // For down migration, we would need to add them back, but PostgreSQL doesn't support
            // adding values in specific positions, so we can't guarantee the same order
            for value in &removed_values {
                down_statements.push(format!(
                    "-- WARNING: Cannot restore enum value '{}' in original position - PostgreSQL limitation",
                    value
                ));
            }
        }

        // If no changes were made, return empty vectors
        if up_statements.is_empty() && down_statements.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        // Add a comment explaining the limitations
        if !removed_values.is_empty() {
            up_statements.insert(
                0,
                format!(
                    "-- Note: PostgreSQL enum limitations: removed values ({}) cannot be dropped",
                    removed_values.join(", ")
                ),
            );
        }

        Ok((up_statements, down_statements))
    }

    fn create_domain(&self, domain: &Domain) -> Result<String> {
        let domain_name = Self::quote_identifier(&domain.name);
        let mut sql = format!("CREATE DOMAIN {} AS {}", domain_name, domain.base_type);

        // Add constraints
        let check_expr = domain
            .constraints
            .iter()
            .map(|c| &c.check)
            .cloned()
            .collect::<Vec<_>>()
            .join(" AND ");
        if !check_expr.is_empty() {
            sql.push_str(&format!(" CHECK ({})", check_expr));
        }
        if let Some(default) = &domain.default {
            sql.push_str(&format!(" DEFAULT {}", default));
        }
        if domain.not_null {
            sql.push_str(" NOT NULL");
        }
        sql.push(';');
        Ok(sql)
    }

    fn create_sequence(&self, seq: &Sequence) -> Result<String> {
        let sequence_name = Self::quote_identifier(&seq.name);

        let mut sql = format!("CREATE SEQUENCE {}", sequence_name);

        // AS <datatype>
        if !seq.data_type.is_empty() {
            sql.push_str(&format!(" AS {}", seq.data_type));
        }

        sql.push_str(&format!(" START {}", seq.start));
        sql.push_str(&format!(" INCREMENT {}", seq.increment));

        // Only include MINVALUE/MAXVALUE if they are explicitly set
        if let Some(min) = seq.min_value {
            sql.push_str(&format!(" MINVALUE {}", min));
        }
        if let Some(max) = seq.max_value {
            sql.push_str(&format!(" MAXVALUE {}", max));
        }
        
        sql.push_str(&format!(" CACHE {}", seq.cache));
        if seq.cycle {
            sql.push_str(" CYCLE");
        }

        // OWNED BY
        if let Some(ref owned_by) = seq.owned_by {
            sql.push_str(&format!(" OWNED BY {}", owned_by));
        }

        sql.push(';');

        // COMMENT
        if let Some(ref comment) = seq.comment {
            sql.push_str(&format!(
                "\nCOMMENT ON SEQUENCE {} IS '{}';",
                sequence_name,
                comment.replace('\'', "''")
            ));
        }

        Ok(sql)
    }

    fn alter_sequence(&self, old: &Sequence, new: &Sequence) -> Result<(Vec<String>, Vec<String>)> {
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
            let up_min = match new.min_value {
                Some(min) => format!("SET MINVALUE {}", min),
                None => "SET NO MINVALUE".to_string(),
            };
            let down_min = match old.min_value {
                Some(min) => format!("SET MINVALUE {}", min),
                None => "SET NO MINVALUE".to_string(),
            };
            up_statements.push(format!("ALTER SEQUENCE {} {};", new.name, up_min));
            down_statements.push(format!("ALTER SEQUENCE {} {};", old.name, down_min));
        }

        // Handle max value changes
        if old.max_value != new.max_value {
            let up_max = match new.max_value {
                Some(max) => format!("SET MAXVALUE {}", max),
                None => "SET NO MAXVALUE".to_string(),
            };
            let down_max = match old.max_value {
                Some(max) => format!("SET MAXVALUE {}", max),
                None => "SET NO MAXVALUE".to_string(),
            };
            up_statements.push(format!("ALTER SEQUENCE {} {};", new.name, up_max));
            down_statements.push(format!("ALTER SEQUENCE {} {};", old.name, down_max));
        }

        // Handle cache changes
        if old.cache != new.cache {
            up_statements.push(format!("ALTER SEQUENCE {} CACHE {};", new.name, new.cache));
            down_statements.push(format!("ALTER SEQUENCE {} CACHE {};", old.name, old.cache));
        }

        // Handle cycle changes
        if old.cycle != new.cycle {
            let cycle_str = if new.cycle { "CYCLE" } else { "NO CYCLE" };
            let old_cycle_str = if old.cycle { "CYCLE" } else { "NO CYCLE" };
            up_statements.push(format!("ALTER SEQUENCE {} {};", new.name, cycle_str));
            down_statements.push(format!("ALTER SEQUENCE {} {};", old.name, old_cycle_str));
        }

        Ok((up_statements, down_statements))
    }

    fn create_extension(&self, ext: &Extension) -> Result<String> {
        let name = if ext.name.contains('-') || Self::is_reserved_keyword(&ext.name) {
            format!("\"{}\"", ext.name)
        } else {
            ext.name.clone()
        };

        let mut sql = format!("CREATE EXTENSION IF NOT EXISTS {}", name);

        if !ext.version.trim().is_empty() {
            sql.push_str(&format!(" VERSION '{}'", ext.version));
        }

        if let Some(schema) = &ext.schema {
            sql.push_str(&format!(" SCHEMA {}", schema));
        }

        if ext.cascade {
            sql.push_str(" CASCADE");
        }

        sql.push(';');
        Ok(sql)
    }

    fn create_trigger(&self, trigger: &Trigger) -> Result<String> {
        let trigger_name = if Self::is_reserved_keyword(&trigger.name) {
            format!("\"{}\"", trigger.name)
        } else {
            Self::quote_identifier(&trigger.name)
        };
        let table_name = Self::quote_identifier(&trigger.table);

        let events: Vec<&str> = trigger
            .events
            .iter()
            .map(|e| match e {
                TriggerEvent::Insert => "INSERT",
                TriggerEvent::Update { .. } => "UPDATE",
                TriggerEvent::Delete => "DELETE",
                TriggerEvent::Truncate => "TRUNCATE",
            })
            .collect();

        let timing = match trigger.timing {
            TriggerTiming::Before => "BEFORE",
            TriggerTiming::After => "AFTER",
            TriggerTiming::InsteadOf => "INSTEAD OF",
        };

        let level = match trigger.for_each {
            TriggerLevel::Row => "FOR EACH ROW",
            TriggerLevel::Statement => "FOR EACH STATEMENT",
        };

        let events_str = events.join(" OR ");
        let function = &trigger.function;

        let args = if !trigger.arguments.is_empty() {
            format!("({})", trigger.arguments.join(", "))
        } else {
            "()".to_string()
        };

        let when = if let Some(condition) = &trigger.condition {
            format!(" WHEN ({})", condition)
        } else {
            String::new()
        };

        Ok(format!(
            "CREATE TRIGGER {} {} {} ON {} {}{} EXECUTE FUNCTION {}{};",
            trigger_name, timing, events_str, table_name, level, when, function, args
        ))
    }

    fn create_policy(&self, policy: &Policy) -> Result<String> {
        let policy_name = Self::quote_identifier(&policy.name);
        let table_name = Self::quote_identifier(&policy.table);

        let mut sql = format!(
            "CREATE POLICY {} ON {} AS {}",
            policy_name,
            table_name,
            if policy.permissive {
                "PERMISSIVE"
            } else {
                "RESTRICTIVE"
            }
        );

        // Add command type
        let command_str = match policy.command {
            PolicyCommand::All => "ALL",
            PolicyCommand::Select => "SELECT",
            PolicyCommand::Insert => "INSERT",
            PolicyCommand::Update => "UPDATE",
            PolicyCommand::Delete => "DELETE",
        };
        sql.push_str(&format!(" FOR {}", command_str));

        if !policy.roles.is_empty() {
            sql.push_str(&format!(" TO {}", policy.roles.join(", ")));
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

    fn create_server(&self, server: &Server) -> Result<String> {
        let server_name = Self::quote_identifier(&server.name);
        let fdw = Self::quote_identifier(&server.foreign_data_wrapper);

        let mut sql = format!("CREATE SERVER {} FOREIGN DATA WRAPPER {}", server_name, fdw);

        // Add VERSION if present
        if let Some(version) = &server.version {
            sql.push_str(&format!(" VERSION '{}'", version.replace('\'', "''")));
        }

        // Add OPTIONS if present
        if !server.options.is_empty() {
            let options = server
                .options
                .iter()
                .map(|(k, v)| format!("{} '{}'", Self::quote_identifier(k), v.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" OPTIONS ({})", options));
        }

        sql.push(';');
        Ok(sql)
    }

    fn drop_view(&self, view: &View) -> Result<String> {
        let name = if let Some(schema) = &view.schema {
            format!("{}.{}", schema, Self::quote_identifier(&view.name))
        } else {
            Self::quote_identifier(&view.name)
        };
        Ok(format!("DROP VIEW IF EXISTS {} CASCADE;", name))
    }

    fn drop_materialized_view(&self, view: &MaterializedView) -> Result<String> {
        let name = if let Some(schema) = &view.schema {
            format!("{}.{}", schema, Self::quote_identifier(&view.name))
        } else {
            Self::quote_identifier(&view.name)
        };
        Ok(format!(
            "DROP MATERIALIZED VIEW IF EXISTS {} CASCADE;",
            name
        ))
    }

    fn drop_function(&self, func: &Function) -> Result<String> {
        let name = if let Some(schema) = &func.schema {
            format!("{}.{}", schema, Self::quote_identifier(&func.name))
        } else {
            Self::quote_identifier(&func.name)
        };

        // Build parameter signature for function identification
        let params = func
            .parameters
            .iter()
            .map(|p| p.type_name.clone())
            .collect::<Vec<_>>()
            .join(", ");

        let signature = if params.is_empty() {
            "()".to_string()
        } else {
            format!("({})", params)
        };

        Ok(format!(
            "DROP FUNCTION IF EXISTS {}{} CASCADE;",
            name, signature
        ))
    }

    fn drop_procedure(&self, proc: &Procedure) -> Result<String> {
        let name = if let Some(schema) = &proc.schema {
            format!("{}.{}", schema, Self::quote_identifier(&proc.name))
        } else {
            Self::quote_identifier(&proc.name)
        };

        // Build parameter signature for procedure identification
        let params = proc
            .parameters
            .iter()
            .map(|p| p.type_name.clone())
            .collect::<Vec<_>>()
            .join(", ");

        let signature = if params.is_empty() {
            "()".to_string()
        } else {
            format!("({})", params)
        };

        Ok(format!(
            "DROP PROCEDURE IF EXISTS {}{} CASCADE;",
            name, signature
        ))
    }

    fn drop_domain(&self, domain: &Domain) -> Result<String> {
        let name = if let Some(schema) = &domain.schema {
            format!("{}.{}", schema, Self::quote_identifier(&domain.name))
        } else {
            Self::quote_identifier(&domain.name)
        };
        Ok(format!("DROP DOMAIN IF EXISTS {} CASCADE;", name))
    }

    fn drop_sequence(&self, seq: &Sequence) -> Result<String> {
        let name = if let Some(schema) = &seq.schema {
            format!("{}.{}", schema, Self::quote_identifier(&seq.name))
        } else {
            Self::quote_identifier(&seq.name)
        };
        Ok(format!("DROP SEQUENCE IF EXISTS {} CASCADE;", name))
    }

    fn alter_extension(&self, ext: &Extension) -> Result<String> {
        let mut sql = format!("ALTER EXTENSION \"{}\"", ext.name);

        if !ext.version.trim().is_empty() {
            sql.push_str(&format!(" UPDATE TO '{}'", ext.version));
        }

        sql.push(';');
        Ok(sql)
    }

    fn drop_extension(&self, ext: &Extension) -> Result<String> {
        let name = if ext.name.contains('-') || Self::is_reserved_keyword(&ext.name) {
            format!("\"{}\"", ext.name)
        } else {
            ext.name.clone()
        };
        Ok(format!("DROP EXTENSION IF EXISTS {} CASCADE;", name))
    }

    fn drop_trigger(&self, trigger: &Trigger) -> Result<String> {
        let trigger_name = if let Some(schema) = &trigger.schema {
            format!("{}.{}", schema, Self::quote_identifier(&trigger.name))
        } else {
            Self::quote_identifier(&trigger.name)
        };
        
        let table_name = if let Some(schema) = &trigger.schema {
            format!("{}.{}", schema, Self::quote_identifier(&trigger.table))
        } else {
            Self::quote_identifier(&trigger.table)
        };
        
        Ok(format!(
            "DROP TRIGGER IF EXISTS {} ON {} CASCADE;",
            trigger_name, table_name
        ))
    }

    fn drop_policy(&self, policy: &Policy) -> Result<String> {
        let policy_name = if let Some(schema) = &policy.schema {
            format!("{}.{}", schema, Self::quote_identifier(&policy.name))
        } else {
            Self::quote_identifier(&policy.name)
        };
        
        let table_name = if let Some(schema) = &policy.schema {
            format!("{}.{}", schema, Self::quote_identifier(&policy.table))
        } else {
            Self::quote_identifier(&policy.table)
        };
        
        Ok(format!("DROP POLICY IF EXISTS {} ON {} CASCADE;", policy_name, table_name))
    }

    fn drop_server(&self, server: &Server) -> Result<String> {
        Ok(format!(
            "DROP SERVER IF EXISTS {} CASCADE;",
            Self::quote_identifier(&server.name)
        ))
    }

    fn create_index(&self, index: &Index) -> Result<String> {
        let mut sql = String::new();

        if index.unique {
            sql.push_str("CREATE UNIQUE INDEX ");
        } else {
            sql.push_str("CREATE INDEX ");
        }

        sql.push_str(&Self::quote_identifier(&index.name));
        sql.push_str(" ON ");

        // Note: Index doesn't have table name, this would need to be passed separately
        // For now, we'll use a placeholder
        sql.push_str("table_name");

        sql.push_str(" USING ");
        sql.push_str(match index.method {
            IndexMethod::Btree => "btree",
            IndexMethod::Hash => "hash",
            IndexMethod::Gist => "gist",
            IndexMethod::Spgist => "spgist",
            IndexMethod::Gin => "gin",
            IndexMethod::Brin => "brin",
        });

        sql.push_str(" (");
        let columns = index
            .columns
            .iter()
            .map(|col| {
                let mut col_def = Self::quote_identifier(&col.name);
                if let Some(expr) = &col.expression {
                    col_def = format!("({})", expr);
                }
                if col.order == SortOrder::Descending {
                    col_def.push_str(" DESC");
                }
                if col.nulls_first {
                    col_def.push_str(" NULLS FIRST");
                }
                if let Some(opclass) = &col.opclass {
                    col_def.push_str(&format!(" {}", opclass));
                }
                col_def
            })
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&columns);
        sql.push_str(")");

        if let Some(where_clause) = &index.where_clause {
            sql.push_str(&format!(" WHERE {}", where_clause));
        }

        if let Some(tablespace) = &index.tablespace {
            sql.push_str(&format!(" TABLESPACE {}", tablespace));
        }

        if !index.storage_parameters.is_empty() {
            sql.push_str(" WITH (");
            let params = index
                .storage_parameters
                .iter()
                .map(|(k, v)| format!("{} = {}", k, v))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&params);
            sql.push_str(")");
        }

        sql.push(';');
        Ok(sql)
    }

    fn drop_index(&self, index: &Index) -> Result<String> {
        Ok(format!(
            "DROP INDEX IF EXISTS {} CASCADE;",
            Self::quote_identifier(&index.name)
        ))
    }

    fn create_collation(&self, collation: &Collation) -> Result<String> {
        let collation_name = Self::quote_identifier(&collation.name);
        let mut sql = format!("CREATE COLLATION {}", collation_name);

        if let Some(schema) = &collation.schema {
            sql = format!("CREATE COLLATION {}.{}", schema, collation_name);
        }

        if let Some(locale) = &collation.locale {
            sql.push_str(&format!(" (LOCALE = '{}')", locale));
        } else if let (Some(lc_collate), Some(lc_ctype)) =
            (&collation.lc_collate, &collation.lc_ctype)
        {
            sql.push_str(&format!(
                " (LC_COLLATE = '{}', LC_CTYPE = '{}')",
                lc_collate, lc_ctype
            ));
        }

        sql.push_str(&format!(
            " PROVIDER {}",
            match collation.provider {
                CollationProvider::Libc => "libc",
                CollationProvider::Icu => "icu",
                CollationProvider::Builtin => "builtin",
            }
        ));

        if collation.deterministic {
            sql.push_str(" DETERMINISTIC");
        }

        sql.push(';');
        Ok(sql)
    }

    fn drop_collation(&self, collation: &Collation) -> Result<String> {
        let name = if let Some(schema) = &collation.schema {
            format!("{}.{}", schema, Self::quote_identifier(&collation.name))
        } else {
            Self::quote_identifier(&collation.name)
        };
        Ok(format!("DROP COLLATION IF EXISTS {} CASCADE;", name))
    }

    fn create_rule(&self, rule: &Rule) -> Result<String> {
        let rule_name = if Self::is_reserved_keyword(&rule.name) {
            format!("\"{}\"", rule.name)
        } else {
            Self::quote_identifier(&rule.name)
        };
        
        let table_name = if let Some(schema) = &rule.schema {
            format!("{}.{}", schema, Self::quote_identifier(&rule.table))
        } else {
            Self::quote_identifier(&rule.table)
        };

        let event_str = match rule.event {
            RuleEvent::Select => "TO SELECT",
            RuleEvent::Update => "TO UPDATE",
            RuleEvent::Insert => "TO INSERT",
            RuleEvent::Delete => "TO DELETE",
        };

        let mut sql = format!("CREATE RULE {} AS ON {} {}", rule_name, table_name, event_str);

        if let Some(condition) = &rule.condition {
            sql.push_str(&format!(" WHERE ({})", condition));
        }

        if rule.instead {
            sql.push_str(" DO INSTEAD");
        } else {
            sql.push_str(" DO ALSO");
        }

        if rule.actions.len() == 1 {
            let action = &rule.actions[0];
            if action == "DO NOTHING" {
                sql.push_str(" NOTHING");
            } else {
                sql.push_str(&format!(" {}", action));
            }
        } else {
            sql.push_str(" (");
            sql.push_str(&rule.actions.join("; "));
            sql.push_str(")");
        }

        sql.push(';');
        Ok(sql)
    }

    fn drop_rule(&self, rule: &Rule) -> Result<String> {
        let rule_name = if let Some(schema) = &rule.schema {
            format!("{}.{}", schema, Self::quote_identifier(&rule.name))
        } else {
            Self::quote_identifier(&rule.name)
        };
        
        let table_name = if let Some(schema) = &rule.schema {
            format!("{}.{}", schema, Self::quote_identifier(&rule.table))
        } else {
            Self::quote_identifier(&rule.table)
        };
        
        Ok(format!(
            "DROP RULE IF EXISTS {} ON {} CASCADE;",
            rule_name, table_name
        ))
    }

    fn create_event_trigger(&self, trigger: &EventTrigger) -> Result<String> {
        let trigger_name = Self::quote_identifier(&trigger.name);

        let event_str = match trigger.event {
            EventTriggerEvent::DdlCommandStart => "DDL_COMMAND_START",
            EventTriggerEvent::DdlCommandEnd => "DDL_COMMAND_END",
            EventTriggerEvent::TableRewrite => "TABLE_REWRITE",
            EventTriggerEvent::SqlDrop => "SQL_DROP",
        };

        let mut sql = format!("CREATE EVENT TRIGGER {} ON {}", trigger_name, event_str);

        if !trigger.tags.is_empty() {
            sql.push_str(" WHEN TAG IN (");
            let tags = trigger
                .tags
                .iter()
                .map(|tag| format!("'{}'", tag))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&tags);
            sql.push_str(")");
        }

        sql.push_str(&format!(" EXECUTE FUNCTION {}();", trigger.function));

        if !trigger.enabled {
            sql.push_str(" DISABLE");
        }

        sql.push(';');
        Ok(sql)
    }

    fn drop_event_trigger(&self, trigger: &EventTrigger) -> Result<String> {
        Ok(format!(
            "DROP EVENT TRIGGER IF EXISTS {} CASCADE;",
            Self::quote_identifier(&trigger.name)
        ))
    }

    fn create_constraint_trigger(&self, trigger: &ConstraintTrigger) -> Result<String> {
        let trigger_name = if Self::is_reserved_keyword(&trigger.name) {
            format!("\"{}\"", trigger.name)
        } else {
            Self::quote_identifier(&trigger.name)
        };
        let table_name = if let Some(schema) = &trigger.schema {
            format!("{}.{}", schema, Self::quote_identifier(&trigger.table))
        } else {
            Self::quote_identifier(&trigger.table)
        };

        let events: Vec<&str> = trigger
            .events
            .iter()
            .map(|e| match e {
                TriggerEvent::Insert => "INSERT",
                TriggerEvent::Update { .. } => "UPDATE",
                TriggerEvent::Delete => "DELETE",
                TriggerEvent::Truncate => "TRUNCATE",
            })
            .collect();

        let events_str = events.join(" OR ");

        let args = if !trigger.arguments.is_empty() {
            format!("({})", trigger.arguments.join(", "))
        } else {
            "()".to_string()
        };

        let mut sql = format!(
            "CREATE CONSTRAINT TRIGGER {}",
            trigger_name
        );
        if !trigger.constraint_name.is_empty() {
            sql.push_str(&format!(" CONSTRAINT {}", trigger.constraint_name));
        }
        sql.push_str(&format!(" AFTER {} ON {}", events_str, table_name));

        if trigger.deferrable {
            sql.push_str(" DEFERRABLE");
            if trigger.initially_deferred {
                sql.push_str(" INITIALLY DEFERRED");
            } else {
                sql.push_str(" INITIALLY IMMEDIATE");
            }
        }

        sql.push_str(" FOR EACH ROW");
        sql.push_str(&format!(" EXECUTE FUNCTION {}{};", trigger.function, args));

        Ok(sql)
    }

    fn drop_constraint_trigger(&self, trigger: &ConstraintTrigger) -> Result<String> {
        let trigger_name = if let Some(schema) = &trigger.schema {
            format!("{}.{}", schema, Self::quote_identifier(&trigger.name))
        } else {
            Self::quote_identifier(&trigger.name)
        };
        
        let table_name = if let Some(schema) = &trigger.schema {
            format!("{}.{}", schema, Self::quote_identifier(&trigger.table))
        } else {
            Self::quote_identifier(&trigger.table)
        };
        
        Ok(format!(
            "DROP TRIGGER IF EXISTS {} ON {} CASCADE;",
            trigger_name, table_name
        ))
    }

    fn comment_on(&self, object_type: &str, object_name: &str, comment: &str) -> Result<String> {
        // Escape single quotes in comment
        let escaped_comment = comment.replace("'", "''");
        Ok(format!(
            "COMMENT ON {} {} IS '{}';",
            object_type, object_name, escaped_comment
        ))
    }

    fn grant_privileges(
        &self,
        privileges: &[String],
        on_object: &str,
        to_roles: &[String],
    ) -> Result<String> {
        let privs = privileges.join(", ");
        let roles = to_roles.join(", ");
        Ok(format!("GRANT {} ON {} TO {};", privs, on_object, roles))
    }

    fn revoke_privileges(
        &self,
        privileges: &[String],
        on_object: &str,
        from_roles: &[String],
    ) -> Result<String> {
        let privs = privileges.join(", ");
        let roles = from_roles.join(", ");
        Ok(format!("REVOKE {} ON {} FROM {};", privs, on_object, roles))
    }

    fn create_base_type(&self, base_type: &BaseType) -> Result<String> {
        let type_name = if let Some(schema) = &base_type.schema {
            format!("{}.{}", schema, Self::quote_identifier(&base_type.name))
        } else {
            Self::quote_identifier(&base_type.name)
        };

        let mut sql = format!("CREATE TYPE {} AS (", type_name);

        // Base types in PostgreSQL are typically created with specific attributes
        // This is a simplified implementation - real base type creation is complex
        let mut attrs = Vec::new();
        
        if let Some(len) = base_type.internal_length {
            attrs.push(format!("INTERNALLENGTH = {}", len));
        }
        
        if base_type.is_passed_by_value {
            attrs.push("PASSEDBYVALUE".to_string());
        }
        
        if !base_type.alignment.is_empty() {
            attrs.push(format!("ALIGNMENT = {}", base_type.alignment));
        }
        
        if !base_type.storage.is_empty() {
            attrs.push(format!("STORAGE = {}", base_type.storage));
        }
        
        if let Some(category) = &base_type.category {
            attrs.push(format!("CATEGORY = '{}'", category));
        }
        
        if base_type.preferred {
            attrs.push("PREFERRED".to_string());
        }
        
        if let Some(default) = &base_type.default {
            attrs.push(format!("DEFAULT = {}", default));
        }
        
        if let Some(element) = &base_type.element {
            attrs.push(format!("ELEMENT = {}", element));
        }
        
        if let Some(delimiter) = &base_type.delimiter {
            attrs.push(format!("DELIMITER = '{}'", delimiter));
        }
        
        if base_type.collatable {
            attrs.push("COLLATABLE".to_string());
        }

        sql.push_str(&attrs.join(", "));
        sql.push_str(");");

        Ok(sql)
    }

    fn drop_base_type(&self, base_type: &BaseType) -> Result<String> {
        let type_name = if let Some(schema) = &base_type.schema {
            format!("{}.{}", schema, Self::quote_identifier(&base_type.name))
        } else {
            Self::quote_identifier(&base_type.name)
        };
        Ok(format!("DROP TYPE IF EXISTS {} CASCADE;", type_name))
    }

    fn create_array_type(&self, array_type: &ArrayType) -> Result<String> {
        let type_name = if let Some(schema) = &array_type.schema {
            format!("{}.{}", schema, Self::quote_identifier(&array_type.name))
        } else {
            Self::quote_identifier(&array_type.name)
        };

        let element_type = if let Some(element_schema) = &array_type.element_schema {
            format!("{}.{}", element_schema, Self::quote_identifier(&array_type.element_type))
        } else {
            Self::quote_identifier(&array_type.element_type)
        };

        let sql = format!("CREATE TYPE {} AS ARRAY OF {};", type_name, element_type);
        Ok(sql)
    }

    fn drop_array_type(&self, array_type: &ArrayType) -> Result<String> {
        let type_name = if let Some(schema) = &array_type.schema {
            format!("{}.{}", schema, Self::quote_identifier(&array_type.name))
        } else {
            Self::quote_identifier(&array_type.name)
        };
        Ok(format!("DROP TYPE IF EXISTS {} CASCADE;", type_name))
    }

    fn create_multirange_type(&self, multirange_type: &MultirangeType) -> Result<String> {
        let type_name = if let Some(schema) = &multirange_type.schema {
            format!("{}.{}", schema, Self::quote_identifier(&multirange_type.name))
        } else {
            Self::quote_identifier(&multirange_type.name)
        };

        let range_type = if let Some(range_schema) = &multirange_type.range_schema {
            format!("{}.{}", range_schema, Self::quote_identifier(&multirange_type.range_type))
        } else {
            Self::quote_identifier(&multirange_type.range_type)
        };

        let sql = format!("CREATE TYPE {} AS MULTIRANGE OF {};", type_name, range_type);
        Ok(sql)
    }

    fn drop_multirange_type(&self, multirange_type: &MultirangeType) -> Result<String> {
        let type_name = if let Some(schema) = &multirange_type.schema {
            format!("{}.{}", schema, Self::quote_identifier(&multirange_type.name))
        } else {
            Self::quote_identifier(&multirange_type.name)
        };
        Ok(format!("DROP TYPE IF EXISTS {} CASCADE;", type_name))
    }
}
