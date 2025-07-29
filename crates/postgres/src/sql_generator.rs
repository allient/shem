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
    Collation, ConstraintTrigger, Domain, EventTrigger, Extension, ForeignDataWrapper,
    ForeignTable, Function, Index, IndexMethod, MaterializedView, Policy, Procedure, Publication,
    Role, Rule, Sequence, Server, Subscription, Table, Tablespace, Trigger, View,
    schema::{
        ArrayType, BaseType, CheckOption, CollationProvider, CompositeType, EventTriggerEvent,
        IdentityGeneration, ParameterMode, PolicyCommand, RangeType, RuleEvent, SortOrder,
        TriggerEvent, TriggerLevel, TriggerTiming, Type,
    },
    traits::SqlGenerator,
};
use shem_core::{EnumType, Result};
use crate::quote_ident;

/// PostgreSQL SQL generator
#[derive(Debug, Clone)]
pub struct PostgresSqlGenerator;

impl PostgresSqlGenerator {
    /// Quote an identifier to handle reserved keywords and preserve case sensitivity
    fn _quote_identifier(identifier: &str) -> String {
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

    fn force_quote_identifier(identifier: &str) -> String {
        format!("\"{}\"", identifier.replace("\"", "\"\""))
    }

    fn is_reserved_keyword(name: &str) -> bool {
        // Add more reserved keywords as needed
        matches!(name.to_ascii_lowercase().as_str(), "order")
    }

    fn generate_create_base_type(&self, base_type: &BaseType) -> Result<String> {
        let type_name = Self::force_quote_identifier(&base_type.info.name);
        let mut sql = format!("CREATE TYPE {} (", type_name);
        
        // Add input and output functions (required)
        sql.push_str(&format!("INPUT = {}, OUTPUT = {}", base_type.input_fn, base_type.output_fn));
        
        // Add internal length
        sql.push_str(&format!(", INTERNALLENGTH = {}", base_type.internal_length));
        
        // Add passed by value
        sql.push_str(&format!(", PASSEDBYVALUE = {}", base_type.is_passed_by_value));
        
        // Add alignment
        sql.push_str(&format!(", ALIGNMENT = {}", base_type.alignment));
        
        // Add storage
        sql.push_str(&format!(", STORAGE = {}", base_type.storage));
        
        // Add category
        sql.push_str(&format!(", CATEGORY = '{}'", base_type.category));
        
        // Add preferred flag
        if base_type.is_preferred {
            sql.push_str(", PREFERRED = true");
        }
        
        // Add default value if specified
        if let Some(default) = &base_type.default_value {
            sql.push_str(&format!(", DEFAULT = {}", default));
        }
        
        // Add element type if it's an array type
        if let Some(element_oid) = base_type.element_type_oid {
            if element_oid > 0 {
                sql.push_str(&format!(", ELEMENT = {}", element_oid));
            }
        }
        
        // Add delimiter
        sql.push_str(&format!(", DELIMITER = '{}'", base_type.delimiter));
        
        // Add collatable flag
        sql.push_str(&format!(", COLLATABLE = {}", base_type.is_collatable));
        
        // Add receive function if specified
        if let Some(receive_fn) = &base_type.receive_fn {
            sql.push_str(&format!(", RECEIVE = {}", receive_fn));
        }
        
        // Add send function if specified
        if let Some(send_fn) = &base_type.send_fn {
            sql.push_str(&format!(", SEND = {}", send_fn));
        }
        
        // Add typmod in function if specified
        if let Some(typmod_in_fn) = &base_type.typmod_in_fn {
            sql.push_str(&format!(", TYPMOD_IN = {}", typmod_in_fn));
        }
        
        // Add typmod out function if specified
        if let Some(typmod_out_fn) = &base_type.typmod_out_fn {
            sql.push_str(&format!(", TYPMOD_OUT = {}", typmod_out_fn));
        }
        
        // Add analyze function if specified
        if let Some(analyze_fn) = &base_type.analyze_fn {
            sql.push_str(&format!(", ANALYZE = {}", analyze_fn));
        }
        
        sql.push_str(");");
        Ok(sql)
    }

    fn generate_create_composite_type(&self, composite_type: &CompositeType) -> Result<String> {
        let type_name = Self::force_quote_identifier(&composite_type.info.name);
        let mut attributes = Vec::new();
        
        for attr in &composite_type.attributes {
            let attr_name = Self::force_quote_identifier(&attr.name);
            let mut attr_def = format!("{} {}", attr_name, attr.type_name);
            
            // Add collation if specified
            if let Some(collation) = &attr.collation {
                attr_def.push_str(&format!(" COLLATE {}", collation));
            }
            
            attributes.push(attr_def);
        }
        
        Ok(format!(
            "CREATE TYPE {} AS ({});",
            type_name,
            attributes.join(", ")
        ))
    }

    fn generate_create_domain(&self, domain: &Domain) -> Result<String> {
        let domain_name = Self::force_quote_identifier(&domain.info.name);
        let mut sql = format!("CREATE DOMAIN {} AS {}", domain_name, domain.base_type);
        
        // Add collation if specified
        if let Some(collation) = &domain.collation {
            sql.push_str(&format!(" COLLATE {}", collation));
        }
        
        // Add NOT NULL if specified
        if domain.not_null {
            sql.push_str(" NOT NULL");
        }
        
        // Add default value if specified
        if let Some(default) = &domain.default {
            sql.push_str(&format!(" DEFAULT {}", default));
        }
        
        // Add constraints
        for constraint in &domain.constraints {
            sql.push_str(&format!(" {}", constraint.definition));
        }
        
        sql.push_str(";");
        Ok(sql)
    }

    fn generate_create_enum_type(&self, enum_type: &EnumType) -> Result<String> {
        let type_name = Self::force_quote_identifier(&enum_type.info.name);
        let values: Vec<String> = enum_type.values.iter()
            .map(|v| format!("'{}'", v.label))
            .collect();
        
        Ok(format!(
            "CREATE TYPE {} AS ENUM ({});",
            type_name,
            values.join(", ")
        ))
    }

    fn generate_create_range_type(&self, range_type: &RangeType) -> Result<String> {
        let type_name = Self::force_quote_identifier(&range_type.info.name);
        let mut sql = format!("CREATE TYPE {} AS RANGE (SUBTYPE = {})", type_name, range_type.subtype);
        
        // Add subtype operator class if specified
        if !range_type.subtype_opclass.is_empty() {
            sql.push_str(&format!(", SUBTYPE_OPCLASS = {}", range_type.subtype_opclass));
        }
        
        // Add collation if specified
        if let Some(collation) = &range_type.collation {
            sql.push_str(&format!(", COLLATION = {}", collation));
        }
        
        // Add canonical function if specified
        if let Some(canonical_fn) = &range_type.canonical_fn {
            sql.push_str(&format!(", CANONICAL = {}", canonical_fn));
        }
        
        // Add subtype diff function if specified
        if let Some(subtype_diff_fn) = &range_type.subtype_diff_fn {
            sql.push_str(&format!(", SUBTYPE_DIFF = {}", subtype_diff_fn));
        }
        
        sql.push_str(";");
        Ok(sql)
    }
}

impl SqlGenerator for PostgresSqlGenerator {
    fn create_type(&self, t: &Type) -> Result<String> {
        match t {
            Type::Base(base_type) => self.generate_create_base_type(base_type),
            Type::Composite(composite_type) => self.generate_create_composite_type(composite_type),
            Type::Domain(domain) => self.generate_create_domain(domain),
            Type::Enum(enum_type) => self.generate_create_enum_type(enum_type),
            Type::Range(range_type) => self.generate_create_range_type(range_type),
            Type::Pseudo(pseudo_type) => {
                // For pseudo types, generate a simple CREATE TYPE statement
                let type_name = Self::force_quote_identifier(&pseudo_type.info.name);
                Ok(format!("CREATE TYPE {};", type_name))
            }
        }
    }

    fn drop_type(&self, t: &Type) -> Result<String> {
        // The DROP statement is simpler and often more uniform
        let (info, kind) = match t {
            Type::Base(t) => (&t.info, "TYPE"),
            Type::Composite(t) => (&t.info, "TYPE"),
            Type::Domain(t) => (&t.info, "DOMAIN"),
            Type::Enum(t) => (&t.info, "TYPE"),
            Type::Range(t) => (&t.info, "TYPE"),
            Type::Pseudo(_) => return Ok(String::new()),
        };

        // Note: Base types need CASCADE due to their I/O functions.
        let cascade = if let Type::Base(_) = t {
            " CASCADE"
        } else {
            ""
        };

        Ok(format!(
            "DROP {} {}.{}{};\n",
            kind,
            quote_ident(&info.schema),
            quote_ident(&info.name),
            cascade
        ))
    }

    fn generate_create_table(&self, table: &Table) -> Result<String> {
        let table_name = Self::_quote_identifier(&table.name);
        let mut sql = format!("CREATE TABLE {} (\n    ", table_name);
        let mut columns = Vec::new();

        // Add columns
        for column in &table.columns {
            let column_name = Self::_quote_identifier(&column.name);
            let mut col_def = format!("{} {}", column_name, column.type_name);
            if column.is_not_null {
                col_def.push_str(" NOT NULL");
            }
            // Note: Column struct doesn't store actual default values, only has_default flag
            if let Some(identity) = &column.identity {
                col_def.push_str(match identity.generation {
                    IdentityGeneration::Always => " GENERATED ALWAYS AS IDENTITY",
                    IdentityGeneration::ByDefault => " GENERATED BY DEFAULT AS IDENTITY",
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

        let old_table_name = Self::_quote_identifier(&old.name);
        let new_table_name = Self::_quote_identifier(&new.name);

        // Handle column changes
        let old_columns: std::collections::HashMap<&str, &shem_core::Column> =
            old.columns.iter().map(|c| (c.name.as_str(), c)).collect();
        let new_columns: std::collections::HashMap<&str, &shem_core::Column> =
            new.columns.iter().map(|c| (c.name.as_str(), c)).collect();

        // Find dropped columns (in old but not in new)
        for (col_name, old_col) in &old_columns {
            if !new_columns.contains_key(col_name) {
                let column_name = Self::_quote_identifier(col_name);
                up_statements.push(format!(
                    "ALTER TABLE {} DROP COLUMN {}",
                    new_table_name, column_name
                ));
                // Down migration: add the column back
                let mut col_def = format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    old_table_name, column_name, old_col.type_name
                );
                if old_col.is_not_null {
                    col_def.push_str(" NOT NULL");
                }
                // Note: Column struct doesn't store actual default values, only has_default flag
                if let Some(identity) = &old_col.identity {
                    col_def.push_str(match identity.generation {
                        IdentityGeneration::Always => " GENERATED ALWAYS AS IDENTITY",
                        IdentityGeneration::ByDefault => " GENERATED BY DEFAULT AS IDENTITY",
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
                let column_name = Self::_quote_identifier(col_name);
                let mut col_def = format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    new_table_name, column_name, new_col.type_name
                );
                if new_col.is_not_null {
                    col_def.push_str(" NOT NULL");
                }
                // Note: Column struct doesn't store actual default values, only has_default flag
                if let Some(identity) = &new_col.identity {
                    col_def.push_str(match identity.generation {
                        IdentityGeneration::Always => " GENERATED ALWAYS AS IDENTITY",
                        IdentityGeneration::ByDefault => " GENERATED BY DEFAULT AS IDENTITY",
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
                let column_name = Self::_quote_identifier(col_name);

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
                if old_col.is_not_null != new_col.is_not_null {
                    if new_col.is_not_null {
                        up_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                            new_table_name, column_name
                        ));
                        down_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                            old_table_name, column_name
                        ));
                    } else {
                        up_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                            new_table_name, column_name
                        ));
                        down_statements.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                            old_table_name, column_name
                        ));
                    }
                }

                // Note: Column struct doesn't store actual default values, only has_default flag
                // Default value changes would need to be handled separately with actual default expressions

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
                            match identity.generation {
                                IdentityGeneration::Always => "ALWAYS",
                                IdentityGeneration::ByDefault => "BY DEFAULT",
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
                            match identity.generation {
                                IdentityGeneration::Always => "ALWAYS",
                                IdentityGeneration::ByDefault => "BY DEFAULT",
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

    fn generate_drop_table(&self, table: &Table) -> Result<String> {
        let table_name = Self::force_quote_identifier(&table.name);
        Ok(format!("DROP TABLE IF EXISTS {} CASCADE;", table_name))
    }

    fn create_view(&self, view: &View) -> Result<String> {
        let view_name = if view.schema == "public" {
            Self::_quote_identifier(&view.name)
        } else {
            format!("{}.{}", view.schema, Self::_quote_identifier(&view.name))
        };
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
        let view_name = Self::_quote_identifier(&view.name);

        // Use the is_populated field to determine WITH DATA vs WITH NO DATA
        let with_clause = if view.is_populated {
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
        let function_name = Self::force_quote_identifier(&function.name);
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
        let procedure_name = Self::force_quote_identifier(&procedure.name);
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

    fn create_sequence(&self, seq: &Sequence) -> Result<String> {
        let sequence_name = Self::_quote_identifier(&seq.name);

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

        let old_name = Self::_quote_identifier(&old.name);
        let new_name = Self::_quote_identifier(&new.name);

        // Handle start value changes
        if old.start != new.start {
            up_statements.push(format!(
                "ALTER SEQUENCE {} RESTART WITH {};",
                new_name, new.start
            ));
            down_statements.push(format!(
                "ALTER SEQUENCE {} RESTART WITH {};",
                old_name, old.start
            ));
        }

        // Handle increment changes
        if old.increment != new.increment {
            up_statements.push(format!(
                "ALTER SEQUENCE {} INCREMENT BY {};",
                new_name, new.increment
            ));
            down_statements.push(format!(
                "ALTER SEQUENCE {} INCREMENT BY {};",
                old_name, old.increment
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
            up_statements.push(format!("ALTER SEQUENCE {} {};", new_name, up_min));
            down_statements.push(format!("ALTER SEQUENCE {} {};", old_name, down_min));
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
            up_statements.push(format!("ALTER SEQUENCE {} {};", new_name, up_max));
            down_statements.push(format!("ALTER SEQUENCE {} {};", old_name, down_max));
        }

        // Handle cache changes
        if old.cache != new.cache {
            up_statements.push(format!("ALTER SEQUENCE {} CACHE {};", new_name, new.cache));
            down_statements.push(format!("ALTER SEQUENCE {} CACHE {};", old_name, old.cache));
        }

        // Handle cycle changes
        if old.cycle != new.cycle {
            let cycle_str = if new.cycle { "CYCLE" } else { "NO CYCLE" };
            let old_cycle_str = if old.cycle { "CYCLE" } else { "NO CYCLE" };
            up_statements.push(format!("ALTER SEQUENCE {} {};", new_name, cycle_str));
            down_statements.push(format!("ALTER SEQUENCE {} {};", old_name, old_cycle_str));
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

        if !ext.schema.is_empty() && ext.schema != "public" {
            sql.push_str(&format!(" SCHEMA {}", ext.schema));
        }

        if ext.relocatable {
            sql.push_str(" CASCADE");
        }

        sql.push(';');
        Ok(sql)
    }

    fn create_trigger(&self, trigger: &Trigger) -> Result<String> {
        let trigger_name = if Self::is_reserved_keyword(&trigger.name) {
            format!("\"{}\"", trigger.name)
        } else {
            Self::force_quote_identifier(&trigger.name)
        };
        let table_name = Self::force_quote_identifier(&trigger.table);

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
        let policy_name = if let Some(name) = &policy.name {
            Self::_quote_identifier(name)
        } else {
            return Ok(format!("ALTER TABLE {}.{} ENABLE ROW LEVEL SECURITY;", 
                policy.schema, Self::_quote_identifier(&policy.table_name)));
        };
        let table_name = Self::_quote_identifier(&policy.table_name);

        let mut sql = format!("CREATE POLICY {} ON {}", policy_name, table_name);

        // Add permissive/restrictive only if not permissive (permissive is default)
        if !policy.permissive {
            sql.push_str(" AS RESTRICTIVE");
        }

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
        let server_name = Self::force_quote_identifier(&server.name);
        let fdw = Self::force_quote_identifier(&server.foreign_data_wrapper);

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
                .map(|(k, v)| {
                    format!(
                        "{} '{}'",
                        Self::force_quote_identifier(k),
                        v.replace('\'', "''")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" OPTIONS ({})", options));
        }

        sql.push(';');
        Ok(sql)
    }

    fn drop_view(&self, view: &View) -> Result<String> {
        let name = if view.schema == "public" {
            Self::_quote_identifier(&view.name)
        } else {
            format!("{}.{}", view.schema, Self::_quote_identifier(&view.name))
        };
        Ok(format!("DROP VIEW IF EXISTS {} CASCADE;", name))
    }

    fn drop_materialized_view(&self, view: &MaterializedView) -> Result<String> {
        let name = if view.schema == "public" {
            Self::_quote_identifier(&view.name)
        } else {
            format!("{}.{}", view.schema, Self::_quote_identifier(&view.name))
        };
        Ok(format!(
            "DROP MATERIALIZED VIEW IF EXISTS {} CASCADE;",
            name
        ))
    }

    fn drop_function(&self, func: &Function) -> Result<String> {
        let name = if let Some(schema) = &func.schema {
            format!("{}.{}", schema, Self::force_quote_identifier(&func.name))
        } else {
            Self::force_quote_identifier(&func.name)
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
            format!("{}.{}", schema, Self::force_quote_identifier(&proc.name))
        } else {
            Self::force_quote_identifier(&proc.name)
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

    fn drop_sequence(&self, seq: &Sequence) -> Result<String> {
        let name = if let Some(schema) = &seq.schema {
            format!("{}.{}", schema, Self::_quote_identifier(&seq.name))
        } else {
            Self::_quote_identifier(&seq.name)
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
            format!("{}.{}", schema, Self::force_quote_identifier(&trigger.name))
        } else {
            Self::force_quote_identifier(&trigger.name)
        };

        let table_name = if let Some(schema) = &trigger.schema {
            format!(
                "{}.{}",
                schema,
                Self::force_quote_identifier(&trigger.table)
            )
        } else {
            Self::force_quote_identifier(&trigger.table)
        };

        Ok(format!(
            "DROP TRIGGER IF EXISTS {} ON {} CASCADE;",
            trigger_name, table_name
        ))
    }

    fn drop_policy(&self, policy: &Policy) -> Result<String> {
        if policy.name.is_none() {
            // This is an ENABLE ROW LEVEL SECURITY policy, so we disable RLS
            return Ok(format!("ALTER TABLE {}.{} DISABLE ROW LEVEL SECURITY;", 
                policy.schema, Self::_quote_identifier(&policy.table_name)));
        }

        let policy_name = Self::_quote_identifier(policy.name.as_ref().unwrap());
        let table_name = if policy.schema != "public" {
            format!("{}.{}", policy.schema, Self::_quote_identifier(&policy.table_name))
        } else {
            Self::_quote_identifier(&policy.table_name)
        };

        Ok(format!(
            "DROP POLICY IF EXISTS {} ON {} CASCADE;",
            policy_name, table_name
        ))
    }

    fn drop_server(&self, server: &Server) -> Result<String> {
        Ok(format!(
            "DROP SERVER IF EXISTS {} CASCADE;",
            Self::force_quote_identifier(&server.name)
        ))
    }

    fn create_index(&self, index: &Index) -> Result<String> {
        let mut sql = String::new();

        if index.unique {
            sql.push_str("CREATE UNIQUE INDEX ");
        } else {
            sql.push_str("CREATE INDEX ");
        }

        sql.push_str(&Self::force_quote_identifier(&index.name));
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
                let mut col_def = Self::force_quote_identifier(&col.name);
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
            Self::force_quote_identifier(&index.name)
        ))
    }

    fn create_collation(&self, collation: &Collation) -> Result<String> {
        let collation_name = Self::force_quote_identifier(&collation.name);
        let mut sql = format!("CREATE COLLATION {}", collation_name);

        // Add schema if not "public"
        if collation.schema != "public" {
            sql = format!("CREATE COLLATION {}.{}", collation.schema, collation_name);
        }

        // Build the options part
        let mut options = Vec::new();

        // Handle locale/ICU locale
        if let Some(icu_locale) = &collation.icu_locale {
            options.push(format!("LOCALE = '{}'", icu_locale));
        } else if let (Some(lc_collate), Some(lc_ctype)) =
            (&collation.lc_collate, &collation.lc_ctype)
        {
            options.push(format!("LC_COLLATE = '{}'", lc_collate));
            options.push(format!("LC_CTYPE = '{}'", lc_ctype));
        }

        // Add provider
        let provider_str = match collation.provider {
            CollationProvider::Libc => "libc",
            CollationProvider::Icu => "icu",
            CollationProvider::Builtin => "builtin",
            CollationProvider::Default => "default",
        };
        options.push(format!("PROVIDER = '{}'", provider_str));

        // Add deterministic flag
        if !collation.deterministic {
            options.push("DETERMINISTIC = false".to_string());
        }

        // Add ICU rules if available
        if let Some(icu_rules) = &collation.icu_rules {
            options.push(format!("RULES = '{}'", icu_rules));
        }

        // Add version if available
        if let Some(version) = &collation.version {
            options.push(format!("VERSION = '{}'", version));
        }

        // Combine options
        if !options.is_empty() {
            sql.push_str(&format!(" ({})", options.join(", ")));
        }

        sql.push(';');
        Ok(sql)
    }

    fn drop_collation(&self, collation: &Collation) -> Result<String> {
        let collation_name = if Self::is_reserved_keyword(&collation.name) {
            format!("\"{}\"", collation.name)
        } else {
            collation.name.clone()
        };

        let name = if collation.schema != "public" {
            format!("{}.{}", collation.schema, collation_name)
        } else {
            collation_name
        };
        Ok(format!("DROP COLLATION IF EXISTS {} CASCADE;", name))
    }

    fn create_rule(&self, rule: &Rule) -> Result<String> {
        // The rule definition already contains the complete CREATE RULE statement
        // We just need to ensure it ends with a semicolon
        let mut sql = rule.definition.clone();
        if !sql.trim_end().ends_with(';') {
            sql.push(';');
        }
        Ok(sql)
    }

    fn drop_rule(&self, rule: &Rule) -> Result<String> {
        let rule_name = Self::_quote_identifier(&rule.name);
        let table_name = if rule.schema != "public" {
            format!("{}.{}", rule.schema, Self::_quote_identifier(&rule.table_name))
        } else {
            Self::_quote_identifier(&rule.table_name)
        };

        Ok(format!(
            "DROP RULE IF EXISTS {} ON {} CASCADE;",
            rule_name, table_name
        ))
    }

    fn create_event_trigger(&self, trigger: &EventTrigger) -> Result<String> {
        let trigger_name = Self::force_quote_identifier(&trigger.name);

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
            Self::force_quote_identifier(&trigger.name)
        ))
    }

    fn create_constraint_trigger(&self, trigger: &ConstraintTrigger) -> Result<String> {
        let trigger_name = if Self::is_reserved_keyword(&trigger.name) {
            format!("\"{}\"", trigger.name)
        } else {
            Self::force_quote_identifier(&trigger.name)
        };
        let table_name = if let Some(schema) = &trigger.schema {
            format!(
                "{}.{}",
                schema,
                Self::force_quote_identifier(&trigger.table)
            )
        } else {
            Self::force_quote_identifier(&trigger.table)
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

        let mut sql = format!("CREATE CONSTRAINT TRIGGER {}", trigger_name);
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
            format!("{}.{}", schema, Self::force_quote_identifier(&trigger.name))
        } else {
            Self::force_quote_identifier(&trigger.name)
        };

        let table_name = if let Some(schema) = &trigger.schema {
            format!(
                "{}.{}",
                schema,
                Self::force_quote_identifier(&trigger.table)
            )
        } else {
            Self::force_quote_identifier(&trigger.table)
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

    fn create_role(&self, role: &Role) -> Result<String> {
        let role_name = Self::force_quote_identifier(&role.name);
        let mut sql = format!("CREATE ROLE {}", role_name);

        // Add role attributes
        if role.superuser {
            sql.push_str(" SUPERUSER");
        }
        if role.createdb {
            sql.push_str(" CREATEDB");
        }
        if role.createrole {
            sql.push_str(" CREATEROLE");
        }
        if role.inherit {
            sql.push_str(" INHERIT");
        }
        if role.login {
            sql.push_str(" LOGIN");
        }
        if role.replication {
            sql.push_str(" REPLICATION");
        }

        // Add connection limit
        if role.connection_limit != -1 {
            sql.push_str(&format!(" CONNECTION LIMIT {}", role.connection_limit));
        }

        // Add password
        if let Some(password) = &role.password {
            sql.push_str(&format!(" PASSWORD '{}'", password.replace('\'', "''")));
        }

        // Add valid until
        if let Some(valid_until) = &role.valid_until {
            sql.push_str(&format!(
                " VALID UNTIL '{}'",
                valid_until.replace('\'', "''")
            ));
        }

        // Add member of roles
        if !role.member_of.is_empty() {
            sql.push_str(&format!(" IN ROLE {}", role.member_of.join(", ")));
        }

        sql.push(';');
        Ok(sql)
    }

    fn drop_role(&self, role: &Role) -> Result<String> {
        let role_name = Self::force_quote_identifier(&role.name);
        Ok(format!("DROP ROLE IF EXISTS {} CASCADE;", role_name))
    }

    fn create_tablespace(&self, tablespace: &Tablespace) -> Result<String> {
        let tablespace_name = Self::force_quote_identifier(&tablespace.name);
        let location = tablespace.location.replace('\'', "''");
        let owner = Self::force_quote_identifier(&tablespace.owner);

        let mut sql = format!(
            "CREATE TABLESPACE {} OWNER {} LOCATION '{}'",
            tablespace_name, owner, location
        );

        // Add options if present
        if !tablespace.options.is_empty() {
            let options = tablespace
                .options
                .iter()
                .map(|(k, v)| format!("{} = {}", k, v))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" WITH ({})", options));
        }

        sql.push(';');

        // Add comment if present
        if let Some(comment) = &tablespace.comment {
            sql.push_str(&format!(
                "\nCOMMENT ON TABLESPACE {} IS '{}';",
                tablespace_name,
                comment.replace('\'', "''")
            ));
        }

        Ok(sql)
    }

    fn drop_tablespace(&self, tablespace: &Tablespace) -> Result<String> {
        let tablespace_name = Self::force_quote_identifier(&tablespace.name);
        Ok(format!(
            "DROP TABLESPACE IF EXISTS {} CASCADE;",
            tablespace_name
        ))
    }

    fn create_publication(&self, publication: &Publication) -> Result<String> {
        let publication_name = Self::force_quote_identifier(&publication.name);
        let mut sql = format!("CREATE PUBLICATION {}", publication_name);

        // Add FOR ALL TABLES if specified
        if publication.all_tables {
            sql.push_str(" FOR ALL TABLES");
        } else if !publication.tables.is_empty() {
            // Add specific tables
            let tables = publication
                .tables
                .iter()
                .map(|t| Self::force_quote_identifier(t))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" FOR TABLE {}", tables));
        }

        // Add operation types
        let mut operations = Vec::new();
        if publication.insert {
            operations.push("INSERT");
        }
        if publication.update {
            operations.push("UPDATE");
        }
        if publication.delete {
            operations.push("DELETE");
        }
        if publication.truncate {
            operations.push("TRUNCATE");
        }

        if !operations.is_empty() {
            sql.push_str(&format!(" WITH ({})", operations.join(", ")));
        }

        sql.push(';');
        Ok(sql)
    }

    fn drop_publication(&self, publication: &Publication) -> Result<String> {
        let publication_name = Self::force_quote_identifier(&publication.name);
        Ok(format!(
            "DROP PUBLICATION IF EXISTS {} CASCADE;",
            publication_name
        ))
    }

    fn create_subscription(&self, subscription: &Subscription) -> Result<String> {
        let subscription_name = Self::force_quote_identifier(&subscription.name);
        let connection_string = subscription.connection.replace('\'', "''");
        let publications = subscription.publication.join(", ");

        let mut sql = format!(
            "CREATE SUBSCRIPTION {} CONNECTION '{}' PUBLICATION {}",
            subscription_name, connection_string, publications
        );

        // Add enabled/disabled
        if !subscription.enabled {
            sql.push_str(" DISABLED");
        }

        // Add slot name if present
        if let Some(slot_name) = &subscription.slot_name {
            sql.push_str(&format!(
                " SLOT_NAME {}",
                Self::force_quote_identifier(slot_name)
            ));
        }

        sql.push(';');
        Ok(sql)
    }

    fn drop_subscription(&self, subscription: &Subscription) -> Result<String> {
        let subscription_name = Self::force_quote_identifier(&subscription.name);
        Ok(format!(
            "DROP SUBSCRIPTION IF EXISTS {} CASCADE;",
            subscription_name
        ))
    }

    fn create_foreign_table(&self, foreign_table: &ForeignTable) -> Result<String> {
        let table_name = if let Some(schema) = &foreign_table.schema {
            format!(
                "{}.{}",
                Self::force_quote_identifier(schema),
                Self::force_quote_identifier(&foreign_table.name)
            )
        } else {
            Self::force_quote_identifier(&foreign_table.name)
        };

        let server_name = Self::force_quote_identifier(&foreign_table.server);

        let mut sql = format!("CREATE FOREIGN TABLE {} (", table_name);

        // Add columns
        let columns = foreign_table
            .columns
            .iter()
            .map(|col| {
                let col_name = Self::force_quote_identifier(&col.name);
                format!("{} {}", col_name, col.type_name)
            })
            .collect::<Vec<_>>()
            .join(", ");

        sql.push_str(&columns);
        sql.push_str(&format!(") SERVER {}", server_name));

        // Add options if present
        if !foreign_table.options.is_empty() {
            let options = foreign_table
                .options
                .iter()
                .map(|(k, v)| format!("{} = {}", k, v))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" OPTIONS ({})", options));
        }

        sql.push(';');

        // Add comment if present
        // if let Some(comment) = &foreign_table.comment {
        //     sql.push_str(&format!(
        //         "\nCOMMENT ON FOREIGN TABLE {} IS '{}';",
        //         table_name,
        //         comment.replace('\'', "''")
        //     ));
        // }

        Ok(sql)
    }

    fn drop_foreign_table(&self, foreign_table: &ForeignTable) -> Result<String> {
        let table_name = if let Some(schema) = &foreign_table.schema {
            format!(
                "{}.{}",
                Self::force_quote_identifier(schema),
                Self::force_quote_identifier(&foreign_table.name)
            )
        } else {
            Self::force_quote_identifier(&foreign_table.name)
        };
        Ok(format!(
            "DROP FOREIGN TABLE IF EXISTS {} CASCADE;",
            table_name
        ))
    }

    fn create_foreign_data_wrapper(&self, fdw: &ForeignDataWrapper) -> Result<String> {
        let fdw_name = Self::force_quote_identifier(&fdw.name);

        let mut sql = format!("CREATE FOREIGN DATA WRAPPER {}", fdw_name);

        // Add handler if present
        if let Some(handler) = &fdw.handler {
            sql.push_str(&format!(
                " HANDLER {}",
                Self::force_quote_identifier(handler)
            ));
        }

        // Add validator if present
        if let Some(validator) = &fdw.validator {
            sql.push_str(&format!(
                " VALIDATOR {}",
                Self::force_quote_identifier(validator)
            ));
        }

        // Add options if present
        if !fdw.options.is_empty() {
            let options = fdw
                .options
                .iter()
                .map(|(k, v)| format!("{} = {}", k, v))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" OPTIONS ({})", options));
        }

        sql.push(';');
        Ok(sql)
    }

    fn drop_foreign_data_wrapper(&self, fdw: &ForeignDataWrapper) -> Result<String> {
        let fdw_name = Self::force_quote_identifier(&fdw.name);
        Ok(format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            fdw_name
        ))
    }
}
