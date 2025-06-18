use anyhow::{Result as AnyhowResult, anyhow};
use async_trait::async_trait;
use std::path::PathBuf;
use tracing::info;

use crate::config::Config;
use shem_core::{
    DatabaseConnection, DatabaseDriver, Error, Result, Schema,
    schema::{
        CheckOption, Column, Constraint, ConstraintKind, Domain, Extension, Function,
        GeneratedColumn, Identity, Index, IndexColumn, MaterializedView, Parameter, ParameterMode,
        Policy, Procedure, ReturnKind, ReturnType, Sequence, Server, SortOrder, Table, Trigger,
        TriggerEvent, TriggerTiming, Type, TypeKind, View,
    },
    traits::SchemaSerializer,
};
use shem_parser::{
    ast::{
        CheckOption as ParserCheckOption, Expression, FunctionReturn,
        ParameterMode as ParserParameterMode, SortOrder as ParserSortOrder, Statement,
        TableConstraint, TriggerEvent as ParserTriggerEvent, TriggerWhen,
    },
    parse_sql,
};
use shem_postgres::PostgresDriver;

pub async fn execute(
    database_url: Option<String>,
    output: PathBuf,
    config: &Config,
) -> AnyhowResult<()> {
    // Connect to database
    let driver = get_driver(config)?;
    let db_url = database_url.unwrap_or_else(|| {
        config
            .database_url
            .clone()
            .expect("Database URL must be set in config or via CLI")
    });
    let conn = driver.connect(&db_url).await?;

    // Introspect database
    info!("Introspecting database schema");
    let schema = conn.introspect().await?;

    // Create output directory if it doesn't exist
    if !output.exists() {
        std::fs::create_dir_all(&output)
            .map_err(|e| anyhow!("Failed to create output directory: {}", e))?;
    }

    // Get serializer based on config
    let serializer = get_serializer(config)?;

    // Serialize schema
    let content = serializer.serialize(&schema).await?;

    // Write schema file
    let schema_file = output.join("schema.sql");
    std::fs::write(&schema_file, content)
        .map_err(|e| anyhow!("Failed to write schema file: {}", e))?;

    info!("Schema written to {}", schema_file.display());

    // Handle enum types
    for (name, type_def) in &schema.types {
        if let Some(values) = get_enum_values(type_def) {
            info!("Found enum type {} with values: {:?}", name, values);
        }
    }

    Ok(())
}

fn get_driver(config: &Config) -> AnyhowResult<Box<dyn DatabaseDriver>> {
    // TODO: Support multiple database drivers
    Ok(Box::new(PostgresDriver::new()))
}

fn get_serializer(config: &Config) -> AnyhowResult<Box<dyn SchemaSerializer>> {
    // TODO: Support multiple serializers
    Ok(Box::new(SqlSerializer))
}

pub struct SqlSerializer;

#[async_trait]
impl SchemaSerializer for SqlSerializer {
    async fn serialize(&self, schema: &Schema) -> Result<String> {
        let mut sql = String::new();

        // Generate CREATE EXTENSION statements
        for (_, ext) in &schema.extensions {
            sql.push_str(&generate_create_extension(ext)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE TYPE statements
        for (_, type_def) in &schema.types {
            match type_def.kind {
                TypeKind::Enum => {
                    sql.push_str(&generate_create_enum(type_def)?);
                    sql.push_str(";\n\n");
                }
                _ => {
                    sql.push_str(&generate_create_type(type_def)?);
                    sql.push_str(";\n\n");
                }
            }
        }

        // Generate CREATE DOMAIN statements
        for (_, domain) in &schema.domains {
            sql.push_str(&generate_create_domain(domain)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE SEQUENCE statements
        for (_, seq) in &schema.sequences {
            sql.push_str(&generate_create_sequence(seq)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE TABLE statements
        for (_, table) in &schema.tables {
            sql.push_str(&generate_create_table(table)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE VIEW statements
        for (_, view) in &schema.views {
            sql.push_str(&generate_create_view(view)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE MATERIALIZED VIEW statements
        for (_, view) in &schema.materialized_views {
            sql.push_str(&generate_create_materialized_view(view)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE FUNCTION statements
        for (_, func) in &schema.functions {
            sql.push_str(&generate_create_function(func)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE PROCEDURE statements
        for (_, proc) in &schema.procedures {
            sql.push_str(&generate_create_procedure(proc)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE TRIGGER statements
        for (_, trigger) in &schema.triggers {
            sql.push_str(&generate_create_trigger(trigger)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE POLICY statements
        for (_, policy) in &schema.policies {
            sql.push_str(&generate_create_policy(policy)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE SERVER statements
        for (_, server) in &schema.servers {
            sql.push_str(&generate_create_server(server)?);
            sql.push_str(";\n\n");
        }

        Ok(sql)
    }

    async fn deserialize(&self, content: &str) -> Result<Schema> {
        let mut schema = Schema::new();

        // Parse SQL statements
        let statements = parse_sql(content).map_err(|e| Error::Schema(e.to_string()))?;

        for stmt in statements {
            match stmt {
                Statement::CreateExtension(create) => {
                    let ext = Extension {
                        name: create.name,
                        schema: create.schema,
                        version: create.version.unwrap_or_default(),
                    };
                    schema.extensions.insert(ext.name.clone(), ext);
                }
                Statement::CreateEnum(create) => {
                    let type_def = Type {
                        name: create.name,
                        schema: create.schema,
                        kind: TypeKind::Enum,
                    };
                    schema.types.insert(type_def.name.clone(), type_def);
                }
                Statement::CreateType(create) => {
                    let type_def = Type {
                        name: create.name,
                        schema: create.schema,
                        kind: TypeKind::Composite,
                    };
                    schema.types.insert(type_def.name.clone(), type_def);
                }
                Statement::CreateDomain(create) => {
                    let domain = Domain {
                        name: create.name,
                        schema: create.schema,
                        base_type: format!("{:?}", create.data_type),
                        constraints: vec![], // TODO: Parse domain constraints
                    };
                    schema.domains.insert(domain.name.clone(), domain);
                }
                Statement::CreateSequence(create) => {
                    let sequence = Sequence {
                        name: create.name,
                        schema: create.schema,
                        start: create.start.unwrap_or(1),
                        increment: create.increment.unwrap_or(1),
                        min_value: create.min_value,
                        max_value: create.max_value,
                        cache: create.cache.unwrap_or(1),
                        cycle: create.cycle,
                    };
                    schema.sequences.insert(sequence.name.clone(), sequence);
                }
                Statement::CreateTable(create) => {
                    let table = Table {
                        name: create.name,
                        schema: create.schema,
                        columns: create
                            .columns
                            .into_iter()
                            .map(|col| Column {
                                name: col.name,
                                type_name: format!("{:?}", col.data_type),
                                nullable: !col.not_null,
                                default: col.default.map(|e| format!("{:?}", e)),
                                identity: col.identity.map(|i| Identity {
                                    always: i.always,
                                    start: i.start.unwrap_or(1),
                                    increment: i.increment.unwrap_or(1),
                                    min_value: i.min_value,
                                    max_value: i.max_value,
                                }),
                                generated: col.generated.map(|g| GeneratedColumn {
                                    expression: format!("{:?}", g.expression),
                                    stored: g.stored,
                                }),
                            })
                            .collect(),
                        constraints: create
                            .constraints
                            .into_iter()
                            .map(|c| match c {
                                TableConstraint::PrimaryKey { columns, name } => Constraint {
                                    name: name.unwrap_or_default(),
                                    kind: ConstraintKind::PrimaryKey,
                                    definition: format!("PRIMARY KEY ({})", columns.join(", ")),
                                },
                                TableConstraint::Unique { columns, name } => Constraint {
                                    name: name.unwrap_or_default(),
                                    kind: ConstraintKind::Unique,
                                    definition: format!("UNIQUE ({})", columns.join(", ")),
                                },
                                TableConstraint::Check { expression, name } => Constraint {
                                    name: name.unwrap_or_default(),
                                    kind: ConstraintKind::Check,
                                    definition: format!("CHECK ({:?})", expression),
                                },
                                TableConstraint::ForeignKey {
                                    columns,
                                    references,
                                    name,
                                } => Constraint {
                                    name: name.unwrap_or_default(),
                                    kind: ConstraintKind::ForeignKey,
                                    definition: format!(
                                        "FOREIGN KEY ({}) REFERENCES {}({})",
                                        columns.join(", "),
                                        references.table,
                                        references.columns.join(", ")
                                    ),
                                },
                                TableConstraint::Exclusion {
                                    elements,
                                    using,
                                    name,
                                } => Constraint {
                                    name: name.unwrap_or_default(),
                                    kind: ConstraintKind::Exclusion,
                                    definition: format!(
                                        "EXCLUDE USING {} ({})",
                                        using,
                                        elements
                                            .iter()
                                            .map(|e| format!(
                                                "{:?} WITH {}",
                                                e.expression, e.operator
                                            ))
                                            .collect::<Vec<_>>()
                                            .join(", ")
                                    ),
                                },
                            })
                            .collect(),
                        indexes: Vec::new(), // TODO: Extract indexes from CREATE INDEX statements
                    };
                    schema.tables.insert(table.name.clone(), table);
                }
                Statement::CreateView(create) => {
                    let view = View {
                        name: create.name,
                        schema: create.schema,
                        definition: create.query,
                        check_option: create
                            .check_option
                            .map(|opt| match opt {
                                ParserCheckOption::Local => CheckOption::Local,
                                ParserCheckOption::Cascaded => CheckOption::Cascaded,
                            })
                            .unwrap_or(CheckOption::None),
                    };
                    schema.views.insert(view.name.clone(), view);
                }
                Statement::CreateMaterializedView(create) => {
                    let view = MaterializedView {
                        name: create.name,
                        schema: create.schema,
                        definition: create.query,
                        check_option: CheckOption::None, // Materialized views don't have check options
                    };
                    schema.materialized_views.insert(view.name.clone(), view);
                }
                Statement::CreateFunction(create) => {
                    let function = Function {
                        name: create.name,
                        schema: create.schema,
                        parameters: create
                            .parameters
                            .into_iter()
                            .map(|param| Parameter {
                                name: param.name.unwrap_or_default(),
                                type_name: format!("{:?}", param.data_type),
                                mode: param
                                    .mode
                                    .map(|mode| match mode {
                                        ParserParameterMode::In => ParameterMode::In,
                                        ParserParameterMode::Out => ParameterMode::Out,
                                        ParserParameterMode::InOut => ParameterMode::InOut,
                                        ParserParameterMode::Variadic => ParameterMode::Variadic,
                                    })
                                    .unwrap_or(ParameterMode::In),
                                default: param.default.map(|e| format!("{:?}", e)),
                            })
                            .collect(),
                        returns: match &create.returns {
                            FunctionReturn::Type(t) => ReturnType {
                                kind: ReturnKind::Scalar,
                                type_name: format!("{:?}", t),
                                is_set: false,
                            },
                            FunctionReturn::Table(cols) => ReturnType {
                                kind: ReturnKind::Table,
                                type_name: format!("{:?}", cols),
                                is_set: false,
                            },
                            FunctionReturn::SetOf(t) => ReturnType {
                                kind: ReturnKind::SetOf,
                                type_name: format!("{:?}", t),
                                is_set: true,
                            },
                        },
                        language: create.language,
                        definition: create.body,
                    };
                    schema.functions.insert(function.name.clone(), function);
                }
                Statement::CreateProcedure(create) => {
                    let procedure = Procedure {
                        name: create.name,
                        schema: create.schema,
                        parameters: create
                            .parameters
                            .into_iter()
                            .map(|param| Parameter {
                                name: param.name.unwrap_or_default(),
                                type_name: format!("{:?}", param.data_type),
                                mode: param
                                    .mode
                                    .map(|mode| match mode {
                                        ParserParameterMode::In => ParameterMode::In,
                                        ParserParameterMode::Out => ParameterMode::Out,
                                        ParserParameterMode::InOut => ParameterMode::InOut,
                                        ParserParameterMode::Variadic => ParameterMode::Variadic,
                                    })
                                    .unwrap_or(ParameterMode::In),
                                default: param.default.map(|e| format!("{:?}", e)),
                            })
                            .collect(),
                        language: create.language,
                        definition: create.body,
                    };
                    schema.procedures.insert(procedure.name.clone(), procedure);
                }
                Statement::CreateTrigger(create) => {
                    let trigger = Trigger {
                        name: create.name,
                        table: create.table,
                        timing: match create.when {
                            TriggerWhen::Before => TriggerTiming::Before,
                            TriggerWhen::After => TriggerTiming::After,
                            TriggerWhen::InsteadOf => TriggerTiming::InsteadOf,
                        },
                        events: create
                            .events
                            .into_iter()
                            .map(|event| match event {
                                ParserTriggerEvent::Insert => TriggerEvent::Insert,
                                ParserTriggerEvent::Update => TriggerEvent::Update,
                                ParserTriggerEvent::Delete => TriggerEvent::Delete,
                                ParserTriggerEvent::Truncate => TriggerEvent::Truncate,
                            })
                            .collect(),
                        function: create.function,
                        arguments: create.arguments,
                    };
                    schema.triggers.insert(trigger.name.clone(), trigger);
                }
                Statement::CreatePolicy(create) => {
                    let policy = Policy {
                        name: create.name,
                        table: create.table,
                        permissive: create.permissive,
                        roles: create.roles,
                        using: create.using.map(|e| format!("{:?}", e)),
                        check: create.with_check.map(|e| format!("{:?}", e)),
                    };
                    schema.policies.insert(policy.name.clone(), policy);
                }
                Statement::CreateServer(create) => {
                    let server = Server {
                        name: create.name,
                        foreign_data_wrapper: create.foreign_data_wrapper,
                        options: create.options,
                    };
                    schema.servers.insert(server.name.clone(), server);
                }
                _ => {}
            }
        }

        Ok(schema)
    }

    fn extension(&self) -> &'static str {
        "sql"
    }
}

// Helper functions for generating SQL statements
// These are similar to the ones in migration.rs but without the down migrations

fn generate_create_extension(ext: &Extension) -> Result<String> {
    let mut sql = format!("CREATE EXTENSION IF NOT EXISTS {}", ext.name);

    if let Some(schema) = &ext.schema {
        sql.push_str(&format!(" SCHEMA {}", schema));
    }

    if !ext.version.trim().is_empty() {
        sql.push_str(&format!(" VERSION '{}'", ext.version));
    }

    Ok(sql)
}

fn generate_create_enum(enum_type: &Type) -> Result<String> {
    // Note: This is a placeholder since we don't have enum values in the Type struct
    // In a real implementation, you'd need to store enum values in the Type struct
    let mut sql = format!("CREATE TYPE {}", enum_type.name);

    if let Some(schema) = &enum_type.schema {
        sql = format!("CREATE TYPE {}.{}", schema, enum_type.name);
    }

    sql.push_str(" AS ENUM ()");
    Ok(sql)
}

fn generate_create_type(type_def: &Type) -> Result<String> {
    let mut sql = format!("CREATE TYPE {}", type_def.name);

    if let Some(schema) = &type_def.schema {
        sql = format!("CREATE TYPE {}.{}", schema, type_def.name);
    }

    match type_def.kind {
        TypeKind::Composite => {
            sql.push_str(" AS ()");
        }
        TypeKind::Enum => {
            sql.push_str(" AS ENUM ()");
        }
        TypeKind::Domain => {
            // Domain types should be handled by generate_create_domain
            return Err(Error::Schema(
                "Domain type should be handled by generate_create_domain".into(),
            ));
        }
        TypeKind::Range => {
            sql.push_str(" AS RANGE");
        }
        TypeKind::Base => {
            sql.push_str(" AS");
        }
    }

    Ok(sql)
}

fn generate_create_domain(domain: &Domain) -> Result<String> {
    let mut sql = format!("CREATE DOMAIN {}", domain.name);

    if let Some(schema) = &domain.schema {
        sql = format!("CREATE DOMAIN {}.{}", schema, domain.name);
    }

    sql.push_str(&format!(" AS {}", domain.base_type));

    for constraint in &domain.constraints {
        sql.push_str(&format!(" CHECK ({})", constraint));
    }

    Ok(sql)
}

fn generate_create_sequence(seq: &Sequence) -> Result<String> {
    let mut sql = format!("CREATE SEQUENCE {}", seq.name);

    if let Some(schema) = &seq.schema {
        sql = format!("CREATE SEQUENCE {}.{}", schema, seq.name);
    }

    if seq.start != 1 {
        sql.push_str(&format!(" START WITH {}", seq.start));
    }

    if seq.increment != 1 {
        sql.push_str(&format!(" INCREMENT BY {}", seq.increment));
    }

    if let Some(min_val) = seq.min_value {
        sql.push_str(&format!(" MINVALUE {}", min_val));
    }

    if let Some(max_val) = seq.max_value {
        sql.push_str(&format!(" MAXVALUE {}", max_val));
    }

    if seq.cache != 1 {
        sql.push_str(&format!(" CACHE {}", seq.cache));
    }

    if seq.cycle {
        sql.push_str(" CYCLE");
    }

    Ok(sql)
}

fn generate_create_table(table: &Table) -> Result<String> {
    let mut sql = format!("CREATE TABLE {}", table.name);

    if let Some(schema) = &table.schema {
        sql = format!("CREATE TABLE {}.{}", schema, table.name);
    }

    sql.push_str(" (");

    let mut columns = Vec::new();

    // Add columns
    for column in &table.columns {
        let mut col_def = format!("{} {}", column.name, column.type_name);

        if !column.nullable {
            col_def.push_str(" NOT NULL");
        }

        if let Some(default) = &column.default {
            col_def.push_str(&format!(" DEFAULT {}", default));
        }

        if let Some(identity) = &column.identity {
            if identity.always {
                col_def.push_str(" GENERATED ALWAYS AS IDENTITY");
            } else {
                col_def.push_str(" GENERATED BY DEFAULT AS IDENTITY");
            }

            if identity.start != 1 {
                col_def.push_str(&format!(" (START WITH {})", identity.start));
            }

            if identity.increment != 1 {
                col_def.push_str(&format!(" (INCREMENT BY {})", identity.increment));
            }
        }

        if let Some(generated) = &column.generated {
            col_def.push_str(&format!(" GENERATED ALWAYS AS ({})", generated.expression));
            if generated.stored {
                col_def.push_str(" STORED");
            }
        }

        columns.push(col_def);
    }

    // Add constraints
    for constraint in &table.constraints {
        columns.push(constraint.definition.clone());
    }

    sql.push_str(&columns.join(",\n    "));
    sql.push_str("\n)");

    Ok(sql)
}

fn generate_create_view(view: &View) -> Result<String> {
    let mut sql = format!("CREATE VIEW {}", view.name);

    if let Some(schema) = &view.schema {
        sql = format!("CREATE VIEW {}.{}", schema, view.name);
    }

    sql.push_str(" AS ");
    sql.push_str(&view.definition);

    match view.check_option {
        CheckOption::Local => sql.push_str(" WITH LOCAL CHECK OPTION"),
        CheckOption::Cascaded => sql.push_str(" WITH CASCADED CHECK OPTION"),
        CheckOption::None => {}
    }

    Ok(sql)
}

fn generate_create_materialized_view(view: &MaterializedView) -> Result<String> {
    let mut sql = format!("CREATE MATERIALIZED VIEW {}", view.name);

    if let Some(schema) = &view.schema {
        sql = format!("CREATE MATERIALIZED VIEW {}.{}", schema, view.name);
    }

    sql.push_str(" AS ");
    sql.push_str(&view.definition);

    Ok(sql)
}

fn generate_create_function(func: &Function) -> Result<String> {
    let mut sql = format!("CREATE FUNCTION {}", func.name);

    if let Some(schema) = &func.schema {
        sql = format!("CREATE FUNCTION {}.{}", schema, func.name);
    }

    // Add parameters
    if !func.parameters.is_empty() {
        sql.push_str(" (");
        let params: Vec<String> = func
            .parameters
            .iter()
            .map(|param| {
                let mut param_str = String::new();

                match param.mode {
                    ParameterMode::In => param_str.push_str("IN "),
                    ParameterMode::Out => param_str.push_str("OUT "),
                    ParameterMode::InOut => param_str.push_str("INOUT "),
                    ParameterMode::Variadic => param_str.push_str("VARIADIC "),
                }

                if !param.name.is_empty() {
                    param_str.push_str(&param.name);
                    param_str.push(' ');
                }

                param_str.push_str(&param.type_name);

                if let Some(default) = &param.default {
                    param_str.push_str(&format!(" DEFAULT {}", default));
                }

                param_str
            })
            .collect();

        sql.push_str(&params.join(", "));
        sql.push_str(")");
    } else {
        sql.push_str(" ()");
    }

    // Add return type
    sql.push_str(" RETURNS ");
    match func.returns.kind {
        ReturnKind::Scalar => {
            sql.push_str(&func.returns.type_name);
        }
        ReturnKind::Table => {
            sql.push_str(&format!("TABLE ({})", func.returns.type_name));
        }
        ReturnKind::SetOf => {
            sql.push_str(&format!("SETOF {}", func.returns.type_name));
        }
    }

    // Add language
    sql.push_str(&format!(" LANGUAGE {}", func.language));

    // Add function body
    sql.push_str(" AS ");
    sql.push_str(&func.definition);

    Ok(sql)
}

fn generate_create_procedure(proc: &Procedure) -> Result<String> {
    let mut sql = format!("CREATE PROCEDURE {}", proc.name);

    if let Some(schema) = &proc.schema {
        sql = format!("CREATE PROCEDURE {}.{}", schema, proc.name);
    }

    // Add parameters
    if !proc.parameters.is_empty() {
        sql.push_str(" (");
        let params: Vec<String> = proc
            .parameters
            .iter()
            .map(|param| {
                let mut param_str = String::new();

                match param.mode {
                    ParameterMode::In => param_str.push_str("IN "),
                    ParameterMode::Out => param_str.push_str("OUT "),
                    ParameterMode::InOut => param_str.push_str("INOUT "),
                    ParameterMode::Variadic => param_str.push_str("VARIADIC "),
                }

                if !param.name.is_empty() {
                    param_str.push_str(&param.name);
                    param_str.push(' ');
                }

                param_str.push_str(&param.type_name);

                if let Some(default) = &param.default {
                    param_str.push_str(&format!(" DEFAULT {}", default));
                }

                param_str
            })
            .collect();

        sql.push_str(&params.join(", "));
        sql.push_str(")");
    } else {
        sql.push_str(" ()");
    }

    // Add language
    sql.push_str(&format!(" LANGUAGE {}", proc.language));

    // Add procedure body
    sql.push_str(" AS ");
    sql.push_str(&proc.definition);

    Ok(sql)
}

fn generate_create_trigger(trigger: &Trigger) -> Result<String> {
    let events: Vec<&str> = trigger
        .events
        .iter()
        .map(|e| match e {
            TriggerEvent::Insert => "INSERT",
            TriggerEvent::Update => "UPDATE",
            TriggerEvent::Delete => "DELETE",
            TriggerEvent::Truncate => "TRUNCATE",
        })
        .collect();

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
        "CREATE TRIGGER {} {} {} ON {} FOR EACH ROW EXECUTE FUNCTION {}{}",
        trigger.name, timing, events_str, trigger.table, function, args
    ))
}

fn generate_create_policy(policy: &Policy) -> Result<String> {
    let mut sql = format!(
        "CREATE POLICY {} ON {} AS {}",
        policy.name,
        policy.table,
        if policy.permissive {
            "PERMISSIVE"
        } else {
            "RESTRICTIVE"
        }
    );

    if !policy.roles.is_empty() {
        sql.push_str(&format!(" FOR {}", policy.roles.join(", ")));
    }

    if let Some(using) = &policy.using {
        sql.push_str(&format!(" USING ({})", using));
    }

    if let Some(check) = &policy.check {
        sql.push_str(&format!(" WITH CHECK ({})", check));
    }

    Ok(sql)
}

fn generate_create_server(server: &Server) -> Result<String> {
    let mut sql = format!(
        "CREATE SERVER {} FOREIGN DATA WRAPPER {}",
        server.name, server.foreign_data_wrapper
    );

    if !server.options.is_empty() {
        sql.push_str(" OPTIONS (");
        let options: Vec<String> = server
            .options
            .iter()
            .map(|(k, v)| format!("{} '{}'", k, v))
            .collect();
        sql.push_str(&options.join(", "));
        sql.push_str(")");
    }

    Ok(sql)
}

// Helper function to extract enum values from a type definition
fn get_enum_values(type_def: &Type) -> Option<Vec<String>> {
    match type_def.kind {
        TypeKind::Enum => {
            // TODO: Implement enum value extraction from database
            // This would require querying the database to get the enum values
            // For now, return None since we don't have enum values stored in the Type struct
            None
        }
        _ => None,
    }
}
