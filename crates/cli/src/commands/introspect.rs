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
        EventTrigger, Collation, Rule, RuleEvent, ConstraintTrigger, RangeType,
        ParallelSafety, PolicyCommand, TriggerLevel, Volatility, CollationProvider,
        EnumType,
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
    for (name, enum_type) in &schema.enums {
        info!("Found enum type {} with values: {:?}", name, enum_type.values);
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
                TypeKind::Composite { attributes: _ } => {
                    sql.push_str(&generate_create_type(type_def)?);
                    sql.push_str(";\n\n");
                }
                TypeKind::Enum { values: _ } => {
                    sql.push_str(&generate_create_enum_from_type(type_def)?);
                    sql.push_str(";\n\n");
                }
                TypeKind::Domain => {
                    // Domain types should be handled by generate_create_domain
                    return Err(Error::Schema(
                        "Domain type should be handled by generate_create_domain".into(),
                    ));
                }
                TypeKind::Range => {
                    // Range types need special handling - they're stored with "range_" prefix
                    sql.push_str(&generate_create_range_type(type_def)?);
                    sql.push_str(";\n\n");
                }
                TypeKind::Base => {
                    sql.push_str(&generate_create_type(type_def)?);
                    sql.push_str(";\n\n");
                }
                _ => {}
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

        // Generate CREATE CONSTRAINT TRIGGER statements
        for (_, trigger) in &schema.constraint_triggers {
            sql.push_str(&generate_create_constraint_trigger(trigger)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE EVENT TRIGGER statements
        for (_, event_trigger) in &schema.event_triggers {
            sql.push_str(&generate_create_event_trigger(event_trigger)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE POLICY statements
        for (_, policy) in &schema.policies {
            sql.push_str(&generate_create_policy(policy)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE COLLATION statements
        for (_, collation) in &schema.collations {
            sql.push_str(&generate_create_collation(collation)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE RULE statements
        for (_, rule) in &schema.rules {
            sql.push_str(&generate_create_rule(rule)?);
            sql.push_str(";\n\n");
        }

        // Generate CREATE SERVER statements
        for (_, server) in &schema.servers {
            sql.push_str(&generate_create_server(server)?);
            sql.push_str(";\n\n");
        }

        // Generate COMMENT statements
        sql.push_str(&generate_comments(schema)?);

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
                        cascade: false,
                        comment: None,
                    };
                    schema.extensions.insert(ext.name.clone(), ext);
                }
                Statement::CreateEnum(create) => {
                    let enum_type = EnumType {
                        name: create.name,
                        schema: create.schema,
                        values: create.values,
                        comment: None,
                    };
                    schema.enums.insert(enum_type.name.clone(), enum_type);
                }
                Statement::CreateType(create) => {
                    // Handle composite types - they can be stored in a separate collection if needed
                    // For now, we'll skip them as they're not enums
                }
                Statement::CreateDomain(create) => {
                    let domain = Domain {
                        name: create.name,
                        schema: create.schema,
                        base_type: format!("{:?}", create.data_type),
                        constraints: vec![], // TODO: Parse domain constraints
                        default: None,
                        not_null: false,
                        comment: None,
                    };
                    schema.domains.insert(domain.name.clone(), domain);
                }
                Statement::CreateSequence(create) => {
                    let sequence = Sequence {
                        name: create.name,
                        schema: create.schema,
                        data_type: "bigint".to_string(),
                        start: create.start.unwrap_or(1),
                        increment: create.increment.unwrap_or(1),
                        min_value: create.min_value,
                        max_value: create.max_value,
                        cache: create.cache.unwrap_or(1),
                        cycle: create.cycle,
                        owned_by: None,
                        comment: None,
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
                                    cache: None,
                                    cycle: false,
                                }),
                                generated: col.generated.map(|g| GeneratedColumn {
                                    expression: format!("{:?}", g.expression),
                                    stored: g.stored,
                                }),
                                comment: None,
                                collation: None,
                                storage: None,
                                compression: None,
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
                                    deferrable: false,
                                    initially_deferred: false,
                                },
                                TableConstraint::Unique { columns, name } => Constraint {
                                    name: name.unwrap_or_default(),
                                    kind: ConstraintKind::Unique,
                                    definition: format!("UNIQUE ({})", columns.join(", ")),
                                    deferrable: false,
                                    initially_deferred: false,
                                },
                                TableConstraint::Check { expression, name } => Constraint {
                                    name: name.unwrap_or_default(),
                                    kind: ConstraintKind::Check,
                                    definition: format!("CHECK ({:?})", expression),
                                    deferrable: false,
                                    initially_deferred: false,
                                },
                                TableConstraint::ForeignKey {
                                    columns,
                                    references,
                                    name,
                                } => Constraint {
                                    name: name.unwrap_or_default(),
                                    kind: ConstraintKind::ForeignKey {
                                        references: format!("{}({})", references.table, references.columns.join(", ")),
                                        on_delete: None,
                                        on_update: None,
                                    },
                                    definition: format!(
                                        "FOREIGN KEY ({}) REFERENCES {}({})",
                                        columns.join(", "),
                                        references.table,
                                        references.columns.join(", ")
                                    ),
                                    deferrable: false,
                                    initially_deferred: false,
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
                                    deferrable: false,
                                    initially_deferred: false,
                                },
                            })
                            .collect(),
                        indexes: Vec::new(), // TODO: Extract indexes from CREATE INDEX statements
                        comment: None,
                        tablespace: None,
                        inherits: Vec::new(),
                        partition_by: None,
                        storage_parameters: std::collections::HashMap::new(),
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
                        comment: None,
                        security_barrier: false,
                        columns: Vec::new(),
                    };
                    schema.views.insert(view.name.clone(), view);
                }
                Statement::CreateMaterializedView(create) => {
                    let view = MaterializedView {
                        name: create.name,
                        schema: create.schema,
                        definition: create.query,
                        check_option: CheckOption::None, // Materialized views don't have check options
                        comment: None,
                        tablespace: None,
                        storage_parameters: std::collections::HashMap::new(),
                        indexes: Vec::new(),
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
                        comment: None,
                        volatility: Volatility::Volatile,
                        strict: false,
                        security_definer: false,
                        parallel_safety: ParallelSafety::Unsafe,
                        cost: None,
                        rows: None,
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
                        comment: None,
                        security_definer: false,
                    };
                    schema.procedures.insert(procedure.name.clone(), procedure);
                }
                Statement::CreateTrigger(create) => {
                    let trigger = Trigger {
                        name: create.name,
                        table: create.table,
                        schema: None,
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
                                ParserTriggerEvent::Update => TriggerEvent::Update { columns: None },
                                ParserTriggerEvent::Delete => TriggerEvent::Delete,
                                ParserTriggerEvent::Truncate => TriggerEvent::Truncate,
                            })
                            .collect(),
                        function: create.function,
                        arguments: create.arguments,
                        condition: None,
                        for_each: TriggerLevel::Row,
                        comment: None,
                    };
                    schema.triggers.insert(trigger.name.clone(), trigger);
                }
                Statement::CreatePolicy(create) => {
                    let policy = Policy {
                        name: create.name,
                        table: create.table,
                        schema: None,
                        command: PolicyCommand::All,
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
                        version: None,
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

fn generate_create_enum(enum_type: &EnumType) -> Result<String> {
    let mut sql = format!("CREATE TYPE {}", enum_type.name);

    if let Some(schema) = &enum_type.schema {
        sql = format!("CREATE TYPE {}.{}", schema, enum_type.name);
    }

    sql.push_str(" AS ENUM (");

    let values = enum_type.values
        .iter()
        .map(|v| format!("'{}'", v))
        .collect::<Vec<_>>()
        .join(", ");

    sql.push_str(&values);
    sql.push_str(");");

    Ok(sql)
}

fn generate_create_type(type_def: &Type) -> Result<String> {
    let mut sql = format!("CREATE TYPE {}", type_def.name);

    if let Some(schema) = &type_def.schema {
        sql = format!("CREATE TYPE {}.{}", schema, type_def.name);
    }

    match &type_def.kind {
        TypeKind::Composite { attributes } => {
            sql.push_str(" AS (");
            let attrs = attributes
                .iter()
                .map(|attr| format!("{} {}", attr.name, attr.type_name))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&attrs);
            sql.push_str(");");
        }
        TypeKind::Range => {
            sql.push_str(" AS RANGE (SUBTYPE = ");
            if let Some(def) = &type_def.definition {
                sql.push_str(def);
            }
            sql.push_str(");");
        }
        _ => {
            sql.push_str("; -- Unsupported type kind");
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
        sql.push_str(&format!(" CHECK ({:?})", constraint));
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
        ReturnKind::Void => {
            sql.push_str("void");
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
        .map(|e| trigger_event_to_str(e))
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

fn generate_create_event_trigger(trigger: &EventTrigger) -> Result<String> {
    // Placeholder: You may want to expand this for full event trigger support
    let tags = if !trigger.tags.is_empty() {
        format!(" TAGS ({})", trigger.tags.join(", "))
    } else {
        String::new()
    };
    let enabled = if trigger.enabled { "ENABLE" } else { "DISABLE" };
    Ok(format!(
        "CREATE EVENT TRIGGER {} ON {:?} EXECUTE FUNCTION {}{} {}",
        trigger.name, trigger.event, trigger.function, tags, enabled
    ))
}

fn generate_create_collation(collation: &Collation) -> Result<String> {
    let mut sql = format!("CREATE COLLATION {}", collation.name);
    if let Some(schema) = &collation.schema {
        sql = format!("CREATE COLLATION {}.{}", schema, collation.name);
    }
    let mut options = Vec::new();
    if let Some(locale) = &collation.locale {
        options.push(format!("LOCALE = '{}'", locale));
    }
    if let Some(lc_ctype) = &collation.lc_ctype {
        options.push(format!("CTYPE = '{}'", lc_ctype));
    }
    match collation.provider {
        CollationProvider::Libc => options.push("PROVIDER = 'libc'".to_string()),
        CollationProvider::Icu => options.push("PROVIDER = 'icu'".to_string()),
    }
    if !options.is_empty() {
        sql.push_str(&format!(" ({} )", options.join(", ")));
    }
    Ok(sql)
}

fn generate_create_rule(rule: &Rule) -> Result<String> {
    let event_str = match rule.event {
        RuleEvent::Select => "SELECT",
        RuleEvent::Update => "UPDATE",
        RuleEvent::Insert => "INSERT",
        RuleEvent::Delete => "DELETE",
    };
    
    let instead_str = if rule.instead { "INSTEAD" } else { "ALSO" };
    
    Ok(format!(
        "CREATE RULE {} AS ON {} {} {} DO {}",
        rule.name, event_str, instead_str, rule.table, rule.actions.join("; ")
    ))
}

fn generate_create_constraint_trigger(trigger: &ConstraintTrigger) -> Result<String> {
    let timing_str = match trigger.timing {
        TriggerTiming::Before => "BEFORE",
        TriggerTiming::After => "AFTER",
        TriggerTiming::InsteadOf => "INSTEAD OF",
    };
    
    let events_str = trigger.events.iter()
        .map(|event| trigger_event_to_str(event))
        .collect::<Vec<_>>()
        .join(" OR ");
    
    let args_str = if !trigger.arguments.is_empty() {
        format!("({})", trigger.arguments.join(", "))
    } else {
        String::new()
    };
    
    Ok(format!(
        "CREATE CONSTRAINT TRIGGER {} {} {} ON {} FOR EACH ROW EXECUTE FUNCTION {}{}",
        trigger.name, timing_str, events_str, trigger.table, trigger.function, args_str
    ))
}

fn generate_create_range_type(type_def: &Type) -> Result<String> {
    // For range types, we need to get the detailed information from the RangeType struct
    // Since we're storing range types with a "range_" prefix, we need to handle this specially
    let name = if type_def.name.starts_with("range_") {
        type_def.name.strip_prefix("range_").unwrap_or(&type_def.name)
    } else {
        &type_def.name
    };
    
    // This is a simplified version - in a real implementation, you'd want to store
    // the full RangeType information and use it here
    Ok(format!(
        "CREATE TYPE {} AS RANGE (SUBTYPE = {})",
        name, "unknown_subtype" // TODO: Get actual subtype from RangeType
    ))
}

fn generate_comments(schema: &Schema) -> Result<String> {
    let mut comments = String::new();
    
    // Table comments
    for (_, table) in &schema.tables {
        if let Some(comment) = &table.comment {
            comments.push_str(&format!(
                "COMMENT ON TABLE {} IS '{}';\n",
                table.name, comment.replace("'", "''")
            ));
        }
        
        // Column comments
        for column in &table.columns {
            if let Some(comment) = &column.comment {
                comments.push_str(&format!(
                    "COMMENT ON COLUMN {}.{} IS '{}';\n",
                    table.name, column.name, comment.replace("'", "''")
                ));
            }
        }
    }
    
    // View comments
    for (_, view) in &schema.views {
        if let Some(comment) = &view.comment {
            comments.push_str(&format!(
                "COMMENT ON VIEW {} IS '{}';\n",
                view.name, comment.replace("'", "''")
            ));
        }
    }
    
    // Function comments
    for (_, function) in &schema.functions {
        if let Some(comment) = &function.comment {
            comments.push_str(&format!(
                "COMMENT ON FUNCTION {} IS '{}';\n",
                function.name, comment.replace("'", "''")
            ));
        }
    }
    
    // Type comments
    for (_, enum_type) in &schema.enums {
        if let Some(comment) = &enum_type.comment {
            comments.push_str(&format!(
                "COMMENT ON TYPE {} IS '{}';\n",
                enum_type.name, comment.replace("'", "''")
            ));
        }
    }
    
    // Domain comments
    for (_, domain) in &schema.domains {
        if let Some(comment) = &domain.comment {
            comments.push_str(&format!(
                "COMMENT ON DOMAIN {} IS '{}';\n",
                domain.name, comment.replace("'", "''")
            ));
        }
    }
    
    // Sequence comments
    for (_, sequence) in &schema.sequences {
        if let Some(comment) = &sequence.comment {
            comments.push_str(&format!(
                "COMMENT ON SEQUENCE {} IS '{}';\n",
                sequence.name, comment.replace("'", "''")
            ));
        }
    }
    
    if !comments.is_empty() {
        comments.push('\n');
    }
    
    Ok(comments)
}

fn trigger_event_to_str(event: &TriggerEvent) -> &'static str {
    match event {
        TriggerEvent::Insert => "INSERT",
        TriggerEvent::Update { .. } => "UPDATE",
        TriggerEvent::Delete => "DELETE",
        TriggerEvent::Truncate => "TRUNCATE",
    }
}

fn generate_create_enum_from_type(type_def: &Type) -> Result<String> {
    let mut sql = format!("CREATE TYPE {}", type_def.name);

    if let Some(schema) = &type_def.schema {
        sql = format!("CREATE TYPE {}.{}", schema, type_def.name);
    }

    sql.push_str(" AS ENUM (");

    if let TypeKind::Enum { values } = &type_def.kind {
        let values_str = values
            .iter()
            .map(|v| format!("'{}'", v))
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&values_str);
    } else {
        return Err(Error::Schema("Expected enum type".into()));
    }

    sql.push_str(");");

    Ok(sql)
}
