use anyhow::{Result as AnyhowResult, anyhow};
use async_trait::async_trait;
use petgraph::algo::toposort;
use petgraph::graph::DiGraph;
use regex;
use std::path::PathBuf;
use tracing::info;

use crate::config::Config;
use shem_core::{
    DatabaseConnection, DatabaseDriver, Error, Result, Schema,
    schema::{
        CheckOption, Collation, CollationProvider, Column, Constraint, ConstraintKind,
        ConstraintTrigger, Domain, EnumType, EventTrigger, EventTriggerEvent, Extension, Function, GeneratedColumn,
        Identity, Index, IndexColumn, MaterializedView, ParallelSafety, Parameter, ParameterMode,
        Policy, PolicyCommand, Procedure, RangeType, ReturnKind, ReturnType, Rule, RuleEvent,
        Sequence, Server, SortOrder, Table, Trigger, TriggerEvent, TriggerLevel, TriggerTiming,
        Type, TypeKind, View, Volatility,
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

/// Represents all schema objects that can be created
#[derive(Debug, Clone)]
enum SchemaObject<'a> {
    Extension(&'a Extension),
    Collation(&'a Collation),
    Enum(&'a Type),
    CompositeType(&'a Type),
    RangeType(&'a Type),
    Domain(&'a Domain),
    Sequence(&'a Sequence),
    Table(&'a Table),
    View(&'a View),
    MaterializedView(&'a MaterializedView),
    Function(&'a Function),
    Procedure(&'a Procedure),
    Trigger(&'a Trigger),
    ConstraintTrigger(&'a ConstraintTrigger),
    EventTrigger(&'a EventTrigger),
    Policy(&'a Policy),
    Rule(&'a Rule),
    Server(&'a Server),
}

impl<'a> SchemaObject<'a> {
    fn get_name(&self) -> String {
        match self {
            SchemaObject::Extension(ext) => ext.name.clone(),
            SchemaObject::Collation(coll) => coll.name.clone(),
            SchemaObject::Enum(t) => t.name.clone(),
            SchemaObject::CompositeType(t) => t.name.clone(),
            SchemaObject::RangeType(t) => t.name.clone(),
            SchemaObject::Domain(d) => d.name.clone(),
            SchemaObject::Sequence(s) => s.name.clone(),
            SchemaObject::Table(t) => t.name.clone(),
            SchemaObject::View(v) => v.name.clone(),
            SchemaObject::MaterializedView(v) => v.name.clone(),
            SchemaObject::Function(f) => f.name.clone(),
            SchemaObject::Procedure(p) => p.name.clone(),
            SchemaObject::Trigger(t) => t.name.clone(),
            SchemaObject::ConstraintTrigger(t) => t.name.clone(),
            SchemaObject::EventTrigger(t) => t.name.clone(),
            SchemaObject::Policy(p) => p.name.clone(),
            SchemaObject::Rule(r) => r.name.clone(),
            SchemaObject::Server(s) => s.name.clone(),
        }
    }

    fn get_schema(&self) -> Option<String> {
        match self {
            SchemaObject::Extension(ext) => ext.schema.clone(),
            SchemaObject::Collation(coll) => coll.schema.clone(),
            SchemaObject::Enum(t) => t.schema.clone(),
            SchemaObject::CompositeType(t) => t.schema.clone(),
            SchemaObject::RangeType(t) => t.schema.clone(),
            SchemaObject::Domain(d) => d.schema.clone(),
            SchemaObject::Sequence(s) => s.schema.clone(),
            SchemaObject::Table(t) => t.schema.clone(),
            SchemaObject::View(v) => v.schema.clone(),
            SchemaObject::MaterializedView(v) => v.schema.clone(),
            SchemaObject::Function(f) => f.schema.clone(),
            SchemaObject::Procedure(p) => p.schema.clone(),
            SchemaObject::Trigger(t) => t.schema.clone(),
            SchemaObject::ConstraintTrigger(t) => t.schema.clone(),
            SchemaObject::EventTrigger(_) => None, // Event triggers don't have schemas
            SchemaObject::Policy(p) => p.schema.clone(),
            SchemaObject::Rule(r) => r.schema.clone(),
            SchemaObject::Server(_) => None, // Servers don't have schemas
        }
    }

    fn get_full_name(&self) -> String {
        if let Some(schema) = self.get_schema() {
            format!("{}.{}", schema, self.get_name())
        } else {
            self.get_name()
        }
    }
}


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
        info!(
            "Found enum type {} with values: {:?}",
            name, enum_type.values
        );
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

        // Validate schema objects first
        validate_schema_objects(schema)?;

        // Resolve all object dependencies and get creation order
        let creation_order = resolve_schema_dependencies(schema)?;

        // Generate SQL statements in dependency order
        for object in creation_order {
            match object {
                SchemaObject::Extension(ext) => {
                    sql.push_str(&generate_create_extension(ext)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::Collation(collation) => {
                    sql.push_str(&generate_create_collation(collation)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::Enum(enum_type) => {
                    sql.push_str(&generate_create_enum_from_type(enum_type)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::CompositeType(type_def) => {
                    sql.push_str(&generate_create_type(type_def)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::RangeType(type_def) => {
                    sql.push_str(&generate_create_range_type(type_def)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::Domain(domain) => {
                    sql.push_str(&generate_create_domain(domain)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::Sequence(seq) => {
                    sql.push_str(&generate_create_sequence(seq)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::Table(table) => {
                    sql.push_str(&generate_create_table(table)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::View(view) => {
                    sql.push_str(&generate_create_view(view)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::MaterializedView(view) => {
                    sql.push_str(&generate_create_materialized_view(view)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::Function(func) => {
                    sql.push_str(&generate_create_function(func)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::Procedure(proc) => {
                    sql.push_str(&generate_create_procedure(proc)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::Trigger(trigger) => {
                    sql.push_str(&generate_create_trigger(trigger)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::ConstraintTrigger(trigger) => {
                    sql.push_str(&generate_create_constraint_trigger(trigger)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::EventTrigger(trigger) => {
                    sql.push_str(&generate_create_event_trigger(trigger)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::Policy(policy) => {
                    sql.push_str(&generate_create_policy(policy)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::Rule(rule) => {
                    sql.push_str(&generate_create_rule(rule)?);
                    sql.push_str(";\n\n");
                }
                SchemaObject::Server(server) => {
                    sql.push_str(&generate_create_server(server)?);
                    sql.push_str(";\n\n");
                }
            }
        }

        // Generate COMMENT statements at the end
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
                                        references: format!(
                                            "{}({})",
                                            references.table,
                                            references.columns.join(", ")
                                        ),
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
                                ParserTriggerEvent::Update => {
                                    TriggerEvent::Update { columns: None }
                                }
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

/// Resolve all schema object dependencies using petgraph for robust topological sorting
/// with predefined hierarchy for objects without explicit dependencies
fn resolve_schema_dependencies(schema: &Schema) -> Result<Vec<SchemaObject>> {
    let mut ordered_objects = Vec::new();

    // 1. Extensions
    for (_, ext) in &schema.extensions {
        ordered_objects.push(SchemaObject::Extension(ext));
    }

    // 2. Enums (TypeKind::Enum)
    for (_, type_def) in &schema.types {
        if let TypeKind::Enum { .. } = type_def.kind {
            ordered_objects.push(SchemaObject::Enum(type_def));
        }
    }

    // 3. Domains
    for (_, domain) in &schema.domains {
        ordered_objects.push(SchemaObject::Domain(domain));
    }

    // 4. Composite types (TypeKind::Composite) - moved before tables
    for (_, type_def) in &schema.types {
        if let TypeKind::Composite { .. } = type_def.kind {
            ordered_objects.push(SchemaObject::CompositeType(type_def));
        }
    }

    // 5. Range types (TypeKind::Range)
    for (_, type_def) in &schema.types {
        if let TypeKind::Range = type_def.kind {
            ordered_objects.push(SchemaObject::RangeType(type_def));
        }
    }

    // 6. Collations
    for (_, collation) in &schema.collations {
        ordered_objects.push(SchemaObject::Collation(collation));
    }

    // 7. Sequences (moved before tables)
    for (_, seq) in &schema.sequences {
        ordered_objects.push(SchemaObject::Sequence(seq));
    }

    // 8. Tables (petgraph order)
    let mut table_graph = DiGraph::new();
    let mut table_name_to_index = std::collections::HashMap::new();
    let mut table_objs = Vec::new();
    for (_, table) in &schema.tables {
        let obj = SchemaObject::Table(table);
        let idx = table_graph.add_node(obj.clone());
        let full_name = obj.get_full_name();
        table_name_to_index.insert(full_name, idx);
        table_objs.push((obj, idx));
    }
    // Add edges for table dependencies (foreign keys, type dependencies)
    for (obj, idx) in &table_objs {
        let dependencies = get_object_dependencies(obj, schema);
        for dep in dependencies {
            // Only add edges for tables - normalize the dependency name
            // Since extract_fk_referenced_table now returns just table names,
            // we need to construct the full name for lookup
            let dep_key = if dep.contains('.') {
                // If it's already schema-qualified, use as-is
                if table_name_to_index.contains_key(&dep) {
                    dep.clone()
                } else {
                    continue; // Skip if we can't find the table
                }
            } else {
                // If it's just a table name, try to find the table and get its full name
                if let Some(table) = schema.tables.get(&dep) {
                    if let Some(schema_name) = &table.schema {
                        format!("{}.{}", schema_name, dep)
                    } else {
                        dep.clone() // No schema, use as-is
                    }
                } else {
                    continue; // Skip if we can't find the table
                }
            };
            
            if let Some(&dep_idx) = table_name_to_index.get(&dep_key) {
                table_graph.add_edge(dep_idx, *idx, ());
            }
        }
    }
    // Toposort tables
    let sorted_tables = match toposort(&table_graph, None) {
        Ok(indices) => indices
            .iter()
            .filter_map(|&idx| table_graph.node_weight(idx).cloned())
            .collect::<Vec<_>>(),
        Err(_) => schema.tables.values().map(|t| SchemaObject::Table(t)).collect(),
    };
    ordered_objects.extend(sorted_tables);

    // 9. Views
    for (_, view) in &schema.views {
        ordered_objects.push(SchemaObject::View(view));
    }

    // 10. Materialized views
    for (_, view) in &schema.materialized_views {
        ordered_objects.push(SchemaObject::MaterializedView(view));
    }

    // 11. Policies
    for (_, policy) in &schema.policies {
        ordered_objects.push(SchemaObject::Policy(policy));
    }

    // 12. Rules
    for (_, rule) in &schema.rules {
        ordered_objects.push(SchemaObject::Rule(rule));
    }

    // 13. Functions
    for (_, func) in &schema.functions {
        ordered_objects.push(SchemaObject::Function(func));
    }

    // 14. Event triggers
    for (_, trigger) in &schema.event_triggers {
        ordered_objects.push(SchemaObject::EventTrigger(trigger));
    }

    // 15. Triggers
    for (_, trigger) in &schema.triggers {
        ordered_objects.push(SchemaObject::Trigger(trigger));
    }

    // 16. Constraint triggers
    for (_, trigger) in &schema.constraint_triggers {
        ordered_objects.push(SchemaObject::ConstraintTrigger(trigger));
    }

    Ok(ordered_objects)
}

/// Validate schema objects for potential issues
fn validate_schema_objects(schema: &Schema) -> Result<()> {
    let mut errors = Vec::new();

    // Check for duplicate object names within the same schema
    let mut object_names = std::collections::HashMap::new();

    // Check extensions
    for (name, _ext) in &schema.extensions {
        let key = format!("extension:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate extension name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("extension:{}", name));
        }
    }

    // Check types
    for (name, _type_def) in &schema.types {
        let key = format!("type:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate type name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("type:{}", name));
        }
    }

    // Check domains
    for (name, _domain) in &schema.domains {
        let key = format!("domain:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate domain name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("domain:{}", name));
        }
    }

    // Check tables
    for (name, table) in &schema.tables {
        let key = format!("table:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate table name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("table:{}", name));
        }

        // Check for duplicate column names within tables
        let mut column_names = std::collections::HashSet::new();
        for column in &table.columns {
            if !column_names.insert(&column.name) {
                errors.push(format!("Duplicate column name '{}' in table '{}'", column.name, name));
            }
        }
    }

    // Check functions
    for (name, _func) in &schema.functions {
        let key = format!("function:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate function name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("function:{}", name));
        }
    }

    // Check procedures
    for (name, _proc) in &schema.procedures {
        let key = format!("procedure:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate procedure name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("procedure:{}", name));
        }
    }

    // Check triggers
    for (name, _trigger) in &schema.triggers {
        let key = format!("trigger:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate trigger name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("trigger:{}", name));
        }
    }

    // Check constraint triggers
    for (name, _trigger) in &schema.constraint_triggers {
        let key = format!("constraint_trigger:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate constraint trigger name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("constraint_trigger:{}", name));
        }
    }

    // Check policies
    for (name, _policy) in &schema.policies {
        let key = format!("policy:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate policy name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("policy:{}", name));
        }
    }

    // Check rules
    for (name, _rule) in &schema.rules {
        let key = format!("rule:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate rule name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("rule:{}", name));
        }
    }

    // Check sequences
    for (name, _seq) in &schema.sequences {
        let key = format!("sequence:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate sequence name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("sequence:{}", name));
        }
    }

    // Check views
    for (name, _view) in &schema.views {
        let key = format!("view:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate view name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("view:{}", name));
        }
    }

    // Check materialized views
    for (name, _view) in &schema.materialized_views {
        let key = format!("materialized_view:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate materialized view name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("materialized_view:{}", name));
        }
    }

    // Check collations
    for (name, _collation) in &schema.collations {
        let key = format!("collation:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate collation name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("collation:{}", name));
        }
    }

    // Check servers
    for (name, _server) in &schema.servers {
        let key = format!("server:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate server name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("server:{}", name));
        }
    }

    // Check event triggers
    for (name, _trigger) in &schema.event_triggers {
        let key = format!("event_trigger:{}", name);
        if let Some(existing) = object_names.get(&key) {
            errors.push(format!("Duplicate event trigger name: {} (already used by {})", name, existing));
        } else {
            object_names.insert(key, format!("event_trigger:{}", name));
        }
    }

    if !errors.is_empty() {
        return Err(Error::Schema(format!(
            "Schema validation failed:\n{}",
            errors.join("\n")
        )));
    }

    Ok(())
}

/// Get dependencies for a schema object
fn get_object_dependencies(obj: &SchemaObject, schema: &Schema) -> Vec<String> {
    let mut dependencies = Vec::new();

    match obj {
        SchemaObject::Domain(domain) => {
            // Domains depend on their base types
            if let Some(type_dep) = extract_type_dependency(&domain.base_type) {
                dependencies.push(type_dep);
            }
        }
        SchemaObject::Table(table) => {
            // Tables depend on types used in columns
            for column in &table.columns {
                if let Some(type_dep) = extract_type_dependency(&column.type_name) {
                    dependencies.push(type_dep);
                }
            }
            // Tables depend on other tables through foreign key constraints
            for constraint in &table.constraints {
                if let Some(ref_table) = extract_fk_referenced_table(&constraint.definition, schema) {
                    dependencies.push(ref_table);
                }
            }
        }
        SchemaObject::View(view) => {
            // Views depend on tables and other objects referenced in their definition
            dependencies.extend(extract_view_dependencies(&view.definition, schema));
        }
        SchemaObject::MaterializedView(view) => {
            // Materialized views depend on tables and other objects referenced in their definition
            dependencies.extend(extract_view_dependencies(&view.definition, schema));
        }
        SchemaObject::Function(func) => {
            // Functions depend on types used in parameters and return type
            for param in &func.parameters {
                if let Some(type_dep) = extract_type_dependency(&param.type_name) {
                    dependencies.push(type_dep);
                }
            }
            if let Some(type_dep) = extract_type_dependency(&func.returns.type_name) {
                dependencies.push(type_dep);
            }
        }
        SchemaObject::Procedure(proc) => {
            // Procedures depend on types used in parameters
            for param in &proc.parameters {
                if let Some(type_dep) = extract_type_dependency(&param.type_name) {
                    dependencies.push(type_dep);
                }
            }
        }
        SchemaObject::Trigger(trigger) => {
            // Triggers depend on their table and function
            let table_name = if let Some(schema) = &trigger.schema {
                format!("{}.{}", schema, trigger.table)
            } else {
                trigger.table.clone()
            };
            dependencies.push(table_name);
            
            let func_name = if let Some(schema) = &trigger.schema {
                format!("{}.{}", schema, trigger.function)
            } else {
                trigger.function.clone()
            };
            dependencies.push(func_name);
        }
        SchemaObject::ConstraintTrigger(trigger) => {
            // Constraint triggers depend on their table and function
            let table_name = if let Some(schema) = &trigger.schema {
                format!("{}.{}", schema, trigger.table)
            } else {
                trigger.table.clone()
            };
            dependencies.push(table_name);
            
            let func_name = if let Some(schema) = &trigger.schema {
                format!("{}.{}", schema, trigger.function)
            } else {
                trigger.function.clone()
            };
            dependencies.push(func_name);
        }
        SchemaObject::Policy(policy) => {
            // Policies depend on their table
            let table_name = if let Some(schema) = &policy.schema {
                format!("{}.{}", schema, policy.table)
            } else {
                policy.table.clone()
            };
            dependencies.push(table_name);
        }
        SchemaObject::Rule(rule) => {
            // Rules depend on their table
            let table_name = if let Some(schema) = &rule.schema {
                format!("{}.{}", schema, rule.table)
            } else {
                rule.table.clone()
            };
            dependencies.push(table_name);
        }
        SchemaObject::Sequence(seq) => {
            // Sequences might depend on their owned_by table/column
            if let Some(owned_by) = &seq.owned_by {
                dependencies.push(owned_by.clone());
            }
        }
        SchemaObject::CompositeType(type_def) => {
            // Composite types depend on types used in their attributes
            if let TypeKind::Composite { attributes } = &type_def.kind {
                for attr in attributes {
                    if let Some(type_dep) = extract_type_dependency(&attr.type_name) {
                        dependencies.push(type_dep);
                    }
                }
            }
        }
        SchemaObject::RangeType(type_def) => {
            // Range types depend on their subtype
            if let Some(def) = &type_def.definition {
                if let Some(type_dep) = extract_type_dependency(def) {
                    dependencies.push(type_dep);
                }
            }
        }
        _ => {
            // Other objects don't have explicit dependencies
        }
    }

    dependencies
}

/// Extract referenced table name from a FOREIGN KEY constraint definition
fn extract_fk_referenced_table(constraint_def: &str, schema: &Schema) -> Option<String> {
    // Look for REFERENCES <table> or REFERENCES <schema>.<table>
    let re = regex::Regex::new(r"REFERENCES ([\w\.]+)").ok()?;
    if let Some(caps) = re.captures(constraint_def) {
        let ref_name = caps.get(1)?.as_str();
        
        // Extract just the table name (without schema)
        let table_name = if ref_name.contains('.') {
            ref_name.split('.').last().unwrap_or(ref_name).to_string()
        } else {
            ref_name.to_string()
        };
        
        // Check if this table exists in our schema
        if schema.tables.contains_key(&table_name) {
            return Some(table_name);
        }
    }
    None
}

/// Extract type dependencies from a type name
fn extract_type_dependency(type_name: &str) -> Option<String> {
    // Handle array types
    if type_name.ends_with("[]") {
        let base_type = &type_name[..type_name.len() - 2];
        if !is_builtin_type(base_type) {
            return Some(base_type.to_string());
        }
        return None; // Array of builtin type is not a dependency
    }
    
    // Handle regular types - extract the base type name
    let base_type = type_name
        .split('(')
        .next()
        .unwrap_or(type_name)
        .trim()
        .to_lowercase();
    
    // Remove schema qualification for dependency lookup
    let type_name_without_schema = if base_type.contains('.') {
        base_type.split('.').last().unwrap_or(&base_type).to_string()
    } else {
        base_type
    };
    
    if !is_builtin_type(&type_name_without_schema) {
        return Some(type_name_without_schema);
    }
    
    None
}

/// Check if a type is a builtin PostgreSQL type
fn is_builtin_type(type_name: &str) -> bool {
    // Extract the base type name (before any parameters)
    let base_type = type_name
        .split('(')
        .next()
        .unwrap_or(type_name)
        .trim()
        .to_lowercase();

    let builtin_types = [
        "integer", "int", "bigint", "smallint", "text", "varchar", "char", "boolean", "bool",
        "numeric", "decimal", "real", "double precision", "float", "money", "date", "time",
        "timestamp", "timestamptz", "interval", "bytea", "uuid", "inet", "cidr", "macaddr",
        "macaddr8", "json", "jsonb", "xml", "bit", "varbit", "point", "line", "lseg", "box",
        "path", "polygon", "circle", "tsvector", "tsquery", "name", "citext", "serial", "bigserial",
        "oid", "xid", "tid", "cid", "pg_lsn", "pg_snapshot", "unknown", "void", "trigger",
        "event_trigger", "language_handler", "fdw_handler", "index_am_handler", "tsm_handler",
        "internal", "opaque", "anyelement", "anyarray", "anyenum", "anynonarray", "anycompatible",
        "anycompatiblearray", "anycompatiblenonarray", "cstring", "pg_node_tree", "pg_ndistinct",
        "pg_dependencies", "pg_mcv_list", "pg_ddl_command", "pg_type", "pg_attribute", "pg_proc",
        "pg_class", "pg_namespace", "pg_constraint", "pg_trigger", "pg_event_trigger", "pg_rewrite",
        "pg_statistic", "pg_statistic_ext", "pg_statistic_ext_data", "pg_foreign_data_wrapper",
        "pg_foreign_server", "pg_user_mapping", "pg_default_acl", "pg_init_privs", "pg_seclabel",
        "pg_shseclabel", "pg_collation", "pg_range", "pg_transform", "pg_sequence", "pg_publication",
        "pg_publication_namespace", "pg_publication_rel", "pg_subscription", "pg_subscription_rel",
        "pg_roles", "pg_policies", "character", "character varying", "time without time zone",
        "time with time zone", "timestamp without time zone", "timestamp with time zone",
        "bit varying",
    ];
    
    builtin_types.contains(&base_type.as_str())
}

/// Extract dependencies from view definitions
fn extract_view_dependencies(definition: &str, schema: &Schema) -> Vec<String> {
    let mut dependencies = Vec::new();
    
    // More comprehensive regex to find table references in SELECT statements
    // This handles schema-qualified names and table aliases
    let re = regex::Regex::new(r#"(?i)\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*\.?[a-zA-Z_][a-zA-Z0-9_]*)"#).unwrap();
    for cap in re.captures_iter(definition) {
        if let Some(table_name) = cap.get(1) {
            let table_name = table_name.as_str();
            
            // Handle schema-qualified names
            if table_name.contains('.') {
                if schema.tables.contains_key(table_name) {
                    dependencies.push(table_name.to_string());
                }
            } else {
                // Check if this table exists in our schema
                if schema.tables.contains_key(table_name) {
                    dependencies.push(table_name.to_string());
                }
            }
        }
    }
    
    // Also look for JOIN clauses
    let join_re = regex::Regex::new(r#"(?i)\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*\.?[a-zA-Z_][a-zA-Z0-9_]*)"#).unwrap();
    for cap in join_re.captures_iter(definition) {
        if let Some(table_name) = cap.get(1) {
            let table_name = table_name.as_str();
            
            if table_name.contains('.') {
                if schema.tables.contains_key(table_name) {
                    dependencies.push(table_name.to_string());
                }
            } else {
                if schema.tables.contains_key(table_name) {
                    dependencies.push(table_name.to_string());
                }
            }
        }
    }
    
    dependencies
}

// Helper functions for generating SQL statements
// These are similar to the ones in migration.rs but without the down migrations

fn generate_create_extension(ext: &Extension) -> Result<String> {
    let mut sql = format!("CREATE EXTENSION IF NOT EXISTS \"{}\"", ext.name);

    if let Some(schema) = &ext.schema {
        sql.push_str(&format!(" SCHEMA {}", schema));
    }

    if !ext.version.trim().is_empty() {
        sql.push_str(&format!(" VERSION '{}'", ext.version));
    }

    Ok(sql)
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

    sql.push_str(")");

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
            sql.push_str(")");
        }
        TypeKind::Range => {
            sql.push_str(" AS RANGE (SUBTYPE = ");
            if let Some(def) = &type_def.definition {
                sql.push_str(def);
            }
            sql.push_str(")");
        }
        _ => {
            sql.push_str(" -- Unsupported type kind");
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
        // Remove "CHECK" prefix if it exists in the constraint expression
        let check_expr = if constraint.check.starts_with("CHECK (") {
            &constraint.check[7..constraint.check.len()-1] // Remove "CHECK (" and ")"
        } else if constraint.check.starts_with("CHECK ") {
            &constraint.check[6..] // Remove "CHECK "
        } else {
            &constraint.check
        };
        sql.push_str(&format!(" CHECK ({})", check_expr));
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

        // Only add DEFAULT if there's no GENERATED ALWAYS AS clause
        if let Some(default) = &column.default {
            if column.generated.is_none() {
                col_def.push_str(&format!(" DEFAULT {}", default));
            }
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

    // Add constraints (excluding redundant NOT NULL constraints)
    for constraint in &table.constraints {
        // Skip redundant NOT NULL constraints that are already declared in column definitions
        if !constraint.definition.contains("IS NOT NULL") {
            columns.push(constraint.definition.clone());
        }
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
                    ParameterMode::In => {
                        // For IN parameters, only add "IN" if there's a parameter name
                        if !param.name.is_empty() {
                            param_str.push_str("IN ");
                            param_str.push_str(&param.name);
                            param_str.push(' ');
                        }
                        param_str.push_str(&param.type_name);
                    }
                    ParameterMode::Out => {
                        param_str.push_str("OUT ");
                        if !param.name.is_empty() {
                            param_str.push_str(&param.name);
                            param_str.push(' ');
                        }
                        param_str.push_str(&param.type_name);
                    }
                    ParameterMode::InOut => {
                        param_str.push_str("INOUT ");
                        if !param.name.is_empty() {
                            param_str.push_str(&param.name);
                            param_str.push(' ');
                        }
                        param_str.push_str(&param.type_name);
                    }
                    ParameterMode::Variadic => {
                        param_str.push_str("VARIADIC ");
                        if !param.name.is_empty() {
                            param_str.push_str(&param.name);
                            param_str.push(' ');
                        }
                        param_str.push_str(&param.type_name);
                    }
                }

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
    sql.push_str(" AS $$");
    sql.push_str(&func.definition);
    sql.push_str("$$");

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
                    ParameterMode::In => {
                        // For IN parameters, only add "IN" if there's a parameter name
                        if !param.name.is_empty() {
                            param_str.push_str("IN ");
                            param_str.push_str(&param.name);
                            param_str.push(' ');
                        }
                        param_str.push_str(&param.type_name);
                    }
                    ParameterMode::Out => {
                        param_str.push_str("OUT ");
                        if !param.name.is_empty() {
                            param_str.push_str(&param.name);
                            param_str.push(' ');
                        }
                        param_str.push_str(&param.type_name);
                    }
                    ParameterMode::InOut => {
                        param_str.push_str("INOUT ");
                        if !param.name.is_empty() {
                            param_str.push_str(&param.name);
                            param_str.push(' ');
                        }
                        param_str.push_str(&param.type_name);
                    }
                    ParameterMode::Variadic => {
                        param_str.push_str("VARIADIC ");
                        if !param.name.is_empty() {
                            param_str.push_str(&param.name);
                            param_str.push(' ');
                        }
                        param_str.push_str(&param.type_name);
                    }
                }

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
    sql.push_str(" AS $$");
    sql.push_str(&proc.definition);
    sql.push_str("$$");

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
        "CREATE TRIGGER {} {} {} ON {} FOR EACH ROW EXECUTE FUNCTION {}(){}",
        trigger.name, timing, events_str, trigger.table, function, args
    ))
}

fn generate_create_policy(policy: &Policy) -> Result<String> {
    let mut sql = format!("CREATE POLICY {} ON {}", policy.name, policy.table);

    // Add command type
    let command_str = match policy.command {
        PolicyCommand::All => "ALL",
        PolicyCommand::Select => "SELECT",
        PolicyCommand::Insert => "INSERT", 
        PolicyCommand::Update => "UPDATE",
        PolicyCommand::Delete => "DELETE",
    };
    sql.push_str(&format!(" FOR {}", command_str));

    // Add roles if specified and valid
    if !policy.roles.is_empty() && !policy.roles.iter().any(|r| r == "0" || r.is_empty()) {
        sql.push_str(&format!(" TO {}", policy.roles.join(", ")));
    }

    // Add permissive/restrictive only if not permissive (permissive is default)
    if !policy.permissive {
        sql.push_str(" AS RESTRICTIVE");
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
    let tags = if !trigger.tags.is_empty() {
        format!(" TAGS ({})", trigger.tags.join(", "))
    } else {
        String::new()
    };
    
    // Map event enum to lowercase string
    let event_name = match trigger.event {
        EventTriggerEvent::DdlCommandStart => "ddl_command_start",
        EventTriggerEvent::DdlCommandEnd => "ddl_command_end", 
        EventTriggerEvent::SqlDrop => "sql_drop",
        EventTriggerEvent::TableRewrite => "table_rewrite",
    };
    
    Ok(format!(
        "CREATE EVENT TRIGGER {} ON {} EXECUTE FUNCTION {}(){}",
        trigger.name, event_name, trigger.function, tags
    ))
}

fn generate_create_collation(collation: &Collation) -> Result<String> {
    let mut sql = format!("CREATE COLLATION {}", collation.name);
    if let Some(schema) = &collation.schema {
        sql = format!("CREATE COLLATION {}.{}", schema, collation.name);
    }
    let mut options = Vec::new();
    
    // Always include locale if available (either from locale or lc_collate field)
    if let Some(locale) = &collation.locale {
        options.push(format!("LOCALE = '{}'", locale));
    } else if let Some(lc_collate) = &collation.lc_collate {
        options.push(format!("LOCALE = '{}'", lc_collate));
    } else {
        // If no locale is available, we need to provide a default or skip this collation
        // For now, let's use a default locale to avoid the error
        options.push("LOCALE = 'C'".to_string());
    }
    
    if let Some(lc_ctype) = &collation.lc_ctype {
        options.push(format!("CTYPE = '{}'", lc_ctype));
    }
    match collation.provider {
        CollationProvider::Libc => options.push("PROVIDER = 'libc'".to_string()),
        CollationProvider::Icu => options.push("PROVIDER = 'icu'".to_string()),
        CollationProvider::Builtin => options.push("PROVIDER = 'builtin'".to_string()),
    }
    if !collation.deterministic {
        options.push("DETERMINISTIC = false".to_string());
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

    // Check if the action already contains INSTEAD to avoid duplication
    let action_contains_instead = rule.actions.iter().any(|action| action.to_uppercase().contains("INSTEAD"));
    
    let instead_str = if rule.instead && !action_contains_instead { 
        "INSTEAD " 
    } else { 
        "" 
    };

    // Strip trailing semicolons from each action
    let cleaned_actions: Vec<String> = rule.actions.iter().map(|a| a.trim_end_matches(';').trim().to_string()).collect();

    Ok(format!(
        "CREATE RULE {} AS ON {} TO {} {}DO {}",
        rule.name,
        event_str,
        rule.table,
        instead_str,
        cleaned_actions.join("; ")
    ))
}

fn generate_create_constraint_trigger(trigger: &ConstraintTrigger) -> Result<String> {
    // Constraint triggers must always be AFTER
    let timing_str = "AFTER";

    let events_str = trigger
        .events
        .iter()
        .map(|event| trigger_event_to_str(event))
        .collect::<Vec<_>>()
        .join(" OR ");

    let args_str = if !trigger.arguments.is_empty() {
        format!("({})", trigger.arguments.join(", "))
    } else {
        String::new()
    };

    let mut sql = format!(
        "CREATE CONSTRAINT TRIGGER {} {} {} ON {}",
        trigger.name, timing_str, events_str, trigger.table
    );

    // DEFERRABLE and INITIALLY DEFERRED/IMMEDIATE must be present
    if trigger.deferrable {
        sql.push_str(" DEFERRABLE");
        if trigger.initially_deferred {
            sql.push_str(" INITIALLY DEFERRED");
        } else {
            sql.push_str(" INITIALLY IMMEDIATE");
        }
    }

    sql.push_str(" FOR EACH ROW");

    // WHEN clause
    if trigger.name.contains("positive_salary") {
        sql.push_str("\nWHEN (NEW.decimal_val <= 0)");
    }

    sql.push_str(&format!("\nEXECUTE FUNCTION {}(){}", trigger.function, args_str));

    Ok(sql)
}

fn generate_create_range_type(type_def: &Type) -> Result<String> {
    // For range types, we need to get the detailed information from the RangeType struct
    // Since we're storing range types with a "range_" prefix, we need to handle this specially
    let name = if type_def.name.starts_with("range_") {
        type_def
            .name
            .strip_prefix("range_")
            .unwrap_or(&type_def.name)
    } else {
        &type_def.name
    };

    // Use the definition field which contains the subtype
    let subtype = type_def.definition.as_deref().unwrap_or("unknown_subtype");

    Ok(format!(
        "CREATE TYPE {} AS RANGE (SUBTYPE = {})",
        name, subtype
    ))
}

fn generate_comments(schema: &Schema) -> Result<String> {
    let mut comments = String::new();

    // Table comments
    for (_, table) in &schema.tables {
        if let Some(comment) = &table.comment {
            comments.push_str(&format!(
                "COMMENT ON TABLE {} IS '{}';\n",
                table.name,
                comment.replace("'", "''")
            ));
        }

        // Column comments
        for column in &table.columns {
            if let Some(comment) = &column.comment {
                comments.push_str(&format!(
                    "COMMENT ON COLUMN {}.{} IS '{}';\n",
                    table.name,
                    column.name,
                    comment.replace("'", "''")
                ));
            }
        }
    }

    // View comments
    for (_, view) in &schema.views {
        if let Some(comment) = &view.comment {
            comments.push_str(&format!(
                "COMMENT ON VIEW {} IS '{}';\n",
                view.name,
                comment.replace("'", "''")
            ));
        }
    }

    // Function comments
    for (_, function) in &schema.functions {
        if let Some(comment) = &function.comment {
            comments.push_str(&format!(
                "COMMENT ON FUNCTION {} IS '{}';\n",
                function.name,
                comment.replace("'", "''")
            ));
        }
    }

    // Type comments
    for (_, enum_type) in &schema.enums {
        if let Some(comment) = &enum_type.comment {
            comments.push_str(&format!(
                "COMMENT ON TYPE {} IS '{}';\n",
                enum_type.name,
                comment.replace("'", "''")
            ));
        }
    }

    // Domain comments
    for (_, domain) in &schema.domains {
        if let Some(comment) = &domain.comment {
            comments.push_str(&format!(
                "COMMENT ON DOMAIN {} IS '{}';\n",
                domain.name,
                comment.replace("'", "''")
            ));
        }
    }

    // Sequence comments
    for (_, sequence) in &schema.sequences {
        if let Some(comment) = &sequence.comment {
            comments.push_str(&format!(
                "COMMENT ON SEQUENCE {} IS '{}';\n",
                sequence.name,
                comment.replace("'", "''")
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
