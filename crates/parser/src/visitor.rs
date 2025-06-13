use anyhow::{Result, Context};
use pg_query::{Node, ParseResult, protobuf::{self, node}};
use crate::ast::{self, *};
use std::collections::HashMap;

/// Parse statements from PostgreSQL parse result
pub fn parse_statements(result: &ParseResult) -> Result<Vec<Statement>> {
    let mut statements = Vec::new();
    
    for stmt in &result.protobuf.stmts {
        let node = stmt.stmt.as_ref().context("Missing statement node")?;
        let statement = match node.node.as_ref().context("Missing node variant")? {
            node::Node::CreateStmt(stmt) => parse_create_table(stmt)?,
            node::Node::ViewStmt(stmt) => parse_create_view(stmt)?,
            node::Node::CreateFunctionStmt(stmt) => parse_create_function(stmt)?,
            node::Node::CreateEnumStmt(stmt) => parse_create_enum(stmt)?,
            node::Node::CompositeTypeStmt(stmt) => parse_create_type(stmt)?,
            node::Node::CreateDomainStmt(stmt) => parse_create_domain(stmt)?,
            node::Node::CreateSeqStmt(stmt) => parse_create_sequence(stmt)?,
            node::Node::CreateExtensionStmt(stmt) => parse_create_extension(stmt)?,
            node::Node::CreateTrigStmt(stmt) => parse_create_trigger(stmt)?,
            node::Node::CreatePolicyStmt(stmt) => parse_create_policy(stmt)?,
            node::Node::CreateForeignServerStmt(stmt) => parse_create_server(stmt)?,
            node::Node::AlterTableStmt(stmt) => parse_alter_table(stmt)?,
            node::Node::DropStmt(stmt) => parse_drop_object(stmt)?,
            _ => continue,
        };
        statements.push(statement);
    }
    
    Ok(statements)
}

fn parse_create_table(stmt: &protobuf::CreateStmt) -> Result<Statement> {
    let name = get_qualified_name(stmt.relation.as_ref().context("Missing relation")?)?;
    let mut columns = Vec::new();
    let mut constraints = Vec::new();
    
    // Parse table elements (columns and constraints)
    for element in &stmt.table_elts {
        match element.node.as_ref().context("Empty node")? {
            node::Node::ColumnDef(stmt) => {
                columns.push(parse_column_def(&**stmt)?);
            }
            node::Node::Constraint(stmt) => {
                constraints.push(parse_table_constraint(&**stmt)?);
            }
            _ => continue,
        }
    }
    
    // Parse inheritance
    let inherits = stmt.inh_relations.iter()
        .filter_map(|rel| {
            if let Some(node::Node::RangeVar(range_var)) = &rel.node {
                get_qualified_name(range_var).ok()
            } else {
                None
            }
        })
        .collect();
    
    // Parse partition info
    let partition_by = if let Some(part) = &stmt.partspec {
        Some(parse_partition_definition(&Node {
            node: Some(node::Node::PartitionSpec(part.clone())),
        })?)
    } else {
        None
    };
    
    // Parse table options
    let with_options = parse_with_options(&stmt.options)?;
    
    Ok(Statement::CreateTable(CreateTable {
        name,
        schema: None, // TODO: Parse schema
        columns,
        constraints,
        partition_by,
        inherits,
        with_options,
        tablespace: None, // TODO: Parse tablespace
        comment: None, // TODO: Parse comment
    }))
}

fn parse_create_view(stmt: &protobuf::ViewStmt) -> Result<Statement> {
    let name = get_qualified_name(stmt.view.as_ref().context("Missing view name")?)?;
    let columns = stmt.aliases.iter()
        .filter_map(|alias| {
            if let Some(node::Node::String(str_val)) = &alias.node {
                Some(str_val.sval.clone())
            } else {
                None
            }
        })
        .collect();
    
    let query = stmt.query.as_ref()
        .context("Missing view query")?
        .to_string_representation()?;
    
    let with_options = parse_with_options(&stmt.options)?;
    
    let check_option = match protobuf::ViewCheckOption::try_from(stmt.with_check_option) {
        Ok(protobuf::ViewCheckOption::NoCheckOption) => None,
        Ok(protobuf::ViewCheckOption::LocalCheckOption) => Some(CheckOption::Local),
        Ok(protobuf::ViewCheckOption::CascadedCheckOption) => Some(CheckOption::Cascaded),
        _ => None,
    };
    
    Ok(Statement::CreateView(CreateView {
        name,
        schema: None, // TODO: Parse schema
        columns,
        query,
        with_options,
        check_option,
        comment: None, // TODO: Parse comment
    }))
}

fn parse_create_function(stmt: &protobuf::CreateFunctionStmt) -> Result<Statement> {
    let name = get_qualified_name_from_nodes(&stmt.funcname)?;
    let mut parameters = Vec::new();
    
    // Parse function parameters
    for param in &stmt.parameters {
        if let Some(node) = &param.node {
            if let node::Node::FunctionParameter(param) = node {
                parameters.push(parse_function_parameter(&**param)?);
            }
        }
    }
    
    // Parse return type
    let returns = if let Some(return_type) = &stmt.return_type {
        parse_function_return(&Node {
            node: Some(node::Node::TypeName(return_type.clone())),
        })?
    } else {
        FunctionReturn::Type(DataType::Text) // Default to Text instead of Void
    };
    
    // Parse function options
    let (language, behavior, security, parallel, cost, rows, support) = 
        parse_function_options(&stmt.options)?;
    
    // Get function body from options
    let body = stmt.options.iter()
        .find_map(|opt| {
            if let Some(node::Node::DefElem(def)) = &opt.node {
                if def.defname == "as" {
                    if let Some(arg) = &def.arg {
                        if let Some(node::Node::String(str_val)) = &arg.node {
                            Some(str_val.sval.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        })
        .context("Missing function body")?;
    
    Ok(Statement::CreateFunction(CreateFunction {
        name,
        schema: None, // TODO: Parse schema
        parameters,
        returns,
        language,
        behavior,
        security,
        parallel,
        cost,
        rows,
        support,
        body,
        comment: None, // TODO: Parse comment
    }))
}

// Helper functions for parsing

fn get_qualified_name_from_nodes(nodes: &[Node]) -> Result<String> {
    let mut parts = Vec::new();
    for node in nodes {
        if let Some(node::Node::String(str_val)) = &node.node {
            parts.push(str_val.sval.clone());
        }
    }
    Ok(parts.join("."))
}

fn get_qualified_name(rel: &protobuf::RangeVar) -> Result<String> {
    let mut name = String::new();
    if !rel.schemaname.is_empty() {
        name.push_str(&rel.schemaname);
        name.push('.');
    }
    name.push_str(&rel.relname);
    Ok(name)
}

fn parse_column_def(col: &protobuf::ColumnDef) -> Result<ColumnDefinition> {
    let name = col.colname.clone();
    let data_type = if let Some(type_name) = &col.type_name {
        parse_data_type(type_name)?
    } else {
        DataType::Text // Default to Text if no type specified
    };
    
    let default = if let Some(expr) = &col.raw_default {
        Some(parse_expression(expr)?)
    } else {
        None
    };
    
    let not_null = col.is_not_null;
    
    // Handle generated column - col.generated is a String
    let generated = if !col.generated.is_empty() {
        Some(parse_generated_column(&Node {
            node: Some(node::Node::String(protobuf::String {
                sval: col.generated.clone(),
            })),
        })?)
    } else {
        None
    };
    
    // Handle identity column - col.identity is a String
    let identity = if !col.identity.is_empty() {
        Some(parse_identity_column(&Node {
            node: Some(node::Node::String(protobuf::String {
                sval: col.identity.clone(),
            })),
        })?)
    } else {
        None
    };
    
    Ok(ColumnDefinition {
        name,
        data_type,
        default,
        not_null,
        generated,
        identity,
        comment: None, // TODO: Parse comment
    })
}

fn parse_data_type(type_name: &protobuf::TypeName) -> Result<DataType> {
    // TODO: Implement full type parsing
    Ok(DataType::Text) // Placeholder
}

fn parse_expression(expr: &protobuf::Node) -> Result<Expression> {
    // TODO: Implement full expression parsing
    Ok(Expression::Literal(Literal::Null)) // Placeholder
}

fn parse_generated_column(expr: &protobuf::Node) -> Result<GeneratedColumn> {
    // TODO: Implement generated column parsing
    Ok(GeneratedColumn {
        expression: Expression::Literal(Literal::Null),
        stored: false,
    })
}

fn parse_identity_column(expr: &protobuf::Node) -> Result<IdentityColumn> {
    // TODO: Implement identity column parsing
    Ok(IdentityColumn {
        always: false,
        start: None,
        increment: None,
        min_value: None,
        max_value: None,
        cache: None,
        cycle: false,
    })
}

fn parse_table_constraint(constraint: &protobuf::Constraint) -> Result<TableConstraint> {
    // TODO: Implement constraint parsing
    Ok(TableConstraint::PrimaryKey {
        columns: Vec::new(),
        name: None,
    })
}

fn parse_partition_definition(part: &protobuf::Node) -> Result<PartitionDefinition> {
    // TODO: Implement partition parsing
    Ok(PartitionDefinition {
        strategy: PartitionStrategy::Range,
        columns: Vec::new(),
        partitions: Vec::new(),
    })
}

fn parse_with_options(options: &[protobuf::Node]) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    // TODO: Implement options parsing
    Ok(map)
}

fn parse_function_parameter(param: &protobuf::FunctionParameter) -> Result<FunctionParameter> {
    // Handle name field - param.name is a String
    let name = if !param.name.is_empty() {
        Some(param.name.clone())
    } else {
        None
    };
    
    let data_type = if let Some(type_name) = &param.arg_type {
        parse_data_type(type_name)?
    } else {
        DataType::Text // Default to Text if no type specified
    };
    
    let mode = match protobuf::FunctionParameterMode::try_from(param.mode) {
        Ok(protobuf::FunctionParameterMode::FuncParamIn) => Some(ParameterMode::In),
        Ok(protobuf::FunctionParameterMode::FuncParamOut) => Some(ParameterMode::Out),
        Ok(protobuf::FunctionParameterMode::FuncParamInout) => Some(ParameterMode::InOut),
        Ok(protobuf::FunctionParameterMode::FuncParamVariadic) => Some(ParameterMode::Variadic),
        _ => None,
    };
    
    Ok(FunctionParameter {
        name,
        data_type,
        default: None, // TODO: Parse default
        mode,
    })
}

fn parse_function_return(return_type: &protobuf::Node) -> Result<FunctionReturn> {
    // TODO: Implement return type parsing
    Ok(FunctionReturn::Type(DataType::Text))
}

fn parse_function_options(options: &[protobuf::Node]) -> Result<(
    String,
    FunctionBehavior,
    SecurityType,
    ParallelType,
    Option<u32>,
    Option<u32>,
    Option<String>,
)> {
    // TODO: Implement options parsing
    Ok((
        "sql".to_string(),
        FunctionBehavior::Volatile,
        SecurityType::Invoker,
        ParallelType::Unsafe,
        None,
        None,
        None,
    ))
}

// Additional parsing functions for other statement types
fn parse_create_enum(stmt: &protobuf::CreateEnumStmt) -> Result<Statement> {
    let name = get_qualified_name_from_nodes(&stmt.type_name)?;
    let values = stmt.vals.iter()
        .filter_map(|val| {
            if let Some(node::Node::String(str_val)) = &val.node {
                Some(str_val.sval.clone())
            } else {
                None
            }
        })
        .collect();
    
    Ok(Statement::CreateEnum(CreateEnum {
        name,
        schema: None, // TODO: Parse schema
        values,
    }))
}

fn parse_create_type(stmt: &protobuf::CompositeTypeStmt) -> Result<Statement> {
    let name = if let Some(typevar) = &stmt.typevar {
        get_qualified_name(typevar)?
    } else {
        return Err(anyhow::anyhow!("Missing type name"));
    };
    
    let mut attributes = Vec::new();
    for element in &stmt.coldeflist {
        if let Some(node) = &element.node {
            if let node::Node::ColumnDef(col) = node {
                attributes.push(TypeAttribute {
                    name: col.colname.clone(),
                    data_type: if let Some(type_name) = &col.type_name {
                        parse_data_type(type_name)?
                    } else {
                        DataType::Text
                    },
                    collation: None, // TODO: Parse collation
                });
            }
        }
    }
    
    Ok(Statement::CreateType(CreateType {
        name,
        schema: None, // TODO: Parse schema
        attributes,
        internallength: None,
        input: None,
        output: None,
        receive: None,
        send: None,
        typmod_in: None,
        typmod_out: None,
        analyze: None,
        alignment: None,
        storage: None,
        category: None,
        preferred: None,
        default: None,
        element: None,
        delimiter: None,
        collatable: None,
    }))
}

fn parse_create_domain(stmt: &protobuf::CreateDomainStmt) -> Result<Statement> {
    let name = get_qualified_name_from_nodes(&stmt.domainname)?;
    let data_type = if let Some(type_name) = &stmt.type_name {
        parse_data_type(type_name)?
    } else {
        DataType::Text
    };
    
    let default = None; // TODO: Parse default from constraints
    let not_null = false; // TODO: Parse from constraints
    let check = None; // TODO: Parse from constraints
    
    Ok(Statement::CreateDomain(CreateDomain {
        name,
        schema: None, // TODO: Parse schema
        data_type,
        default,
        not_null,
        check,
        comment: None, // TODO: Parse comment
    }))
}

fn parse_create_sequence(stmt: &protobuf::CreateSeqStmt) -> Result<Statement> {
    let name = if let Some(sequence) = &stmt.sequence {
        get_qualified_name(sequence)?
    } else {
        return Err(anyhow::anyhow!("Missing sequence name"));
    };
    
    let with_options = parse_with_options(&stmt.options)?;
    
    Ok(Statement::CreateSequence(CreateSequence {
        name,
        schema: None, // TODO: Parse schema
        start: None, // TODO: Parse from options
        increment: None, // TODO: Parse from options
        min_value: None, // TODO: Parse from options
        max_value: None, // TODO: Parse from options
        cache: None, // TODO: Parse from options
        cycle: false, // TODO: Parse from options
        owned_by: None, // TODO: Parse from options
    }))
}

fn parse_create_extension(stmt: &protobuf::CreateExtensionStmt) -> Result<Statement> {
    let name = stmt.extname.clone();
    let schema = None; // TODO: Parse schema from options
    let version = None; // TODO: Parse version from options
    let cascade = false; // TODO: Parse cascade from options
    
    Ok(Statement::CreateExtension(ast::CreateExtension {
        name,
        schema,
        version,
        cascade,
    }))
}

fn parse_create_trigger(stmt: &protobuf::CreateTrigStmt) -> Result<Statement> {
    let name = stmt.trigname.clone();
    let table = get_qualified_name(stmt.relation.as_ref().context("Missing relation")?)?;
    let schema = None; // TODO: Parse schema
    let when = TriggerWhen::Before; // TODO: Parse from timing
    let events = vec![TriggerEvent::Insert]; // TODO: Parse from events
    let function = get_qualified_name_from_nodes(&stmt.funcname)?;
    let arguments = Vec::new(); // TODO: Parse from args
    
    Ok(Statement::CreateTrigger(CreateTrigger {
        name,
        table,
        schema,
        when,
        events,
        function,
        arguments,
    }))
}

fn parse_create_policy(stmt: &protobuf::CreatePolicyStmt) -> Result<Statement> {
    let name = stmt.policy_name.clone();
    let table = get_qualified_name(stmt.table.as_ref().context("Missing table")?)?;
    let schema = None; // TODO: Parse schema
    let permissive = true; // TODO: Parse from policy type
    
    Ok(Statement::CreatePolicy(CreatePolicy {
        name,
        table,
        schema,
        permissive,
        roles: Vec::new(), // TODO: Parse roles
        using: None, // TODO: Parse using expression
        with_check: None, // TODO: Parse with check expression
    }))
}

fn parse_create_server(stmt: &protobuf::CreateForeignServerStmt) -> Result<Statement> {
    let name = stmt.servername.clone();
    let server_type = None; // TODO: Parse from options
    let version = None; // TODO: Parse from options
    let foreign_data_wrapper = stmt.fdwname.clone();
    let options = parse_with_options(&stmt.options)?;
    
    Ok(Statement::CreateServer(CreateServer {
        name,
        server_type,
        version,
        foreign_data_wrapper,
        options,
    }))
}

fn parse_alter_table(stmt: &protobuf::AlterTableStmt) -> Result<Statement> {
    let name = get_qualified_name(stmt.relation.as_ref().context("Missing relation")?)?;
    let schema = None; // TODO: Parse schema
    let mut actions = Vec::new();
    
    for cmd in &stmt.cmds {
        if let Some(node) = &cmd.node {
            actions.push(parse_alter_table_action(&Node {
                node: Some(node.clone()),
            })?);
        }
    }
    
    Ok(Statement::AlterTable(AlterTable {
        name,
        schema,
        actions,
    }))
}

fn parse_drop_object(stmt: &protobuf::DropStmt) -> Result<Statement> {
    let object_type = match protobuf::ObjectType::try_from(stmt.remove_type) {
        Ok(typ) => parse_object_type(&typ)?,
        Err(_) => return Err(anyhow::anyhow!("Invalid object type")),
    };
    
    let names = stmt.objects.iter()
        .filter_map(|obj| get_object_name(obj).ok())
        .collect::<Vec<_>>();
    
    let name = names.first()
        .context("Missing object name")?
        .clone();
    
    let schema = if names.len() > 1 {
        Some(names[0].clone())
    } else {
        None
    };
    
    let cascade = match protobuf::DropBehavior::try_from(stmt.behavior) {
        Ok(protobuf::DropBehavior::DropCascade) => true,
        _ => false,
    };
    
    let restrict = !cascade;
    
    Ok(Statement::DropObject(DropObject {
        object_type,
        name,
        schema,
        cascade,
        restrict,
    }))
}

// Helper functions

fn get_role_name(role: &protobuf::Node) -> Result<String> {
    if let Some(node) = &role.node {
        if let node::Node::RoleSpec(role) = node {
            return Ok(role.rolename.clone());
        }
    }
    Err(anyhow::anyhow!("Invalid role specification"))
}

fn get_object_name(obj: &protobuf::Node) -> Result<String> {
    if let Some(node) = &obj.node {
        if let node::Node::String(obj) = node {
            return Ok(obj.sval.clone());
        }
    }
    Err(anyhow::anyhow!("Invalid object name"))
}

fn parse_object_type(typ: &protobuf::ObjectType) -> Result<ObjectType> {
    match typ {
        protobuf::ObjectType::ObjectTable => Ok(ObjectType::Table),
        protobuf::ObjectType::ObjectView => Ok(ObjectType::View),
        protobuf::ObjectType::ObjectFunction => Ok(ObjectType::Function),
        protobuf::ObjectType::ObjectProcedure => Ok(ObjectType::Procedure),
        protobuf::ObjectType::ObjectType => Ok(ObjectType::Type),
        protobuf::ObjectType::ObjectDomain => Ok(ObjectType::Domain),
        protobuf::ObjectType::ObjectSequence => Ok(ObjectType::Sequence),
        protobuf::ObjectType::ObjectExtension => Ok(ObjectType::Extension),
        protobuf::ObjectType::ObjectTrigger => Ok(ObjectType::Trigger),
        protobuf::ObjectType::ObjectPolicy => Ok(ObjectType::Policy),
        protobuf::ObjectType::ObjectForeignServer => Ok(ObjectType::Server),
        _ => Err(anyhow::anyhow!("Unsupported object type")),
    }
}

fn parse_drop_behavior(behavior: &protobuf::DropBehavior) -> Result<DropBehavior> {
    match behavior {
        protobuf::DropBehavior::DropRestrict => Ok(DropBehavior::Restrict),
        protobuf::DropBehavior::DropCascade => Ok(DropBehavior::Cascade),
        _ => Ok(DropBehavior::Restrict),
    }
}

fn parse_alter_table_action(cmd: &protobuf::Node) -> Result<AlterTableAction> {
    if let Some(node::Node::AlterTableCmd(cmd)) = &cmd.node {
        match protobuf::AlterTableType::try_from(cmd.subtype) {
            Ok(protobuf::AlterTableType::AtAddColumn) => {
                if let Some(node) = &cmd.def {
                    if let Some(node::Node::ColumnDef(col)) = &node.node {
                        Ok(AlterTableAction::AddColumn(parse_column_def(col)?))
                    } else {
                        Err(anyhow::anyhow!("Expected ColumnDef node"))
                    }
                } else {
                    Err(anyhow::anyhow!("Missing column definition"))
                }
            }
            // ... handle other alter table actions ...
            _ => Err(anyhow::anyhow!("Unsupported alter table action")),
        }
    } else {
        Err(anyhow::anyhow!("Expected AlterTableCmd node"))
    }
}

// Add helper method for Node to convert to string representation
trait NodeToString {
    fn to_string_representation(&self) -> Result<String>;
}

impl NodeToString for Box<Node> {
    fn to_string_representation(&self) -> Result<String> {
        // Implement a custom string representation for Node
        // This is a placeholder - you'll need to implement the actual conversion
        // based on your needs
        Ok(format!("{:?}", self))
    }
} 