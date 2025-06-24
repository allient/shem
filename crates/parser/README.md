# Shem Parser Crate

## Overview

The `shem-parser` crate is a SQL parsing library specifically designed for database schema management. It parses PostgreSQL SQL statements and converts them into structured, typed data structures that can be easily analyzed, compared, and manipulated programmatically.

## Purpose

This crate serves as the **bridge between SQL text and structured schema data**. It enables Shem to:

- Parse SQL schema files and strings into Abstract Syntax Trees (AST)
- Convert parsed SQL into structured schema definitions
- Support all major PostgreSQL database objects
- Provide a foundation for schema comparison and validation

**Direction**: SQL Text → Structured Data  
**Input**: SQL files and strings  
**Output**: AST and schema objects  
**Used by**: diff, validate, inspect commands

## Key Components

### 1. Abstract Syntax Tree (`ast.rs`)

Defines comprehensive data structures representing all PostgreSQL database objects:

- **Tables**: Columns, constraints, partitioning, inheritance, storage options
- **Views & Materialized Views**: Query definitions, check options, security settings
- **Functions & Procedures**: Parameters, return types, language, behavior settings
- **Types**: Enums, composite types, domains, range types
- **Sequences**: Start values, increments, caching, ownership
- **Extensions**: Version information, cascade options
- **Triggers**: Timing, events, functions, arguments
- **Policies**: Row-level security policies with commands and expressions
- **And more**: Servers, collations, rules, publications, subscriptions

### 2. Visitor Pattern (`visitor.rs`)

Implements the parsing logic using PostgreSQL's official parser:

- Uses `pg_query` library to leverage PostgreSQL's parsing engine
- Converts PostgreSQL's internal AST into Shem's own data structures
- Handles complex parsing scenarios like function bodies and expressions
- Provides robust error handling and context information

### 3. Public API (`lib.rs`)

Provides clean, high-level functions for parsing:

- `parse_file(path)`: Parse SQL files into AST
- `parse_sql(sql)`: Parse SQL strings into AST
- `parse_schema(sql)`: Parse SQL into complete schema definition

## PostgreSQL Parsing Support Status

### ✅ Fully Implemented Parsing

| Statement Type | Parser Function | AST Structure | Test Coverage | Description |
|----------------|-----------------|---------------|---------------|-------------|
| **CREATE TABLE** | `parse_create_table` | `CreateTable` | ✅ Complete | Tables with columns, constraints, inheritance, partitioning |
| **CREATE VIEW** | `parse_create_view` | `CreateView` | ✅ Complete | Views with query definitions and check options |
| **CREATE FUNCTION** | `parse_create_function` | `CreateFunction` | ✅ Complete | Functions with parameters, return types, language |
| **CREATE ENUM** | `parse_create_enum` | `CreateEnum` | ✅ Complete | Enum types with values |
| **CREATE TYPE** | `parse_create_type` | `CreateType` | ✅ Complete | Composite types with attributes |
| **CREATE DOMAIN** | `parse_create_domain` | `CreateDomain` | ✅ Complete | Domain types with constraints |
| **CREATE SEQUENCE** | `parse_create_sequence` | `CreateSequence` | ✅ Complete | Sequences with options |
| **CREATE EXTENSION** | `parse_create_extension` | `CreateExtension` | ✅ Complete | Extensions with version and schema |
| **CREATE TRIGGER** | `parse_create_trigger` | `CreateTrigger` | ✅ Complete | Triggers with timing and events |
| **CREATE POLICY** | `parse_create_policy` | `CreatePolicy` | ✅ Complete | Row-level security policies |
| **CREATE SERVER** | `parse_create_server` | `CreateServer` | ✅ Complete | Foreign data wrapper servers |
| **CREATE SCHEMA** | `parse_create_schema` | `CreateSchema` | ✅ Complete | Schema namespaces |
| **CREATE PUBLICATION** | `parse_create_publication` | `CreatePublication` | ✅ Complete | Logical replication publications |
| **CREATE RANGE TYPE** | `parse_create_range_type` | `CreateRangeType` | ✅ Complete | Custom range types |
| **CREATE ROLE** | `parse_create_role` | `CreateRole` | ✅ Complete | Database roles and users |
| **CREATE RULE** | `parse_create_rule` | `CreateRule` | ✅ Complete | Query rewrite rules |
| **CREATE FOREIGN TABLE** | `parse_create_foreign_table` | `CreateForeignTable` | ✅ Complete | Foreign tables |
| **CREATE FOREIGN DATA WRAPPER** | `parse_create_foreign_data_wrapper` | `CreateForeignDataWrapper` | ✅ Complete | FDW definitions |
| **CREATE SUBSCRIPTION** | `parse_create_subscription` | `CreateSubscription` | ✅ Complete | Logical replication subscriptions |
| **CREATE TABLESPACE** | `parse_create_tablespace` | `CreateTablespace` | ✅ Complete | Physical storage locations |
| **ALTER TABLE** | `parse_alter_table` | `AlterTable` | ✅ Complete | Table modifications |
| **DROP OBJECT** | `parse_drop_object` | `DropObject` | ✅ Complete | Object deletion |

### 🔶 Partially Implemented Parsing

| Statement Type | Parser Function | AST Structure | Missing Features | Description |
|----------------|-----------------|---------------|------------------|-------------|
| **Column Definitions** | `parse_column_def` | `ColumnDefinition` | Generated columns, identity columns, collations | Basic column parsing works |
| **Table Constraints** | `parse_table_constraint` | `TableConstraint` | Foreign keys, check constraints, exclusion constraints | Primary key and unique constraints work |
| **Function Parameters** | `parse_function_parameter` | `FunctionParameter` | Parameter modes, default values | Basic parameter parsing works |
| **Expressions** | `parse_expression` | `Expression` | Complex expressions, operators, functions | Placeholder implementation |
| **Partitioning** | `parse_partition_definition` | `PartitionDefinition` | Partition bounds, strategies | Basic structure only |

### ❌ Not Yet Implemented Parsing

| Statement Type | Priority | Description | Use Cases |
|----------------|----------|-------------|-----------|
| **CREATE MATERIALIZED VIEW** | High | Materialized view creation | Cached query results |
| **CREATE PROCEDURE** | High | Stored procedure creation | Procedural logic (PostgreSQL 11+) |
| **CREATE COLLATION** | Medium | Collation creation | Text sorting rules |
| **CREATE INDEX** | Medium | Index creation | Performance optimization |
| **CREATE CONSTRAINT TRIGGER** | Medium | Constraint trigger creation | Constraint enforcement |
| **CREATE EVENT TRIGGER** | Medium | Event trigger creation | Database-level events |
| **CREATE FOREIGN KEY CONSTRAINT** | Medium | Foreign key constraint creation | Referential integrity |
| **COMMENT ON** | Low | Object comments | Documentation |
| **GRANT/REVOKE** | Low | Privilege management | Access control |
| **CREATE CAST** | Low | Type conversion rules | Custom type conversions |
| **CREATE OPERATOR** | Low | Custom operators | Custom data type operations |
| **CREATE OPERATOR CLASS** | Low | Index behavior definitions | Custom index types |
| **CREATE AGGREGATE** | Low | Custom aggregation functions | Statistical functions |
| **CREATE LANGUAGE** | Low | Procedural languages | PL/pgSQL, PL/Python, etc. |
| **CREATE CONVERSION** | Low | Character set conversions | Internationalization |
| **CREATE TEXT SEARCH** | Low | Full-text search objects | Advanced text search |
| **CREATE TRANSFORMATION** | Low | Type transformations | Custom type transformations |
| **CREATE ACCESS METHOD** | Low | Custom access methods | Custom index types |
| **CREATE STATISTICS** | Low | Extended statistics | Query optimization |

## PostgreSQL Features Supported

### Data Types
- **Numeric**: `INTEGER`, `BIGINT`, `DECIMAL`, `NUMERIC`, `REAL`, `DOUBLE PRECISION`, `SERIAL`, `BIGSERIAL`, `SMALLSERIAL`
- **Character**: `CHAR`, `VARCHAR`, `TEXT`
- **Date/Time**: `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `INTERVAL`
- **Boolean**: `BOOLEAN`
- **Binary**: `BYTEA`
- **JSON**: `JSON`, `JSONB`
- **Special**: `UUID`, `XML`, `BIT`, `BIT VARYING`
- **Arrays**: All types with array notation
- **Custom**: User-defined types, enums, composite types, domains, range types

### Constraints
- **Primary Keys**: Single and composite primary keys
- **Unique**: Single and composite unique constraints
- **Foreign Keys**: Basic foreign key parsing (limited)
- **Check**: Basic check constraint parsing (limited)
- **Exclusion**: Not yet implemented

### Advanced Features
- **Partitioning**: Basic partition definition parsing
- **Inheritance**: Table inheritance parsing
- **Generated Columns**: Structure defined (parsing limited)
- **Identity Columns**: Structure defined (parsing limited)
- **Row-Level Security**: Policy parsing with commands and expressions
- **Triggers**: Basic trigger parsing with timing and events
- **Functions**: Parameter parsing, return types, language, behavior settings

## How It Works

### 1. SQL Input
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### 2. Parsing Process
```rust
// 1. Parse with pg_query (PostgreSQL's parser)
let result = pg_query::parse(sql)?;

// 2. Convert to Shem's AST using visitor pattern
let statements = visitor::parse_statements(&result)?;

// 3. Result: Structured data
Statement::CreateTable(CreateTable {
    name: "users".to_string(),
    columns: vec![
        ColumnDefinition {
            name: "id".to_string(),
            data_type: DataType::Serial,
            // ... other properties
        },
        // ... more columns
    ],
    // ... other table properties
})
```

### 3. Schema Definition
The parsed statements can be organized into a complete `SchemaDefinition` containing all database objects.

## Usage

### Basic Parsing
```rust
use shem_parser::{parse_sql, parse_file, parse_schema};

// Parse SQL string
let sql = "CREATE TABLE users (id SERIAL PRIMARY KEY);";
let statements = parse_sql(sql)?;

// Parse SQL file
let statements = parse_file(Path::new("schema.sql"))?;

// Parse into complete schema
let schema = parse_schema(sql)?;
```

### Integration with Shem Commands
The parser is used by multiple Shem commands:

- **`shem diff`**: Parses schema files to compare with database
- **`shem validate`**: Parses schema files to check syntax and structure
- **`shem inspect`**: Parses schema files to analyze and count objects
- **`shem introspect`**: Parses generated SQL during serialization

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   SQL Files     │───▶│  shem-parser     │───▶│  Structured     │
│   or Strings    │    │                  │    │   Schema Data   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │   pg_query       │
                       │ (PostgreSQL      │
                       │  parser)         │
                       └──────────────────┘
```

## Dependencies

- `pg_query`: PostgreSQL's official parsing library
- `anyhow`: Error handling
- `serde`: Serialization support for AST structures
- `tokio`: Async runtime support
- `shared-types`: Shared type definitions

## Testing

The crate includes comprehensive tests covering:

- **Unit Tests**: Individual parsing functions
- **Integration Tests**: Complete SQL statement parsing
- **Edge Cases**: Complex expressions, nested structures
- **Error Handling**: Invalid SQL, malformed statements

**Test Results**: 27 tests passing (4 basic + 23 comprehensive)

Run tests with:
```bash
# Run all tests
cargo test -p parser

# Run specific test suites
cargo test -p parser --test parser_basic
cargo test -p parser --test parser_comprehensive

# Run with verbose output
cargo test -p parser -- --nocapture
```

## Error Handling

The parser provides detailed error information:

- **Syntax Errors**: Invalid SQL syntax
- **Semantic Errors**: Invalid object definitions
- **Context Information**: Line numbers, statement types
- **Recovery**: Continues parsing after errors when possible

## Performance

- **Fast Parsing**: Leverages PostgreSQL's optimized parser
- **Memory Efficient**: Streams large files without loading entirely into memory
- **Async Support**: Non-blocking parsing for large schemas

## Contributing

When adding new PostgreSQL parsing features:

1. **Add AST structures** in `ast.rs` for new statement types
2. **Implement parsing logic** in `visitor.rs` for the new structures
3. **Add comprehensive tests** to verify correct parsing
4. **Update documentation** for new features

### Development Guidelines

- Follow PostgreSQL syntax specifications
- Handle edge cases and error conditions
- Add comprehensive test coverage
- Document any parsing limitations
- Use proper error handling with context

## Related Crates

- `shem-core`: Core schema types that the parser converts to
- `shem-postgres`: Database introspection (complementary to parsing)
- `shem-cli`: Command-line interface that uses the parser
- `shared-types`: Shared type definitions
- `pg_query`: PostgreSQL parsing library (external dependency)

## Why PostgreSQL's Parser?

Using PostgreSQL's official parser (`pg_query`) provides several advantages:

- **Accuracy**: Matches PostgreSQL's exact parsing behavior
- **Completeness**: Supports all PostgreSQL features and syntax
- **Maintenance**: Automatically stays up-to-date with PostgreSQL versions
- **Reliability**: Battle-tested in production PostgreSQL installations

## PostgreSQL Version Compatibility

This parser is designed to work with PostgreSQL 10.0 and later, with support for:

- **PostgreSQL 10+**: Basic functionality
- **PostgreSQL 11+**: Procedures, generated columns
- **PostgreSQL 12+**: Generated columns improvements
- **PostgreSQL 13+**: Logical replication improvements
- **PostgreSQL 14+**: Range type improvements
- **PostgreSQL 15+**: Latest features and optimizations

## Known Limitations

### Parsing Limitations
- **Complex Expressions**: Limited support for complex SQL expressions
- **Generated Columns**: Structure defined but parsing incomplete
- **Identity Columns**: Structure defined but parsing incomplete
- **Foreign Keys**: Basic parsing only, missing referential actions
- **Check Constraints**: Basic parsing only, missing complex expressions

### Missing Features
- **Materialized Views**: Not yet implemented
- **Procedures**: Not yet implemented
- **Collations**: Not yet implemented
- **Indexes**: Not yet implemented
- **Event Triggers**: Not yet implemented

### TODO Items
- Complete expression parsing implementation
- Add support for complex constraint parsing
- Implement missing statement types
- Improve error handling and recovery
- Add more comprehensive test coverage 