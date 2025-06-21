# Shem Parser Crate

## Overview

The `shem-parser` crate is a SQL parsing library specifically designed for database schema management. It parses PostgreSQL SQL statements and converts them into structured, typed data structures that can be easily analyzed, compared, and manipulated programmatically.

## Purpose

This crate serves as the **bridge between SQL text and structured schema data**. It enables Shem to:

- Parse SQL schema files and strings into Abstract Syntax Trees (AST)
- Convert parsed SQL into structured schema definitions
- Support all major PostgreSQL database objects
- Provide a foundation for schema comparison and validation

Direction: SQL Text → Structured Data
Input: SQL files and strings
Output: AS
T and schema objects
Used by: diff, validate, inspect commands
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

## PostgreSQL Features Supported

### Data Types
- **Numeric**: `INTEGER`, `BIGINT`, `DECIMAL`, `NUMERIC`, `REAL`, `DOUBLE PRECISION`
- **Character**: `CHAR`, `VARCHAR`, `TEXT`
- **Date/Time**: `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `INTERVAL`
- **Boolean**: `BOOLEAN`
- **Binary**: `BYTEA`
- **JSON**: `JSON`, `JSONB`
- **Special**: `UUID`, `XML`, `BIT`, `BIT VARYING`
- **Arrays**: All types with array notation
- **Custom**: User-defined types

### Constraints
- **Primary Keys**: Single and composite
- **Foreign Keys**: With referential actions
- **Unique**: Single and composite
- **Check**: Complex expressions
- **Exclusion**: With operator classes

### Advanced Features
- **Partitioning**: Range, list, and hash partitioning
- **Inheritance**: Table inheritance
- **Generated Columns**: Stored and virtual
- **Identity Columns**: `GENERATED ALWAYS AS IDENTITY`
- **Row-Level Security**: Policies with complex expressions
- **Triggers**: Multiple timing and event combinations
- **Functions**: All parameter modes, return types, and behaviors

## Testing

The crate includes comprehensive tests covering:

- **Unit Tests**: Individual parsing functions
- **Integration Tests**: Complete SQL statement parsing
- **Edge Cases**: Complex expressions, nested structures
- **Error Handling**: Invalid SQL, malformed statements

Run tests with:
```bash
cargo test
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

When adding new PostgreSQL features:

1. **Add AST structures** in `ast.rs` for new object types
2. **Implement parsing logic** in `visitor.rs` for the new structures
3. **Add tests** to verify correct parsing
4. **Update documentation** for new features

## Related Crates

- `shem-core`: Core schema types that the parser converts to
- `shem-postgres`: Database introspection (complementary to parsing)
- `shem-cli`: Command-line interface that uses the parser
- `pg_query`: PostgreSQL parsing library (external dependency)

## Why PostgreSQL's Parser?

Using PostgreSQL's official parser (`pg_query`) provides several advantages:

- **Accuracy**: Matches PostgreSQL's exact parsing behavior
- **Completeness**: Supports all PostgreSQL features and syntax
- **Maintenance**: Automatically stays up-to-date with PostgreSQL versions
- **Reliability**: Battle-tested in production PostgreSQL installations 