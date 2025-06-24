# Shem PostgreSQL Crate

## Overview

The `shem-postgres` crate provides PostgreSQL-specific functionality for the Shem database schema management tool. It handles database introspection, connection management, and SQL generation for PostgreSQL databases.

## Purpose

This crate serves as the PostgreSQL implementation of Shem's database driver interface. It enables Shem to:

- Connect to PostgreSQL databases
- Introspect existing database schemas
- Generate PostgreSQL-specific SQL statements
- Handle PostgreSQL-specific features and data types

**Direction**: Database → Structured Data  
**Input**: Live PostgreSQL database connection  
**Output**: Schema objects from database queries  
**Used by**: introspect command and database operations

## Key Components

### 1. Database Introspection (`introspection.rs`)

The core functionality that queries PostgreSQL's system catalogs to extract schema information:

- **Tables**: Columns, constraints, indexes, inheritance, partitioning
- **Views & Materialized Views**: Definitions, check options, security settings
- **Functions & Procedures**: Parameters, return types, language, behavior settings
- **Types**: Enums, composite types, range types, domains
- **Sequences**: Start values, increments, caching, ownership
- **Extensions**: Version information, schema placement
- **Triggers**: Timing, events, functions, arguments
- **Policies**: Row-level security policies
- **And more**: Servers, collations, rules, publications, subscriptions, roles, tablespaces

### 2. SQL Generation (`sql_generator.rs`)

Converts Shem's internal schema representation back to PostgreSQL SQL:

- Generates `CREATE` statements for all database objects
- Handles PostgreSQL-specific syntax and features
- Maintains proper dependency ordering
- Supports all PostgreSQL data types and constraints

### 3. Database Driver Implementation

Implements the `DatabaseDriver` trait from `shem-core`:

- Connection management using `tokio-postgres`
- Async/await support for all operations
- Error handling and result types
- PostgreSQL-specific configuration

## PostgreSQL Object Support Status

### ✅ Fully Implemented Objects

| Object Type | Introspection | SQL Generation | Description |
|-------------|---------------|----------------|-------------|
| **Tables** | ✅ Complete | ✅ Complete | Base tables with columns, constraints, indexes |
| **Views** | ✅ Complete | ✅ Complete | Virtual tables based on SQL queries |
| **Materialized Views** | ✅ Complete | ✅ Complete | Materialized query results with refresh options |
| **Functions** | ✅ Complete | ✅ Complete | User-defined functions with parameters and return types |
| **Procedures** | ✅ Complete | ✅ Complete | Stored procedures (PostgreSQL 11+) |
| **Enums** | ✅ Complete | ✅ Complete | Custom enumerated types |
| **Composite Types** | ✅ Complete | ✅ Complete | User-defined composite types |
| **Range Types** | ✅ Complete | ✅ Complete | Custom range types (int4range, etc.) |
| **Domains** | ✅ Complete | ✅ Complete | Custom data types with constraints |
| **Sequences** | ✅ Complete | ✅ Complete | Auto-incrementing number generators |
| **Extensions** | ✅ Complete | ✅ Complete | PostgreSQL extensions and their objects |
| **Triggers** | ✅ Complete | ✅ Complete | Row and statement-level triggers |
| **Constraint Triggers** | ✅ Complete | ✅ Complete | Triggers for constraint enforcement |
| **Event Triggers** | ✅ Complete | ✅ Complete | Database-level event triggers |
| **Policies** | ✅ Complete | ✅ Complete | Row-level security policies |
| **Indexes** | ✅ Complete | ✅ Complete | All index types (B-tree, Hash, GiST, etc.) |
| **Collations** | ✅ Complete | ✅ Complete | Text sorting and comparison rules |
| **Rules** | ✅ Complete | ✅ Complete | Query rewrite rules |
| **Servers** | ✅ Complete | ✅ Complete | Foreign data wrapper servers |
| **Foreign Tables** | ✅ Complete | ✅ Complete | Tables in external data sources |
| **Foreign Data Wrappers** | ✅ Complete | ✅ Complete | External data source connectors |
| **Publications** | ✅ Complete | ✅ Complete | Logical replication publications |
| **Subscriptions** | ✅ Complete | ✅ Complete | Logical replication subscriptions |
| **Roles** | ✅ Complete | ✅ Complete | Database users and roles |
| **Tablespaces** | ✅ Complete | ✅ Complete | Physical storage locations |
| **Named Schemas** | ✅ Complete | ✅ Complete | Schema namespaces |
| **Foreign Key Constraints** | ✅ Complete | ✅ Complete | Referential integrity constraints |

### 🔶 Partially Implemented Objects

| Object Type | Introspection | SQL Generation | Description | Missing Features |
|-------------|---------------|----------------|-------------|------------------|
| **Comments** | ✅ Complete | ✅ Complete | Object documentation | Limited to basic COMMENT ON statements |
| **Grants/Privileges** | ❌ Missing | ✅ Basic | Permission management | No introspection of existing grants |

### ❌ Not Yet Implemented Objects

| Object Type | Priority | Description | Use Cases |
|-------------|----------|-------------|-----------|
| **Casts** | Medium | Type conversion rules | Custom type conversions |
| **Operators** | Medium | Custom operators (e.g., `#>`, `+=`) | Custom data type operations |
| **Operator Classes** | Medium | Index behavior definitions | Custom index types |
| **Aggregates** | Low | Custom aggregation functions | Statistical and analytical functions |
| **Languages** | Low | Procedural languages | PL/pgSQL, PL/Python, etc. |
| **Conversions** | Low | Character set conversions | Internationalization |
| **Text Search Configurations** | Low | Full-text search settings | Advanced text search |
| **Text Search Dictionaries** | Low | Text search dictionaries | Full-text search customization |
| **Text Search Parsers** | Low | Text search parsers | Full-text search parsing |
| **Text Search Templates** | Low | Text search templates | Full-text search templates |
| **Foreign Data Wrapper Handlers** | Low | FDW handler functions | Custom FDW implementations |
| **Foreign Data Wrapper Validators** | Low | FDW validator functions | FDW configuration validation |
| **Transformations** | Low | Type transformations | Custom type transformations |
| **Access Methods** | Low | Custom access methods | Custom index types |
| **Statistics Objects** | Low | Extended statistics | Query optimization |
| **Replication Origins** | Low | Logical replication origins | Replication tracking |

## PostgreSQL Features Supported

### Data Types
- **Native Types**: All PostgreSQL built-in types (integer, text, boolean, etc.)
- **Array Types**: Multi-dimensional arrays with custom element types
- **JSON Types**: JSON, JSONB with operators and functions
- **Geometric Types**: Point, line, polygon, circle, etc.
- **Network Types**: Inet, cidr, macaddr, macaddr8
- **UUID**: Universally unique identifiers
- **Range Types**: Built-in and custom range types
- **Composite Types**: User-defined structured types
- **Domain Types**: Constrained base types

### Constraints
- **Primary Keys**: Single and composite primary keys
- **Foreign Keys**: Referential integrity with cascade options
- **Unique Constraints**: Single and composite unique constraints
- **Check Constraints**: Custom validation rules
- **Exclusion Constraints**: Complex constraint types
- **NOT NULL**: Column-level nullability constraints

### Indexes
- **B-tree**: Default balanced tree indexes
- **Hash**: Hash-based indexes for equality
- **GiST**: Generalized Search Tree indexes
- **SP-GiST**: Space-partitioned GiST indexes
- **GIN**: Generalized Inverted indexes
- **BRIN**: Block Range INdexes
- **Partial Indexes**: Indexes with WHERE conditions
- **Expression Indexes**: Indexes on computed expressions
- **Operator Classes**: Custom index behavior

### Advanced Features
- **Partitioning**: Range, list, and hash partitioning
- **Inheritance**: Table inheritance hierarchies
- **Row-Level Security**: Fine-grained access control
- **Generated Columns**: Computed column values
- **Identity Columns**: Auto-incrementing columns
- **Foreign Data**: External data source integration
- **Logical Replication**: Publication and subscription support
- **Event Triggers**: Database-level event handling

## Usage

This crate is primarily used internally by the Shem CLI tool. It's not typically used directly by end users, but rather through the main Shem commands:

```bash
# Introspect a PostgreSQL database
shem introspect postgresql://user:pass@localhost/dbname

# Generate migrations by comparing schema files with database
shem diff schema.sql --database postgresql://user:pass@localhost/dbname

# Validate schema files
shem validate schema.sql

# Generate SQL from schema files
shem generate schema.sql
```

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Shem CLI      │───▶│  shem-postgres   │───▶│  PostgreSQL     │
│                 │    │                  │    │   Database      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │   shem-core      │
                       │  (Schema types)  │
                       └──────────────────┘
```

## Dependencies

- `tokio-postgres`: PostgreSQL client library
- `shem-core`: Core schema types and traits
- `anyhow`: Error handling
- `serde`: Serialization support
- `async-trait`: Async trait support

## Testing

The crate includes comprehensive tests that verify:

- **Complete SQL Generation Coverage**: All PostgreSQL object types have corresponding unit tests in `tests/sql_generator.rs`
- **Correct SQL Generation**: Proper PostgreSQL-specific syntax and features for all schema objects
- **Edge Cases**: Handling of PostgreSQL-specific features, reserved keywords, and complex scenarios
- **Error Cases**: Proper error handling and validation

### Test Coverage Status

**✅ Fully Tested SQL Generation Methods (50 tests):**
- Tables, Views, Materialized Views
- Functions, Procedures, Enums, Domains, Sequences
- Triggers, Constraint Triggers, Event Triggers
- Policies, Servers, Indexes, Collations, Rules
- Extensions, Comments, Grants/Privileges
- Base Types, Array Types, Multirange Types
- All DROP operations for the above objects
- ALTER operations for sequences and enums

**Note:**  
Direct unit tests for introspection functions (`introspect_*`) are not included in this test suite, as these require a live database connection. Introspection is tested via integration and CLI tests, which connect to a test database and verify the extracted schema matches expectations.

### Running Tests

```bash
# Run all tests with output
cargo test -p postgres -- --nocapture

# Run specific test suites
cargo test -p postgres --test sql_generator -- --nocapture

# List all available tests
cargo test -p postgres -- --list

# Run tests with verbose output
cargo test -p postgres -- --nocapture --test-threads=1
```

### Test Structure

The test suite in `tests/sql_generator.rs` includes:

1. **Basic Creation Tests**: Verify correct SQL generation for all object types
2. **Drop Operation Tests**: Ensure proper DROP statements with CASCADE options
3. **Alter Operation Tests**: Test schema modification operations
4. **Edge Case Tests**: Reserved keywords, complex constraints, special syntax
5. **Integration Tests**: Combined create/drop operations

### Contributing to Tests

When adding new PostgreSQL features:

1. **Add SQL Generation Test**: Create a test that verifies the generated SQL matches expected PostgreSQL syntax
2. **Add Drop Test**: Include a corresponding drop operation test
3. **Test Edge Cases**: Include tests for schema qualification, reserved keywords, and complex scenarios
4. **Update This Section**: Add the new test to the coverage list above

## Contributing

When adding new PostgreSQL features:

1. **Add introspection**: Implement the `introspect_*` function in `introspection.rs`
2. **Add SQL generation**: Implement the corresponding `create_*` and `drop_*` methods in `sql_generator.rs`
3. **Update schema types**: Add new types to `shem-core` if needed
4. **Add tests**: Create comprehensive tests for the new functionality
5. **Update documentation**: Add the new object type to this README

### Development Guidelines

- Follow PostgreSQL naming conventions
- Handle edge cases and error conditions
- Ensure proper dependency ordering
- Add comprehensive test coverage
- Document any PostgreSQL-specific behavior

## Related Crates

- `shem-core`: Core schema types and database driver traits
- `shem-parser`: SQL parsing for schema files
- `shem-cli`: Command-line interface that uses this crate
- `shem-shared-types`: Shared type definitions

## PostgreSQL Version Compatibility

This crate is designed to work with PostgreSQL 10.0 and later, with full support for:

- **PostgreSQL 10+**: Basic functionality
- **PostgreSQL 11+**: Procedures, generated columns
- **PostgreSQL 12+**: Generated columns improvements
- **PostgreSQL 13+**: Logical replication improvements
- **PostgreSQL 14+**: Range type improvements
- **PostgreSQL 15+**: Latest features and optimizations

## Performance Considerations

- **Introspection**: Optimized queries to minimize database load
- **SQL Generation**: Efficient string building and formatting
- **Memory Usage**: Streaming processing for large schemas
- **Connection Pooling**: Reuses database connections when possible

## Security Features

- **Connection Security**: Supports SSL/TLS connections
- **Privilege Management**: Handles GRANT/REVOKE statements
- **Row-Level Security**: Full support for RLS policies
- **Schema Isolation**: Proper schema namespace handling 