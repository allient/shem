# Shem PostgreSQL Crate

## Overview

The `shem-postgres` crate provides PostgreSQL-specific functionality for the Shem database schema management tool. It handles database introspection, connection management, and SQL generation for PostgreSQL databases.

## Purpose

This crate serves as the PostgreSQL implementation of Shem's database driver interface. It enables Shem to:

- Connect to PostgreSQL databases
- Introspect existing database schemas
- Generate PostgreSQL-specific SQL statements
- Handle PostgreSQL-specific features and data types

Direction: Database → Structured Data
Input: Live PostgreSQL database connection
Output: Schema objects from database queries
Used by: introspect command and database operations

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

## Usage

This crate is primarily used internally by the Shem CLI tool. It's not typically used directly by end users, but rather through the main Shem commands:

```bash
# Introspect a PostgreSQL database
shem introspect postgresql://user:pass@localhost/dbname

# Generate migrations by comparing schema files with database
shem diff schema.sql --database postgresql://user:pass@localhost/dbname
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

## PostgreSQL Features Supported

- **Data Types**: All PostgreSQL native types including arrays, JSON, UUID, etc.
- **Constraints**: Primary keys, foreign keys, unique, check, exclusion
- **Indexes**: B-tree, hash, GiST, SP-GiST, GIN, BRIN with custom options
- **Partitioning**: Range, list, and hash partitioning
- **Inheritance**: Table inheritance
- **Row-Level Security**: Policies and security barriers
- **Extensions**: Custom extensions and their objects
- **Foreign Data**: Foreign tables, servers, and data wrappers
- **Replication**: Publications and subscriptions
- **Advanced Features**: Event triggers, constraint triggers, rules

## Testing

The crate includes comprehensive tests that verify:

- Correct introspection of all PostgreSQL object types
- Proper SQL generation for all schema objects
- Handling of PostgreSQL-specific features
- Error cases and edge conditions

Run tests with:
```bash
cargo test -p postgres -- --list
cargo test
cargo test -p postgres --test sql_generator -- --nocapture
cargo test test_generate_create_table -- --nocapture
```

## Contributing

When adding new PostgreSQL features:

1. Add the introspection query in `introspection.rs`
2. Add corresponding SQL generation in `sql_generator.rs`
3. Update tests to cover the new functionality
4. Ensure proper error handling and edge cases

## Related Crates

- `shem-core`: Core schema types and database driver traits
- `shem-parser`: SQL parsing for schema files
- `shem-cli`: Command-line interface that uses this crate 