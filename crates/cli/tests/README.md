# Shem CLI Tests

This directory contains comprehensive tests for the Shem CLI tool, organized by command and object type. The tests are designed to work with the new unified model architecture.

## Test Structure

```
tests/
├── README.md                    # This file
├── run_tests.sh                 # Test runner script
├── integration_tests.rs         # Main integration test file
├── mod.rs                       # Test module organization
├── common/                      # Common test utilities
│   └── mod.rs                   # Test environment, DB helpers, CLI helpers
├── fixtures/                    # Test data and fixtures
│   └── mod.rs                   # SQL fixtures, expected outputs, configs
└── introspect/                  # Introspect command tests
    ├── mod.rs                   # Introspect test organization
    ├── tables.rs                # Table introspection tests
    ├── views.rs                 # View introspection tests
    ├── functions.rs             # Function introspection tests
    ├── triggers.rs              # Trigger introspection tests
    ├── types.rs                 # Type introspection tests (enums, domains, etc.)
    ├── sequences.rs             # Sequence introspection tests
    ├── extensions.rs            # Extension introspection tests
    ├── domains.rs               # Domain introspection tests
    ├── policies.rs              # Policy introspection tests
    ├── rules.rs                 # Rule introspection tests
    ├── event_triggers.rs        # Event trigger introspection tests
    ├── materialized_views.rs    # Materialized view introspection tests
    ├── procedures.rs            # Procedure introspection tests
    ├── collations.rs            # Collation introspection tests
    ├── foreign_tables.rs        # Foreign table introspection tests
    ├── publications.rs          # Publication introspection tests
    ├── subscriptions.rs         # Subscription introspection tests
    ├── roles.rs                 # Role introspection tests
    ├── tablespaces.rs           # Tablespace introspection tests
    ├── foreign_data_wrappers.rs # FDW introspection tests
    ├── servers.rs               # Server introspection tests
    ├── operators.rs             # Operator introspection tests
    ├── operator_classes.rs      # Operator class introspection tests
    ├── operator_families.rs     # Operator family introspection tests
    ├── conversions.rs           # Conversion introspection tests
    └── integration.rs           # Introspect integration tests
```

## Prerequisites

1. **PostgreSQL**: Make sure PostgreSQL is running on your system
   - macOS: `brew services start postgresql`
   - Linux: `sudo systemctl start postgresql`
   - Windows: Start PostgreSQL service

2. **Test Database**: The tests will automatically create a `shem_test` database if it doesn't exist

3. **Environment Variables**: The tests use these environment variables:
   - `TEST_DATABASE_URL`: Defaults to `postgresql://postgres:postgres@localhost:5432/shem_test`
   - `RUST_LOG`: Set to `debug` for verbose output

## Running Tests

### Using the Test Runner Script

The easiest way to run tests is using the provided script:

```bash
cd crates/cli
./tests/run_tests.sh
```

This script will:
1. Check if PostgreSQL is running
2. Create the test database if needed
3. Set up environment variables
4. Run all integration tests

### Using Cargo

You can also run tests directly with cargo:

```bash
cd crates/cli

# Run all tests
cargo test

# Run only integration tests
cargo test --test integration_tests

# Run tests with verbose output
cargo test --test integration_tests -- --nocapture

# Run a specific test
cargo test --test integration_tests test_basic_introspect 

# Run tests with specific features
cargo test --features test-db

# Run with debug logging
RUST_LOG=debug cargo test --test integration_tests test_introspect_with_extensions -- --nocapture
RUST_LOG=debug cargo test --test integration_tests test_introspect_simple_extension -- --nocapture
RUST_LOG=debug bacon test -- -p cli --test generator test_introspect_simple_extension -- --nocapture
RUST_LOG=debug cargo test -p cli --test generator comprehensive_integration -- --nocapture
```

### Running Individual Test Modules

To run tests for specific object types:

```bash
# Run only table introspection tests
cargo test --test integration_tests -- tables

# Run only view introspection tests
cargo test --test integration_tests -- views

# Run only type introspection tests (enums, domains, etc.)
cargo test --test integration_tests -- types
```

## Test Categories

### 1. Table Tests (`tables.rs`)
Tests for introspecting various table types using the unified `Relation::Table` model:
- Simple tables with basic columns
- Tables with foreign keys
- Tables with enums and domains
- Tables with check constraints
- Tables with indexes
- Tables with comments
- Tables with default values
- Tables with NOT NULL constraints
- Tables with unique constraints
- Tables with composite primary keys
- Tables with identity columns
- Tables with generated columns
- Tables with partitioning
- Tables with inheritance

### 2. View Tests (`views.rs`)
Tests for introspecting various view types using the unified `Relation::View` model:
- Simple views
- Views with joins
- Views with aggregation
- Views with subqueries
- Views with window functions
- Views with CTEs
- Views with comments
- Views with security barriers
- Views with check options

### 3. Materialized View Tests (`materialized_views.rs`)
Tests for introspecting materialized views using the unified `Relation::MaterializedView` model:
- Simple materialized views
- Materialized views with indexes
- Materialized views with storage parameters
- Materialized views with refresh options

### 4. Foreign Table Tests (`foreign_tables.rs`)
Tests for introspecting foreign tables using the unified `Relation::ForeignTable` model:
- Foreign tables with server connections
- Foreign tables with column options
- Foreign tables with constraints

### 5. Type Tests (`types.rs`)
Tests for introspecting various type objects using the unified `Type` enum:
- **Enum Types** (`Type::Enum`): Custom enumerated types
- **Composite Types** (`Type::Composite`): User-defined structured types
- **Domain Types** (`Type::Domain`): Constrained base types
- **Range Types** (`Type::Range`): Custom range types
- **Base Types** (`Type::Base`): Fundamental types
- **Pseudo Types** (`Type::Pseudo`): Special types like 'any', 'void'

### 6. Routine Tests (`functions.rs`, `procedures.rs`)
Tests for introspecting functions and procedures using the unified `Routine` enum:
- **Functions** (`Routine::Function`): User-defined functions
- **Procedures** (`Routine::Procedure`): Stored procedures
- **Aggregates** (`Routine::Aggregate`): Custom aggregation functions

### 7. Infrastructure Tests
Tests for various infrastructure objects:
- **Sequences** (`sequences.rs`): Auto-incrementing number generators
- **Extensions** (`extensions.rs`): PostgreSQL extensions
- **Schemas** (`schemas.rs`): Schema namespaces
- **Collations** (`collations.rs`): Text sorting rules
- **Conversions** (`conversions.rs`): Character set conversions

### 8. Security Tests
Tests for security-related objects:
- **Policies** (`policies.rs`): Row-level security policies
- **Roles** (`roles.rs`): Database users and roles

### 9. Replication Tests
Tests for replication objects:
- **Publications** (`publications.rs`): Logical replication publications
- **Subscriptions** (`subscriptions.rs`): Logical replication subscriptions

### 10. Foreign Data Tests
Tests for foreign data wrapper objects:
- **Foreign Data Wrappers** (`foreign_data_wrappers.rs`): External data source connectors
- **Servers** (`servers.rs`): Foreign data wrapper servers

### 11. Operator Tests
Tests for operator-related objects:
- **Operators** (`operators.rs`): Custom operators
- **Operator Classes** (`operator_classes.rs`): Index behavior definitions
- **Operator Families** (`operator_families.rs`): Operator family groupings

### 12. Trigger Tests
Tests for trigger objects:
- **Triggers** (`triggers.rs`): Row and statement-level triggers
- **Event Triggers** (`event_triggers.rs`): Database-level event triggers

### 13. Integration Tests (`integration.rs`)
End-to-end tests that verify:
- Complete schema introspection using unified models
- Custom output directories
- Configuration file usage
- Verbose output
- Empty database handling
- Error handling
- Dependency ordering
- System object exclusion
- Comment preservation
- Unified model serialization

## Test Utilities

### TestEnv
Provides a temporary test environment with:
- Temporary directory for test files
- Helper methods for file/directory creation
- Assertion methods for file content and existence

### Database Helpers
Utilities for database operations:
- `setup_test_db()`: Creates and cleans test database
- `cleanup_test_db()`: Cleans up test data
- `execute_sql()`: Executes SQL statements

### CLI Helpers
Utilities for running CLI commands:
- `run_shem_command()`: Run shem CLI commands
- `run_shem_command_in_dir()`: Run commands in specific directory
- `assert_command_success()`: Assert command succeeded
- `assert_command_failure()`: Assert command failed with expected error

## Test Fixtures

The `fixtures` module provides:
- **SQL Fixtures**: Pre-defined SQL statements for creating test objects
- **Expected Outputs**: Expected schema output for comparison
- **Configuration Fixtures**: Test configuration files
- **Unified Model Fixtures**: Test data for the new unified model architecture

## Adding New Tests

### 1. Create Test Module
Create a new file in the appropriate directory (e.g., `introspect/new_object.rs`)

### 2. Write Test Function
```rust
#[tokio::test]
async fn test_introspect_simple_new_object() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db().await?;
    
    // Setup test data
    db::execute_sql(&pool, "CREATE NEW_OBJECT test_obj(...);").await?;
    
    // Run introspect command
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", "postgresql://postgres:postgres@localhost:5432/shem_test", "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify results using unified model
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE NEW_OBJECT test_obj"));
    
    Ok(())
}
```

### 3. Add to Module
Update the appropriate `mod.rs` file to include your new test module.

### 4. Add Fixtures
If needed, add SQL fixtures to `fixtures/mod.rs`.

## Testing Unified Model Architecture

The tests now verify the new unified model architecture:

### 1. Unified Object Models
Tests verify that objects are correctly represented using the unified enums:
- `Relation` enum for tables, views, materialized views, foreign tables
- `Type` enum for all type variants
- `Routine` enum for functions, procedures, aggregates

### 2. Database Model Organization
Tests verify that objects are correctly organized by scope in the `DatabaseModel`:
- Global objects (roles, tablespaces)
- Database-scoped objects (schemas, extensions, publications)
- Schema-scoped objects (relations, types, routines)

### 3. Unified SQL Generation
Tests verify that the unified SQL generator correctly handles all object variants:
- `create_relation()` handles all relation types
- `create_type()` handles all type variants
- `create_routine()` handles all routine types

## Troubleshooting

### Database Connection Issues
- Ensure PostgreSQL is running
- Check that the test database exists: `psql -l | grep shem_test`
- Verify connection string: `psql postgresql://postgres:postgres@localhost:5432/shem_test`

### Permission Issues
- Make sure the test runner script is executable: `chmod +x tests/run_tests.sh`
- Ensure you have permission to create databases

### Test Failures
- Check the test output for specific error messages
- Verify that the CLI binary can be built: `cargo build --bin shem`
- Run tests with verbose output to see more details
- Check that the unified model architecture is working correctly

## Contributing

When adding new tests:
1. Follow the existing naming conventions
2. Use the provided test utilities
3. Add appropriate fixtures
4. Document any new test patterns
5. Ensure tests are isolated and don't interfere with each other
6. Test the unified model architecture for new object types
7. Verify that objects are correctly organized by scope in the DatabaseModel 