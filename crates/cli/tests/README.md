# Shem CLI Tests

This directory contains comprehensive tests for the Shem CLI tool, organized by command and object type.

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
    ├── functions.rs             # Function introspection tests (TODO)
    ├── triggers.rs              # Trigger introspection tests (TODO)
    ├── types.rs                 # Type introspection tests (TODO)
    ├── sequences.rs             # Sequence introspection tests (TODO)
    ├── extensions.rs            # Extension introspection tests (TODO)
    ├── domains.rs               # Domain introspection tests (TODO)
    ├── policies.rs              # Policy introspection tests (TODO)
    ├── rules.rs                 # Rule introspection tests (TODO)
    ├── event_triggers.rs        # Event trigger introspection tests (TODO)
    ├── materialized_views.rs    # Materialized view introspection tests (TODO)
    ├── procedures.rs            # Procedure introspection tests (TODO)
    ├── collations.rs            # Collation introspection tests (TODO)
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

# 
cargo test --test integration_tests test_introspect_with_extensions -- --nocapture
cargo test --test integration_tests test_introspect_simple_extension -- --nocapture
```



### Running Individual Test Modules

To run tests for specific object types:

```bash
# Run only table introspection tests
cargo test --test integration_tests -- tables

# Run only view introspection tests
cargo test --test integration_tests -- views
```

## Test Categories

### 1. Table Tests (`tables.rs`)
Tests for introspecting various table types:
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

### 2. View Tests (`views.rs`)
Tests for introspecting various view types:
- Simple views
- Views with joins
- Views with aggregation
- Views with subqueries
- Views with window functions
- Views with CTEs
- Views with comments
- Views with security barriers
- Views with check options

### 3. Integration Tests (`integration.rs`)
End-to-end tests that verify:
- Complete schema introspection
- Custom output directories
- Configuration file usage
- Verbose output
- Empty database handling
- Error handling
- Dependency ordering
- System object exclusion
- Comment preservation

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

## Adding New Tests

### 1. Create Test Module
Create a new file in the appropriate directory (e.g., `introspect/functions.rs`)

### 2. Write Test Function
```rust
#[tokio::test]
async fn test_introspect_simple_function() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db().await?;
    
    // Setup test data
    db::execute_sql(&pool, "CREATE FUNCTION test_func() RETURNS INTEGER AS 'SELECT 1' LANGUAGE SQL;").await?;
    
    // Run introspect command
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", "postgresql://postgres:postgres@localhost:5432/shem_test", "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify results
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE FUNCTION test_func"));
    
    Ok(())
}
```

### 3. Add to Module
Update the appropriate `mod.rs` file to include your new test module.

### 4. Add Fixtures
If needed, add SQL fixtures to `fixtures/mod.rs`.

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

## Contributing

When adding new tests:
1. Follow the existing naming conventions
2. Use the provided test utilities
3. Add appropriate fixtures
4. Document any new test patterns
5. Ensure tests are isolated and don't interfere with each other 