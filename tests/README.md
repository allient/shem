# Shem Introspect Testing Framework

This directory contains a comprehensive testing framework for the Shem PostgreSQL introspection feature. The framework is designed to test each PostgreSQL object type in isolation, ensuring that the introspection functionality works correctly for all supported database objects.

## 🏗️ Architecture

The testing framework is organized into the following structure:

```
tests/
├── introspect.rs              # Main test entry point
├── introspect/
│   ├── mod.rs                 # Module declarations
│   ├── helpers.rs             # Database setup/teardown and utility functions
│   ├── fixtures.rs            # SQL fixtures for different PostgreSQL objects
│   └── test_cases.rs          # Individual test cases for each object type
└── README.md                  # This file
```

## 🎯 Testing Strategy

### Isolated Testing
Each PostgreSQL object type is tested in complete isolation:
- **Individual Test Databases**: Each test creates its own temporary database
- **Clean Setup/Teardown**: Tests start with a clean slate and clean up after themselves
- **Focused Assertions**: Each test focuses on specific object types and their properties

### Supported PostgreSQL Objects

The framework tests introspection for the following PostgreSQL objects:

1. **Tables** - Base tables with columns, constraints, and indexes
2. **Views** - Regular views with their definitions
3. **Materialized Views** - Materialized views with refresh strategies
4. **Functions** - PL/pgSQL and other function types
5. **Procedures** - Stored procedures with IN/OUT parameters
6. **Enums** - User-defined enumeration types
7. **Domains** - Custom data types with constraints
8. **Composite Types** - Structured types with multiple fields
9. **Range Types** - Range data types
10. **Sequences** - Auto-incrementing sequences
11. **Extensions** - PostgreSQL extensions
12. **Triggers** - Database triggers
13. **Policies** - Row-level security policies
14. **Servers** - Foreign data wrapper servers
15. **Collations** - Custom collations
16. **Rules** - Database rules

## 🚀 Getting Started

### Prerequisites

1. **PostgreSQL Server**: A running PostgreSQL server (version 12 or higher)
2. **psql Command**: The `psql` command-line tool must be available
3. **Database Access**: Ability to create/drop databases (typically requires superuser privileges)

### Environment Setup

1. **Database Connection**: Ensure PostgreSQL is running and accessible
   ```bash
   # Test connection
   psql -h localhost -U postgres -d postgres -c "SELECT version();"
   ```

2. **Environment Variables** (optional):
   ```bash
   export PGHOST=localhost
   export PGUSER=postgres
   export PGPASSWORD=your_password
   ```

### Running Tests

#### Run All Introspect Tests
```bash
cargo test introspect
```

#### Run Individual Feature Tests
```bash
# Test only tables introspection
cargo test test_tables_introspect

# Test only functions introspection
cargo test test_functions_introspect

# Test with all objects
cargo test test_introspect_with_all_objects
```

#### Run Tests with Output
```bash
# Show test output
cargo test -- --nocapture

# Run specific test with output
cargo test test_tables_introspect -- --nocapture
```

## 📋 Test Structure

### Test Database Lifecycle

Each test follows this pattern:

1. **Setup**: Create a unique test database
2. **Execute**: Run SQL fixtures to create test objects
3. **Introspect**: Call the introspection function
4. **Assert**: Verify the introspected schema matches expectations
5. **Cleanup**: Drop the test database

### Example Test Flow

```rust
pub async fn test_tables_introspect(test_db: &TestDatabase) {
    // 1. Setup: Create tables
    execute_sql(&test_db.connection, TABLES_FIXTURE).await.unwrap();
    
    // 2. Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // 3. Assert: Verify tables exist and have correct properties
    assert_table_exists(&schema, "users");
    assert_table_exists(&schema, "posts");
    
    // 4. Assert: Check detailed properties
    let users_table = schema.tables.get("users").unwrap();
    assert_eq!(users_table.columns.len(), 7);
    assert_eq!(users_table.constraints.len(), 3);
}
```

## 🔧 Fixtures

The `fixtures.rs` file contains SQL scripts for creating various PostgreSQL objects:

- **TABLES_FIXTURE**: Creates tables with various column types and constraints
- **VIEWS_FIXTURE**: Creates regular views
- **FUNCTIONS_FIXTURE**: Creates PL/pgSQL functions
- **ENUMS_FIXTURE**: Creates enumeration types
- **DOMAINS_FIXTURE**: Creates custom domains with constraints
- And many more...

Each fixture is designed to be:
- **Self-contained**: Includes all necessary dependencies
- **Realistic**: Uses real-world examples
- **Comprehensive**: Covers various edge cases and features

## 🛠️ Helper Functions

The `helpers.rs` file provides utility functions:

### Database Management
- `setup_test_database()`: Creates a unique test database
- `cleanup_test_database()`: Safely drops the test database
- `execute_sql()`: Executes SQL scripts on the test database

### Assertion Helpers
- `assert_table_exists()`: Verifies a table exists in the schema
- `assert_view_exists()`: Verifies a view exists in the schema
- `assert_function_exists()`: Verifies a function exists in the schema
- And similar functions for all object types

### Debugging
- `print_schema_summary()`: Prints a summary of all introspected objects

## 🐛 Debugging Tests

### Enable Verbose Output
```bash
cargo test -- --nocapture --test-threads=1
```

### Run Single Test
```bash
cargo test test_tables_introspect -- --nocapture
```

### Check Database State
If a test fails, you can manually inspect the database:
```bash
# Connect to the test database (name will be in error output)
psql -h localhost -U postgres -d shem_test_<uuid>
```

### Common Issues

1. **Database Connection Failed**
   - Ensure PostgreSQL is running
   - Check connection parameters
   - Verify user has CREATE/DROP privileges

2. **Test Database Already Exists**
   - The framework uses UUIDs to avoid conflicts
   - Check for orphaned databases: `psql -c "\l" | grep shem_test`

3. **Permission Denied**
   - Ensure the PostgreSQL user has superuser privileges
   - Or grant specific privileges for database creation

## 📊 Test Coverage

The framework provides comprehensive coverage for:

- **Object Detection**: Ensures all PostgreSQL objects are properly detected
- **Property Extraction**: Verifies all relevant properties are extracted
- **Relationship Mapping**: Tests foreign keys, dependencies, etc.
- **Edge Cases**: Handles complex scenarios and edge cases
- **Error Handling**: Tests behavior with invalid or missing objects

## 🔄 Continuous Integration

To integrate this testing framework into CI/CD:

1. **Setup PostgreSQL**: Install and configure PostgreSQL in CI environment
2. **Run Tests**: Execute `cargo test introspect`
3. **Artifacts**: Collect test output and database logs if needed

Example GitHub Actions workflow:
```yaml
- name: Setup PostgreSQL
  run: |
    sudo apt-get update
    sudo apt-get install -y postgresql postgresql-contrib
    sudo systemctl start postgresql
    
- name: Run Introspect Tests
  run: cargo test introspect -- --nocapture
```

## 🤝 Contributing

When adding new PostgreSQL object types to Shem:

1. **Add Fixture**: Create SQL fixture in `fixtures.rs`
2. **Add Test Case**: Create test function in `test_cases.rs`
3. **Add Assertion Helper**: Add helper function in `helpers.rs`
4. **Update Main Test**: Add to the main test runner in `introspect.rs`

## 📝 Best Practices

1. **Isolation**: Each test should be completely independent
2. **Cleanup**: Always clean up test databases, even on failure
3. **Realistic Data**: Use realistic examples that mirror real-world usage
4. **Comprehensive Assertions**: Test both existence and properties
5. **Error Handling**: Test both success and failure scenarios
6. **Documentation**: Keep fixtures and tests well-documented

This testing framework ensures that the Shem introspection feature is robust, reliable, and handles all PostgreSQL object types correctly. 