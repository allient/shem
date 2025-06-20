# Quick Start: Testing Shem Introspect

This guide will help you quickly set up and run tests for the Shem PostgreSQL introspection feature.

## 🚀 Quick Setup

### 1. Prerequisites
- PostgreSQL server running (version 12+)
- `psql` command available
- Rust and Cargo installed

### 2. Check PostgreSQL Connection
```bash
# Test if you can connect to PostgreSQL
psql -h localhost -U postgres -d postgres -c "SELECT version();"
```

If this fails, ensure:
- PostgreSQL is running
- You have access to the `postgres` user
- The user has CREATE/DROP privileges

### 3. Run Your First Test
```bash
# Test tables introspection
make test-feature FEATURE=tables
```

## 📋 Common Commands

### Using Make (Recommended)
```bash
# Run all introspect tests
make test-introspect

# Test specific feature
make test-feature FEATURE=functions
make test-feature FEATURE=enums
make test-feature FEATURE=triggers

# Clean up orphaned databases
make clean-dbs

# Check PostgreSQL connection
make check-postgres

# Show test database status
make db-status
```

### Using the Script Directly
```bash
# Run all tests
./scripts/test-introspect.sh all

# Test specific feature
./scripts/test-introspect.sh feature tables
./scripts/test-introspect.sh feature functions

# Clean up
./scripts/test-introspect.sh cleanup

# Check connection
./scripts/test-introspect.sh check
```

### Using Cargo Directly
```bash
# Run all introspect tests
cargo test introspect

# Run specific test
cargo test test_tables_introspect -- --nocapture

# Run with output
cargo test introspect -- --nocapture
```

## 🎯 Available Test Features

Test these PostgreSQL objects in isolation:

| Feature | Description | Test Command |
|---------|-------------|--------------|
| `tables` | Base tables with columns and constraints | `make test-feature FEATURE=tables` |
| `views` | Regular views | `make test-feature FEATURE=views` |
| `materialized_views` | Materialized views | `make test-feature FEATURE=materialized_views` |
| `functions` | PL/pgSQL functions | `make test-feature FEATURE=functions` |
| `procedures` | Stored procedures | `make test-feature FEATURE=procedures` |
| `enums` | Enumeration types | `make test-feature FEATURE=enums` |
| `domains` | Custom domains | `make test-feature FEATURE=domains` |
| `composite_types` | Composite types | `make test-feature FEATURE=composite_types` |
| `range_types` | Range types | `make test-feature FEATURE=range_types` |
| `sequences` | Auto-incrementing sequences | `make test-feature FEATURE=sequences` |
| `extensions` | PostgreSQL extensions | `make test-feature FEATURE=extensions` |
| `triggers` | Database triggers | `make test-feature FEATURE=triggers` |
| `policies` | Row-level security policies | `make test-feature FEATURE=policies` |
| `servers` | Foreign data wrapper servers | `make test-feature FEATURE=servers` |
| `collations` | Custom collations | `make test-feature FEATURE=collations` |
| `rules` | Database rules | `make test-feature FEATURE=rules` |

## 🔧 Troubleshooting

### PostgreSQL Connection Issues
```bash
# Check if PostgreSQL is running
brew services list | grep postgresql  # macOS
sudo systemctl status postgresql      # Linux

# Test connection
make check-postgres
```

### Permission Issues
```bash
# Grant necessary privileges (run as superuser)
psql -h localhost -U postgres -d postgres -c "GRANT CREATE ON DATABASE postgres TO postgres;"
```

### Clean Up Orphaned Databases
```bash
# Remove test databases that weren't cleaned up
make clean-dbs
```

### Verbose Output
```bash
# Run with detailed output
cargo test introspect -- --nocapture --test-threads=1
```

## 📊 Understanding Test Results

### Successful Test Output
```
🚀 Testing tables introspection in isolation...
📋 Testing tables introspection in isolation...
Testing tables introspection...
✅ Tables introspection test passed
✅ tables introspection test passed
```

### Failed Test Output
```
❌ tables introspection test failed: panicked at 'assertion failed: `(left == right)`
  left: `2`
 right: `3`: Table 'users' not found in schema. Available tables: []'
```

### Debugging Failed Tests
1. **Check PostgreSQL connection**: `make check-postgres`
2. **Clean up databases**: `make clean-dbs`
3. **Run with verbose output**: `cargo test test_tables_introspect -- --nocapture`
4. **Check database state**: Connect to the test database manually

## 🎉 Next Steps

1. **Run all tests**: `make test-introspect`
2. **Explore specific features**: Try different `FEATURE` values
3. **Read the full documentation**: See `tests/README.md`
4. **Contribute**: Add new test cases or improve existing ones

## 📚 Additional Resources

- **Full Documentation**: `tests/README.md`
- **Test Structure**: `tests/introspect/`
- **SQL Fixtures**: `tests/introspect/fixtures.rs`
- **Helper Functions**: `tests/introspect/helpers.rs`
- **Test Cases**: `tests/introspect/test_cases.rs`

Happy testing! 🚀 