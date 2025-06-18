# Shem CLI

Declarative Database Schema Management for PostgreSQL

---

## Overview

**Shem** is a CLI tool for managing your PostgreSQL database schema declaratively. Instead of writing imperative migration scripts, you declare your desired schema state in SQL files, and Shem generates versioned migrations for you. This approach is inspired by modern tools like Supabase CLI and aims to make schema management safer, more reproducible, and developer-friendly.

---

## Features

- **Declarative schema files**: Describe your database in SQL, not migration scripts
- **Automatic migration generation**: Generate migrations by diffing your schema files against the current database
- **Shadow database diffing**: Safe, isolated schema comparison using a temporary database
- **Glob pattern support**: Organize your schema files flexibly
- **Safety checks**: Warnings for destructive operations (e.g., DROP statements)
- **Migration history tracking**: Reliable, versioned migrations

---

## Installation

```sh
# Clone the repository
$ git clone https://github.com/yourusername/shem.git
$ cd shem

# Build the CLI (requires Rust toolchain)
$ cargo build --release

# Optionally, add to your PATH
$ cp target/release/shem /usr/local/bin/
```

## Quick Start: Step-by-Step Example

### 1. Initialize a New Project

```sh
$ shem init my_project
$ cd my_project
```

This creates:
- `schema/` directory for your declarative SQL files
- `migrations/` directory for generated migration scripts
- `shem.yaml` or `shem.toml` config file

### 2. Declare Your Schema

Create a file `schema/01_employees.sql`:

```sql
CREATE TABLE employees (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL
);
```

You can split your schema into multiple files. Files are processed in lexicographic order.

### 3. Configure Your Database Connection

You have two options to configure your database connection:

#### Option A: Configure in shem.toml (Recommended)

Edit `shem.toml` and uncomment the database URL line:

```toml
# shem.toml
[database]
url = "postgresql://user:password@localhost:5432/mydb"

[declarative]
enabled = true
schema_paths = ["./schema/*.sql"]
shadow_port = 5432
```

**Important**: Remove the `#` comment symbol from the beginning of the line.

#### Option B: Use Command Line Parameter

Provide the database URL directly when running commands:

```sh
cargo run --bin shem -- migrate --database-url "postgresql://user:password@localhost:5432/mydb"
```

#### Database URL Format

```
postgresql://username:password@host:port/database_name
```

**Examples:**
- Local PostgreSQL: `postgresql://postgres:password@localhost:5432/myapp_dev`
- No password: `postgresql://postgres@localhost:5432/myapp_dev`
- Remote database: `postgresql://user:pass@db.example.com:5432/production`

#### Quick PostgreSQL Setup

If you don't have PostgreSQL running, you can start it with Docker:

```sh
# Start PostgreSQL container
docker run --name postgres-dev \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=myapp_dev \
  -p 5432:5432 \
  -d postgres:15

# Then use this URL in shem.toml:
# url = "postgresql://postgres:postgres@localhost:5432/myapp_dev"
```

### 4. Generate a Migration (Diff)

```sh
$ shem diff
```

- Shem will spin up a shadow database, apply existing migrations, and diff it against your declared schema.
- A new migration file will be created in `migrations/` (e.g., `20241004112233_create_employees_table.sql`).
- If destructive changes (e.g., DROP) are detected, you will be warned.

### 5. Apply the Migration

```sh
$ shem migrate
```

- Applies all pending migrations to your target database.
- Migration history is tracked for safety and reproducibility.

### 6. Update Your Schema

Edit `schema/01_employees.sql` to add a new column:

```sql
CREATE TABLE employees (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  age SMALLINT NOT NULL
);
```

Generate and apply the migration:

```sh
$ shem diff -m "add_age_column"
$ shem migrate
```

### 7. Rollback (Optional)

To rollback to a previous migration version:

```sh
$ shem reset --version 20241004112233
```

---

## Safety Features

- **DROP statement detection**: Shem warns you if a migration contains potentially destructive operations.
- **Shadow database**: All diffs are performed in an isolated environment, never against production data.
- **Migration history**: All applied migrations are tracked in a dedicated table.

---

## Advanced Usage

- **Custom schema file order**: Use numeric prefixes or configure `schema_paths` in your config file for precise control.
- **Multiple environments**: Use different config files for dev, staging, and production.
- **Glob patterns**: Organize your schema files by feature or domain.

---

## Development

To test the CLI in development mode without installing it system-wide:

```sh
# Run CLI directly with Cargo (dev mode)
$ cargo run --bin shem -- --help

# Initialize a new project
$ cargo run --bin shem -- init my_project

# 2. Change to the project directory
$ cd my_project

# Generate a migration diff
$ cargo run --bin shem -- diff

# Run tests
$ cargo test
```

**Note**: The `--` after `cargo run --bin shem` is important—it tells Cargo to pass the following arguments to your CLI, not to Cargo itself.

---

## Troubleshooting

### Database Connection Issues

**Error: "No database URL provided"**
- Make sure you've uncommented the `url` line in `shem.toml`
- Or provide the URL with `--database-url` parameter
- Verify your PostgreSQL server is running

**Error: "Connection refused"**
- Check if PostgreSQL is running: `pg_isready -h localhost -p 5432`
- Verify the host, port, and credentials in your database URL
- For Docker: ensure the container is running with `docker ps`

**Error: "Authentication failed"**
- Double-check username and password in the database URL
- Verify the user has access to the specified database
- For local PostgreSQL: try `psql -U postgres -d myapp_dev` to test connection

### Common Issues

**"Schema path does not exist"**
- Run `shem init` first to create the project structure
- Make sure you're in the correct directory
- Check that `schema/` directory exists

**"TOML parse error"**
- Ensure your `shem.toml` file has the correct structure
- Check for missing or extra brackets `[]`
- Verify all required fields are present

**"No migrations found"**
- Run `shem diff` first to generate migration files
- Check that `migrations/` directory contains `.sql` files
- Verify migration files have the correct timestamp format

### Verbose Output

For detailed debugging information, use the `--verbose` flag:

```sh
cargo run --bin shem -- --verbose diff
cargo run --bin shem -- --verbose migrate
```

---

## Contributing

Contributions are welcome! Please open issues or pull requests for bug fixes, features, or documentation improvements.

---

## License

MIT








## Sample data
-- ========== EXTENSIONS ==========
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "btree_gist";

-- ========== DOMAIN ==========
CREATE DOMAIN email AS TEXT
  CHECK (VALUE ~* '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$');

-- ========== ENUM ==========
CREATE TYPE user_role AS ENUM ('admin', 'user', 'guest');

-- ========== COMPOSITE TYPE ==========
CREATE TYPE address_type AS (
    street TEXT,
    city TEXT,
    zip TEXT
);

-- ========== RANGE TYPE ==========
CREATE TYPE int4range_custom AS RANGE (subtype = integer);

-- ========== COLLATION ==========
CREATE COLLATION german_ci (provider = icu, locale = 'de-DE-u-co-phonebk', deterministic = false);

-- ========== SEQUENCE ==========
CREATE SEQUENCE user_seq START 1000;

-- ========== TABLES, COLUMNS, CHECK, UNIQUE, FK ==========
CREATE TABLE departments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT UNIQUE NOT NULL,
    budget NUMERIC CHECK (budget > 0)
);

CREATE TABLE employees (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email email NOT NULL UNIQUE,
    role user_role NOT NULL DEFAULT 'user',
    salary NUMERIC CHECK (salary >= 0),
    department_id UUID REFERENCES departments(id),
    address address_type,
    created_at TIMESTAMP DEFAULT now(),
    CONSTRAINT salary_nonzero CHECK (salary <> 0)
);

-- ========== INDEX ==========
CREATE INDEX idx_employees_salary ON employees(salary);

-- ========== EXCLUSION CONSTRAINT ==========
CREATE TABLE meeting_rooms (
    id SERIAL PRIMARY KEY,
    room_name TEXT,
    during TSRANGE,
    EXCLUDE USING gist (room_name WITH =, during WITH &&)
);

-- ========== VIEW ==========
CREATE VIEW active_employees AS
SELECT id, email FROM employees WHERE role <> 'guest';

-- ========== MATERIALIZED VIEW ==========
CREATE MATERIALIZED VIEW department_budgets AS
SELECT d.name, SUM(e.salary) AS total_salary
FROM departments d
JOIN employees e ON e.department_id = d.id
GROUP BY d.name;

-- ========== FUNCTION ==========
CREATE FUNCTION get_department_budget(dept_id UUID) RETURNS NUMERIC AS $$
    SELECT COALESCE(SUM(salary), 0)
    FROM employees
    WHERE department_id = dept_id;
$$ LANGUAGE SQL;

-- ========== PROCEDURE ==========
CREATE PROCEDURE increase_salary_all(pct NUMERIC)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE employees SET salary = salary + (salary * pct);
END;
$$;

-- ========== FOREIGN SERVER ==========
CREATE SERVER foreign_pg_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'localhost', dbname 'other_db', port '5432');

-- ========== ROW-LEVEL SECURITY ==========
ALTER TABLE employees ENABLE ROW LEVEL SECURITY;

CREATE POLICY only_admins ON employees
  FOR SELECT USING (role = 'admin');

-- ========== TRIGGER FUNCTION & TRIGGER ==========
CREATE FUNCTION log_insert() RETURNS trigger AS $$
BEGIN
    RAISE NOTICE 'Inserted row with ID: %', NEW.id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_log_insert
AFTER INSERT ON employees
FOR EACH ROW
EXECUTE FUNCTION log_insert();

-- ========== CONSTRAINT TRIGGER ==========
CREATE FUNCTION validate_department_budget() RETURNS trigger AS $$
BEGIN
    IF (SELECT SUM(salary) FROM employees WHERE department_id = NEW.department_id) > 100000 THEN
        RAISE EXCEPTION 'Department budget exceeded';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_budget_check
AFTER INSERT OR UPDATE ON employees
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION validate_department_budget();

-- ========== EVENT TRIGGER ==========
CREATE FUNCTION on_ddl_command() RETURNS event_trigger AS $$
BEGIN
    RAISE NOTICE 'DDL Command: %', tg_tag;
END;
$$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER ddl_logger
ON ddl_command_start
EXECUTE FUNCTION on_ddl_command();

-- ========== COMMENTS ==========
COMMENT ON TABLE employees IS 'Stores employee data';
COMMENT ON COLUMN employees.email IS 'Work email of the employee';
COMMENT ON FUNCTION get_department_budget IS 'Returns total budget per department';





cargo check
cargo build


cargo upgrade




cargo build --workspace --release
cargo install --path crates/cli




cargo run -- init example2
shem diff --database-url "postgresql://postgres:postgres@localhost:5432/myapp_dev"


cargo run -- diff --database-url "postgresql://postgres:postgres@localhost:5432/myapp_dev"





Tables and columns
Indexes
Check, unique, and exclusion constraints
Foreign keys constraints
Enum types
Comments
Views
Materialized views
Stored procedures
Functions
Sequences
Extensions
Row-level security/policies
Composite types
Domain types
Range types
Foreign servers
Triggers, constraint triggers and event triggers