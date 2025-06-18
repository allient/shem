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