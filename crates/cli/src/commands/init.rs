use crate::config::Config;
use anyhow::{Context, Result};
use std::path::PathBuf;
use tracing::info;

pub async fn execute(path: &str, _config: &Config) -> Result<()> {
    let base_path = PathBuf::from(path);
    
    // Create base directory if it doesn't exist
    std::fs::create_dir_all(&base_path)
        .context("Failed to create base directory")?;
    
    // Create schema directory
    let schema_path = base_path.join("schema");
    std::fs::create_dir_all(&schema_path)
        .context("Failed to create schema directory")?;
    
    // Create migrations directory
    let migrations_path = base_path.join("migrations");
    std::fs::create_dir_all(&migrations_path)
        .context("Failed to create migrations directory")?;
    
    // Create initial schema file
    let initial_schema = schema_path.join("00_initial.sql");
    let schema_content = r"-- Initial schema file
-- Add your initial schema definitions here
-- Files in this directory are processed in alphabetical order
-- Use numeric prefixes to control the order, e.g.:
--   00_initial.sql
--   01_users.sql
--   02_profiles.sql
--   etc.

-- Example table definition:
CREATE TABLE example (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);";
    
    std::fs::write(&initial_schema, schema_content)
        .context("Failed to write initial schema file")?;
    
    // Create README.md in schema directory explaining the convention
    let readme_path = schema_path.join("README.md");
    let readme_content = r#"# Schema Directory

This directory contains SQL schema files that define your database structure. Files are processed in alphabetical order, so you can control the order of execution using numeric prefixes.

## Naming Convention

Use numeric prefixes to control the order of schema files:

- `00_initial.sql` - Initial schema setup
- `01_users.sql` - User-related tables
- `02_profiles.sql` - Profile-related tables
- etc.

## File Organization

You can organize your schema files by feature or domain:

- `00_initial.sql` - Initial setup, extensions, types
- `01_auth.sql` - Authentication tables
- `02_users.sql` - User management
- `03_profiles.sql` - User profiles
- `04_content.sql` - Content management
- etc.

## Best Practices

1. Use meaningful prefixes to indicate dependencies
2. Keep related tables in the same file
3. Use descriptive names that indicate the purpose
4. Add comments to explain complex schemas
5. Consider dependencies when ordering files

## Example

```sql
-- 01_users.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 02_profiles.sql
CREATE TABLE profiles (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```
"#;
    
    std::fs::write(&readme_path, readme_content)
        .context("Failed to write schema README")?;
    
    info!("Initialized schema project at {}", base_path.display());
    info!("Created schema directory at {}", schema_path.display());
    info!("Created migrations directory at {}", migrations_path.display());
    info!("Created initial schema file at {}", initial_schema.display());
    info!("Created schema README at {}", readme_path.display());
    
    Ok(())
}
