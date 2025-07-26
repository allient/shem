//! Test fixtures and mock data for CLI tests

/// SQL fixtures for testing different database objects
pub mod sql {
    /// Basic table creation SQL
    pub const SIMPLE_TABLE: &str = r#"
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    "#;

    /// Table with foreign key
    pub const TABLE_WITH_FK: &str = r#"
        CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            title VARCHAR(255) NOT NULL,
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    "#;

    /// Enum type
    pub const ENUM_TYPE: &str = r#"
        CREATE TYPE user_status AS ENUM ('active', 'inactive', 'suspended');
    "#;

    /// Table using enum
    pub const TABLE_WITH_ENUM: &str = r#"
        CREATE TABLE user_profiles (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            status user_status DEFAULT 'active',
            bio TEXT
        );
    "#;

    /// View
    pub const VIEW: &str = r#"
        CREATE VIEW active_users AS
        SELECT id, name, email
        FROM users
        WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '30 days';
    "#;

    /// Function
    pub const FUNCTION: &str = r#"
        CREATE OR REPLACE FUNCTION get_user_count()
        RETURNS INTEGER AS $$
        BEGIN
            RETURN (SELECT COUNT(*) FROM users);
        END;
        $$ LANGUAGE plpgsql;
    "#;

    /// Trigger
    pub const TRIGGER: &str = r#"
        CREATE OR REPLACE FUNCTION update_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER update_users_updated_at
            BEFORE UPDATE ON users
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at();
    "#;

    /// Sequence
    pub const SEQUENCE: &str = r#"
        CREATE SEQUENCE custom_id_seq START 1000;
    "#;

    /// Domain
    pub const DOMAIN: &str = r#"
        CREATE DOMAIN email_address AS VARCHAR(255)
        CHECK (VALUE ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
    "#;

    /// Extension
    pub const EXTENSION: &str = r#"
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    "#;

    /// Multiple extensions
    pub const MULTIPLE_EXTENSIONS: &str = r#"
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        CREATE EXTENSION IF NOT EXISTS "pgcrypto";
        CREATE EXTENSION IF NOT EXISTS "citext";
    "#;

    /// Extension with version
    pub const EXTENSION_WITH_VERSION: &str = r#"
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp" VERSION '1.1';
    "#;

    /// Extension with schema
    pub const EXTENSION_WITH_SCHEMA: &str = r#"
        CREATE EXTENSION IF NOT EXISTS "pgcrypto" SCHEMA crypto;
    "#;

    /// Extension with all options
    pub const EXTENSION_WITH_ALL_OPTIONS: &str = r#"
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp" 
        SCHEMA extensions 
        VERSION '1.1' 
        CASCADE;
    "#;

    /// Extension with objects
    pub const EXTENSION_WITH_OBJECTS: &str = r#"
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        CREATE TABLE test_table (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            name TEXT
        );
    "#;

    // Schema-related fixtures

    /// Simple schema
    pub const SIMPLE_SCHEMA: &str = r#"
        CREATE SCHEMA IF NOT EXISTS test_schema;
    "#;

    /// Schema with owner
    pub const SCHEMA_WITH_OWNER: &str = r#"
        CREATE SCHEMA IF NOT EXISTS app_schema AUTHORIZATION postgres;
    "#;

    /// Multiple schemas
    pub const MULTIPLE_SCHEMAS: &str = r#"
        CREATE SCHEMA IF NOT EXISTS app;
        CREATE SCHEMA IF NOT EXISTS audit;
        CREATE SCHEMA IF NOT EXISTS reporting;
    "#;

    /// Schema with various objects
    pub const SCHEMA_WITH_OBJECTS: &str = r#"
        CREATE SCHEMA IF NOT EXISTS app;
        
        CREATE TYPE app.user_status AS ENUM ('active', 'inactive', 'suspended');
        
        CREATE TABLE app.users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            status app.user_status DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE OR REPLACE FUNCTION app.get_user_count()
        RETURNS INTEGER AS $$
        BEGIN
            RETURN (SELECT COUNT(*) FROM app.users);
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE VIEW app.active_users AS
        SELECT id, name, email
        FROM app.users
        WHERE status = 'active';
    "#;

    /// Schema with comment
    pub const SCHEMA_WITH_COMMENT: &str = r#"
        CREATE SCHEMA IF NOT EXISTS documented_schema;
        COMMENT ON SCHEMA documented_schema IS 'Application schema for user management';
    "#;

    /// Schemas with dependencies
    pub const SCHEMA_DEPENDENCIES: &str = r#"
        CREATE SCHEMA IF NOT EXISTS base;
        CREATE SCHEMA IF NOT EXISTS dependent;
        
        CREATE TABLE base.core_table (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );
        
        CREATE TABLE dependent.dependent_table (
            id SERIAL PRIMARY KEY,
            core_id INTEGER REFERENCES base.core_table(id),
            description TEXT
        );
    "#;

    /// Schema with extension
    pub const SCHEMA_WITH_EXTENSION: &str = r#"
        CREATE SCHEMA IF NOT EXISTS crypto_schema;
        CREATE EXTENSION IF NOT EXISTS "pgcrypto" SCHEMA crypto_schema;
    "#;

    /// Schema with role
    pub const SCHEMA_WITH_ROLE: &str = r#"
        CREATE ROLE app_user WITH LOGIN PASSWORD 'password';
        CREATE SCHEMA IF NOT EXISTS app_schema AUTHORIZATION app_user;
    "#;

    /// Schema with invalid objects (for testing error handling)
    pub const SCHEMA_WITH_INVALID_OBJECTS: &str = r#"
        CREATE SCHEMA IF NOT EXISTS test_schema;
        
        -- This will fail but shouldn't break schema introspection
        CREATE TABLE test_schema.invalid_table (
            id INTEGER REFERENCES nonexistent_table(id)
        );
    "#;

    /// Complete schema with all object types
    pub const COMPLETE_SCHEMA: &str = r#"
        -- Create schemas
        CREATE SCHEMA IF NOT EXISTS app;
        CREATE SCHEMA IF NOT EXISTS audit;
        CREATE SCHEMA IF NOT EXISTS reporting;
        
        -- Create extensions
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        CREATE EXTENSION IF NOT EXISTS "pgcrypto" SCHEMA app;
        
        -- Create types
        CREATE TYPE app.user_status AS ENUM ('active', 'inactive', 'suspended');
        CREATE DOMAIN app.email_address AS VARCHAR(255)
            CHECK (VALUE ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
        
        -- Create sequences
        CREATE SEQUENCE app.user_id_seq START 1000;
        
        -- Create tables
        CREATE TABLE app.users (
            id INTEGER PRIMARY KEY DEFAULT nextval('app.user_id_seq'),
            name VARCHAR(255) NOT NULL,
            email app.email_address UNIQUE NOT NULL,
            status app.user_status DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE app.posts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES app.users(id),
            title VARCHAR(255) NOT NULL,
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create views
        CREATE VIEW app.active_users AS
        SELECT id, name, email
        FROM app.users
        WHERE status = 'active';
        
        -- Create functions
        CREATE OR REPLACE FUNCTION app.get_user_count()
        RETURNS INTEGER AS $$
        BEGIN
            RETURN (SELECT COUNT(*) FROM app.users);
        END;
        $$ LANGUAGE plpgsql;
        
        -- Create triggers
        CREATE OR REPLACE FUNCTION app.update_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER update_users_updated_at
            BEFORE UPDATE ON app.users
            FOR EACH ROW
            EXECUTE FUNCTION app.update_updated_at();
        
        -- Create policies
        ALTER TABLE app.users ENABLE ROW LEVEL SECURITY;
        CREATE POLICY users_select_policy ON app.users
            FOR SELECT USING (status = 'active');
        
        -- Create comments
        COMMENT ON SCHEMA app IS 'Application schema for user management';
        COMMENT ON TABLE app.users IS 'User accounts table';
        COMMENT ON COLUMN app.users.email IS 'User email address';
    "#;
}

pub mod expected {
    /// Expected schema output for simple table
    pub const SIMPLE_TABLE_SCHEMA: &str = r#"
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    "#;

    /// Expected schema output for enum type
    pub const ENUM_TYPE_SCHEMA: &str = r#"
        CREATE TYPE user_status AS ENUM ('active', 'inactive', 'suspended');
    "#;

    /// Expected schema output for view
    pub const VIEW_SCHEMA: &str = r#"
        CREATE VIEW active_users AS
        SELECT id, name, email
        FROM users
        WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '30 days';
    "#;

    /// Expected schema output for extension
    pub const EXTENSION_SCHEMA: &str = r#"
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    "#;

    /// Expected schema output for extension with version
    pub const EXTENSION_WITH_VERSION_SCHEMA: &str = r#"
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp" VERSION '1.1';
    "#;

    /// Expected schema output for extension with schema
    pub const EXTENSION_WITH_SCHEMA_SCHEMA: &str = r#"
        CREATE EXTENSION IF NOT EXISTS "pgcrypto" SCHEMA crypto;
    "#;
}

pub mod config {
    /// Default configuration
    pub const DEFAULT_CONFIG: &str = r#"
[database]
url = "postgresql://postgres:postgres@localhost:5432/test_db"

[declarative]
enabled = true
schema_paths = ["./schema/*.sql"]
shadow_port = 5432
    "#;

    /// Custom configuration
    pub const CUSTOM_CONFIG: &str = r#"
[database]
url = "postgresql://user:pass@localhost:5432/custom_db"

[declarative]
enabled = true
schema_paths = ["./schemas/**/*.sql", "./types/*.sql"]
shadow_port = 5433
    "#;
} 