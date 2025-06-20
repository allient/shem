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
        WHERE status = 'active';
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
        CREATE SCHEMA IF NOT EXISTS extensions;
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA extensions;
    "#;

    /// Extension with all options
    pub const EXTENSION_WITH_ALL_OPTIONS: &str = r#"
        CREATE SCHEMA IF NOT EXISTS extensions;
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp" VERSION '1.1' SCHEMA extensions CASCADE;
        COMMENT ON EXTENSION "uuid-ossp" IS 'UUID generation extension with all options';
    "#;

    /// Extension with objects using it
    pub const EXTENSION_WITH_OBJECTS: &str = r#"
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        
        CREATE TABLE documents (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            title TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE FUNCTION generate_document_id() RETURNS UUID AS $$
        BEGIN
            RETURN gen_random_uuid();
        END;
        $$ LANGUAGE plpgsql;
    "#;

    /// Complete schema with multiple objects
    pub const COMPLETE_SCHEMA: &str = r#"
        -- Extensions
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        
        -- Enums
        CREATE TYPE user_status AS ENUM ('active', 'inactive', 'suspended');
        CREATE TYPE post_status AS ENUM ('draft', 'published', 'archived');
        
        -- Domains
        CREATE DOMAIN email_address AS VARCHAR(255)
        CHECK (VALUE ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
        
        -- Sequences
        CREATE SEQUENCE custom_id_seq START 1000;
        
        -- Tables
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email email_address UNIQUE NOT NULL,
            status user_status DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            title VARCHAR(255) NOT NULL,
            content TEXT,
            status post_status DEFAULT 'draft',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Views
        CREATE VIEW active_users AS
        SELECT id, name, email, created_at
        FROM users
        WHERE status = 'active';
        
        CREATE VIEW published_posts AS
        SELECT p.id, p.title, p.content, u.name as author_name, p.created_at
        FROM posts p
        JOIN users u ON p.user_id = u.id
        WHERE p.status = 'published';
        
        -- Functions
        CREATE OR REPLACE FUNCTION get_user_count()
        RETURNS INTEGER AS $$
        BEGIN
            RETURN (SELECT COUNT(*) FROM users);
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE OR REPLACE FUNCTION get_posts_by_user(user_id INTEGER)
        RETURNS TABLE (
            id INTEGER,
            title VARCHAR(255),
            status post_status,
            created_at TIMESTAMP
        ) AS $$
        BEGIN
            RETURN QUERY
            SELECT p.id, p.title, p.status, p.created_at
            FROM posts p
            WHERE p.user_id = get_posts_by_user.user_id;
        END;
        $$ LANGUAGE plpgsql;
        
        -- Triggers
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
            
        CREATE TRIGGER update_posts_updated_at
            BEFORE UPDATE ON posts
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at();
    "#;
}

/// Expected schema output fixtures
pub mod expected {
    use super::*;

    /// Expected output for simple table introspection
    pub const SIMPLE_TABLE_SCHEMA: &str = r#"
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    "#;

    /// Expected output for enum type introspection
    pub const ENUM_TYPE_SCHEMA: &str = r#"
        CREATE TYPE user_status AS ENUM ('active', 'inactive', 'suspended');
    "#;

    /// Expected output for view introspection
    pub const VIEW_SCHEMA: &str = r#"
        CREATE VIEW active_users AS
        SELECT id, name, email
        FROM users
        WHERE status = 'active';
    "#;

    /// Expected output for extension introspection
    pub const EXTENSION_SCHEMA: &str = r#"
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    "#;

    /// Expected output for extension with version
    pub const EXTENSION_WITH_VERSION_SCHEMA: &str = r#"
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp" VERSION '1.1';
    "#;

    /// Expected output for extension with schema
    pub const EXTENSION_WITH_SCHEMA_SCHEMA: &str = r#"
        CREATE SCHEMA IF NOT EXISTS extensions;
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA extensions;
    "#;
}

/// Test configuration fixtures
pub mod config {
    /// Default test configuration
    pub const DEFAULT_CONFIG: &str = r#"
        [database]
        url = "postgresql://postgres:postgres@localhost:5432/shem_test"
        
        [output]
        format = "sql"
        directory = "schema"
    "#;

    /// Configuration with custom settings
    pub const CUSTOM_CONFIG: &str = r#"
        [database]
        url = "postgresql://test:test@localhost:5432/test_db"
        
        [output]
        format = "sql"
        directory = "custom_schema"
        
        [introspect]
        include_system_objects = false
        exclude_schemas = ["information_schema", "pg_catalog"]
    "#;
} 