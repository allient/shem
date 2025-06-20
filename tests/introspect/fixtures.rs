/// SQL fixtures for testing different PostgreSQL objects in isolation

pub const TABLES_FIXTURE: &str = r#"
-- Test tables with various column types and constraints
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INTEGER CHECK (age >= 0),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE posts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    title VARCHAR(200) NOT NULL,
    content TEXT,
    published_at TIMESTAMP,
    tags TEXT[]
);

CREATE TABLE comments (
    id BIGSERIAL PRIMARY KEY,
    post_id UUID REFERENCES posts(id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"#;

pub const VIEWS_FIXTURE: &str = r#"
-- Test views
CREATE VIEW active_users AS
SELECT id, username, email, created_at
FROM users
WHERE is_active = true;

CREATE VIEW post_stats AS
SELECT 
    p.id,
    p.title,
    COUNT(c.id) as comment_count,
    p.published_at
FROM posts p
LEFT JOIN comments c ON p.id = c.post_id
GROUP BY p.id, p.title, p.published_at;

CREATE VIEW recent_activity AS
SELECT 'post' as type, id, title as name, published_at as activity_date
FROM posts
WHERE published_at IS NOT NULL
UNION ALL
SELECT 'comment' as type, id, content as name, created_at as activity_date
FROM comments
ORDER BY activity_date DESC;
"#;

pub const MATERIALIZED_VIEWS_FIXTURE: &str = r#"
-- Test materialized views
CREATE MATERIALIZED VIEW user_post_counts AS
SELECT 
    u.id,
    u.username,
    COUNT(p.id) as post_count,
    COUNT(c.id) as comment_count
FROM users u
LEFT JOIN posts p ON u.id = p.user_id
LEFT JOIN comments c ON u.id = c.user_id
GROUP BY u.id, u.username;

CREATE MATERIALIZED VIEW popular_posts AS
SELECT 
    p.id,
    p.title,
    COUNT(c.id) as comment_count,
    p.published_at
FROM posts p
LEFT JOIN comments c ON p.id = c.post_id
WHERE p.published_at IS NOT NULL
GROUP BY p.id, p.title, p.published_at
HAVING COUNT(c.id) > 0
ORDER BY COUNT(c.id) DESC;
"#;

pub const FUNCTIONS_FIXTURE: &str = r#"
-- Test functions
CREATE FUNCTION get_user_posts(user_id INTEGER)
RETURNS TABLE(id UUID, title VARCHAR, published_at TIMESTAMP) AS $$
BEGIN
    RETURN QUERY
    SELECT p.id, p.title, p.published_at
    FROM posts p
    WHERE p.user_id = get_user_posts.user_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION calculate_age(birth_date DATE)
RETURNS INTEGER AS $$
BEGIN
    RETURN EXTRACT(YEAR FROM AGE(birth_date));
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_post_comments(post_uuid UUID)
RETURNS SETOF comments AS $$
BEGIN
    RETURN QUERY
    SELECT c.*
    FROM comments c
    WHERE c.post_id = post_uuid
    ORDER BY c.created_at;
END;
$$ LANGUAGE plpgsql;
"#;

pub const PROCEDURES_FIXTURE: &str = r#"
-- Test procedures
CREATE PROCEDURE create_user(
    IN p_username VARCHAR,
    IN p_email VARCHAR,
    IN p_age INTEGER,
    OUT p_user_id INTEGER
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO users (username, email, age)
    VALUES (p_username, p_email, p_age)
    RETURNING id INTO p_user_id;
    
    COMMIT;
END;
$$;

CREATE PROCEDURE archive_old_posts(
    IN p_days_old INTEGER DEFAULT 365
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- This is a placeholder procedure
    -- In a real scenario, you might move old posts to an archive table
    RAISE NOTICE 'Archiving posts older than % days', p_days_old;
END;
$$;
"#;

pub const ENUMS_FIXTURE: &str = r#"
-- Test enums
CREATE TYPE user_status AS ENUM ('active', 'inactive', 'suspended', 'pending');
CREATE TYPE post_type AS ENUM ('article', 'news', 'tutorial', 'review');
CREATE TYPE comment_status AS ENUM ('approved', 'pending', 'rejected', 'spam');
CREATE TYPE priority_level AS ENUM ('low', 'medium', 'high', 'critical');
"#;

pub const DOMAINS_FIXTURE: &str = r#"
-- Test domains
CREATE DOMAIN email_address AS VARCHAR(255) 
CHECK (VALUE ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');

CREATE DOMAIN positive_integer AS INTEGER 
CHECK (VALUE > 0);

CREATE DOMAIN non_empty_string AS VARCHAR 
CHECK (LENGTH(TRIM(VALUE)) > 0);

CREATE DOMAIN url_string AS VARCHAR(2048) 
CHECK (VALUE ~* '^https?://');

CREATE DOMAIN phone_number AS VARCHAR(20) 
CHECK (VALUE ~* '^\+?[0-9\s\-\(\)]+$');
"#;

pub const COMPOSITE_TYPES_FIXTURE: &str = r#"
-- Test composite types
CREATE TYPE address AS (
    street VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    country VARCHAR(100)
);

CREATE TYPE contact_info AS (
    email email_address,
    phone phone_number,
    address address
);

CREATE TYPE post_metadata AS (
    tags TEXT[],
    categories TEXT[],
    author_info contact_info,
    seo_title VARCHAR(255),
    seo_description TEXT
);
"#;

pub const RANGE_TYPES_FIXTURE: &str = r#"
-- Test range types
CREATE TYPE date_range AS RANGE (subtype = date);
CREATE TYPE int4_range AS RANGE (subtype = int4);
CREATE TYPE num_range AS RANGE (subtype = numeric);
CREATE TYPE ts_range AS RANGE (subtype = timestamp);
"#;

pub const SEQUENCES_FIXTURE: &str = r#"
-- Test sequences
CREATE SEQUENCE custom_user_id_seq START 1000 INCREMENT 5;
CREATE SEQUENCE order_number_seq START 1 INCREMENT 1;
CREATE SEQUENCE invoice_seq START 10000 INCREMENT 1;
CREATE SEQUENCE ticket_seq START 1 INCREMENT 1 CYCLE;
"#;

pub const EXTENSIONS_FIXTURE: &str = r#"
-- Test extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "citext";
CREATE EXTENSION IF NOT EXISTS "hstore";
"#;

pub const TRIGGERS_FIXTURE: &str = r#"
-- Test triggers
CREATE TRIGGER update_user_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER update_post_updated_at
    BEFORE UPDATE ON posts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER log_user_changes
    AFTER INSERT OR UPDATE OR DELETE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();
"#;

pub const POLICIES_FIXTURE: &str = r#"
-- Test policies (requires RLS to be enabled)
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE posts ENABLE ROW LEVEL SECURITY;
ALTER TABLE comments ENABLE ROW LEVEL SECURITY;

CREATE POLICY users_select_policy ON users
    FOR SELECT USING (is_active = true);

CREATE POLICY users_insert_policy ON users
    FOR INSERT WITH CHECK (username IS NOT NULL AND email IS NOT NULL);

CREATE POLICY posts_select_policy ON posts
    FOR SELECT USING (published_at IS NOT NULL);

CREATE POLICY comments_select_policy ON comments
    FOR SELECT USING (true);
"#;

pub const COLLATIONS_FIXTURE: &str = r#"
-- Test collations
CREATE COLLATION german_phonebook (provider = icu, locale = 'de-u-co-phonebk');
CREATE COLLATION french_phonebook (provider = icu, locale = 'fr-u-co-phonebk');
CREATE COLLATION spanish_phonebook (provider = icu, locale = 'es-u-co-phonebk');
"#;

pub const RULES_FIXTURE: &str = r#"
-- Test rules
CREATE RULE prevent_user_deletion AS
    ON DELETE TO users DO INSTEAD
    UPDATE users SET is_active = false WHERE id = OLD.id;

CREATE RULE log_post_changes AS
    ON UPDATE TO posts DO ALSO
    INSERT INTO post_audit_log (post_id, action, changed_at)
    VALUES (NEW.id, 'UPDATE', CURRENT_TIMESTAMP);

CREATE RULE prevent_comment_spam AS
    ON INSERT TO comments DO INSTEAD
    INSERT INTO comments (post_id, user_id, content, created_at)
    SELECT NEW.post_id, NEW.user_id, NEW.content, CURRENT_TIMESTAMP
    WHERE LENGTH(NEW.content) > 10;
"#;

pub const SERVERS_FIXTURE: &str = r#"
-- Test foreign servers
CREATE SERVER external_db
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'external.example.com', port '5432', dbname 'external_db');

CREATE SERVER analytics_db
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'analytics.example.com', port '5432', dbname 'analytics');
"#; 