-- Test script to verify improved introspection
-- This script creates the same schema as in test/tables.sql but with proper cleanup

-- Clean up existing objects
DROP TABLE IF EXISTS adult_profiles CASCADE;
DROP TABLE IF EXISTS profiles CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS departments CASCADE;
DROP TYPE IF EXISTS user_role CASCADE;

-- 1. Extension for citext and UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "citext";

-- 2. Enum type for role
CREATE TYPE user_role AS ENUM ('admin', 'staff', 'viewer');

-- 3. Departments
CREATE TABLE departments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 4. Users
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email CITEXT NOT NULL UNIQUE,
    password TEXT NOT NULL,
    role user_role NOT NULL DEFAULT 'viewer',
    department_id UUID REFERENCES departments(id),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 5. Profiles
CREATE TABLE profiles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    full_name TEXT,
    bio TEXT,
    age INTEGER CHECK (age >= 0),
    preferences JSONB DEFAULT '{}'::jsonb,
    search_vector TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(full_name, '') || ' ' || coalesce(bio, ''))
    ) STORED
);

-- 6. Adult profiles (based on age >= 18)
CREATE VIEW TABLE adult_profiles AS
SELECT * FROM profiles WHERE age >= 18;

-- Insert test data
INSERT INTO departments (id, name) VALUES
  (uuid_generate_v4(), 'Engineering'),
  (uuid_generate_v4(), 'Marketing'),
  (uuid_generate_v4(), 'Human Resources');

INSERT INTO users (id, email, password, role, department_id)
SELECT
  uuid_generate_v4(),
  email,
  password,
  role::user_role,
  d.id
FROM (
  VALUES
    ('alice@example.com', 'hashedpassword1', 'admin'),
    ('bob@example.com', 'hashedpassword2', 'staff'),
    ('carol@example.com', 'hashedpassword3', 'viewer')
) AS u(email, password, role)
CROSS JOIN LATERAL (
  SELECT id FROM departments ORDER BY random() LIMIT 1
) d;

INSERT INTO profiles (id, user_id, full_name, bio, age, preferences)
SELECT
  uuid_generate_v4(),
  u.id,
  full_name,
  bio,
  age,
  prefs::jsonb
FROM (
  VALUES
    ('Alice Johnson', 'Loves backend dev', 28, '{"theme": "dark"}'),
    ('Bob Smith', 'Frontend wizard', 17, '{"notifications": false}'),
    ('Carol White', 'Marketing guru', 35, '{"theme": "light", "language": "en"}')
) AS p(full_name, bio, age, prefs)
JOIN users u ON u.email = lower(split_part(p.full_name, ' ', 1)) || '@example.com'; 