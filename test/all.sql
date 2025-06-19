-- Load extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "citext";

-- ENUM type
CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok');

-- DOMAIN with constraint
CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);

-- Composite Type
CREATE TYPE address_type AS (
    street TEXT,
    city TEXT,
    zipcode TEXT
);

-- Range Type
CREATE TYPE salary_range AS RANGE (subtype = numeric);

-- Collation
--CREATE COLLATION german (provider = icu, locale = 'de-u-co-phonebk', deterministic = false);

-- Main table with all base types
CREATE TABLE sample_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    small_val SMALLINT NOT NULL,
    int_val INTEGER NOT NULL,
    big_val BIGINT NOT NULL,
    serial_val SERIAL,
    bigserial_val BIGSERIAL,
    decimal_val DECIMAL(10,2),
    numeric_val NUMERIC(10,4),
    real_val REAL,
    double_val DOUBLE PRECISION,
    money_val MONEY,

    char_val CHAR(5),
    varchar_val VARCHAR(100),
    text_val TEXT,
    citext_val CITEXT,
    name_val NAME,

    bool_val BOOLEAN,
    date_val DATE,
    time_val TIME,
    timetz_val TIMETZ,
    timestamp_val TIMESTAMP,
    timestamptz_val TIMESTAMPTZ,
    interval_val INTERVAL,

    bytea_val BYTEA,
    uuid_val UUID,

    inet_val INET,
    cidr_val CIDR,
    macaddr_val MACADDR,
    macaddr8_val MACADDR8,

    json_val JSON,
    jsonb_val JSONB,
    int_array INTEGER[],

    tsvector_val TSVECTOR,
    tsquery_val TSQUERY,
    xml_val XML,
    bit_val BIT(4),
    varbit_val BIT VARYING(8),
    mood_val MOOD,
    point_val POINT,
    line_val LINE,
    lseg_val LSEG,
    box_val BOX,
    path_val PATH,
    polygon_val POLYGON,
    circle_val CIRCLE,
    address address_type,
    salary_range salary_range,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT positive_salary CHECK (decimal_val > 0),
    CONSTRAINT unique_nullable UNIQUE (text_val),
    CONSTRAINT always_null UNIQUE (varchar_val)
);

-- Related table with FK and NULL/NOT NULL combo
CREATE TABLE related_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sample_id UUID NOT NULL,
    optional_sample_id UUID,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    comment TEXT UNIQUE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    -- Explicit foreign key constraints
    CONSTRAINT fk_sample_id FOREIGN KEY (sample_id) REFERENCES sample_data(id) ON DELETE CASCADE,
    CONSTRAINT fk_optional_sample FOREIGN KEY (optional_sample_id) REFERENCES sample_data(id)
);

-- Sequence
CREATE SEQUENCE custom_seq START 100;

-- Function
CREATE FUNCTION square(x INTEGER) RETURNS INTEGER AS $$
BEGIN
    RETURN x * x;
END;
$$ LANGUAGE plpgsql;

-- Replacing procedure with function for event trigger
CREATE OR REPLACE FUNCTION log_change() RETURNS event_trigger AS $$
BEGIN
    RAISE NOTICE 'DDL command ended.';
END;
$$ LANGUAGE plpgsql;

-- Trigger Function
CREATE FUNCTION trg_set_time() RETURNS trigger AS $$
BEGIN
    NEW.created_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger
CREATE TRIGGER set_created_at
BEFORE INSERT ON related_data
FOR EACH ROW EXECUTE FUNCTION trg_set_time();

-- View
CREATE VIEW active_samples AS
SELECT * FROM sample_data WHERE bool_val = TRUE;

-- Materialized View
CREATE MATERIALIZED VIEW cached_samples AS
SELECT id, decimal_val FROM sample_data WHERE decimal_val > 100;

-- Policy
CREATE POLICY select_policy ON sample_data
    FOR SELECT USING (bool_val = TRUE);

-- Rule
CREATE RULE prevent_update AS
    ON UPDATE TO sample_data DO INSTEAD NOTHING;

-- Event Trigger
CREATE EVENT TRIGGER log_ddl
    ON ddl_command_end
    EXECUTE FUNCTION log_change();

-- Constraint Trigger
CREATE CONSTRAINT TRIGGER ensure_positive_salary
AFTER INSERT OR UPDATE ON sample_data
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
WHEN (NEW.decimal_val <= 0)
EXECUTE FUNCTION trg_set_time();

