CREATE EXTENSION IF NOT EXISTS postgres_fdw VERSION '1.1';

CREATE EXTENSION IF NOT EXISTS pgcrypto VERSION '1.3';

CREATE EXTENSION IF NOT EXISTS plpgsql VERSION '1.0';

CREATE EXTENSION IF NOT EXISTS uuid-ossp VERSION '1.1';

CREATE EXTENSION IF NOT EXISTS btree_gist VERSION '1.7';

CREATE TYPE public.users AS ();

CREATE TYPE public.department_budgets AS ();

CREATE TYPE public.active_employees AS ();

CREATE TYPE public.departments AS ();

CREATE TYPE public.address_type AS ();

CREATE TYPE public.animal_type AS ENUM ();

CREATE TYPE public.user_role AS ENUM ();

CREATE TYPE public.int4range_custom AS RANGE;

CREATE TYPE public.employees AS ();

CREATE TYPE public.meeting_rooms AS ();

CREATE DOMAIN public.email AS text;

CREATE SEQUENCE public.meeting_rooms_id_seq MINVALUE 1 MAXVALUE 2147483647;

CREATE SEQUENCE public.user_seq START WITH 1000 MINVALUE 1 MAXVALUE 9223372036854775807;

CREATE TABLE public.employees (id uuid NOT NULL DEFAULT uuid_generate_v4(),
    email text NOT NULL,
    role USER-DEFINED NOT NULL DEFAULT 'user'::user_role,
    salary numeric,
    department_id uuid,
    address USER-DEFINED,
    created_at timestamp without time zone DEFAULT now(),
    ((salary >= (0)::numeric)),
    ((salary <> (0)::numeric)),
    PRIMARY KEY (id),
    UNIQUE (email),
    FOREIGN KEY (department_id) REFERENCES public.departments (id) ON UPDATE NO ACTION ON DELETE NO ACTION,
    id IS NOT NULL,
    email IS NOT NULL,
    role IS NOT NULL
);

CREATE TABLE public.users (id uuid NOT NULL DEFAULT gen_random_uuid(),
    email text NOT NULL,
    password_hash text NOT NULL,
    full_name text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE (email),
    id IS NOT NULL,
    email IS NOT NULL,
    password_hash IS NOT NULL
);

CREATE TABLE public.departments (id uuid NOT NULL DEFAULT uuid_generate_v4(),
    name text NOT NULL,
    budget numeric,
    ((budget > (0)::numeric)),
    PRIMARY KEY (id),
    UNIQUE (name),
    id IS NOT NULL,
    name IS NOT NULL
);

CREATE TABLE public.meeting_rooms (id integer NOT NULL DEFAULT nextval('meeting_rooms_id_seq'::regclass),
    room_name text,
    during tsrange,
    PRIMARY KEY (id),
    id IS NOT NULL
);

CREATE VIEW public.active_employees AS  SELECT id,
    email
   FROM employees
  WHERE (role <> 'guest'::user_role);;

CREATE MATERIALIZED VIEW public.department_budgets AS  SELECT d.name,
    sum(e.salary) AS total_salary
   FROM (departments d
     JOIN employees e ON ((e.department_id = d.id)))
  GROUP BY d.name;;

CREATE FUNCTION public.pgp_armor_headers (IN OUT key, IN OUT value) RETURNS SETOF record LANGUAGE c AS pgp_armor_headers;

CREATE FUNCTION public.gbt_bit_penalty () RETURNS internal LANGUAGE c AS gbt_bit_penalty;

CREATE FUNCTION public.gbt_int4_picksplit () RETURNS internal LANGUAGE c AS gbt_int4_picksplit;

CREATE FUNCTION public.gbt_float4_fetch () RETURNS internal LANGUAGE c AS gbt_float4_fetch;

CREATE FUNCTION public.gbt_text_penalty () RETURNS internal LANGUAGE c AS gbt_text_penalty;

CREATE FUNCTION public.pgp_pub_encrypt_bytea () RETURNS bytea LANGUAGE c AS pgp_pub_encrypt_bytea;

CREATE FUNCTION public.gbt_bool_compress () RETURNS internal LANGUAGE c AS gbt_bool_compress;

CREATE FUNCTION public.gbt_inet_compress () RETURNS internal LANGUAGE c AS gbt_inet_compress;

CREATE FUNCTION public.gbt_decompress () RETURNS internal LANGUAGE c AS gbt_decompress;

CREATE FUNCTION public.decrypt () RETURNS bytea LANGUAGE c AS pg_decrypt;

CREATE FUNCTION public.gbt_tstz_compress () RETURNS internal LANGUAGE c AS gbt_tstz_compress;

CREATE FUNCTION public.gbt_time_compress () RETURNS internal LANGUAGE c AS gbt_time_compress;

CREATE FUNCTION public.pgp_pub_decrypt_bytea () RETURNS bytea LANGUAGE c AS pgp_pub_decrypt_bytea;

CREATE FUNCTION public.gbt_intv_consistent () RETURNS boolean LANGUAGE c AS gbt_intv_consistent;

CREATE FUNCTION public.uuid_ns_dns () RETURNS uuid LANGUAGE c AS uuid_ns_dns;

CREATE FUNCTION public.gbt_macad_penalty () RETURNS internal LANGUAGE c AS gbt_macad_penalty;

CREATE FUNCTION public.gbt_var_decompress () RETURNS internal LANGUAGE c AS gbt_var_decompress;

CREATE FUNCTION public.gbt_int4_penalty () RETURNS internal LANGUAGE c AS gbt_int4_penalty;

CREATE FUNCTION public.cash_dist () RETURNS money LANGUAGE c AS cash_dist;

CREATE FUNCTION public.gbt_inet_consistent () RETURNS boolean LANGUAGE c AS gbt_inet_consistent;

CREATE FUNCTION public.gbt_ts_distance (IN timestamp without) RETURNS double precision LANGUAGE c AS gbt_ts_distance;

CREATE FUNCTION public.gbt_time_same () RETURNS internal LANGUAGE c AS gbt_time_same;

CREATE FUNCTION public.gbtreekey4_in () RETURNS gbtreekey4 LANGUAGE c AS gbtreekey_in;

CREATE FUNCTION public.int8_dist () RETURNS bigint LANGUAGE c AS int8_dist;

CREATE FUNCTION public.dearmor () RETURNS bytea LANGUAGE c AS pg_dearmor;

CREATE FUNCTION public.gbt_text_picksplit () RETURNS internal LANGUAGE c AS gbt_text_picksplit;

CREATE FUNCTION public.gbt_numeric_compress () RETURNS internal LANGUAGE c AS gbt_numeric_compress;

CREATE FUNCTION public.gbt_bool_fetch () RETURNS internal LANGUAGE c AS gbt_bool_fetch;

CREATE FUNCTION public.tstz_dist (IN timestamp with, IN timestamp with) RETURNS interval LANGUAGE c AS tstz_dist;

CREATE FUNCTION public.gbt_cash_same () RETURNS internal LANGUAGE c AS gbt_cash_same;

CREATE FUNCTION public.uuid_ns_oid () RETURNS uuid LANGUAGE c AS uuid_ns_oid;

CREATE FUNCTION public.gbtreekey8_in () RETURNS gbtreekey8 LANGUAGE c AS gbtreekey_in;

CREATE FUNCTION public.gbt_int8_union () RETURNS gbtreekey16 LANGUAGE c AS gbt_int8_union;

CREATE FUNCTION public.gbt_date_fetch () RETURNS internal LANGUAGE c AS gbt_date_fetch;

CREATE FUNCTION public.gbt_int8_picksplit () RETURNS internal LANGUAGE c AS gbt_int8_picksplit;

CREATE FUNCTION public.gbt_int2_penalty () RETURNS internal LANGUAGE c AS gbt_int2_penalty;

CREATE FUNCTION public.uuid_generate_v1 () RETURNS uuid LANGUAGE c AS uuid_generate_v1;

CREATE FUNCTION public.gbt_float8_distance (IN double precision) RETURNS double precision LANGUAGE c AS gbt_float8_distance;

CREATE FUNCTION public.int4range_custom () RETURNS int4range_custom LANGUAGE internal AS range_constructor3;

CREATE FUNCTION public.gbt_inet_same () RETURNS internal LANGUAGE c AS gbt_inet_same;

CREATE FUNCTION public.pgp_sym_decrypt_bytea () RETURNS bytea LANGUAGE c AS pgp_sym_decrypt_bytea;

CREATE FUNCTION public.uuid_nil () RETURNS uuid LANGUAGE c AS uuid_nil;

CREATE FUNCTION public.gbt_int2_same () RETURNS internal LANGUAGE c AS gbt_int2_same;

CREATE FUNCTION public.gbt_enum_penalty () RETURNS internal LANGUAGE c AS gbt_enum_penalty;

CREATE FUNCTION public.gbt_int4_compress () RETURNS internal LANGUAGE c AS gbt_int4_compress;

CREATE FUNCTION public.gbt_intv_decompress () RETURNS internal LANGUAGE c AS gbt_intv_decompress;

CREATE FUNCTION public.gbt_numeric_penalty () RETURNS internal LANGUAGE c AS gbt_numeric_penalty;

CREATE FUNCTION public.gbt_int2_picksplit () RETURNS internal LANGUAGE c AS gbt_int2_picksplit;

CREATE FUNCTION public.gbt_text_same () RETURNS internal LANGUAGE c AS gbt_text_same;

CREATE FUNCTION public.gbt_oid_penalty () RETURNS internal LANGUAGE c AS gbt_oid_penalty;

CREATE FUNCTION public.uuid_generate_v1mc () RETURNS uuid LANGUAGE c AS uuid_generate_v1mc;

CREATE FUNCTION public.pgp_sym_encrypt_bytea () RETURNS bytea LANGUAGE c AS pgp_sym_encrypt_bytea;

CREATE FUNCTION public.gbt_uuid_same () RETURNS internal LANGUAGE c AS gbt_uuid_same;

CREATE FUNCTION public.postgres_fdw_get_connections (IN OUT server_name, IN OUT valid) RETURNS SETOF record LANGUAGE c AS postgres_fdw_get_connections;

CREATE FUNCTION public.gbt_intv_distance () RETURNS double precision LANGUAGE c AS gbt_intv_distance;

CREATE FUNCTION public.int4multirange_custom (IN VARIADIC int4range_custom[]) RETURNS int4multirange_custom LANGUAGE internal AS multirange_constructor2;

CREATE FUNCTION public.int2_dist () RETURNS smallint LANGUAGE c AS int2_dist;

CREATE FUNCTION public.gbt_text_union () RETURNS gbtreekey_var LANGUAGE c AS gbt_text_union;

CREATE FUNCTION public.gbt_bit_picksplit () RETURNS internal LANGUAGE c AS gbt_bit_picksplit;

CREATE FUNCTION public.gbt_int4_fetch () RETURNS internal LANGUAGE c AS gbt_int4_fetch;

CREATE FUNCTION public.gbt_float8_picksplit () RETURNS internal LANGUAGE c AS gbt_float8_picksplit;

CREATE FUNCTION public.pgp_pub_encrypt () RETURNS bytea LANGUAGE c AS pgp_pub_encrypt_text;

CREATE FUNCTION public.gbt_cash_picksplit () RETURNS internal LANGUAGE c AS gbt_cash_picksplit;

CREATE FUNCTION public.gbt_int8_distance () RETURNS double precision LANGUAGE c AS gbt_int8_distance;

CREATE FUNCTION public.gbt_uuid_consistent () RETURNS boolean LANGUAGE c AS gbt_uuid_consistent;

CREATE FUNCTION public.gbt_text_compress () RETURNS internal LANGUAGE c AS gbt_text_compress;

CREATE FUNCTION public.gbt_macad8_union () RETURNS gbtreekey16 LANGUAGE c AS gbt_macad8_union;

CREATE FUNCTION public.int4_dist () RETURNS integer LANGUAGE c AS int4_dist;

CREATE FUNCTION public.gbt_cash_compress () RETURNS internal LANGUAGE c AS gbt_cash_compress;

CREATE FUNCTION public.crypt () RETURNS text LANGUAGE c AS pg_crypt;

CREATE FUNCTION public.gbt_var_fetch () RETURNS internal LANGUAGE c AS gbt_var_fetch;

CREATE FUNCTION public.postgres_fdw_validator () RETURNS void LANGUAGE c AS postgres_fdw_validator;

CREATE FUNCTION public.hmac () RETURNS bytea LANGUAGE c AS pg_hmac;

CREATE FUNCTION public.gbtreekey16_out () RETURNS cstring LANGUAGE c AS gbtreekey_out;

CREATE FUNCTION public.gbt_int8_compress () RETURNS internal LANGUAGE c AS gbt_int8_compress;

CREATE FUNCTION public.gbt_macad8_same () RETURNS internal LANGUAGE c AS gbt_macad8_same;

CREATE FUNCTION public.gbt_float4_same () RETURNS internal LANGUAGE c AS gbt_float4_same;

CREATE FUNCTION public.gbt_date_same () RETURNS internal LANGUAGE c AS gbt_date_same;

CREATE FUNCTION public.gbt_oid_same () RETURNS internal LANGUAGE c AS gbt_oid_same;

CREATE FUNCTION public.gbt_intv_penalty () RETURNS internal LANGUAGE c AS gbt_intv_penalty;

CREATE FUNCTION public.gbt_macad_union () RETURNS gbtreekey16 LANGUAGE c AS gbt_macad_union;

CREATE FUNCTION public.gbt_macad_fetch () RETURNS internal LANGUAGE c AS gbt_macad_fetch;

CREATE FUNCTION public.gbt_oid_compress () RETURNS internal LANGUAGE c AS gbt_oid_compress;

CREATE FUNCTION public.uuid_generate_v4 () RETURNS uuid LANGUAGE c AS uuid_generate_v4;

CREATE FUNCTION public.gbt_intv_picksplit () RETURNS internal LANGUAGE c AS gbt_intv_picksplit;

CREATE FUNCTION public.gbt_ts_same () RETURNS internal LANGUAGE c AS gbt_ts_same;

CREATE FUNCTION public.gbt_enum_picksplit () RETURNS internal LANGUAGE c AS gbt_enum_picksplit;

CREATE FUNCTION public.on_ddl_command () RETURNS event_trigger LANGUAGE plpgsql AS 
BEGIN
    RAISE NOTICE 'DDL Command: %', tg_tag;
END;
;

CREATE FUNCTION public.encrypt () RETURNS bytea LANGUAGE c AS pg_encrypt;

CREATE FUNCTION public.gbt_bool_picksplit () RETURNS internal LANGUAGE c AS gbt_bool_picksplit;

CREATE FUNCTION public.gen_salt () RETURNS text LANGUAGE c AS pg_gen_salt_rounds;

CREATE FUNCTION public.gbt_ts_consistent (IN timestamp without) RETURNS boolean LANGUAGE c AS gbt_ts_consistent;

CREATE FUNCTION public.gbt_intv_same () RETURNS internal LANGUAGE c AS gbt_intv_same;

CREATE FUNCTION public.gbt_date_consistent () RETURNS boolean LANGUAGE c AS gbt_date_consistent;

CREATE FUNCTION public.gbt_date_penalty () RETURNS internal LANGUAGE c AS gbt_date_penalty;

CREATE FUNCTION public.gbt_numeric_consistent () RETURNS boolean LANGUAGE c AS gbt_numeric_consistent;

CREATE FUNCTION public.gbt_ts_picksplit () RETURNS internal LANGUAGE c AS gbt_ts_picksplit;

CREATE FUNCTION public.gbt_timetz_consistent (IN time with) RETURNS boolean LANGUAGE c AS gbt_timetz_consistent;

CREATE FUNCTION public.gbt_bpchar_compress () RETURNS internal LANGUAGE c AS gbt_bpchar_compress;

CREATE FUNCTION public.gbt_numeric_union () RETURNS gbtreekey_var LANGUAGE c AS gbt_numeric_union;

CREATE FUNCTION public.gbt_time_picksplit () RETURNS internal LANGUAGE c AS gbt_time_picksplit;

CREATE FUNCTION public.decrypt_iv () RETURNS bytea LANGUAGE c AS pg_decrypt_iv;

CREATE FUNCTION public.gbt_date_union () RETURNS gbtreekey8 LANGUAGE c AS gbt_date_union;

CREATE FUNCTION public.gbt_bit_compress () RETURNS internal LANGUAGE c AS gbt_bit_compress;

CREATE FUNCTION public.gbt_numeric_same () RETURNS internal LANGUAGE c AS gbt_numeric_same;

CREATE FUNCTION public.gbt_inet_picksplit () RETURNS internal LANGUAGE c AS gbt_inet_picksplit;

CREATE FUNCTION public.log_insert () RETURNS trigger LANGUAGE plpgsql AS 
BEGIN
    RAISE NOTICE 'Inserted row with ID: %', NEW.id;
    RETURN NEW;
END;
;

CREATE FUNCTION public.oid_dist () RETURNS oid LANGUAGE c AS oid_dist;

CREATE FUNCTION public.gbt_bpchar_consistent () RETURNS boolean LANGUAGE c AS gbt_bpchar_consistent;

CREATE FUNCTION public.gbt_int2_consistent () RETURNS boolean LANGUAGE c AS gbt_int2_consistent;

CREATE FUNCTION public.gbt_cash_consistent () RETURNS boolean LANGUAGE c AS gbt_cash_consistent;

CREATE FUNCTION public.gbt_macad8_compress () RETURNS internal LANGUAGE c AS gbt_macad8_compress;

CREATE FUNCTION public.gbt_bytea_same () RETURNS internal LANGUAGE c AS gbt_bytea_same;

CREATE FUNCTION public.gbt_bool_penalty () RETURNS internal LANGUAGE c AS gbt_bool_penalty;

CREATE FUNCTION public.gbtreekey16_in () RETURNS gbtreekey16 LANGUAGE c AS gbtreekey_in;

CREATE FUNCTION public.time_dist (IN time without, IN time without) RETURNS interval LANGUAGE c AS time_dist;

CREATE FUNCTION public.gbt_macad_compress () RETURNS internal LANGUAGE c AS gbt_macad_compress;

CREATE FUNCTION public.gbt_inet_penalty () RETURNS internal LANGUAGE c AS gbt_inet_penalty;

CREATE FUNCTION public.gbtreekey8_out () RETURNS cstring LANGUAGE c AS gbtreekey_out;

CREATE FUNCTION public.gbt_uuid_penalty () RETURNS internal LANGUAGE c AS gbt_uuid_penalty;

CREATE FUNCTION public.uuid_generate_v5 (IN namespace uuid, IN name text) RETURNS uuid LANGUAGE c AS uuid_generate_v5;

CREATE FUNCTION public.gbt_uuid_picksplit () RETURNS internal LANGUAGE c AS gbt_uuid_picksplit;

CREATE FUNCTION public.gbt_int8_fetch () RETURNS internal LANGUAGE c AS gbt_int8_fetch;

CREATE FUNCTION public.armor () RETURNS text LANGUAGE c AS pg_armor;

CREATE FUNCTION public.float4_dist () RETURNS real LANGUAGE c AS float4_dist;

CREATE FUNCTION public.gbt_date_distance () RETURNS double precision LANGUAGE c AS gbt_date_distance;

CREATE FUNCTION public.gbt_bytea_penalty () RETURNS internal LANGUAGE c AS gbt_bytea_penalty;

CREATE FUNCTION public.gbt_ts_penalty () RETURNS internal LANGUAGE c AS gbt_ts_penalty;

CREATE FUNCTION public.gbt_cash_distance () RETURNS double precision LANGUAGE c AS gbt_cash_distance;

CREATE FUNCTION public.gbt_macad8_penalty () RETURNS internal LANGUAGE c AS gbt_macad8_penalty;

CREATE FUNCTION public.gbt_enum_compress () RETURNS internal LANGUAGE c AS gbt_enum_compress;

CREATE FUNCTION public.postgres_fdw_disconnect () RETURNS boolean LANGUAGE c AS postgres_fdw_disconnect;

CREATE FUNCTION public.digest () RETURNS bytea LANGUAGE c AS pg_digest;

CREATE FUNCTION public.gbt_bool_union () RETURNS gbtreekey2 LANGUAGE c AS gbt_bool_union;

CREATE FUNCTION public.gbt_float8_fetch () RETURNS internal LANGUAGE c AS gbt_float8_fetch;

CREATE FUNCTION public.gbt_int8_same () RETURNS internal LANGUAGE c AS gbt_int8_same;

CREATE FUNCTION public.gbt_time_union () RETURNS gbtreekey16 LANGUAGE c AS gbt_time_union;

CREATE FUNCTION public.gbt_intv_union () RETURNS gbtreekey32 LANGUAGE c AS gbt_intv_union;

CREATE FUNCTION public.uuid_ns_url () RETURNS uuid LANGUAGE c AS uuid_ns_url;

CREATE FUNCTION public.gbt_float8_penalty () RETURNS internal LANGUAGE c AS gbt_float8_penalty;

CREATE FUNCTION public.gbt_cash_fetch () RETURNS internal LANGUAGE c AS gbt_cash_fetch;

CREATE FUNCTION public.gbt_float4_distance () RETURNS double precision LANGUAGE c AS gbt_float4_distance;

CREATE FUNCTION public.gbt_int4_distance () RETURNS double precision LANGUAGE c AS gbt_int4_distance;

CREATE FUNCTION public.gbt_int4_consistent () RETURNS boolean LANGUAGE c AS gbt_int4_consistent;

CREATE FUNCTION public.pgp_sym_decrypt () RETURNS text LANGUAGE c AS pgp_sym_decrypt_text;

CREATE FUNCTION public.gbtreekey32_out () RETURNS cstring LANGUAGE c AS gbtreekey_out;

CREATE FUNCTION public.float8_dist (IN double precision, IN double precision) RETURNS double precision LANGUAGE c AS float8_dist;

CREATE FUNCTION public.gbt_intv_fetch () RETURNS internal LANGUAGE c AS gbt_intv_fetch;

CREATE FUNCTION public.gbt_uuid_union () RETURNS gbtreekey32 LANGUAGE c AS gbt_uuid_union;

CREATE FUNCTION public.gbt_bytea_union () RETURNS gbtreekey_var LANGUAGE c AS gbt_bytea_union;

CREATE FUNCTION public.gbt_inet_union () RETURNS gbtreekey16 LANGUAGE c AS gbt_inet_union;

CREATE FUNCTION public.encrypt_iv () RETURNS bytea LANGUAGE c AS pg_encrypt_iv;

CREATE FUNCTION public.gbt_bool_same () RETURNS internal LANGUAGE c AS gbt_bool_same;

CREATE FUNCTION public.gbt_oid_consistent () RETURNS boolean LANGUAGE c AS gbt_oid_consistent;

CREATE FUNCTION public.gbt_int2_fetch () RETURNS internal LANGUAGE c AS gbt_int2_fetch;

CREATE FUNCTION public.gbt_ts_compress () RETURNS internal LANGUAGE c AS gbt_ts_compress;

CREATE FUNCTION public.gbt_float4_union () RETURNS gbtreekey8 LANGUAGE c AS gbt_float4_union;

CREATE FUNCTION public.gbt_tstz_distance (IN timestamp with) RETURNS double precision LANGUAGE c AS gbt_tstz_distance;

CREATE FUNCTION public.gbt_oid_fetch () RETURNS internal LANGUAGE c AS gbt_oid_fetch;

CREATE FUNCTION public.date_dist () RETURNS integer LANGUAGE c AS date_dist;

CREATE FUNCTION public.gbt_date_compress () RETURNS internal LANGUAGE c AS gbt_date_compress;

CREATE FUNCTION public.gbt_date_picksplit () RETURNS internal LANGUAGE c AS gbt_date_picksplit;

CREATE FUNCTION public.gbtreekey2_out () RETURNS cstring LANGUAGE c AS gbtreekey_out;

CREATE FUNCTION public.get_department_budget (IN dept_id uuid) RETURNS numeric LANGUAGE sql AS 
    SELECT COALESCE(SUM(salary), 0)
    FROM employees
    WHERE department_id = dept_id;
;

CREATE FUNCTION public.gbtreekey_var_out () RETURNS cstring LANGUAGE c AS gbtreekey_out;

CREATE FUNCTION public.gbt_enum_same () RETURNS internal LANGUAGE c AS gbt_enum_same;

CREATE FUNCTION public.gbt_ts_union () RETURNS gbtreekey16 LANGUAGE c AS gbt_ts_union;

CREATE FUNCTION public.gbtreekey_var_in () RETURNS gbtreekey_var LANGUAGE c AS gbtreekey_in;

CREATE FUNCTION public.gbt_float4_penalty () RETURNS internal LANGUAGE c AS gbt_float4_penalty;

CREATE FUNCTION public.gbt_numeric_picksplit () RETURNS internal LANGUAGE c AS gbt_numeric_picksplit;

CREATE FUNCTION public.gbt_oid_union () RETURNS gbtreekey8 LANGUAGE c AS gbt_oid_union;

CREATE FUNCTION public.gbt_bytea_picksplit () RETURNS internal LANGUAGE c AS gbt_bytea_picksplit;

CREATE FUNCTION public.gbt_bit_union () RETURNS gbtreekey_var LANGUAGE c AS gbt_bit_union;

CREATE FUNCTION public.gbt_int4_union () RETURNS gbtreekey8 LANGUAGE c AS gbt_int4_union;

CREATE FUNCTION public.gbt_macad_consistent () RETURNS boolean LANGUAGE c AS gbt_macad_consistent;

CREATE FUNCTION public.gbt_macad8_fetch () RETURNS internal LANGUAGE c AS gbt_macad8_fetch;

CREATE FUNCTION public.gbt_bit_same () RETURNS internal LANGUAGE c AS gbt_bit_same;

CREATE FUNCTION public.gbt_int8_consistent () RETURNS boolean LANGUAGE c AS gbt_int8_consistent;

CREATE FUNCTION public.gbt_enum_consistent () RETURNS boolean LANGUAGE c AS gbt_enum_consistent;

CREATE FUNCTION public.gbt_oid_distance () RETURNS double precision LANGUAGE c AS gbt_oid_distance;

CREATE FUNCTION public.gbt_float4_consistent () RETURNS boolean LANGUAGE c AS gbt_float4_consistent;

CREATE FUNCTION public.postgres_fdw_handler () RETURNS fdw_handler LANGUAGE c AS postgres_fdw_handler;

CREATE FUNCTION public.pgp_sym_encrypt () RETURNS bytea LANGUAGE c AS pgp_sym_encrypt_text;

CREATE FUNCTION public.gbt_timetz_compress () RETURNS internal LANGUAGE c AS gbt_timetz_compress;

CREATE FUNCTION public.uuid_generate_v3 (IN namespace uuid, IN name text) RETURNS uuid LANGUAGE c AS uuid_generate_v3;

CREATE FUNCTION public.gbt_tstz_consistent (IN timestamp with) RETURNS boolean LANGUAGE c AS gbt_tstz_consistent;

CREATE FUNCTION public.gbt_float8_union () RETURNS gbtreekey16 LANGUAGE c AS gbt_float8_union;

CREATE FUNCTION public.gbt_oid_picksplit () RETURNS internal LANGUAGE c AS gbt_oid_picksplit;

CREATE FUNCTION public.gbt_intv_compress () RETURNS internal LANGUAGE c AS gbt_intv_compress;

CREATE FUNCTION public.gbtreekey2_in () RETURNS gbtreekey2 LANGUAGE c AS gbtreekey_in;

CREATE FUNCTION public.gbt_macad_same () RETURNS internal LANGUAGE c AS gbt_macad_same;

CREATE FUNCTION public.gbt_cash_penalty () RETURNS internal LANGUAGE c AS gbt_cash_penalty;

CREATE FUNCTION public.gbt_uuid_compress () RETURNS internal LANGUAGE c AS gbt_uuid_compress;

CREATE FUNCTION public.gbt_time_consistent (IN time without) RETURNS boolean LANGUAGE c AS gbt_time_consistent;

CREATE FUNCTION public.gbt_bytea_compress () RETURNS internal LANGUAGE c AS gbt_bytea_compress;

CREATE FUNCTION public.uuid_ns_x500 () RETURNS uuid LANGUAGE c AS uuid_ns_x500;

CREATE FUNCTION public.gbtreekey32_in () RETURNS gbtreekey32 LANGUAGE c AS gbtreekey_in;

CREATE FUNCTION public.gbt_float4_compress () RETURNS internal LANGUAGE c AS gbt_float4_compress;

CREATE FUNCTION public.gbt_float8_consistent (IN double precision) RETURNS boolean LANGUAGE c AS gbt_float8_consistent;

CREATE FUNCTION public.gen_random_bytes () RETURNS bytea LANGUAGE c AS pg_random_bytes;

CREATE FUNCTION public.gbt_time_fetch () RETURNS internal LANGUAGE c AS gbt_time_fetch;

CREATE FUNCTION public.gbt_macad8_consistent () RETURNS boolean LANGUAGE c AS gbt_macad8_consistent;

CREATE FUNCTION public.gbt_bit_consistent () RETURNS boolean LANGUAGE c AS gbt_bit_consistent;

CREATE FUNCTION public.gbt_text_consistent () RETURNS boolean LANGUAGE c AS gbt_text_consistent;

CREATE FUNCTION public.gbt_enum_fetch () RETURNS internal LANGUAGE c AS gbt_enum_fetch;

CREATE FUNCTION public.gbt_float4_picksplit () RETURNS internal LANGUAGE c AS gbt_float4_picksplit;

CREATE FUNCTION public.gbt_time_distance (IN time without) RETURNS double precision LANGUAGE c AS gbt_time_distance;

CREATE FUNCTION public.gbt_bytea_consistent () RETURNS boolean LANGUAGE c AS gbt_bytea_consistent;

CREATE FUNCTION public.gbt_bool_consistent () RETURNS boolean LANGUAGE c AS gbt_bool_consistent;

CREATE FUNCTION public.gbt_float8_same () RETURNS internal LANGUAGE c AS gbt_float8_same;

CREATE FUNCTION public.validate_department_budget () RETURNS trigger LANGUAGE plpgsql AS 
BEGIN
    IF (SELECT SUM(salary) FROM employees WHERE department_id = NEW.department_id) > 100000 THEN
        RAISE EXCEPTION 'Department budget exceeded';
    END IF;
    RETURN NEW;
END;
;

CREATE FUNCTION public.ts_dist (IN timestamp without, IN timestamp without) RETURNS interval LANGUAGE c AS ts_dist;

CREATE FUNCTION public.interval_dist () RETURNS interval LANGUAGE c AS interval_dist;

CREATE FUNCTION public.gbt_int2_compress () RETURNS internal LANGUAGE c AS gbt_int2_compress;

CREATE FUNCTION public.gbt_ts_fetch () RETURNS internal LANGUAGE c AS gbt_ts_fetch;

CREATE FUNCTION public.gbt_macad_picksplit () RETURNS internal LANGUAGE c AS gbt_macad_picksplit;

CREATE FUNCTION public.pgp_key_id () RETURNS text LANGUAGE c AS pgp_key_id_w;

CREATE FUNCTION public.gbt_enum_union () RETURNS gbtreekey8 LANGUAGE c AS gbt_enum_union;

CREATE FUNCTION public.gbtreekey4_out () RETURNS cstring LANGUAGE c AS gbtreekey_out;

CREATE FUNCTION public.pgp_pub_decrypt () RETURNS text LANGUAGE c AS pgp_pub_decrypt_text;

CREATE FUNCTION public.gbt_cash_union () RETURNS gbtreekey16 LANGUAGE c AS gbt_cash_union;

CREATE FUNCTION public.gbt_int2_union () RETURNS gbtreekey4 LANGUAGE c AS gbt_int2_union;

CREATE FUNCTION public.gbt_int8_penalty () RETURNS internal LANGUAGE c AS gbt_int8_penalty;

CREATE FUNCTION public.gbt_float8_compress () RETURNS internal LANGUAGE c AS gbt_float8_compress;

CREATE FUNCTION public.gbt_macad8_picksplit () RETURNS internal LANGUAGE c AS gbt_macad8_picksplit;

CREATE FUNCTION public.postgres_fdw_disconnect_all () RETURNS boolean LANGUAGE c AS postgres_fdw_disconnect_all;

CREATE FUNCTION public.gbt_int2_distance () RETURNS double precision LANGUAGE c AS gbt_int2_distance;

CREATE FUNCTION public.gbt_int4_same () RETURNS internal LANGUAGE c AS gbt_int4_same;

CREATE FUNCTION public.gen_random_uuid () RETURNS uuid LANGUAGE c AS pg_random_uuid;

CREATE FUNCTION public.gbt_uuid_fetch () RETURNS internal LANGUAGE c AS gbt_uuid_fetch;

CREATE FUNCTION public.gbt_time_penalty () RETURNS internal LANGUAGE c AS gbt_time_penalty;

CREATE PROCEDURE public.increase_salary_all (IN IN pct) LANGUAGE plpgsql AS 
BEGIN
    UPDATE employees SET salary = salary + (salary * pct);
END;
;

CREATE EVENT TRIGGER ddl_logger ON ddl_command_start EXECUTE FUNCTION on_ddl_command ENABLE;

CREATE POLICY only_admins ON employees AS PERMISSIVE FOR 0 USING ((role = 'admin'::user_role));

CREATE COLLATION public.german_ci (PROVIDER = 'i' );

CREATE SERVER foreign_pg_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname 'other_db', port '5432', host 'localhost');

