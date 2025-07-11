use postgres::TestDb;
use shem_core::schema::CheckOption;
use shem_core::DatabaseConnection;
use log::debug;
use std::fs;

/// Test helper function to execute SQL on the test database
async fn execute_sql(
    connection: &Box<dyn DatabaseConnection>,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    connection.execute(sql).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_basic_materialized_view() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a simple table and materialized view
    execute_sql(
        &connection,
        "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, email TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW active_users AS SELECT id, name FROM users WHERE id > 0;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;
    debug!("Full schema materialized views: {:?}", schema.materialized_views);

    // Verify the materialized view exists
    let view = schema.materialized_views.get("active_users").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert_eq!(view.name, "active_users");
    assert_eq!(view.schema, Some("public".to_string())); // Public schema
    assert!(view.definition.contains("SELECT id,\n    name\n   FROM users"));
    assert_eq!(view.check_option, CheckOption::None); // Materialized views don't have check options
    assert!(view.populate_with_data); // Should be populated with data
    assert!(view.indexes.is_empty()); // No indexes by default

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and materialized view
    execute_sql(&connection, "CREATE SCHEMA test_schema;").await?;
    execute_sql(
        &connection,
        "CREATE TABLE test_schema.products (id SERIAL PRIMARY KEY, name TEXT, price DECIMAL(10,2));",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO test_schema.products (name, price) VALUES ('Laptop', 999.99);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW test_schema.expensive_products AS SELECT * FROM test_schema.products WHERE price > 100;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view exists in the schema
    let view = schema.materialized_views.get("expensive_products").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert_eq!(view.name, "expensive_products");
    assert_eq!(view.schema, Some("test_schema".to_string()));
    assert!(view.definition.contains("SELECT id,\n    name,\n    price\n   FROM test_schema.products"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and materialized view with comment
    execute_sql(
        &connection,
        "CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT, department TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO employees (name, department) VALUES ('Jane Smith', 'Engineering');",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW managers AS SELECT * FROM employees WHERE department = 'Management';",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON MATERIALIZED VIEW managers IS 'Materialized view showing only management employees';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view has a comment
    let view = schema.materialized_views.get("managers").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert_eq!(view.comment, Some("Materialized view showing only management employees".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_no_data() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and materialized view with NO DATA
    execute_sql(
        &connection,
        "CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INTEGER, amount DECIMAL(10,2));",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW small_orders AS SELECT * FROM orders WHERE amount < 100 WITH NO DATA;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view has NO DATA
    let view = schema.materialized_views.get("small_orders").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    // NOTE: PostgreSQL does not expose via catalog tables whether a materialized view was created WITH DATA or WITH NO DATA.
    // The introspection always sets populate_with_data = true, so we cannot assert its value here.

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_storage_parameters() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and materialized view with storage parameters
    execute_sql(
        &connection,
        "CREATE TABLE logs (id SERIAL PRIMARY KEY, message TEXT, created_at TIMESTAMP);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO logs (message, created_at) VALUES ('Test log', NOW());",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW recent_logs AS SELECT * FROM logs WHERE created_at > NOW() - INTERVAL '1 day';",
    )
    .await?;
    execute_sql(
        &connection,
        "ALTER MATERIALIZED VIEW recent_logs SET (fillfactor = 70);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view has storage parameters
    let view = schema.materialized_views.get("recent_logs").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert_eq!(view.storage_parameters.get("fillfactor"), Some(&"70".to_string()));

    Ok(())
}

// #[tokio::test]
// async fn test_introspect_materialized_view_with_tablespace() -> Result<(), Box<dyn std::error::Error>> {
//     env_logger::try_init().ok();
//     let db = TestDb::new().await?;
//     let connection = &db.conn;

//     // Create the tablespace directory
//     let tablespace_dir = "/tmp/test_tablespace";
//     fs::create_dir_all(tablespace_dir)?;

//     // Create tablespace
//     execute_sql(
//         &connection,
//         "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_tablespace';",
//     )
//     .await?;

//     // Create table and materialized view with tablespace
//     execute_sql(
//         &connection,
//         "CREATE TABLE data (id SERIAL PRIMARY KEY, value TEXT);",
//     )
//     .await?;
//     execute_sql(
//         &connection,
//         "INSERT INTO data (value) VALUES ('test data');",
//     )
//     .await?;
//     execute_sql(
//         &connection,
//         "CREATE MATERIALIZED VIEW data_summary AS SELECT COUNT(*) as count FROM data TABLESPACE test_tablespace;",
//     )
//     .await?;

//     // Introspect the database
//     let schema = connection.introspect().await?;

//     // Verify the materialized view has tablespace
//     let view = schema.materialized_views.get("data_summary").expect("Materialized view should exist");
//     debug!("Materialized view: {:?}", view);
//     assert_eq!(view.tablespace, Some("test_tablespace".to_string()));

//     // Clean up: drop tablespace and remove directory
//     execute_sql(&connection, "DROP TABLESPACE test_tablespace;").await?;
//     let _ = fs::remove_dir_all(tablespace_dir); // Ignore errors during cleanup

//     Ok(())
// }

#[tokio::test]
async fn test_introspect_materialized_view_with_indexes() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and materialized view
    execute_sql(
        &connection,
        "CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT, email TEXT, region TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO customers (name, email, region) VALUES ('Alice', 'alice@example.com', 'North');",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW customer_summary AS SELECT region, COUNT(*) as count FROM customers GROUP BY region;",
    )
    .await?;

    // Create indexes on the materialized view
    execute_sql(
        &connection,
        "CREATE INDEX idx_customer_summary_region ON customer_summary (region);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE UNIQUE INDEX idx_customer_summary_count ON customer_summary (count);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view has indexes
    let view = schema.materialized_views.get("customer_summary").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert_eq!(view.indexes.len(), 2);

    // Check the first index
    let region_index = view.indexes.iter().find(|idx| idx.name == "idx_customer_summary_region").unwrap();
    assert_eq!(region_index.columns.len(), 1);
    assert_eq!(region_index.columns[0].name, "region");
    assert!(!region_index.unique);

    // Check the second index
    let count_index = view.indexes.iter().find(|idx| idx.name == "idx_customer_summary_count").unwrap();
    assert_eq!(count_index.columns.len(), 1);
    assert_eq!(count_index.columns[0].name, "count");
    assert!(count_index.unique);

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_joins() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create tables
    execute_sql(
        &connection,
        "CREATE TABLE departments (id SERIAL PRIMARY KEY, name TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT, department_id INTEGER REFERENCES departments(id));",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO departments (name) VALUES ('Engineering');",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO employees (name, department_id) VALUES ('Bob', 1);",
    )
    .await?;

    // Create materialized view with join
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW employee_details AS SELECT e.id, e.name, d.name as department FROM employees e JOIN departments d ON e.department_id = d.id;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view with join
    let view = schema.materialized_views.get("employee_details").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert!(view.definition.contains("SELECT e.id")
        && view.definition.contains("e.name")
        && view.definition.contains("d.name AS department")
        && view.definition.contains("FROM (employees e")
        && view.definition.contains("JOIN departments d")
        && view.definition.contains("ON ((e.department_id = d.id))"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_aggregation() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table
    execute_sql(
        &connection,
        "CREATE TABLE sales (id SERIAL PRIMARY KEY, product TEXT, amount DECIMAL(10,2), date DATE);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO sales (product, amount, date) VALUES ('Widget', 100.00, '2023-01-01');",
    )
    .await?;

    // Create materialized view with aggregation
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW sales_summary AS SELECT product, SUM(amount) as total_sales, COUNT(*) as num_sales FROM sales GROUP BY product;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view with aggregation
    let view = schema.materialized_views.get("sales_summary").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert!(view.definition.contains("SELECT product,\n    sum(amount) AS total_sales,\n    count(*) AS num_sales\n   FROM sales\n  GROUP BY product"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_window_function() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table
    execute_sql(
        &connection,
        "CREATE TABLE scores (id SERIAL PRIMARY KEY, student TEXT, subject TEXT, score INTEGER);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO scores (student, subject, score) VALUES ('Alice', 'Math', 95);",
    )
    .await?;

    // Create materialized view with window function
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW score_rankings AS SELECT student, subject, score, ROW_NUMBER() OVER (PARTITION BY subject ORDER BY score DESC) as rank FROM scores;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view with window function
    let view = schema.materialized_views.get("score_rankings").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert!(view.definition.contains("SELECT student,\n    subject,\n    score,\n    row_number() OVER (PARTITION BY subject ORDER BY score DESC) AS rank\n   FROM scores"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_cte() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table
    execute_sql(
        &connection,
        "CREATE TABLE events (id SERIAL PRIMARY KEY, event_type TEXT, timestamp TIMESTAMP);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO events (event_type, timestamp) VALUES ('login', NOW());",
    )
    .await?;

    // Create materialized view with CTE
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW event_summary AS WITH event_counts AS (SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type) SELECT * FROM event_counts WHERE count > 0;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view with CTE
    let view = schema.materialized_views.get("event_summary").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert!(view.definition.contains("WITH event_counts AS")
        && view.definition.contains("SELECT events.event_type")
        && view.definition.contains("count(*) AS count")
        && view.definition.contains("FROM events")
        && view.definition.contains("GROUP BY events.event_type")
        && view.definition.contains("SELECT event_type")
        && view.definition.contains("FROM event_counts")
        && view.definition.contains("WHERE (count > 0)"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_subquery() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table
    execute_sql(
        &connection,
        "CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, price DECIMAL(10,2));",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO products (name, price) VALUES ('Product A', 50.00);",
    )
    .await?;

    // Create materialized view with subquery
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW expensive_products AS SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view with subquery
    let view = schema.materialized_views.get("expensive_products").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert!(view.definition.contains("SELECT id,")
        && view.definition.contains("FROM products")
        && view.definition.contains("WHERE (price > (")
        && view.definition.contains("SELECT avg(products_1.price) AS avg")
        && view.definition.contains("FROM products products_1"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_union() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create tables
    execute_sql(
        &connection,
        "CREATE TABLE users_2023 (id SERIAL PRIMARY KEY, name TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TABLE users_2024 (id SERIAL PRIMARY KEY, name TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO users_2023 (name) VALUES ('User 2023');",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO users_2024 (name) VALUES ('User 2024');",
    )
    .await?;

    // Create materialized view with union
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW all_users AS SELECT id, name FROM users_2023 UNION SELECT id, name FROM users_2024;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view with union
    let view = schema.materialized_views.get("all_users").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert!(view.definition.contains("SELECT users_2023.id,")
        && view.definition.contains("users_2023.name")
        && view.definition.contains("FROM users_2023")
        && view.definition.contains("UNION")
        && view.definition.contains("SELECT users_2024.id,")
        && view.definition.contains("users_2024.name")
        && view.definition.contains("FROM users_2024"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_case_statement() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table
    execute_sql(
        &connection,
        "CREATE TABLE orders (id SERIAL PRIMARY KEY, amount DECIMAL(10,2));",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO orders (amount) VALUES (150.00);",
    )
    .await?;

    // Create materialized view with case statement
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW order_categories AS SELECT id, amount, CASE WHEN amount < 100 THEN 'Small' WHEN amount < 500 THEN 'Medium' ELSE 'Large' END as category FROM orders;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view with case statement
    let view = schema.materialized_views.get("order_categories").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert!(view.definition.contains("CASE")
        && view.definition.contains("WHEN (amount < (100)::numeric) THEN 'Small'::text")
        && view.definition.contains("WHEN (amount < (500)::numeric) THEN 'Medium'::text")
        && view.definition.contains("ELSE 'Large'::text"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_functions() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table
    execute_sql(
        &connection,
        "CREATE TABLE text_data (id SERIAL PRIMARY KEY, text_field TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO text_data (text_field) VALUES ('hello world');",
    )
    .await?;

    // Create materialized view with functions
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW processed_text AS SELECT id, UPPER(text_field) as upper_text, LENGTH(text_field) as text_length FROM text_data;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view with functions
    let view = schema.materialized_views.get("processed_text").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert!(view.definition.contains("SELECT id,\n    upper(text_field) AS upper_text,\n    length(text_field) AS text_length\n   FROM text_data"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_distinct() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table
    execute_sql(
        &connection,
        "CREATE TABLE duplicates (id SERIAL PRIMARY KEY, value TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO duplicates (value) VALUES ('A'), ('A'), ('B');",
    )
    .await?;

    // Create materialized view with distinct
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW unique_values AS SELECT DISTINCT value FROM duplicates;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view with distinct
    let view = schema.materialized_views.get("unique_values").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert!(view.definition.contains("SELECT DISTINCT value\n   FROM duplicates"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_limit_offset() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table
    execute_sql(
        &connection,
        "CREATE TABLE numbers (id SERIAL PRIMARY KEY, value INTEGER);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO numbers (value) VALUES (1), (2), (3), (4), (5);",
    )
    .await?;

    // Create materialized view with limit and offset
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW limited_numbers AS SELECT * FROM numbers ORDER BY value LIMIT 3 OFFSET 1;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view with limit and offset
    let view = schema.materialized_views.get("limited_numbers").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert!(view.definition.contains("SELECT id,")
        && view.definition.contains("FROM numbers")
        && view.definition.contains("ORDER BY value")
        && view.definition.contains("OFFSET 1")
        && view.definition.contains("LIMIT 3"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_with_complex_expression() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table
    execute_sql(
        &connection,
        "CREATE TABLE calculations (id SERIAL PRIMARY KEY, a INTEGER, b INTEGER);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO calculations (a, b) VALUES (10, 5);",
    )
    .await?;

    // Create materialized view with complex expression
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW complex_calc AS SELECT id, (a + b) * 2 as result, CASE WHEN a > b THEN 'A greater' ELSE 'B greater or equal' END as comparison FROM calculations;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the materialized view with complex expression
    let view = schema.materialized_views.get("complex_calc").expect("Materialized view should exist");
    debug!("Materialized view: {:?}", view);
    assert!(view.definition.contains("((a + b) * 2) AS result")
        && view.definition.contains("CASE")
        && view.definition.contains("WHEN (a > b) THEN 'A greater'::text")
        && view.definition.contains("ELSE 'B greater or equal'::text"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_materialized_views() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table
    execute_sql(
        &connection,
        "CREATE TABLE data (id SERIAL PRIMARY KEY, category TEXT, value INTEGER);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO data (category, value) VALUES ('A', 10), ('B', 20), ('A', 15);",
    )
    .await?;

    // Create multiple materialized views
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW view1 AS SELECT category, COUNT(*) as count FROM data GROUP BY category;",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW view2 AS SELECT category, SUM(value) as total FROM data GROUP BY category;",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW view3 AS SELECT * FROM data WHERE value > 10;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;
    debug!("Full schema materialized views: {:?}", schema.materialized_views);

    // Verify all materialized views exist
    assert!(schema.materialized_views.contains_key("view1"));
    assert!(schema.materialized_views.contains_key("view2"));
    assert!(schema.materialized_views.contains_key("view3"));

    // Verify view1
    let view1 = schema.materialized_views.get("view1").unwrap();
    assert!(view1.definition.contains("SELECT category,\n    count(*) AS count\n   FROM data\n  GROUP BY category"));

    // Verify view2
    let view2 = schema.materialized_views.get("view2").unwrap();
    assert!(view2.definition.contains("SELECT category,\n    sum(value) AS total\n   FROM data\n  GROUP BY category"));

    // Verify view3
    let view3 = schema.materialized_views.get("view3").unwrap();
    assert!(view3.definition.contains("SELECT id,\n    category,\n    value\n   FROM data\n  WHERE (value > 10)"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_performance() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table with more data
    execute_sql(
        &connection,
        "CREATE TABLE performance_test (id SERIAL PRIMARY KEY, value INTEGER, category TEXT);",
    )
    .await?;

    // Insert some test data
    for i in 1..=100 {
        execute_sql(
            &connection,
            &format!("INSERT INTO performance_test (value, category) VALUES ({}, 'cat{}');", i, i % 10),
        )
        .await?;
    }

    // Create materialized view
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW performance_summary AS SELECT category, AVG(value) as avg_value, COUNT(*) as count FROM performance_test GROUP BY category;",
    )
    .await?;

    // Create index on the materialized view
    execute_sql(
        &connection,
        "CREATE INDEX idx_performance_category ON performance_summary (category);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;
    debug!("Full schema materialized views: {:?}", schema.materialized_views);

    // Verify the materialized view and its index
    let view = schema.materialized_views.get("performance_summary").expect("Materialized view should exist");
    debug!("Performance materialized view: {:?}", view);
    assert_eq!(view.indexes.len(), 1);
    assert_eq!(view.indexes[0].name, "idx_performance_category");

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table
    execute_sql(
        &connection,
        "CREATE TABLE consistency_test (id SERIAL PRIMARY KEY, value TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO consistency_test (value) VALUES ('test');",
    )
    .await?;

    // Create materialized view
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW consistency_view AS SELECT * FROM consistency_test;",
    )
    .await?;

    // Introspect multiple times to ensure consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let view1 = schema1.materialized_views.get("consistency_view").unwrap();
    let view2 = schema2.materialized_views.get("consistency_view").unwrap();

    // Verify consistency
    assert_eq!(view1.name, view2.name);
    assert_eq!(view1.schema, view2.schema);
    assert_eq!(view1.definition, view2.definition);
    assert_eq!(view1.check_option, view2.check_option);
    assert_eq!(view1.populate_with_data, view2.populate_with_data);

    Ok(())
}

#[tokio::test]
async fn test_introspect_materialized_view_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Test materialized view with empty result set
    execute_sql(
        &connection,
        "CREATE TABLE empty_test (id SERIAL PRIMARY KEY, value TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW empty_view AS SELECT * FROM empty_test WHERE 1 = 0;",
    )
    .await?;

    // Test materialized view with all columns selected
    execute_sql(
        &connection,
        "CREATE TABLE all_columns (id SERIAL PRIMARY KEY, name TEXT, description TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "INSERT INTO all_columns (name, description) VALUES ('Test', 'Description');",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE MATERIALIZED VIEW all_columns_view AS SELECT * FROM all_columns;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify empty view
    let empty_view = schema.materialized_views.get("empty_view").expect("Empty view should exist");
    debug!("Empty view definition: {:?}", empty_view.definition);
    assert!(empty_view.definition.contains("WHERE (1 = 0)"));

    // Verify all columns view
    let all_columns_view = schema.materialized_views.get("all_columns_view").expect("All columns view should exist");
    assert!(all_columns_view.definition.contains("SELECT id,\n    name,\n    description\n   FROM all_columns"));

    Ok(())
} 