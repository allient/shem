use postgres::TestDb;
use shem_core::schema::CheckOption;
use shem_core::DatabaseConnection;
use tracing::debug;

/// Test helper function to execute SQL on the test database
async fn execute_sql(
    connection: &Box<dyn DatabaseConnection>,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    connection.execute(sql).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_basic_view() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a simple table and view
    execute_sql(
        &connection,
        "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, email TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW active_users AS SELECT id, name FROM users WHERE id > 0;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view exists
    let view = schema.views.get("active_users").expect("View should exist");
    debug!("View: {:?}", view);
    assert_eq!(view.name, "active_users");
    assert_eq!(view.schema, Some("public".to_string())); // Public schema
    assert!(view.definition.contains("SELECT id,\n    name\n   FROM users"));
    assert_eq!(view.check_option, CheckOption::None);
    assert!(!view.security_barrier);
    assert_eq!(view.columns, vec!["id", "name"]);

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and view
    execute_sql(&connection, "CREATE SCHEMA test_schema;").await?;
    execute_sql(
        &connection,
        "CREATE TABLE test_schema.products (id SERIAL PRIMARY KEY, name TEXT, price DECIMAL(10,2));",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW test_schema.expensive_products AS SELECT * FROM test_schema.products WHERE price > 100;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view exists in the schema
    let view = schema.views.get("expensive_products").expect("View should exist");
    debug!("View: {:?}", view);
    assert_eq!(view.name, "expensive_products");
    assert_eq!(view.schema, Some("test_schema".to_string()));
    assert!(view.definition.contains("SELECT id,\n    name,\n    price\n   FROM test_schema.products"));
    assert_eq!(view.columns, vec!["id", "name", "price"]);

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with comment
    execute_sql(
        &connection,
        "CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT, department TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW managers AS SELECT * FROM employees WHERE department = 'Management';",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON VIEW managers IS 'View showing only management employees';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view has a comment
    let view = schema.views.get("managers").expect("View should exist");
    debug!("View: {:?}", view);
    assert_eq!(view.comment, Some("View showing only management employees".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_check_option() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with check option
    execute_sql(
        &connection,
        "CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INTEGER, amount DECIMAL(10,2));",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW small_orders AS SELECT * FROM orders WHERE amount < 100 WITH CHECK OPTION;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view has check option
    let view = schema.views.get("small_orders").expect("View should exist");
    debug!("View: {:?}", view);
    assert_eq!(view.check_option, CheckOption::Cascaded);

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_cascaded_check_option() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with cascaded check option
    execute_sql(
        &connection,
        "CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, category TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW electronics AS SELECT * FROM products WHERE category = 'Electronics' WITH CASCADED CHECK OPTION;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view has cascaded check option
    let view = schema.views.get("electronics").expect("View should exist");
    debug!("View: {:?}", view);
    assert_eq!(view.check_option, CheckOption::Cascaded);

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_security_barrier() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with security barrier
    execute_sql(
        &connection,
        "CREATE TABLE sensitive_data (id SERIAL PRIMARY KEY, data TEXT, user_id INTEGER);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW user_data AS SELECT * FROM sensitive_data;",
    )
    .await?;
    execute_sql(
        &connection,
        "ALTER VIEW user_data SET (security_barrier = true);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view has security barrier
    let view = schema.views.get("user_data").expect("View should exist");
    debug!("View: {:?}", view);
    assert!(view.security_barrier);

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_column_aliases() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with column aliases
    execute_sql(
        &connection,
        "CREATE TABLE customers (id SERIAL PRIMARY KEY, first_name TEXT, last_name TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW customer_names AS SELECT id, first_name AS fname, last_name AS lname FROM customers;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view has correct column names (aliases)
    let view = schema.views.get("customer_names").expect("View should exist");
    assert_eq!(view.columns, vec!["id", "fname", "lname"]);
    assert!(view.definition.contains("first_name AS fname"));
    assert!(view.definition.contains("last_name AS lname"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_joins() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create tables and view with joins
    execute_sql(
        &connection,
        "CREATE TABLE departments (id SERIAL PRIMARY KEY, name TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id));",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW employee_departments AS SELECT e.id, e.name, d.name as dept_name FROM employees e JOIN departments d ON e.dept_id = d.id;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view with joins
    let view = schema.views.get("employee_departments").expect("View should exist");
    debug!("View: {:?}", view);
    assert_eq!(view.columns, vec!["id", "name", "dept_name"]);
    assert!(view.definition.contains("JOIN departments d ON ((e.dept_id = d.id))"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_aggregation() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with aggregation
    execute_sql(
        &connection,
        "CREATE TABLE sales (id SERIAL PRIMARY KEY, product_id INTEGER, amount DECIMAL(10,2), date DATE);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW daily_sales AS SELECT date, COUNT(*) as sales_count, SUM(amount) as total_amount FROM sales GROUP BY date;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view with aggregation
    let view = schema.views.get("daily_sales").expect("View should exist");
    debug!("View: {:?}", view);
    assert_eq!(view.columns, vec!["date", "sales_count", "total_amount"]);
    assert!(view.definition.contains("GROUP BY date"));
    assert!(view.definition.contains("count(*) AS sales_count"));
    assert!(view.definition.contains("sum(amount) AS total_amount"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_window_function() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with window function
    execute_sql(
        &connection,
        "CREATE TABLE scores (id SERIAL PRIMARY KEY, student TEXT, subject TEXT, score INTEGER);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW student_rankings AS SELECT student, subject, score, ROW_NUMBER() OVER (PARTITION BY subject ORDER BY score DESC) as rank FROM scores;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view with window function
    let view = schema.views.get("student_rankings").expect("View should exist");
    debug!("View: {:?}", view);
    assert_eq!(view.columns, vec!["student", "subject", "score", "rank"]);
    assert!(view.definition.contains("row_number() OVER"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_cte() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with CTE
    execute_sql(
        &connection,
        "CREATE TABLE events (id SERIAL PRIMARY KEY, name TEXT, date DATE, attendees INTEGER);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW popular_events AS WITH event_stats AS (SELECT name, AVG(attendees) as avg_attendees FROM events GROUP BY name) SELECT name, avg_attendees FROM event_stats WHERE avg_attendees > 50;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view with CTE
    let view = schema.views.get("popular_events").expect("View should exist");
    assert_eq!(view.columns, vec!["name", "avg_attendees"]);
    assert!(view.definition.contains("WITH event_stats AS"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_subquery() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with subquery
    execute_sql(
        &connection,
        "CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, price DECIMAL(10,2), category TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW expensive_products AS SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view with subquery
    let view = schema.views.get("expensive_products").expect("View should exist");
    debug!("View: {:?}", view);
    assert_eq!(view.columns, vec!["id", "name", "price", "category"]);
    assert!(view.definition.contains("SELECT avg(products_1.price) AS avg"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_union() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create tables and view with union
    execute_sql(
        &connection,
        "CREATE TABLE active_users (id SERIAL PRIMARY KEY, name TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TABLE inactive_users (id SERIAL PRIMARY KEY, name TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW all_users AS SELECT id, name FROM active_users UNION SELECT id, name FROM inactive_users;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view with union
    let view = schema.views.get("all_users").expect("View should exist");
    assert_eq!(view.columns, vec!["id", "name"]);
    assert!(view.definition.contains("UNION"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_case_statement() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with case statement
    execute_sql(
        &connection,
        "CREATE TABLE grades (id SERIAL PRIMARY KEY, student TEXT, score INTEGER);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW grade_letters AS SELECT student, score, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' ELSE 'F' END as letter_grade FROM grades;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view with case statement
    let view = schema.views.get("grade_letters").expect("View should exist");
    debug!("View: {:?}", view);
    assert_eq!(view.columns, vec!["student", "score", "letter_grade"]);
    assert!(view.definition.contains("CASE"));
    assert!(view.definition.contains("WHEN (score >= 90)"));
    assert!(view.definition.contains("letter_grade"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_functions() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with functions
    execute_sql(
        &connection,
        "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, created_at TIMESTAMP);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW user_summary AS SELECT id, UPPER(name) as upper_name, EXTRACT(YEAR FROM created_at) as birth_year FROM users;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view with functions
    let view = schema.views.get("user_summary").expect("View should exist");
    debug!("View: {:?}", view);
    assert_eq!(view.columns, vec!["id", "upper_name", "birth_year"]);
    assert!(view.definition.contains("upper(name)"));
    assert!(view.definition.contains("EXTRACT(year FROM created_at)"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_distinct() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with distinct
    execute_sql(
        &connection,
        "CREATE TABLE visits (id SERIAL PRIMARY KEY, user_id INTEGER, page TEXT, visit_date DATE);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW unique_visitors AS SELECT DISTINCT user_id, COUNT(*) as visit_count FROM visits GROUP BY user_id;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view with distinct
    let view = schema.views.get("unique_visitors").expect("View should exist");
    assert_eq!(view.columns, vec!["user_id", "visit_count"]);
    assert!(view.definition.contains("DISTINCT user_id"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_limit_offset() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with limit/offset
    execute_sql(
        &connection,
        "CREATE TABLE articles (id SERIAL PRIMARY KEY, title TEXT, views INTEGER);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW top_articles AS SELECT * FROM articles ORDER BY views DESC LIMIT 10;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view with limit
    let view = schema.views.get("top_articles").expect("View should exist");
    assert_eq!(view.columns, vec!["id", "title", "views"]);
    assert!(view.definition.contains("LIMIT 10"));
    assert!(view.definition.contains("ORDER BY views DESC"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_complex_expression() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with complex expression
    execute_sql(
        &connection,
        "CREATE TABLE measurements (id SERIAL PRIMARY KEY, value1 DECIMAL(10,2), value2 DECIMAL(10,2));",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW calculated_metrics AS SELECT id, (value1 + value2) / 2 as average, SQRT(value1 * value2) as geometric_mean FROM measurements;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view with complex expression
    let view = schema.views.get("calculated_metrics").expect("View should exist");
    debug!("View: {:?}", view);
    assert_eq!(view.columns, vec!["id", "average", "geometric_mean"]);
    assert!(view.definition.contains("(value1 + value2) / (2)::numeric"));
    assert!(view.definition.contains("sqrt((value1 * value2))"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_views() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and multiple views
    execute_sql(
        &connection,
        "CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT, salary INTEGER, department TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW high_salary_employees AS SELECT * FROM employees WHERE salary > 50000;",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW department_summary AS SELECT department, COUNT(*) as count, AVG(salary) as avg_salary FROM employees GROUP BY department;",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW employee_names AS SELECT id, name FROM employees ORDER BY name;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify all views exist
    assert!(schema.views.contains_key("high_salary_employees"));
    assert!(schema.views.contains_key("department_summary"));
    assert!(schema.views.contains_key("employee_names"));

    // Verify view details
    let high_salary = schema.views.get("high_salary_employees").unwrap();
    assert_eq!(high_salary.columns, vec!["id", "name", "salary", "department"]);

    let dept_summary = schema.views.get("department_summary").unwrap();
    assert_eq!(dept_summary.columns, vec!["department", "count", "avg_salary"]);

    let names = schema.views.get("employee_names").unwrap();
    assert_eq!(names.columns, vec!["id", "name"]);

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_performance() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table with many columns and view
    execute_sql(
        &connection,
        "CREATE TABLE large_table (
            id SERIAL PRIMARY KEY,
            col1 TEXT, col2 INTEGER, col3 DECIMAL(10,2), col4 BOOLEAN,
            col5 TEXT, col6 INTEGER, col7 DECIMAL(10,2), col8 BOOLEAN,
            col9 TEXT, col10 INTEGER, col11 DECIMAL(10,2), col12 BOOLEAN,
            col13 TEXT, col14 INTEGER, col15 DECIMAL(10,2), col16 BOOLEAN,
            col17 TEXT, col18 INTEGER, col19 DECIMAL(10,2), col20 BOOLEAN
        );",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW filtered_large_table AS SELECT * FROM large_table WHERE col2 > 100;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view with many columns
    let view = schema.views.get("filtered_large_table").expect("View should exist");
    debug!("View: {:?}", view);
    assert_eq!(view.columns.len(), 21); // id + 20 columns
    assert!(view.definition.contains("WHERE (col2 > 100)"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view
    execute_sql(
        &connection,
        "CREATE TABLE test_table (id INTEGER, name TEXT, value DECIMAL(10,2));",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW test_view AS SELECT id, name, value * 2 as doubled_value FROM test_table;",
    )
    .await?;

    // Introspect multiple times to ensure consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let view1 = schema1.views.get("test_view").expect("View should exist");
    let view2 = schema2.views.get("test_view").expect("View should exist");

    // Verify consistency
    assert_eq!(view1.name, view2.name);
    assert_eq!(view1.schema, view2.schema);
    assert_eq!(view1.definition, view2.definition);
    assert_eq!(view1.check_option, view2.check_option);
    assert_eq!(view1.security_barrier, view2.security_barrier);
    assert_eq!(view1.columns, view2.columns);

    Ok(())
}

#[tokio::test]
async fn test_introspect_view_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and view with edge cases
    execute_sql(
        &connection,
        "CREATE TABLE edge_cases (id INTEGER, \"quoted column\" TEXT, \"UPPER_CASE\" TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE VIEW \"quoted view\" AS SELECT id, \"quoted column\", \"UPPER_CASE\" FROM edge_cases WHERE id IS NOT NULL;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the view with quoted identifiers
    let view = schema.views.get("quoted view").expect("View should exist");
    assert_eq!(view.name, "quoted view");
    assert_eq!(view.columns, vec!["id", "quoted column", "UPPER_CASE"]);
    assert!(view.definition.contains("\"quoted column\""));

    Ok(())
} 