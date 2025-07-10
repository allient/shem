use log::debug;
use postgres::TestDb;
use shem_core::DatabaseConnection;

/// Test helper function to execute SQL on the test database
async fn execute_sql(
    connection: &Box<dyn DatabaseConnection>,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    connection.execute(sql).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_basic_composite_type() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic composite type
    execute_sql(
        &connection,
        "CREATE TYPE address_type AS (street TEXT, city TEXT, zipcode TEXT);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected
    let composite_type = schema.composite_types.get("address_type");
    debug!("Composite type: {:?}", composite_type);
    assert!(
        composite_type.is_some(),
        "Composite type 'address_type' should be introspected"
    );

    let comp_type = composite_type.unwrap();
    assert_eq!(comp_type.name, "address_type");
    assert_eq!(comp_type.attributes.len(), 3, "Should have 3 attributes");

    // Verify attributes
    let street_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "street")
        .unwrap();
    assert_eq!(street_attr.type_name, "text");
    assert_eq!(street_attr.nullable, true);

    let city_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "city")
        .unwrap();
    assert_eq!(city_attr.type_name, "text");
    assert_eq!(city_attr.nullable, true);

    let zipcode_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "zipcode")
        .unwrap();
    assert_eq!(zipcode_attr.type_name, "text");
    assert_eq!(zipcode_attr.nullable, true);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and composite type in that schema
    execute_sql(&connection, "CREATE SCHEMA test_composite;").await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_composite.person_type AS (name TEXT, age INTEGER);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected with correct schema
    let composite_type = schema.composite_types.get("person_type");
    assert!(
        composite_type.is_some(),
        "Composite type 'person_type' should be introspected"
    );

    let comp_type = composite_type.unwrap();
    assert_eq!(comp_type.name, "person_type");
    assert_eq!(
        comp_type.schema,
        Some("test_composite".to_string()),
        "Composite type should be in the specified schema"
    );
    assert_eq!(comp_type.attributes.len(), 2, "Should have 2 attributes");

    // Verify attributes
    let name_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "name")
        .unwrap();
    assert_eq!(name_attr.type_name, "text");

    let age_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "age")
        .unwrap();
    assert_eq!(age_attr.type_name, "integer");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create composite type and add comment
    execute_sql(
        &connection,
        "CREATE TYPE point_type AS (x NUMERIC, y NUMERIC);",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON TYPE point_type IS '2D point with x,y coordinates';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected with comment
    let composite_type = schema.composite_types.get("point_type");
    assert!(
        composite_type.is_some(),
        "Composite type 'point_type' should be introspected"
    );

    let comp_type = composite_type.unwrap();
    assert_eq!(comp_type.name, "point_type");
    assert_eq!(
        comp_type.comment,
        Some("2D point with x,y coordinates".to_string()),
        "Composite type should have the specified comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_with_detailed_comments() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create composite type with multiple attributes
    execute_sql(
        &connection,
        "CREATE TYPE employee_info AS (
            id UUID,
            name TEXT,
            email TEXT,
            salary NUMERIC(10,2),
            department TEXT,
            hire_date DATE
        );",
    )
    .await?;

    // Add comment to the composite type itself
    execute_sql(
        &connection,
        "COMMENT ON TYPE employee_info IS 'Employee information composite type for HR system';",
    )
    .await?;

    // Add comments to individual attributes (if supported by the introspection)
    // Note: PostgreSQL doesn't support comments on composite type attributes directly
    // but we can test the type-level comment

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected with comment
    let composite_type = schema.composite_types.get("employee_info");
    assert!(
        composite_type.is_some(),
        "Composite type 'employee_info' should be introspected"
    );

    let comp_type = composite_type.unwrap();
    assert_eq!(comp_type.name, "employee_info");
    assert_eq!(
        comp_type.comment,
        Some("Employee information composite type for HR system".to_string()),
        "Composite type should have the specified comment"
    );

    // Verify all attributes are present
    assert_eq!(comp_type.attributes.len(), 6, "Should have 6 attributes");

    // Verify attribute types
    let id_attr = comp_type.attributes.iter().find(|a| a.name == "id").unwrap();
    assert_eq!(id_attr.type_name, "uuid");

    let name_attr = comp_type.attributes.iter().find(|a| a.name == "name").unwrap();
    assert_eq!(name_attr.type_name, "text");

    let email_attr = comp_type.attributes.iter().find(|a| a.name == "email").unwrap();
    assert_eq!(email_attr.type_name, "text");

    let salary_attr = comp_type.attributes.iter().find(|a| a.name == "salary").unwrap();
    assert_eq!(salary_attr.type_name, "numeric(10,2)");

    let department_attr = comp_type.attributes.iter().find(|a| a.name == "department").unwrap();
    assert_eq!(department_attr.type_name, "text");

    let hire_date_attr = comp_type.attributes.iter().find(|a| a.name == "hire_date").unwrap();
    assert_eq!(hire_date_attr.type_name, "date");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_with_schema_and_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema
    execute_sql(&connection, "CREATE SCHEMA hr_schema;").await?;

    // Create composite type in specific schema
    execute_sql(
        &connection,
        "CREATE TYPE hr_schema.address_type AS (
            street TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            country TEXT
        );",
    )
    .await?;

    // Add comment to the composite type in schema
    execute_sql(
        &connection,
        "COMMENT ON TYPE hr_schema.address_type IS 'Complete address information for addresses';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected with schema and comment
    let composite_type = schema.composite_types.get("address_type");
    assert!(
        composite_type.is_some(),
        "Composite type 'address_type' should be introspected"
    );

    let comp_type = composite_type.unwrap();
    assert_eq!(comp_type.name, "address_type");
    assert_eq!(
        comp_type.schema,
        Some("hr_schema".to_string()),
        "Composite type should be in the specified schema"
    );
    assert_eq!(
        comp_type.comment,
        Some("Complete address information for addresses".to_string()),
        "Composite type should have the specified comment"
    );

    // Verify attributes
    assert_eq!(comp_type.attributes.len(), 5, "Should have 5 attributes");

    let country_attr = comp_type.attributes.iter().find(|a| a.name == "country").unwrap();
    assert_eq!(country_attr.type_name, "text");
    assert_eq!(country_attr.default, None, "country should not have default value");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create composite type (all fields are nullable by default)
    execute_sql(
        &connection,
        "CREATE TYPE user_info AS (id UUID, email TEXT, name TEXT);",
    )
    .await?;

    let schema = connection.introspect().await?;

    let composite_type = schema.composite_types.get("user_info");
    assert!(composite_type.is_some());

    let comp_type = composite_type.unwrap();
    assert_eq!(comp_type.attributes.len(), 3);

    // All fields should be nullable in composite types
    for attr in &comp_type.attributes {
        assert_eq!(
            attr.nullable, true,
            "All composite type fields are nullable"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_with_complex_types()
-> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create composite type with complex built-in types
    execute_sql(
        &connection,
        "CREATE TYPE complex_type AS (
            id UUID,
            data JSONB,
            tags TEXT[],
            created_at TIMESTAMPTZ,
            location POINT,
            price DECIMAL(10,2)
        );",
    )
    .await?;

    let schema = connection.introspect().await?;    
    let comp_type = schema.composite_types.get("complex_type").unwrap();
    debug!("Comp type: {:?}", comp_type);

    assert_eq!(comp_type.attributes.len(), 6);

    // Verify complex types
    let id_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "id")
        .unwrap();
    assert_eq!(id_attr.type_name, "uuid");

    let data_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "data")
        .unwrap();
    assert_eq!(data_attr.type_name, "jsonb");

    let tags_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "tags")
        .unwrap();
    assert_eq!(tags_attr.type_name, "text[]");

    let created_at_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "created_at")
        .unwrap();
    assert_eq!(created_at_attr.type_name, "timestamptz");

    let location_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "location")
        .unwrap();
    assert_eq!(location_attr.type_name, "point");

    let price_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "price")
        .unwrap();
    assert_eq!(price_attr.type_name, "numeric(10,2)");

    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_composite_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple composite types
    execute_sql(
        &connection,
        "CREATE TYPE point_2d AS (x NUMERIC, y NUMERIC);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE point_3d AS (x NUMERIC, y NUMERIC, z NUMERIC);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE rectangle AS (top_left point_2d, bottom_right point_2d);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify all composite types were introspected
    assert!(
        schema.composite_types.contains_key("point_2d"),
        "Composite type 'point_2d' should be introspected"
    );
    assert!(
        schema.composite_types.contains_key("point_3d"),
        "Composite type 'point_3d' should be introspected"
    );
    assert!(
        schema.composite_types.contains_key("rectangle"),
        "Composite type 'rectangle' should be introspected"
    );

    // Verify type details
    let point_2d = schema.composite_types.get("point_2d").unwrap();
    let point_3d = schema.composite_types.get("point_3d").unwrap();
    let rectangle = schema.composite_types.get("rectangle").unwrap();

    assert_eq!(point_2d.attributes.len(), 2);
    assert_eq!(point_3d.attributes.len(), 3);
    assert_eq!(rectangle.attributes.len(), 2);

    // Verify nested composite type
    let top_left_attr = rectangle
        .attributes
        .iter()
        .find(|a| a.name == "top_left")
        .unwrap();
    assert_eq!(top_left_attr.type_name, "point_2d");

    let bottom_right_attr = rectangle
        .attributes
        .iter()
        .find(|a| a.name == "bottom_right")
        .unwrap();
    assert_eq!(bottom_right_attr.type_name, "point_2d");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_with_collation() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create composite type with collation
    execute_sql(
        &connection,
        "CREATE TYPE localized_text AS (text_value TEXT COLLATE \"C\", description TEXT COLLATE \"en_US\");",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected
    let composite_type = schema.composite_types.get("localized_text");
    assert!(
        composite_type.is_some(),
        "Composite type 'localized_text' should be introspected"
    );

    let comp_type = composite_type.unwrap();
    assert_eq!(comp_type.attributes.len(), 2, "Should have 2 attributes");

    // Verify collation
    let text_value_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "text_value")
        .unwrap();
    assert_eq!(text_value_attr.type_name, "text");
    assert_eq!(text_value_attr.collation, Some("C".to_string()));

    let description_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "description")
        .unwrap();
    assert_eq!(description_attr.type_name, "text");
    assert_eq!(description_attr.collation, Some("en_US".to_string()));

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_with_storage() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a table with explicit storage settings
    execute_sql(
        &connection,
        "CREATE TABLE storage_test_table (
                plain_text TEXT STORAGE PLAIN,
                extended_text TEXT STORAGE EXTENDED,
                main_text TEXT STORAGE MAIN,
                external_text TEXT STORAGE EXTERNAL
            )",
    )
    .await?;

    // Create a composite type from the table
    execute_sql(
        &connection,
        "CREATE TYPE storage_test AS (
            plain_text TEXT,
            extended_text TEXT,
            main_text TEXT,
            external_text TEXT
        )",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected
    let composite_type = schema.composite_types.get("storage_test");
    assert!(
        composite_type.is_some(),
        "Composite type 'storage_test' should be introspected"
    );

    let comp_type = composite_type.unwrap();
    assert_eq!(comp_type.attributes.len(), 4, "Should have 4 attributes");

    debug!("Comp type: {:?}", comp_type);
    // Verify storage specifications
    let plain_text_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "plain_text")
        .unwrap();
    assert_eq!(plain_text_attr.type_name, "text");
    assert_eq!(
        plain_text_attr.storage,
        Some(shem_core::ColumnStorage::Extended)
    );

    let extended_text_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "extended_text")
        .unwrap();
    assert_eq!(extended_text_attr.type_name, "text");
    assert_eq!(
        extended_text_attr.storage,
        Some(shem_core::ColumnStorage::Extended)
    );

    let main_text_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "main_text")
        .unwrap();
    assert_eq!(main_text_attr.type_name, "text");
    assert_eq!(
        main_text_attr.storage,
        Some(shem_core::ColumnStorage::Extended)
    );

    let external_text_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "external_text")
        .unwrap();
    assert_eq!(external_text_attr.type_name, "text");
    assert_eq!(
        external_text_attr.storage,
        Some(shem_core::ColumnStorage::Extended)
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_no_composite_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user composite types
    let schema = connection.introspect().await?;

    // Verify no user composite types are present
    // Note: System composite types should be filtered out
    let user_composite_types: Vec<&String> = schema.composite_types.keys().collect();
    assert!(
        user_composite_types.is_empty(),
        "No user composite types should be introspected: {:?}",
        user_composite_types
    );

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Test case 1: Composite type with very long attribute names
    execute_sql(
        &connection,
        "CREATE TYPE edge_case_type AS (
            very_long_attribute_name_that_exceeds_normal_length TEXT,
            \"quoted_attribute_name\" INTEGER,
            \"attribute_with_spaces\" BOOLEAN
        );",
    )
    .await?;

    let schema = connection.introspect().await?;
    let composite_type = schema.composite_types.get("edge_case_type");
    assert!(
        composite_type.is_some(),
        "Composite type with edge case names should be introspected"
    );

    let comp_type = composite_type.unwrap();
    assert_eq!(comp_type.attributes.len(), 3, "Should have 3 attributes");

    // Verify quoted attribute names are handled correctly
    let quoted_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "quoted_attribute_name")
        .unwrap();
    assert_eq!(quoted_attr.type_name, "integer");

    let spaces_attr = comp_type
        .attributes
        .iter()
        .find(|a| a.name == "attribute_with_spaces")
        .unwrap();
    assert_eq!(spaces_attr.type_name, "boolean");

    // Test case 2: Composite type with single attribute
    execute_sql(&connection, "CREATE TYPE single_attr_type AS (value TEXT);").await?;

    let schema2 = connection.introspect().await?;
    let single_type = schema2.composite_types.get("single_attr_type");
    assert!(
        single_type.is_some(),
        "Composite type with single attribute should be introspected"
    );

    let single_comp_type = single_type.unwrap();
    assert_eq!(
        single_comp_type.attributes.len(),
        1,
        "Should have 1 attribute"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create composite type
    execute_sql(
        &connection,
        "CREATE TYPE consistency_test AS (field1 TEXT, field2 INTEGER);",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let comp_type1 = schema1.composite_types.get("consistency_test").unwrap();
    let comp_type2 = schema2.composite_types.get("consistency_test").unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(comp_type1.name, comp_type2.name);
    assert_eq!(comp_type1.schema, comp_type2.schema);
    assert_eq!(comp_type1.comment, comp_type2.comment);
    assert_eq!(comp_type1.attributes.len(), comp_type2.attributes.len());

    // Verify attribute consistency
    for (attr1, attr2) in comp_type1
        .attributes
        .iter()
        .zip(comp_type2.attributes.iter())
    {
        assert_eq!(attr1.name, attr2.name);
        assert_eq!(attr1.type_name, attr2.type_name);
        assert_eq!(attr1.nullable, attr2.nullable);
        assert_eq!(attr1.default, attr2.default);
        assert_eq!(attr1.collation, attr2.collation);
        assert_eq!(attr1.storage, attr2.storage);
    }

    // Clean up
    db.cleanup().await?;
    Ok(())
}
