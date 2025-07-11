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
async fn test_introspect_basic_sequence() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic sequence
    execute_sql(
        &connection,
        "CREATE SEQUENCE test_basic_sequence;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;
    
    // Verify the sequence was introspected
    let sequence = schema.sequences.get("test_basic_sequence");
    debug!("Sequence: {:?}", sequence);
    assert!(
        sequence.is_some(),
        "Sequence 'test_basic_sequence' should be introspected"
    );

    let seq = sequence.unwrap();
    assert_eq!(seq.name, "test_basic_sequence");
    assert_eq!(seq.data_type, "bigint", "Default sequence should be bigint");
    assert_eq!(seq.start, 1, "Default start value should be 1");
    assert_eq!(seq.increment, 1, "Default increment should be 1");
    assert_eq!(seq.min_value, Some(1), "Default min value should be 1");
    assert_eq!(seq.max_value, Some(9223372036854775807), "Default max value should be bigint max");
    assert_eq!(seq.cache, 1, "Default cache should be 1");
    assert!(!seq.cycle, "Default sequence should not cycle");
    assert!(seq.owned_by.is_none(), "Sequence should not be owned by any column initially");
    assert!(seq.comment.is_none(), "Sequence should not have comment initially");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_sequence_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and sequence in that schema
    execute_sql(&connection, "CREATE SCHEMA test_sequence_schema;").await?;
    execute_sql(
        &connection,
        "CREATE SEQUENCE test_sequence_schema.schema_sequence;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the sequence was introspected with correct schema
    let sequence = schema.sequences.get("schema_sequence");
    assert!(
        sequence.is_some(),
        "Sequence 'schema_sequence' should be introspected"
    );

    let seq = sequence.unwrap();
    assert_eq!(seq.name, "schema_sequence");
    assert_eq!(
        seq.schema,
        Some("test_sequence_schema".to_string()),
        "Sequence should be in the specified schema"
    );
    assert_eq!(seq.data_type, "bigint");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_sequence_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create sequence and add comment
    execute_sql(
        &connection,
        "CREATE SEQUENCE test_comment_sequence;",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON SEQUENCE test_comment_sequence IS 'Sequence with comment';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the sequence was introspected with comment
    let sequence = schema.sequences.get("test_comment_sequence");
    assert!(
        sequence.is_some(),
        "Sequence 'test_comment_sequence' should be introspected"
    );

    let seq = sequence.unwrap();
    assert_eq!(seq.name, "test_comment_sequence");
    assert_eq!(
        seq.comment,
        Some("Sequence with comment".to_string()),
        "Sequence should have the specified comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_sequence_with_custom_parameters() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create sequence with custom parameters
    execute_sql(
        &connection,
        "CREATE SEQUENCE test_custom_sequence START 100 INCREMENT 5 MINVALUE 50 MAXVALUE 1000 CACHE 10 CYCLE;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the sequence was introspected with custom parameters
    let sequence = schema.sequences.get("test_custom_sequence");
    assert!(
        sequence.is_some(),
        "Sequence 'test_custom_sequence' should be introspected"
    );

    let seq = sequence.unwrap();
    assert_eq!(seq.name, "test_custom_sequence");
    assert_eq!(seq.start, 100, "Sequence should have custom start value");
    assert_eq!(seq.increment, 5, "Sequence should have custom increment");
    assert_eq!(seq.min_value, Some(50), "Sequence should have custom min value");
    assert_eq!(seq.max_value, Some(1000), "Sequence should have custom max value");
    assert_eq!(seq.cache, 10, "Sequence should have custom cache");
    assert!(seq.cycle, "Sequence should cycle");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_sequence_data_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create sequences with different data types
    execute_sql(
        &connection,
        "CREATE SEQUENCE test_smallint_sequence AS smallint;",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE SEQUENCE test_integer_sequence AS integer;",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE SEQUENCE test_bigint_sequence AS bigint;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify sequences with different data types
    let smallint_seq = schema.sequences.get("test_smallint_sequence").unwrap();
    let integer_seq = schema.sequences.get("test_integer_sequence").unwrap();
    let bigint_seq = schema.sequences.get("test_bigint_sequence").unwrap();

    debug!("Smallint sequence: {:?}", smallint_seq);
    debug!("Integer sequence: {:?}", integer_seq);
    debug!("Bigint sequence: {:?}", bigint_seq);

    assert_eq!(smallint_seq.data_type, "smallint");
    assert_eq!(integer_seq.data_type, "integer");
    assert_eq!(bigint_seq.data_type, "bigint");

    // Verify min/max values correspond to data types
    assert_eq!(smallint_seq.min_value, Some(1));
    assert_eq!(smallint_seq.max_value, Some(32767));
    assert_eq!(integer_seq.min_value, Some(1));
    assert_eq!(integer_seq.max_value, Some(2147483647));
    assert_eq!(bigint_seq.min_value, Some(1));
    assert_eq!(bigint_seq.max_value, Some(9223372036854775807));

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_sequence_owned_by() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table with identity column
    execute_sql(
        &connection,
        "CREATE TABLE test_owned_table (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Find the sequence owned by the identity column
    let owned_sequence = schema.sequences.values().find(|seq| seq.owned_by.is_some());
    assert!(
        owned_sequence.is_some(),
        "Should find a sequence owned by an identity column"
    );

    let seq = owned_sequence.unwrap();
    assert!(seq.owned_by.is_some(), "Sequence should be owned by a column");
    let owned_by = seq.owned_by.as_ref().unwrap();
    assert!(owned_by.contains("test_owned_table"), "Sequence should be owned by test_owned_table");
    assert!(owned_by.contains("id"), "Sequence should be owned by id column");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_sequences() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple sequences
    execute_sql(
        &connection,
        "CREATE SEQUENCE test_sequence1;",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE SEQUENCE test_sequence2;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify both sequences were introspected
    assert!(
        schema.sequences.contains_key("test_sequence1"),
        "Sequence 'test_sequence1' should be introspected"
    );
    assert!(
        schema.sequences.contains_key("test_sequence2"),
        "Sequence 'test_sequence2' should be introspected"
    );

    // Verify sequence details
    let seq1 = schema.sequences.get("test_sequence1").unwrap();
    let seq2 = schema.sequences.get("test_sequence2").unwrap();

    assert_eq!(seq1.name, "test_sequence1");
    assert_eq!(seq2.name, "test_sequence2");
    assert_eq!(seq1.data_type, "bigint");
    assert_eq!(seq2.data_type, "bigint");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_no_sequences() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user sequences
    let schema = connection.introspect().await?;

    // Verify no user sequences are present
    // Note: System sequences should be filtered out
    let user_sequences: Vec<&String> = schema.sequences.keys().collect();
    assert!(
        user_sequences.is_empty(),
        "No user sequences should be introspected: {:?}",
        user_sequences
    );

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_sequence_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Test case 1: Sequence with very long name
    let long_name = "a".repeat(50);
    execute_sql(
        &connection,
        &format!("CREATE SEQUENCE {};", long_name),
    )
    .await?;

    let schema = connection.introspect().await?;
    let sequence = schema.sequences.get(&long_name);
    assert!(
        sequence.is_some(),
        "Sequence with long name should be introspected"
    );

    // Test case 2: Sequence with special characters in name
    execute_sql(
        &connection,
        "CREATE SEQUENCE \"test-sequence-with-dashes\";",
    )
    .await?;

    let schema2 = connection.introspect().await?;
    let sequence2 = schema2.sequences.get("test-sequence-with-dashes");
    assert!(
        sequence2.is_some(),
        "Sequence with special characters should be introspected"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_sequence_performance() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple sequences
    for i in 1..=10 {
        execute_sql(
            &connection,
            &format!("CREATE SEQUENCE test_perf_sequence_{};", i),
        )
        .await?;
    }

    // Measure introspection performance
    let start = std::time::Instant::now();
    let schema = connection.introspect().await?;
    let duration = start.elapsed();

    // Verify all sequences were introspected
    for i in 1..=10 {
        assert!(
            schema.sequences.contains_key(&format!("test_perf_sequence_{}", i)),
            "Sequence test_perf_sequence_{} should be introspected",
            i
        );
    }

    // Performance assertion (adjust threshold as needed)
    assert!(
        duration.as_millis() < 1000,
        "Introspection should complete within 1 second"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_sequence_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a sequence
    execute_sql(
        &connection,
        "CREATE SEQUENCE test_consistency_sequence;",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let seq1 = schema1.sequences.get("test_consistency_sequence").unwrap();
    let seq2 = schema2.sequences.get("test_consistency_sequence").unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(seq1.name, seq2.name);
    assert_eq!(seq1.schema, seq2.schema);
    assert_eq!(seq1.data_type, seq2.data_type);
    assert_eq!(seq1.start, seq2.start);
    assert_eq!(seq1.increment, seq2.increment);
    assert_eq!(seq1.min_value, seq2.min_value);
    assert_eq!(seq1.max_value, seq2.max_value);
    assert_eq!(seq1.cache, seq2.cache);
    assert_eq!(seq1.cycle, seq2.cycle);
    assert_eq!(seq1.owned_by, seq2.owned_by);
    assert_eq!(seq1.comment, seq2.comment);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_sequence_all_features() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create sequence with all features
    execute_sql(
        &connection,
        "CREATE SEQUENCE test_all_features_sequence AS integer START 1000 INCREMENT 10 MINVALUE 500 MAXVALUE 5000 CACHE 5 CYCLE;",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON SEQUENCE test_all_features_sequence IS 'Sequence with all features';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the sequence was introspected with all features
    let sequence = schema.sequences.get("test_all_features_sequence");
    assert!(
        sequence.is_some(),
        "Sequence 'test_all_features_sequence' should be introspected"
    );

    let seq = sequence.unwrap();
    debug!("Sequence: {:?}", seq);
    assert_eq!(seq.name, "test_all_features_sequence");
    assert_eq!(seq.data_type, "smallint");
    assert_eq!(seq.start, 1000);
    assert_eq!(seq.increment, 10);
    assert_eq!(seq.min_value, Some(500));
    assert_eq!(seq.max_value, Some(5000));
    assert_eq!(seq.cache, 5);
    assert!(seq.cycle, "Sequence should cycle");
    assert_eq!(
        seq.comment,
        Some("Sequence with all features".to_string()),
        "Sequence should have the specified comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_sequence_schema_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and sequence
    execute_sql(&connection, "CREATE SCHEMA test_sequence_schema_consistency;").await?;
    execute_sql(
        &connection,
        "CREATE SEQUENCE test_sequence_schema_consistency.schema_consistency_sequence;",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let seq1 = schema1.sequences.get("schema_consistency_sequence").unwrap();
    let seq2 = schema2.sequences.get("schema_consistency_sequence").unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(seq1.name, seq2.name);
    assert_eq!(seq1.schema, seq2.schema);
    assert_eq!(seq1.data_type, seq2.data_type);
    assert_eq!(seq1.start, seq2.start);
    assert_eq!(seq1.increment, seq2.increment);
    assert_eq!(seq1.min_value, seq2.min_value);
    assert_eq!(seq1.max_value, seq2.max_value);
    assert_eq!(seq1.cache, seq2.cache);
    assert_eq!(seq1.cycle, seq2.cycle);
    assert_eq!(seq1.owned_by, seq2.owned_by);
    assert_eq!(seq1.comment, seq2.comment);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_sequence_comment_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create sequence with comment
    execute_sql(
        &connection,
        "CREATE SEQUENCE test_comment_consistency_sequence;",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON SEQUENCE test_comment_consistency_sequence IS 'Sequence for consistency testing';",
    )
    .await?;

    // Introspect multiple times to verify comment consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let seq1 = schema1.sequences.get("test_comment_consistency_sequence").unwrap();
    let seq2 = schema2.sequences.get("test_comment_consistency_sequence").unwrap();

    // Verify comment consistency across multiple introspections
    assert_eq!(seq1.comment, seq2.comment);
    assert_eq!(
        seq1.comment,
        Some("Sequence for consistency testing".to_string()),
        "Sequence should have the correct comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_sequence_owned_by_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and table with identity column
    execute_sql(&connection, "CREATE SCHEMA test_owned_schema;").await?;
    execute_sql(
        &connection,
        "CREATE TABLE test_owned_schema.owned_table (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    debug!("Schema: {:?}", schema);
    // Find the sequence owned by the identity column
    let owned_sequence = schema.sequences.values().find(|seq| seq.owned_by.is_some());
    debug!("Owned sequence: {:?}", owned_sequence);
    assert!(
        owned_sequence.is_some(),
        "Should find a sequence owned by an identity column"
    );

    let seq = owned_sequence.unwrap();
    assert!(seq.owned_by.is_some(), "Sequence should be owned by a column");
    let owned_by = seq.owned_by.as_ref().unwrap();
    assert!(owned_by.contains("test_owned_schema"), "Sequence should be owned by test_owned_schema");
    assert!(owned_by.contains("owned_table"), "Sequence should be owned by owned_table");
    assert!(owned_by.contains("id"), "Sequence should be owned by id column");

    // Clean up
    db.cleanup().await?;
    Ok(())
} 