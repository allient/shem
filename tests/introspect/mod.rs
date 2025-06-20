pub mod fixtures;
pub mod helpers;
pub mod test_cases;

use crate::fixtures::*;
use crate::helpers::*;
use crate::test_cases::*;

#[tokio::test]
async fn test_all_introspect_features() {
    let test_db = setup_test_database().await.unwrap();
    
    // Run all test cases
    test_tables_introspect(&test_db).await;
    test_views_introspect(&test_db).await;
    test_materialized_views_introspect(&test_db).await;
    test_functions_introspect(&test_db).await;
    test_procedures_introspect(&test_db).await;
    test_enums_introspect(&test_db).await;
    test_domains_introspect(&test_db).await;
    test_composite_types_introspect(&test_db).await;
    test_range_types_introspect(&test_db).await;
    test_sequences_introspect(&test_db).await;
    test_extensions_introspect(&test_db).await;
    test_triggers_introspect(&test_db).await;
    test_policies_introspect(&test_db).await;
    test_servers_introspect(&test_db).await;
    test_collations_introspect(&test_db).await;
    test_rules_introspect(&test_db).await;
    
    cleanup_test_database(test_db).await.unwrap();
} 