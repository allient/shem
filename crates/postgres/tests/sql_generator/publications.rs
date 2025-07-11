use shem_core::schema::Publication;
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;

#[test]
fn test_create_publication_all_tables() {
    let publication = Publication {
        name: "pub1".to_string(),
        tables: vec![],
        all_tables: true,
        insert: true,
        update: true,
        delete: true,
        truncate: true,
    };
    let sql = PostgresSqlGenerator.create_publication(&publication).unwrap();
    assert!(sql.contains("CREATE PUBLICATION \"pub1\" FOR ALL TABLES"));
    assert!(sql.contains("WITH (INSERT, UPDATE, DELETE, TRUNCATE)"));
}

#[test]
fn test_create_publication_specific_tables() {
    let publication = Publication {
        name: "pub2".to_string(),
        tables: vec!["table1".to_string(), "table2".to_string()],
        all_tables: false,
        insert: true,
        update: false,
        delete: true,
        truncate: false,
    };
    let sql = PostgresSqlGenerator.create_publication(&publication).unwrap();
    assert!(sql.contains("CREATE PUBLICATION \"pub2\" FOR TABLE \"table1\", \"table2\""));
    assert!(sql.contains("WITH (INSERT, DELETE)"));
}

#[test]
fn test_drop_publication() {
    let publication = Publication {
        name: "pub1".to_string(),
        tables: vec![],
        all_tables: true,
        insert: true,
        update: true,
        delete: true,
        truncate: true,
    };
    let sql = PostgresSqlGenerator.drop_publication(&publication).unwrap();
    assert_eq!(sql, "DROP PUBLICATION IF EXISTS \"pub1\" CASCADE;");
} 