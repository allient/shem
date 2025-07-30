use pg::model::collation::{Collation, CollationProvider};
use pg::traits::SqlGenerator;
use pg::sql_generator::PostgresSqlGenerator;

#[test]
fn test_create_collation() {
    let collation = Collation {
        oid: 0,
        name: "my_collation".to_string(),
        owner: "postgres".to_string(),
        schema: "public".to_string(),
        provider: CollationProvider::Icu,
        deterministic: true,
        lc_collate: None,
        lc_ctype: None,
        icu_locale: Some("en_US".to_string()),
        icu_rules: None,
        version: None,
        comment: None,
        is_user_defined: false,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_collation(&collation).unwrap();
    
    println!("test_create_collation - Generated SQL: {}", result);
    
    assert!(result.contains("CREATE COLLATION \"my_collation\""));
    assert!(result.contains("(LOCALE = 'en_US'"));
    assert!(result.contains("PROVIDER = 'icu'"));
    // DETERMINISTIC is not included when it's true (default)
}

#[test]
fn test_create_collation_not_deterministic() {
    let collation = Collation {
        oid: 0,
        name: "my_collation".to_string(),
        owner: "postgres".to_string(),
        schema: "public".to_string(),
        provider: CollationProvider::Libc,
        deterministic: false,
        lc_collate: Some("C".to_string()),
        lc_ctype: Some("C".to_string()),
        icu_locale: None,
        icu_rules: None,
        version: None,
        comment: None,
        is_user_defined: false,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_collation(&collation).unwrap();
    
    println!("test_create_collation_not_deterministic - Generated SQL: {}", result);
    
    assert!(result.contains("CREATE COLLATION \"my_collation\""));
    assert!(result.contains("(LC_COLLATE = 'C'"));
    assert!(result.contains("LC_CTYPE = 'C'"));
    assert!(result.contains("PROVIDER = 'libc'"));
    assert!(result.contains("DETERMINISTIC = false"));
}

#[test]
fn test_create_collation_with_reserved_keyword() {
    let collation = Collation {
        oid: 0,
        name: "order".to_string(), // Reserved keyword
        owner: "postgres".to_string(),
        schema: "public".to_string(),
        provider: CollationProvider::Icu,
        deterministic: true,
        lc_collate: None,
        lc_ctype: None,
        icu_locale: Some("en_US".to_string()),
        icu_rules: None,
        version: None,
        comment: None,
        is_user_defined: false,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_collation(&collation).unwrap();
    
    println!("test_create_collation_with_reserved_keyword - Generated SQL: {}", result);
    
    assert!(result.contains("CREATE COLLATION \"order\""));
    assert!(result.contains("(LOCALE = 'en_US'"));
    assert!(result.contains("PROVIDER = 'icu'"));
    // DETERMINISTIC is not included when it's true (default)
}

#[test]
fn test_drop_collation() {
    let collation = Collation {
        oid: 0,
        name: "my_collation".to_string(),
        owner: "postgres".to_string(),
        schema: "public".to_string(),
        provider: CollationProvider::Icu,
        deterministic: true,
        lc_collate: None,
        lc_ctype: None,
        icu_locale: Some("en_US".to_string()),
        icu_rules: None,
        version: None,
        comment: None,
        is_user_defined: false,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_collation(&collation).unwrap();
    
    println!("test_drop_collation - Generated SQL: {}", result);
    
    assert_eq!(result, "DROP COLLATION IF EXISTS my_collation CASCADE;");
}

#[test]
fn test_drop_collation_with_schema() {
    let collation = Collation {
        oid: 0,
        name: "my_collation".to_string(),
        owner: "postgres".to_string(),
        schema: "custom_schema".to_string(),
        provider: CollationProvider::Icu,
        deterministic: true,
        lc_collate: None,
        lc_ctype: None,
        icu_locale: Some("en_US".to_string()),
        icu_rules: None,
        version: None,
        comment: None,
        is_user_defined: false,
        is_from_extension: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_collation(&collation).unwrap();
    
    println!("test_drop_collation_with_schema - Generated SQL: {}", result);
    
    assert_eq!(result, "DROP COLLATION IF EXISTS custom_schema.my_collation CASCADE;");
}

#[test]
fn test_create_collation_common_locales() {
    let common_locales = vec![
        ("en_US", CollationProvider::Icu),
        ("C", CollationProvider::Libc),
        ("POSIX", CollationProvider::Libc),
        ("de_DE", CollationProvider::Icu),
        ("fr_FR", CollationProvider::Icu),
        ("es_ES", CollationProvider::Icu),
    ];

    for (locale, provider) in common_locales {
        let collation = Collation {
            oid: 0,
            name: format!("collation_{}", locale.replace('-', "_")),
            owner: "postgres".to_string(),
            schema: "public".to_string(),
            provider: provider.clone(),
            deterministic: true,
            lc_collate: None,
            lc_ctype: None,
            icu_locale: Some(locale.to_string()),
            icu_rules: None,
            version: None,
            comment: None,
            is_user_defined: false,
            is_from_extension: false,
        };

        let generator = PostgresSqlGenerator;
        let result = generator.create_collation(&collation).unwrap();
        
        assert!(result.contains(&format!("LOCALE = '{}'", locale)));
        let provider_str = match &provider {
            CollationProvider::Libc => "libc",
            CollationProvider::Icu => "icu",
            CollationProvider::Builtin => "builtin",
            CollationProvider::Default => "default",
        };
        assert!(result.contains(&format!("PROVIDER = '{}'", provider_str)));
    }
} 