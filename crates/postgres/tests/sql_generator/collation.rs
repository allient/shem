use shem_core::schema::{Rule, Collation, RuleEvent, CollationProvider};
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;

fn test_create_collation() {
    let collation = Collation {
        name: "my_collation".to_string(),
        schema: Some("public".to_string()),
        locale: Some("en_US".to_string()),
        lc_collate: None,
        lc_ctype: None,
        provider: CollationProvider::Icu,
        deterministic: true,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_collation(&collation).unwrap();
    
    assert!(result.contains("CREATE COLLATION public.my_collation"));
    assert!(result.contains("(LOCALE = 'en_US')"));
    assert!(result.contains("PROVIDER icu"));
    assert!(result.contains("DETERMINISTIC"));
}

#[test]
fn test_create_collation_not_deterministic() {
    let collation = Collation {
        name: "my_collation".to_string(),
        schema: None,
        locale: Some("C".to_string()),
        lc_collate: None,
        lc_ctype: None,
        provider: CollationProvider::Libc,
        deterministic: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_collation(&collation).unwrap();
    
    assert!(result.contains("CREATE COLLATION my_collation"));
    assert!(result.contains("(LOCALE = 'C')"));
    assert!(result.contains("PROVIDER libc"));
    assert!(!result.contains("DETERMINISTIC"));
}

#[test]
fn test_create_collation_with_reserved_keyword() {
    let collation = Collation {
        name: "order".to_string(), // Reserved keyword
        schema: None,
        locale: Some("en_US".to_string()),
        lc_collate: None,
        lc_ctype: None,
        provider: CollationProvider::Icu,
        deterministic: true,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_collation(&collation).unwrap();
    
    assert!(result.contains("CREATE COLLATION \"order\""));
    assert!(result.contains("(LOCALE = 'en_US')"));
    assert!(result.contains("PROVIDER icu"));
    assert!(result.contains("DETERMINISTIC"));
}

#[test]
fn test_drop_collation() {
    let collation = Collation {
        name: "my_collation".to_string(),
        schema: None,
        locale: Some("en_US".to_string()),
        lc_collate: None,
        lc_ctype: None,
        provider: CollationProvider::Icu,
        deterministic: true,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_collation(&collation).unwrap();
    
    assert_eq!(result, "DROP COLLATION IF EXISTS my_collation CASCADE;");
}

#[test]
fn test_drop_collation_with_schema() {
    let collation = Collation {
        name: "my_collation".to_string(),
        schema: Some("public".to_string()),
        locale: Some("en_US".to_string()),
        lc_collate: None,
        lc_ctype: None,
        provider: CollationProvider::Icu,
        deterministic: true,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_collation(&collation).unwrap();
    
    assert_eq!(result, "DROP COLLATION IF EXISTS public.my_collation CASCADE;");
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
            name: format!("collation_{}", locale.replace('-', "_")),
            schema: None,
            locale: Some(locale.to_string()),
            lc_collate: None,
            lc_ctype: None,
            provider: provider.clone(),
            deterministic: true,
        };

        let generator = PostgresSqlGenerator;
        let result = generator.create_collation(&collation).unwrap();
        
        assert!(result.contains(&format!("LOCALE = '{}'", locale)));
        let provider_str = match &provider {
            CollationProvider::Libc => "libc",
            CollationProvider::Icu => "icu",
            CollationProvider::Builtin => "builtin",
        };
        assert!(result.contains(&format!("PROVIDER {}", provider_str)));
    }
} 