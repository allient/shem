use shem_core::schema::Extension;
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;

#[test]
fn test_create_extension() {
    let extension = Extension {
        name: "uuid-ossp".to_string(),
        schema: None,
        version: "1.1".to_string(),
        cascade: false,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_extension(&extension).unwrap();
    
    assert_eq!(result, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" VERSION '1.1';");
}

#[test]
fn test_create_extension_no_version() {
    let extension = Extension {
        name: "pgcrypto".to_string(),
        schema: None,
        version: "".to_string(),
        cascade: false,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_extension(&extension).unwrap();
    
    assert_eq!(result, "CREATE EXTENSION IF NOT EXISTS pgcrypto;");
}

#[test]
fn test_create_extension_with_schema() {
    let extension = Extension {
        name: "postgis".to_string(),
        schema: Some("public".to_string()),
        version: "3.1.4".to_string(),
        cascade: false,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_extension(&extension).unwrap();
    
    assert_eq!(result, "CREATE EXTENSION IF NOT EXISTS postgis VERSION '3.1.4' SCHEMA public;");
}

#[test]
fn test_create_extension_with_reserved_keyword() {
    let extension = Extension {
        name: "order".to_string(), // Reserved keyword
        schema: None,
        version: "1.0".to_string(),
        cascade: false,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_extension(&extension).unwrap();
    
    assert_eq!(result, "CREATE EXTENSION IF NOT EXISTS \"order\" VERSION '1.0';");
}

#[test]
fn test_create_extension_with_hyphen() {
    let extension = Extension {
        name: "uuid-ossp".to_string(),
        schema: None,
        version: "1.1".to_string(),
        cascade: false,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_extension(&extension).unwrap();
    
    assert_eq!(result, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" VERSION '1.1';");
}

#[test]
fn test_drop_extension() {
    let extension = Extension {
        name: "my_extension".to_string(),
        schema: None,
        version: "".to_string(),
        cascade: false,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_extension(&extension).unwrap();
    
    assert_eq!(result, "DROP EXTENSION IF EXISTS my_extension CASCADE;");
}

#[test]
fn test_drop_extension_with_schema() {
    let extension = Extension {
        name: "my_extension".to_string(),
        schema: Some("public".to_string()),
        version: "".to_string(),
        cascade: false,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_extension(&extension).unwrap();
    
    assert_eq!(result, "DROP EXTENSION IF EXISTS my_extension CASCADE;");
}

#[test]
fn test_drop_extension_with_reserved_keyword() {
    let extension = Extension {
        name: "order".to_string(), // Reserved keyword
        schema: None,
        version: "".to_string(),
        cascade: false,
        comment: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_extension(&extension).unwrap();
    
    assert_eq!(result, "DROP EXTENSION IF EXISTS \"order\" CASCADE;");
}

#[test]
fn test_create_extension_common_extensions() {
    let common_extensions = vec![
        ("uuid-ossp", "1.1"),
        ("pgcrypto", "1.3"),
        ("postgis", "3.1.4"),
        ("hstore", "1.8"),
        ("ltree", "1.2"),
        ("unaccent", "1.1"),
        ("pg_trgm", "1.6"),
        ("btree_gin", "1.3"),
        ("btree_gist", "1.6"),
    ];

    for (name, version) in common_extensions {
        let extension = Extension {
            name: name.to_string(),
            schema: None,
            version: version.to_string(),
            cascade: false,
            comment: None,
        };

        let generator = PostgresSqlGenerator;
        let result = generator.create_extension(&extension).unwrap();
        
        if name.contains('-') {
            assert!(result.contains(&format!("\"{}\"", name)));
        } else {
            assert!(result.contains(name));
        }
        assert!(result.contains(&format!("VERSION '{}'", version)));
    }
} 