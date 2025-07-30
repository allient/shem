use pg::model::extension::Extension;
use pg::traits::SqlGenerator;
use pg::sql_generator::PostgresSqlGenerator;

#[test]
fn test_create_extension() {
    let extension = Extension {
        oid: 0,
        name: "uuid-ossp".to_string(),
        owner: "postgres".to_string(),
        relocatable: false,
        version: "1.1".to_string(),
        schema: "public".to_string(),
        comment: None,
        is_user_defined: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_extension(&extension).unwrap();
    
    assert_eq!(result, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" VERSION '1.1';");
}

#[test]
fn test_create_extension_no_version() {
    let extension = Extension {
        oid: 0,
        name: "pgcrypto".to_string(),
        owner: "postgres".to_string(),
        relocatable: false,
        version: "".to_string(),
        schema: "public".to_string(),
        comment: None,
        is_user_defined: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_extension(&extension).unwrap();
    
    assert_eq!(result, "CREATE EXTENSION IF NOT EXISTS pgcrypto;");
}

#[test]
fn test_create_extension_with_schema() {
    let extension = Extension {
        oid: 0,
        name: "postgis".to_string(),
        owner: "postgres".to_string(),
        relocatable: false,
        version: "3.1.4".to_string(),
        schema: "public".to_string(),
        comment: None,
        is_user_defined: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_extension(&extension).unwrap();
    
    assert_eq!(result, "CREATE EXTENSION IF NOT EXISTS postgis VERSION '3.1.4';");
}

#[test]
fn test_create_extension_with_reserved_keyword() {
    let extension = Extension {
        oid: 0,
        name: "order".to_string(), // Reserved keyword
        owner: "postgres".to_string(),
        relocatable: false,
        version: "1.0".to_string(),
        schema: "public".to_string(),
        comment: None,
        is_user_defined: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_extension(&extension).unwrap();
    
    assert_eq!(result, "CREATE EXTENSION IF NOT EXISTS \"order\" VERSION '1.0';");
}

#[test]
fn test_create_extension_with_hyphen() {
    let extension = Extension {
        oid: 0,
        name: "uuid-ossp".to_string(),
        owner: "postgres".to_string(),
        relocatable: false,
        version: "1.1".to_string(),
        schema: "public".to_string(),
        comment: None,
        is_user_defined: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_extension(&extension).unwrap();
    
    assert_eq!(result, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" VERSION '1.1';");
}

#[test]
fn test_drop_extension() {
    let extension = Extension {
        oid: 0,
        name: "my_extension".to_string(),
        owner: "postgres".to_string(),
        relocatable: false,
        version: "".to_string(),
        schema: "public".to_string(),
        comment: None,
        is_user_defined: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_extension(&extension).unwrap();
    
    assert_eq!(result, "DROP EXTENSION IF EXISTS my_extension CASCADE;");
}

#[test]
fn test_drop_extension_with_schema() {
    let extension = Extension {
        oid: 0,
        name: "my_extension".to_string(),
        owner: "postgres".to_string(),
        relocatable: false,
        version: "".to_string(),
        schema: "public".to_string(),
        comment: None,
        is_user_defined: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_extension(&extension).unwrap();
    
    assert_eq!(result, "DROP EXTENSION IF EXISTS my_extension CASCADE;");
}

#[test]
fn test_drop_extension_with_reserved_keyword() {
    let extension = Extension {
        oid: 0,
        name: "order".to_string(), // Reserved keyword
        owner: "postgres".to_string(),
        relocatable: false,
        version: "".to_string(),
        schema: "public".to_string(),
        comment: None,
        is_user_defined: false,
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
            oid: 0,
            name: name.to_string(),
            owner: "postgres".to_string(),
            relocatable: false,
            version: version.to_string(),
            schema: "public".to_string(),
            comment: None,
            is_user_defined: false,
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