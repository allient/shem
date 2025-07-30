use crate::{
    helpers::{get_last_system_oid, get_qualified_name_map},
    model::types::{
        Attribute, BaseType, CompositeType, Domain, DomainConstraint, EnumType, EnumValue,
        PseudoType, RangeType, Type, TypeInfo,
    },
};
use common::error::Result;
use std::collections::HashMap;
use tokio_postgres::GenericClient;

pub async fn introspect_types<C: GenericClient>(
    client: &C,
    include_predefined: bool,
) -> Result<Vec<Type>> {
    // 1. Check if datlastsysoid column exists in pg_database
    let last_system_oid = get_last_system_oid(client).await?;

    tracing::debug!("Last system OID (types): {}", last_system_oid);

    // --- QUERY 1: Fetch all types from pg_type with all common and base-type properties ---
    let types_query = r#"
        SELECT
            t.oid, t.typname AS name, n.nspname AS schema_name, pg_get_userbyid(t.typowner) AS owner,
            t.typtype, t.typacl::text AS acl, obj_description(t.oid, 'pg_type') AS comment,
            t.typarray AS array_type_oid,
            -- Base type properties
            t.typlen AS internal_length, t.typbyval AS is_passed_by_value, t.typalign, t.typstorage,
            t.typcategory, t.typispreferred AS is_preferred, pg_get_expr(t.typdefaultbin, 0) AS default_value,
            t.typelem AS element_type_oid, t.typdelim AS delimiter, (t.typcollation <> 0) AS is_collatable,
            t.typinput::regproc::text AS input_fn, t.typoutput::regproc::text AS output_fn,
            t.typreceive::regproc::text AS receive_fn, t.typsend::regproc::text AS send_fn,
            t.typmodin::regproc::text AS typmod_in_fn, t.typmodout::regproc::text AS typmod_out_fn,
            t.typanalyze::regproc::text AS analyze_fn,
            -- Domain properties
            pg_catalog.format_type(t.typbasetype, t.typtypmod) AS base_type,
            t.typnotnull AS not_null,
            CASE WHEN t.typcollation <> bt.typcollation THEN t.typcollation ELSE 0 END AS collation_oid,
            -- Composite type properties
            t.typrelid as class_oid,
            -- Extension dependency
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = t.oid AND d.classid = 'pg_type'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        LEFT JOIN pg_type bt ON t.typbasetype = bt.oid;
    "#;
    let type_rows = client.query(types_query, &[]).await?;

    // --- QUERY 2: Fetch all composite type attributes at once ---
    let attributes_query = r#"
        SELECT
            a.attrelid AS class_oid,
            a.attname AS name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_name,
            CASE WHEN a.attcollation <> t.typcollation THEN a.attcollation ELSE 0 END AS collation_oid
        FROM pg_attribute a
        JOIN pg_type t ON a.atttypid = t.oid
        WHERE a.attnum > 0 AND NOT a.attisdropped
          AND EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = a.attrelid AND c.relkind = 'c')
        ORDER BY a.attrelid, a.attnum;
    "#;
    let attribute_rows = client.query(attributes_query, &[]).await?;

    // --- QUERY 3: Fetch all enum labels at once ---
    let enums_query =
        "SELECT oid, enumtypid, enumlabel FROM pg_enum ORDER BY enumtypid, enumsortorder";
    let enum_rows = client.query(enums_query, &[]).await?;

    // --- QUERY 4: Fetch all range type details at once ---
    let ranges_query = r#"
        SELECT
            r.rngtypid AS oid,
            pg_catalog.format_type(r.rngsubtype, NULL) AS subtype,
            r.rngsubopc AS subtype_opclass_oid,
            r.rngcollation AS collation_oid,
            r.rngcanonical::regproc::text AS canonical_fn,
            r.rngsubdiff::regproc::text AS subtype_diff_fn,
            r.rngmultitypid as multirange_type_oid
        FROM pg_range r;
    "#;
    let range_rows = client.query(ranges_query, &[]).await?;

    // --- QUERY 5: Fetch all domain constraints ---
    let domain_constraints_query = "SELECT oid, conname, contypid, pg_get_constraintdef(oid) AS definition, NOT convalidated AS not_valid FROM pg_constraint WHERE contypid != 0 AND contype = 'c'";
    let domain_constraint_rows = client.query(domain_constraints_query, &[]).await?;

    // --- Helper Data: Fetch collations and opclasses for name resolution ---
    let collations_map =
        get_qualified_name_map(client, "pg_collation", "collname", "collnamespace").await?;
    let opclasses_map =
        get_qualified_name_map(client, "pg_opclass", "opcname", "opcnamespace").await?;

    // --- Process and Assemble in Rust ---
    let mut types_map: HashMap<u32, Type> = HashMap::new();

    for row in type_rows {
        let oid: u32 = row.get("oid");
        let name: String = row.get("name");

        let info = TypeInfo {
            oid,
            name: name.clone(),
            schema: row.get("schema_name"),
            owner: row.get("owner"),
            acl: row.get("acl"),
            comment: row.get("comment"),
            is_user_defined: oid > last_system_oid,
            is_from_extension: row.get("is_from_extension"),
            array_type_oid: row.get::<_, u32>("array_type_oid").checked_sub(0),
        };

        let type_char: i8 = row.get("typtype");
        let new_type = match type_char as u8 as char {
            'b' => {
                let receive_fn: Option<String> = row.get("receive_fn");
                let send_fn: Option<String> = row.get("send_fn");
                Type::Base(BaseType {
                    info,
                    internal_length: row.get("internal_length"),
                    is_passed_by_value: row.get("is_passed_by_value"),
                    alignment: row.get::<_, i8>("typalign") as u8 as char,
                    storage: row.get::<_, i8>("typstorage") as u8 as char,
                    category: row.get::<_, i8>("typcategory") as u8 as char,
                    is_preferred: row.get("is_preferred"),
                    default_value: row.get("default_value"),
                    element_type_oid: row.get::<_, u32>("element_type_oid").checked_sub(0),
                    delimiter: row.get::<_, i8>("delimiter") as u8 as char,
                    is_collatable: row.get("is_collatable"),
                    input_fn: row.get("input_fn"),
                    output_fn: row.get("output_fn"),
                    receive_fn: if receive_fn.as_deref() == Some("-") {
                        None
                    } else {
                        receive_fn
                    },
                    send_fn: if send_fn.as_deref() == Some("-") {
                        None
                    } else {
                        send_fn
                    },
                    typmod_in_fn: row.get("typmod_in_fn"),
                    typmod_out_fn: row.get("typmod_out_fn"),
                    analyze_fn: row.get("analyze_fn"),
                })
            }
            'c' => Type::Composite(CompositeType {
                info,
                attributes: Vec::new(), // Will be filled in below
                class_oid: row.get("class_oid"),
            }),
            'd' => {
                let collation_oid: u32 = row.get("collation_oid");
                Type::Domain(Domain {
                    info,
                    base_type: row.get("base_type"),
                    collation: if collation_oid > 0 {
                        // For built-in collations (pg_catalog schema), return just the name
                        // For user-defined collations, return the fully qualified name
                        if let Some(qualified_name) = collations_map.get(&collation_oid) {
                            if qualified_name.starts_with("\"pg_catalog\".") {
                                // Extract just the collation name from "pg_catalog"."name"
                                let name_part = qualified_name.split('.').nth(1);
                                name_part.map(|s| s.to_string())
                            } else {
                                Some(qualified_name.clone())
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    },
                    not_null: row.get("not_null"),
                    default: row.get("default_value"),
                    constraints: Vec::new(), // Will be filled in below
                })
            }
            'e' => Type::Enum(EnumType {
                info,
                values: Vec::new(), // Will be filled in below
            }),
            'r' => Type::Range(RangeType {
                info,
                subtype: String::new(), // Will be filled in below
                subtype_opclass: String::new(),
                collation: None,
                canonical_fn: None,
                subtype_diff_fn: None,
                multirange_type_oid: None,
            }),
            'p' => Type::Pseudo(PseudoType { info }),
            _ => continue, // Ignore other types like internal array types handled by `typarray`
        };
        types_map.insert(oid, new_type);
    }

    // Populate composite attributes
    for row in attribute_rows {
        let class_oid: u32 = row.get("class_oid");
        if let Some(Type::Composite(c)) = types_map.values_mut().find(|t| match t {
            Type::Composite(c) => c.class_oid == class_oid,
            _ => false,
        }) {
            let collation_oid: u32 = row.get("collation_oid");
            c.attributes.push(Attribute {
                name: row.get("name"),
                type_name: row.get("type_name"),
                collation: if collation_oid > 0 {
                    collations_map.get(&collation_oid).cloned()
                } else {
                    None
                },
            });
        }
    }

    // Populate enum values
    for row in enum_rows {
        let type_oid: u32 = row.get("enumtypid");
        if let Some(Type::Enum(e)) = types_map.get_mut(&type_oid) {
            e.values.push(EnumValue {
                oid: row.get("oid"),
                label: row.get("enumlabel"),
            });
        }
    }

    // Populate range details
    for row in range_rows {
        let oid: u32 = row.get("oid");
        if let Some(Type::Range(r)) = types_map.get_mut(&oid) {
            let subtype_opclass_oid: u32 = row.get("subtype_opclass_oid");
            let collation_oid: u32 = row.get("collation_oid");
            let canonical_fn: Option<String> = row.get("canonical_fn");
            let subtype_diff_fn: Option<String> = row.get("subtype_diff_fn");

            r.subtype = row.get("subtype");
            r.subtype_opclass = opclasses_map
                .get(&subtype_opclass_oid)
                .cloned()
                .unwrap_or_default();
            r.collation = if collation_oid > 0 {
                // For built-in collations (pg_catalog schema), return just the name
                // For user-defined collations, return the fully qualified name
                if let Some(qualified_name) = collations_map.get(&collation_oid) {
                    if qualified_name.starts_with("\"pg_catalog\".") {
                        // Extract just the collation name from "pg_catalog"."name"
                        let name_part = qualified_name.split('.').nth(1);
                        name_part.map(|s| s.to_string())
                    } else {
                        Some(qualified_name.clone())
                    }
                } else {
                    None
                }
            } else {
                None
            };
            r.canonical_fn = if canonical_fn.as_deref() == Some("-") {
                None
            } else {
                canonical_fn
            };
            r.subtype_diff_fn = if subtype_diff_fn.as_deref() == Some("-") {
                None
            } else {
                subtype_diff_fn
            };
            r.multirange_type_oid = row.get::<_, u32>("multirange_type_oid").checked_sub(0);
        }
    }

    // Populate domain constraints
    for row in domain_constraint_rows {
        let domain_oid: u32 = row.get("contypid");
        if let Some(Type::Domain(d)) = types_map.get_mut(&domain_oid) {
            d.constraints.push(DomainConstraint {
                oid: row.get("oid"),
                name: row.get("conname"),
                definition: row.get("definition"),
                not_valid: row.get("not_valid"),
            });
        }
    }

    // --- Final Filtering ---
    let all_types: Vec<Type> = types_map.into_values().collect();
    if include_predefined {
        Ok(all_types)
    } else {
        let dumpable_types = all_types
            .into_iter()
            .filter(|t| match t {
                Type::Base(t) => {
                    t.info.is_user_defined
                        && !t.info.is_from_extension
                        && t.element_type_oid.is_none()
                }
                Type::Composite(t) => t.info.is_user_defined && !t.info.is_from_extension,
                Type::Domain(t) => t.info.is_user_defined && !t.info.is_from_extension,
                Type::Enum(t) => t.info.is_user_defined && !t.info.is_from_extension,
                Type::Range(t) => t.info.is_user_defined && !t.info.is_from_extension,
                Type::Pseudo(_) => false, // Never dump pseudo-types
            })
            .collect();
        Ok(dumpable_types)
    }
}
