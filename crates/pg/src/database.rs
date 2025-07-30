use crate::introspection::{
    collation::introspect_collations,
    conversion::introspect_conversions,
    event_trigger::introspect_event_triggers,
    extension::introspect_extensions,
    fdw::{introspect_fdws, introspect_servers},
    global::{introspect_roles, introspect_tablespaces},
    operator::{introspect_op_classes, introspect_op_families, introspect_operators},
    policy::introspect_policies,
    publication::{introspect_publication_tables, introspect_publications},
    relation::introspect_relations_unified,
    routine::introspect_routines,
    rule::introspect_rules,
    schema::introspect_named_schemas,
    sequence::introspect_sequences,
    subscription::introspect_subscriptions,
    trigger::introspect_triggers,
    types::introspect_types,
};
use crate::model::{
    collation::Collation,
    conversion::Conversion,
    event_trigger::EventTrigger,
    extension::Extension,
    fdw::{ForeignDataWrapper, Server},
    global::{Role, Tablespace},
    operator::{OpClass, OpFamily, Operator},
    policy::Policy,
    publication::{Publication, PublicationTable},
    relation::{MaterializedView, Relation, Table, View},
    routine::{Aggregate, Function, Procedure, Routine},
    rule::Rule,
    schema::Schema,
    sequence::Sequence,
    subscription::Subscription,
    trigger::Trigger,
    types::{BaseType, CompositeType, Domain, EnumType, RangeType, Type},
};
use common::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_postgres::GenericClient;
use tracing::debug;
/// Represents a complete, in-memory snapshot of a PostgreSQL database's schema.
///
/// This struct is the root of the introspection model and the main result
/// produced by the introspection process.
/// Represents a complete, in-memory snapshot of a PostgreSQL database's schema.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DatabaseModel {
    // --- Global Objects (Cluster-wide) ---
    pub roles: HashMap<String, Role>,
    pub tablespaces: HashMap<String, Tablespace>,

    // --- Database-Scoped Objects (Live inside one database) ---
    pub name: Option<String>,
    pub schemas: HashMap<String, Schema>,
    pub extensions: HashMap<String, Extension>,
    pub publications: HashMap<String, Publication>,
    pub subscriptions: HashMap<String, Subscription>,
    pub foreign_data_wrappers: HashMap<String, ForeignDataWrapper>,
    pub servers: HashMap<String, Server>,
    pub event_triggers: HashMap<String, EventTrigger>,

    // --- Schema-Scoped Objects (Live inside a schema) ---
    pub relations: HashMap<String, Relation>,
    pub sequences: HashMap<String, Sequence>,
    pub types: HashMap<String, Type>,
    pub routines: HashMap<String, Routine>,
    pub collations: HashMap<String, Collation>,
    pub conversions: HashMap<String, Conversion>,
    pub operators: HashMap<String, Operator>,
    pub op_classes: HashMap<String, OpClass>,
    pub op_families: HashMap<String, OpFamily>,

    // --- Relationship Objects ---
    /// Links Publications to the Tables they contain. Stored as a Vec as it's a list of relations.
    pub publication_tables: Vec<PublicationTable>,
}

// The `#[derive(Default)]` gives you `Database::default()`.
// Your manual `new()` implementation should be removed.
impl DatabaseModel {
    pub fn with_name(name: String) -> Self {
        Self {
            name: Some(name),
            ..Self::default() // Use the derived default constructor
        }
    }

    pub async fn introspect_database_model<C>(client: &C) -> Result<Self>
    where
        C: GenericClient + Sync,
    {
        let mut schema = DatabaseModel::default();

        // Independent Objects (Standalone)

        // Introspect roles
        // Purpose: Manage authentication and permissions.
        // CREATE ROLE analyst WITH LOGIN PASSWORD 'secure123';
        // GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst;
        let roles = introspect_roles(&*client, false).await?;
        for role in roles {
            schema.roles.insert(role.name.clone(), role);
        }

        // Introspect extensions
        let extensions = introspect_extensions(&*client, false).await?;
        for ext in extensions {
            schema.extensions.insert(ext.name.clone(), ext);
        }

        // Introspect named schemas
        // Purpose: Namespace to organize objects (tables, functions, etc.).
        let named_schemas = introspect_named_schemas(&*client, false).await?;
        for named_schema in named_schemas {
            schema
                .named_schemas
                .insert(named_schema.name.clone(), named_schema);
        }

        // Introspect collations
        //Purpose: Define string sorting/rules (e.g., case-insensitive comparison).
        let collations = introspect_collations(&*client, false).await?;
        for collation in collations {
            schema.collations.insert(collation.name.clone(), collation);
        }

        // Introspect tablespaces
        // Purpose: Control physical storage locations on disk.
        let tablespaces = introspect_tablespaces(&*client).await?;
        for tablespace in tablespaces {
            schema
                .tablespaces
                .insert(tablespace.name.clone(), tablespace);
        }

        // Introspect types
        // Introspect enums
        //Purpose: Define a static set of values (e.g., statuses, categories).
        // Introspect domains
        // Purpose: Create a custom type with constraints (e.g., positive integers).

        // Introspect base types
        // Purpose: Fundamental types like INTEGER, TEXT, JSONB.

        // Introspect composite types
        // Purpose: Combine multiple base types (e.g., address with street, city, state).

        // Introspect range types separately for detailed information
        // Purpose: Represent a range of values (e.g., dates, numbers).

        // Introspect multirange types
        // Purpose: Discontinuous ranges (PostgreSQL 14+).
        // SELECT '[2023-01-01, 2023-01-05), [2023-02-01, 2023-02-03)'::DATEMULTIRANGE;

        // Introspect array types
        // Purpose: Store arrays of any base/composite type.
        let postgres_types = introspect_types(&*client, false).await?;
        for postgres_type in postgres_types {
            let type_name = match &postgres_type {
                Type::Base(t) => t.info.name.clone(),
                Type::Composite(t) => t.info.name.clone(),
                Type::Domain(t) => t.info.name.clone(),
                Type::Enum(t) => t.info.name.clone(),
                Type::Range(t) => t.info.name.clone(),
                Type::Pseudo(t) => t.info.name.clone(),
            };
            schema.types.insert(type_name, postgres_type);
        }

        // Introspect sequences
        //Purpose: Generate auto-incrementing IDs.
        let sequences = introspect_sequences(&*client, false).await?;
        for seq in sequences {
            schema.sequences.insert(seq.name.clone(), seq);
        }

        // Semi-Independent Objects

        // Introspect tables
        // Purpose: Store data.
        // Introspect views
        // Purpose: Virtual table from a query.
        // Introspect materialized views
        // Constraint
        // 1. Make a single call to the unified function to get ALL relations.
        let all_relations: Vec<Relation> = introspect_relations_unified(client).await?;

        // 2. Iterate over the results and use a `match` to sort them into the correct HashMaps.
        for relation in all_relations {
            match relation {
                Relation::Table(table) => {
                    let key = if table.schema == "public" {
                        table.name.clone()
                    } else {
                        format!("{}.{}", table.schema, table.name)
                    };
                    debug!("Found Table: {:?}, using key: {}", table, key);
                    schema.tables.insert(key, table);
                }
                Relation::View(view) => {
                    let key = if view.schema == "public" {
                        view.name.clone()
                    } else {
                        format!("{}.{}", view.schema, view.name)
                    };
                    debug!("Found View: {:?}, using key: {}", view, key);
                    schema.views.insert(key, view);
                }
                Relation::MaterializedView(matview) => {
                    let key = if matview.schema == "public" {
                        matview.name.clone()
                    } else {
                        format!("{}.{}", matview.schema, matview.name)
                    };
                    debug!("Found Materialized View: {:?}, using key: {}", matview, key);
                    schema.materialized_views.insert(key, matview);
                }
                Relation::ForeignTable(ftable) => {
                    let key = format!(
                        "{}.{}",
                        ftable.schema.as_deref().unwrap_or("public"),
                        ftable.name
                    );
                    debug!("Found Foreign Table: {:?}, using key: {}", ftable, key);
                    schema.foreign_tables.insert(key, ftable);
                }
            }
        }

        // Introspect policies
        let policies = introspect_policies(&*client, false).await?;
        for policy in policies {
            // 1. Determine the correct, unique key for the HashMap.
            let key = if let Some(policy_name) = &policy.name {
                // This is a regular policy: "schema.table.policy"
                format!("{}.{}.{}", policy.schema, policy.table_name, policy_name)
            } else {
                // This is the special 'ENABLE ROW LEVEL SECURITY' object.
                // We create a synthetic, unique key for it.
                format!("{}.{}.__ENABLE_RLS__", policy.schema, policy.table_name)
            };

            debug!("Policy found: {:?}, using key: {}", policy, key);

            // 2. Insert into the HashMap using the new unique key.
            schema.policies.insert(key, policy);
        }

        // Introspect rules
        let rules = introspect_rules(&*client, false).await?;
        for rule in &rules {
            debug!("Rule: {:?}", rule);
        }
        for rule in rules {
            schema.rules.insert(rule.name.clone(), rule);
        }

        // Introspect publications
        let publications = introspect_publications(&*client).await?;
        for publication in publications {
            schema
                .publications
                .insert(publication.name.clone(), publication);
        }

        // Introspect publication tables
        let publication_tables = introspect_publication_tables(&*client).await?;
        for table in publication_tables {
            let key = format!("{}.{}", table.table_schema, table.table_name);
            schema.publication_tables.insert(key, table);
        }

        // Introspect routines
        let routines = introspect_routines(&*client).await?;
        for routine in routines {
            let (name, schema_name) = match &routine {
                Routine::Function(func) => (&func.name, &func.schema),
                Routine::Procedure(proc) => (&proc.name, &proc.schema),
                Routine::Aggregate(agg) => (&agg.name, &agg.schema),
            };

            let key = if schema_name == "public" {
                name.clone()
            } else {
                format!("{}.{}", schema_name, name)
            };
            schema.routines.insert(key, routine);
        }

        // Introspect triggers
        // Get the OIDs of all tables that can have triggers
        let table_oids: Vec<u32> = schema.tables.values().map(|t| t.oid).collect();
        let triggers_map = introspect_triggers(client, &table_oids).await?;

        // Distribute the fetched triggers into their parent Table objects and schema-level triggers.
        tracing::debug!("Triggers map: {:?}", triggers_map);
        for (_, table) in schema.tables.iter_mut() {
            tracing::debug!(
                "Checking table {} (OID: {}) for triggers",
                table.name,
                table.oid
            );
            if let Some(triggers_for_this_table) = triggers_map.get(&table.oid) {
                tracing::debug!(
                    "Found {} triggers for table {}",
                    triggers_for_this_table.len(),
                    table.name
                );
                // Clone the triggers into the table's `triggers` field.
                table.triggers = triggers_for_this_table.clone();

                // Also add triggers to the schema-level triggers HashMap
                for trigger in triggers_for_this_table {
                    schema
                        .triggers
                        .insert(trigger.name.clone(), trigger.clone());
                }
            } else {
                tracing::debug!("No triggers found for table {}", table.name);
            }
        }

        // Introspect event triggers
        let event_triggers = introspect_event_triggers(&*client).await?;
        for trigger in event_triggers {
            schema.event_triggers.insert(trigger.name.clone(), trigger);
        }

        // // Introspect servers
        // let servers = introspect_servers(&*client).await?;
        // for server in servers {
        //     schema.servers.insert(server.name.clone(), server);
        // }

        // // Introspect foreign tables
        // let foreign_tables = introspect_foreign_tables(&*client).await?;
        // for table in foreign_tables {
        //     schema.foreign_tables.insert(table.name.clone(), table);
        // }

        // // Introspect subscriptions
        // let subscriptions = introspect_subscriptions(&*client).await?;
        // for subscription in subscriptions {
        //     schema
        //         .subscriptions
        //         .insert(subscription.name.clone(), subscription);
        // }

        // // Introspect foreign data wrappers
        // let foreign_data_wrappers = introspect_foreign_data_wrappers(&*client).await?;
        // for fdw in foreign_data_wrappers {
        //     schema.foreign_data_wrappers.insert(fdw.name.clone(), fdw);
        // }

        Ok(schema)
    }
}
