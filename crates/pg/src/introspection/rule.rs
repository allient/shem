use crate::model::rule::Rule;
use common::error::Result;
use tokio_postgres::GenericClient;

// This function now fetches all rules efficiently.
pub async fn introspect_rules<C: GenericClient>(
    client: &C,
    include_predefined: bool, // Not very useful for rules, but for consistency
) -> Result<Vec<Rule>> {
    // 1. Check if datlastsysoid column exists in pg_database
    let column_exists_query = r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'pg_catalog' 
            AND table_name = 'pg_database' 
            AND column_name = 'datlastsysoid'
        ) as column_exists;
    "#;
    let column_exists_row = client.query_one(column_exists_query, &[]).await?;
    let datlastsysoid_exists: bool = column_exists_row.get("column_exists");

    // Get the last system OID to reliably distinguish system objects.
    let last_system_oid: u32 = if datlastsysoid_exists {
        let last_system_oid_row = client
            .query_one(
                "SELECT datlastsysoid FROM pg_database WHERE datname = current_database()",
                &[],
            )
            .await?;
        last_system_oid_row.get("datlastsysoid")
    } else {
        // Fallback for older PostgreSQL versions - use a reasonable default
        // This is the OID where user objects typically start
        16384
    };

    // --- QUERY: Fetch all non-implicit rules from the database ---
    let rules_query = r#"
        SELECT
            r.oid,
            r.rulename AS name,
            c.oid AS table_oid,
            c.relname AS table_name,
            n.nspname AS schema_name,
            pg_get_ruledef(r.oid) AS definition,
            obj_description(r.oid, 'pg_rewrite') AS comment,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = r.oid AND d.classid = 'pg_rewrite'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_rewrite r
        JOIN pg_class c ON r.ev_class = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE
            -- Exclude the implicit _RETURN rules created for views
            r.rulename != '_RETURN'
            -- We only care about rules on user-defined tables/views
            AND c.oid > $1;
    "#;
    let rule_rows = client.query(rules_query, &[&last_system_oid]).await?;

    // --- Assemble the final Vec<Rule> ---
    let mut all_rules = Vec::new();
    for row in rule_rows {
        let oid: u32 = row.get("oid");

        all_rules.push(Rule {
            oid,
            name: row.get("name"),
            table_oid: row.get("table_oid"),
            table_name: row.get("table_name"),
            schema: row.get("schema_name"),
            definition: row.get("definition"),
            comment: row.get("comment"),
            is_user_defined: true, // All rules we fetch here are considered user-defined
            is_from_extension: row.get("is_from_extension"),
        });
    }

    // --- Final Filtering ---
    if include_predefined {
        // Technically, no rules are "predefined", so this flag has little effect
        Ok(all_rules)
    } else {
        let dumpable_rules = all_rules
            .into_iter()
            .filter(|r| !r.is_from_extension)
            .collect();
        Ok(dumpable_rules)
    }
}
