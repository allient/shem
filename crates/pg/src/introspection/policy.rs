use crate::{
    helpers::get_last_system_oid,
    model::policy::{Policy, PolicyCommand},
};
use common::error::Result;
use std::collections::HashMap;
use tokio_postgres::GenericClient;

// Helper to fetch OID -> name mapping for all roles
async fn get_roles_oid_map<C: GenericClient>(client: &C) -> Result<HashMap<u32, String>> {
    let rows = client
        .query("SELECT oid, rolname FROM pg_roles", &[])
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get("oid"), row.get("rolname")))
        .collect())
}

// This function now fetches all policies efficiently and assembles them.
pub async fn introspect_policies<C: GenericClient>(
    client: &C,
    include_predefined: bool,
) -> Result<Vec<Policy>> {
    // 1. Check if datlastsysoid column exists in pg_database
    let last_system_oid = get_last_system_oid(client).await?;

    tracing::debug!("Last system OID (policies): {}", last_system_oid);

    let roles_map = get_roles_oid_map(client).await?; // Helper to get OID -> name

    // --- QUERY 1: Fetch all policies and their properties ---
    let policies_query = r#"
        SELECT 
            p.oid, p.polname AS name,
            c.oid AS table_oid, c.relname AS table_name, n.nspname AS schema_name,
            p.polpermissive AS permissive, p.polroles AS role_oids,
            p.polcmd,
            pg_get_expr(p.polqual, p.polrelid) AS using_expression,
            pg_get_expr(p.polwithcheck, p.polrelid) AS check_expression,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = p.oid AND d.classid = 'pg_policy'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_policy p
        JOIN pg_class c ON p.polrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid;
    "#;
    let policy_rows = client.query(policies_query, &[]).await?;

    // --- QUERY 2: Fetch all tables with Row Level Security enabled ---
    let rls_enabled_query = "SELECT oid FROM pg_class WHERE relrowsecurity";
    let rls_enabled_rows = client.query(rls_enabled_query, &[]).await?;
    let rls_enabled_oids: HashMap<u32, bool> = rls_enabled_rows
        .into_iter()
        .map(|r| (r.get("oid"), true))
        .collect();

    // --- Assemble final Vec<Policy> ---
    let mut all_policies = Vec::new();

    // First, create entries for the policies themselves
    for row in policy_rows {
        let oid: u32 = row.get("oid");
        let role_oids: Vec<u32> = row.get("role_oids");

        let roles = if role_oids.is_empty() || (role_oids.len() == 1 && role_oids[0] == 0) {
            vec!["PUBLIC".to_string()] // OID 0 in polroles means PUBLIC
        } else {
            role_oids
                .iter()
                .filter_map(|&oid| roles_map.get(&oid).cloned())
                .collect()
        };

        let command_char: i8 = row.get("polcmd");
        let policy_command = match command_char as u8 as char {
            'r' => PolicyCommand::Select,
            'a' => PolicyCommand::Insert,
            'w' => PolicyCommand::Update,
            'd' => PolicyCommand::Delete,
            '*' => PolicyCommand::All,
            _ => PolicyCommand::All,
        };

        all_policies.push(Policy {
            oid,
            name: Some(row.get("name")),
            table_oid: row.get("table_oid"),
            table_name: row.get("table_name"),
            schema: row.get("schema_name"),
            command: policy_command,
            permissive: row.get("permissive"),
            roles,
            using: row.get("using_expression"),
            check: row.get("check_expression"),
            is_user_defined: oid > last_system_oid,
            is_from_extension: row.get("is_from_extension"),
        });
    }

    // Now, find all tables that have RLS enabled but might not have explicit policies yet.
    // We create a special "Policy" entry for them.
    for (table_oid, _) in rls_enabled_oids {
        // Check if we already have a policy for this table. If so, RLS is implicitly handled.
        if all_policies.iter().any(|p| p.table_oid == table_oid) {
            continue;
        }

        // If not, we need a dedicated "ENABLE ROW LEVEL SECURITY" entry.
        // We find the table's info from the policies list (or would fetch it if needed).
        if let Some(policy_for_table) = all_policies.iter().find(|p| p.table_oid == table_oid) {
            all_policies.push(Policy {
                oid: 0,     // No real OID for this conceptual object
                name: None, // This signifies ENABLE RLS
                table_oid,
                table_name: policy_for_table.table_name.clone(),
                schema: policy_for_table.schema.clone(),
                command: PolicyCommand::All, // Not applicable
                permissive: false,           // Not applicable
                roles: Vec::new(),           // Not applicable
                using: None,                 // Not applicable
                check: None,                 // Not applicable
                is_user_defined: true,       // The act of enabling RLS is user-defined
                is_from_extension: false,    // Cannot be from an extension
            });
        }
    }

    // --- Final Filtering ---
    if include_predefined {
        Ok(all_policies)
    } else {
        let dumpable_policies = all_policies
            .into_iter()
            .filter(|p| p.is_user_defined && !p.is_from_extension)
            .collect();
        Ok(dumpable_policies)
    }
}
