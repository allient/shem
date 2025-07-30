use crate::model::trigger::Trigger;
use common::error::Result;
use std::collections::HashMap;
use tokio_postgres::GenericClient;

pub async fn introspect_triggers<C: GenericClient>(
    client: &C,
    table_oids: &[u32],
) -> Result<HashMap<u32, Vec<Trigger>>> {
    if table_oids.is_empty() {
        return Ok(HashMap::new());
    }

    tracing::debug!("Introspecting triggers for table OIDs: {:?}", table_oids);

    // This query fetches all triggers for the given tables.
    let query = r#"
        SELECT
            t.oid,
            t.tgname AS name,
            t.tgrelid AS table_oid,
            c.relname AS table_name,
            n.nspname AS schema_name,
            -- This function gives us the complete CREATE statement
            pg_get_triggerdef(t.oid) AS definition,
            t.tgconstraint <> 0 AS is_constraint,
            obj_description(t.oid, 'pg_trigger') as comment,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = t.oid AND d.classid = 'pg_trigger'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE t.tgrelid = ANY($1)
        AND NOT t.tgisinternal; -- Exclude internal triggers
    "#;
    let rows = client.query(query, &[&table_oids]).await?;

    tracing::debug!("Found {} trigger rows", rows.len());

    let mut triggers_map: HashMap<u32, Vec<Trigger>> = HashMap::new();
    for row in rows {
        let table_oid: u32 = row.get("table_oid");
        let trigger_name: String = row.get("name");
        let table_name: String = row.get("table_name");
        tracing::debug!(
            "Found trigger '{}' on table '{}' (OID: {})",
            trigger_name,
            table_name,
            table_oid
        );
        triggers_map.entry(table_oid).or_default().push(Trigger {
            oid: row.get("oid"),
            name: trigger_name,
            table_oid,
            table_name,
            schema: row.get("schema_name"),
            definition: row.get("definition"),
            is_constraint: row.get("is_constraint"),
            comment: row.get("comment"),
            is_from_extension: row.get("is_from_extension"),
        });
    }

    Ok(triggers_map)
}
