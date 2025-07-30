use crate::{helpers::get_last_system_oid, model::event_trigger::EventTrigger};
use common::error::Result;
use tokio_postgres::GenericClient;

pub async fn introspect_event_triggers<C: GenericClient>(client: &C) -> Result<Vec<EventTrigger>> {
    // Check if datlastsysoid column exists in pg_database
    let last_system_oid = get_last_system_oid(client).await?;

    // This query fetches all user-defined event triggers.
    let query = r#"
        SELECT
            e.oid,
            e.evtname AS name,
            pg_get_userbyid(e.evtowner) AS owner,
            obj_description(e.oid, 'pg_event_trigger') AS comment,
            -- We reconstruct the definition here, as there's no single pg_get_* function for it.
            'CREATE EVENT TRIGGER ' || quote_ident(e.evtname) ||
            ' ON ' || e.evtevent ||
            CASE WHEN e.evttags IS NOT NULL
                THEN ' WHEN TAG IN (' || (SELECT string_agg(quote_literal(t), ', ') FROM unnest(e.evttags) t) || ')'
                ELSE ''
            END ||
            ' EXECUTE FUNCTION ' || e.evtfoid::regprocedure::text || '();'
            AS definition,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = e.oid AND d.classid = 'pg_event_trigger'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_event_trigger e
        WHERE e.oid > $1;
    "#;
    let rows = client.query(query, &[&last_system_oid]).await?;

    let mut event_triggers = Vec::new();
    for row in rows {
        event_triggers.push(EventTrigger {
            oid: row.get("oid"),
            name: row.get("name"),
            owner: row.get("owner"),
            definition: row.get("definition"),
            comment: row.get("comment"),
            is_from_extension: row.get("is_from_extension"),
        });
    }

    Ok(event_triggers)
}
