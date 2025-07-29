use crate::model::conversion::Conversion;
use tokio_postgres::{Client, Error, GenericClient};

pub async fn introspect_conversions<C: GenericClient>(
    client: &C,
) -> Result<Vec<Conversion>, Error> {
    let last_system_oid = get_last_system_oid(client).await?; // Assuming helper exists
    let query = r#"
        SELECT
            c.oid, c.conname AS name, n.nspname AS schema_name, pg_get_userbyid(c.conowner) AS owner,
            pg_encoding_to_char(c.conforencoding) AS for_encoding,
            pg_encoding_to_char(c.contoencoding) AS to_encoding,
            c.conproc::regprocedure::text AS function_name,
            c.condefault AS is_default,
            obj_description(c.oid, 'pg_conversion') AS comment,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = c.oid AND d.classid = 'pg_conversion'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_conversion c
        JOIN pg_namespace n ON c.connamespace = n.oid
        WHERE c.oid > $1;
    "#;
    let rows = client.query(query, &[&last_system_oid]).await?;
    let conversions = rows
        .into_iter()
        .map(|row| Conversion {
            oid: row.get("oid"),
            name: row.get("name"),
            schema: row.get("schema_name"),
            owner: row.get("owner"),
            for_encoding: row.get("for_encoding"),
            to_encoding: row.get("to_encoding"),
            function_name: row.get("function_name"),
            is_default: row.get("is_default"),
            comment: row.get("comment"),
            is_from_extension: row.get("is_from_extension"),
        })
        .collect();
    Ok(conversions)
}
