use crate::{
    helpers::pg_options_to_map,
    model::fdw::{ForeignDataWrapper, Server},
};
use common::error::Result;
use tokio_postgres::GenericClient;

pub async fn introspect_fdws<C: GenericClient>(client: &C) -> Result<Vec<ForeignDataWrapper>> {
    // The query now returns the raw text[] array
    let query = r#"
        SELECT
            oid, fdwname AS name, pg_get_userbyid(fdwowner) AS owner,
            fdwhandler::regproc::text AS handler, fdwvalidator::regproc::text AS validator,
            fdwoptions, -- Get the raw text[]
            fdwacl::text AS acl, obj_description(oid, 'pg_foreign_data_wrapper') AS comment,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = fdw.oid AND d.classid = 'pg_foreign_data_wrapper'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_foreign_data_wrapper fdw;
    "#;
    let rows = client.query(query, &[]).await?;
    let fdws = rows
        .into_iter()
        .map(|row| {
            // 1. Get the raw Option<Vec<String>>
            let options_vec: Option<Vec<String>> = row.get("fdwoptions");

            let handler: Option<String> = row.get("handler");
            let validator: Option<String> = row.get("validator");

            ForeignDataWrapper {
                oid: row.get("oid"),
                name: row.get("name"),
                owner: row.get("owner"),
                handler: if handler.as_deref() == Some("-") {
                    None
                } else {
                    handler
                },
                validator: if validator.as_deref() == Some("-") {
                    None
                } else {
                    validator
                },
                // 2. Parse it into the HashMap
                options: pg_options_to_map(options_vec),
                acl: row.get("acl"),
                comment: row.get("comment"),
                is_from_extension: row.get("is_from_extension"),
            }
        })
        .collect();
    Ok(fdws)
}

pub async fn introspect_servers<C: GenericClient>(client: &C) -> Result<Vec<Server>> {
    // The query now returns the raw text[] array
    let query = r#"
        SELECT
            s.oid, s.srvname AS name, pg_get_userbyid(s.srvowner) AS owner,
            s.srvfdw AS fdw_oid, f.fdwname AS fdw_name,
            s.srvtype, s.srvversion AS version,
            s.srvoptions, -- Get the raw text[]
            s.srvacl::text AS acl, obj_description(s.oid, 'pg_foreign_server') AS comment,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = s.oid AND d.classid = 'pg_foreign_server'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_foreign_server s
        JOIN pg_foreign_data_wrapper f ON s.srvfdw = f.oid;
    "#;
    let rows = client.query(query, &[]).await?;
    let servers = rows
        .into_iter()
        .map(|row| {
            // 1. Get the raw Option<Vec<String>>
            let options_vec: Option<Vec<String>> = row.get("srvoptions");

            Server {
                oid: row.get("oid"),
                name: row.get("name"),
                owner: row.get("owner"),
                fdw_oid: row.get("fdw_oid"),
                fdw_name: row.get("fdw_name"),
                server_type: row.get("server_type"),
                version: row.get("version"),
                // 2. Parse it into the HashMap
                options: pg_options_to_map(options_vec),
                acl: row.get("acl"),
                comment: row.get("comment"),
                is_from_extension: row.get("is_from_extension"),
            }
        })
        .collect();
    Ok(servers)
}
