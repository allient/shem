use crate::{
    helpers::{get_last_system_oid, quote_ident},
    model::operator::{OpClass, OpFamily, Operator},
};
use common::error::Result;
use tokio_postgres::GenericClient;

pub async fn introspect_operators<C: GenericClient>(client: &C) -> Result<Vec<Operator>> {
    let last_system_oid = get_last_system_oid(client).await?;

    // This comprehensive query fetches all parts needed to reconstruct the CREATE OPERATOR statement.
    let query = r#"
        SELECT
            o.oid,
            o.oprname AS name,
            n.nspname AS schema_name,
            pg_get_userbyid(o.oprowner) AS owner,
            o.oprcode::regprocedure::text AS function_name,
            o.oprleft::regtype::text AS left_arg_type,
            o.oprright::regtype::text AS right_arg_type,
            o.oprcom::regoperator::text AS commutator,
            o.oprnegate::regoperator::text AS negator,
            o.oprrest::regprocedure::text AS restrict_fn,
            o.oprjoin::regprocedure::text AS join_fn,
            o.oprcanmerge AS can_merge,
            o.oprcanhash AS can_hash,
            obj_description(o.oid, 'pg_operator') AS comment,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = o.oid AND d.classid = 'pg_operator'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_operator o
        JOIN pg_namespace n ON o.oprnamespace = n.oid
        WHERE o.oid > $1;
    "#;

    let rows = client.query(query, &[&last_system_oid]).await?;

    let operators = rows
        .into_iter()
        .map(|row| {
            let schema: String = row.get("schema_name");
            let name: String = row.get("name");

            // --- Reconstruct the CREATE OPERATOR statement ---
            let mut def = format!(
                "CREATE OPERATOR {}.{} (\n",
                quote_ident(&schema),
                quote_ident(&name)
            );
            def.push_str(&format!(
                "    FUNCTION = {}\n",
                row.get::<_, String>("function_name")
            ));

            let left_arg: Option<String> = row.get("left_arg_type");
            if let Some(la) = left_arg.filter(|s| s != "-") {
                def.push_str(&format!("    , LEFTARG = {}\n", la));
            }

            let right_arg: Option<String> = row.get("right_arg_type");
            if let Some(ra) = right_arg.filter(|s| s != "-") {
                def.push_str(&format!("    , RIGHTARG = {}\n", ra));
            }

            let commutator: Option<String> = row.get("commutator");
            if let Some(c) = commutator.filter(|s| s != "0") {
                def.push_str(&format!("    , COMMUTATOR = {}\n", c));
            }

            let negator: Option<String> = row.get("negator");
            if let Some(n) = negator.filter(|s| s != "0") {
                def.push_str(&format!("    , NEGATOR = {}\n", n));
            }

            let restrict_fn: Option<String> = row.get("restrict_fn");
            if let Some(r) = restrict_fn.filter(|s| s != "-") {
                def.push_str(&format!("    , RESTRICT = {}\n", r));
            }

            let join_fn: Option<String> = row.get("join_fn");
            if let Some(j) = join_fn.filter(|s| s != "-") {
                def.push_str(&format!("    , JOIN = {}\n", j));
            }

            if row.get("can_merge") {
                def.push_str("    , MERGES\n");
            }
            if row.get("can_hash") {
                def.push_str("    , HASHES\n");
            }

            // Remove leading comma from first line if it exists
            if let Some(idx) = def.find(',') {
                if def[..idx].contains("FUNCTION") {
                    def.remove(idx);
                }
            }
            def.push_str(");");

            Operator {
                oid: row.get("oid"),
                name,
                schema,
                owner: row.get("owner"),
                definition: def,
                comment: row.get("comment"),
                is_from_extension: row.get("is_from_extension"),
            }
        })
        .collect();

    Ok(operators)
}
pub async fn introspect_op_families<C: GenericClient>(client: &C) -> Result<Vec<OpFamily>> {
    let last_system_oid = get_last_system_oid(client).await?;

    let query = r#"
        SELECT
            of.oid,
            of.opfname AS name,
            n.nspname AS schema,
            pg_get_userbyid(of.opfowner) AS owner,
            am.amname AS index_method,
            obj_description(of.oid, 'pg_opfamily') AS comment,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = of.oid AND d.classid = 'pg_opfamily'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_opfamily of
        JOIN pg_namespace n ON of.opfnamespace = n.oid
        JOIN pg_am am ON of.opfmethod = am.oid
        WHERE of.oid > $1;
    "#;

    let rows = client.query(query, &[&last_system_oid]).await?;

    let op_families = rows
        .into_iter()
        .map(|row| OpFamily {
            oid: row.get("oid"),
            name: row.get("name"),
            schema: row.get("schema"),
            owner: row.get("owner"),
            index_method: row.get("index_method"),
            comment: row.get("comment"),
            is_from_extension: row.get("is_from_extension"),
        })
        .collect();

    Ok(op_families)
}
pub async fn introspect_op_classes<C: GenericClient>(client: &C) -> Result<Vec<OpClass>> {
    let last_system_oid = get_last_system_oid(client).await?;

    // This query fetches the data needed to manually construct the CREATE OPERATOR CLASS statement
    let query = r#"
        SELECT
            oc.oid,
            oc.opcname AS name,
            n.nspname AS schema,
            pg_get_userbyid(oc.opcowner) AS owner,
            am.amname AS index_method,
            oc.opcintype::regtype::text AS data_type,
            oc.opcdefault AS is_default,
            obj_description(oc.oid, 'pg_opclass') AS comment,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = oc.oid AND d.classid = 'pg_opclass'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_opclass oc
        JOIN pg_namespace n ON oc.opcnamespace = n.oid
        JOIN pg_am am ON oc.opcmethod = am.oid
        WHERE oc.oid > $1;
    "#;

    let rows = client.query(query, &[&last_system_oid]).await?;

    let op_classes = rows
        .into_iter()
        .map(|row| {
            let schema: String = row.get("schema");
            let name: String = row.get("name");
            let index_method: String = row.get("index_method");
            let data_type: String = row.get("data_type");
            let is_default: bool = row.get("is_default");

            // Manually construct the CREATE OPERATOR CLASS statement
            let mut def = format!(
                "CREATE OPERATOR CLASS {}.{}\n",
                quote_ident(&schema),
                quote_ident(&name)
            );
            
            if is_default {
                def.push_str("    DEFAULT ");
            }
            
            def.push_str(&format!("FOR TYPE {} USING {};", data_type, index_method));

            OpClass {
                oid: row.get("oid"),
                name,
                schema,
                owner: row.get("owner"),
                definition: def,
                comment: row.get("comment"),
                is_from_extension: row.get("is_from_extension"),
            }
        })
        .collect();

    Ok(op_classes)
}
