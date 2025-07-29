use crate::model::subscription::Subscription;
use tokio_postgres::{Client, Error, GenericClient};

pub async fn introspect_subscriptions<C: GenericClient>(client: &C) -> Result<Vec<Subscription>, Error> {
    let query = r#"
        SELECT
            oid, subname AS name, pg_get_userbyid(subowner) AS owner,
            subconninfo AS connection_info,
            subpublications AS publication_names,
            subslotname AS slot_name,
            subenabled AS is_enabled,
            obj_description(oid, 'pg_subscription') AS comment,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = sub.oid AND d.classid = 'pg_subscription'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_subscription sub;
    "#;
    let rows = client.query(query, &[]).await?;
    let subscriptions = rows.into_iter().map(|row| Subscription {
        oid: row.get("oid"),
        name: row.get("name"),
        owner: row.get("owner"),
        connection_info: row.get("connection_info"),
        publication_names: row.get("publication_names"),
        slot_name: row.get("slot_name"),
        is_enabled: row.get("is_enabled"),
        comment: row.get("comment"),
        is_from_extension: row.get("is_from_extension"),
    }).collect();
    Ok(subscriptions)
}