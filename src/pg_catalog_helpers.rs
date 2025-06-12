use datafusion::error::Result as DFResult;
use serde::{Deserialize};
use std::collections::BTreeMap;
use datafusion::execution::context::SessionContext;
use datafusion::{    
    common::ScalarValue,
};

use std::sync::atomic::{AtomicI32, Ordering};

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnDef {
    #[serde(rename = "type")]
    pub col_type: String,
    pub nullable: bool,
}

static NEXT_OID: AtomicI32 = AtomicI32::new(50010);

fn map_type_to_oid(t: &str) -> i32 {
    match t.to_lowercase().as_str() {
        "int" | "integer" | "int4" => 23,
        "bigint" | "int8" => 20,
        "bool" | "boolean" => 16,
        _ => 25, // default to text
    }
}

pub async fn register_user_tables(
    ctx: &SessionContext,
    table_name: &str,
    columns: Vec<BTreeMap<String, ColumnDef>>,
) -> DFResult<()> {
    let df = ctx.sql("SELECT 1 FROM pg_catalog.pg_class WHERE relname=$relname")
        .await?.with_param_values(vec![
           ("relname", ScalarValue::from(table_name))
        ])?;

    if df.count().await? > 0 {
        log::info!("table already exists {:}?", table_name);
        return Ok(());
    }

    let table_oid = NEXT_OID.fetch_add(1, Ordering::SeqCst);
    let type_oid = NEXT_OID.fetch_add(1, Ordering::SeqCst);

    if ctx
        .sql(&format!(
            "SELECT 1 FROM pg_catalog.pg_class WHERE oid = {table_oid}"
        ))
        .await?
        .count()
        .await?
        == 0
    {
        let sql = format!(
            "INSERT INTO pg_catalog.pg_class \
                 (oid, relname, relnamespace, relkind, reltuples, reltype, relispartition) \
                 VALUES ({table_oid},'{}',2200,'r',0,{type_oid}, false)",
            table_name.replace('\'', "''")
        );
        ctx.sql(&sql).await?.collect().await?;
    }

    if ctx
        .sql(&format!(
            "SELECT 1 FROM pg_catalog.pg_type WHERE oid = {type_oid}"
        ))
        .await?
        .count()
        .await?
        == 0
    {
        let sql = format!(
            "INSERT INTO pg_catalog.pg_type \
                 (oid, typname, typrelid, typlen, typcategory) \
                 VALUES ({type_oid},'_{table_name}',{table_oid},-1,'C')"
        );
        ctx.sql(&sql).await?.collect().await?;
    }

    for (idx, col) in columns.iter().enumerate() {
        let (name, def) = col.iter().next().unwrap();
        let atttypid = map_type_to_oid(&def.col_type);
        let notnull = if def.nullable { "false" } else { "true" };
        let sql = format!(
            "INSERT INTO pg_catalog.pg_attribute \
                 (attrelid,attnum,attname,atttypid,atttypmod,attnotnull,attisdropped) \
                 VALUES ({table_oid},{},'{}',{atttypid},-1,{notnull},false)",
            idx + 1,
            name.replace('\'', "''")
        );
        ctx.sql(&sql).await?.collect().await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::get_base_session_context;

    #[tokio::test]
    async fn test_register_user_tables_dynamic() -> DFResult<()> {
        let (ctx, _) = get_base_session_context(
            &"pg_catalog_data/pg_schema".to_string(),
            "pgtry".to_string(),
            "public".to_string(),
        )
        .await?;

        let mut c1 = BTreeMap::new();
        c1.insert(
            "id".to_string(),
            ColumnDef { col_type: "int".to_string(), nullable: true },
        );
        let mut c2 = BTreeMap::new();
        c2.insert(
            "name".to_string(),
            ColumnDef { col_type: "text".to_string(), nullable: true },
        );

        register_user_tables(&ctx, "contacts", vec![c1, c2]).await?;

        let df = ctx
            .sql("SELECT relname FROM pg_catalog.pg_class WHERE relname='contacts'")
            .await?;
        assert_eq!(df.count().await?, 1);

        let df = ctx
            .sql(
                "SELECT attname FROM pg_catalog.pg_attribute \
                 WHERE attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname='contacts') \
                 ORDER BY attnum",
            )
            .await?;
        let batches = df.collect().await?;
        assert_eq!(batches[0].num_rows(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_register_user_tables_idempotent() -> DFResult<()> {
        let (ctx, _) = get_base_session_context(
            &"pg_catalog_data/pg_schema".to_string(),
            "pgtry".to_string(),
            "public".to_string(),
        )
        .await?;

        let mut c1 = BTreeMap::new();
        c1.insert(
            "id".to_string(),
            ColumnDef { col_type: "int".to_string(), nullable: true },
        );
        let mut c2 = BTreeMap::new();
        c2.insert(
            "name".to_string(),
            ColumnDef { col_type: "text".to_string(), nullable: true },
        );

        register_user_tables(&ctx, "contacts", vec![c1.clone(), c2.clone()]).await?;
        // call again to ensure idempotency
        register_user_tables(&ctx, "contacts", vec![c1, c2]).await?;

        let df = ctx
            .sql("SELECT relname FROM pg_catalog.pg_class WHERE relname='contacts'")
            .await?;
        assert_eq!(df.count().await?, 1);

        let df = ctx
            .sql(
                "SELECT attname FROM pg_catalog.pg_attribute \
                 WHERE attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname='contacts') \
                 ORDER BY attnum",
            )
            .await?;
        let batches = df.collect().await?;
        assert_eq!(batches[0].num_rows(), 2);
        Ok(())
    }
}
