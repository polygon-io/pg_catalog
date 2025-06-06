use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::error::Result;

/// Register a new table in the given catalog and schema.
/// Creates the catalog or schema if it does not exist.
pub fn register_table(
    ctx: &SessionContext,
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    columns: Vec<(&str, DataType)>,
) -> Result<()> {
    let catalog: Arc<dyn CatalogProvider> = if let Some(cat) = ctx.catalog(catalog_name) {
        cat
    } else {
        let cat = Arc::new(MemoryCatalogProvider::new());
        ctx.register_catalog(catalog_name, cat.clone());
        cat
    };

    let schema: Arc<dyn SchemaProvider> = if let Some(sch) = catalog.schema(schema_name) {
        sch
    } else {
        let sch = Arc::new(MemorySchemaProvider::new());
        catalog.register_schema(schema_name, sch.clone())?;
        sch
    };

    let fields: Vec<Field> = columns
        .into_iter()
        .map(|(name, dt)| Field::new(name, dt, false))
        .collect();
    let table_schema = Arc::new(Schema::new(fields));

    // use an empty record batch so the table exists
    let batch = RecordBatch::new_empty(table_schema.clone());
    let table = MemTable::try_new(table_schema, vec![vec![batch]])?;

    schema.register_table(table_name.to_string(), Arc::new(table))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[tokio::test]
    async fn test_register_table() -> Result<()> {
        let mut config = datafusion::execution::context::SessionConfig::new()
            .with_default_catalog_and_schema("crm", "crm");
        let ctx = SessionContext::new_with_config(config);

        register_table(
            &ctx,
            "crm",
            "crm",
            "mytable",
            vec![("id", DataType::Int32), ("name", DataType::Utf8)],
        )?;

        let catalog = ctx.catalog("crm").unwrap();
        let schema = catalog.schema("crm").unwrap();
        assert!(schema.table_names().contains(&"mytable".to_string()));

        let df = ctx.sql("select count(*) from crm.mytable").await?;
        let batches = df.collect().await?;
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 0);
        Ok(())
    }
}
