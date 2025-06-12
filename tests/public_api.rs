use datafusion_pg_catalog::{dispatch_query, get_base_session_context, start_server};
use datafusion::execution::context::SessionContext;
use arrow::datatypes::Schema;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn test_dispatch_query_public() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let called = Arc::new(Mutex::new(false));
    let called_clone = called.clone();
    let handler = move |_ctx: &SessionContext, _sql: &str, _p, _t| {
        let called_clone = called_clone.clone();
        async move {
            *called_clone.lock().unwrap() = true;
            Ok((Vec::new(), Arc::new(Schema::empty())))
        }
    };

    dispatch_query(&ctx, "SELECT 1", None, None, handler).await?;
    assert!(*called.lock().unwrap());
    Ok(())
}

#[tokio::test]
async fn test_get_base_session_context_public() -> datafusion::error::Result<()> {
    let _ = get_base_session_context(
        &"pg_catalog_data/pg_schema".to_string(),
        "pgtry".to_string(),
        "public".to_string(),
    ).await?;
    Ok(())
}

#[test]
fn test_start_server_public() {
    let _f = start_server;
}
