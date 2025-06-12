// Entry point for the pg_catalog compatibility server.
// Parses CLI arguments, builds a SessionContext and starts the pgwire server.
// Provides a simple way to run the DataFusion-backed PostgreSQL emulator.



use std::env;
use std::sync::Arc;
// use arrow::util::pretty;
use datafusion_pg_catalog::server::start_server;
use datafusion_pg_catalog::session::get_base_session_context;
use datafusion_pg_catalog::register_table::register_table;
use arrow::datatypes::{DataType, Schema};
use datafusion_pg_catalog::router::dispatch_query;

async fn run() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!("Usage: {} schema_directory --default-catalog public --default-schema postgres", args[0]);
        std::process::exit(1);
    }

    let schema_path = &args[1];

    let default_catalog = args.iter()
        .position(|x| x == "--default-catalog")
        .and_then(|i| args.get(i + 1))
        .unwrap_or(&"datafusion".to_string())
        .clone();

    let default_schema = args.iter()
        .position(|x| x == "--default-schema")
        .and_then(|i| args.get(i + 1))
        .unwrap_or(&"public".to_string())
        .clone();

    let host = args.iter()
        .position(|x| x == "--host")
        .and_then(|i| args.get(i + 1))
        .unwrap_or(&"127.0.0.1".to_string())
        .clone();

    let port = args.iter()
        .position(|x| x == "--port")
        .and_then(|i| args.get(i + 1))
        .unwrap_or(&"5433".to_string())
        .clone();

    let address = format!("{}:{}", host, port);

    let capture_file = args.iter()
        .position(|x| x == "--capture")
        .and_then(|i| args.get(i + 1))
        .cloned();


    let (ctx, log) = get_base_session_context(schema_path, default_catalog.clone(), default_schema.clone()).await?;

    // register_table(
    //     &ctx,
    //     "crm",
    //     "crm",
    //     "users",
    //     vec![
    //         ("id", DataType::Int32, false),
    //         ("name", DataType::Utf8, true),
    //     ],
    // )?;

    // let _ = dispatch_query(&ctx, "SELECT 1", None, None, |_c, _q, _p, _t| {
    //     async { Ok((Vec::new(), Arc::new(Schema::empty()))) }
    // })
    // .await?;

    start_server(
        Arc::new(ctx),
        &address,
        &default_catalog,
        &default_schema,
        capture_file.map(|p| p.into()),
    )
    .await?;

    Ok(())
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if let Err(e) = run().await {
        eprintln!("server crashed: {:?}", e);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use datafusion_pg_catalog::router::dispatch_query;
    use datafusion::execution::context::SessionContext;
    use std::sync::Arc;
    use arrow::datatypes::Schema;

    #[tokio::test]
    async fn test_dispatch_in_main() -> anyhow::Result<()> {
        let ctx = SessionContext::new();
        dispatch_query(&ctx, "SELECT 1", None, None, |_c, _q, _p, _t| {
            async { Ok((Vec::new(), Arc::new(Schema::empty()))) }
        })
        .await?;
        Ok(())
    }
}
