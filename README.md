# pg_catalog_rs

**pg_catalog_rs** is a PostgreSQL system catalog compatibility layer for [Apache DataFusion](https://github.com/apache/datafusion).  
It enables PostgreSQL clients (e.g., DBeaver, pgAdmin, JDBC, ODBC, BI platforms) to connect to a DataFusion-backed database by emulating the behavior of PostgreSQL's `pg_catalog` schema.

This makes it possible to support tooling that expects PostgreSQL system metadata, even if the underlying engine is not Postgres.

Note: This is WIP heavily and API can change.

---

## Features
- Emulates core system tables from the `pg_catalog` schema:
  - `pg_class`
  - `pg_attribute`
  - `pg_namespace`
  - `pg_type`
  - `pg_proc`

- Supports PostgreSQL-specific built-in functions:
  - `pg_get_constraintdef(oid)`
  - `current_database()`
  - `has_schema_privilege(...)`

- Allows standard metadata queries used by:
  - DBeaver, DataGrip, and other GUI tools
  - BI tools using JDBC or ODBC
  - Postgres CLI (`psql`)

- Compatible with [`pgwire`](https://crates.io/crates/pgwire`) for Postgres wire protocol handling

- Fully in-memory and extensible via DataFusion APIs

---

## Installation

Add the crate from GitHub to your `Cargo.toml`:

```toml
[dependencies]
datafusion_pg_catalog = { git = "https://github.com/ybrs/pg_catalog" }
```

---

## Example Usage

Create a `SessionContext` preloaded with the catalog tables and register your own schema:
```rust
use std::collections::BTreeMap;
use pg_catalog_rs::{
    get_base_session_context, register_user_database, register_schema,
    register_user_tables, ColumnDef,
};

let (ctx, _log) = get_base_session_context(
    Some("pg_catalog_data/pg_schema"),
    "pgtry".to_string(),
    "public".to_string(),
).await?;

register_user_database(&ctx, "crm").await?;
register_schema(&ctx, "crm", "public").await?;

let mut cols = BTreeMap::new();
cols.insert(
    "id".to_string(),
    ColumnDef { col_type: "int".to_string(), nullable: false },
);
register_user_tables(&ctx, "crm", "public", "users", vec![cols]).await?;
```

Then you can run queries like:

```sql
SELECT oid, relname FROM pg_class WHERE relnamespace = 2200;
SELECT attname FROM pg_attribute WHERE attrelid = 12345;
```

Or use DBeaver/psql to introspect the schema.

## Query Routing with `dispatch_query`

When executing SQL, call [`dispatch_query`](src/router.rs) to automatically
route catalog queries to the internal handler while forwarding all other
statements to your application logic.

```rust
use pg_catalog_rs::router::dispatch_query;

let result = dispatch_query(&ctx, "SELECT * FROM pg_class", |ctx, sql| async move {
    ctx.sql(sql).await?.collect().await
}).await?;
```

---

## Integration

- Works with the [`pgwire`](https://github.com/sunng87/pgwire) crate for wire protocol emulation
- Can be combined with custom `MemTable` or real storage backends (Parquet, Arrow, etc.)
- Designed to be embedded in hybrid SQL engines or compatibility layers

---

## Embedding the pgwire Server

Launch a PostgreSQL-compatible endpoint using the provided helper:

```rust
use std::sync::Arc;
use pg_catalog_rs::{get_base_session_context, start_server};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (ctx, _log) = get_base_session_context(
        Some("pg_catalog_data/pg_schema"),
        "pgtry".to_string(),
        "public".to_string(),
    ).await?;

start_server(Arc::new(ctx), "127.0.0.1:5433", "pgtry", "public", None).await?;
    Ok(())
}
```

---

## Using with an Existing Server

If you already expose your own SQL server you can integrate `pg_catalog_rs` without
starting the bundled pgwire endpoint. Build a `SessionContext` and register your
tables, then call [`dispatch_query`](src/router.rs) from your handler. Catalog
queries will be executed internally and all other statements are forwarded.

```rust
use pg_catalog_rs::router::dispatch_query;

async fn handle_request(ctx: &SessionContext, sql: &str) -> DFResult<Vec<RecordBatch>> {
    let (batches, _schema) = dispatch_query(ctx, sql, None, None, |ctx, sql| async move {
        ctx.sql(sql).await?.collect().await
    }).await?;
    Ok(batches)
}
```

---

## Limitations

- ❌ No persistence — catalog is in-memory only
- 🟡 Partial function support (more can be added)
- 🟠 Schema reflection based on user-defined tables must be manually tracked
- ❌ No write-back support to catalog (read-only)

---

## Roadmap
- [ ] Hook into `CREATE TABLE` to auto-populate metadata
- [ ] Add missing catalog tables (`pg_index`, `pg_constraint`, etc.)
- [ ] Catalog persistence to disk or external store
- [ ] Enhanced type inference and function overloads

---

## Testing

Functional tests are written in Python and executed with [pytest](https://docs.pytest.org/).
After installing the dependencies from `requirements.txt`, run:

```bash
pytest
```

---

## License

Licensed under either of:

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT license](LICENSE-MIT)

at your option.

---

## Contributing

Pull requests are welcome. Contributions that improve PostgreSQL compatibility or support new clients are especially appreciated.

Before submitting a change please run the Rust unit tests and the Python functional
tests:

```bash
cargo test
pytest
```

---

## Acknowledgments

This project is inspired by:

- PostgreSQL's system catalog architecture
- Apache DataFusion as the execution backend
- [`pgwire`](https://github.com/sunng87/pgwire) for wire protocol support

---
