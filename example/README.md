# Example Usage

This `pg_catalog_example` crate shows how to route SQL statements between an in-memory SQLite
connection and the `pg_catalog` emulation provided by this repository.

## Building

```bash
cargo build
```

## Running

Pass the SQL query as an argument to `cargo run`:

```bash
cargo run -- "SELECT name FROM users ORDER BY id"
```

The above query is executed against the embedded SQLite database.

Queries that reference `pg_catalog` are handled by the catalog layer:

```bash
cargo run -- "SELECT datname FROM pg_catalog.pg_database LIMIT 1"
```

