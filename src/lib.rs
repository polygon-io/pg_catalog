// Library module for pg_catalog functionality.
// Exposes modules so the crate can be used as a library as well as a binary.

pub mod clean_duplicate_columns;
pub mod db_table;
pub mod logical_plan_rules;
pub mod pg_catalog_helpers;
pub mod register_table;
pub mod replace;
pub mod replace_any_group_by;
pub mod router;
pub mod scalar_to_cte;
pub mod server;
pub mod session;
pub mod user_functions;
// Re-export all public functions from pg_catalog_helpers for convenience.
pub use pg_catalog_helpers::*;
// Re-export commonly used functions at crate root for convenience.
pub use router::{is_catalog_query, qualify_catalog_tables, dispatch_query};
pub use server::start_server;
pub use session::{get_base_session_context, setup_context, execute_sql, rewrite_filters};
