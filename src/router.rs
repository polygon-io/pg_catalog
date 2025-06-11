use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow::datatypes::Schema;
use datafusion::execution::context::SessionContext;

use crate::session::{execute_sql, ClientOpts};

use sqlparser::ast::*;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

fn parse_search_path(path: &str) -> Vec<String> {
    let mut parts: Vec<String> = path
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.trim_matches('"').to_string())
        .collect();
    if !parts
        .iter()
        .any(|s| s.eq_ignore_ascii_case("pg_catalog"))
    {
        parts.insert(0, "pg_catalog".to_string());
    }
    parts
}

fn table_exists(ctx: &SessionContext, catalog: &str, schema: &str, table: &str) -> bool {
    ctx.catalog(catalog)
        .and_then(|c| c.schema(schema))
        .map(|s| s.table_exist(table))
        .unwrap_or(false)
}

fn qualify_table_name(name: &mut ObjectName, schema: &str) {
    if name.0.len() == 1 {
        let ident = name.0.remove(0);
        name.0.push(ObjectNamePart::Identifier(Ident::new(schema)));
        name.0.push(ident);
    }
}

fn qualify_factor(ctx: &SessionContext, factor: &mut TableFactor) {
    match factor {
        TableFactor::Table { name, .. } => {
            if let Some(schema) = resolve_schema(ctx, name) {
                if schema.eq_ignore_ascii_case("pg_catalog")
                    || schema.eq_ignore_ascii_case("information_schema")
                {
                    qualify_table_name(name, &schema);
                }
            }
        }
        TableFactor::Derived { subquery, .. } => qualify_query(ctx, subquery),
        TableFactor::NestedJoin { table_with_joins, .. } => {
            qualify_table_with_joins(ctx, table_with_joins)
        }
        _ => {}
    }
}

fn qualify_table_with_joins(ctx: &SessionContext, twj: &mut TableWithJoins) {
    qualify_factor(ctx, &mut twj.relation);
    for join in &mut twj.joins {
        qualify_factor(ctx, &mut join.relation);
    }
}

fn qualify_setexpr(ctx: &SessionContext, expr: &mut SetExpr) {
    match expr {
        SetExpr::Select(select) => {
            for twj in &mut select.from {
                qualify_table_with_joins(ctx, twj);
            }
        }
        SetExpr::Query(q) => qualify_query(ctx, q),
        SetExpr::SetOperation { left, right, .. } => {
            qualify_setexpr(ctx, left);
            qualify_setexpr(ctx, right);
        }
        _ => {}
    }
}

fn qualify_query(ctx: &SessionContext, query: &mut Query) {
    qualify_setexpr(ctx, &mut query.body);
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            qualify_query(ctx, &mut cte.query);
        }
    }
}

fn qualify_catalog_tables(ctx: &SessionContext, sql: &str) -> datafusion::error::Result<String> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)?;
    for stmt in &mut statements {
        if let Statement::Query(q) = stmt {
            qualify_query(ctx, q);
        }
    }
    Ok(statements
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; "))
}

fn resolve_schema(ctx: &SessionContext, name: &ObjectName) -> Option<String> {
    let state = ctx.state();
    let options = state.config_options();
    let default_catalog = &options.catalog.default_catalog;
    let default_schema = &options.catalog.default_schema;
    let search_path = options
        .extensions
        .get::<ClientOpts>()
        .map(|opts| parse_search_path(&opts.search_path))
        .unwrap_or_else(|| vec!["pg_catalog".to_string(), default_schema.clone()]);

    let parts: Vec<String> = name.0.iter().filter_map(|p| p.as_ident().map(|i| i.value.clone())).collect();
    match parts.as_slice() {
        [catalog, schema, table, ..] => {
            if table_exists(ctx, catalog, schema, table) {
                Some(schema.clone())
            } else {
                None
            }
        }
        [schema, table] => {
            if table_exists(ctx, default_catalog, schema, table) {
                Some(schema.clone())
            } else {
                None
            }
        }
        [table] => {
            for sp in &search_path {
                let schema_name = if sp == "$user" { default_schema } else { sp };
                if table_exists(ctx, default_catalog, schema_name, table) {
                    return Some(schema_name.to_string());
                }
            }
            None
        }
        _ => None,
    }
}

fn object_is_catalog(ctx: &SessionContext, name: &ObjectName) -> bool {
    if let Some(schema) = resolve_schema(ctx, name) {
        schema.eq_ignore_ascii_case("pg_catalog")
            || schema.eq_ignore_ascii_case("information_schema")
    } else {
        false
    }
}

fn factor_has_catalog(ctx: &SessionContext, factor: &TableFactor) -> bool {
    match factor {
        TableFactor::Table { name, .. } => object_is_catalog(ctx, name),
        TableFactor::Derived { subquery, .. } => query_has_catalog(ctx, subquery),
        TableFactor::NestedJoin { table_with_joins, .. } => {
            table_with_joins_contains_catalog(ctx, table_with_joins)
        }
        _ => false,
    }
}

fn table_with_joins_contains_catalog(ctx: &SessionContext, twj: &TableWithJoins) -> bool {
    if factor_has_catalog(ctx, &twj.relation) {
        return true;
    }
    for join in &twj.joins {
        if factor_has_catalog(ctx, &join.relation) {
            return true;
        }
    }
    false
}

fn setexpr_has_catalog(ctx: &SessionContext, expr: &SetExpr) -> bool {
    match expr {
        SetExpr::Select(select) => {
            for twj in &select.from {
                if table_with_joins_contains_catalog(ctx, twj) {
                    return true;
                }
            }
            false
        }
        SetExpr::Query(query) => query_has_catalog(ctx, query),
        SetExpr::SetOperation { left, right, .. } => {
            setexpr_has_catalog(ctx, left) || setexpr_has_catalog(ctx, right)
        }
        _ => false,
    }
}

fn query_has_catalog(ctx: &SessionContext, query: &Query) -> bool {
    if setexpr_has_catalog(ctx, &query.body) {
        return true;
    }
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            if query_has_catalog(ctx, &cte.query) {
                return true;
            }
        }
    }
    false
}

fn statement_has_catalog(ctx: &SessionContext, stmt: &Statement) -> bool {
    match stmt {
        Statement::Query(q) => query_has_catalog(ctx, q),
        _ => false,
    }
}

fn is_catalog_query(ctx: &SessionContext, sql: &str) -> datafusion::error::Result<bool> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    Ok(statements.iter().any(|s| statement_has_catalog(ctx, s)))
}

pub async fn dispatch_query<F, Fut>(
    ctx: &SessionContext,
    sql: &str,
    handler: F,
) -> datafusion::error::Result<(Vec<RecordBatch>, Arc<Schema>)>
where
    F: for<'a> Fn(&'a SessionContext, &'a str) -> Fut,
    Fut: std::future::Future<Output = datafusion::error::Result<(Vec<RecordBatch>, Arc<Schema>)>>,
{
    if is_catalog_query(ctx, sql)? {
        let qualified = qualify_catalog_tables(ctx, sql)?;
        execute_sql(ctx, &qualified, None, None).await
    } else {
        handler(ctx, sql).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::register_table::register_table;
    use arrow::datatypes::DataType;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn test_dispatch_user_table_calls_handler() -> datafusion::error::Result<()> {
        let ctx = SessionContext::new();
        register_table(&ctx, "crm", "crm", "users", vec![("id", DataType::Int32, false)])?;

        let called = Arc::new(Mutex::new(false));
        let called_clone = called.clone();
        let handler = move |_ctx: &SessionContext, _sql: &str| {
            let called_clone = called_clone.clone();
            async move {
                *called_clone.lock().unwrap() = true;
                Ok((Vec::new(), Arc::new(Schema::empty())))
            }
        };

        let _ = dispatch_query(&ctx, "SELECT * FROM users", handler).await?;
        assert!(*called.lock().unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_dispatch_catalog_query_internal() -> datafusion::error::Result<()> {
        let ctx = SessionContext::new();
        register_table(&ctx, "datafusion", "pg_catalog", "pg_class", vec![("oid", DataType::Int32, false)])?;
        let called = Arc::new(Mutex::new(false));
        let called_clone = called.clone();
        let handler = move |_ctx: &SessionContext, _sql: &str| {
            let called_clone = called_clone.clone();
            async move {
                *called_clone.lock().unwrap() = true;
                Ok((Vec::new(), Arc::new(Schema::empty())))
            }
        };

        let _ = dispatch_query(&ctx, "SELECT * FROM pg_catalog.pg_class", handler).await?;
        assert!(!*called.lock().unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_dispatch_unqualified_catalog_query_internal() -> datafusion::error::Result<()> {
        let ctx = SessionContext::new();
        register_table(&ctx, "datafusion", "pg_catalog", "pg_class", vec![("oid", DataType::Int32, false)])?;

        let called = Arc::new(Mutex::new(false));
        let called_clone = called.clone();
        let handler = move |_ctx: &SessionContext, _sql: &str| {
            let called_clone = called_clone.clone();
            async move {
                *called_clone.lock().unwrap() = true;
                Ok((Vec::new(), Arc::new(Schema::empty())))
            }
        };

        let _ = dispatch_query(&ctx, "SELECT * FROM pg_class", handler).await?;
        assert!(!*called.lock().unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_dispatch_user_table_precedence() -> datafusion::error::Result<()> {
        let mut config = datafusion::execution::context::SessionConfig::new()
            .with_default_catalog_and_schema("datafusion", "public")
            .with_option_extension(ClientOpts::default());

        if let Some(opts) = config.options_mut().extensions.get_mut::<ClientOpts>() {
            opts.search_path = "crm, pg_catalog".to_string();
        }

        let ctx = SessionContext::new_with_config(config);
        register_table(&ctx, "datafusion", "pg_catalog", "pg_class", vec![("oid", DataType::Int32, false)])?;
        register_table(&ctx, "datafusion", "crm", "pg_class", vec![("id", DataType::Int32, false)])?;

        let called = Arc::new(Mutex::new(false));
        let called_clone = called.clone();
        let handler = move |_ctx: &SessionContext, _sql: &str| {
            let called_clone = called_clone.clone();
            async move {
                *called_clone.lock().unwrap() = true;
                Ok((Vec::new(), Arc::new(Schema::empty())))
            }
        };

        let _ = dispatch_query(&ctx, "SELECT * FROM pg_class", handler).await?;
        assert!(*called.lock().unwrap());
        Ok(())
    }
}

