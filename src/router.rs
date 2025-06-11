use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow::datatypes::Schema;
use datafusion::execution::context::SessionContext;

use crate::session::execute_sql;

use sqlparser::ast::*;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

fn object_is_catalog(name: &ObjectName) -> bool {
    if let Some(first) = name.0.first().and_then(|p| p.as_ident()) {
        if first.value.eq_ignore_ascii_case("pg_catalog")
            || first.value.eq_ignore_ascii_case("information_schema")
        {
            return true;
        }
    }
    if let Some(last) = name.0.last().and_then(|p| p.as_ident()) {
        if last.value.to_lowercase().starts_with("pg_") {
            return true;
        }
    }
    false
}

fn factor_has_catalog(factor: &TableFactor) -> bool {
    match factor {
        TableFactor::Table { name, .. } => object_is_catalog(name),
        TableFactor::Derived { subquery, .. } => query_has_catalog(subquery),
        TableFactor::NestedJoin { table_with_joins, .. } => {
            table_with_joins_contains_catalog(table_with_joins)
        }
        _ => false,
    }
}

fn table_with_joins_contains_catalog(twj: &TableWithJoins) -> bool {
    if factor_has_catalog(&twj.relation) {
        return true;
    }
    for join in &twj.joins {
        if factor_has_catalog(&join.relation) {
            return true;
        }
    }
    false
}

fn setexpr_has_catalog(expr: &SetExpr) -> bool {
    match expr {
        SetExpr::Select(select) => {
            for twj in &select.from {
                if table_with_joins_contains_catalog(twj) {
                    return true;
                }
            }
            false
        }
        SetExpr::Query(query) => query_has_catalog(query),
        SetExpr::SetOperation { left, right, .. } => {
            setexpr_has_catalog(left) || setexpr_has_catalog(right)
        }
        _ => false,
    }
}

fn query_has_catalog(query: &Query) -> bool {
    if setexpr_has_catalog(&query.body) {
        return true;
    }
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            if query_has_catalog(&cte.query) {
                return true;
            }
        }
    }
    false
}

fn statement_has_catalog(stmt: &Statement) -> bool {
    match stmt {
        Statement::Query(q) => query_has_catalog(q),
        _ => false,
    }
}

fn is_catalog_query(sql: &str) -> datafusion::error::Result<bool> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    Ok(statements.iter().any(statement_has_catalog))
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
    if is_catalog_query(sql)? {
        execute_sql(ctx, sql, None, None).await
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
}

