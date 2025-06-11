use datafusion::error::Result as DFResult;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::{stream, Stream};
use futures::stream::BoxStream;
use arrow::array::{Array, ArrayRef, Float32Array, Float64Array};
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::auth::md5pass::{hash_md5_password, Md5PasswordAuthStartupHandler};
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;
use serde::{Serialize, Deserialize};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use arrow::array::{BooleanArray, Int32Array, Int64Array, LargeStringArray, ListArray, StringArray, StringViewArray};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use datafusion::{
    logical_expr::{create_udf, Volatility, ColumnarValue},
    common::ScalarValue,
};

use crate::replace::{regclass_udfs};
use crate::session::{execute_sql, ClientOpts};
use crate::router::dispatch_query;
use crate::user_functions::{
    register_array_agg,
    register_current_schema,
    register_current_schemas,
    register_pg_get_array,
    register_oidvector_to_array,
    register_pg_get_function_arguments,
    register_pg_get_function_result,
    register_pg_get_function_sqlbody,
    register_pg_get_indexdef,
    register_pg_get_triggerdef,
    register_pg_get_ruledef,
    register_pg_get_one,
    register_pg_get_statisticsobjdef_columns,
    register_pg_get_viewdef,
    register_pg_get_keywords,
    register_pg_available_extension_versions,
    register_pg_postmaster_start_time,
    register_pg_relation_is_publishable,
    register_has_database_privilege,
    register_has_schema_privilege,
    register_pg_relation_size,
    register_pg_total_relation_size,
    register_quote_ident,
    register_scalar_array_to_string,
    register_scalar_format_type,
    register_scalar_pg_age,
    register_scalar_pg_encoding_to_char,
    register_scalar_pg_get_expr,
    register_scalar_pg_get_partkeydef,
    register_scalar_pg_get_userbyid,
    register_scalar_pg_is_in_recovery,
    register_scalar_pg_table_is_visible,
    register_scalar_pg_tablespace_location,
    register_scalar_regclass_oid,
    register_scalar_txid_current,
    register_encode,
    register_upper,
    register_version_fn,
    register_translate,
};
use tokio::net::TcpStream;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use log;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::Statement;


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
}
