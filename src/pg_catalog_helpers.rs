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


pub async fn register_user_tables(ctx: &SessionContext) -> DFResult<()> {
    // 1. pg_class (row for oid 50010)
    if ctx
        .sql("SELECT 1 FROM pg_catalog.pg_class WHERE oid = 50010")
        .await?
        .count()
        .await?
        == 0
    {
        ctx.sql("INSERT INTO pg_catalog.pg_class
                 (oid, relname, relnamespace, relkind, reltuples, reltype, relispartition)
                 VALUES (50010,'users',2200,'r',0,50011, false)")
            .await?
            .collect()
            .await?;
    }

    // 2. pg_type (row for oid 50011)
    if ctx
        .sql("SELECT 1 FROM pg_catalog.pg_type WHERE oid = 50011")
        .await?
        .count()
        .await?
        == 0
    {
        ctx.sql("INSERT INTO pg_catalog.pg_type
                 (oid, typname, typrelid, typlen, typcategory)
                 VALUES (50011,'_users',50010,-1,'C')")
            .await?
            .collect()
            .await?;
    }

    // 3. pg_attribute (rows for attrelid 50010)
    if ctx
        .sql("SELECT 1 FROM pg_catalog.pg_attribute
              WHERE attrelid = 50010 AND attnum = 1")
        .await?
        .count()
        .await?
        == 0
    {
        ctx.sql("INSERT INTO pg_catalog.pg_attribute
                 (attrelid,attnum,attname,atttypid,atttypmod,attnotnull,attisdropped)
                 VALUES
                 (50010,1,'id',23,-1,false,false),
                 (50010,2,'name',25,-1,false,false)")
            .await?
            .collect()
            .await?;
    }
    Ok(())
}
