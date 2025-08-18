// TCP server exposing the DataFusion session via the PostgreSQL wire protocol.
// Implements pgwire handlers and registers compatibility UDFs so external clients can connect.
// Exists to test the pg_catalog emulation over real network connections.

use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Float32Array, Float64Array};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::BoxStream;
use futures::{stream, Stream, TryFutureExt, TryStreamExt};
use pgwire::api::auth::md5pass::{hash_md5_password, Md5PasswordAuthStartupHandler};
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use tokio::net::TcpListener;

use arrow::array::{
    BooleanArray, Int32Array, Int64Array, LargeStringArray, ListArray, StringArray, StringViewArray,
};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use datafusion::{
    common::ScalarValue,
    logical_expr::{create_udf, ColumnarValue, Volatility},
};

use crate::router::dispatch_query;
use crate::session::{execute_sql, ClientOpts};
use log;
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// PostgreSQL version reported to clients during startup and via `SHOW server_version`.
pub const SERVER_VERSION: &str = "17.4.0";

#[derive(Serialize, Deserialize, Clone, Debug)]
struct CapturedQuery {
    query: String,
    parameters: Vec<Option<serde_json::Value>>,
    result: Vec<BTreeMap<String, serde_json::Value>>,
    success: bool,
    error_details: Option<String>,
}

#[derive(Clone)]
struct CaptureStore {
    path: PathBuf,
    entries: Arc<Mutex<Vec<CapturedQuery>>>,
}

impl CaptureStore {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            entries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn encode_yaml_value(v: &serde_json::Value, indent: usize, out: &mut String) {
        match v {
            serde_json::Value::Null => out.push_str("null"),
            serde_json::Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            serde_json::Value::Number(n) => out.push_str(&n.to_string()),
            serde_json::Value::String(s) => {
                out.push('"');
                for ch in s.chars() {
                    match ch {
                        '"' => out.push_str("\\\""),
                        '\\' => out.push_str("\\\\"),
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    out.push_str("[]");
                } else {
                    for item in arr {
                        out.push('\n');
                        out.push_str(&" ".repeat(indent));
                        out.push_str("- ");
                        Self::encode_yaml_value(item, indent + 2, out);
                    }
                }
            }
            serde_json::Value::Object(map) => {
                if map.is_empty() {
                    out.push_str("{}");
                } else {
                    for (k, v) in map {
                        out.push('\n');
                        out.push_str(&" ".repeat(indent));
                        out.push_str(k);
                        out.push_str(": ");
                        Self::encode_yaml_value(v, indent + 2, out);
                    }
                }
            }
        }
    }

    fn save_entries(&self, entries: &[CapturedQuery]) {
        if let Ok(mut file) = File::create(&self.path) {
            if let Ok(val) = serde_json::to_value(entries) {
                let mut out = String::new();
                if let serde_json::Value::Array(arr) = val {
                    let mut first = true;
                    for item in arr {
                        if let serde_json::Value::Object(map) = item {
                            if !first {
                                out.push('\n');
                            }
                            first = false;
                            out.push_str("- ");
                            let mut iter = map.iter();
                            if let Some((k, v)) = iter.next() {
                                out.push_str(k);
                                out.push_str(": ");
                                Self::encode_yaml_value(v, 2, &mut out);
                            }
                            for (k, v) in iter {
                                out.push('\n');
                                out.push_str("  ");
                                out.push_str(k);
                                out.push_str(": ");
                                Self::encode_yaml_value(v, 2, &mut out);
                            }
                            out.push('\n');
                        }
                    }
                }
                let _ = file.write_all(out.as_bytes());
            }
        }
    }

    fn append(&self, entry: CapturedQuery) {
        let mut vec = self.entries.lock().unwrap();
        vec.push(entry);
        self.save_entries(&vec);
    }
}

pub struct DatafusionBackend {
    ctx: Arc<SessionContext>,
    query_parser: Arc<NoopQueryParser>,
    capture: Option<CaptureStore>,
}

impl DatafusionBackend {
    pub fn new(ctx: Arc<SessionContext>, capture: Option<CaptureStore>) -> Self {
        Self {
            ctx,
            query_parser: Arc::new(NoopQueryParser::new()),
            capture,
        }
    }

    fn register_current_database<C>(&self, client: &C) -> datafusion::error::Result<()>
    where
        C: ClientInfo + ?Sized,
    {
        static KEY: &str = "current_database";

        if self.ctx.state().scalar_functions().contains_key(KEY) {
            return Ok(());
        }

        if let Some(db) = client
            .metadata()
            .get(pgwire::api::METADATA_DATABASE)
            .cloned()
        {
            let fun = Arc::new(move |_args: &[ColumnarValue]| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(db.clone()))))
            });
            let udf = create_udf(KEY, vec![], DataType::Utf8, Volatility::Stable, fun.clone());
            self.ctx.register_udf(udf);
            let udf = create_udf(
                "pg_catalog.current_database",
                vec![],
                DataType::Utf8,
                Volatility::Stable,
                fun.clone(),
            );
            self.ctx.register_udf(udf);
        }

        Ok(())
    }

    fn register_session_user<C>(&self, client: &C) -> datafusion::error::Result<()>
    where
        C: ClientInfo + ?Sized,
    {
        static KEY: &str = "session_user";
        if self.ctx.state().scalar_functions().contains_key(KEY) {
            return Ok(());
        }

        if let Some(user) = client.metadata().get(pgwire::api::METADATA_USER).cloned() {
            let fun = Arc::new(move |_args: &[ColumnarValue]| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(user.clone()))))
            });
            let udf = create_udf(KEY, vec![], DataType::Utf8, Volatility::Stable, fun);
            self.ctx.register_udf(udf);
        }

        Ok(())
    }

    fn register_current_user<C>(&self, client: &C) -> datafusion::error::Result<()>
    where
        C: ClientInfo + ?Sized,
    {
        static KEY: &str = "current_user";

        if self.ctx.state().scalar_functions().contains_key(KEY) {
            return Ok(());
        }

        if let Some(user) = client.metadata().get(pgwire::api::METADATA_USER).cloned() {
            let fun = Arc::new(move |_args: &[ColumnarValue]| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(user.clone()))))
            });
            let udf = create_udf(KEY, vec![], DataType::Utf8, Volatility::Stable, fun.clone());
            self.ctx.register_udf(udf);
            let udf = create_udf(
                "pg_catalog.current_user",
                vec![],
                DataType::Utf8,
                Volatility::Stable,
                fun,
            );
            self.ctx.register_udf(udf);
        }

        Ok(())
    }

    fn show_variable_response<'a>(&self, name: &str, format: FieldFormat) -> Option<Response<'a>> {
        let state = self.ctx.state();
        let opts = state.config_options().extensions.get::<ClientOpts>()?;

        let value = match name {
            "application_name" => opts.application_name.as_str(),
            "datestyle" => opts.datestyle.as_str(),
            "search_path" => opts.search_path.as_str(),
            _ => return None,
        };

        let fields = Arc::new(vec![FieldInfo::new(
            name.to_string(),
            None,
            None,
            Type::TEXT,
            format,
        )]);

        let mut encoder = DataRowEncoder::new(fields.clone());
        encoder.encode_field(&Some(value)).ok()?;
        let row = encoder.finish().ok()?;
        let rows = stream::iter(vec![Ok(row)]);
        let rows: BoxStream<'a, PgWireResult<DataRow>> = Box::pin(rows);
        Some(Response::Query(QueryResponse::new(fields, rows)))
    }

    fn parse_show_variable(sql: &str) -> Option<String> {
        let dialect = PostgreSqlDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).ok()?;
        if statements.len() != 1 {
            return None;
        }
        match statements.pop()? {
            Statement::ShowVariable { variable } if variable.len() == 1 => {
                Some(variable[0].value.clone())
            }
            _ => None,
        }
    }
}

pub struct DummyAuthSource;

#[async_trait]
impl AuthSource for DummyAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        log::info!("login info: {:?}", login_info);

        let salt = vec![0, 0, 0, 0];
        let password = "pencil";

        let hash_password = hash_md5_password(login_info.user().as_ref().unwrap(), password, &salt);
        Ok(Password::new(Some(salt), hash_password.as_bytes().to_vec()))
    }
}

fn arrow_to_pg_type(dt: &DataType) -> Type {
    use arrow::datatypes::DataType::*;

    match dt {
        Boolean => Type::BOOL,
        Int16 => Type::INT2,
        Int32 => Type::INT4,
        Int64 => Type::INT8,
        Utf8 | Utf8View | LargeUtf8 => Type::TEXT,
        Timestamp(_, _) => Type::TIMESTAMP,
        Float32 => Type::FLOAT4, // real
        Float64 => Type::FLOAT8, // double precision

        // ── arrays ───────────────────────────────────────────────
        List(inner) => match inner.data_type() {
            Utf8 => Type::TEXT_ARRAY,    // text[]
            Int32 => Type::INT4_ARRAY,   // int4[]
            Int64 => Type::INT8_ARRAY,   // int8[]
            Boolean => Type::BOOL_ARRAY, // bool[]
            // add more element types here as you need them
            other => panic!("arrow_to_pg_type: no pgwire::Type for list<{other:?}>"),
        },

        // anything else – send as plain text so the client can at
        // least see something instead of us mangling it away
        other => {
            log::warn!("arrow_to_pg_type: mapping {other:?} to TEXT");
            Type::TEXT
        }
    }
}

fn batch_to_field_info(batch: &RecordBatch, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    Ok(batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            FieldInfo::new(
                f.name().to_string(),
                None,
                None,
                arrow_to_pg_type(f.data_type()),
                format.format_for(idx),
            )
        })
        .collect())
}

fn batches_to_json_rows(batches: &[RecordBatch]) -> Vec<BTreeMap<String, serde_json::Value>> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut map = BTreeMap::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let val = match field.data_type() {
                    DataType::Utf8 => {
                        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::String(arr.value(row_idx).to_string())
                        }
                    }
                    DataType::Utf8View => {
                        let arr = col.as_any().downcast_ref::<StringViewArray>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::String(arr.value(row_idx).to_string())
                        }
                    }
                    DataType::LargeUtf8 => {
                        let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::String(arr.value(row_idx).to_string())
                        }
                    }
                    DataType::Int32 => {
                        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::json!(arr.value(row_idx))
                        }
                    }
                    DataType::Int64 => {
                        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::json!(arr.value(row_idx))
                        }
                    }
                    DataType::Boolean => {
                        let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::json!(arr.value(row_idx))
                        }
                    }
                    DataType::Float32 => {
                        let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::json!(arr.value(row_idx))
                        }
                    }
                    DataType::Float64 => {
                        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::json!(arr.value(row_idx))
                        }
                    }
                    DataType::List(inner) if inner.data_type() == &DataType::Utf8 => {
                        let list = col.as_any().downcast_ref::<ListArray>().unwrap();
                        if list.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            let arr = list.value(row_idx);
                            let sa = arr.as_any().downcast_ref::<StringArray>().unwrap();
                            let mut items = Vec::with_capacity(sa.len());
                            for i in 0..sa.len() {
                                if sa.is_null(i) {
                                    items.push(serde_json::Value::Null);
                                } else {
                                    items.push(serde_json::Value::String(sa.value(i).to_string()));
                                }
                            }
                            serde_json::Value::Array(items)
                        }
                    }
                    _ => serde_json::Value::Null,
                };
                map.insert(field.name().clone(), val);
            }
            rows.push(map);
        }
    }
    rows
}

fn decode_parameters(params: &[Option<Bytes>], types: &[Type]) -> Vec<Option<serde_json::Value>> {
    params
        .iter()
        .zip(types.iter())
        .map(|(param, typ)| match (param, typ) {
            (None, _) => None,
            (Some(bytes), &Type::INT2) => Some(serde_json::json!(i16::from_be_bytes(
                bytes[..].try_into().unwrap()
            ))),
            (Some(bytes), &Type::INT4) => Some(serde_json::json!(i32::from_be_bytes(
                bytes[..].try_into().unwrap()
            ))),
            (Some(bytes), &Type::INT8) => Some(serde_json::json!(i64::from_be_bytes(
                bytes[..].try_into().unwrap()
            ))),
            (Some(bytes), &Type::OID) => Some(serde_json::json!(u32::from_be_bytes(
                bytes[..].try_into().unwrap()
            ))),
            (Some(bytes), &Type::VARCHAR)
            | (Some(bytes), &Type::TEXT)
            | (Some(bytes), &Type::BPCHAR)
            | (Some(bytes), &Type::NAME)
            | (Some(bytes), &Type::UNKNOWN) => Some(serde_json::Value::String(
                String::from_utf8(bytes.to_vec()).unwrap(),
            )),
            _ => None,
        })
        .collect()
}

fn batch_to_row_stream(
    batch: &RecordBatch,
    schema: Arc<Vec<FieldInfo>>,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    let mut rows = Vec::new();
    for row_idx in 0..batch.num_rows() {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for col in batch.columns() {
            // if row_idx == 0 {
            //     println!("col {:?} type {:?}", col, col.data_type());
            // }

            match col.data_type() {
                DataType::Utf8 => {
                    let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                    let value = if col.is_null(row_idx) {
                        None::<String>
                    } else {
                        Some(array.value(row_idx).to_string())
                    };
                    encoder.encode_field(&value).unwrap();
                }
                DataType::Utf8View => {
                    let array = col.as_any().downcast_ref::<StringViewArray>().unwrap();
                    let value = if col.is_null(row_idx) {
                        None::<String>
                    } else {
                        Some(array.value(row_idx).to_string())
                    };
                    encoder.encode_field(&value).unwrap();
                }
                DataType::LargeUtf8 => {
                    let array = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
                    let value = if col.is_null(row_idx) {
                        None::<String>
                    } else {
                        Some(array.value(row_idx).to_string())
                    };
                    encoder.encode_field(&value).unwrap();
                }
                DataType::Int32 => {
                    let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
                    let value = if col.is_null(row_idx) {
                        None::<i32>
                    } else {
                        Some(array.value(row_idx))
                    };
                    encoder.encode_field(&value).unwrap();
                }
                DataType::Int64 => {
                    let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    let value = if col.is_null(row_idx) {
                        None::<i64>
                    } else {
                        Some(array.value(row_idx))
                    };
                    encoder.encode_field(&value).unwrap();
                }
                /* ----------  F L O A T S  ---------- */
                DataType::Float32 => {
                    let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                    let value = if col.is_null(row_idx) {
                        None::<f32>
                    } else {
                        Some(arr.value(row_idx))
                    };
                    encoder.encode_field(&value).unwrap();
                }
                DataType::Float64 => {
                    let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                    let value = if col.is_null(row_idx) {
                        None::<f64>
                    } else {
                        Some(arr.value(row_idx))
                    };
                    encoder.encode_field(&value).unwrap();
                }

                // ---------- TIMESTAMP μs / ms / ns ----------
                DataType::Timestamp(unit, _) => {
                    match unit {
                        TimeUnit::Microsecond => {
                            use arrow::array::TimestampMicrosecondArray;
                            let arr = col
                                .as_any()
                                .downcast_ref::<TimestampMicrosecondArray>()
                                .unwrap();
                            let value = if arr.is_null(row_idx) {
                                None::<String>
                            } else {
                                let v = arr.value(row_idx); // micro-seconds
                                let secs = v / 1_000_000;
                                let micros = (v % 1_000_000) as u32;
                                let ts =
                                    chrono::NaiveDateTime::from_timestamp_opt(secs, micros * 1_000)
                                        .unwrap();
                                Some(ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                            };
                            encoder.encode_field(&value).unwrap();
                        }
                        TimeUnit::Millisecond => {
                            use arrow::array::TimestampMillisecondArray;
                            let arr = col
                                .as_any()
                                .downcast_ref::<TimestampMillisecondArray>()
                                .unwrap();
                            let value = if arr.is_null(row_idx) {
                                None::<String>
                            } else {
                                let v = arr.value(row_idx); // milli-seconds
                                let secs = v / 1_000;
                                let millis = (v % 1_000) as u32;
                                let ts = chrono::NaiveDateTime::from_timestamp_opt(
                                    secs,
                                    millis * 1_000_000,
                                )
                                .unwrap();
                                Some(ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
                            };
                            encoder.encode_field(&value).unwrap();
                        }
                        TimeUnit::Nanosecond => {
                            use arrow::array::TimestampNanosecondArray;
                            let arr = col
                                .as_any()
                                .downcast_ref::<TimestampNanosecondArray>()
                                .unwrap();
                            let value = if arr.is_null(row_idx) {
                                None::<String>
                            } else {
                                let v = arr.value(row_idx); // nano-seconds
                                let secs = v / 1_000_000_000;
                                let nanos = (v % 1_000_000_000) as u32;
                                let ts =
                                    chrono::NaiveDateTime::from_timestamp_opt(secs, nanos).unwrap();
                                Some(ts.format("%Y-%m-%d %H:%M:%S%.9f").to_string())
                            };
                            encoder.encode_field(&value).unwrap();
                        }
                        _ => unreachable!(), // TimeUnit::Second isn’t used by Arrow today
                    }
                }
                DataType::Boolean => {
                    let array = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                    let value = if col.is_null(row_idx) {
                        None::<bool>
                    } else {
                        Some(array.value(row_idx))
                    };
                    encoder.encode_field(&value).unwrap();
                }

                DataType::List(inner) if inner.data_type() == &DataType::Utf8 => {
                    let list = col.as_any().downcast_ref::<ListArray>().unwrap();
                    let value: Option<Vec<String>> = if list.is_null(row_idx) {
                        None
                    } else {
                        let arr = list.value(row_idx);
                        let sa = arr.as_any().downcast_ref::<StringArray>().unwrap();
                        let mut items = Vec::with_capacity(sa.len());
                        for i in 0..sa.len() {
                            if sa.is_null(i) {
                                items.push("NULL".to_string());
                            } else {
                                items.push(sa.value(i).to_string());
                            }
                        }
                        Some(items)
                    };
                    encoder.encode_field(&value).unwrap();
                }

                _ => {
                    if col.is_null(row_idx) {
                        encoder.encode_field::<Option<&str>>(&None).unwrap();
                    } else {
                        let value = Some(format!("[unsupported {}]", col.data_type()));
                        encoder.encode_field(&value).unwrap();
                    }
                }
            }
        }
        rows.push(encoder.finish());
    }
    stream::iter(rows.into_iter())
}

#[async_trait]
impl SimpleQueryHandler for DatafusionBackend {
    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        log::debug!("query handler");

        let trimmed = query.trim();
        let lowercase = trimmed.to_lowercase();
        if lowercase.starts_with("begin") {
            return Ok(vec![Response::Execution(Tag::new("BEGIN"))]);
        } else if lowercase.starts_with("commit") {
            return Ok(vec![Response::Execution(Tag::new("COMMIT"))]);
        } else if lowercase.starts_with("rollback") {
            return Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]);
        } else if lowercase.starts_with("discard all") {
            return Ok(vec![Response::Execution(Tag::new("DISCARD ALL"))]);
        } else if lowercase == "show transaction isolation level" {
            let field_infos = Arc::new(vec![FieldInfo::new(
                "transaction_isolation".to_string(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )]);

            let mut encoder = DataRowEncoder::new(field_infos.clone());
            encoder.encode_field(&Some("read committed"))?;
            let row = encoder.finish()?;

            let rows = stream::iter(vec![Ok(row)]);
            return Ok(vec![Response::Query(QueryResponse::new(field_infos, rows))]);
        } else if let Some(var) = Self::parse_show_variable(trimmed) {
            if let Some(resp) = self.show_variable_response(&var.to_lowercase(), FieldFormat::Text)
            {
                return Ok(vec![resp]);
            }
        } else if lowercase == "" {
            return Ok(vec![Response::Execution(Tag::new(""))]);
        }

        let user = client.metadata().get(pgwire::api::METADATA_USER).cloned();
        let database = client
            .metadata()
            .get(pgwire::api::METADATA_DATABASE)
            .cloned();
        log::debug!("database: {:?} {:?}", database, user);

        let _ = self.register_current_database(client);
        let _ = self.register_session_user(client);
        let _ = self.register_current_user(client);

        let exec_res = dispatch_query(&self.ctx, query, None, None, |ctx, sql, p, t| async move {
            let lsql = sql.to_lowercase();
            if lsql.contains("from users") {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, true),
                ]));
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                        Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")])) as ArrayRef,
                    ],
                )
                .unwrap();
                // Ok((vec![batch], schema))
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    stream::once(async { Ok(batch) }),
                )) as SendableRecordBatchStream)
            } else {
                execute_sql(ctx, sql, p, t).await
            }
        })
        .and_then(|stream| {
            let schema = stream.schema();
            stream
                .try_collect::<Vec<RecordBatch>>()
                .map_ok(move |results| (results, schema))
        });

        let (results, schema) = match exec_res.await {
            Ok(v) => v,
            Err(e) => {
                if let Some(c) = &self.capture {
                    c.append(CapturedQuery {
                        query: query.to_string(),
                        parameters: Vec::new(),
                        result: Vec::new(),
                        success: false,
                        error_details: Some(e.to_string()),
                    });
                }
                return Err(PgWireError::ApiError(Box::new(e)));
            }
        };

        let mut responses = Vec::new();

        if results.is_empty() {
            // Prior implementation re-ran the query to obtain the schema when no
            // rows were returned. `execute_sql` already provides the schema, so
            // use it directly and build an empty batch from it.
            let batch = RecordBatch::new_empty(schema.clone());
            let field_infos = Arc::new(batch_to_field_info(&batch, &Format::UnifiedText)?);
            let rows = batch_to_row_stream(&batch, field_infos.clone());

            responses.push(Response::Query(QueryResponse::new(field_infos, rows)));
        } else {
            for batch in &results {
                let schema = Arc::new(batch_to_field_info(batch, &Format::UnifiedText)?);
                let rows = batch_to_row_stream(batch, schema.clone());
                responses.push(Response::Query(QueryResponse::new(schema, rows)));
            }
        }

        if lowercase.starts_with("set") {
            if let Some(c) = &self.capture {
                c.append(CapturedQuery {
                    query: query.to_string(),
                    parameters: Vec::new(),
                    result: batches_to_json_rows(&results),
                    success: true,
                    error_details: None,
                });
            }
            return Ok(vec![Response::Execution(Tag::new("SET"))]);
        }
        if let Some(c) = &self.capture {
            c.append(CapturedQuery {
                query: query.to_string(),
                parameters: Vec::new(),
                result: batches_to_json_rows(&results),
                success: true,
                error_details: None,
            });
        }

        Ok(responses)
    }
}

#[async_trait]
impl ExtendedQueryHandler for DatafusionBackend {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        log::debug!(
            "query start extended {:?} {:?}",
            portal.statement.statement.as_str(),
            portal.parameters
        );

        let sql_trim = portal.statement.statement.trim();
        let lowercase = sql_trim.to_lowercase();

        if sql_trim.is_empty() {
            return Ok(Response::Execution(Tag::new("")));
        } else if lowercase.starts_with("discard all") {
            return Ok(Response::Execution(Tag::new("DISCARD ALL")));
        } else if lowercase == "show transaction isolation level" {
            let field_infos = Arc::new(vec![FieldInfo::new(
                "transaction_isolation".to_string(),
                None,
                None,
                Type::TEXT,
                portal.result_column_format.format_for(0),
            )]);

            let mut encoder = DataRowEncoder::new(field_infos.clone());
            encoder.encode_field(&Some("read committed"))?;
            let row = encoder.finish()?;
            let rows = stream::iter(vec![Ok(row)]);
            return Ok(Response::Query(QueryResponse::new(field_infos, rows)));
        } else if let Some(var) = Self::parse_show_variable(sql_trim) {
            if let Some(resp) = self.show_variable_response(
                &var.to_lowercase(),
                portal.result_column_format.format_for(0),
            ) {
                return Ok(resp);
            }
        }

        let _ = self.register_current_database(client);
        let _ = self.register_session_user(client);
        let _ = self.register_current_user(client);

        let exec_res = dispatch_query(
            &self.ctx,
            portal.statement.statement.as_str(),
            Some(portal.parameters.clone()),
            Some(portal.statement.parameter_types.clone()),
            |ctx, sql, params, types| async move {
                let lsql = sql.to_lowercase();
                if lsql.contains("from users") {
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("id", DataType::Int32, false),
                        Field::new("name", DataType::Utf8, true),
                    ]));
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                            Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")]))
                                as ArrayRef,
                        ],
                    )
                    .unwrap();
                    Ok(Box::pin(RecordBatchStreamAdapter::new(
                        schema,
                        stream::once(async { Ok(batch) }),
                    )) as SendableRecordBatchStream)
                } else {
                    execute_sql(ctx, sql, params, types).await
                }
            },
        )
        .and_then(|stream| {
            let schema = stream.schema();
            stream
                .try_collect::<Vec<RecordBatch>>()
                .map_ok(move |results| (results, schema))
        });

        let (results, schema) = match exec_res.await {
            Ok(v) => v,
            Err(e) => {
                if let Some(c) = &self.capture {
                    let params =
                        decode_parameters(&portal.parameters, &portal.statement.parameter_types);
                    c.append(CapturedQuery {
                        query: portal.statement.statement.clone(),
                        parameters: params,
                        result: Vec::new(),
                        success: false,
                        error_details: Some(e.to_string()),
                    });
                }
                return Err(PgWireError::ApiError(Box::new(e)));
            }
        };

        let batch = if results.is_empty() {
            RecordBatch::new_empty(schema.clone())
        } else {
            results[0].clone()
        };

        let field_infos = Arc::new(batch_to_field_info(&batch, &portal.result_column_format)?);
        let rows = batch_to_row_stream(&batch, field_infos.clone());
        if let Some(c) = &self.capture {
            let params = decode_parameters(&portal.parameters, &portal.statement.parameter_types);
            c.append(CapturedQuery {
                query: portal.statement.statement.clone(),
                parameters: params,
                result: batches_to_json_rows(&results),
                success: true,
                error_details: None,
            });
        }
        Ok(Response::Query(QueryResponse::new(field_infos, rows)))
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        log::debug!("do_describe_statement");

        let sql_trim = stmt.statement.trim();
        let lowercase = sql_trim.to_lowercase();

        if sql_trim.is_empty() {
            return Ok(DescribeStatementResponse::new(vec![], vec![]));
        } else if lowercase.starts_with("discard all") {
            return Ok(DescribeStatementResponse::new(vec![], vec![]));
        } else if lowercase == "show transaction isolation level" {
            let fields = vec![FieldInfo::new(
                "transaction_isolation".to_string(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Binary,
            )];
            return Ok(DescribeStatementResponse::new(vec![], fields));
        } else if lowercase.starts_with("show ") {
            let fields = vec![
                FieldInfo::new(
                    "name".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    FieldFormat::Binary,
                ),
                FieldInfo::new(
                    "setting".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    FieldFormat::Binary,
                ),
            ];
            return Ok(DescribeStatementResponse::new(vec![], fields));
        }

        let (results, schema) = execute_sql(&self.ctx, stmt.statement.as_str(), None, None)
            .and_then(|stream| {
                let schema = stream.schema();
                stream
                    .try_collect::<Vec<RecordBatch>>()
                    .map_ok(move |results| (results, schema))
            })
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        log::debug!("do_describe_statement {:?}", schema);

        if results.is_empty() {
            return Ok(DescribeStatementResponse::new(vec![], vec![]));
        }

        let batch = &results[0];
        let param_types = stmt.parameter_types.clone();
        let fields = batch_to_field_info(batch, &Format::UnifiedBinary)?;
        log::debug!("return from do_describe {:?}", fields);
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        log::debug!("do_describe_portal");
        let sql_trim = portal.statement.statement.trim();
        let lowercase = sql_trim.to_lowercase();

        if sql_trim.is_empty() {
            return Ok(DescribePortalResponse::new(vec![]));
        } else if lowercase.starts_with("discard all") {
            return Ok(DescribePortalResponse::new(vec![]));
        } else if lowercase == "show transaction isolation level" {
            let fields = vec![FieldInfo::new(
                "transaction_isolation".to_string(),
                None,
                None,
                Type::TEXT,
                portal.result_column_format.format_for(0),
            )];
            return Ok(DescribePortalResponse::new(fields));
        } else if lowercase.starts_with("show ") {
            let fields = vec![
                FieldInfo::new(
                    "name".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    portal.result_column_format.format_for(0),
                ),
                FieldInfo::new(
                    "setting".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    portal.result_column_format.format_for(1),
                ),
            ];
            return Ok(DescribePortalResponse::new(fields));
        }

        let (results, schema) = execute_sql(
            &self.ctx,
            portal.statement.statement.as_str(),
            Some(portal.parameters.clone()),
            Some(portal.statement.parameter_types.clone()),
        )
        .and_then(|stream| {
            let schema = stream.schema();
            stream
                .try_collect::<Vec<RecordBatch>>()
                .map_ok(move |results| (results, schema))
        })
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        // println!("do_describe_portal {:?}", schema);

        let batch = if results.is_empty() {
            RecordBatch::new_empty(schema.clone())
        } else {
            results[0].clone()
        };

        let fields = batch_to_field_info(&batch, &portal.result_column_format)?;
        Ok(DescribePortalResponse::new(fields))
    }
}

pub struct DatafusionBackendFactory {
    handler: Arc<DatafusionBackend>,
}

impl PgWireServerHandlers for DatafusionBackendFactory {
    type StartupHandler =
        Md5PasswordAuthStartupHandler<DummyAuthSource, DefaultServerParameterProvider>;
    type SimpleQueryHandler = DatafusionBackend;
    type ExtendedQueryHandler = DatafusionBackend;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = pgwire::api::NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        let mut params = DefaultServerParameterProvider::default();
        params.server_version = SERVER_VERSION.to_string();
        Arc::new(Md5PasswordAuthStartupHandler::new(
            Arc::new(DummyAuthSource),
            Arc::new(params),
        ))
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(pgwire::api::NoopErrorHandler)
    }
}

async fn detect_gssencmode(mut socket: TcpStream) -> Option<TcpStream> {
    let mut buf = [0u8; 8];

    if let Ok(n) = socket.peek(&mut buf).await {
        if n == 8 {
            let request_code = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
            if request_code == 80877104 {
                if let Err(e) = socket.read_exact(&mut buf).await {
                    log::error!("Failed to consume GSSAPI request: {:?}", e);
                }
                if let Err(e) = socket.write_all(b"N").await {
                    log::error!("Failed to send rejection message: {:?}", e);
                }
            }
        }
    }

    Some(socket)
}

pub async fn start_server(
    base_ctx: Arc<SessionContext>,
    addr: &str,
    _default_catalog: &str,
    _default_schema: &str,
    capture: Option<std::path::PathBuf>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    log::info!("Listening on {}", addr);

    let capture_store = capture.map(CaptureStore::new);

    loop {
        let (socket, _) = listener.accept().await?;
        if let Some(socket) = detect_gssencmode(socket).await {
            let ctx = base_ctx.clone();
            let factory = Arc::new(DatafusionBackendFactory {
                handler: Arc::new(DatafusionBackend::new(
                    Arc::clone(&ctx),
                    capture_store.clone(),
                )),
            });
            let factory = factory.clone();

            tokio::spawn(async move {
                if let Err(e) = process_socket(socket, None, factory).await {
                    log::error!("connection error: {:?}", e);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, BooleanArray, Int32Array};
    use arrow::datatypes::{Field, Schema};
    use futures::StreamExt;

    #[test]
    fn test_batch_to_row_stream_types_and_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("flag", DataType::Boolean, true),
            Field::new("num", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(42), None])) as ArrayRef,
            ],
        )
        .unwrap();

        let info = batch_to_field_info(&batch, &Format::UnifiedText).unwrap();
        assert_eq!(info[0].datatype(), &Type::BOOL);
        assert_eq!(info[1].datatype(), &Type::INT4);

        let rows = futures::executor::block_on(
            batch_to_row_stream(&batch, Arc::new(info)).collect::<Vec<_>>(),
        );
        assert_eq!(rows.len(), 2);

        let row0 = rows[0].as_ref().unwrap();
        assert_eq!(row0.field_count, 2);
        let buf = &row0.data;
        assert_eq!(&buf[0..4], &1i32.to_be_bytes());
        assert_eq!(buf[4], b't');
        assert_eq!(&buf[5..9], &2i32.to_be_bytes());
        assert_eq!(&buf[9..11], b"42");

        let row1 = rows[1].as_ref().unwrap();
        let buf = &row1.data;
        assert_eq!(&buf[0..4], &(-1i32).to_be_bytes());
        assert_eq!(&buf[4..8], &(-1i32).to_be_bytes());
    }

    #[test]
    fn test_arrow_to_pg_type() {
        assert_eq!(arrow_to_pg_type(&DataType::Boolean), Type::BOOL);
        assert_eq!(arrow_to_pg_type(&DataType::Int32), Type::INT4);
        assert_eq!(arrow_to_pg_type(&DataType::Int64), Type::INT8);
        assert_eq!(arrow_to_pg_type(&DataType::Int16), Type::INT2);
        assert_eq!(arrow_to_pg_type(&DataType::Utf8), Type::TEXT);
        assert_eq!(arrow_to_pg_type(&DataType::Utf8View), Type::TEXT);
        assert_eq!(arrow_to_pg_type(&DataType::LargeUtf8), Type::TEXT);
        assert_eq!(arrow_to_pg_type(&DataType::Float32), Type::FLOAT4);
        assert_eq!(arrow_to_pg_type(&DataType::Float64), Type::FLOAT8);
    }

    #[test]
    fn test_server_version_constant() {
        assert_eq!(SERVER_VERSION, "17.4.0");
    }
}
