// Session management utilities.
// Loads YAML schemas into MemTables, registers UDFs and executes rewritten queries using DataFusion.
// Separated to encapsulate DataFusion setup and query execution behaviour.

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use serde::Deserialize;
use serde_yaml;

use crate::clean_duplicate_columns::alias_all_columns;
use crate::replace::{
    alias_subquery_tables, regclass_udfs, replace_regclass, replace_set_command_with_namespace,
    rewrite_array_subquery, rewrite_available_updates, rewrite_brace_array_literal,
    rewrite_char_cast, rewrite_name_cast, rewrite_oid_cast, rewrite_oidvector_any,
    rewrite_oidvector_unnest, rewrite_pg_custom_operator, rewrite_regoper_cast,
    rewrite_regoperator_cast, rewrite_regproc_cast, rewrite_regprocedure_cast,
    rewrite_regtype_cast, rewrite_schema_qualified_custom_types, rewrite_schema_qualified_text,
    rewrite_schema_qualified_udtfs, rewrite_time_zone_utc, rewrite_tuple_equality,
    rewrite_xid_cast, strip_default_collate,
};
use pgwire::api::Type;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{Cursor, Read};
use std::path::Path;
use std::ptr::null;
use std::sync::{Arc, Mutex};
use zip::ZipArchive;

use crate::user_functions::{
    register_array_agg, register_current_schema, register_current_schemas, register_encode,
    register_has_database_privilege, register_has_schema_privilege, register_oidvector_to_array,
    register_pg_available_extension_versions, register_pg_get_array,
    register_pg_get_function_arguments, register_pg_get_function_result,
    register_pg_get_function_sqlbody, register_pg_get_indexdef, register_pg_get_keywords,
    register_pg_get_one, register_pg_get_ruledef, register_pg_get_statisticsobjdef_columns,
    register_pg_get_triggerdef, register_pg_get_viewdef, register_pg_postmaster_start_time,
    register_pg_relation_is_publishable, register_pg_relation_size,
    register_pg_total_relation_size, register_quote_ident, register_scalar_array_to_string,
    register_scalar_format_type, register_scalar_pg_age, register_scalar_pg_encoding_to_char,
    register_scalar_pg_get_expr, register_scalar_pg_get_partkeydef,
    register_scalar_pg_get_userbyid, register_scalar_pg_is_in_recovery,
    register_scalar_pg_table_is_visible, register_scalar_pg_tablespace_location,
    register_scalar_regclass_oid, register_scalar_txid_current, register_translate, register_upper,
    register_version_fn,
};

use crate::scalar_to_cte::rewrite_subquery_as_cte;
use bytes::Bytes;

use datafusion::common::{config::ConfigEntry, config_err};
use datafusion::scalar::ScalarValue;

static SCHEMA_ZIP: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/pg_catalog_data/postgres-schema-nightly.zip"
));
use crate::db_table::{map_pg_type, ObservableMemTable, ScanTrace};
use crate::replace_any_group_by::rewrite_group_by_for_any;
use datafusion::common::config::{ConfigExtension, ExtensionOptions};
use df_subquery_udf::rewrite_query;

#[derive(Clone, Debug)]
pub struct ClientOpts {
    pub application_name: String,
    pub datestyle: String,
    pub search_path: String,
}

impl Default for ClientOpts {
    fn default() -> Self {
        Self {
            application_name: String::new(),
            datestyle: "ISO, MDY".to_string(),
            search_path: "\"$user\", public".to_string(),
        }
    }
}

impl ConfigExtension for ClientOpts {
    const PREFIX: &'static str = "pg_catalog";
}

impl ExtensionOptions for ClientOpts {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion::error::Result<()> {
        log::debug!("set key {:?}", key);
        match key {
            "application_name" => {
                self.application_name = value.to_string();
                log::debug!("value is set!!!");
                Ok(())
            }
            "datestyle" => {
                self.datestyle = value.to_string();
                Ok(())
            }
            "search_path" => {
                self.search_path = value.to_string();
                Ok(())
            }
            "extra_float_digits" => Ok(()),
            _ => config_err!("unknown key {key}"),
        }
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![
            ConfigEntry {
                key: "application_name".to_string(),
                value: Some(self.application_name.clone()),
                description: "",
            },
            ConfigEntry {
                key: "datestyle".to_string(),
                value: Some(self.datestyle.clone()),
                description: "",
            },
            ConfigEntry {
                key: "search_path".to_string(),
                value: Some(self.search_path.clone()),
                description: "",
            },
        ]
    }
}

#[derive(Debug, Deserialize)]
struct TableDef {
    #[serde(rename = "type", default)]
    table_type: Option<String>,
    schema: BTreeMap<String, String>,
    #[serde(default)]
    pg_types: Option<BTreeMap<String, String>>,
    rows: Option<Vec<BTreeMap<String, serde_json::Value>>>,
}

#[derive(Debug, Deserialize)]
struct YamlSchema(HashMap<String, HashMap<String, HashMap<String, TableDef>>>);

fn rename_columns(batch: &RecordBatch, name_map: &HashMap<String, String>) -> RecordBatch {
    let new_fields = batch
        .schema()
        .fields()
        .iter()
        .map(|old_field| {
            let new_name = name_map
                .get(old_field.name())
                .map(|s| s.as_str())
                .unwrap_or_else(|| old_field.name().as_str());
            Field::new(
                new_name,
                old_field.data_type().clone(),
                old_field.is_nullable(),
            )
        })
        .collect::<Vec<_>>();

    let new_schema = std::sync::Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, batch.columns().to_vec()).unwrap()
}

/// Remove system columns from `batches` if they were not explicitly referenced
/// in the original SQL statement. PostgreSQL exposes virtual system columns
/// like `xmin` and `ctid` which are hidden from `SELECT *` results. We emulate
/// this behaviour by checking if the SQL contains the column name. If not,
/// the column is pruned from the result batches and schema.
fn remove_virtual_system_columns(
    sql: &str,
    batches: Vec<RecordBatch>,
    schema: Arc<Schema>,
) -> (Vec<RecordBatch>, Arc<Schema>) {
    let lowered = sql.to_lowercase();
    let system_cols = ["xmin", "xmax", "ctid", "tableoid", "cmin", "cmax"];

    let mut indices: Vec<usize> = Vec::new();
    for (i, field) in schema.fields().iter().enumerate() {
        let name = field.name().to_lowercase();
        if !system_cols.contains(&name.as_str()) || lowered.contains(&name) {
            indices.push(i);
        }
    }

    if indices.len() == schema.fields().len() {
        return (batches, schema);
    }

    let fields = indices
        .iter()
        .map(|i| schema.field(*i).clone())
        .collect::<Vec<_>>();
    let new_schema = Arc::new(Schema::new(fields));

    let new_batches = batches
        .into_iter()
        .map(|b| b.project(&indices).unwrap())
        .collect();

    (new_batches, new_schema)
}

pub fn print_params(params: &Vec<Option<Bytes>>) {
    for (i, param) in params.iter().enumerate() {
        match param {
            Some(bytes) => match bytes.len() {
                4 => {
                    let v = u32::from_be_bytes(bytes[..4].try_into().unwrap());
                    log::debug!("param[{}] as u32: {}", i, v);
                }
                8 => {
                    let v = u64::from_be_bytes(bytes[..8].try_into().unwrap());
                    log::debug!("param[{}] as u64: {}", i, v);
                }
                _ => {
                    log::debug!(
                        "param[{}] raw bytes ({} bytes): {:?}",
                        i,
                        bytes.len(),
                        bytes
                    );
                }
            },
            None => {
                log::debug!("param[{}] is NULL", i);
            }
        }
    }
}

/// Run the input SQL through all available rewrite passes and return
/// the transformed query together with any alias mappings produced.
pub fn rewrite_filters(sql: &str) -> datafusion::error::Result<(String, HashMap<String, String>)> {
    let sql = replace_set_command_with_namespace(&sql)?;
    let sql = strip_default_collate(&sql)?;
    let sql = rewrite_time_zone_utc(&sql)?;
    let sql = rewrite_regoper_cast(&sql)?;
    let sql = rewrite_regoperator_cast(&sql)?;
    let sql = rewrite_regprocedure_cast(&sql)?;
    let sql = rewrite_regproc_cast(&sql)?;
    let sql = rewrite_available_updates(&sql)?;
    let sql = rewrite_array_subquery(&sql).unwrap();
    let sql = rewrite_brace_array_literal(&sql).unwrap();
    let sql = rewrite_pg_custom_operator(&sql)?;
    let sql = rewrite_schema_qualified_text(&sql)?;
    let sql = rewrite_schema_qualified_custom_types(&sql)?;
    let sql = rewrite_schema_qualified_udtfs(&sql)?;
    let sql = rewrite_char_cast(&sql)?;
    let sql = replace_regclass(&sql)?;
    let sql = rewrite_regtype_cast(&sql)?;
    let sql = rewrite_xid_cast(&sql)?;
    let sql = rewrite_name_cast(&sql)?;
    let sql = rewrite_oid_cast(&sql)?;
    let sql = rewrite_oidvector_unnest(&sql)?;
    let sql = rewrite_oidvector_any(&sql)?;
    let sql = rewrite_tuple_equality(&sql)?;
    let sql = alias_subquery_tables(&sql)?;
    let (sql, aliases) = alias_all_columns(&sql)?;
    let sql = rewrite_subquery_as_cte(&sql);

    log::debug!("before group by {}", sql);
    let sql = rewrite_group_by_for_any(&sql);

    return Ok((sql, aliases));
}

pub async fn execute_sql_inner(
    ctx: &SessionContext,
    sql: &str,
    vec: Option<Vec<Option<Bytes>>>,
    vec0: Option<Vec<Type>>,
) -> datafusion::error::Result<(Vec<RecordBatch>, Arc<Schema>)> {
    log::debug!("input sql {:?}", sql);

    let (sql, aliases) = rewrite_filters(&sql)?;

    let (sql, temp_udfs) = rewrite_query(&sql, &mut ctx.clone()).await?;

    let df = if let (Some(params), Some(types)) = (vec, vec0) {
        log::debug!("params {:?}", params);
        print_params(&params);

        let mut scalars = Vec::new();

        for (param, typ) in params.into_iter().zip(types.into_iter()) {
            let value = match (param, typ) {
                (Some(bytes), Type::INT2) => {
                    let v = i16::from_be_bytes(bytes[..].try_into().unwrap());
                    ScalarValue::Int16(Some(v))
                }
                (Some(bytes), Type::INT8) => {
                    let v = i64::from_be_bytes(bytes[..].try_into().unwrap());
                    ScalarValue::Int64(Some(v))
                }
                (Some(bytes), Type::INT4) => {
                    let v = i32::from_be_bytes(bytes[..].try_into().unwrap());
                    ScalarValue::Int32(Some(v))
                }
                (Some(bytes), Type::OID) => {
                    // OID values are 32-bit unsigned integers. We map them to
                    // BIGINT to align with `rewrite_oid_cast`, which rewrites
                    // `::oid` casts on parameters to BIGINT.
                    let v = u32::from_be_bytes(bytes[..].try_into().unwrap());
                    ScalarValue::Int64(Some(v as i64))
                }
                (Some(bytes), Type::VARCHAR)
                | (Some(bytes), Type::TEXT)
                | (Some(bytes), Type::BPCHAR)
                | (Some(bytes), Type::NAME)
                | (Some(bytes), Type::UNKNOWN) => {
                    let s = String::from_utf8(bytes.to_vec()).unwrap();
                    ScalarValue::Utf8(Some(s))
                }
                (None, Type::INT2) => ScalarValue::Int16(None),
                (None, Type::INT8) => ScalarValue::Int64(None),
                (None, Type::INT4) => ScalarValue::Int32(None),
                (None, Type::OID) => ScalarValue::Int64(None),
                (None, Type::VARCHAR)
                | (None, Type::TEXT)
                | (None, Type::BPCHAR)
                | (None, Type::NAME)
                | (None, Type::UNKNOWN) => ScalarValue::Utf8(None),
                (some, other_type) => {
                    panic!("unsupported param {:?} type {:?}", some, other_type);
                }
            };
            scalars.push(value);
        }

        let df = ctx.sql(&sql).await?.with_param_values(scalars)?;
        df
    } else {
        log::debug!("final sql {:?}", sql);
        let df = ctx.sql(&sql).await?;
        // log::debug!("executed sql");
        df
    };

    // TODO: fix scope
    let original_schema = df.schema();
    let renamed_fields = original_schema
        .fields()
        .iter()
        .map(|f| {
            let new_name = aliases
                .get(f.name())
                .map(|s| s.as_str())
                .unwrap_or_else(|| f.name().as_str());
            Field::new(new_name, f.data_type().clone(), f.is_nullable())
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(renamed_fields));

    let results = df.collect().await?;
    let results = results
        .iter()
        .map(|batch| rename_columns(batch, &aliases))
        .collect::<Vec<_>>();

    let (results, schema) = remove_virtual_system_columns(&sql, results, schema);

    // after the execution of the query we do a cleanup for added udfs
    for name in temp_udfs {
        ctx.deregister_udf(&name);
    }

    Ok((results, schema))
}

pub async fn execute_sql(
    ctx: &SessionContext,
    sql: &str,
    vec: Option<Vec<Option<Bytes>>>,
    vec0: Option<Vec<Type>>,
) -> datafusion::error::Result<(Vec<RecordBatch>, Arc<Schema>)> {
    let params_for_log = vec.clone();
    match execute_sql_inner(ctx, sql, vec, vec0).await {
        Ok(v) => Ok(v),
        Err(e) => {
            log::error!("exec_error query: {:?}", sql);
            log::error!("exec_error params: {:?}", params_for_log);
            log::error!("exec_error error: {:?}", e);
            Err(e)
        }
    }
}

fn parse_schema(
    schema_path: Option<&str>,
) -> HashMap<String, HashMap<String, HashMap<String, (SchemaRef, Vec<RecordBatch>)>>> {
    if let Some(schema_path) = schema_path {
        if schema_path.is_empty() {
            return parse_schema_zip_bytes(SCHEMA_ZIP);
        }
        let path = Path::new(schema_path);
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("zip") {
            parse_schema_zip(schema_path)
        } else if path.is_file() {
            parse_schema_file(schema_path)
        } else if path.is_dir() {
            parse_schema_dir(schema_path)
        } else {
            panic!(
                "schema_path {} is neither a file nor a directory",
                schema_path
            );
        }
    } else {
        parse_schema_zip_bytes(SCHEMA_ZIP)
    }
}

fn parse_schema_file(
    path: &str,
) -> HashMap<String, HashMap<String, HashMap<String, (SchemaRef, Vec<RecordBatch>)>>> {
    let contents = fs::read_to_string(path).expect("Failed to read schema file");
    parse_schema_contents(&contents)
}

fn parse_schema_zip(
    path: &str,
) -> HashMap<String, HashMap<String, HashMap<String, (SchemaRef, Vec<RecordBatch>)>>> {
    let file = fs::File::open(path).expect("Failed to open schema zip file");
    let mut archive = ZipArchive::new(file).expect("Failed to read zip file");
    let mut all = HashMap::new();
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).expect("Invalid zip entry");
        if !entry.name().ends_with(".yaml") {
            continue;
        }
        let mut contents = String::new();
        entry
            .read_to_string(&mut contents)
            .expect("Failed to read zip entry");
        let parsed = parse_schema_contents(&contents);
        merge_schema_maps(&mut all, parsed);
    }
    all
}

fn parse_schema_zip_bytes(
    bytes: &[u8],
) -> HashMap<String, HashMap<String, HashMap<String, (SchemaRef, Vec<RecordBatch>)>>> {
    let reader = Cursor::new(bytes);
    let mut archive = ZipArchive::new(reader).expect("Failed to read zip bytes");
    let mut all = HashMap::new();
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).expect("Invalid zip entry");
        if !entry.name().ends_with(".yaml") {
            continue;
        }
        let mut contents = String::new();
        entry
            .read_to_string(&mut contents)
            .expect("Failed to read zip entry");
        let parsed = parse_schema_contents(&contents);
        merge_schema_maps(&mut all, parsed);
    }
    all
}

fn parse_schema_contents(
    contents: &str,
) -> HashMap<String, HashMap<String, HashMap<String, (SchemaRef, Vec<RecordBatch>)>>> {
    let parsed: YamlSchema = serde_yaml::from_str(contents).expect("Invalid YAML");
    parsed
        .0
        .into_iter()
        .map(|(catalog, schemas)| {
            let schemas = schemas
                .into_iter()
                .map(|(schema, tables)| {
                    let tables = tables
                        .into_iter()
                        .map(|(table, def)| {
                            let (schema_ref, batches) = build_table(def);
                            (table, (schema_ref, batches))
                        })
                        .collect();
                    (schema, tables)
                })
                .collect();
            (catalog, schemas)
        })
        .collect()
}

fn parse_schema_dir(
    dir_path: &str,
) -> HashMap<String, HashMap<String, HashMap<String, (SchemaRef, Vec<RecordBatch>)>>> {
    let mut all = HashMap::new();

    for entry in fs::read_dir(dir_path).expect("Failed to read directory") {
        let path = entry.expect("Invalid dir entry").path();
        if path.extension().and_then(|s| s.to_str()) == Some("yaml") {
            let mut partial = parse_schema_file(path.to_str().unwrap());

            merge_schema_maps(&mut all, partial);
        }
    }

    all
}

fn merge_schema_maps(
    target: &mut HashMap<String, HashMap<String, HashMap<String, (SchemaRef, Vec<RecordBatch>)>>>,
    other: HashMap<String, HashMap<String, HashMap<String, (SchemaRef, Vec<RecordBatch>)>>>,
) {
    for (catalog, schemas) in other {
        let catalog_entry = target.entry(catalog).or_insert_with(HashMap::new);
        for (schema, tables) in schemas {
            let schema_entry = catalog_entry.entry(schema).or_insert_with(HashMap::new);
            schema_entry.extend(tables);
        }
    }
}

fn build_table(def: TableDef) -> (SchemaRef, Vec<RecordBatch>) {
    let mut fields: Vec<Field> = def
        .schema
        .iter()
        .map(|(col, typ)| {
            let mapped_typ = def
                .pg_types
                .as_ref()
                .and_then(|m| m.get(col))
                .map(|s| s.as_str())
                .filter(|t| *t == "bytea")
                .unwrap_or(typ);
            Field::new(col, map_pg_type(mapped_typ), true)
        })
        .collect();

    let is_system_catalog = matches!(def.table_type.as_deref(), Some("system_catalog"));

    let record_batches = if let Some(rows) = def.rows {
        let mut cols: Vec<Vec<serde_json::Value>> = vec![vec![]; fields.len()];
        for row in rows {
            for (i, field) in fields.iter().enumerate() {
                cols[i].push(
                    row.get(field.name())
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                );
            }
        }

        let arrays = fields
            .iter()
            .zip(cols.into_iter())
            .map(|(field, col_data)| {
                use arrow::array::*;
                use arrow::datatypes::DataType;
                let array: ArrayRef = match field.data_type() {
                    DataType::Utf8 => Arc::new(StringArray::from(
                        col_data
                            .into_iter()
                            .map(|v| v.as_str().map(|s| s.to_string()))
                            .collect::<Vec<_>>(),
                    )),
                    DataType::Int32 => Arc::new(Int32Array::from(
                        col_data
                            .into_iter()
                            .map(|v| v.as_i64().map(|i| i as i32))
                            .collect::<Vec<_>>(),
                    )),
                    DataType::Int64 => Arc::new(Int64Array::from(
                        col_data.into_iter().map(|v| v.as_i64()).collect::<Vec<_>>(),
                    )),
                    DataType::Boolean => Arc::new(BooleanArray::from(
                        col_data
                            .into_iter()
                            .map(|v| v.as_bool())
                            .collect::<Vec<_>>(),
                    )),
                    DataType::Binary => {
                        let mut builder = BinaryBuilder::new();
                        for v in col_data {
                            match v.as_str() {
                                Some(s) => builder.append_value(s.as_bytes()),
                                None => builder.append_null(),
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    DataType::List(inner) if inner.data_type() == &DataType::Utf8 => {
                        let mut builder = ListBuilder::new(StringBuilder::new());
                        for v in col_data {
                            if let Some(items) = v.as_array() {
                                for item in items {
                                    match item.as_str() {
                                        Some(s) => builder.values().append_value(s),
                                        None => builder.values().append_null(),
                                    }
                                }
                                builder.append(true);
                            } else if v.is_null() {
                                builder.append(false);
                            } else {
                                builder.values().append_value(v.to_string());
                                builder.append(true);
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    DataType::List(inner) if inner.data_type() == &DataType::Int64 => {
                        let mut builder = ListBuilder::new(Int64Builder::new());
                        for v in col_data {
                            if let Some(items) = v.as_array() {
                                for item in items {
                                    match item.as_i64() {
                                        Some(num) => builder.values().append_value(num),
                                        None => builder.values().append_null(),
                                    }
                                }
                                builder.append(true);
                            } else if let Some(s) = v.as_str() {
                                for part in s.split_whitespace() {
                                    match part.parse::<i64>() {
                                        Ok(num) => builder.values().append_value(num),
                                        Err(_) => builder.values().append_null(),
                                    }
                                }
                                builder.append(true);
                            } else if v.is_null() {
                                builder.append(false);
                            } else {
                                builder.append(false);
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    DataType::List(inner) if inner.data_type() == &DataType::Int32 => {
                        let mut builder = ListBuilder::new(Int32Builder::new());
                        for v in col_data {
                            if let Some(items) = v.as_array() {
                                for item in items {
                                    match item.as_i64() {
                                        Some(num) => builder.values().append_value(num as i32),
                                        None => builder.values().append_null(),
                                    }
                                }
                                builder.append(true);
                            } else if let Some(s) = v.as_str() {
                                for part in s.split_whitespace() {
                                    match part.parse::<i32>() {
                                        Ok(num) => builder.values().append_value(num),
                                        Err(_) => builder.values().append_null(),
                                    }
                                }
                                builder.append(true);
                            } else if v.is_null() {
                                builder.append(false);
                            } else {
                                builder.append(false);
                            }
                        }
                        Arc::new(builder.finish())
                    }

                    _ => Arc::new(StringArray::from(
                        col_data
                            .into_iter()
                            .map(|v| Some(v.to_string()))
                            .collect::<Vec<_>>(),
                    )),
                };
                array
            })
            .collect::<Vec<_>>();

        let mut arrays = arrays;

        if is_system_catalog {
            let row_count = arrays.first().map(|a| a.len()).unwrap_or(0);
            let system_cols = ["xmin", "xmax", "ctid", "tableoid", "cmin", "cmax"];
            for col in system_cols {
                if !fields.iter().any(|f| f.name() == col) {
                    fields.push(Field::new(col, DataType::Int32, true));
                    let data = vec![Some(1); row_count];
                    arrays.push(Arc::new(Int32Array::from(data)) as ArrayRef);
                }
            }
        }

        let schema_ref = Arc::new(Schema::new(fields.clone()));
        vec![RecordBatch::try_new(schema_ref.clone(), arrays).unwrap()]
    } else {
        if is_system_catalog {
            let system_cols = ["xmin", "xmax", "ctid", "tableoid", "cmin", "cmax"];
            for col in system_cols {
                if !fields.iter().any(|f| f.name() == col) {
                    fields.push(Field::new(col, DataType::Int32, true));
                }
            }
        }

        let schema_ref = Arc::new(Schema::new(fields.clone()));
        vec![RecordBatch::new_empty(schema_ref.clone())]
    };
    (Arc::new(Schema::new(fields)), record_batches)
}

fn register_catalogs_from_schemas(
    ctx: &SessionContext,
    schemas: HashMap<String, HashMap<String, HashMap<String, (Arc<Schema>, Vec<RecordBatch>)>>>,
    default_catalog: String,
    log: Arc<Mutex<Vec<ScanTrace>>>,
) -> datafusion::error::Result<&SessionContext, DataFusionError> {
    for (catalog_name, schemas) in schemas {
        // "public" is the *database* name we used in exports
        // so we copy the schema/tables under that database to default_catalog/database
        let current_catalog = if catalog_name.clone() == "public" {
            default_catalog.to_string()
        } else {
            catalog_name.clone()
        };

        let catalog_provider = if let Some(catalog_provider) = ctx.catalog(&current_catalog) {
            catalog_provider
        } else {
            let catalog_provider = Arc::new(MemoryCatalogProvider::new());
            ctx.register_catalog(&current_catalog, catalog_provider.clone());
            catalog_provider
        };

        for (schema_name, tables) in schemas {
            let schema_provider =
                if let Some(schema_provider) = catalog_provider.schema(&schema_name) {
                    schema_provider
                } else {
                    Arc::new(MemorySchemaProvider::new())
                };

            let _ = catalog_provider.register_schema(&schema_name, schema_provider.clone());
            log::debug!(
                "catalog/database: {:?} schema: {:?}",
                current_catalog,
                schema_name
            );

            for (table, (schema_ref, batches)) in tables {
                log::debug!("-- table {:?}", &table);

                let wrapped =
                    ObservableMemTable::new(table.clone(), schema_ref, log.clone(), batches);
                schema_provider.register_table(table, Arc::new(wrapped))?;
            }
        }
    }
    Ok(ctx)
}

fn default_current_schemas(ctx: &SessionContext) -> Vec<String> {
    let state = ctx.state();
    let options = state.config_options();
    let default_schema = options.catalog.default_schema.clone();
    let user_schema = if default_schema == "pg_catalog" {
        "public".to_string()
    } else {
        default_schema
    };
    vec!["pg_catalog".to_string(), user_schema]
}

pub async fn setup_context(
    ctx: &mut SessionContext,
    schema_path: Option<&str>,
    current_schemas_getter: Option<Arc<dyn Fn(&SessionContext) -> Vec<String> + Send + Sync>>,
) -> datafusion::error::Result<Arc<Mutex<Vec<ScanTrace>>>> {
    let _current_schemas_getter: Arc<dyn Fn(&SessionContext) -> Vec<String> + Send + Sync> =
        current_schemas_getter.unwrap_or_else(|| Arc::new(default_current_schemas));

    let log: Arc<Mutex<Vec<ScanTrace>>> = Arc::new(Mutex::new(Vec::new()));

    let schemas = parse_schema(schema_path);

    let session_config = ctx
        .copied_config()
        .with_option_extension(ClientOpts::default())
        .set_bool("datafusion.catalog.information_schema", false);

    *ctx = SessionContext::new_with_state(
        ctx.clone()
            .into_state_builder()
            .with_config(session_config)
            .build(),
    );

    register_catalogs_from_schemas(
        &ctx,
        schemas,
        ctx.copied_config()
            .options()
            .catalog
            .default_catalog
            .clone(),
        log.clone(),
    )?;

    for f in regclass_udfs(&ctx) {
        ctx.register_udf(f);
    }

    ctx.register_udtf(
        "regclass_oid",
        Arc::new(crate::user_functions::RegClassOidFunc),
    );

    register_scalar_regclass_oid(&ctx)?;
    register_scalar_pg_tablespace_location(&ctx)?;
    register_scalar_format_type(&ctx)?;
    ctx.register_udtf(
        "regclass_oid",
        Arc::new(crate::user_functions::RegClassOidFunc),
    );

    register_current_schema(&ctx, _current_schemas_getter.clone())?;
    register_current_schemas(&ctx, _current_schemas_getter.clone())?;

    register_scalar_format_type(&ctx)?;
    register_scalar_pg_get_expr(&ctx)?;
    register_scalar_pg_get_partkeydef(&ctx)?;
    register_scalar_pg_table_is_visible(&ctx)?;
    register_scalar_pg_get_userbyid(&ctx)?;
    register_scalar_pg_encoding_to_char(&ctx)?;
    register_scalar_array_to_string(&ctx)?;
    register_pg_get_one(&ctx)?;
    register_pg_get_array(&ctx)?;
    register_oidvector_to_array(&ctx)?;
    register_array_agg(&ctx)?;
    register_pg_get_statisticsobjdef_columns(&ctx)?;
    register_pg_relation_is_publishable(&ctx)?;
    register_has_database_privilege(&ctx)?;
    register_has_schema_privilege(&ctx)?;
    register_pg_postmaster_start_time(&ctx)?;
    register_pg_relation_size(&ctx)?;
    register_pg_total_relation_size(&ctx)?;
    register_scalar_pg_age(&ctx)?;
    register_scalar_pg_is_in_recovery(&ctx)?;
    register_scalar_txid_current(&ctx)?;
    register_quote_ident(&ctx)?;
    register_translate(&ctx)?;
    register_pg_available_extension_versions(&ctx)?;
    register_pg_get_keywords(&ctx)?;
    register_pg_get_viewdef(&ctx)?;
    register_pg_get_function_arguments(&ctx)?;
    register_pg_get_function_result(&ctx)?;
    register_pg_get_function_sqlbody(&ctx)?;
    register_pg_get_indexdef(&ctx)?;
    register_pg_get_triggerdef(&ctx)?;
    register_pg_get_ruledef(&ctx)?;
    register_encode(&ctx)?;
    register_upper(&ctx)?;
    register_version_fn(&ctx)?;

    let catalogs = ctx.catalog_names();
    log::info!("registered catalogs: {:?}", catalogs);

    // println!("Current catalog: {}", default_catalog);

    Ok(log)
}

pub async fn get_base_session_context(
    schema_path: Option<&str>,
    default_catalog: String,
    default_schema: String,
    current_schemas_getter: Option<Arc<dyn Fn(&SessionContext) -> Vec<String> + Send + Sync>>,
) -> datafusion::error::Result<(SessionContext, Arc<Mutex<Vec<ScanTrace>>>)> {
    let session_config = datafusion::execution::context::SessionConfig::new()
        .with_default_catalog_and_schema(&default_catalog, &default_schema);

    let mut ctx: SessionContext = SessionContext::new_with_config(session_config);

    let log = setup_context(&mut ctx, schema_path, current_schemas_getter).await?;

    Ok((ctx, log))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::DataType;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parse_schema_file() {
        let yaml = r#"
public:
  myschema:
    employees:
      type: table
      schema:
        id: int
        name: varchar
      rows:
        - id: 1
          name: Alice
        - id: 2
          name: Bob
"#;

        let mut file = NamedTempFile::new().unwrap();
        write!(file, "{}", yaml).unwrap();

        let parsed = parse_schema_file(file.path().to_str().unwrap());

        let myschema = parsed.get("public").unwrap().get("myschema").unwrap();
        let (schema_ref, batches) = myschema.get("employees").unwrap();

        let fields = schema_ref.fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "id");
        assert_eq!(fields[0].data_type(), &DataType::Int32);
        assert_eq!(fields[1].name(), "name");
        assert_eq!(fields[1].data_type(), &DataType::Utf8);

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);

        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");
    }

    #[test]
    fn test_rename_columns_all() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
            ],
        )
        .unwrap();

        let mut map = HashMap::new();
        map.insert("a".to_string(), "alpha".to_string());
        map.insert("b".to_string(), "beta".to_string());

        let renamed = rename_columns(&batch, &map);

        assert_eq!(renamed.schema().field(0).name(), "alpha");
        assert_eq!(renamed.schema().field(1).name(), "beta");

        assert!(Arc::ptr_eq(batch.column(0), renamed.column(0)));
        assert!(Arc::ptr_eq(batch.column(1), renamed.column(1)));
    }

    #[test]
    fn test_parse_schema_text_array() {
        use arrow::array::ListArray;
        let yaml = r#"
public:
  myschema:
    cfgtable:
      type: table
      schema:
        cfg: _text
      rows:
        - cfg:
            - "x"
            - "y"
"#;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut file, yaml.as_bytes()).unwrap();

        let parsed = parse_schema_file(file.path().to_str().unwrap());
        let myschema = parsed.get("public").unwrap().get("myschema").unwrap();
        let (schema_ref, batches) = myschema.get("cfgtable").unwrap();

        let field = &schema_ref.fields()[0];
        assert!(matches!(field.data_type(), DataType::List(_)));

        let batch = &batches[0];
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let binding = list.value(0);
        let inner = binding.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(inner.value(0), "x");
        assert_eq!(inner.value(1), "y");
    }

    #[test]
    fn test_rename_columns_partial() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef,
            ],
        )
        .unwrap();

        let mut map = HashMap::new();
        map.insert("name".to_string(), "username".to_string());

        let renamed = rename_columns(&batch, &map);

        assert_eq!(renamed.schema().field(0).name(), "id");
        assert_eq!(renamed.schema().field(1).name(), "username");

        assert!(Arc::ptr_eq(batch.column(0), renamed.column(0)));
        assert!(Arc::ptr_eq(batch.column(1), renamed.column(1)));
    }

    #[test]
    fn test_remove_virtual_system_columns() {
        use arrow::array::Int32Array;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("xmin", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int32Array::from(vec![42])) as ArrayRef,
            ],
        )
        .unwrap();

        let (out, out_schema) =
            remove_virtual_system_columns("SELECT * FROM t", vec![batch.clone()], schema.clone());
        assert_eq!(out_schema.fields().len(), 1);
        assert_eq!(out_schema.field(0).name(), "id");
        assert_eq!(out[0].num_columns(), 1);

        // when the result already only contains the system column, it should be preserved
        let xmin_schema = Arc::new(Schema::new(vec![Field::new(
            "xmin",
            DataType::Int32,
            false,
        )]));
        let xmin_batch = RecordBatch::try_new(
            xmin_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![42])) as ArrayRef],
        )
        .unwrap();

        let (out, out_schema) = remove_virtual_system_columns(
            "SELECT xmin FROM t",
            vec![xmin_batch.clone()],
            xmin_schema.clone(),
        );
        assert_eq!(out_schema.fields().len(), 1);
        assert_eq!(out_schema.field(0).name(), "xmin");
        assert_eq!(out[0].num_columns(), 1);
    }
}
