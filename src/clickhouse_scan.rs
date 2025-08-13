use clickhouse_rs::{types::SqlType, Pool};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use std::{error::Error, sync::Arc};
use tokio::runtime::Runtime;

#[repr(C)]
struct ClickHouseScanBindData {
    url: String,
    user: String,
    password: String,
    query: String,
    column_names: Vec<String>,
    column_types: Vec<LogicalTypeId>,
}

#[repr(C)]
struct ClickHouseScanInitData {
    runtime: Option<Arc<Runtime>>,
    block_data: Option<Vec<Vec<String>>>,
    column_types: Vec<LogicalTypeId>,
    column_names: Vec<String>,
    current_row: usize,
    total_rows: usize,
    done: bool,
}

fn map_clickhouse_type(sql_type: SqlType) -> LogicalTypeId {
    match sql_type {
        // Boolean types
        SqlType::Bool => LogicalTypeId::Boolean,
        
        // Signed integer types
        SqlType::Int8 => LogicalTypeId::Tinyint,
        SqlType::Int16 => LogicalTypeId::Smallint,
        SqlType::Int32 => LogicalTypeId::Integer,
        SqlType::Int64 => LogicalTypeId::Bigint,
        
        // Unsigned integer types
        SqlType::UInt8 => LogicalTypeId::UTinyint,
        SqlType::UInt16 => LogicalTypeId::USmallint,
        SqlType::UInt32 => LogicalTypeId::UInteger,
        SqlType::UInt64 => LogicalTypeId::UBigint,
        
        // Floating point types
        SqlType::Float32 => LogicalTypeId::Float,
        SqlType::Float64 => LogicalTypeId::Double,
        
        // String types
        SqlType::String | SqlType::FixedString(_) => LogicalTypeId::Varchar,
        
        // Date and time types
        SqlType::Date => LogicalTypeId::Date,
        SqlType::DateTime(_) => LogicalTypeId::Timestamp,
        
        // Network types - map to string representation
        SqlType::Ipv4 | SqlType::Ipv6 => LogicalTypeId::Varchar,
        
        // UUID type
        SqlType::Uuid => LogicalTypeId::Uuid,
        
        // Enum types - map to string representation
        SqlType::Enum8(_) | SqlType::Enum16(_) => LogicalTypeId::Varchar,
        
        // Decimal type
        SqlType::Decimal(_, _) => LogicalTypeId::Decimal,
        
        // Nullable types - recursively map the inner type
        SqlType::Nullable(inner_type) => map_clickhouse_type(inner_type.clone()),
        
        // Array types - map to List
        SqlType::Array(_) => LogicalTypeId::List,
        
        // Map types
        SqlType::Map(_, _) => LogicalTypeId::Map,
        
        // Complex aggregate function types - map to string for now
        SqlType::SimpleAggregateFunction(_, _) => LogicalTypeId::Varchar,
    }
}


struct ClickHouseScanVTab;

const NULL_DATETIME: &'static str = "1970-01-01T00:00:00Z";
const NULL_NUMERIC: &'static str = "0.0";
const NULL_DATE: &'static str = "1970-01-01";
const NULL_UUID: &'static str = "00000000-0000-0000-0000-000000000000";
const NULL_INT: &'static str = "0";
const NULL_BOOLEAN: &'static str = "false";

impl VTab for ClickHouseScanVTab {
    type InitData = ClickHouseScanInitData;
    type BindData = ClickHouseScanBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let query = bind.get_parameter(0).to_string();
        let url = bind
            .get_named_parameter("url")
            .map(|v| v.to_string())
            .unwrap_or_else(|| {
                std::env::var("CLICKHOUSE_URL")
                    .unwrap_or_else(|_| "tcp://localhost:9000".to_string())
            });
        let user = bind
            .get_named_parameter("user")
            .map(|v| v.to_string())
            .unwrap_or_else(|| {
                std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string())
            });
        let password = bind
            .get_named_parameter("password")
            .map(|v| v.to_string())
            .unwrap_or_else(|| std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default());

        let runtime = Arc::new(Runtime::new().map_err(|e| format!("Failed to create runtime: {}", e))?);

        let result = runtime.block_on(async {
            let pool = Pool::new(url.clone());
            let mut client = pool.get_handle().await?;
            let block = client.query(&query).fetch_all().await?;

            let columns = block.columns();
            let mut names = Vec::new();
            let mut types = Vec::new();

            for col in columns {
                names.push(col.name().to_string());
                types.push(map_clickhouse_type(col.sql_type()));
            }

            Ok::<(Vec<String>, Vec<LogicalTypeId>), Box<dyn Error>>((names, types))
        })?;

        let (names, types) = result;

        for (name, type_id) in names.iter().zip(types.iter()) {
            let logical_type = match type_id {
                LogicalTypeId::Integer => LogicalTypeId::Integer,
                LogicalTypeId::Bigint => LogicalTypeId::Bigint,
                LogicalTypeId::UInteger => LogicalTypeId::UInteger,
                LogicalTypeId::UBigint => LogicalTypeId::UBigint,
                LogicalTypeId::Float => LogicalTypeId::Float,
                LogicalTypeId::Double => LogicalTypeId::Double,
                LogicalTypeId::Varchar => LogicalTypeId::Varchar,
                LogicalTypeId::Date => LogicalTypeId::Date,
                LogicalTypeId::Timestamp => LogicalTypeId::Timestamp,
                LogicalTypeId::Boolean => LogicalTypeId::Boolean,
                _ => LogicalTypeId::Varchar,
            };
            let type_handle = LogicalTypeHandle::from(logical_type);
            bind.add_result_column(name, type_handle);
        }

        Ok(ClickHouseScanBindData {
            url,
            user,
            password,
            query,
            column_names: names,
            column_types: types,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = info.get_bind_data::<ClickHouseScanBindData>();
        let bind_data = unsafe { &*bind_data };

        let runtime = Arc::new(Runtime::new().map_err(|e| format!("Failed to create runtime: {}", e))?);

        let result = runtime.block_on(async {
            let pool = Pool::new(bind_data.url.clone());
            let mut client = pool.get_handle().await?;
            let block = client.query(&bind_data.query).fetch_all().await?;

            let columns = block.columns();
            let mut data: Vec<Vec<String>> = Vec::new();

            for _ in columns {
                data.push(Vec::new());
            }

            let mut row_count = 0;
            for row in block.rows() {
                for (col_idx, col) in columns.iter().enumerate() {
                    let value = match col.sql_type() {
                        // Boolean
                        SqlType::Bool => {
                            match row.get::<bool, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => NULL_BOOLEAN.to_string(),
                            }
                        },
                        
                        // Signed integers
                        SqlType::Int8 => {
                            match row.get::<i8, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => NULL_INT.to_string(),
                            }
                        },
                        SqlType::Int16 => {
                            match row.get::<i16, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => NULL_INT.to_string(),
                            }
                        },
                        SqlType::Int32 => {
                            match row.get::<i32, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => NULL_INT.to_string(),
                            }
                        },
                        SqlType::Int64 => {
                            match row.get::<i64, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => NULL_INT.to_string(),
                            }
                        },
                        
                        // Unsigned integers
                        SqlType::UInt8 => {
                            match row.get::<u8, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => NULL_INT.to_string(),
                            }
                        },
                        SqlType::UInt16 => {
                            match row.get::<u16, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => NULL_INT.to_string(),
                            }
                        },
                        SqlType::UInt32 => {
                            match row.get::<u32, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => NULL_INT.to_string(),
                            }
                        },
                        SqlType::UInt64 => {
                            match row.get::<u64, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => NULL_INT.to_string(),
                            }
                        },
                        
                        // Floating point
                        SqlType::Float32 => {
                            match row.get::<f32, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => NULL_NUMERIC.to_string(),
                            }
                        },
                        SqlType::Float64 => {
                            match row.get::<f64, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => NULL_NUMERIC.to_string(),
                            }
                        },
                        
                        // Strings and text types
                        SqlType::String | SqlType::FixedString(_) => {
                            row.get::<String, &str>(col.name()).unwrap_or_else(|_| String::new())
                        },
                        
                        // Date and time types - convert to string representation
                        SqlType::Date => {
                            row.get::<String, &str>(col.name()).unwrap_or_else(|_| NULL_DATE.to_string())
                        },
                        SqlType::DateTime(_) => {
                            row.get::<String, &str>(col.name()).unwrap_or_else(|_| NULL_DATETIME.to_string())
                        },
                        
                        // Network types
                        SqlType::Ipv4 | SqlType::Ipv6 => {
                            row.get::<String, &str>(col.name()).unwrap_or_else(|_| String::new())
                        },
                        
                        // UUID
                        SqlType::Uuid => {
                            row.get::<String, &str>(col.name()).unwrap_or_else(|_| NULL_UUID.to_string())
                        },
                        
                        // Enum types
                        SqlType::Enum8(_) | SqlType::Enum16(_) => {
                            row.get::<String, &str>(col.name()).unwrap_or_else(|_| String::new())
                        },
                        
                        // Decimal - try as string first
                        SqlType::Decimal(_, _) => {
                            row.get::<String, &str>(col.name()).unwrap_or_else(|_| "0".to_string())
                        },
                        
                        // Nullable types - handle based on inner type
                        SqlType::Nullable(inner_type) => {
                            match inner_type {
                                SqlType::Int64 => {
                                    match row.get::<Option<i64>, &str>(col.name()) {
                                        Ok(Some(val)) => val.to_string(),
                                        Ok(None) => String::new(), // NULL value
                                        Err(_) => {
                                            // Fallback: try as non-nullable int64
                                            match row.get::<i64, &str>(col.name()) {
                                                Ok(val) => val.to_string(),
                                                Err(_) => "0".to_string(),
                                            }
                                        }
                                    }
                                },
                                SqlType::Int32 => {
                                    match row.get::<Option<i32>, &str>(col.name()) {
                                        Ok(Some(val)) => val.to_string(),
                                        Ok(None) => String::new(), // NULL value
                                        Err(_) => {
                                            match row.get::<i32, &str>(col.name()) {
                                                Ok(val) => val.to_string(),
                                                Err(_) => "0".to_string(),
                                            }
                                        }
                                    }
                                },
                                SqlType::String | SqlType::FixedString(_) => {
                                    match row.get::<Option<String>, &str>(col.name()) {
                                        Ok(Some(val)) => val,
                                        Ok(None) => String::new(), // NULL value
                                        Err(_) => {
                                            row.get::<String, &str>(col.name()).unwrap_or_else(|_| String::new())
                                        }
                                    }
                                },
                                _ => {
                                    // For other nullable types, fall back to string handling
                                    match row.get::<Option<String>, &str>(col.name()) {
                                        Ok(Some(val)) => val,
                                        Ok(None) => String::new(),
                                        Err(_) => {
                                            row.get::<String, &str>(col.name()).unwrap_or_else(|_| String::new())
                                        }
                                    }
                                }
                            }
                        },
                        
                        // Complex types - convert to string representation for now
                        SqlType::Array(_) | SqlType::Map(_, _) | SqlType::SimpleAggregateFunction(_, _) => {
                            row.get::<String, &str>(col.name()).unwrap_or_else(|_| "[]".to_string())
                        },
                    };
                    data[col_idx].push(value);
                }
                row_count += 1;
            }

            Ok::<(Vec<Vec<String>>, usize), Box<dyn Error>>((data, row_count))
        })?;

        let (block_data, total_rows) = result;

        let column_types = bind_data.column_types.iter().map(|t| match t {
            LogicalTypeId::Integer => LogicalTypeId::Integer,
            LogicalTypeId::Bigint => LogicalTypeId::Bigint,
            LogicalTypeId::UInteger => LogicalTypeId::UInteger,
            LogicalTypeId::UBigint => LogicalTypeId::UBigint,
            LogicalTypeId::Float => LogicalTypeId::Float,
            LogicalTypeId::Double => LogicalTypeId::Double,
            LogicalTypeId::Varchar => LogicalTypeId::Varchar,
            LogicalTypeId::Date => LogicalTypeId::Date,
            LogicalTypeId::Timestamp => LogicalTypeId::Timestamp,
            LogicalTypeId::Boolean => LogicalTypeId::Boolean,
            _ => LogicalTypeId::Varchar,
        }).collect();
        let column_names = bind_data.column_names.iter().cloned().collect();

        Ok(ClickHouseScanInitData {
            runtime: Some(runtime),
            block_data: Some(block_data),
            column_types,
            column_names,
            current_row: 0,
            total_rows,
            done: false,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data() as *const ClickHouseScanInitData as *mut ClickHouseScanInitData;
        
        unsafe {
            if (*init_data).done || (*init_data).current_row >= (*init_data).total_rows {
                output.set_len(0);
                (*init_data).done = true;
                return Ok(());
            }

            let block_data = match (*init_data).block_data.as_ref() {
                Some(data) => data,
                None => return Err("Block data is not available".into()),
            };

            let batch_size = 1024.min((*init_data).total_rows - (*init_data).current_row);

            for col_idx in 0..(*init_data).column_types.len() {
                let mut vector = output.flat_vector(col_idx);
                let type_id = &(*init_data).column_types[col_idx];

                match type_id {
                    LogicalTypeId::Integer | LogicalTypeId::UInteger => {
                        let slice = vector.as_mut_slice::<i32>();
                        for row_offset in 0..batch_size {
                            let row_idx = (*init_data).current_row + row_offset;
                            let val_str = &block_data[col_idx][row_idx];

                            let val = if let Ok(v) = val_str.parse::<i32>() {
                                v
                            } else if let Ok(v) = val_str.parse::<u32>() {
                                v as i32
                            } else if let Ok(v) = i32::from_str_radix(val_str.trim(), 10) {
                                v
                            } else {
                                0
                            };
                            slice[row_offset] = val;
                        }
                    }
                    LogicalTypeId::Bigint => {
                        let slice = vector.as_mut_slice::<i64>();
                        for row_offset in 0..batch_size {
                            let row_idx = (*init_data).current_row + row_offset;
                            if let Ok(val) = block_data[col_idx][row_idx].parse::<i64>() {
                                slice[row_offset] = val;
                            } else {
                                slice[row_offset] = 0;
                            }
                        }
                    }
                    _ => {
                        for row_offset in 0..batch_size {
                            let row_idx = (*init_data).current_row + row_offset;
                            let val = block_data[col_idx][row_idx].as_str();
                            vector.insert(row_offset, val);
                        }
                    }
                }
            }

            (*init_data).current_row += batch_size;
            output.set_len(batch_size);
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

const FUNCTION_NAME: &'static str = "clickhouse_query";

pub fn register_clickhouse_scan(con: &Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ClickHouseScanVTab>(FUNCTION_NAME)?;
    Ok(())
}
