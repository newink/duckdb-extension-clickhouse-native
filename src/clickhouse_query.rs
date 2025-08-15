use clickhouse::{Client, Row};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use serde_derive::{Deserialize, Serialize};
use std::{error::Error, sync::Arc};
use tokio::runtime::Runtime;

// Use DuckDB's native decimal support through the LogicalTypeHandle

#[derive(Debug, Row, Serialize, Deserialize)]
struct DescribeResult {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    default_type: String,
    default_expression: String,
    comment: String,
    codec_expression: String,
    ttl_expression: String,
}

fn create_clickhouse_client(url: &str, user: &str, password: &str) -> Result<Client, Box<dyn Error>> {
    let client = Client::default()
        .with_url(url)
        .with_user(user)
        .with_password(password);
    Ok(client)
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current_field = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                if in_quotes && chars.peek() == Some(&'"') {
                    // Escaped quote
                    current_field.push('"');
                    chars.next(); // consume the second quote
                } else {
                    // Toggle quote state
                    in_quotes = !in_quotes;
                }
            },
            ',' if !in_quotes => {
                // End of field
                fields.push(current_field.trim().to_string());
                current_field.clear();
            },
            _ => {
                current_field.push(c);
            }
        }
    }
    
    // Add the last field
    fields.push(current_field.trim().to_string());
    fields
}

fn map_clickhouse_type_string(type_str: &str) -> LogicalTypeId {
    match type_str {
        "Bool" => LogicalTypeId::Boolean,
        "Int8" => LogicalTypeId::Tinyint,
        "Int16" => LogicalTypeId::Smallint,
        "Int32" => LogicalTypeId::Integer,
        "Int64" => LogicalTypeId::Bigint,
        "UInt8" => LogicalTypeId::UTinyint,
        "UInt16" => LogicalTypeId::USmallint,
        "UInt32" => LogicalTypeId::UInteger,
        "UInt64" => LogicalTypeId::UBigint,
        "Float32" => LogicalTypeId::Float,
        "Float64" => LogicalTypeId::Double,
        "String" => LogicalTypeId::Varchar,
        "Date" => LogicalTypeId::Date,
        "UUID" => LogicalTypeId::Uuid,
        s if s.starts_with("DateTime") => LogicalTypeId::Timestamp,
        s if s.starts_with("FixedString") => LogicalTypeId::Varchar,
        s if s.starts_with("Decimal") => LogicalTypeId::Decimal,
        s if s.starts_with("Nullable") => {
            let inner_type = s.trim_start_matches("Nullable(").trim_end_matches(')');
            // Map the inner type but mark it as nullable
            map_clickhouse_type_string(inner_type)
        },
        s if s.starts_with("Array") => LogicalTypeId::List,
        s if s.starts_with("Map") => LogicalTypeId::Map,
        _ => LogicalTypeId::Varchar,
    }
}

fn parse_decimal_params(type_str: &str) -> Option<(u8, u8)> {
    if let Some(params_start) = type_str.find('(') {
        if let Some(params_end) = type_str.find(')') {
            let params = &type_str[params_start + 1..params_end];
            let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();
            if parts.len() == 2 {
                if let (Ok(precision), Ok(scale)) = (parts[0].parse::<u8>(), parts[1].parse::<u8>()) {
                    return Some((precision, scale));
                }
            }
        }
    }
    None
}

struct ClickHouseQueryVTab;

impl VTab for ClickHouseQueryVTab {
    type InitData = ClickHouseQueryInitData;
    type BindData = ClickHouseQueryBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let query = bind.get_parameter(0).to_string();
        let url = bind
            .get_named_parameter("url")
            .map(|v| v.to_string())
            .unwrap_or_else(|| {
                std::env::var("CLICKHOUSE_URL")
                    .unwrap_or_else(|_| "http://localhost:8123".to_string())
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

        let describe_results = runtime.block_on(async {
            let client = create_clickhouse_client(&url, &user, &password)?;
            
            let describe_query = format!("DESCRIBE ({})", query);
            
            let rows = client
                .query(&describe_query)
                .fetch_all::<DescribeResult>()
                .await?;
            
            Ok::<Vec<DescribeResult>, Box<dyn Error>>(rows)
        })?;

        for describe_result in &describe_results {
            let logical_type_id = map_clickhouse_type_string(&describe_result.data_type);
            
            // Handle decimal types with proper precision/scale
            if logical_type_id == LogicalTypeId::Decimal {
                if let Some((precision, scale)) = parse_decimal_params(&describe_result.data_type) {
                    // Support all decimal precisions including 128-bit (hugeint)
                    let type_handle = LogicalTypeHandle::decimal(precision, scale);
                    bind.add_result_column(&describe_result.name, type_handle);
                } else {
                    // Fallback to VARCHAR if we can't parse decimal params
                    let type_handle = LogicalTypeHandle::from(LogicalTypeId::Varchar);
                    bind.add_result_column(&describe_result.name, type_handle);
                }
            } else {
                // Convert other complex types to VARCHAR temporarily to avoid crashes
                let final_type_id = match logical_type_id {
                    LogicalTypeId::Date | LogicalTypeId::Timestamp | LogicalTypeId::Uuid | LogicalTypeId::List | LogicalTypeId::Map => {
                        LogicalTypeId::Varchar
                    },
                    _ => logical_type_id,
                };
                let type_handle = LogicalTypeHandle::from(final_type_id);
                bind.add_result_column(&describe_result.name, type_handle);
            }
        }

        Ok(ClickHouseQueryBindData {
            url,
            user,
            password,
            query,
            describe_results,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = info.get_bind_data::<ClickHouseQueryBindData>();
        let bind_data = unsafe { &*bind_data };

        let runtime = Arc::new(Runtime::new().map_err(|e| format!("Failed to create runtime: {}", e))?);

        let result = runtime.block_on(async {
            let client = create_clickhouse_client(&bind_data.url, &bind_data.user, &bind_data.password)?;
            
            let mut cursor = client
                .query(&bind_data.query)
                .fetch_bytes("CSV")?;
                
            let csv_bytes = cursor.collect().await?;
            let csv_string = String::from_utf8(csv_bytes.to_vec())?;
            let lines: Vec<&str> = csv_string.lines().collect();
            
            let mut csv_data: Vec<Vec<String>> = Vec::new();
            let num_columns = bind_data.describe_results.len();
            
            for _ in 0..num_columns {
                csv_data.push(Vec::new());
            }
            
            for line in lines.iter() {
                let fields = parse_csv_line(line);
                
                for (col_idx, field) in fields.iter().enumerate() {
                    if col_idx < num_columns {
                        csv_data[col_idx].push(field.to_string());
                    }
                }
            }
            
            let total_rows = if csv_data.is_empty() { 0 } else { csv_data[0].len() };
            
            Ok::<(Vec<Vec<String>>, usize), Box<dyn Error>>((csv_data, total_rows))
        })?;

        let (csv_data, total_rows) = result;
        let mut column_types = Vec::new();
        let mut decimal_params = Vec::new();
        
        for desc in &bind_data.describe_results {
            let logical_type_id = map_clickhouse_type_string(&desc.data_type);
            
            if logical_type_id == LogicalTypeId::Decimal {
                if let Some((precision, scale)) = parse_decimal_params(&desc.data_type) {
                    // Support all decimal precisions including 128-bit
                    column_types.push(LogicalTypeId::Decimal);
                    decimal_params.push(Some((precision, scale)));
                } else {
                    column_types.push(LogicalTypeId::Varchar);
                    decimal_params.push(None);
                }
            } else {
                // Apply same conversion as bind phase
                let final_type_id = match logical_type_id {
                    LogicalTypeId::Date | LogicalTypeId::Timestamp | LogicalTypeId::Uuid | LogicalTypeId::List | LogicalTypeId::Map => {
                        LogicalTypeId::Varchar
                    },
                    _ => logical_type_id,
                };
                column_types.push(final_type_id);
                decimal_params.push(None);
            }
        }
        
        Ok(ClickHouseQueryInitData {
            runtime: Some(runtime),
            csv_data: Some(csv_data),
            column_types,
            decimal_params,
            current_row: 0,
            total_rows,
            done: false,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data() as *const ClickHouseQueryInitData as *mut ClickHouseQueryInitData;

        unsafe {
            if (*init_data).done || (*init_data).current_row >= (*init_data).total_rows {
                output.set_len(0);
                (*init_data).done = true;
                return Ok(());
            }

            let csv_data = match (*init_data).csv_data.as_ref() {
                Some(data) => data,
                None => return Err("CSV data is not available".into()),
            };

            let batch_size = 1024.min((*init_data).total_rows - (*init_data).current_row);

            for col_idx in 0..(*init_data).column_types.len() {
                let mut vector = output.flat_vector(col_idx);
                let type_id = &(&(*init_data).column_types)[col_idx];

                match type_id {
                    LogicalTypeId::Tinyint => {
                        for row_offset in 0..batch_size {
                            let row_idx = (*init_data).current_row + row_offset;
                            let val_str = &csv_data[col_idx][row_idx];
                            if val_str == "\\N" || val_str == "\\\\N" {
                                let slice = vector.as_mut_slice::<i8>();
                                slice[row_offset] = 0;
                                let _ = slice;
                                vector.set_null(row_offset);
                            } else {
                                let val = val_str.parse::<i8>().unwrap_or(0);
                                let slice = vector.as_mut_slice::<i8>();
                                slice[row_offset] = val;
                            }
                        }
                    }
                    LogicalTypeId::Decimal => {
                        if let Some(Some((precision, scale))) 
                            = (&(*init_data).decimal_params).get(col_idx)
                        {
                            for row_offset in 0..batch_size {
                                let row_idx = (*init_data).current_row + row_offset;
                                let val_str = &csv_data[col_idx][row_idx];

                                if val_str == "\\N" || val_str == "\\\\N" {
                                    vector.set_null(row_offset);
                                } else if let Ok(decimal_val) = val_str.parse::<f64>() {
                                    let scale_factor = if *scale <= 18 {
                                        10_i64.pow(*scale as u32)
                                    } else {
                                        // For very large scales, use i128::MAX to avoid overflow
                                        i64::MAX
                                    };

                                    if *precision <= 4 {
                                        let scaled_val = (decimal_val * scale_factor as f64).round() as i16;
                                        let slice = vector.as_mut_slice::<i16>();
                                        slice[row_offset] = scaled_val;
                                    } else if *precision <= 9 {
                                        let scaled_val = (decimal_val * scale_factor as f64).round() as i32;
                                        let slice = vector.as_mut_slice::<i32>();
                                        slice[row_offset] = scaled_val;
                                    } else if *precision <= 18 {
                                        let scaled_val = (decimal_val * scale_factor as f64).round() as i64;
                                        let slice = vector.as_mut_slice::<i64>();
                                        slice[row_offset] = scaled_val;
                                    } else {
                                        // INT128 storage for precision > 18
                                        // Use bigdecimal for arbitrary precision decimal arithmetic
                                        use bigdecimal::{BigDecimal, ToPrimitive};
                                        use std::str::FromStr;

                                        if let Ok(big_decimal) = BigDecimal::from_str(val_str) {
                                            // Scale by multiplying by 10^scale
                                            let scale_factor = if *scale <= 18 {
                                                BigDecimal::from(10_i64.pow(*scale as u32))
                                            } else {
                                                // For very large scales, build the scale factor iteratively
                                                let mut factor = BigDecimal::from(1);
                                                for _ in 0..*scale {
                                                    factor = factor * BigDecimal::from(10);
                                                }
                                                factor
                                            };
                                            let scaled_decimal = big_decimal * scale_factor;
                                            
                                            // Convert to i128
                                            if let Some(scaled_i128) = scaled_decimal.to_i128() {
                                                let slice = vector.as_mut_slice::<i128>();
                                                slice[row_offset] = scaled_i128;
                                            } else {
                                                // Value too large even for i128, set to 0 and null
                                                let slice = vector.as_mut_slice::<i128>();
                                                slice[row_offset] = 0;
                                                let _ = slice;
                                                vector.set_null(row_offset);
                                            }
                                        } else {
                                            // Parse failed, set to 0
                                            let slice = vector.as_mut_slice::<i128>();
                                            slice[row_offset] = 0;
                                        }
                                    }
                                } else {
                                    if *precision <= 4 {
                                        let slice = vector.as_mut_slice::<i16>();
                                        slice[row_offset] = 0;
                                    } else if *precision <= 9 {
                                        let slice = vector.as_mut_slice::<i32>();
                                        slice[row_offset] = 0;
                                    } else if *precision <= 18 {
                                        let slice = vector.as_mut_slice::<i64>();
                                        slice[row_offset] = 0;
                                    } else {
                                        let slice = vector.as_mut_slice::<i128>();
                                        slice[row_offset] = 0;
                                    }
                                }
                            }
                        } else {
                            for row_offset in 0..batch_size {
                                let row_idx = (*init_data).current_row + row_offset;
                                let val_str = &csv_data[col_idx][row_idx];
                                let final_val = if val_str == "\\N" || val_str == "\\\\N" { "0.0" } else { val_str };
                                vector.insert(row_offset, final_val);
                            }
                        }
                    }
                    _ => {
                        for row_offset in 0..batch_size {
                            let row_idx = (*init_data).current_row + row_offset;
                            let val = csv_data[col_idx][row_idx].as_str();
                            let final_val = if val == "\\N" { "" } else { val };
                            vector.insert(row_offset, final_val);
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


#[repr(C)]
struct ClickHouseQueryBindData {
    url: String,
    user: String,
    password: String,
    query: String,
    describe_results: Vec<DescribeResult>,
}

#[repr(C)]
struct ClickHouseQueryInitData {
    runtime: Option<Arc<Runtime>>,
    csv_data: Option<Vec<Vec<String>>>,
    column_types: Vec<LogicalTypeId>,
    decimal_params: Vec<Option<(u8, u8)>>, // Store (precision, scale) for decimal columns
    current_row: usize,
    total_rows: usize,
    done: bool,
}

const FUNCTION_NAME: &'static str = "clickhouse_query";

pub fn register_clickhouse_query(con: &Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ClickHouseQueryVTab>(FUNCTION_NAME)?;
    Ok(())
}