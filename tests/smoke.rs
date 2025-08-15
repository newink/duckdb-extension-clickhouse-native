mod common;

#[test]
fn boots_clickhouse() {
    let ch = common::clickhouse();
    // Basic sanity on URL format
    assert!(ch.url.contains("127.0.0.1:"));
}

#[test]
fn duckdb_in_memory_works() {
    common::with_duckdb(|conn| {
        // Simple SQL to validate the connection
        let mut stmt = conn.prepare("SELECT 1 + 2").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let v: i64 = row.get::<_, i64>(0).unwrap();
        assert_eq!(v, 3);
    });
}

#[test]
fn test_clickhouse_extension_load_and_query() {
    // First ensure we have a ClickHouse server running
    let _ch = common::clickhouse();

    common::with_duckdb(|conn| {
        // Load the built extension
        let extension_path = std::env::current_dir()
            .unwrap()
            .join("build/release/chsql_native.duckdb_extension");

        let load_sql = format!("LOAD '{}'", extension_path.display());
        println!("Loading extension: {}", load_sql);

        conn.execute(&load_sql, [])
            .expect("Failed to load extension");

        // Test clickhouse_query function with a simple query
        let query_sql =
            "SELECT * FROM clickhouse_query('SELECT 1 as test_col, ''hello'' as str_col')";
        println!("Executing query: {}", query_sql);

        let mut stmt = conn.prepare(query_sql).expect("Failed to prepare query");
        
        // Get column info before executing query
        let column_count = stmt.column_count();
        let mut column_names = Vec::new();
        for i in 0..column_count {
            column_names.push(stmt.column_name(i).expect("Failed to get column name").clone());
        }
        
        let mut rows = stmt.query([]).expect("Failed to execute query");

        println!("Results:");
        let mut row_count = 0;
        while let Ok(Some(row)) = rows.next() {
            row_count += 1;
            println!("Row {}:", row_count);

            for i in 0..column_count {
                let column_name = &column_names[i];

                // Try to get value as different types and print
                let value_str = if let Ok(val) = row.get::<_, i64>(i) {
                    val.to_string()
                } else if let Ok(val) = row.get::<_, String>(i) {
                    val.to_string()
                } else if let Ok(val) = row.get::<_, f64>(i) {
                    val.to_string()
                } else {
                    "NULL".to_string()
                };

                println!("  {}: {}", column_name, value_str);
            }
        }

        println!("Total rows: {}", row_count);
        assert!(row_count > 0, "Query should return at least one row");
    });
}

#[test]
fn test_clickhouse_query_all_types() {
    // Ensure ClickHouse server is running and setup test data
    let _ch = common::clickhouse();
    
    // Setup test data in a blocking runtime
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async {
        common::setup_clickhouse_test_data().await
            .expect("Failed to setup ClickHouse test data");
    });

    common::with_duckdb(|conn| {
        // Load the built extension
        let extension_path = std::env::current_dir()
            .unwrap()
            .join("build/release/chsql_native.duckdb_extension");

        let load_sql = format!("LOAD '{}'", extension_path.display());
        println!("Loading extension: {}", load_sql);

        conn.execute(&load_sql, [])
            .expect("Failed to load extension");

        // Query all types from the all_types_test table
        let query_sql = "SELECT * FROM clickhouse_query('SELECT * FROM all_types_test ORDER BY int32_col')";
        println!("Executing comprehensive all_types query: {}", query_sql);

        let mut stmt = conn.prepare(query_sql).expect("Failed to prepare all_types query");
        
        // Get column info before executing query
        let column_count = stmt.column_count();
        println!("Column count: {}", column_count);

        // Print column names and types - collect them first
        let mut column_names = Vec::new();
        println!("Columns:");
        for i in 0..column_count {
            let column_name = stmt.column_name(i).expect("Failed to get column name").clone();
            println!("  [{}]: {}", i, column_name);
            column_names.push(column_name);
        }
        
        let mut rows = stmt.query([]).expect("Failed to execute all_types query");

        let mut row_count = 0;
        println!("Results:");
        while let Ok(Some(row)) = rows.next() {
            row_count += 1;
            println!("Row {}:", row_count);

            for i in 0..column_count {
                let column_name = &column_names[i];

                // Try to get value as different types
                let value_str = if let Ok(val) = row.get::<_, i64>(i) {
                    val.to_string()
                } else if let Ok(val) = row.get::<_, String>(i) {
                    val.to_string()
                } else if let Ok(val) = row.get::<_, f64>(i) {
                    val.to_string()
                } else if let Ok(val) = row.get::<_, bool>(i) {
                    if val { "1" } else { "0" }.to_string()
                } else {
                    "NULL".to_string()
                };

                println!("  {}: {}", column_name, value_str);
                
                // Basic type validation - ensure we can read the values without errors
                match column_name.as_str() {
                    name if name.ends_with("_col") && name.contains("int") => {
                        // Integer types should parse as numbers
                        if value_str != "NULL" {
                            assert!(value_str.parse::<i64>().is_ok() || value_str.parse::<u64>().is_ok(),
                                "Integer column {} should contain numeric value, got: {}", name, value_str);
                        }
                    },
                    name if name.ends_with("_col") && name.contains("float") => {
                        // Float types should parse as numbers
                        if value_str != "NULL" {
                            assert!(value_str.parse::<f64>().is_ok(),
                                "Float column {} should contain numeric value, got: {}", name, value_str);
                        }
                    },
                    name if name.ends_with("_col") && name.contains("bool") => {
                        // Bool types should be 0 or 1
                        assert!(value_str == "0" || value_str == "1",
                            "Bool column {} should be 0 or 1, got: {}", name, value_str);
                    },
                    name if name.ends_with("_col") && name.contains("string") => {
                        // String types should be readable (no specific validation needed)
                    },
                    name if name.ends_with("_col") && (name.contains("date") || name.contains("time")) => {
                        // Date/time types should contain date-like strings
                        if value_str != "NULL" {
                            assert!(value_str.contains("-") || value_str.contains(":"),
                                "Date/time column {} should contain date/time format, got: {}", name, value_str);
                        }
                    },
                    name if name.ends_with("_col") && name.contains("uuid") => {
                        // UUID should contain hyphens
                        if value_str != "NULL" {
                            assert!(value_str.contains("-"),
                                "UUID column {} should contain UUID format, got: {}", name, value_str);
                        }
                    },
                    name if name.ends_with("_col") && name.contains("ip") => {
                        // IP addresses should contain dots or colons
                        if value_str != "NULL" {
                            assert!(value_str.contains(".") || value_str.contains(":"),
                                "IP column {} should contain IP format, got: {}", name, value_str);
                        }
                    },
                    name if name.ends_with("_col") && name.contains("array") => {
                        // Arrays should be in bracket format
                        if value_str != "NULL" {
                            assert!(value_str.starts_with("[") && value_str.ends_with("]"),
                                "Array column {} should be in bracket format, got: {}", name, value_str);
                        }
                    },
                    name if name.ends_with("_col") && name.contains("map") => {
                        // Maps should be in brace format
                        if value_str != "NULL" {
                            assert!(value_str.starts_with("{") && value_str.ends_with("}"),
                                "Map column {} should be in brace format, got: {}", name, value_str);
                        }
                    },
                    _ => {
                        // Other columns - just ensure they're readable
                    }
                }
            }
        }

        println!("Total rows: {}", row_count);
        assert_eq!(row_count, 3, "Should have exactly 3 test rows");
        
        // Test specific type queries
        println!("\n=== Testing specific type queries ===");
        
        // Test integer types
        let int_query = "SELECT int8_col, int32_col, uint64_col FROM clickhouse_query('SELECT int8_col, int32_col, uint64_col FROM all_types_test WHERE int32_col = 1000')";
        let mut stmt = conn.prepare(int_query).expect("Failed to prepare integer query");
        let mut rows = stmt.query([]).expect("Failed to execute integer query");
        
        if let Ok(Some(row)) = rows.next() {
            let int8_val: i32 = row.get(0).expect("Failed to get int8 value");
            let int32_val: i32 = row.get(1).expect("Failed to get int32 value");
            let uint64_val: String = row.get(2).expect("Failed to get uint64 as string");
            
            println!("Integer values: int8={}, int32={}, uint64={}", int8_val, int32_val, uint64_val);
            assert_eq!(int8_val, -128);
            assert_eq!(int32_val, 1000);
            assert_eq!(uint64_val, "18446744073709551615");
        }
        
        // Test string types
        let string_query = "SELECT string_col, fixedstring_col FROM clickhouse_query('SELECT string_col, fixedstring_col FROM all_types_test WHERE int32_col = 1000')";
        let mut stmt = conn.prepare(string_query).expect("Failed to prepare string query");
        let mut rows = stmt.query([]).expect("Failed to execute string query");
        
        if let Ok(Some(row)) = rows.next() {
            let string_val: String = row.get(0).expect("Failed to get string value");
            let fixed_string_val: String = row.get(1).expect("Failed to get fixed string value");
            
            println!("String values: string='{}', fixedstring='{}'", string_val, fixed_string_val);
            assert_eq!(string_val, "Hello World");
            assert_eq!(fixed_string_val, "test      ");
        }
        
        // Test boolean type
        let bool_query = "SELECT bool_col FROM clickhouse_query('SELECT bool_col FROM all_types_test ORDER BY int32_col')";
        let mut stmt = conn.prepare(bool_query).expect("Failed to prepare boolean query");
        let mut rows = stmt.query([]).expect("Failed to execute boolean query");
        
        let mut bool_values = Vec::new();
        while let Ok(Some(row)) = rows.next() {
            if let Ok(bool_val) = row.get::<_, bool>(0) {
                bool_values.push(bool_val);
            } else if let Ok(int_val) = row.get::<_, i32>(0) {
                bool_values.push(int_val != 0);
            }
        }
        
        println!("Boolean values: {:?}", bool_values);
        assert_eq!(bool_values.len(), 3);
        assert_eq!(bool_values, vec![false, true, true]); // Based on test data
        
        println!("✓ All type mappings and data integrity tests passed!");
    });
}
