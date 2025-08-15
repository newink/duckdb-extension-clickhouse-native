use once_cell::sync::OnceCell;
use duckdb::{Config, Connection};
use std::sync::Mutex;
use testcontainers::{
    core::WaitFor,
    GenericImage,
    ImageExt,
    Container,
    runners::SyncRunner
};
use testcontainers::core::ContainerPort;
use clickhouse_rs::{ClientHandle, Pool};
use std::fs;

/// Holds the running container and derived connection info.
pub struct ClickHouseServer {
    pub url: String,
    pub user: String,
    pub password: String,
}

static CLICKHOUSE_SERVER: OnceCell<ClickHouseServer> = OnceCell::new();
static CONTAINER: OnceCell<Mutex<Option<Container<GenericImage>>>> = OnceCell::new();
static DUCKDB: OnceCell<Mutex<Option<Connection>>> = OnceCell::new();

/// Start ClickHouse in Docker once per test run and return connection info.
pub fn clickhouse() -> &'static ClickHouseServer {
    CLICKHOUSE_SERVER.get_or_init(|| {
        // Use official ClickHouse server image, expose 9000 (native TCP) and 8123 (HTTP)
        let image = GenericImage::new("clickhouse/clickhouse-server", "24.8")
            .with_wait_for(WaitFor::seconds(20))
            .with_exposed_port(ContainerPort::Tcp(9000))
            .with_exposed_port(ContainerPort::Tcp(8123))
            .with_env_var("CLICKHOUSE_DB", "default")
            .with_env_var("CLICKHOUSE_USER", "default")
            .with_env_var("CLICKHOUSE_PASSWORD", "default");

        // In testcontainers 0.25+, call .start() directly on the image
        let container = image.start().expect("Failed to start ClickHouse container");

        // Resolve the mapped port for the native protocol - handle Result
        let host_port = container
            .get_host_port_ipv4(9000)
            .expect("Failed to get host port for ClickHouse");

        let user = "default".to_string();
        let password = "default".to_string();
        let url = format!("tcp://{}:{}@127.0.0.1:{}", user, password, host_port);
        println!("ClickHouse URL: {}", url);

        // Set environment so the extension/client tests can pick it up
        std::env::set_var("CLICKHOUSE_URL", &url);
        std::env::set_var("CLICKHOUSE_USER", &user);
        std::env::set_var("CLICKHOUSE_PASSWORD", &password);

        // Save container so we can drop it during teardown
        let _ = CONTAINER.set(Mutex::new(Some(container)));

        let server = ClickHouseServer {
            url,
            user,
            password,
        };

        println!("✓ ClickHouse container started successfully");

        server
    })
}

//noinspection RsUnsafeError
// Ensure Docker container and environment are cleaned up after the test process ends.
#[ctor::dtor]
fn teardown_clickhouse() {
    if let Some(m) = CONTAINER.get() {
        if let Ok(mut guard) = m.lock() {
            // Drop container to stop it
            let _ = guard.take();
        }
    }
    // Clean env vars to avoid polluting parent shells if tests run in-process
    // (usually not necessary, but harmless)
    for k in ["CLICKHOUSE_URL", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD"] {
        std::env::remove_var(k);
    }

    // Drop DuckDB connection
    if let Some(m) = DUCKDB.get() {
        if let Ok(mut guard) = m.lock() {
            let _ = guard.take();
        }
    }
}

/// Initialize a global in-memory DuckDB connection once and provide safe mutable access.
pub fn with_duckdb<F, T>(f: F) -> T
where
    F: FnOnce(&mut Connection) -> T,
{
    let m = DUCKDB.get_or_init(|| {
        // Use in-memory DuckDB; requires running cargo test --features test-duckdb
        let config = Config::default()
            .allow_unsigned_extensions()
            .expect("Failed to set DuckDB config");
        let conn = Connection::open_in_memory_with_flags(config).expect("Failed to open in-memory DuckDB");
        Mutex::new(Some(conn))
    });

    let mut guard = m.lock().unwrap_or_else(|poisoned| {
        // If the mutex is poisoned, we can still recover the data
        // This happens when a previous test panicked while holding the lock
        poisoned.into_inner()
    });
    let conn = guard.as_mut().expect("DuckDB connection missing");
    f(conn)
}

/// Create a ClickHouse client using the server connection info
pub async fn clickhouse_client() -> Result<ClientHandle, Box<dyn std::error::Error>> {
    let server = clickhouse();
    connect_client_to_server(server).await
}

/// Create a ClickHouse client from a specific server
async fn connect_client_to_server(server: &ClickHouseServer) -> Result<ClientHandle, Box<dyn std::error::Error>> {
    // Parse URL to get host and port
    let url_without_protocol = server.url.strip_prefix("tcp://").unwrap_or(&server.url);
    let url_without_auth = if let Some(at_pos) = url_without_protocol.find('@') {
        &url_without_protocol[at_pos + 1..]
    } else {
        url_without_protocol
    };
    
    let (host, port_str) = if let Some(colon_pos) = url_without_auth.rfind(':') {
        (&url_without_auth[..colon_pos], &url_without_auth[colon_pos + 1..])
    } else {
        (url_without_auth, "9000")
    };
    
    let port: u16 = port_str.parse().unwrap_or(9000);
    
    println!("Connecting to ClickHouse at {}:{} with user: {}", host, port, server.user);
    
    let pool = Pool::new(format!("tcp://{}:{}@{}:{}/default",
                                 server.user, server.password, host, port));
    
    // Add retry logic with timeout
    for attempt in 1..=5 {
        match tokio::time::timeout(
            tokio::time::Duration::from_secs(10), 
            pool.get_handle()
        ).await {
            Ok(Ok(handle)) => {
                println!("✓ Connected to ClickHouse on attempt {}", attempt);
                return Ok(handle);
            }
            Ok(Err(e)) => {
                println!("⚠ Connection attempt {} failed: {}", attempt, e);
                if attempt < 5 {
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                }
            }
            Err(_) => {
                println!("⚠ Connection attempt {} timed out", attempt);
                if attempt < 5 {
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                }
            }
        }
    }
    
    Err("Failed to connect to ClickHouse after 5 attempts".into())
}

/// Execute SQL file in ClickHouse
pub async fn execute_sql_file(file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let server = clickhouse();
    execute_sql_file_on_server(file_path, server).await
}

/// Execute SQL file with specific server
async fn execute_sql_file_on_server(file_path: &str, server: &ClickHouseServer) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_client_to_server(server).await?;
    let sql_content = fs::read_to_string(file_path)?;
    
    println!("Executing SQL file: {}", file_path);
    println!("SQL Content: {}", sql_content);
    
    // Parse and execute only the CREATE TABLE statement, ignoring comments
    let lines: Vec<&str> = sql_content.lines().collect();
    let mut in_create_statement = false;
    let mut create_statement = String::new();
    
    for line in lines {
        let trimmed_line = line.trim();
        // Skip comment lines
        if trimmed_line.starts_with("--") || trimmed_line.is_empty() {
            continue;
        }
        
        // Start collecting when we see CREATE TABLE
        if trimmed_line.starts_with("CREATE TABLE") {
            in_create_statement = true;
        }
        
        if in_create_statement {
            create_statement.push_str(line);
            create_statement.push('\n');
            
            // End collecting when we see the semicolon
            if trimmed_line.ends_with(";") {
                break;
            }
        }
    }
    
    if !create_statement.is_empty() {
        let final_statement = create_statement.trim();
        println!("Executing statement: {}", final_statement);
        match client.execute(final_statement).await {
            Ok(_) => println!("✓ Statement executed successfully"),
            Err(e) => {
                println!("⚠ Failed to execute statement: {}", e);
                return Err(e.into());
            }
        }
        
        // Verify table was created by checking if it exists
        match client.query("SHOW TABLES LIKE 'all_types_test'").fetch_all().await {
            Ok(rows) => {
                println!("Table verification result: {:?}", rows);
                if rows.is_empty() {
                    return Err("Table was not created successfully".into());
                }
            }
            Err(e) => println!("⚠ Could not verify table creation: {}", e)
        }
    }
    
    Ok(())
}

/// Internal function to setup test data during container initialization
async fn setup_clickhouse_test_data_internal(server: &ClickHouseServer) -> Result<(), Box<dyn std::error::Error>> {
    // First create the table
    execute_sql_file_on_server("tests/clickhouse_all_types.sql", server).await?;
    
    // Wait a moment for table creation to complete
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    // Then load some simple test data with direct INSERT statements
    let mut client = connect_client_to_server(server).await?;
    
    println!("Inserting test data into all_types_test table...");
    
    // Insert a few simple test rows
    let test_inserts = vec![
        "INSERT INTO all_types_test (int8_col, int16_col, int32_col, int64_col, uint8_col, uint16_col, uint32_col, uint64_col, float32_col, float64_col, bool_col, string_col, fixedstring_col, date_col, datetime_col, datetime64_col, uuid_col, ipv4_col, ipv6_col, enum8_col, enum16_col, decimal32_col, decimal64_col, decimal128_col, array_int_col, array_string_col, nullable_int_col, nullable_string_col, map_col, nested_array_col) VALUES (-128, 32767, 1000, 9223372036854775807, 255, 65535, 4000000000, 18446744073709551615, 3.14159, 3.141592653589793, 1, 'Hello World', 'test      ', '2023-12-01', '2023-12-01 12:30:45', '2023-12-01 12:30:45.123', '550e8400-e29b-41d4-a716-446655440000', '192.168.1.1', '2001:0db8:85a3::8a2e:370:7334', 'red', 'large', 123.4567, 12345.67890123, 1234567890.1234567890123456, [1,2,3,4,5], ['apple','banana','cherry'], 42, 'Sample Text', {'key1':10,'key2':20}, [[1,2],[3,4],[5,6]])",
        
        "INSERT INTO all_types_test (int8_col, int16_col, int32_col, int64_col, uint8_col, uint16_col, uint32_col, uint64_col, float32_col, float64_col, bool_col, string_col, fixedstring_col, date_col, datetime_col, datetime64_col, uuid_col, ipv4_col, ipv6_col, enum8_col, enum16_col, decimal32_col, decimal64_col, decimal128_col, array_int_col, array_string_col, nullable_int_col, nullable_string_col, map_col, nested_array_col) VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0, 'Empty String', 'empty     ', '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.000', '00000000-0000-0000-0000-000000000000', '0.0.0.0', '::1', 'green', 'medium', 0.0000, 0.00000000, 0.0000000000000000, [10,20,30], ['one','two','three'], NULL, NULL, {'empty':0}, [[10,20,30],[40,50]])",
        
        "INSERT INTO all_types_test (int8_col, int16_col, int32_col, int64_col, uint8_col, uint16_col, uint32_col, uint64_col, float32_col, float64_col, bool_col, string_col, fixedstring_col, date_col, datetime_col, datetime64_col, uuid_col, ipv4_col, ipv6_col, enum8_col, enum16_col, decimal32_col, decimal64_col, decimal128_col, array_int_col, array_string_col, nullable_int_col, nullable_string_col, map_col, nested_array_col) VALUES (127, -32768, -1000000, -9223372036854775808, 128, 32768, 2000000000, 9223372036854775808, 2.718, 2.718281828459045, 1, 'Unicode Test', 'unicode   ', '2025-06-15', '2025-06-15 18:45:30', '2025-06-15 18:45:30.999', '123e4567-e89b-12d3-a456-426614174000', '10.0.0.1', 'fe80::1', 'blue', 'small', 999.9999, 99999.99999999, 999999999.9999999999999999, [-1,-2,-3], ['test','example','sample'], 100, 'Test Value', {'alpha':1,'beta':2,'gamma':3}, [[100],[200,300],[400,500,600]])",
    ];
    
    for (i, insert_sql) in test_inserts.iter().enumerate() {
        println!("Executing INSERT statement {}: {}", i + 1, insert_sql);
        match client.execute(insert_sql).await {
            Ok(_) => println!("✓ Successfully inserted row {}", i + 1),
            Err(e) => println!("⚠ Failed to insert row {}: {}", i + 1, e),
        }
    }
    
    // Verify data was inserted
    let count_result = client.query("SELECT COUNT(*) FROM all_types_test").fetch_all().await?;
    println!("Total rows in all_types_test: {:?}", count_result);
    
    Ok(())
}

/// Setup comprehensive test data in ClickHouse (public function for manual use)
pub async fn setup_clickhouse_test_data() -> Result<(), Box<dyn std::error::Error>> {
    let server = clickhouse();
    
    // Wait for ClickHouse to be fully ready
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    
    println!("Setting up ClickHouse test data...");
    match setup_clickhouse_test_data_internal(server).await {
        Ok(_) => {
            println!("✓ ClickHouse test data setup complete");
            Ok(())
        },
        Err(e) => {
            println!("⚠ Failed to setup ClickHouse test data: {}", e);
            Err(e)
        }
    }
}