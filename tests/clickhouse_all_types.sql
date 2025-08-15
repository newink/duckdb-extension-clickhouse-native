-- ClickHouse table with comprehensive type coverage
CREATE TABLE all_types_test (
    -- Integer types (signed)
    int8_col Int8,
    int16_col Int16, 
    int32_col Int32,
    int64_col Int64,
    
    -- Integer types (unsigned)
    uint8_col UInt8,
    uint16_col UInt16,
    uint32_col UInt32, 
    uint64_col UInt64,
    
    -- Floating point types
    float32_col Float32,
    float64_col Float64,
    
    -- Boolean type
    bool_col Bool,
    
    -- String types
    string_col String,
    fixedstring_col FixedString(10),
    
    -- Date and time types
    date_col Date,
    datetime_col DateTime,
    datetime64_col DateTime64(3),
    
    -- UUID type
    uuid_col UUID,
    
    -- IPv4/IPv6 types
    ipv4_col IPv4,
    ipv6_col IPv6,
    
    -- Enum types
    enum8_col Enum8('red' = 1, 'green' = 2, 'blue' = 3),
    enum16_col Enum16('small' = 1, 'medium' = 2, 'large' = 3),
    
    -- Decimal types
    decimal32_col Decimal32(4),
    decimal64_col Decimal64(8),
    decimal128_col Decimal128(16),
    
    -- Array types
    array_int_col Array(Int32),
    array_string_col Array(String),
    
    -- Nullable types
    nullable_int_col Nullable(Int32),
    nullable_string_col Nullable(String),
    
    -- Map type
    map_col Map(String, Int32),
    
    -- Nested array of arrays
    nested_array_col Array(Array(Int32))
) ENGINE = MergeTree()
ORDER BY int32_col;