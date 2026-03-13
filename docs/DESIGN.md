# XQL Extension Design Document

**Project:** xql - DuckDB Extension for n-Dimensional Array Data
**Status:** Design Draft
**Version:** 0.1.0

---

## 1. Problem Statement

### Why Query Arrays with SQL?

Scientific data is increasingly stored in cloud-native formats like Zarr, which organize data as n-dimensional arrays with chunked, compressed storage. This is particularly common in:

- **Weather and climate data**: NOAA, ECMWF, and climate reanalysis datasets
- **Satellite imagery**: Landsat, Sentinel, MODIS
- **Oceanography**: Ocean model outputs, sea surface temperature
- **Medical imaging**: CT, MRI scans stored as 3D arrays
- **Machine learning**: Training data stored as tensors

### The Challenge

Current workflows require:

1. Loading entire arrays into memory (impossible for large datasets)
2. Using Python/R libraries with specialized APIs
3. Pre-processing data before analysis
4. Writing custom code for each query pattern

### The Opportunity

By exposing Zarr arrays as SQL tables in DuckDB, users can:

- Query massive datasets directly from cloud storage
- Use familiar SQL syntax for array operations
- Leverage DuckDB's pushdown predicates and column pruning
- Combine array data with traditional relational data
- Benefit from DuckDB's excellent performance characteristics

---

## 2. Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         DuckDB Core                             │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                   Query Execution Engine                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              XQL Extension (this project)               │   │
│  │  ┌─────────────────────────────────────────────────────┐ │   │
│  │  │              TableProvider Implementation          │ │   │
│  │  │  • scan_plan() - Creates execution plan            │ │   │
│  │  │  • Scan() - Returns RecordBatch iterator           │ │   │
│  │  └─────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                   │
└──────────────────────────────┼───────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    zarr-datafusion Crate                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  ZarrScanBuilder → ZarrExecNode → RecordBatch           │   │
│  │  • Chunk selection based on query predicates            │   │
│  │  • Dimension filtering                                   │   │
│  │  • Type conversion                                       │   │
│  └─────────────────────────────────────────────────────────┘   │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Zarr Storage Layer                           │
│  • Remote: S3, GCS, Azure Blob                                  │
│  • Local: Filesystem                                           │
│  • Formats: Zarr v2/v3                                         │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Registration**: User registers a Zarr store with `CREATE TABLE ... USING xql`
2. **Planning**: DuckDB creates a scan plan via `TableProvider::scan_plan()`
3. **Execution**: XQL creates a DataFusion execution plan for Zarr
4. **Reading**: DataFusion reads chunks, applies filters, converts types
5. **Conversion**: RecordBatch is converted to DuckDB vectors

---

## 3. Key Interfaces

### DuckDB Extension Interface

The extension follows the standard DuckDB extension pattern:

```cpp
class XqlExtension : public Extension {
public:
    void Load(ExtensionLoader &db) override;
    std::string Name() override;
    std::string Version() const override;
};
```

This registers the extension and its functions with DuckDB at load time.

### TableProvider Implementation

The core interface is DataFusion's `TableProvider`, which exposes external data as a SQL table:

```cpp
#include <duckdb/function/table_function.hpp>

class ZarrTableProvider : public TableFunction {
public:
    // TableProvider interface
    virtual std::string GetName() override;
    virtual TableFunctionType GetTableFunctionType() override;
    virtual std::unique_ptr<FunctionData> Bind(
        ClientContext &context,
        TableFunctionBindInput &input,
        vector<LogicalType> &return_types,
        vector<string> &names
    ) override;
    virtual std::unique_ptr<PhysicalOperator> Scan(
        ClientContext &context,
        const FunctionData *bind_data,
        const vector<column_t> &column_ids,
        const vector<idx_t> &max_throughput_columns,
        const vector<string> &names,
        typename idx_t max_output_rows,
        parallel_state_t *parallel_state
    ) override;
};
```

### Zarr to Table Mapping

A Zarr array maps to a SQL table as follows:

| Zarr Concept | SQL Concept |
|---------------|--------------|
| Array | Table |
| Dimensions (axes) | Columns (coordinate dimensions) |
| Attributes | Table metadata / comments |
| Chunks | Internal storage (not directly visible) |
| Data variables | Non-coordinate columns |

#### Example: Temperature Array

```
Zarr Array (temperature):
  Shape: (time=1000, lat=180, lon=360)
  Chunks: (100, 30, 60)
  dtype: float32

SQL Table (temperature):
  time    | lat    | lon    | temp
  --------|--------|--------|-------
  0       | -90.0  | 0.0    | 273.15
  0       | -89.0  | 0.0    | 272.90
  ...     | ...    | ...    | ...
```

### Scan Execution Flow

```cpp
// In ZarrTableProvider implementation
std::unique_ptr<PhysicalOperator> ZarrTableProvider::Scan(
    ClientContext &context,
    const FunctionData *bind_data,
    const vector<column_id> &column_ids,
    ...
) {
    // 1. Extract bind parameters (path, array name, filters)
    auto &config = bind_data->config;

    // 2. Create DataFusion execution plan
    auto df_ctx = ExecutionContext();
    auto scan = ZarrScanBuilder(df_ctx)
        .with_store(config.zarr_path)
        .with_array(config.array_name)
        .with_predicates(convert_duckdb_filters(column_ids, column_ids))
        .build();

    // 3. Return DuckDB physical operator that wraps DataFusion
    return std::make_unique<ZarrScanOperator>(std::move(scan));
}
```

### Predicate Pushdown

The extension must translate DuckDB's filter expressions to Zarr chunk selection:

```cpp
// Example: WHERE time BETWEEN 100 AND 200 AND temp > 300
// Becomes: Read only chunks where time index ∈ [100, 200]

struct ZarrPredicate {
    // Convert DuckDB expression to Zarr chunk selection
    std::optional<ChunkSelection> Convert(
        const Expression &duckdb_expr,
        const ArraySchema &schema
    );
};
```

---

## 4. User-Facing API

### Registering a Zarr Array

```sql
-- Basic registration (infers schema from Zarr metadata)
CREATE TABLE temperature
USING xql
OPTIONS (
    path 's3://noaa-gfs-pds/gfs.20210101/00/temp',
    array 'temperature'
);

-- With explicit column mapping
CREATE TABLE weather_data
USING xql
OPTIONS (
    path 's3://my-bucket/weather.zarr',
    array 'surface',
    coord_columns 'time,lat,lon',
    data_columns 'temperature,pressure,humidity'
);
```

### Querying Array Data

```sql
-- Basic query
SELECT * FROM temperature WHERE time = 0 LIMIT 100;

-- Aggregate over dimensions
SELECT AVG(temperature) FROM temperature
WHERE time BETWEEN 0 AND 100
GROUP BY lat, lon;

-- Spatial query (DuckDB's GIS functions work!)
SELECT ST_Point(lon, lat) as geom, temperature
FROM temperature
WHERE time = 0 AND lat BETWEEN 40 AND 50 AND lon BETWEEN -120 AND -100;

-- Time series extraction
SELECT time, temperature FROM temperature
WHERE lat = 45.0 AND lon = -122.0
ORDER BY time;
```

### Extension Management

```sql
-- Load extension
LOAD xql;

-- Show extension version
SELECT xql_version();

-- List registered arrays
SELECT * FROM xql_arrays();
```

---

## 5. Dependencies

### Core Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| DuckDB | (submodule) | Core database, extension framework |
| zarr | 0.5+ | Zarr array format (Rust crate) |
| datafusion | 40+ | Query execution engine |
| arrow | 50+ | Columnar data format |
| async-trait | 0.1 | Async interface support |
| object_store | 0.10+ | Cloud storage abstraction |

### Dependency Graph

```
duckdb
  │
  └── xql_extension
        │
        ├── zarr-datafusion (Rust crate, to be integrated)
        │     │
        │     ├── datafusion (physical planning, execution)
        │     │     │
        │     │     ├── arrow
        │     │     └── ...
        │     │
        │     ├── zarr (array format)
        │     │     │
        │     │     └── bytes
        │     │
        │     └── object_store (S3, GCS, Azure)
        │           │
        │           └── async-trait
        │
        └── openssl (already in vcpkg.json)
```

### Integration with zarr-datafusion

The `zarr-datafusion` crate provides the DataFusion integration:

```rust
// From zarr-datafusion crate
pub struct ZarrScanBuilder {
    store: Store,
    array_name: String,
    predicate: Option<Expr>,
    projection: Option<Vec<String>>,
}

impl ZarrScanBuilder {
    pub fn new(store: Store, array_name: String) -> Self;
    pub fn with_predicate(mut self, predicate: Expr) -> Self;
    pub fn with_projection(mut self, columns: Vec<String>) -> Self;
    pub fn build(self) -> Result<Arc<dyn ExecutionPlan>>;
}
```

This crate handles:
- Reading Zarr metadata (`.zarray`, `.zattrs`)
- Chunk selection based on predicates
- Async I/O for remote stores
- Type conversion (Zarr dtype → Arrow dtype)

---

## 6. Roadmap / Milestones

### Milestone 1: Extension Foundation (Week 1-2)

**Goal**: Basic extension structure with Zarr discovery

- [ ] Set up zarr-datafusion as a vcpkg/Cargo dependency
- [ ] Implement basic table function registration
- [ ] Implement `ZARR_LIST` function to discover arrays in a store
- [ ] Read Zarr metadata and expose as DuckDB result
- [ ] Basic test with local filesystem Zarr

**Deliverable**: `SELECT * FROM xql_list_arrays('/path/to/zarr.zarr')`

### Milestone 2: TableProvider Implementation (Week 3-4)

**Goal**: Basic table scans work

- [ ] Implement `TableProvider` interface
- [ ] Integrate zarr-datafusion `ZarrScanBuilder`
- [ ] Convert DataFusion `RecordBatch` to DuckDB vectors
- [ ] Support simple queries (SELECT without filters)
- [ ] Test with small Zarr arrays

**Deliverable**: `SELECT * FROM zarr_table` returns data

### Milestone 3: Predicate Pushdown (Week 5-6)

**Goal**: Efficient queries with filters

- [ ] Translate DuckDB expressions to Zarr chunk selection
- [ ] Implement time/dimension range filtering
- [ ] Column pruning (only read requested columns)
- [ ] Chunk cache for frequently accessed data
- [ ] Benchmark with large datasets

**Deliverable**: `SELECT * FROM t WHERE time BETWEEN x AND y` only reads relevant chunks

### Milestone 4: Cloud Storage Support (Week 7-8)

**Goal**: Works with S3, GCS, Azure

- [ ] Configure object_store for cloud providers
- [ ] Handle authentication (IAM, credentials)
- [ ] Async I/O for remote reads
- [ ] Connection pooling
- [ ] Test with public cloud datasets

**Deliverable**: Query data directly from `s3://bucket/data.zarr`

### Milestone 5: Advanced Features (Week 9-10)

**Goal**: Production-ready features

- [ ] Support for Zarr v3 format
- [ ] Multi-variable arrays
- [ ] Coordinate variable handling
- [ ] Null/missing value handling
- [ ] Error handling and diagnostics

**Deliverable**: Full-featured production extension

### Milestone 6: Integration & Distribution (Week 11-12)

**Goal**: Release-ready

- [ ] CI/CD pipeline for multi-platform builds
- [ ] Submit to DuckDB community extensions
- [ ] Documentation and examples
- [ ] Performance benchmarking
- [ ] Compatibility testing with DuckDB versions

**Deliverable**: `INSTALL xql FROM community; LOAD xql;`

---

## 7. Technical Considerations

### Chunking Strategy

Zarr arrays use chunked storage. The extension must:
1. **Read minimal chunks**: Only fetch chunks that contain data matching query predicates
2. **Cache chunks**: Keep frequently accessed chunks in memory
3. **Parallel reads**: Read multiple chunks concurrently from remote stores

### Type Mapping

| Zarr dtype | DuckDB Type |
|------------|-------------|
| int8 | TINYINT |
| int16 | SMALLINT |
| int32 | INTEGER |
| int64 | BIGINT |
| uint8 | UTINYINT |
| float32 | FLOAT |
| float64 | DOUBLE |
| string | VARCHAR |
| bytes | BLOB |

### Memory Management

- **Vector-based**: Convert Arrow batches to DuckDB vectors in batches
- **Streaming**: Process large arrays in chunks to avoid memory pressure
- **Parallel execution**: Use DuckDB's parallel scan infrastructure

### Extension Loading

The extension uses DuckDB's standard extension mechanism:

```cpp
extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(xql, loader) {
    duckdb::LoadInternal(loader);
}
}
```

This allows:
- Static linking into DuckDB binary
- Dynamic loading as `.duckdb_extension` file
- Installation via `INSTALL xql FROM community`

---

## 8. Related Projects

- **xarray-sql** (https://github.com/alxmrs/xarray-sql): Parent project, Python library
- **zarr-datafusion** (https://github.com/alxmrs/zarr-datafusion): Rust crate for Zarr + DataFusion
- **DuckDB** (https://github.com/duckdb/duckdb): Analytical database
- **DuckDB Extension Template** (https://github.com/duckdb/extension-template): This project's foundation
- **Zarr** (https://zarr.dev): N-dimensional array storage format
- **Apache Arrow** (https://arrow.apache.org): Columnar data format
- **DataFusion** (https://datafusion.apache.org): Query engine

---

## 9. Open Questions

1. **Multidimensional primary keys**: How to handle tables where the "primary key" is a tuple of dimension values?

2. **Schema evolution**: How to handle Zarr arrays that change over time (new variables, changed chunks)?

3. **Nested arrays**: Zarr supports arrays of arrays. Should these be exposed as JSON or nested DuckDB types?

4. **Write support**: Read-only initially, but should we plan for write support?

5. **Aggregation pushdown**: Should aggregations (SUM, AVG) be pushed to the Zarr layer where possible?

6. **Time handling**: Zarr doesn't have a native datetime type. How to handle time coordinates?

---

*Document Version: 0.1.0*
*Last Updated: 2026-03-13*
