# Proposed Simplified Design for duckdb-zarr

## Executive Summary

After analyzing the zarrs_ffi documentation more thoroughly, we found that the original design overestimated what zarrs_ffi provides. This document proposes a significantly simplified architecture that:

1. **Uses zarrs_ffi for what it actually does well** (chunk retrieval, decompression, metadata access)
2. **Leverages Arrow C++** (already a DuckDB dependency) for efficient data transfer
3. **Minimizes custom code** to the essential DuckDB integration layer

---

## Revised Architecture

### Original (Overcomplicated)

```
┌─────────────────────────────────────────────────────────────────┐
│                    duckdb-zarr Extension                        │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Phase 1: Zarr Metadata Parser (~200 LOC)                 │  │
│  │  • Custom JSON parsing with yyjson                        │  │
│  │  • Zarr v2/v3 format detection                            │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Phase 2: Chunk Reader via zarrs_ffi (~150 LOC)          │  │
│  │  • Chunk retrieval and decompression                      │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Phase 3: Pivot Algorithm (~200 LOC)                      │  │
│  │  • Strided memory access for array→table transform       │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Phase 4: DuckDB Integration (~150 LOC)                   │  │
│  │  • Table function, vector conversion                      │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Revised (Simplified)

```
┌─────────────────────────────────────────────────────────────────┐
│                    duckdb-zarr Extension                        │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  DuckDB Integration Layer (~150 LOC)                     │  │
│  │  • Table function registration                           │  │
│  │  • Arrow→DuckDB vector conversion                         │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  zarrs_ffi (single header, ~50 LOC wrapper)              │  │
│  │  • Array opening, metadata access, chunk retrieval        │  │
│  │  • Built-in codec support (blosc2, zstd, gzip, lz4)      │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Arrow-Based Pivot (~100 LOC)                            │  │
│  │  • Use Arrow buffers directly                            │  │
│  │  • Dictionary encoding for coordinates (memory efficient) │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Phase-by-Phase Analysis

### Phase 1: Zarr Metadata Parser (~200 LOC) → SKIP/REDUCE

**Original Plan:** Parse .zarr JSON metadata files using yyjson

**Simplified Approach:** Use zarrs_ffi metadata APIs

```cpp
// Instead of parsing JSON ourselves, use zarrs_ffi:
ZarrsArray array;
zarrsOpenArrayRW(storage, path, &array);

// Get metadata directly via zarrs_ffi
size_t dimensionality;
zarrsArrayGetDimensionality(array, &dimensionality);

uint64_t* shape = new uint64_t[dimensionality];
zarrsArrayGetShape(array, &shape);

// Get dtype as string
char* dtype;
zarrsArrayGetDataType(array, &dtype);
```

**Result:** Reduce Phase 1 from ~200 LOC to ~50 LOC (just schema translation)

---

### Phase 2: Chunk Reader via zarrs_ffi (~150 LOC) → KEEP (with simplification)

**Original Plan:** Build wrapper around zarrs_ffi chunk retrieval

**Simplified Approach:** Direct integration with minimal wrapper

```cpp
// Simplified chunk retrieval
void RetrieveChunk(ZarrsArray array, const uint64_t* indices, 
                   size_t chunk_size, uint8_t* buffer) {
    zarrsArrayRetrieveChunk(array, dimensionality, indices, 
                           chunk_size, buffer);
}
```

**Result:** Keep ~100 LOC for chunk handling (reduced from 150)

---

### Phase 3: Pivot Algorithm (~200 LOC) → REDESIGN (use Arrow)

**Original Plan:** Custom strided memory access to convert to DuckDB vectors

**Simplified Approach:** Use Arrow buffers directly

```cpp
// Instead of custom pivot, use Arrow:
// 1. Create Arrow RecordBatch with coordinate + data columns
// 2. Use Arrow's DictionaryArray for coordinate columns (memory efficient)
// 3. Convert Arrow batch to DuckDB DataChunk

class ArrowPivot {
    // Create coordinate arrays with dictionary encoding
    arrow::DictionaryArray::Make(
        arrow::datum(coord_values),  // Unique coordinate values
        arrow::datum(coord_indices)   // Row indices into dictionary
    );
    
    // Create data array directly from chunk bytes
    arrow::MakeArrayOfFloat(chunk_data, shape);
    
    // Combine into RecordBatch
    arrow::RecordBatch::Make(schema, {coord_dict, data_array});
};
```

**Result:** ~100 LOC using Arrow's built-in functionality

---

### Phase 4: DuckDB Integration (~150 LOC) → EXPAND

**Original Plan:** Basic table function with vector conversion

**Simplified Approach:** Full integration with Arrow

```cpp
// DuckDB table function using Arrow
static void ZarrScanFunction(ClientContext &ctx, 
                              TableFunctionInput &data, 
                              DataChunk &output) {
    auto& bind_data = data.bind_data->Cast<ZarrScanBindData>();
    
    // Read chunk via zarrs_ffi
    auto chunk_data = bind_data.reader->ReadChunk(...);
    
    // Convert to Arrow RecordBatch (pivot)
    auto batch = bind_data.pivot->ToArrowBatch(chunk_data);
    
    // Convert Arrow → DuckDB vectors (using DuckDB's Arrow integration)
    ArrowToDuckDB(batch, output);
}
```

**Result:** ~150 LOC (main phase, cannot simplify further)

---

## Minimum Viable Implementation

Based on this analysis, here's the MINIMUM code we need:

### Total: ~300-400 LOC (vs original ~800 LOC estimate)

| Component | Original | Revised | Savings |
|-----------|----------|---------|---------|
| Metadata parsing | ~200 LOC | ~50 LOC | 75% |
| Chunk reader | ~150 LOC | ~100 LOC | 33% |
| Pivot algorithm | ~200 LOC | ~100 LOC | 50% |
| DuckDB integration | ~150 LOC | ~150 LOC | 0% |
| **Total** | **~700 LOC** | **~400 LOC** | **43%** |

---

## Key Simplifications

### 1. Use zarrs_ffi Metadata APIs Instead of JSON Parsing

**Before (custom JSON parsing):**
```cpp
// zarr_metadata.cpp - 200+ lines of yyjson parsing
auto* doc = yyjson_read(content.c_str(), content.size(), 0);
auto* shape_val = yyjson_obj_get(json, "shape");
// ... 200 more lines
```

**After (zarrs_ffi API):**
```cpp
// ~20 lines
zarrsArrayGetShape(array, shape.get());
zarrsArrayGetDataType(array, &dtype);
zarrsArrayGetChunkShape(array, chunks.get());
```

### 2. Use Arrow for Data Transfer

**Before (custom vector construction):**
```cpp
// Manual vector construction - 50+ lines
auto& vec = output.data[col_idx];
for (size_t i = 0; i < nrows; i++) {
    vec.SetValue(i, CalculateValue(...));
}
```

**After (Arrow → DuckDB):**
```cpp
// DuckDB has built-in Arrow support
// Just use: arrow::RecordBatch → DataChunk conversion
ArrowConverter::ToDuckDB(batch, output);  // ~5 lines
```

### 3. Simplified Pivot with Arrow DictionaryArray

**Before (custom strided access):**
```cpp
// Complex index calculation - ~100 lines
for (size_t row = 0; row < nrows; row++) {
    size_t idx = 0;
    size_t temp = row;
    for (int dim = n_dims - 1; dim >= 0; dim++) {
        idx += (temp % shape[dim]) * strides[dim];
        temp /= shape[dim];
    }
    output[row] = chunk_data[idx];
}
```

**After (Arrow buffers):**
```cpp
// Arrow handles this internally - ~20 lines
auto array = MakeArrayFromBuffer(chunk_data, dtype, shape);
auto dict = MakeDictionaryArray(coord_values, coord_indices);
return RecordBatch::Make({dict, array});
```

---

## Implementation Roadmap

### Phase 1: zarrs_ffi Integration (Priority: HIGH)
- [ ] Add zarrs_ffi header and static library
- [ ] Build basic array opening and metadata retrieval
- [ ] Test chunk retrieval with simple arrays

### Phase 2: Arrow Integration (Priority: HIGH)
- [ ] Use Arrow buffers for chunk data
- [ ] Implement dictionary encoding for coordinates
- [ ] Connect Arrow RecordBatch to DuckDB vectors

### Phase 3: Table Function (Priority: HIGH)
- [ ] Register `read_zarr` table function
- [ ] Implement predicate pushdown (which chunks to read)
- [ ] Handle parallel scanning

### Phase 4: Polish (Priority: MEDIUM)
- [ ] Error handling and edge cases
- [ ] Support for Zarr v2 and v3
- [ ] Cloud storage (S3/GCS) - leverage zarrs_ffi support

---

## Dependencies

| Dependency | Purpose | Status |
|------------|---------|--------|
| zarrs_ffi | Zarr reading (chunk retrieval, decompression) | To be added |
| Arrow C++ | Data transfer (already a DuckDB dependency) | Available |
| DuckDB | Query engine (extension target) | Core |

---

## Conclusion

The original design overestimated zarrs_ffi's capabilities and underestimated Arrow's utility. By:

1. **Using zarrs_ffi metadata APIs** instead of custom JSON parsing
2. **Leveraging Arrow** for efficient data transfer and dictionary encoding
3. **Focusing on DuckDB integration** as the core complexity

We can reduce the implementation from ~700 LOC to ~400 LOC while maintaining the same functionality.

The key insight is that **Arrow is the bridge**: zarrs_ffi gives us raw bytes, Arrow gives us typed arrays with dictionary encoding, and DuckDB consumes Arrow natively.
