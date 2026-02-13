# S2Index Implementation Documentation

This document describes the implementation of the S2Index type in StarRocks, which utilizes Google's S2 geometry library for efficient spatial indexing in geo-related functions.

## Overview

S2Index is a new index type added to StarRocks that enables efficient spatial queries on geographic data. The index uses Google's S2 library to convert geographic shapes (points, lines, polygons) into cell coverings at configurable levels.

## Changes Made

### 1. Frontend (FE) Changes

#### 1.1 IndexDef.java
**File**: `fe/fe-parser/src/main/java/com/starrocks/sql/ast/IndexDef.java`

- Added `S2("S2")` to the `IndexType` enum
- Updated `isCompatibleIndex()` method to include S2 index type as a compatible index

```java
public enum IndexType {
    BITMAP,
    GIN("GIN"),
    NGRAMBF("NGRAMBF"),
    VECTOR("VECTOR"),
    S2("S2");  // NEW

    // Whether the index type is compatible with the new metadata
    public static boolean isCompatibleIndex(IndexType indexType) {
        return indexType == GIN || indexType == VECTOR || indexType == S2;  // Updated
    }
}
```

#### 1.2 IndexAnalyzer.java
**File**: `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/IndexAnalyzer.java`

- Added import for `S2IndexParams`
- Added S2 index type validation in `checkColumn()` method
- Implemented `checkS2IndexValid()` method for S2 index validation:
  - Checks if experimental feature is enabled via `enable_experimental_s2_index` config
  - Validates table type (DUPLICATE or PRIMARY only)
  - Validates column type (VARCHAR or VARBINARY only, using explicit `PrimitiveType` checks)
  - Rejects unknown/unsupported property keys (following the same pattern as GIN index validation)
  - Validates S2-specific properties (e.g., `s2_level`)
  - Adds default properties
- Implemented `addDefaultS2Properties()` helper method

#### 1.3 IndexParams.java
**File**: `fe/fe-core/src/main/java/com/starrocks/catalog/IndexParams.java`

- Added import for `S2IndexParams`
- Registered S2 index parameters in the constructor:
  - `S2_LEVEL` parameter with default value of 13 and valid range 0-30

```java
/* S2 Index */
// common
register(builder, IndexType.S2, IndexParamType.COMMON, S2IndexParams.CommonIndexParamKey.S2_LEVEL, false, true,
        "13", null);
```

#### 1.4 Config.java
**File**: `fe/fe-core/src/main/java/com/starrocks/common/Config.java`

- Added experimental feature flag with description:

```java
@ConfField(mutable = true, comment = "Enable experimental S2 spatial index for geographic data")
public static boolean enable_experimental_s2_index = false;
```

#### 1.5 S2IndexParams.java (New File)
**File**: `fe/fe-core/src/main/java/com/starrocks/common/S2IndexParams.java`

Created new parameter definition file for S2 index with:
- `SUPPORTED_PARAM_KEYS` - Set of all valid S2 index parameter keys (used for unknown property rejection)
- `CommonIndexParamKey.S2_LEVEL` - S2 cell level for indexing (0-30, default 13)
- `IndexParamsKey` - Placeholder for future index build parameters
- `SearchParamsKey` - Placeholder for future search parameters
- `validateInteger()` helper method for parameter validation

#### 1.6 StarRocksLex.g4
**File**: `fe/fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocksLex.g4`

- Added `S2` lexer token so the SQL parser recognizes the keyword:

```antlr
S2: 'S2';
```

#### 1.7 StarRocks.g4
**File**: `fe/fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocks.g4`

- Added `S2` as a valid alternative in the `indexType` grammar rule:

```antlr
indexType
    : USING (BITMAP | GIN | NGRAMBF | VECTOR | S2)
    ;
```

- Added `S2` to the `nonReserved` rule so it can still be used as an identifier

#### 1.8 AstBuilder.java
**File**: `fe/fe-core/src/main/java/com/starrocks/sql/parser/AstBuilder.java`

- Added S2 branch to `getIndexType()` method to map the parsed `S2` token to `IndexDef.IndexType.S2`:

```java
} else if (indexTypeContext.S2() != null) {
    index = IndexDef.IndexType.S2;
}
```

### 2. Thrift Definition

#### 2.1 Descriptors.thrift
**File**: `gensrc/thrift/Descriptors.thrift`

- Added `S2` to `TIndexType` enum:

```thrift
enum TIndexType {
  BITMAP,
  GIN,
  NGRAMBF,
  VECTOR,
  S2,  // NEW
}
```

### 3. Protocol Buffer Definition

#### 3.1 types.proto
**File**: `gensrc/proto/types.proto`

- Added `S2 = 5` to `IndexType` enum:

```protobuf
enum IndexType {
    BITMAP = 0;
    GIN = 1;
    INDEX_UNKNOWN = 2;
    NGRAMBF = 3;
    VECTOR = 4;
    S2 = 5;  // NEW
}
```

### 4. Backend (BE) Changes

#### 4.1 tablet_index.cpp
**File**: `be/src/storage/tablet_index.cpp`

- Added S2 index type conversion in `_convert_index_type_from_thrift()`:

```cpp
StatusOr<IndexType> TabletIndex::_convert_index_type_from_thrift(TIndexType::type index_type) {
    switch (index_type) {
    case TIndexType::BITMAP:
        return IndexType::BITMAP;
    case TIndexType::GIN:
        return IndexType::GIN;
    case TIndexType::VECTOR:
        return IndexType::VECTOR;
    case TIndexType::S2:      // NEW
        return IndexType::S2; // NEW
    default:
        // ... error handling
    }
}
```

#### 4.2 metadata_util.cpp
**File**: `be/src/storage/metadata_util.cpp`

- Added S2 index handling in `convert_t_schema_to_pb_schema()` within the index type `if/else if` chain
- Validates that S2 index is built on a single column
- Sets `IndexType::S2` on the protobuf index
- Resolves `col_unique_id` from the column map
- Serializes all property maps (common, index, search, extra) to JSON

```cpp
} else if (index.index_type == TIndexType::type::S2) {
    RETURN_IF(index.columns.size() != 1,
              Status::Cancelled("S2 index " + index.index_name +
                                " do not support to build with more than one column"));
    index_pb->set_index_type(IndexType::S2);

    const auto& index_col_name = index.columns[0];
    const auto& mit = column_map.find(boost::to_lower_copy(index_col_name));

    if (mit != column_map.end()) {
        index_pb->add_col_unique_id(mit->second->unique_id());
    } else {
        return Status::Cancelled(
                strings::Substitute("index column $0 can not be found in table columns", index_col_name));
    }

    std::map<std::string, std::map<std::string, std::string>> properties_map = {
            {COMMON_PROPERTIES, index.common_properties},
            {INDEX_PROPERTIES, index.index_properties},
            {SEARCH_PROPERTIES, index.search_properties},
            {EXTRA_PROPERTIES, index.extra_properties}};
    index_pb->set_index_properties(to_json(properties_map));
}
```

#### 4.3 schema_change_utils.cpp
**File**: `be/src/storage/schema_change_utils.cpp`

- Added S2 index change detection in `parse_request_normal()` so that adding or removing an S2 index triggers a direct schema change:

```cpp
} else if (new_schema->has_index(new_column.unique_id(), S2) !=
           base_schema->has_index(ref_column.unique_id(), S2)) {
    *sc_directly = true;
    return Status::OK();
}
```

#### 4.4 index_descriptor.h
**File**: `be/src/storage/index/index_descriptor.h`

- Added `S2` case to `get_index_file_path()` switch statement
- Added `s2_index_file_path()` static method that generates S2 index file paths with `.s2i` extension

```cpp
case S2:
    return s2_index_file_path(rowset_dir, rowset_id, segment_id, index_id);

// ...

static std::string s2_index_file_path(const std::string& rowset_dir, const std::string& rowset_id,
                                      int segment_id, int64_t index_id) {
    // {rowset_dir}/{schema_hash}/{rowset_id}_{seg_num}_{index_id}.s2i
    return fmt::format("{}/{}_{}_{}.{}", rowset_dir, rowset_id, segment_id, index_id, "s2i");
}
```

#### 4.5 rowset.cpp
**File**: `be/src/storage/rowset/rowset.cpp`

- Added S2 index file handling in three operations:
  - **`remove()`**: Deletes `.s2i` files when a rowset is removed (single file delete, like VECTOR)
  - **`link_files_to()`**: Hard-links S2 index files when a rowset is cloned/linked (single file link)
  - **`copy_files_to()`**: Copies S2 index files during snapshot operations (single file copy)

#### 4.6 snapshot_manager.cpp
**File**: `be/src/storage/snapshot_manager.cpp`

- Added S2 index handling in `_rename_rowset_id()`: re-links S2 index files from old rowset ID to new rowset ID during snapshot restore

#### 4.7 segment_replicate_executor.cpp
**File**: `be/src/storage/segment_replicate_executor.cpp`

- Extended `_send_segments()` to handle S2 index files during segment replication (reads `.s2i` file and sends to replicas, same as VECTOR)

#### 4.8 rowset_writer.cpp
**File**: `be/src/storage/rowset/rowset_writer.cpp`

- Added S2 index cleanup in `HorizontalRowsetWriter` destructor: deletes orphaned `.s2i` files on abort
- Added S2 index cleanup in `VerticalRowsetWriter` destructor: same cleanup for vertical writer
- Added S2 index to `SegmentIndexPB` population in `_flush_segment()`: registers S2 index files in segment info so replication channels know about them

#### 4.9 segment_writer.cpp
**File**: `be/src/storage/rowset/segment_writer.cpp`

- Added `need_s2_index` check in `init()`: detects if a column has an S2 index and sets up the standalone index file path using `IndexDescriptor::s2_index_file_path()`

#### 4.10 column_writer.h
**File**: `be/src/storage/rowset/column_writer.h`

- Added `bool need_s2_index = false` to `ColumnWriterOptions` struct
- Updated `to_string()` to include the new field
- Added `#include "storage/index/s2/s2_index_writer.h"`
- Added `virtual Status write_s2_index(uint64_t* index_size)` to `ColumnWriter` base class (default returns OK)
- Added `Status write_s2_index(uint64_t* index_size) override` to `ScalarColumnWriter`
- Added `std::unique_ptr<S2IndexWriter> _s2_index_writer` member to `ScalarColumnWriter`

#### 4.11 column_writer.cpp
**File**: `be/src/storage/rowset/column_writer.cpp`

- **`ScalarColumnWriter::init()`**: Creates `S2IndexWriter` when `_opts.need_s2_index` is true.
  Extracts the `TabletIndex` from `_opts.tablet_index[S2]` and the file path from
  `_opts.standalone_index_file_paths[S2]`. Calls `_s2_index_writer->init()`.
- **`ScalarColumnWriter::append()`**: Feeds data to S2 writer via `INDEX_ADD_VALUES(_s2_index_writer, ...)`
  and `INDEX_ADD_NULLS(_s2_index_writer, ...)` macros in both the null-aware and non-null branches.
- **`ScalarColumnWriter::estimate_buffer_size()`**: Includes `_s2_index_writer->size()` in the total.
- **`ScalarColumnWriter::write_s2_index()`**: New method that calls `_s2_index_writer->finish(index_size)`.

#### 4.12 segment_writer.cpp (finalize)
**File**: `be/src/storage/rowset/segment_writer.cpp`

- Added `write_s2_index(&standalone_index_size)` call in `finalize_columns()`, after
  `write_vector_index()`. Both standalone index sizes are accumulated into the same
  `standalone_index_size` variable.

### 5. S2 Index Engine (New Files)

#### 5.1 S2IndexWriter
**Files**: `be/src/storage/index/s2/s2_index_writer.h`, `be/src/storage/index/s2/s2_index_writer.cpp`

The S2IndexWriter builds a spatial index for a VARCHAR/VARBINARY column containing WKT geometry strings.

**Interface** (follows InvertedWriter pattern for ScalarColumnWriter integration):
- `S2IndexWriter(shared_ptr<TabletIndex>, string file_path)` — Constructor
- `Status init()` — Reads `s2_level` from tablet index properties (default 13, range 0-30)
- `void add_values(const void* values, size_t count)` — Processes a batch of Slice values
- `void add_nulls(uint32_t count)` — Advances row counter for null rows
- `Status finish(uint64_t* index_size)` — Writes the `.s2i` file and reports size
- `uint64_t size() const` — Returns approximate memory usage

**Processing pipeline** (in `add_values()`):
1. Casts input to `Slice*` (each Slice contains a WKT string)
2. Parses each WKT via `GeoShape::from_wkt()` from `geo/geo_types.h`
3. Based on geometry type:
   - **Point**: Computes single cell at `s2_level` via `S2CellId(point).parent(s2_level)`
   - **LineString**: Uses `S2RegionCoverer::GetCovering()` with `set_fixed_level(s2_level)`
   - **Polygon**: Uses `S2RegionCoverer::GetCovering()` with `set_fixed_level(s2_level)`
4. Stores `cell_id -> [row_ids]` mapping in `std::map<uint64_t, vector<uint32_t>>`
5. Invalid/unparseable WKT strings are silently skipped

**File format** (`.s2i`):
```
[Header - 20 bytes]
  magic:        uint32_t = 0x53324958 ("S2IX")
  version:      uint32_t = 1
  s2_level:     uint32_t
  num_rows:     uint32_t (total rows in segment)
  num_entries:  uint32_t (distinct cell_id count)

[Cell Entries - variable, sorted by cell_id]
  cell_id:      uint64_t
  num_row_ids:  uint32_t
  row_ids:      uint32_t[] (sorted, deduplicated)

[Footer - 4 bytes]
  checksum:     uint32_t (CRC32C of all preceding bytes)
```

#### 5.2 S2IndexReader
**Files**: `be/src/storage/index/s2/s2_index_reader.h`, `be/src/storage/index/s2/s2_index_reader.cpp`

The S2IndexReader loads a `.s2i` file and provides spatial lookup.

**Interface**:
- `Status open(const string& index_file_path)` — Reads and validates the entire file (magic, version, checksum)
- `Status query_cells(const vector<uint64_t>& cell_ids, vector<uint32_t>* row_ids) const` — Returns sorted, deduplicated row IDs for matching cells
- `int s2_level() const`, `uint32_t num_rows() const`, `uint32_t num_entries() const` — Metadata accessors
- `bool is_open() const` — Whether the reader has been successfully opened

**Query flow**: Looks up each input cell_id in the in-memory `std::map`, collects all matching row IDs, sorts and deduplicates the result.

**Thread safety**: After `open()`, `query_cells()` is safe for concurrent read-only access.

#### 5.3 CMakeLists.txt
**File**: `be/src/storage/CMakeLists.txt`

- Added `index/s2/s2_index_writer.cpp` and `index/s2/s2_index_reader.cpp` to `STORAGE_FILES`

### 6. SegmentIterator Integration (Query-Time Filtering)

#### 6.1 PlanNodes.thrift
**File**: `gensrc/thrift/PlanNodes.thrift`

- Added `TSpatialSearchOptions` struct for passing spatial query parameters from FE to BE:

```thrift
struct TSpatialSearchOptions {
    1: optional bool enable_use_s2_index
    2: optional string query_shape_wkt
    3: optional string spatial_predicate
    4: optional double distance_threshold
    5: optional i32 spatial_column_id
    6: optional i32 s2_level_override
}
```

- Added field 42 on `TOlapScanNode`: `optional TSpatialSearchOptions spatial_search_options`
- Added field 47 on `TLakeScanNode`: `optional TSpatialSearchOptions spatial_search_options` (forward compatibility; lake data source does not use S2 at this time)

#### 6.2 spatial_search_option.h (New File)
**File**: `be/src/storage/index/s2/spatial_search_option.h`

- C++ struct `SpatialSearchOption` mirroring `TSpatialSearchOptions`:
  - `enable_use_s2_index`, `query_shape_wkt`, `spatial_predicate`, `distance_threshold`, `spatial_column_id`, `s2_level_override`
- Type alias `SpatialSearchOptionPtr = std::shared_ptr<SpatialSearchOption>`

#### 6.3 olap_common.h
**File**: `be/src/storage/olap_common.h`

- Added two statistics fields to `OlapReaderStatistics` (after vector index stats):
  - `int64_t rows_s2_index_filtered = 0` — rows eliminated by S2 index
  - `int64_t get_row_ranges_by_s2_index_timer = 0` — time spent in S2 index filtering (ns)

#### 6.4 tablet_reader_params.h
**File**: `be/src/storage/tablet_reader_params.h`

- Added forward declaration for `SpatialSearchOption`
- Added `bool use_s2_index = false` and `SpatialSearchOptionPtr spatial_search_option` fields (after vector search fields)

#### 6.5 segment_options.h
**File**: `be/src/storage/rowset/segment_options.h`

- Added forward declaration for `SpatialSearchOption`
- Added `bool use_s2_index = false` and `SpatialSearchOptionPtr spatial_search_option` fields (after vector search fields)

#### 6.6 olap_chunk_source.h / olap_chunk_source.cpp
**Files**: `be/src/exec/pipeline/scan/olap_chunk_source.h`, `be/src/exec/pipeline/scan/olap_chunk_source.cpp`

- Added `#include "storage/index/s2/spatial_search_option.h"`
- Added `_use_s2_index` bool member
- Added `_s2_index_filtered_counter` and `_s2_index_filter_timer` profile counters
- **`prepare()`**: Reads `TSpatialSearchOptions` from `TOlapScanNode` thrift node, creates `SpatialSearchOption` and sets `_use_s2_index`
- **`_init_reader_params()`**: Copies `_use_s2_index` and spatial search option into `_params`
- **`_init_counter()`**: Initializes S2 profile counters (`S2IndexFilterRows`, `S2IndexFilterTime`)
- **`_update_counter()`**: Updates profile counters from `OlapReaderStatistics`

#### 6.7 tablet_reader.cpp
**File**: `be/src/storage/tablet_reader.cpp`

- Copies `use_s2_index` and `spatial_search_option` from `TabletReaderParams` to `SegmentReadOptions` at both param-copying locations (~lines 291-292 and ~367-368)

#### 6.8 segment_iterator.cpp
**File**: `be/src/storage/rowset/segment_iterator.cpp`

- Added includes: `s2_index_reader.h`, `spatial_search_option.h`, geo headers, S2 geometry headers
- Added `S2IndexContext` struct:

```cpp
struct S2IndexContext {
    bool use_s2_index = false;
    std::shared_ptr<S2IndexReader> s2_index_reader;
};
```

- Added `_s2_index_ctx` member to SegmentIterator
- **`_init()`**: Calls `_init_s2_reader()` and `_get_row_ranges_by_s2_index()` in the filter pipeline, placed after inverted_index/del_vec and before vector_index
- **`_init_s2_reader()`**: Finds S2 index in segment schema, constructs `.s2i` file path, opens the S2IndexReader, populates `_s2_index_ctx`
- **`_get_row_ranges_by_s2_index()`**: 
  1. Parses `query_shape_wkt` via `GeoShape::from_wkt()`
  2. Computes S2 cell covering at the index's S2 level (points → single cell, lines/polygons → `S2RegionCoverer` with `set_fixed_level()`)
  3. Calls `query_cells()` on the reader
  4. Builds a `SparseRange` from matching row IDs
  5. Intersects with `_scan_range` to produce the filtered range
  6. Records statistics (`rows_s2_index_filtered`, timer)

**Design note**: S2 index is a **candidate filter** (pre-filter). S2 cell coverings are an approximation — the expression-level predicate (e.g., `st_contains`, `st_intersects`) still evaluates on surviving rows for exact results. Same conceptual pattern as bloom filters.

### 7. FE Optimizer Integration

#### 7.1 SpatialSearchOptions.java (New File)
**File**: `fe/fe-core/src/main/java/com/starrocks/common/SpatialSearchOptions.java`

Java POJO for spatial search parameters, following the `VectorSearchOptions` pattern:
- Fields: `enableUseS2Index`, `queryShapeWkt`, `spatialPredicate`, `distanceThreshold`, `spatialColumnId`, `s2LevelOverride`
- `toThrift()` method converts to `TSpatialSearchOptions` for serialization
- `getExplainString()` method for EXPLAIN output

#### 7.2 LogicalOlapScanOperator.java
**File**: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/logical/LogicalOlapScanOperator.java`

- Added `SpatialSearchOptions spatialSearchOptions` field
- Added getter `getSpatialSearchOptions()` and setter `setSpatialSearchOptions()`
- Updated builder `withOperator()` to copy `spatialSearchOptions`

#### 7.3 PhysicalOlapScanOperator.java
**File**: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/physical/PhysicalOlapScanOperator.java`

- Added `SpatialSearchOptions spatialSearchOptions` field
- Copies from `LogicalOlapScanOperator` in the constructor
- Added getter and setter
- Updated builder `withOperator()` to copy

#### 7.4 OlapScanNode.java
**File**: `fe/fe-core/src/main/java/com/starrocks/planner/OlapScanNode.java`

- Added `SpatialSearchOptions spatialSearchOptions` field with setter
- Added EXPLAIN output in `getNodeExplainString()` (after vector search section)
- Added thrift serialization in `toThrift()`: calls `spatialSearchOptions.toThrift()` and sets field 42 on `TOlapScanNode`

#### 7.5 PlanFragmentBuilder.java
**File**: `fe/fe-core/src/main/java/com/starrocks/sql/plan/PlanFragmentBuilder.java`

- Added `scanNode.setSpatialSearchOptions(node.getSpatialSearchOptions())` in `visitPhysicalOlapScan()` to propagate spatial search options from the physical plan node to the scan node

#### 7.6 AddDecodeNodeForDictStringRule.java
**File**: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/tree/AddDecodeNodeForDictStringRule.java`

- Added `newOlapScan.setSpatialSearchOptions(scanOperator.getSpatialSearchOptions())` after the `PhysicalOlapScanOperator` construction to preserve spatial options through the dict decode optimization

#### 7.7 RewriteToSpatialPlanRule.java (New File)
**File**: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/RewriteToSpatialPlanRule.java`

Optimizer rule that detects spatial predicates and enables S2 index usage:

- **Pattern**: Matches `OlapScan` nodes (predicates already pushed down to scan)
- **Detection**: Looks for `st_contains(column, shape)` function calls in scan predicates. Handles both direct column references and column references wrapped in geo constructor functions (e.g., `ST_GeomFromText(column)`)
- **Index matching**: Verifies that the column has an S2 index on the table
- **Column ref unwrapping**: The `extractColumnRef()` method handles direct `ColumnRefOperator` and unwraps geo constructor wrappers like `ST_GeomFromText(column)` using the `GEO_CONSTRUCTOR_FUNCTIONS` set (`st_geometryfromtext`, `st_geomfromtext`, `st_polyfromtext`, `st_polygonfromtext`, `st_linefromtext`, `st_linestringfromtext`)
- **WKT extraction**: Handles three argument patterns:
  - `ST_Point(lng, lat)` → constructs `"POINT(lng lat)"` WKT
  - `ST_GeomFromText('WKT')` / `ST_GeometryFromText('WKT')` → extracts the WKT string
  - String literal → uses directly as WKT
- **Output**: Populates `SpatialSearchOptions` on the `LogicalOlapScanOperator` with `enableUseS2Index=true`, query WKT, predicate type, and column ID
- **Key design**: The rule **keeps the predicate intact** (unlike vector index which removes it), because S2 cell coverings are approximate — the expression-level predicate must still evaluate for exact results
- **Note**: `st_distance_sphere` is **not supported** because its signature takes 4 raw DOUBLE coordinate arguments (x_lng, x_lat, y_lng, y_lat), not geometry column references. There is no single geometry column to match against an S2 index

#### 7.8 RuleType.java
**File**: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/RuleType.java`

- Added `TF_SPATIAL_REWRITE_RULE` enum entry (after `TF_VECTOR_REWRITE_RULE`)
- Added `GP_SPATIAL_REWRITE` group enum entry (after `GP_VECTOR_REWRITE`)

#### 7.9 RuleSet.java
**File**: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/RuleSet.java`

- Added import for `RewriteToSpatialPlanRule`
- Added `SPATIAL_REWRITE_RULES` constant (after `VECTOR_REWRITE_RULES`):

```java
public static final Rule SPATIAL_REWRITE_RULES = new CombinationRule(RuleType.GP_SPATIAL_REWRITE, ImmutableList.of(
        new RewriteToSpatialPlanRule()
));
```

#### 7.10 QueryOptimizer.java
**File**: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/QueryOptimizer.java`

- Added `scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.SPATIAL_REWRITE_RULES)` after the vector rewrite line, so spatial predicate rewriting runs as part of the post-MV RBO phase

### 8. BE Unit Tests

#### 8.1 s2_index_test.cpp
**File**: `be/test/storage/index/s2_index_test.cpp`

- 20 test cases covering S2IndexWriter and S2IndexReader:
  - Writer initialization with valid/invalid S2 levels
  - Point/line/polygon WKT processing
  - Invalid WKT handling (silent skip)
  - Null value handling
  - File format validation (magic, version, checksum)
  - Reader open/query_cells operations
  - Multi-row, multi-cell queries
  - Empty index files
  - Round-trip writer→reader tests
  - Corrupted file detection
  - Mixed geometry types

#### 8.2 CMakeLists.txt (Test Registration)
**File**: `be/test/CMakeLists.txt`

- Added `${TEST_DIR}/storage/index/s2_index_test.cpp` to `DW_TEST_FILES` list (after `builtin_inverted_index_test.cpp`)

### 9. FE Optimizer Unit Tests

#### 9.1 S2IndexTest.java
**File**: `fe/fe-core/src/test/java/com/starrocks/planner/S2IndexTest.java`

- 15 plan-level test cases for `RewriteToSpatialPlanRule`, extending `PlanTestBase`:
  - **Positive cases** (S2INDEX: ON):
    - `st_contains(column, 'POINT(...)')` — direct column ref with string literal WKT
    - `st_contains('POLYGON(...)', column)` — reversed argument order
    - `st_contains(column, ST_Point(lng, lat))` — ST_Point constructor
    - `st_contains(ST_GeomFromText(column), ST_GeomFromText('...'))` — wrapped geo constructors
    - `st_contains` in AND compound predicate
    - Different table with different `s2_level`
  - **Negative cases** (S2INDEX: OFF):
    - Table without S2 index
    - Predicate on non-indexed column
    - Non-spatial predicate (e.g., `id > 10`)
    - No WHERE clause
    - Both arguments are constants
    - Both arguments are column references
    - OR compound predicate
    - Feature flag disabled → S2INDEX line absent entirely
  - **Predicate preservation**: Verifies that `st_contains` predicate remains in the plan output (S2 is a pre-filter, not exact)

- Setup creates 3 test tables: `test_s2_geo` (S2 index on `geo_col`), `test_no_s2_index` (no index), `test_s2_level20` (S2 index at level 20)

## Usage

### Enabling S2 Index

Before using S2 index, enable the experimental feature in FE configuration:

```sql
-- Via SQL
ADMIN SET FRONTEND CONFIG ("enable_experimental_s2_index" = "true");

-- Or in fe.conf
enable_experimental_s2_index = true
```

### Creating Tables with S2 Index

```sql
-- Create a DUPLICATE KEY table with S2 index (custom s2_level)
CREATE TABLE geo_table (
    id INT,
    location VARCHAR(255),
    INDEX idx_location (location) USING S2 ("s2_level"="13") COMMENT 'S2 index for geo queries'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10;

-- Create a DUPLICATE KEY table with S2 index (default s2_level of 13)
CREATE TABLE geo_table_default (
    id INT,
    location VARCHAR(255),
    INDEX idx_location (location) USING S2 COMMENT 'S2 index for geo queries'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10;

-- Create a PRIMARY KEY table with S2 index
CREATE TABLE geo_table_pk (
    id INT,
    location VARCHAR(255),
    INDEX idx_location (location) USING S2 ("s2_level"="15") COMMENT 'S2 index for geo queries'
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10;

-- Create a table with VARBINARY column for encoded geo data
CREATE TABLE geo_table_binary (
    id INT,
    location VARBINARY,
    INDEX idx_location (location) USING S2 ("s2_level"="15") COMMENT 'S2 index for binary geo data'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10;
```

### Adding and Removing S2 Indexes on Existing Tables

```sql
-- Add an S2 index to an existing table using ALTER TABLE
ALTER TABLE geo_table ADD INDEX idx_location (location) USING S2 ("s2_level"="13") COMMENT 'S2 index';

-- Add an S2 index using the standalone CREATE INDEX statement
CREATE INDEX idx_location ON geo_table (location) USING S2 ("s2_level"="13");

-- Drop an S2 index using ALTER TABLE
ALTER TABLE geo_table DROP INDEX idx_location;

-- Drop an S2 index using the standalone DROP INDEX statement
DROP INDEX idx_location ON geo_table;
```

### Supported Column Types

- `VARCHAR` - For WKT (Well-Known Text) format geo data
- `VARBINARY` - For encoded binary geo data

### Index Parameters

| Parameter | Type | Default | Range | Description |
|-----------|------|---------|-------|-------------|
| s2_level | Integer | 13 | 0-30 | S2 cell level for indexing. Higher levels provide finer granularity but use more storage |

**Note:** Property keys and values must be quoted strings in SQL, e.g. `("s2_level"="13")`.
Unsupported property keys will be rejected with an error.

### Supported Table Types

- DUPLICATE KEY tables
- PRIMARY KEY tables

## S2 Level Recommendations

The `s2_level` parameter controls the granularity of the spatial index:

| Level | Approximate Cell Size | Use Case |
|-------|----------------------|----------|
| 10 | ~100 km | Continental/regional queries |
| 13 | ~10 km | City-level queries |
| 15 | ~2.5 km | Neighborhood-level queries |
| 20 | ~100 m | Building-level queries |
| 30 | ~1 cm | Precise measurements |

Higher levels provide more precise indexing but require more storage and computation during index building.

## Implementation Status

### Completed

- **FE**: SQL parsing, validation, parameter handling, config flag
- **Thrift/Protobuf**: Enum values for cross-component communication, `TSpatialSearchOptions` struct
- **BE metadata**: Schema conversion, schema change detection, index descriptors
- **BE storage**: File delete/link/copy/replicate/snapshot/write path for `.s2i` files
- **S2IndexWriter**: Parses WKT geometry, computes S2 cell coverings, writes `.s2i` files
- **S2IndexReader**: Reads `.s2i` files, validates integrity, provides cell-based spatial lookup
- **ScalarColumnWriter integration**: S2IndexWriter creation in `init()`, data feeding in `append()`, `write_s2_index()` in finalize pipeline
- **SegmentIterator integration**: Query-time S2 index filtering with `SpatialSearchOption` context, `_init_s2_reader()`, `_get_row_ranges_by_s2_index()`, profile counters, statistics
- **OlapChunkSource**: Reads `TSpatialSearchOptions` from thrift, propagates through TabletReader to SegmentIterator
- **FE Optimizer**: `RewriteToSpatialPlanRule` detects spatial predicates (`st_contains`), matches columns with S2 indexes, extracts query WKT (with column ref unwrapping for geo constructor wrappers), populates `SpatialSearchOptions` on scan operators, serialized to `TSpatialSearchOptions` in `OlapScanNode.toThrift()`. Note: `st_distance_sphere` is not supported (takes 4 DOUBLE args, not geometry columns)
- **FE unit tests**: 15 plan-level test cases for `RewriteToSpatialPlanRule` (positive and negative scenarios)
- **BE unit tests**: 20 test cases for S2IndexWriter and S2IndexReader
- **CMake**: S2 source files in `STORAGE_FILES`, test file in `DW_TEST_FILES`

### Remaining

- SQL integration tests need to be recorded (R file generated via `python3 run.py -d sql/test_s2_index/T/test_s2_index -r` against a running cluster)

## Future Enhancements

Potential future improvements for S2Index:

1. **Multi-level Indexing**: Support for adaptive cell sizing with min/max level configuration
2. **Query Parameters**: Search-time parameters for covering configuration
3. **Geo Functions**: Built-in functions for converting WKT to S2 cell IDs
4. **Lake Support**: Enable S2 index filtering in `LakeDataSource` (Thrift field already added for forward compatibility)
5. **Additional Spatial Predicates**: Support `st_intersects`, `st_within`, `st_dwithin` in the optimizer rule
6. **st_distance_sphere Support**: Would require special handling since `st_distance_sphere(DOUBLE, DOUBLE, DOUBLE, DOUBLE)` takes raw coordinate args, not geometry column references. Could be supported via a custom pattern that detects point columns used in distance calculations

## References

- [Google S2 Geometry Library](https://s2geometry.io/)
- [S2 Cell Hierarchy](https://s2geometry.io/devguide/s2cell_hierarchy)
- StarRocks Index Documentation
