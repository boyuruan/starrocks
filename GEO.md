# Geo Functions Implementation Guide for StarRocks

This document describes the implementation of geo functions in StarRocks and the modifications made to add `ST_AsGeoJSON` function support.

## Modifications Made

### 1. Frontend (FE) - Function Registration

**File**: `fe/fe-core/src/main/java/com/starrocks/catalog/FunctionSet.java`

Added function name constant:
```java
// Geo functions:
public static final String ST_ASTEXT = "st_astext";
public static final String ST_ASGEOJSON = "st_asgeojson";  // NEW
public static final String ST_ASWKT = "st_aswkt";
```

### 2. Code Generation - Function Definition

**File**: `gensrc/script/functions.py`

Added function definition with ID `120015`:
```python
[120015, "ST_AsGeoJSON", False, False, "VARCHAR", ["VARCHAR"], "GeoFunctions::st_asgeojson"],
```

### 3. Backend (BE) - Function Implementation

**File**: `be/src/exprs/geo_functions.cpp`

The `st_asgeojson` function was already implemented:
```cpp
StatusOr<ColumnPtr> GeoFunctions::st_asgeojson(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> shape_viewer(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    
    for (int row = 0; row < size; ++row) {
        if (shape_viewer.is_null(row)) {
            result.append_null();
            continue;
        }
        
        auto shape_value = shape_viewer.value(row);
        std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(shape_value.data, shape_value.size));
        if (shape == nullptr) {
            result.append_null();
            continue;
        }
        
        auto geojson = shape->as_geojson();
        result.append(Slice(geojson.data(), geojson.size()));
    }
    
    return result.build(ColumnHelper::is_all_const(columns));
}
```

**File**: `be/src/exprs/geo_functions.h`

Declaration in header file:
```cpp
/**
 * Convert geometry to GeoJSON format.
 */
DEFINE_VECTORIZED_FN(st_asgeojson);
```

## Function ID Ranges Analysis

### Findings

The function IDs in StarRocks are organized by category:

| ID Range | Category | Examples |
|----------|----------|----------|
| 120000 - 1200xx | **Geo Functions** | ST_Point (120000), ST_X (120001), ST_Y (120002), ST_AsText (120004), ST_Contains (120014), **ST_AsGeoJSON (120015)** |
| 130000 - 1300xx | Percentile Functions | percentile_hash, percentile_approx_raw |
| 140000 - 1400xx | Grouping Functions | grouping_id, grouping |
| 150000 - 150xxx | Array Functions | array_length, array_append, array_contains |

### Geo Function IDs (12xxxx)

| ID | Function Name | Return Type | Arguments |
|----|---------------|-------------|-----------|
| 120000 | ST_Point | VARCHAR | [DOUBLE, DOUBLE] |
| 120001 | ST_X | DOUBLE | [VARCHAR] |
| 120002 | ST_Y | DOUBLE | [VARCHAR] |
| 120003 | ST_Distance_Sphere | DOUBLE | [DOUBLE, DOUBLE, DOUBLE, DOUBLE] |
| 120004 | ST_AsText | VARCHAR | [VARCHAR] |
| 120005 | ST_AsWKT | VARCHAR | [VARCHAR] |
| 120006 | ST_GeometryFromText | VARCHAR | [VARCHAR] |
| 120007 | ST_GeomFromText | VARCHAR | [VARCHAR] |
| 120008 | ST_LineFromText | VARCHAR | [VARCHAR] |
| 120009 | ST_LineStringFromText | VARCHAR | [VARCHAR] |
| 120010 | ST_Polygon | VARCHAR | [VARCHAR] |
| 120011 | ST_PolyFromText | VARCHAR | [VARCHAR] |
| 120012 | ST_PolygonFromText | VARCHAR | [VARCHAR] |
| 120013 | ST_Circle | VARCHAR | [DOUBLE, DOUBLE, DOUBLE] |
| 120014 | ST_Contains | BOOLEAN | [VARCHAR, VARCHAR] |
| **120015** | **ST_AsGeoJSON** | **VARCHAR** | **[VARCHAR]** |

## How to Add a New Geo Function

### Step 1: Implement in Backend (C++)

1. Add function implementation in `be/src/exprs/geo_functions.cpp`:
```cpp
StatusOr<ColumnPtr> GeoFunctions::your_function(FunctionContext* context, const Columns& columns) {
    // Implementation
}
```

2. Add declaration in `be/src/exprs/geo_functions.h`:
```cpp
DEFINE_VECTORIZED_FN(your_function);
```

### Step 2: Register in Code Generation

Add entry in `gensrc/script/functions.py` with the next available ID in the 12xxxx range:
```python
[120016, "YourFunctionName", False, False, "RETURN_TYPE", ["ARG_TYPE1", "ARG_TYPE2"], "GeoFunctions::your_function"],
```

Field meanings:
- `120016`: Unique function ID (increment from last geo function)
- `"YourFunctionName"`: SQL function name (case-insensitive)
- `False`: Is aggregate function
- `False`: Can apply dict optimization
- `"RETURN_TYPE"`: Return data type
- `["ARG_TYPE1", ...]`: Argument types
- `"GeoFunctions::your_function"`: Backend function path

### Step 3: Register in Frontend (Java)

Add constant in `fe/fe-core/src/main/java/com/starrocks/catalog/FunctionSet.java`:
```java
public static final String YOUR_FUNCTION = "yourfunctionname";
```

### Step 4: Build

```bash
# Full build
./build.sh

# Or FE only
./build.sh --fe

# Or BE only  
./build.sh --be
```

## Using the ST_AsGeoJSON Function

After building, the function can be used in SQL:

```sql
-- Convert geometry to GeoJSON format
SELECT ST_AsGeoJSON(geometry_column) FROM table_name;

-- Works with case-insensitive names
SELECT st_asgeojson(geometry_column) FROM table_name;
SELECT St_AsGeoJson(geometry_column) FROM table_name;
```

## Notes

1. **Function Name Case**: SQL function names are case-insensitive. Use `ST_` prefix for consistency.

2. **Geometry Encoding**: Geo functions use VARCHAR to store encoded geometry data internally.

3. **Null Handling**: Functions should handle NULL inputs by returning NULL (use `result.append_null()`).

4. **Constant Optimization**: Use `ColumnHelper::is_all_const(columns)` for constant folding.

5. **Prepare/Close**: Functions that need initialization can specify prepare/close functions:
   ```python
   [120013, "ST_Circle", False, False, "VARCHAR", ["DOUBLE", "DOUBLE", "DOUBLE"], 
    "GeoFunctions::st_circle", "GeoFunctions::st_circle_prepare", "GeoFunctions::st_from_wkt_close"],
   ```

## References

- Backend implementation: `be/src/exprs/geo_functions.cpp`
- Backend header: `be/src/exprs/geo_functions.h`
- Function definitions: `gensrc/script/functions.py`
- FE constants: `fe/fe-core/src/main/java/com/starrocks/catalog/FunctionSet.java`
