# Rollups Data Flow - Canonical 8-Stage Pipeline

**CRITICAL**: This data flow specification must NEVER be changed. It defines the clean separation of concerns that ensures data integrity throughout the AAP Controller billing metrics system.

## Overview

The rollups data flow follows a strict 10-stage pipeline that separates concerns between raw CSV input, internal processing with native Polars types, and storage format conversion. Each stage has a specific purpose and uses appropriate data types for that stage.

## The 10 Stages - Definitive Flow

### Stage 1: Raw CSV Input (JSON Strings)
- **Operation**: CSV loading with PyArrow schema validation
- **Input Schema**: Raw CSV files (no schema - unstructured data)
- **Output Schema**: `collector_schema()` - PyArrow schema with JSON strings
- **Exact Operation**: `pd.read_csv()` + `validate_with_schema(df, collector_schema())`
- **Data Types**: JSON strings in complex columns (`pa.string()` for facts, lists, etc.)
- **Example**: `{"canonical_facts": "{\"os\": \"linux\", \"arch\": \"x86_64\"}"}`

### Stage 2: CSV to Native Type Conversion (Input Boundary)
- **Operation**: JSON string parsing to native Polars types
- **Input Schema**: `collector_schema()` (JSON strings from Stage 1)
- **Output Schema**: `collector_dataframe_schema()` (Native Polars types)
- **Exact Operation**: `apply_complete_schema(df, schema_type="collector_dataframe")`
- **Conversion Logic**: 
  - JSON strings → Native List types via `map_elements(parse_json_to_list, return_dtype=pd.List)`
  - `pa.string()` → `'List'` for facts dictionaries as key-value pairs
  - `pa.string()` → `'List'` for collections (organizations, inventories)
  - **CRITICAL**: Never use `pd.Object` - transform all JSON to List format instead
- **Purpose**: Prepare data for efficient native aggregation operations

### Stage 3: Initial Aggregation (Group Method)
- **Operation**: Group-by aggregation within single CSV batch
- **Input Schema**: `collector_dataframe_schema()` (native types from Stage 2)
- **Output Schema**: `dataframe_schema()` (native types, identical structure)
- **Exact Operation**: `df.group_by(unique_index_columns()).agg(initial_aggregations())`
- **Aggregation Logic**: 
  - List columns: `.unique()` to collect unique values
  - Struct columns: `.first()` or custom merge functions for dictionaries
  - Numeric columns: `.sum()`, `.max()`, `.min()` as appropriate
- **Purpose**: Aggregate duplicate records within single CSV file

### Stage 4: Rollup Aggregation (Regroup Method)
- **Operation**: Cross-file rollup merging with native type operations
- **Input Schema**: Multiple DataFrames with `dataframe_schema()` types
- **Output Schema**: Single DataFrame with `dataframe_schema()` types (same structure)
- **Exact Operation**: `concat(dataframes).group_by(unique_index_columns()).agg(operations())`
- **Aggregation Logic**:
  - List merging: `merge_native_lists()` - concatenate and deduplicate
  - Struct merging: `merge_native_dicts()` - deep merge dictionary contents
  - Numeric aggregation: `.sum()`, `.max()` based on business logic
- **Purpose**: Combine pre-aggregated data from multiple rollup sources

### Stage 5: Storage Conversion (Storage Boundary)
- **Operation**: Native type to JSON string conversion for storage
- **Input Schema**: `dataframe_schema()` (native types from Stage 4)
- **Output Schema**: `parquet_schema()` (JSON strings for storage)
- **Exact Operation**: `df.with_columns([struct_cols.map_elements(json.dumps), list_cols.map_elements(json.dumps)])`
- **Conversion Logic**:
  - Native Struct → JSON string via `json.dumps()`
  - Native List → JSON array string via `json.dumps()`
  - Primitive types remain unchanged
- **Purpose**: Ensure parquet compatibility and storage consistency

### Stage 6: Parquet Storage (JSON Strings)
- **Operation**: Write DataFrame to parquet with schema validation
- **Input Schema**: `parquet_schema()` (JSON strings from Stage 5)
- **Output Schema**: Stored parquet files with `parquet_schema()` structure
- **Exact Operation**: `validate_with_schema(df, parquet_schema())` + `df.write_parquet()`
- **Data Types**: All complex types as JSON strings (`pa.string()`)
- **Purpose**: Persistent storage with cross-system compatibility

---

## Report Generation Pipeline (Additional Stages)

### Stage 7: Parquet Loading (Report Generation)
- **Operation**: Load multiple parquet files for cross-date report generation
- **Input Schema**: Multiple parquet files with `parquet_schema()` structure
- **Output Schema**: DataFrames with `parquet_schema()` types (JSON strings)
- **Exact Operation**: `pd.read_parquet(file)` + `validate_with_schema(df, parquet_schema())`
- **Data Loading**: Load parquet files from date range, validate schema consistency
- **Purpose**: Load persisted rollup data for multi-file aggregation

### Stage 8: Parquet to Working Schema Conversion
- **Operation**: Convert loaded JSON strings back to native types
- **Input Schema**: `parquet_schema()` (JSON strings from Stage 7)
- **Output Schema**: `dataframe_schema()` (Native Polars types)
- **Exact Operation**: `apply_complete_schema(df, schema_type="dataframe")`
- **Conversion Logic**:
  - JSON strings → Native List types via `map_elements(parse_json_to_list, return_dtype=pd.List)`
  - `pa.string()` → `'List'` for facts dictionaries as key-value pairs  
  - `pa.string()` → `'List'` for collections arrays
  - **CRITICAL**: Never use `pd.Object` - transform all JSON to List format instead
- **Purpose**: Restore native types for efficient cross-file processing

### Stage 9: Multi-File Rollup Merging
- **Operation**: Merge rollups across multiple dates/sources
- **Input Schema**: Multiple DataFrames with `dataframe_schema()` types
- **Output Schema**: Single DataFrame with `dataframe_schema()` types (same structure)
- **Exact Operation**: `concat(all_dataframes).group_by(unique_index_columns()).agg(operations())`
- **Aggregation Logic**: Same as Stage 4 - native type merging operations
- **Purpose**: Combine daily rollups across date ranges for comprehensive reporting

### Stage 10: Report Sheet Generation
- **Operation**: Generate XLSX sheets via specialized group-by aggregations
- **Input Schema**: `dataframe_schema()` (native types from Stage 9)
- **Output Schema**: Report-specific aggregated DataFrames (optimized for XLSX)
- **Exact Operation**: `df.group_by(report_columns).agg(report_specific_aggregations())`
- **Aggregation Examples**:
  - Managed Nodes: Group by host, aggregate job runs and task counts
  - Usage by Collections: Group by collection name, sum durations and counts
  - Inventory Scope: Group by host, merge all organizational associations
- **Purpose**: Create final aggregated data optimized for XLSX report sheets

## Critical Separation Points

### Input Boundary (Stage 1→2)
```python
# CORRECT: Convert JSON strings to native types at input
billing_data = billing_data.with_columns([
    billing_data['canonical_facts'].map_elements(convert_json_to_dict, return_dtype=pd.Object).alias('canonical_facts'),
    billing_data['facts'].map_elements(convert_json_to_dict, return_dtype=pd.Object).alias('facts'),
])
```

### Processing Core (Stages 3-6)
```python
# CORRECT: All internal processing uses native types
collector_dataframe_schema = {
    'canonical_facts': 'Struct',  # Native Struct type
    'facts': 'Struct',            # Native Struct type  
    'organizations': 'List',      # Native List type
}

dataframe_schema = {
    'canonical_facts': 'Struct',  # Native Struct type
    'facts': 'Struct',            # Native Struct type
    'organizations': 'List',      # Native List type
}
```

### Storage Boundary (Stage 6→7)
```python
# CORRECT: Convert native types to JSON strings before storage
result = result.with_columns([
    result['canonical_facts'].map_elements(lambda x: json.dumps(x) if x else '{}', return_dtype=pd.Utf8).alias('canonical_facts'),
    result['organizations'].map_elements(lambda x: json.dumps(x) if x else '[]', return_dtype=pd.Utf8).alias('organizations'),
])
```

## Schema Method Responsibilities

### `collector_schema()` - Stage 1
- **Purpose**: Validate raw CSV input
- **Format**: JSON strings (`pa.string()` for all complex columns)
- **Usage**: Raw CSV validation only

### `collector_dataframe_schema()` - Stages 3-4
- **Purpose**: Define processing schema with native types
- **Format**: Native Polars types (`'Struct'`, `'List'`)
- **Usage**: After input conversion, before/during `group()` method

### `dataframe_schema()` - Stages 4-6  
- **Purpose**: Define working schema for all internal operations
- **Format**: Native Polars types (same as collector_dataframe_schema)
- **Usage**: All intermediate processing, `regroup()` method

### `parquet_schema()` - Stage 8
- **Purpose**: Define storage format
- **Format**: JSON strings (`pa.string()` for complex columns)
- **Usage**: Parquet writing/reading validation

## Aggregation Patterns

### Native Type Aggregation (Stages 4-6)
```python
# Lists: Collect unique values
pd.col('organizations').unique().alias('organizations')

# Structs: Merge dictionaries with custom logic
pd.col('canonical_facts').map_batches(
    lambda s: pd.Series([merge_native_dicts(s.to_list())]),
    return_dtype=pd.Object
).first().alias('canonical_facts')
```

### Custom Merge Functions
```python
def merge_native_lists(series):
    """Merge multiple List columns into single List with unique values"""
    all_values = []
    for lst in series:
        if lst is not None and isinstance(lst, list):
            all_values.extend(lst)
        elif lst is not None:
            all_values.append(lst)
    return sorted(list(dict.fromkeys([str(v) for v in all_values if v is not None])))

def merge_native_dicts(series):
    """Merge multiple dict columns following demo_prompt_facts.md patterns"""
    merged_dict = {}
    for d in series:
        if d is not None and isinstance(d, dict):
            for key, values in d.items():
                if key not in merged_dict:
                    merged_dict[key] = []
                
                if isinstance(values, list):
                    merged_dict[key].extend(values)
                else:
                    merged_dict[key].append(values)
    
    # Remove duplicates and sort
    for key in merged_dict:
        filtered = [str(x) for x in merged_dict[key] if x is not None]
        merged_dict[key] = sorted(list(set(filtered)))
    
    return merged_dict
```

## Dict-to-List Transformation Strategy

### Why Lists Instead of Structs/Objects

**CRITICAL PRINCIPLE**: Never use `pd.Object` in Polars - it doesn't exist and causes casting failures.

Instead, transform dictionary-like JSON data into Lists of key-value pairs:

```python
# WRONG: Trying to use Object/Struct types
facts = {"fact2": ["value1", "value2"], "fact3": ["value1"]}  # pd.Object - fails

# CORRECT: Transform to List of key-value pairs with consistent array format
facts = [["fact2", ["value1", "value2"]], ["fact3", ["value1"]]]  # pd.List - works
```

### Dictionary-to-List Conversion

**CRITICAL**: All values must be arrays for consistent merging operations.

```python
def dict_to_list_pairs(json_dict):
    """Convert dictionary to list of [key, value] pairs with consistent array format."""
    if json_dict is None or not isinstance(json_dict, dict):
        return []
    result = []
    for key, value in json_dict.items():
        # Ensure all values are arrays for consistent merging
        if isinstance(value, list):
            result.append([key, value])
        else:
            result.append([key, [value]])  # Wrap single values in arrays
    return result

def list_pairs_to_dict(list_pairs):
    """Convert list of [key, value] pairs back to dictionary."""
    if not isinstance(list_pairs, list):
        return {}
    return {pair[0]: pair[1] for pair in list_pairs if len(pair) == 2}
```

### Benefits of List-Based Approach

1. **Native Polars Support**: Lists are fully supported native types
2. **Efficient Aggregation**: Can use `.unique()`, `.concat()` operations  
3. **Consistent Schema**: No type casting failures
4. **Storage Compatible**: Lists serialize cleanly to JSON arrays

## Type Conversion Utilities

### JSON to Native (Stage 1→2)
```python
def convert_json_to_dict(json_str):
    """Convert JSON string to native dict for Polars Struct"""
    import json
    if json_str is None or json_str == '':
        return {}
    try:
        parsed = json.loads(json_str)
        return parsed if isinstance(parsed, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}
```

### Native to JSON (Stage 6→7)
```python
def convert_dict_to_json(native_dict):
    """Convert native dict to JSON string for storage"""
    import json
    if native_dict is None:
        return '{}'
    try:
        return json.dumps(native_dict)
    except (TypeError, ValueError):
        return '{}'
```

## Type Flow Summary

### Complete 10-Stage Pipeline Flow

```
ROLLUP GENERATION PIPELINE:
Stage 1: Raw CSV Input (JSON Strings)
         ↓ collector_schema() validation
Stage 2: CSV to Native Type Conversion
         ↓ convert_json_to_dict()
Stage 3: Collector DataFrame Schema (Native Types)  
         ↓ collector_dataframe_schema() applied
Stage 4: Initial Aggregation (Group Method)
         ↓ group() with native type operations
Stage 5: Rollup Aggregation (Regroup Method)
         ↓ regroup() with native type merging
Stage 6: Parquet Storage (JSON Strings)
         ↓ parquet_schema() validation & storage

REPORT GENERATION PIPELINE:
Stage 7: Parquet Loading (Report Generation)
         ↓ load multiple parquet files
Stage 8: Parquet to Working Schema Conversion
         ↓ parquet_schema() → dataframe_schema()
Stage 9: Multi-File Rollup Merging
         ↓ concat + regroup with native types
Stage 10: Report Sheet Generation
         ↓ group_by.agg for XLSX sheets
```

### Detailed Schema Application Flow

```
Stage 1: Raw CSV Input
         ↓ Schema: collector_schema() - JSON Strings (pa.string)
         ↓ Purpose: Validate CSV structure

Stage 2: Input Boundary Conversion  
         ↓ Process: convert_json_to_dict()
         ↓ Transform: JSON Strings → Native Types

Stage 3: Collector DataFrame Processing
         ↓ Schema: collector_dataframe_schema() - Native Types (Struct/List)
         ↓ Purpose: Efficient processing before aggregation

Stage 4: Initial Aggregation (Group)
         ↓ Process: group() method with native operations
         ↓ Types: Native Polars aggregation (.unique(), .first())

Stage 5: Working DataFrame Operations
         ↓ Schema: dataframe_schema() - Native Types (Struct/List)  
         ↓ Purpose: Internal processing and merging

Stage 6: Rollup Aggregation (Regroup)
         ↓ Process: regroup() method with native merging
         ↓ Functions: merge_native_lists(), merge_native_dicts()

Stage 6: Parquet Storage
         ↓ Schema: parquet_schema() - JSON Strings (pa.string)
         ↓ Purpose: Persistent storage with compatibility
```

### Data Type Evolution by Stage

```
Stages 1:     JSON Strings      (Raw CSV)
Stage 2:      Native Types      (Input Boundary)
Stages 3-5:   Native Types      (Processing Core)
Stage 6:      JSON Strings      (Parquet Storage)
```

### Schema Method Usage by Stage

```
Stage 1: collector_schema()           → JSON Strings (pa.string)
Stage 3: collector_dataframe_schema() → Native Types (Struct/List)
Stage 5: dataframe_schema()           → Native Types (Struct/List)
Stage 6: parquet_schema()             → JSON Strings (pa.string)
```

## Validation Rules

### Stage Validation
1. **Stage 1**: Use PyArrow schema validation for CSV structure
2. **Stages 3-5**: Use Polars native type operations - no explicit validation needed
3. **Stage 6**: Use PyArrow schema validation for storage format

### Schema Consistency
- `collector_dataframe_schema()` and `dataframe_schema()` MUST be identical
- Both MUST use native Polars types for all complex columns
- `collector_schema()` and `parquet_schema()` use JSON strings for compatibility

## Error Handling

### Type Conversion Failures
- Always provide safe defaults (empty dict `{}`, empty list `[]`)
- Log conversion errors but continue processing
- Use `map_elements()` with proper error handling

### Aggregation Failures  
- Graceful degradation for invalid data types
- Preserve valid data when some records fail
- Comprehensive error logging with OpenTelemetry spans

## Performance Considerations

### Memory Efficiency
- Native types reduce memory overhead vs JSON strings
- Process in batches for large datasets
- Use lazy evaluation where possible

### Processing Speed
- Native Polars operations are significantly faster than JSON manipulation
- Batch operations reduce overhead
- Minimize type conversions (only at boundaries)

## Integration Points

### Base Class Integration
- Schema validation handled by base class methods
- Type conversion logic in dataframe-specific transformation methods  
- Aggregation patterns implemented in `group()` and `regroup()` methods

### OpenTelemetry Tracing
- Track performance at each stage boundary
- Monitor type conversion overhead
- Log aggregation performance metrics

## Examples

### Complete Pipeline Example
```python
# Stage 1: Raw CSV with JSON strings
csv_data = pd.read_csv("data.csv")  # canonical_facts as JSON string

# Stage 2: Convert to native types
df = csv_data.with_columns([
    csv_data['canonical_facts'].map_elements(convert_json_to_dict, return_dtype=pd.Object).alias('canonical_facts')
])

# Stages 3-5: All processing with native types
grouped = df.group_by(['host_name']).agg([
    pd.col('canonical_facts').map_batches(
        lambda s: pd.Series([merge_native_dicts(s.to_list())]),
        return_dtype=pd.Object
    ).first().alias('canonical_facts')
])

# Stage 6: Convert back to JSON for storage and write to parquet
storage_ready = grouped.with_columns([
    grouped['canonical_facts'].map_elements(convert_dict_to_json, return_dtype=pd.Utf8).alias('canonical_facts')
])
storage_ready.write_parquet("output.parquet")
```

This data flow ensures optimal performance during processing while maintaining compatibility with storage systems and external interfaces. The clean separation of concerns prevents data corruption and ensures consistent behavior across all dataframe engines.