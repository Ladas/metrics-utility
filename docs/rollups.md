# Rollups System

The Rollups system provides pre-computed daily aggregations of metrics data stored as parquet files, enabling faster report generation by avoiding the need to reprocess raw data for each report request.

## Quick Start

### Basic Usage

**1. Compute rollups for a date range**:
```bash
# In Docker environment (recommended)
docker compose -f tools/docker/docker-compose.yaml exec metrics-utility-env uv run python manage.py compute_rollups --since=2025-07-08 --until=2025-07-11 --force

# Or locally with environment variables set
python manage.py compute_rollups --since=2025-07-08 --until=2025-07-11 --force
```

**2. Generate reports using rollups**:
```bash
# Reports automatically use rollups when available, and compute missing ones as needed
docker compose -f tools/docker/docker-compose.yaml exec metrics-utility-env uv run python manage.py build_report --since=2025-07-08 --until=2025-07-11 --force
```

---

## Overview

The rollup system provides a unified architecture for pre-computing and loading daily aggregated dataframes:

- **Rollup Computation**: Generates daily aggregated dataframes using Polars for efficient processing and stores them as parquet files
- **Report Generation**: Loads pre-computed parquet files with schema consistency and merges them using Polars operations
- **Data Integrity**: Enforces strict validation of unique index columns and proper aggregation operations
- **Error Handling**: Comprehensive error detection for missing operations and data integrity violations

## Architecture

### Directory Structure

```
SHIP_PATH/
├── data/                                    # Raw input data (tarballs)
│   └── YYYY/
│       └── MM/
│           └── DD/
│               └── *.tar.gz
├── rollups/                                 # Pre-computed rollup data
│   └── daily/
│       └── YYYY/
│           └── MM/
│               └── DD/
│                   ├── DataframeJobhostSummaryUsage/
│                   │   ├── 20250821_160000_123456_v1.0/
│                   │   │   ├── data.parquet
│                   │   │   └── metadata.json
│                   │   ├── 20250821_170000_654321_v1.0/
│                   │   │   ├── data.parquet
│                   │   │   └── metadata.json
│                   │   ├── 20250821_180000_789012_v1.0__status__error/
│                   │   │   └── metadata.json    # Error status (no data.parquet)
│                   │   └── 20250821_190000_567890_v1.0__status__no_data/
│                   │       └── metadata.json    # No data status (no data.parquet)
│                   ├── DataframeContentUsage/
│                   │   └── 20250821_160000_234567_v1.0/
│                   │       ├── data.parquet
│                   │       └── metadata.json
│                   ├── DataframeInventoryScope/
│                   │   └── 20250821_160000_345678_v1.0/
│                   │       ├── data.parquet
│                   │       └── metadata.json
│                   └── DataframeCollectionStatus/ (CCSPv2 only)
│                       └── 20250821_160000_456789_v1.0__status__no_data/
│                           └── metadata.json    # Often has no data for many dates
└── reports/                                 # Generated XLSX reports
    └── YYYY/
        └── MM/
            └── ReportType-YYYY-MM-DD--YYYY-MM-DD.xlsx
```

### Metadata Schema

Each version directory contains a `metadata.json` file that tracks computation metadata:

```json
{
  "computation_timestamp": "2025-08-21T16:00:00.123456Z",
  "processing_status": "complete",
  "dataframe_name": "DataframeJobhostSummaryUsage",
  "since_date": "2025-03-01",
  "until_date": "2025-03-01", 
  "records_processed": 1250,
  "processing_time_seconds": 15.7,
  "version": "20250821_160000_123456_v1.0",
  "error_message": null
}
```

#### Versioning System

Versions are identified by timestamp with microsecond precision to prevent conflicts:

- **Data versions**: `YYYYMMDD_HHMMSS_FFFFFF_v1.0`
- **Error versions**: `YYYYMMDD_HHMMSS_FFFFFF_v1.0__status__error` (processing failed)
- **No source data versions**: `YYYYMMDD_HHMMSS_FFFFFF_v1.0__status__no_source_data` (no tarballs found)
- **No data versions**: `YYYYMMDD_HHMMSS_FFFFFF_v1.0__status__no_data` (tarballs found but empty data)
- **Automatic latest detection**: System finds the latest valid version by timestamp sorting
- **Status-aware loading**: Skips status versions (`__status__*`) when loading actual data

#### Status Version Behavior

- **Data versions**: Contain `data.parquet` + `metadata.json` files with actual metrics data
- **Status versions**: Contain only `metadata.json` with processing status information:
  - `__status__no_source_data`: No tarballs found for this date/dataframe combination
  - `__status__no_data`: Tarballs existed but contained no data after processing
  - `__status__error`: Exception occurred during dataframe computation or parquet storage
- **No duplicate versions**: Fixed logic ensures only one version per computation (either data or status)
- **Smart loading**: Reports automatically merge only available data versions, skipping status versions
- **Graceful degradation**: Reports generate with partial data when some dates have no source data

## Rollup Computation Flow with Unified Architecture

```mermaid
graph TD
    A[compute_rollups.py Command.handle] --> B[Parse date parameters --since --until]
    B --> C[Initialize RollupManager with ship_path]
    C --> D[RollupManager.create_smart_dependency_plan]
    D --> E[RollupManager.scan_rollups_directory]
    E --> F[ExtractorDirectory.scan_tarballs_for_date]
    F --> G[RollupManager.identify_stale_rollups]
    G --> H{Source data newer than rollup?}
    H -->|Yes| I[Mark rollup for recomputation]
    H -->|No| J[Keep existing rollup]
    I --> K[Track new tarballs for incremental update]
    J --> L[Generate BatchRollupTask date groups]
    K --> L
    L --> M[Execute date groups sequentially]
    M --> N[Command._compute_rollup_for_batch_task]
    N --> O[RollupDataframeFactory._create_dataframes_for_date]
    O --> P[Create dataframe instances for all required classes]
    P --> Q[ExtractorDirectory.iter_batches for target_date]
    Q --> R[For each batch: Add _date_context to batch_data]
    R --> S[For each DataframeClass: call build_dataframe with single_batch_iterator]
    S --> T[DataframeClass.build_dataframe processes CSV data from batch]
    T --> U[DataframeClass._process_batch_data converts raw CSV to grouped data]
    U --> V[DataframeClass.merge results with accumulated dataframe]
    V --> W{More batches for same date?}
    W -->|Yes| X[Continue accumulating with merge operations]
    W -->|No| Y[Daily processing complete: dict DataframeName -> grouped_dataframe]
    X --> Q
    Y --> Z[RollupManager.save_rollup_data for each dataframe]
    Z --> AA{Data available?}
    AA -->|No| BB[RollupManager.save_no_data_metadata]
    AA -->|Yes| CC[Save data.parquet + metadata.json with version]
    AA -->|Error| DD[RollupManager.save_error_metadata with Date group failed: exception]
    BB --> EE[All dataframes stored for this date]
    CC --> EE
    DD --> EE
    EE --> FF{More dates?}
    FF -->|Yes| M
    FF -->|No| GG[Complete rollup computation]
```

## Report Generation from Rollups

```mermaid
graph TD
    A[build_report.py Command.handle] --> B[Parse date range --since --until]
    B --> C[Create ExtractorFactory and RollupDataframeFactory]
    C --> D[RollupDataframeFactory.create]
    D --> E[Check rollups availability with source data scanning]
    E --> F{Missing or stale rollups?}
    F -->|Yes| G[Auto-compute missing/stale rollups]
    F -->|No| H[Load existing rollups]
    G --> H
    
    H --> I[RollupReader.load_rollup_dataframes]
    I --> J[For each date and required dataframe]
    J --> K[Load parquet with schema consistency]
    K --> L[Merge daily data using dataframe class operations]
    L --> M[Accumulate merged results]
    M --> N{More dates?}
    N -->|Yes| J
    N -->|No| O[Map to standard names for reports]
    
    O --> P[Apply deduplication]
    P --> Q{Data available?}
    Q -->|No| R[Generate empty report with warnings]
    Q -->|Yes| S[Generate report with data]
    R --> T[Save XLSX report]
    S --> T

    subgraph "Schema Consistency"
        K --> K1[Ensure complete schema]
        K --> K2[Apply consistent casting]
        K --> K3[Handle missing columns]
    end

    subgraph "Error Handling"
        L --> L1[Mixed type comparison protection]
        L --> L2[Safe min/max operations with NaN handling]
        L --> L3[Detailed error logging]
    end
```

## Dataframe Mappings

The rollup system stores aggregated dataframes that directly correspond to report sections:

| Dataframe Class | Parquet File | Report Sections | Key Columns |
|-----------------|--------------|-----------------|-------------|
| `DataframeJobhostSummaryUsage` | `DataframeJobhostSummaryUsage.parquet` | Managed Nodes | `host_name`, `organization_name`, `task_runs`, `host_runs`, `first_automation`, `last_automation` |
| `DataframeContentUsage` | `DataframeContentUsage.parquet` | Usage by Collections/Roles/Modules | `host_name`, `collection_name`, `role_name`, `module_name`, `task_runs`, `duration` |
| `DataframeInventoryScope` | `DataframeInventoryScope.parquet` | Inventory Scope | `host_name`, `organizations`, `inventories`, `canonical_facts`, `facts` |
| `DataframeCollectionStatus` | `DataframeCollectionStatus.parquet` | Data Collection Status (CCSPv2) | `cluster_id`, `reporting_date`, `collection_status`, `data_quality_score` |

## Key Architecture Classes and Methods

**RollupDataframeFactory (Main Factory):**
- `RollupDataframeFactory.create()` - Main entry point for dataframe creation
- `RollupDataframeFactory._create_dataframes_for_date()` - Unified data loading for single date
- `RollupDataframeFactory._create_from_rollups()` - Load from existing rollups with schema consistency
- `RollupDataframeFactory._get_table_to_class_mapping()` - Maps CSV table names to dataframe classes
- `RollupDataframeFactory._get_rollup_to_standard_mapping()` - Maps class names to standard report names

**Dataframe Classes (Unified Interface):**
- `DataframeJobhostSummaryUsage.build_dataframe(batch_data_iterator)` - Process job_host_summary and indirect_nodes
- `DataframeContentUsage.build_dataframe(batch_data_iterator)` - Process main_jobevent data
- `DataframeInventoryScope.build_dataframe(batch_data_iterator)` - Process main_host data
- `DataframeCollectionStatus.build_dataframe(batch_data_iterator)` - Process data_collection_status data

**Base Class Features (Polars Implementation):**
- `Base.load_from_parquet(parquet_path)` - Schema-consistent parquet loading with complex type handling
- `Base.merge(rollup, new_group)` - Safe dataframe merging with schema alignment and data integrity checks
- `Base.summarize_merged_dataframes()` - Enforced operation definitions with _right column validation
- `Base._ensure_complete_schema()` - Add missing columns with proper defaults and type compatibility
- `Base._apply_consistent_casting()` - Polars-native type consistency for merge compatibility
- `Base._clean_right_columns()` - Remove conflicting _right columns before join operations
- `Base._align_schemas_for_join()` - Ensure compatible schemas with type conversion for joins

**RollupReader (Rollup Loading):**
- `RollupReader.load_rollup_dataframes()` - Load and merge rollups with schema consistency
- `RollupReader._merge_using_dataframe_class()` - Use dataframe class merge operations
- `RollupReader.check_rollups_with_source_data()` - Smart dependency checking with source data scanning

**RollupManager (Storage & Versioning):**
- `RollupManager.save_rollup_data()` - Store successful rollup with data.parquet
- `RollupManager.save_no_data_metadata()` - Store no-data status with metadata only
- `RollupManager.save_error_metadata()` - Store error status with exception details
- `RollupManager.create_smart_dependency_plan()` - Incremental update planning

## Error Handling and Data Quality

### Data Integrity and Operation Validation

The system includes comprehensive data integrity checks and operation validation:

```python
# In Base.summarize_merged_dataframes() - Polars implementation
if operations.get(col) == 'min':
    try:
        # Use Polars min_horizontal for proper column-wise operation
        df = df.with_columns(
            pd.min_horizontal([left_col, right_col]).alias(col)
        )
    except TypeError as e:
        if 'not supported between instances' in str(e):
            logger.warning(f'Mixed type comparison detected for column {col} during min operation: {e}. Using safe comparison fallback.')
            # Use coalesce approach for mixed types
            df = df.with_columns(df[left_col].fill_null(df[right_col]).alias(col))
else:
    # CRITICAL: Missing operation definition is a configuration error
    raise ValueError(
        f"Missing operation definition for column '{col}' during merge in {dataframe_class_name}.\n"
        f"  Required action: Add '{col}: \"operation_name\"' to the operations() method.\n"
        f"  Valid operations: 'min', 'max', 'sum', 'combine_set', 'combine_json', 'combine_json_values'"
    )
```

**Critical Data Integrity Checks:**
- **Unique Index Validation**: Detects `_right` columns on unique index columns, indicating improper grouping
- **Operation Enforcement**: Requires explicit operation definitions for all merge conflicts
- **Schema Alignment**: Ensures compatible schemas before join operations with type conversion
- **_right Column Cleanup**: Verifies complete removal of suffix columns after merge operations

**Common Protected Operations:**
- `first_automation` min operations - handles float NaN vs Timestamp comparisons
- `last_automation` max operations - handles float NaN vs Timestamp comparisons  
- `job_created` max operations - handles float NaN vs Timestamp comparisons

**Error Logging:**
```
Mixed type comparison detected for column first_automation during min operation: '<=' not supported between instances of 'float' and 'Timestamp'. Using safe comparison fallback.
```

### Robust Error Handling

**Date Group Processing:**
- Individual date group failures don't stop overall computation
- Error metadata saved with detailed exception information
- Processing continues with partial data for successful dates

**Status Tracking:**
- `__status__error` - Processing failed with detailed error message
- `__status__no_data` - Processing succeeded but no data available
- `__status__no_source_data` - No source tarballs found for date

**Graceful Degradation:**
- Reports generate with available data when some dates fail
- Clear warnings about missing data in report output
- No duplicate version creation prevents storage conflicts

## Usage Commands

### Computing Rollups

```bash
# Compute rollups for a single date
python manage.py compute_rollups --since=2025-03-01

# Compute rollups for a date range  
python manage.py compute_rollups --since=2025-03-01 --until=2025-03-31

# Force recomputation of existing rollups
python manage.py compute_rollups --since=2025-03-01 --force

# Show parallel execution plan without executing
python manage.py compute_rollups --since=2025-03-01 --until=2025-03-07 --parallel

# List available rollups with versions
python manage.py compute_rollups --list

# Clean invalid rollups
python manage.py compute_rollups --since=2025-03-01 --until=2025-03-31 --clean
```

### Generating Reports from Rollups

```bash
# Generate report using rollups (auto-computes missing/stale rollups first - DEFAULT)
python manage.py build_report --since=2025-03-01 --until=2025-03-31

# Generate monthly report using rollups with validation
python manage.py build_report --month=2025-03

# Force report generation with fresh rollup computation
python manage.py build_report --month=2025-03 --force
```

## Performance Benefits

Rollups with Polars provide significant performance improvements:

- **5x I/O Reduction**: Unified data loading reads each tarball exactly once instead of 5 times
- **Polars Performance**: Native Rust implementation provides 2-10x faster processing than pandas
- **Memory Efficiency**: Polars lazy evaluation and optimized memory usage with shared batch iterators
- **Schema Consistency**: Unified parquet loading with automatic type compatibility ensures reliable merging
- **Data Integrity Enforcement**: Strict validation prevents silent data corruption during merge operations
- **Native Parquet Support**: Optimized parquet I/O with columnar storage and predicate pushdown
- **Type Safety**: Polars strong typing prevents runtime errors common in pandas workflows

## Rollup Management

### Smart Computation
- **Batch Processing**: Computes all dataframes for each date together using shared data loading
- **Incremental Updates**: Only computes missing or stale rollups based on source data timestamps
- **Source Data Scanning**: Automatically detects new tarballs and marks rollups for recomputation
- **Dependency Resolution**: Smart planning identifies exactly which dates and dataframes need computation
- **Version Management**: Conflict-free versioning with microsecond timestamp precision

### Status System and Error Handling
- **Data versions**: Successful computation with `data.parquet` files containing actual metrics
- **No source data versions**: `__status__no_source_data` for dates with no tarballs available
- **No data versions**: `__status__no_data` for dates with tarballs but empty processed data
- **Error versions**: `__status__error` for failed computations with detailed error messages
- **Mixed Type Protection**: Automatic handling of float vs Timestamp comparison errors with warning logs
- **Graceful Recovery**: Processing continues with partial data when individual dates fail
- **Smart Loading**: Reports automatically skip status versions and merge only valid data

### Storage Optimization
- **Parquet Format**: Efficient compression and fast loading with 60-80% size reduction vs CSV
- **Columnar Storage**: Supports predicate pushdown for efficient filtering
- **Native Types**: Optimal storage for sets, lists, and JSON data structures
- **Schema Evolution**: Versioned storage allows schema changes without data loss
- **Conflict Prevention**: Microsecond timestamps prevent version conflicts during rapid computation

## Integration with Existing System

The rollup system integrates seamlessly with the existing codebase:

1. **No breaking changes**: Report generation commands remain unchanged
2. **Automatic rollup computation**: Missing rollups are computed on-demand during report generation
3. **Unified Architecture**: Same dataframe merge logic used for both live data and rollup data
4. **Transparent Operation**: System automatically manages rollup computation, versioning, and loading
5. **Robust Error Handling**: Mixed type comparison protection ensures processing reliability
6. **Performance Monitoring**: Comprehensive OpenTelemetry tracing for observability

## Configuration

Rollups are controlled by the same environment variables as standard report generation:

```bash
# Required
METRICS_UTILITY_SHIP_PATH=./data
METRICS_UTILITY_SHIP_TARGET=directory  
METRICS_UTILITY_REPORT_TYPE=CCSPv2

# Optional
METRICS_UTILITY_DEDUPLICATOR=ccsp-experimental
METRICS_UTILITY_ORGANIZATION_FILTER=org1;org2
```

## Monitoring and Troubleshooting

### Check Rollup Status
```bash
# List all available rollups with status
python manage.py compute_rollups --list
```

### Validate Rollup Integrity
```bash
# Clean corrupted rollups for a date range
python manage.py compute_rollups --since=2025-03-01 --until=2025-03-31 --clean
```

### Debug Rollup Issues
1. **Check Error Logs**: Look for mixed type comparison warnings and date group failures
2. **Validate Metadata**: Review `metadata.json` files in error versions for detailed error messages
3. **Verify Source Data**: Ensure tarballs exist and are readable for the date range
4. **Monitor Performance**: Use OpenTelemetry traces to identify bottlenecks
5. **Check Data Quality**: Review warning logs for mixed type comparisons in datetime columns

### Common Error Patterns

**Mixed Type Comparison Errors:**
```
Mixed type comparison detected for column first_automation during min operation: '<=' not supported between instances of 'float' and 'Timestamp'. Using safe comparison fallback.
```
- **Cause**: Generated or corrupted data with mixed float NaN and Timestamp values
- **Resolution**: System automatically uses safe comparison fallback with warning log
- **Action**: Review data quality and generation processes

**Date Group Failures:**
```
Date group failed: Cannot convert non-finite values (NA or inf) to integer
```
- **Cause**: Invalid data values during casting operations
- **Resolution**: System saves error metadata and continues with other dates
- **Action**: Examine source data for the failing date

## OpenTelemetry Tracing

The rollup system includes comprehensive OpenTelemetry tracing for performance monitoring and debugging:

**Key Trace Operations**:
- `rollup.computation` - Overall rollup command execution with smart dependency planning
- `rollup.task.execution` - Individual rollup task processing per date with batch optimization
- `rollup.dataframe.processing` - Unified data loading and dataframe creation
- `rollup.parquet.save` - Parquet file writing with versioning and metadata
- `report.build` - Report generation using pre-computed rollups
- `report.rollup.loading` - Loading and merging rollup dataframes with schema consistency

**Performance Metrics**:
- I/O reduction tracking (5x improvement with unified loading)
- Memory usage optimization with shared batch iterators
- Processing time per date and dataframe
- Mixed type comparison frequency and impact

**Enable Tracing**:
```bash
export OTEL_TRACES_ENABLED=true
export OTEL_SERVICE_NAME=metrics-utility
# Use with observability stack from MONITORING.md
docker compose -f tools/docker/docker-compose.yaml --profile=otel up -d
```

## Schema-Driven Architecture

The rollup system is built on a comprehensive schema-driven architecture that ensures data consistency and quality across all processing stages.

### Base Class Schema System

The `Base` class (`metrics_utility/automation_controller_billing/dataframe_engine/base.py`) provides centralized schema operations that all dataframe engines inherit:

#### Core Schema Methods

**Schema Application**:
```python
apply_complete_schema(df, schema_type="dataframe", operation_context="after_grouping")
```
- Applies proper types, adds missing columns with defaults, enforces consistent ordering
- Supports three schema types: `collector_dataframe`, `dataframe`, `parquet`
- Provides detailed debug logging for troubleshooting schema issues

**Data Validation**:
```python
validate_collector_dataframe(df) -> pd.DataFrame
```
- Filters invalid records based on validation rules from `collector_dataframe_validation_schema()`
- Tracks data quality metrics (rows filtered, quality ratio, validation failures)
- Logs significant data quality issues automatically

#### Schema Pipeline Flow

1. **CSV Processing** → `collector_dataframe_schema()` applied
2. **After Grouping** → `dataframe_schema()` applied  
3. **Before Storage** → `parquet_schema()` applied
4. **After Loading** → `dataframe_schema()` restored
5. **During Merging** → Schema consistency enforced

### Wrapper Class Responsibilities

Each dataframe wrapper (DataframeJobhostSummaryUsage, DataframeContentUsage, etc.) defines:

#### Required Static Methods

```python
@staticmethod
def collector_dataframe_schema() -> Dict[str, str]:
    """Schema for processed CSV data (before grouping)"""
    return {
        'host_name': 'String',
        'task_runs': 'Int64',
        'created': 'Datetime',
        # ...
    }

@staticmethod  
def dataframe_schema() -> Dict[str, str]:
    """Schema for working dataframes (after grouping)"""
    return {
        'host_name': 'String', 
        'first_automation': 'Datetime',
        'last_automation': 'Datetime',
        # ...
    }

@staticmethod
def parquet_schema() -> pa.Schema:
    """PyArrow schema for parquet storage"""
    return pa.schema([
        pa.field("host_name", pa.string()),
        pa.field("first_automation", pa.timestamp('us')),
        # ...
    ])

@staticmethod
def collector_dataframe_validation_schema() -> Dict[str, Dict[str, Any]]:
    """Validation rules for data quality control"""
    return {
        'host_name': {'required': True, 'allow_null': False},
        'task_runs': {'required': True, 'min_value': 0},
        # ...
    }
```

#### Business Logic Methods

```python
def _process_batch_data(self, batch_data, current_span):
    """Process individual batch with domain-specific logic"""
    # 1. Validate CSV data
    # 2. Apply business transformations  
    # 3. Call apply_complete_schema()
    
def group(self, dataframe):
    """Group and aggregate data using Polars operations"""
    
def regroup(self, dataframe): 
    """Regroup pre-aggregated data during rollup merging"""
```

### Clear Separation of Concerns

- **Base Class**: Handles all technical schema operations, validation, and data infrastructure
- **Wrapper Classes**: Define only business logic and schema definitions specific to their data domain
- **No Duplication**: Schema operations are never implemented inline in wrapper classes
- **Consistent Interface**: All wrappers follow the same schema application pipeline

### Data Quality Benefits

This architecture provides:
- **Automatic Type Safety**: All data conforms to defined schemas throughout processing
- **Data Quality Filtering**: Invalid records are automatically filtered with quality metrics
- **Consistent Column Ordering**: Prevents concat/merge errors between dataframes
- **Complex Type Support**: Proper handling of JSON strings, collections, and facts
- **Performance Optimization**: Schema operations are cached and optimized

## Future Enhancements

- **Distributed Processing**: Process date groups in parallel across multiple workers
- **Hierarchical Rollups**: Create weekly/monthly rollups from daily rollups for faster long-term reporting
- **Advanced Caching**: Intelligent caching strategies for frequently accessed rollup combinations
- **Data Quality Monitoring**: Enhanced tracking and alerting for mixed type comparisons and data quality issues
- **Schema Evolution**: Support for schema versioning and backward compatibility