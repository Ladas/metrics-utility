# Rollups Data Format Flow and Schema Operations

This document provides a comprehensive diagram of the data format flow through all operations in the rollups system, showing exactly where schema operations occur and what data types are expected at each stage.

## Comprehensive Data Format Flow Diagram

```mermaid
graph TB
    %% Raw Data Input
    A[Raw CSV Data from Tarballs] --> A1{CSV Column Types}
    A1 --> A2[Strings, Mixed Types, Inconsistent Formats]
    
    %% Stage 1: CSV Processing with Collector Schema
    A2 --> B[_process_batch_data - CSV Processing]
    B --> B1[apply_complete_schema - collector_dataframe]
    B1 --> B2[Schema: created as String, task_runs as Int64]
    B2 --> B3[Complete: columns added, types cast, ordering applied]
    B3 --> B4[Result: Data ready for grouping - BEFORE group()]
    
    %% Stage 2: Grouping with Initial Aggregations  
    B4 --> C[group - Initial Aggregation Operations]
    C --> C1[Apply initial_aggregations rules + complex type processing]
    C1 --> C2[created.min→first_automation, created.max→last_automation]
    C1 --> C2a[Facts: JSON→list format merge, Collections: unique arrays]
    C2 --> C3[apply_complete_schema - dataframe - AFTER group()]
    C2a --> C3
    C3 --> C4[Schema: first_automation as Datetime, complex types as JSON strings]
    C4 --> C5[Complete: columns added, types cast, ordering applied]
    C5 --> C6[Result: Grouped data with proper working types + complex data]
    
    %% Stage 3: Merging Multiple CSV Groups 
    C6 --> D[merge - Combine Multiple CSV Groups]
    D --> D1[_align_schemas_for_concat with dataframe_schema]
    D1 --> D2[Both DataFrames converted to consistent working types]
    D2 --> D3[polars.concat - Vertical concatenation]
    D3 --> D4[regroup_with_schema - Re-aggregate with complex type merging]
    D4 --> D4a[Facts: merge list format JSONs, Collections: merge arrays]
    D4 --> D5[apply_complete_schema - dataframe after merge]
    D4a --> D5
    D5 --> D6[Complete: consistent working types maintained]
    D6 --> D7[Result: Merged data with consistent Polars types + merged complex data]
    
    %% Stage 4: Final Processing Before Storage
    D7 --> E[Final Processing Before Storage]
    E --> E1[apply_complete_schema - dataframe final validation]
    E1 --> E2[Complete: all columns, types, ordering validated]
    E2 --> E3[Convert to parquet_schema types for storage]
    E3 --> E4[Result: Storage-ready data with parquet schema types]
    
    %% Stage 5: Parquet Storage
    E4 --> F[Save to Parquet File]
    F --> F1[parquet_schema types optimized for storage]
    F1 --> F2[Datetime→timestamp[us], String→large_string, etc.]
    F2 --> F3[Stored: Efficient parquet with proper types]
    
    %% Stage 6: Loading from Parquet
    F3 --> G[Load from Parquet - load_from_parquet]
    G --> G1[PyArrow/Polars read_parquet with Object type handling]
    G1 --> G2[Object types converted to JSON strings for compatibility]
    G2 --> G3[apply_complete_schema - dataframe after load]
    G3 --> G4[Complete: parquet→working type conversion]
    G4 --> G5[Result: Working data with consistent dataframe_schema types]
    
    %% Stage 7: Merging Multiple Rollup Files
    G5 --> H[Merge Multiple Rollup Files in Reader]
    H --> H1[All files have consistent dataframe_schema types]
    H1 --> H2[polars.concat - Combine multiple daily rollups]
    H2 --> H3[regroup - Final aggregation across dates]
    H3 --> H4[apply_complete_schema - dataframe final merge]
    H4 --> H5[Complete: final working types for reporting]
    H5 --> H6[Result: Final aggregated data for reporting]
    
    %% Centralized Schema Operations
    subgraph "Centralized apply_complete_schema Method"
        CS[apply_complete_schema - Base Class Method]
        CS --> CS1[Step 1: Get schema dict and default values]
        CS1 --> CS2[Step 2: _ensure_schema_columns - add missing with defaults]
        CS2 --> CS3[Step 3: _apply_schema_casting - convert types]
        CS3 --> CS4[Step 4: _ensure_consistent_column_ordering]
        CS4 --> CS5[Result: Complete schema applied consistently]
    end
    
    %% Schema Type Definitions
    subgraph "Schema Type Definitions"
        S1[collector_dataframe_schema]
        S1 --> S1A[CSV Processing Types - Before Grouping]
        S1A --> S1B[created: String, job_created: String]
        S1A --> S1C[task_runs: Int64, canonical_facts: String]
        
        S2[dataframe_schema - WORKING TYPES]
        S2 --> S2A[In-Memory Processing Types - After Grouping]
        S2A --> S2B[first_automation: Datetime, last_automation: Datetime]
        S2A --> S2C[task_runs: Int64, canonical_facts: String]
        
        S3[parquet_schema - STORAGE TYPES]
        S3 --> S3A[Parquet Storage Types - Optimized for Disk]
        S3A --> S3B[first_automation: timestamp[us], last_automation: timestamp[us]]
        S3A --> S3C[task_runs: int64, canonical_facts: large_string]
    end
    
    %% Critical Points - All using centralized method
    classDef critical fill:#ff9999,stroke:#333,stroke-width:4px
    classDef schema fill:#99ccff,stroke:#333,stroke-width:2px
    classDef working fill:#99ff99,stroke:#333,stroke-width:2px
    classDef storage fill:#ffcc99,stroke:#333,stroke-width:2px
    classDef centralized fill:#ff99ff,stroke:#333,stroke-width:3px
    
    class B1,C3,D5,E1,G3,H4 critical
    class S1,S2,S3 schema
    class C6,D7,G5,H6 working
    class F2,F3,G2 storage
    class CS,CS1,CS2,CS3,CS4,CS5 centralized
```

## Schema Operation Points

### Critical Schema Application Points

1. **CSV Processing - BEFORE group() (Stage 1)**
   - **Input**: Raw CSV strings, mixed types
   - **Operation**: `collector_dataframe_schema()` applied
   - **Output**: Consistent types ready for aggregation (strings for timestamps, Int64 for counters)
   - **Purpose**: Prepare data for group() operations with proper types

2. **After Grouping - AFTER group() (Stage 2)**
   - **Input**: Grouped/aggregated data (first_automation, last_automation created)
   - **Operation**: `dataframe_schema()` applied  
   - **Output**: Working types (Datetime for timestamps, proper aggregated columns)
   - **Purpose**: Convert aggregated data to working types for merging and processing
   - **Complex Types**: Facts converted to list format JSON, collections to unique arrays

3. **During Multiple CSV Merging (Stage 3)**
   - **Input**: Multiple DataFrames from different CSV files to merge
   - **Operation**: `dataframe_schema()` alignment + `regroup_with_schema()`
   - **Output**: Type-consistent merged data with properly merged complex types
   - **Purpose**: Combine data from multiple CSV files while preserving complex type integrity
   - **Complex Types**: Facts list formats merged, collections arrays merged with unique values

4. **Before Storage (Stage 4)**
   - **Input**: Final processed data after CSV merging
   - **Operation**: Convert to `parquet_schema()` types
   - **Output**: Storage-optimized types
   - **Purpose**: Optimize for parquet storage efficiency

5. **After Loading (Stage 6)**
   - **Input**: Data loaded from parquet
   - **Operation**: Convert to `dataframe_schema()` types
   - **Output**: Working types for processing
   - **Purpose**: Restore working types from storage format

6. **During Rollup Merging (Stage 7)**
   - **Input**: Multiple rollup files from different dates
   - **Operation**: `dataframe_schema()` consistency + complex type merging
   - **Output**: Final aggregated working data
   - **Purpose**: Merge daily rollups across date ranges for reporting
   - **Complex Types**: Cross-date fact merging and collection aggregation

## Type Flow Summary

### Part 1: Data Processing and Storage Flow

```
Raw CSV → collector_dataframe_schema → group() → dataframe_schema → merge() → dataframe_schema → parquet_schema → PARQUET STORAGE
                     ↓                     ↓           ↓              ↓            ↓                ↓                    ↓
              (String timestamps)    Aggregation  (Datetime objects)   Merging   Working Types    Storage Types      [Files on Disk]
                     ↓                     ↓           ↓              ↓            ↓                ↓                    ↓
                Processing Ready      Min/Max Ops   Working Types    Combine CSVs  Consistent Types  Optimized Types    Persistent Data
                     ↓                     ↓           ↓              ↓            ↓                ↓                    ↓
               BEFORE group()        Grouping      AFTER group()    Multiple CSVs  Working Schema   Parquet Schema     Daily Rollups
```

### Part 2: Rollups Loading and Report Generation Flow

```
PARQUET STORAGE → Reader.load_from_parquet → dataframe_schema → Merging → Final Reports
        ↓                        ↓                    ↓             ↓            ↓
  [Files on Disk]      Object type handling    Working Types   Cross-Date    XLSX Output
        ↓                        ↓                    ↓             ↓            ↓
  Daily Rollups        PyArrow conversion     Datetime objects   Aggregation   CCSPv2 Report
        ↓                        ↓                    ↓             ↓            ↓
  Multiple Dates    Enhanced compatibility   Consistent Types   Combined Data  Final Report
```

### Complete End-to-End Flow

```
CSV Input → Processing → Grouping → Merging → Storage → Loading → Rollup Merging → Reporting
    ↓           ↓           ↓          ↓         ↓         ↓            ↓               ↓
collector → dataframe → dataframe → parquet → dataframe → dataframe →     Output
 schema      schema      schema     schema     schema     schema      
    ↓           ↓           ↓          ↓         ↓         ↓            ↓               ↓
String      Datetime   Datetime   timestamp   Datetime   Datetime    Excel
types       objects    objects     [us]       objects    objects     Report
    ↓           ↓           ↓          ↓         ↓         ↓            ↓               ↓
Raw CSV    Working    Working     Storage    Working    Working     Final
Data       Types      Types       Types      Types      Types      Report
    ↓           ↓           ↓          ↓         ↓         ↓            ↓               ↓
 Initial    After       After      Before     After     Cross-Date   Final
 Load      group()     merge()    Storage    Load      Aggregate   Report
```

## Centralized Schema-Driven Architecture

The metrics utility now implements a comprehensive schema-driven architecture that centralizes all data validation, type casting, and column management in the base class, ensuring consistent processing across all dataframe engines.

### Base Class Responsibilities (metrics_utility/automation_controller_billing/dataframe_engine/base.py)

The `Base` class provides centralized schema operations that handle:

1. **Schema Application**: `apply_complete_schema()` method applies proper types, adds missing columns, and enforces consistent ordering
2. **Data Validation**: `validate_collector_dataframe()` filters invalid records based on validation rules  
3. **Type Casting**: Automatic conversion between different schema types (collector_dataframe → dataframe → parquet)
4. **Column Management**: Ensures all required columns are present with appropriate defaults
5. **Complex Type Handling**: Manages JSON strings, collections, and facts using standardized merge operations

### Wrapper Class Responsibilities

Dataframe wrapper classes (DataframeJobhostSummaryUsage, DataframeContentUsage, etc.) focus purely on:

1. **Schema Definitions**: Define schemas at the top of the class using static methods
2. **Business Logic**: Implement domain-specific transformations and processing rules
3. **Validation Rules**: Specify data quality requirements via `collector_dataframe_validation_schema()`
4. **Aggregation Logic**: Define how to group and merge data using `group()` and `regroup()` methods

### Clear Separation of Concerns

- **Base Class**: Handles all technical schema operations, validation, and data infrastructure
- **Wrapper Classes**: Handle only business logic and schema definitions specific to their data domain
- **No Duplication**: Schema operations are never implemented inline in wrapper classes
- **Consistent Interface**: All wrappers use the same standardized schema application flow

This architecture ensures:
- **Maintainability**: Schema logic is centralized and not duplicated across wrappers
- **Consistency**: All data processing follows the same schema validation pipeline  
- **Reliability**: Data quality is enforced automatically without manual intervention
- **Performance**: Schema operations are optimized and cached where possible