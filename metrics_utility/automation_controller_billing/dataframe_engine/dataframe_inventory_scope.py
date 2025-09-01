import time
from typing import Any, Dict

import polars as pd
import pyarrow as pa

from opentelemetry import trace

from metrics_utility.automation_controller_billing.dataframe_engine.base import (
    Base,
    merge_and_stringify_facts,
    merge_list_arrays,
    convert_list_pairs_to_dict,
    merge_json_lists_to_dict,
    merge_native_dicts,
    merge_native_lists,
)
from metrics_utility.automation_controller_billing.helpers import parse_json
from metrics_utility.tracing import add_span_attributes, traced_method


def compute_serial(row):
    # Handle both formats: list of key-value pairs (collector) and JSON string (rollup)
    facts_data = row['canonical_facts']

    # Debug: Check what we're receiving
    if 'web01' in str(row.get('host_name', '')) or 'web01' in str(facts_data):
        print(f'DEBUG COMPUTE_SERIAL: host={row.get("host_name", "unknown")}, facts_data type={type(facts_data)}, value={facts_data}')

    if isinstance(facts_data, list):
        # Handle list of key-value pairs format from collector_dataframe_schema
        facts_dict = convert_list_pairs_to_dict(facts_data)
    elif isinstance(facts_data, str):
        # Handle JSON string format from rollup data
        try:
            import json

            facts_dict = json.loads(facts_data)
        except (json.JSONDecodeError, ValueError):
            if 'web01' in str(row.get('host_name', '')) or 'web01' in str(facts_data):
                print(f'DEBUG COMPUTE_SERIAL: Failed to parse JSON: {facts_data}')
            return None
    else:
        if 'web01' in str(row.get('host_name', '')) or 'web01' in str(facts_data):
            print(f'DEBUG COMPUTE_SERIAL: Unexpected type: {type(facts_data)}')
        return None

    # Handle consistent array format - all values are now arrays
    ansible_product_serial = facts_dict.get('ansible_product_serial', [])
    ansible_machine_id = facts_dict.get('ansible_machine_id', [])

    if 'web01' in str(row.get('host_name', '')) or 'web01' in str(facts_data):
        print(f'DEBUG COMPUTE_SERIAL: ansible_product_serial={ansible_product_serial}, ansible_machine_id={ansible_machine_id}')

    # Extract first value from arrays (or empty string if missing)
    # Handle both list and Series objects
    if ansible_product_serial is not None and len(ansible_product_serial) > 0:
        # If it's a list of strings, take the first element; if it's a string, use it directly
        if isinstance(ansible_product_serial, list):
            serial = ansible_product_serial[0] if ansible_product_serial[0] is not None else ''
        else:
            serial = str(ansible_product_serial)
    else:
        serial = ''
        
    if ansible_machine_id is not None and len(ansible_machine_id) > 0:
        # If it's a list of strings, take the first element; if it's a string, use it directly
        if isinstance(ansible_machine_id, list):
            machine_id = ansible_machine_id[0] if ansible_machine_id[0] is not None else ''
        else:
            machine_id = str(ansible_machine_id)
    else:
        machine_id = ''

    if 'web01' in str(row.get('host_name', '')) or 'web01' in str(facts_data):
        print(f"DEBUG COMPUTE_SERIAL: extracted serial='{serial}', machine_id='{machine_id}'")

    if not serial or not machine_id:
        if 'web01' in str(row.get('host_name', '')) or 'web01' in str(facts_data):
            print(f"DEBUG COMPUTE_SERIAL: Returning None because serial='{serial}' or machine_id='{machine_id}' is empty")
        return None

    result = serial + '/' + machine_id
    if 'web01' in str(row.get('host_name', '')) or 'web01' in str(facts_data):
        print(f"DEBUG COMPUTE_SERIAL: Returning result='{result}'")
    return result


# dataframe for main_host
class DataframeInventoryScope(Base):
    """
    DataframeInventoryScope processes main_host CSV data for inventory scope tracking.

    CLEAN SEPARATION OF CONCERNS:
    - collector_schema(): Raw CSV input (JSON strings)
    - collector_dataframe_schema() & dataframe_schema(): Native Polars types for processing
    - parquet_schema(): Storage format (JSON strings for compatibility)

    All internal processing uses native Polars types (List, Struct, etc.)
    JSON serialization only happens at storage boundaries.
    """

    # ========================================
    # SCHEMA DEFINITIONS (CLEAN SEPARATION)
    # ========================================

    @staticmethod
    def collector_schema() -> pa.Schema:
        """Define PyArrow schema for raw CSV collector data validation.

        This schema defines the raw CSV input format with JSON strings.
        Conversion to native types happens in collector_dataframe_schema.

        Returns:
            PyArrow schema for raw CSV data validation
        """
        return pa.schema(
            [
                # Index columns (unique identifiers)
                pa.field('host_name', pa.string()),
                pa.field('install_uuid', pa.string()),
                # Raw CSV data columns (before aggregation)
                pa.field('last_automation', pa.string()),  # Keep as string for Polars compatibility
                pa.field('organization_name', pa.string()),
                pa.field('inventory_name', pa.string()),
                pa.field('ansible_host_variable', pa.string()),  # Host variable mapping for deduplication
                # Complex data columns (stored as JSON strings)
                pa.field('canonical_facts', pa.string()),  # JSON string for dynamic dictionary content
                pa.field('facts', pa.string()),  # JSON string for dynamic dictionary content
                pa.field('host_names_before_dedup', pa.string()),  # Store as string for dedup tracking
                # Calculated columns
                pa.field('organizations', pa.string()),  # JSON array string
                pa.field('inventories', pa.string()),  # JSON array string
                pa.field('serials', pa.string()),  # JSON array string
            ]
        )

    @staticmethod
    def collector_dataframe_schema() -> Dict[str, str]:
        """Define Polars dataframe schema for processed CSV data (before grouping).

        Uses native Polars types for efficient processing. JSON strings from CSV
        are converted to native types at the input boundary.

        Returns:
            Dictionary mapping column names to Polars dtypes as strings
        """
        return {
            # Index columns
            'host_name': 'String',
            'install_uuid': 'String',
            # Data columns
            'last_automation': 'Datetime',  # Proper datetime type
            'organization_name': 'String',
            'inventory_name': 'String',
            'ansible_host_variable': 'String',
            'canonical_facts': 'String',  # JSON string for consistent processing
            'facts': 'String',  # JSON string for consistent processing
            'original_host_name': 'String',
            'serial': 'String',
            'host_names_before_dedup': 'String',  # String initially, converted to List during aggregation
        }

    @staticmethod
    def dataframe_schema() -> Dict[str, str]:
        """Define Polars dataframe schema for working dataframes (after grouping).

        Uses native Polars types throughout processing pipeline.
        JSON serialization only happens at storage boundaries (parquet_schema).

        Returns:
            Dictionary mapping column names to Polars dtypes as strings
        """
        return {
            # Index columns
            'host_name': 'String',
            'install_uuid': 'String',
            # Aggregated data columns
            'last_automation': 'Datetime',  # Max automation time as proper datetime
            # Complex aggregated data - String types to match parquet schema
            'canonical_facts': 'String',  # JSON object string for facts aggregation
            'facts': 'String',  # JSON object string for facts aggregation
            # Collections as native List types for efficient aggregation
            'organizations': 'List',  # Native List[String] for organizations
            'inventories': 'List',  # Native List[String] for inventories
            'serials': 'List',  # Native List[String] for serials
            'host_names_before_dedup': 'List',  # Native List[String] for host names
        }

    @staticmethod
    def parquet_schema() -> pa.Schema:
        """Define PyArrow schema for aggregated rollup data validation.

        This schema is used for validating aggregated data during rollup merging operations.
        It includes only the final aggregated columns after group_by operations.

        Returns:
            PyArrow schema for rollup data validation
        """
        return pa.schema(
            [
                # Index columns (unique identifiers)
                pa.field('host_name', pa.string()),
                pa.field('install_uuid', pa.string()),
                # Aggregated data columns
                pa.field('last_automation', pa.timestamp('us')),  # Proper timestamp for datetime data
                # Complex aggregated data (JSON strings for merging compatibility)
                pa.field('canonical_facts', pa.string()),  # JSON object string
                pa.field('facts', pa.string()),  # JSON object string
                pa.field('organizations', pa.string()),  # JSON array string
                pa.field('inventories', pa.string()),  # JSON array string
                pa.field('serials', pa.string()),  # JSON array string
                pa.field('host_names_before_dedup', pa.string()),  # JSON array string
            ]
        )

    def get_collector_default_values(self) -> Dict[str, Any]:
        """Get custom default values for collector schema columns.

        These defaults override the standard type-based defaults for domain-specific
        requirements for inventory scope processing.

        Returns:
            Dictionary mapping column names to custom default values
        """
        return {
            'host_name': '',
            'last_automation': None,  # Null datetime
            'organization_name': 'No organization name',
            'inventory_name': '',
            'ansible_host_variable': '',
            'canonical_facts': '',  # Empty string for facts aggregation
            'facts': '',  # Empty string for facts aggregation
            'install_uuid': '',
            'original_host_name': '',
            # After Stage 2 conversion, these are all native Lists
            'organizations': [],  # Empty list for native List
            'inventories': [],  # Empty list for native List
            'serials': [],  # Empty list for native List
            'host_names_before_dedup': [],  # Empty list for native List
        }

    def get_rollup_default_values(self) -> Dict[str, Any]:
        """Get custom default values for rollup schema columns.

        Uses native types to match dataframe_schema().

        Returns:
            Dictionary mapping column names to custom default values
        """
        return {
            'host_name': '',
            'install_uuid': '',
            'last_automation': None,  # Null datetime for aggregated data
            'canonical_facts': '',  # Empty string for facts aggregation
            'facts': '',  # Empty string for facts aggregation
            'organizations': [],  # Empty list for native List
            'inventories': [],  # Empty list for native List
            'serials': [],  # Empty list for native List
            'host_names_before_dedup': [],  # Empty list for native List
        }

    @staticmethod
    def collector_dataframe_validation_schema() -> Dict[str, Dict[str, Any]]:
        """Define validation rules for collector dataframe columns.

        This schema specifies which columns are required, which can be null,
        and validation rules for inventory scope data processing.

        Returns:
            Dictionary mapping column names to validation rule dictionaries
        """
        return {
            # Required identification columns that cannot be null or empty
            'host_name': {'required': True, 'allow_null': False, 'allow_empty': False},
            'install_uuid': {'required': True, 'allow_null': False},
            # Optional columns that can be null
            'organization_name': {'required': False, 'allow_null': True},
            'inventory_name': {'required': False, 'allow_null': True},
            'ansible_host_variable': {'required': False, 'allow_null': True},
            'original_host_name': {'required': False, 'allow_null': True},
            # JSON columns can be null/empty
            'canonical_facts': {'required': False, 'allow_null': True},
            'facts': {'required': False, 'allow_null': True},
            'serial': {'required': False, 'allow_null': True},
            'host_names_before_dedup': {'required': False, 'allow_null': True},
        }

    # ========================================
    # STATIC CONFIGURATION METHODS
    # ========================================

    @staticmethod
    def unique_index_columns():
        """Define columns that uniquely identify a record for grouping/deduplication."""
        return ['host_name', 'install_uuid']

    @staticmethod
    def data_columns():
        """Define data columns that need aggregation when grouping records."""
        return ['last_automation', 'canonical_facts', 'facts', 'organizations', 'inventories', 'serials', 'host_names_before_dedup']

    @staticmethod
    def group_aggregations():
        """Define how to aggregate raw CSV data when grouping by unique_index_columns during initial processing.

        This is used in the group() method when processing CSV data from a single file/batch.
        For duplicate records with the same unique index, these aggregations combine the data.

        Returns:
            dict: Mapping of column_name -> aggregation_name for centralized aggregation system
        """
        return {
            # Data columns aggregation rules for initial CSV processing
            'last_automation': 'max_non_null',  # Latest automation timestamp (null-aware)
            'canonical_facts': 'merge_json_facts',  # Take first list of facts for single batch processing
            'facts': 'merge_json_facts',  # Take first list of facts for single batch processing
            'organizations': 'unique',  # Collect unique organization names
            'inventories': 'unique',  # Collect unique inventory names
            'serials': 'unique',  # Collect unique serial numbers
            'host_names_before_dedup': 'unique',  # Collect unique host names during aggregation
        }

    @staticmethod
    def regroup_aggregations():
        """Define how to aggregate rollup data when combining multiple rollup files during regroup operations.

        This is used in the regroup() method when merging pre-aggregated data from different batches/files.
        
        Returns:
            dict: Mapping of column_name -> aggregation_name for centralized aggregation system
        """
        return {
            'last_automation': 'max_non_null',  # Latest automation timestamp across rollups (null-aware)
            'canonical_facts': 'merge_json_facts',  # Merge JSON objects from different rollups
            'facts': 'merge_json_facts',  # Merge JSON objects from different rollups
            'organizations': 'flatten_unique',  # Flatten and get unique from List columns
            'inventories': 'flatten_unique',  # Flatten and get unique from List columns
            'serials': 'flatten_unique',  # Flatten and get unique from List columns
            'host_names_before_dedup': 'flatten_unique',  # Flatten and get unique from List columns
        }

    @staticmethod
    def operations():
        """Define how to merge rollup data when combining multiple rollup files.

        NOTE: This method is no longer used since we switched to concat+regroup approach.
        Previously used by summarize_merged_dataframes() for resolving join conflicts.
        Now the regroup() method handles all aggregation logic directly.

        TODO: Remove this method after confirming concat+regroup works correctly.
        """
        # No longer used with concat+regroup approach
        return {}

    # ========================================
    # BATCH PROCESSING METHODS
    # ========================================

    def _process_batch_data_with_schema(self, batch_data, current_span):
        """Process batch data and apply collector_dataframe_schema (BEFORE grouping).

        Override base class method to avoid double schema application since
        _process_batch_inventory already applies the schema.
        """
        # Get main_host data from this batch
        billing_data = batch_data.get('main_host')
        if billing_data is None or len(billing_data) == 0:
            return self.empty()

        # Process this batch using existing logic (includes schema application)
        date = batch_data.get('_date_context')  # Get date from context
        processed_data = self._process_batch_inventory(billing_data, batch_data, current_span, date)
        return processed_data

    def _process_batch_data(self, batch_data, current_span):
        """Process inventory scope batch data.

        Args:
            batch_data: Dictionary containing batch data with 'main_host' key

        Returns:
            Processed Polars DataFrame ready for grouping, or None if no valid data
        """
        # Get main_host data from this batch
        billing_data = batch_data.get('main_host')
        if billing_data is None or len(billing_data) == 0:
            return None

        # Process this batch using existing logic (need to provide all required parameters)
        date = batch_data.get('_date_context')  # Get date from context
        processed_data = self._process_batch_inventory(billing_data, batch_data, current_span, date)
        return processed_data

    def _process_batch_inventory(self, billing_data, batch_data, current_span, date):
        """Process individual batch inventory data using centralized schema-driven approach.

        This method uses the new validation system:
        1. CSV validation using collector_schema
        2. Business logic transformations (host mapping, serial computation, etc.)
        3. Schema application with automatic validation via collector_dataframe_validation_schema

        All validation, type casting, and column completion is handled by the
        centralized schema system in the base class.
        """
        from metrics_utility.tracing import add_span_attributes

        print(f'===== PIPELINE START: _process_batch_inventory called with {len(billing_data) if billing_data is not None else 0} records =====')

        # Debug: Check raw CSV data schema and content
        if billing_data is not None and len(billing_data) > 0:
            print(f'===== RAW CSV DATA SCHEMA: {billing_data.schema} =====')
            print(f'===== RAW CSV COLUMNS: {billing_data.columns} =====')
            for i in range(min(2, len(billing_data))):
                row = billing_data.row(i, named=True)
                print(f'===== RAW CSV Record {i}: host={row.get("host_name")} =====')
                print(f'===== RAW CSV Record {i}: canonical_facts={row.get("canonical_facts")} (type: {type(row.get("canonical_facts"))}) =====')
                print(f'===== RAW CSV Record {i}: facts={row.get("facts")} (type: {type(row.get("facts"))}) =====')

        # Handle empty DataFrame case
        if billing_data is None or len(billing_data) == 0:
            return self.empty()

        input_row_count = len(billing_data)

        # Step 1: Add core metadata columns first (required for validation)
        billing_data = billing_data.with_columns(pd.lit(batch_data['config']['install_uuid']).alias('install_uuid'))

        # Step 2: Validate CSV data against collector schema (after adding required columns)
        print(f'===== BEFORE COLLECTOR VALIDATION =====')
        if len(billing_data) > 0:
            row = billing_data.row(0, named=True)
            print(f'===== PRE-VALIDATION: canonical_facts={row.get("canonical_facts")} =====')
            print(f'===== PRE-VALIDATION: facts={row.get("facts")} =====')

        billing_data = self.validate_collector_data(billing_data, strict_columns=['host_name'], default_values=self.get_collector_default_values())

        print(f'===== AFTER COLLECTOR VALIDATION =====')
        print(f'===== POST-VALIDATION SCHEMA: {billing_data.schema} =====')
        if len(billing_data) > 0:
            row = billing_data.row(0, named=True)
            print(f'===== POST-VALIDATION: canonical_facts={row.get("canonical_facts")} (type: {type(row.get("canonical_facts"))}) =====')
            print(f'===== POST-VALIDATION: facts={row.get("facts")} (type: {type(row.get("facts"))}) =====')

        # Step 3: Apply business logic transformations on raw CSV data
        print(f'===== BEFORE BUSINESS TRANSFORMATIONS =====')
        billing_data = self._apply_inventory_transformations(billing_data, current_span, date)

        print(f'===== AFTER BUSINESS TRANSFORMATIONS =====')
        print(f'===== POST-TRANSFORM SCHEMA: {billing_data.schema} =====')
        if len(billing_data) > 0:
            row = billing_data.row(0, named=True)
            print(f'===== POST-TRANSFORM: canonical_facts={row.get("canonical_facts")} (type: {type(row.get("canonical_facts"))}) =====')
            print(f'===== POST-TRANSFORM: facts={row.get("facts")} (type: {type(row.get("facts"))}) =====')

        # Step 4: Apply collector_dataframe schema to convert JSON strings to native types
        print(f'===== BEFORE COLLECTOR_DATAFRAME SCHEMA =====')
        billing_data = self.apply_complete_schema(
            billing_data, schema_type='collector_dataframe', operation_context='after_inventory_transformations'
        )

        print(f'===== AFTER COLLECTOR_DATAFRAME SCHEMA =====')
        print(f'===== COLLECTOR_DATAFRAME SCHEMA: {billing_data.schema} =====')
        if len(billing_data) > 0:
            row = billing_data.row(0, named=True)
            print(f'===== COLLECTOR_DATAFRAME: canonical_facts={row.get("canonical_facts")} (type: {type(row.get("canonical_facts"))}) =====')
            print(f'===== COLLECTOR_DATAFRAME: facts={row.get("facts")} (type: {type(row.get("facts"))}) =====')

        # Add validation metrics for tracing
        final_count = len(billing_data) if billing_data is not None else 0
        add_span_attributes(
            current_span,
            **{
                f'data_quality.{date.isoformat()}.input_rows': input_row_count,
                f'data_quality.{date.isoformat()}.valid_rows': final_count,
            },
        )

        return billing_data

    def _apply_inventory_transformations(self, billing_data, current_span, date):
        """Apply business logic transformations specific to inventory scope processing."""
        import time
        from metrics_utility.tracing import add_span_attributes

        # Add original host name tracking
        billing_data = billing_data.with_columns(billing_data['host_name'].alias('original_host_name'))

        # Apply ansible_host_variable mapping if present
        if 'ansible_host_variable' in billing_data.columns:
            # Replace missing or empty ansible_host_variable with host name and use it as host_name
            billing_data = billing_data.with_columns(
                [
                    pd.when((billing_data['ansible_host_variable'].is_null()) | (billing_data['ansible_host_variable'] == ''))
                    .then(billing_data['host_name'])
                    .otherwise(billing_data['ansible_host_variable'])
                    .alias('ansible_host_variable')
                ]
            )
            billing_data = billing_data.with_columns([billing_data['ansible_host_variable'].alias('host_name')])

        # NOTE: JSON string to native type conversion is handled by centralized schema system
        # No manual conversions should be done here - this violates separation of concerns

        # Create aggregation columns from raw CSV data
        # These will be aggregated during the group() phase
        aggregation_columns = []
        if 'organization_name' in billing_data.columns:
            aggregation_columns.append(billing_data['organization_name'].alias('organizations'))
        else:
            aggregation_columns.append(pd.lit([]).alias('organizations'))

        if 'inventory_name' in billing_data.columns:
            aggregation_columns.append(billing_data['inventory_name'].alias('inventories'))
        else:
            aggregation_columns.append(pd.lit([]).alias('inventories'))

        if aggregation_columns:
            billing_data = billing_data.with_columns(aggregation_columns)

        # Serial computation - often computationally expensive
        serial_start = time.time()
        billing_data = billing_data.with_columns(
            [
                billing_data['canonical_facts']
                .map_elements(lambda x: compute_serial({'canonical_facts': x}) if x else None, return_dtype=pd.Utf8)
                .alias('serial'),
                billing_data['host_name'].alias('host_names_before_dedup'),
            ]
        )

        # Add serials column after serial computation
        billing_data = billing_data.with_columns([billing_data['serial'].alias('serials')])
        serial_duration = time.time() - serial_start

        # Log slow serial computations
        if serial_duration > 0.1:
            add_span_attributes(
                current_span,
                **{
                    f'dataframe.build.{date.isoformat()}.serial_computation_duration': serial_duration,
                    f'dataframe.build.{date.isoformat()}.serial_records_processed': len(billing_data),
                },
            )

        return billing_data

    # ========================================
    # AGGREGATION METHODS (NATIVE TYPES)
    # ========================================

    @traced_method('inventory_scope.group')
    def group(self, dataframe):
        """Group and aggregate inventory scope dataframe with performance tracking."""
        print('===== GROUP() OPERATION START =====')
        print(f'===== GROUP INPUT SCHEMA: {dataframe.schema if dataframe is not None else "None"} =====')
        print(f'===== GROUP INPUT RECORD COUNT: {len(dataframe) if dataframe is not None else 0} =====')

        if dataframe is not None and len(dataframe) > 0:
            row = dataframe.row(0, named=True)
            print(f'===== GROUP INPUT SAMPLE: host={row.get("host_name")} =====')
            print(f'===== GROUP INPUT: canonical_facts={row.get("canonical_facts")} (type: {type(row.get("canonical_facts"))}) =====')
            print(f'===== GROUP INPUT: facts={row.get("facts")} (type: {type(row.get("facts"))}) =====')

        current_span = trace.get_current_span()

        start_time = time.time()
        input_count = len(dataframe) if dataframe is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.group.input_record_count': input_count,
                'dataframe.group.index_columns': len(self.unique_index_columns()),
                'dataframe.group.operation': 'inventory_scope_groupby',
            },
        )

        # Use centralized aggregation system from base class
        from metrics_utility.automation_controller_billing.dataframe_engine.base import build_aggregation_expressions
        
        # Build aggregation expressions using centralized system
        agg_exprs = build_aggregation_expressions(self.group_aggregations())
        
        print(f'===== GROUP AGGREGATION: Using centralized system with {len(agg_exprs)} expressions =====')
        for expr in agg_exprs:
            print(f'  - {expr}')

        print(f'===== GROUP BEFORE AGGREGATION: {len(agg_exprs)} aggregation expressions =====')

        # Perform the group by operation with detailed metrics
        groupby_start = time.time()
        input_count_for_groupby = len(dataframe) if dataframe is not None else 0
        group = dataframe.group_by(self.unique_index_columns(), maintain_order=True).agg(agg_exprs)
        groupby_duration = time.time() - groupby_start
        output_count_for_groupby = len(group) if group is not None else 0
        
        add_span_attributes(
            current_span,
            **{
                'dataframe.group.groupby_duration_seconds': groupby_duration,
                'dataframe.group.maintain_order': True,
                'polars.group_by.input_records': input_count_for_groupby,
                'polars.group_by.output_records': output_count_for_groupby,
                'polars.group_by.compression_ratio': (input_count_for_groupby - output_count_for_groupby) / input_count_for_groupby if input_count_for_groupby > 0 else 0,
            },
        )

        print(f'===== GROUP AFTER AGGREGATION =====')
        print(f'===== GROUP OUTPUT SCHEMA: {group.schema} =====')
        print(f'===== GROUP OUTPUT RECORD COUNT: {len(group)} =====')

        # Post-process combine_json_values columns
        if 'canonical_facts_list' in group.columns or 'facts_list' in group.columns:
            print('===== GROUP POST-PROCESSING: Handling combine_json_values columns =====')
            post_process_cols = []

            if 'canonical_facts_list' in group.columns:
                post_process_cols.append(
                    group['canonical_facts_list']
                    .map_elements(
                        lambda x: merge_json_lists_to_dict(x.to_list() if hasattr(x, 'to_list') else x, 'canonical_facts'), return_dtype=pd.String
                    )
                    .alias('canonical_facts')
                )

            if 'facts_list' in group.columns:
                post_process_cols.append(
                    group['facts_list']
                    .map_elements(lambda x: merge_json_lists_to_dict(x.to_list() if hasattr(x, 'to_list') else x, 'facts'), return_dtype=pd.String)
                    .alias('facts')
                )

            if post_process_cols:
                group = group.with_columns(post_process_cols)
                # Drop the temporary list columns
                drop_cols = [col for col in ['canonical_facts_list', 'facts_list'] if col in group.columns]
                if drop_cols:
                    group = group.drop(drop_cols)

        grouped_count = len(group) if group is not None else 0
        result = group

        duration = time.time() - start_time
        add_span_attributes(
            current_span,
            **{
                'dataframe.group.duration_seconds': duration,
                'dataframe.group.output_record_count': grouped_count,
                'dataframe.group.compression_ratio': (input_count - grouped_count) / input_count if input_count > 0 else 0,
                'dataframe.group.records_aggregated': input_count - grouped_count,
            },
        )

        if duration > 1.0:
            add_span_attributes(
                current_span, **{'dataframe.group.slow_operation': True, 'dataframe.group.performance_warning': f'Group took {duration:.2f}s'}
            )

        return result

    # Merge pre-aggregated
    @traced_method('inventory_scope.regroup')
    def regroup(self, dataframe):
        """Regroup pre-aggregated inventory scope dataframe with performance tracking."""
        print('===== REGROUP() OPERATION START =====')
        print(f'===== REGROUP INPUT SCHEMA: {dataframe.schema if dataframe is not None else "None"} =====')
        print(f'===== REGROUP INPUT RECORD COUNT: {len(dataframe) if dataframe is not None else 0} =====')

        if dataframe is not None and len(dataframe) > 0:
            row = dataframe.row(0, named=True)
            print(f'===== REGROUP INPUT SAMPLE: host={row.get("host_name")} =====')
            print(f'===== REGROUP INPUT: canonical_facts={row.get("canonical_facts")} (type: {type(row.get("canonical_facts"))}) =====')
            print(f'===== REGROUP INPUT: facts={row.get("facts")} (type: {type(row.get("facts"))}) =====')

        current_span = trace.get_current_span()

        start_time = time.time()
        input_count = len(dataframe) if dataframe is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.regroup.input_record_count': input_count,
                'dataframe.regroup.index_columns': len(self.unique_index_columns()),
                'dataframe.regroup.operation': 'inventory_scope_regroup_after_dedup',
            },
        )

        # Use native Polars types for merging during rollup aggregation

        def merge_native_lists(series):
            """Merge multiple List columns into single List with unique values"""
            all_values = []
            for lst in series:
                if lst is not None and isinstance(lst, list):
                    all_values.extend(lst)
                elif lst is not None:
                    all_values.append(lst)
            # Remove duplicates while preserving order
            unique_values = list(set([str(v) for v in all_values if v is not None]))
            return sorted(unique_values)

        def merge_native_dicts_local(series):
            """Merge multiple dict columns following demo_prompt_facts.md patterns"""
            merged_dict = {}
            for d in series:
                if d is not None and isinstance(d, dict):
                    for key, values in d.items():
                        if key not in merged_dict:
                            merged_dict[key] = []

                        # Ensure values is a list and extend
                        if isinstance(values, list):
                            merged_dict[key].extend(values)
                        else:
                            merged_dict[key].append(values)

            # Remove duplicates and sort for consistency
            for key in merged_dict:
                filtered = [str(x) for x in merged_dict[key] if x is not None]
                merged_dict[key] = sorted(list(set(filtered)))

            return merged_dict

        print('===== REGROUP AGGREGATION START =====')

        # Use centralized aggregation system for regroup operations
        from metrics_utility.automation_controller_billing.dataframe_engine.base import build_aggregation_expressions
        
        # Build regroup aggregation expressions using centralized system
        regroup_exprs = build_aggregation_expressions(self.regroup_aggregations())
        print(f'===== REGROUP AGGREGATION: Using centralized system with {len(regroup_exprs)} expressions =====')
        
        # Perform the regroup by operation with detailed metrics
        regroup_start = time.time()
        input_count_for_regroup = len(dataframe) if dataframe is not None else 0
        result = dataframe.group_by(self.unique_index_columns(), maintain_order=True).agg(regroup_exprs)
        regroup_duration = time.time() - regroup_start
        output_count_for_regroup = len(result) if result is not None else 0
        
        add_span_attributes(
            current_span,
            **{
                'dataframe.regroup.groupby_duration_seconds': regroup_duration,
                'dataframe.regroup.maintain_order': True,
                'polars.group_by.input_records': input_count_for_regroup,
                'polars.group_by.output_records': output_count_for_regroup,
                'polars.group_by.compression_ratio': (input_count_for_regroup - output_count_for_regroup) / input_count_for_regroup if input_count_for_regroup > 0 else 0,
            },
        )

        print(f'===== REGROUP AFTER AGGREGATION =====')
        print(f'===== REGROUP OUTPUT SCHEMA: {result.schema} =====')
        print(f'===== REGROUP OUTPUT RECORD COUNT: {len(result)} =====')

        if result is not None and len(result) > 0:
            row = result.row(0, named=True)
            print(f'===== REGROUP OUTPUT SAMPLE: host={row.get("host_name")} =====')
            print(f'===== REGROUP OUTPUT: canonical_facts={row.get("canonical_facts")} (type: {type(row.get("canonical_facts"))}) =====')
            print(f'===== REGROUP OUTPUT: facts={row.get("facts")} (type: {type(row.get("facts"))}) =====')

        # Post-process to merge JSON objects for canonical_facts and facts
        if 'canonical_facts_list' in result.columns or 'facts_list' in result.columns:
            print('===== REGROUP POST-PROCESSING: Handling JSON merging =====')
            post_process_cols = []

            if 'canonical_facts_list' in result.columns:
                print('===== REGROUP: Processing canonical_facts_list =====')
                post_process_cols.append(
                    result['canonical_facts_list']
                    .map_elements(
                        lambda x: merge_native_dicts(x.to_list() if hasattr(x, 'to_list') else x, 'canonical_facts'), return_dtype=pd.String
                    )
                    .alias('canonical_facts')
                )

            if 'facts_list' in result.columns:
                print('===== REGROUP: Processing facts_list =====')
                post_process_cols.append(
                    result['facts_list']
                    .map_elements(lambda x: merge_native_dicts(x.to_list() if hasattr(x, 'to_list') else x, 'facts'), return_dtype=pd.String)
                    .alias('facts')
                )

            if post_process_cols:
                result = result.with_columns(post_process_cols)
                # Drop the temporary list columns
                drop_cols = [col for col in ['canonical_facts_list', 'facts_list'] if col in result.columns]
                if drop_cols:
                    result = result.drop(drop_cols)

        duration = time.time() - start_time
        output_count = len(result) if result is not None else 0

        # Schema application will be handled by base class regroup_with_schema

        add_span_attributes(
            current_span,
            **{
                'dataframe.regroup.duration_seconds': duration,
                'dataframe.regroup.output_record_count': output_count,
                'dataframe.regroup.compression_ratio': (input_count - output_count) / input_count if input_count > 0 else 0,
            },
        )

        if duration > 0.5:
            add_span_attributes(current_span, **{'dataframe.regroup.slow_operation': True})

        return result

