import time
from typing import Any, Dict

import polars as pd
import pyarrow as pa

from opentelemetry import trace

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base, merge_and_stringify_facts, merge_list_arrays, convert_list_pairs_to_dict, merge_json_lists_to_dict, merge_native_dicts, merge_native_lists
from metrics_utility.automation_controller_billing.helpers import parse_json
from metrics_utility.tracing import add_span_attributes, traced_method


def compute_serial(row):
    # Handle list of key-value pairs format from collector_dataframe_schema
    facts_list = row['canonical_facts']
    if not isinstance(facts_list, list):
        return None
    
    # Convert list of [key, value] pairs back to dict for lookup
    facts_dict = convert_list_pairs_to_dict(facts_list)
    
    # Handle consistent array format - all values are now arrays
    ansible_product_serial = facts_dict.get('ansible_product_serial', [])
    ansible_machine_id = facts_dict.get('ansible_machine_id', [])
    
    # Extract first value from arrays (or empty string if missing)
    # Handle both list and Series objects
    serial = ansible_product_serial[0] if (ansible_product_serial is not None and len(ansible_product_serial) > 0) else ''
    machine_id = ansible_machine_id[0] if (ansible_machine_id is not None and len(ansible_machine_id) > 0) else ''
    
    if not serial or not machine_id:
        return None
    return serial + '/' + machine_id


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
        return pa.schema([
            # Index columns (unique identifiers)
            pa.field("host_name", pa.string()),
            pa.field("install_uuid", pa.string()),
            
            # Raw CSV data columns (before aggregation)
            pa.field("last_automation", pa.string()),  # Keep as string for Polars compatibility
            pa.field("organization_name", pa.string()),
            pa.field("inventory_name", pa.string()),
            pa.field("ansible_host_variable", pa.string()),  # Host variable mapping for deduplication
            
            # Complex data columns (stored as JSON strings)
            pa.field("canonical_facts", pa.string()),  # JSON string for dynamic dictionary content
            pa.field("facts", pa.string()),  # JSON string for dynamic dictionary content
            pa.field("host_names_before_dedup", pa.string()),  # Store as string for dedup tracking
            
            # Calculated columns
            pa.field("organizations", pa.string()),  # JSON array string
            pa.field("inventories", pa.string()),  # JSON array string
            pa.field("serials", pa.string()),  # JSON array string
        ])

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
        return pa.schema([
            # Index columns (unique identifiers)
            pa.field("host_name", pa.string()),
            pa.field("install_uuid", pa.string()),
            
            # Aggregated data columns
            pa.field("last_automation", pa.timestamp('us')),  # Proper timestamp for datetime data
            
            # Complex aggregated data (JSON strings for merging compatibility)
            pa.field("canonical_facts", pa.string()),  # JSON object string
            pa.field("facts", pa.string()),  # JSON object string
            pa.field("organizations", pa.string()),  # JSON array string
            pa.field("inventories", pa.string()),  # JSON array string
            pa.field("serials", pa.string()),  # JSON array string
            pa.field("host_names_before_dedup", pa.string()),  # JSON array string
        ])

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
    def initial_aggregations():
        """Define how to aggregate raw CSV data when grouping by unique_index_columns during initial processing.

        This is used in the group() method when processing CSV data from a single file/batch.
        For duplicate records with the same unique index, these aggregations combine the data.

        Returns:
            dict: Mapping of column_name -> aggregation_expression for Polars group_by().agg()
        """
        return {
            # Data columns aggregation rules for initial CSV processing
            'last_automation': 'max',  # Latest automation timestamp
            'canonical_facts': 'first',  # Take first list of facts for single batch processing
            'facts': 'first',  # Take first list of facts for single batch processing
            'organizations': 'unique',  # Collect unique organization names
            'inventories': 'unique',  # Collect unique inventory names
            'serials': 'unique',  # Collect unique serial numbers
            'host_names_before_dedup': 'first',  # Single host name initially
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

        print(f"===== PIPELINE START: _process_batch_inventory called with {len(billing_data) if billing_data is not None else 0} records =====")
        
        # Debug: Check raw CSV data schema and content
        if billing_data is not None and len(billing_data) > 0:
            print(f"===== RAW CSV DATA SCHEMA: {billing_data.schema} =====")
            print(f"===== RAW CSV COLUMNS: {billing_data.columns} =====")
            for i in range(min(2, len(billing_data))):
                row = billing_data.row(i, named=True)
                print(f"===== RAW CSV Record {i}: host={row.get('host_name')} =====")
                print(f"===== RAW CSV Record {i}: canonical_facts={row.get('canonical_facts')} (type: {type(row.get('canonical_facts'))}) =====")
                print(f"===== RAW CSV Record {i}: facts={row.get('facts')} (type: {type(row.get('facts'))}) =====")
        
        # Handle empty DataFrame case
        if billing_data is None or len(billing_data) == 0:
            return self.empty()

        input_row_count = len(billing_data)

        # Step 1: Add core metadata columns first (required for validation)
        billing_data = billing_data.with_columns(pd.lit(batch_data['config']['install_uuid']).alias('install_uuid'))
        
        # Step 2: Validate CSV data against collector schema (after adding required columns)
        print(f"===== BEFORE COLLECTOR VALIDATION =====")
        if len(billing_data) > 0:
            row = billing_data.row(0, named=True)
            print(f"===== PRE-VALIDATION: canonical_facts={row.get('canonical_facts')} =====")
            print(f"===== PRE-VALIDATION: facts={row.get('facts')} =====")
        
        billing_data = self.validate_collector_data(
            billing_data,
            strict_columns=['host_name'], 
            default_values=self.get_collector_default_values()
        )
        
        print(f"===== AFTER COLLECTOR VALIDATION =====")
        print(f"===== POST-VALIDATION SCHEMA: {billing_data.schema} =====")
        if len(billing_data) > 0:
            row = billing_data.row(0, named=True)
            print(f"===== POST-VALIDATION: canonical_facts={row.get('canonical_facts')} (type: {type(row.get('canonical_facts'))}) =====")
            print(f"===== POST-VALIDATION: facts={row.get('facts')} (type: {type(row.get('facts'))}) =====")
        
        # Step 3: Apply business logic transformations on raw CSV data
        print(f"===== BEFORE BUSINESS TRANSFORMATIONS =====")
        billing_data = self._apply_inventory_transformations(billing_data, current_span, date)
        
        print(f"===== AFTER BUSINESS TRANSFORMATIONS =====")
        print(f"===== POST-TRANSFORM SCHEMA: {billing_data.schema} =====")
        if len(billing_data) > 0:
            row = billing_data.row(0, named=True)
            print(f"===== POST-TRANSFORM: canonical_facts={row.get('canonical_facts')} (type: {type(row.get('canonical_facts'))}) =====")
            print(f"===== POST-TRANSFORM: facts={row.get('facts')} (type: {type(row.get('facts'))}) =====")
        
        # Step 4: Apply collector_dataframe schema to convert JSON strings to native types
        print(f"===== BEFORE COLLECTOR_DATAFRAME SCHEMA =====")
        billing_data = self.apply_complete_schema(billing_data, schema_type="collector_dataframe", operation_context="after_inventory_transformations")
        
        print(f"===== AFTER COLLECTOR_DATAFRAME SCHEMA =====")
        print(f"===== COLLECTOR_DATAFRAME SCHEMA: {billing_data.schema} =====")
        if len(billing_data) > 0:
            row = billing_data.row(0, named=True)
            print(f"===== COLLECTOR_DATAFRAME: canonical_facts={row.get('canonical_facts')} (type: {type(row.get('canonical_facts'))}) =====")
            print(f"===== COLLECTOR_DATAFRAME: facts={row.get('facts')} (type: {type(row.get('facts'))}) =====")

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
            billing_data = billing_data.with_columns([
                pd.when((billing_data['ansible_host_variable'].is_null()) | (billing_data['ansible_host_variable'] == ''))
                .then(billing_data['host_name'])
                .otherwise(billing_data['ansible_host_variable'])
                .alias('ansible_host_variable')
            ])
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
        billing_data = billing_data.with_columns([
            billing_data['canonical_facts'].map_elements(lambda x: compute_serial({'canonical_facts': x}) if x else None, return_dtype=pd.Utf8).alias('serial'),
            billing_data['host_name'].alias('host_names_before_dedup'),
        ])
        
        # Add serials column after serial computation
        billing_data = billing_data.with_columns([
            billing_data['serial'].alias('serials')
        ])
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
        print("===== GROUP() OPERATION START =====")
        print(f"===== GROUP INPUT SCHEMA: {dataframe.schema if dataframe is not None else 'None'} =====")
        print(f"===== GROUP INPUT RECORD COUNT: {len(dataframe) if dataframe is not None else 0} =====")
        
        if dataframe is not None and len(dataframe) > 0:
            row = dataframe.row(0, named=True)
            print(f"===== GROUP INPUT SAMPLE: host={row.get('host_name')} =====")
            print(f"===== GROUP INPUT: canonical_facts={row.get('canonical_facts')} (type: {type(row.get('canonical_facts'))}) =====")
            print(f"===== GROUP INPUT: facts={row.get('facts')} (type: {type(row.get('facts'))}) =====")
        
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

        # Build aggregation expressions based on existing columns after transformation
        agg_exprs = []
        initial_aggs = self.initial_aggregations()
        
        for final_col in self.data_columns():
            agg_rule = initial_aggs.get(final_col)
            
            # All columns should exist directly on the dataframe after transformation
            if agg_rule == 'max':
                agg_exprs.append(pd.col(final_col).max().alias(final_col))
            elif agg_rule == 'first_non_null':
                agg_exprs.append(pd.col(final_col).filter(pd.col(final_col).is_not_null()).first().alias(final_col))
            elif agg_rule == 'combine_json_values':
                # For columns like canonical_facts and facts that need JSON merging
                # Collect all values for later processing
                print(f"===== GROUP AGGREGATION: Adding combine_json_values for {final_col} =====")
                agg_exprs.append(pd.col(final_col).filter(pd.col(final_col).is_not_null()).alias(f'{final_col}_list'))
            elif agg_rule == 'first':
                agg_exprs.append(pd.col(final_col).first().alias(final_col))
            elif agg_rule == 'unique':
                # For String columns that were transformed from CSV (organizations, inventories, serials)
                # collect all non-null/non-empty values and create proper Lists with unique values
                if final_col in ['organizations', 'inventories', 'serials']:
                    # Use Polars' proper aggregation syntax to collect unique values into a list
                    # In groupby context, we need to collect all values then get unique
                    agg_exprs.append(
                        pd.col(final_col)
                        .filter((pd.col(final_col).is_not_null()) & (pd.col(final_col) != ''))
                        .unique()
                        .alias(final_col)
                    )
                else:
                    # For other columns, use standard unique aggregation
                    agg_exprs.append(pd.col(final_col).filter(
                        (pd.col(final_col).is_not_null()) & 
                        (pd.col(final_col) != '')
                    ).unique().alias(final_col))
            elif agg_rule == 'collect_unique_as_json_set':
                # Collect unique values as a List (will be converted to JSON later)
                agg_exprs.append(
                    pd.col(final_col)
                    .filter((pd.col(final_col).is_not_null()) & (pd.col(final_col) != ''))
                    .unique()
                    .alias(final_col)
                )
            else:
                # Default to first() for unspecified columns
                agg_exprs.append(pd.col(final_col).first().alias(final_col))

        print(f"===== GROUP BEFORE AGGREGATION: {len(agg_exprs)} aggregation expressions =====")
        
        group = dataframe.group_by(self.unique_index_columns(), maintain_order=True).agg(agg_exprs)
        
        print(f"===== GROUP AFTER AGGREGATION =====")
        print(f"===== GROUP OUTPUT SCHEMA: {group.schema} =====")
        print(f"===== GROUP OUTPUT RECORD COUNT: {len(group)} =====")
        
        # Post-process combine_json_values columns
        if 'canonical_facts_list' in group.columns or 'facts_list' in group.columns:
            print("===== GROUP POST-PROCESSING: Handling combine_json_values columns =====")
            post_process_cols = []
            
            if 'canonical_facts_list' in group.columns:
                print("DEBUG: Processing canonical_facts_list in group()")
                post_process_cols.append(
                    group['canonical_facts_list'].map_elements(
                        lambda x: merge_json_lists_to_dict(x.to_list() if hasattr(x, 'to_list') else x, 'canonical_facts'), 
                        return_dtype=pd.String
                    ).alias('canonical_facts')
                )
                
            if 'facts_list' in group.columns:
                print("DEBUG: Processing facts_list in group()")
                post_process_cols.append(
                    group['facts_list'].map_elements(
                        lambda x: merge_json_lists_to_dict(x.to_list() if hasattr(x, 'to_list') else x, 'facts'), 
                        return_dtype=pd.String
                    ).alias('facts')
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
        print("===== REGROUP() OPERATION START =====")
        print(f"===== REGROUP INPUT SCHEMA: {dataframe.schema if dataframe is not None else 'None'} =====")
        print(f"===== REGROUP INPUT RECORD COUNT: {len(dataframe) if dataframe is not None else 0} =====")
        
        if dataframe is not None and len(dataframe) > 0:
            row = dataframe.row(0, named=True)
            print(f"===== REGROUP INPUT SAMPLE: host={row.get('host_name')} =====")
            print(f"===== REGROUP INPUT: canonical_facts={row.get('canonical_facts')} (type: {type(row.get('canonical_facts'))}) =====")
            print(f"===== REGROUP INPUT: facts={row.get('facts')} (type: {type(row.get('facts'))}) =====")
        
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
        
        print("===== REGROUP AGGREGATION START =====")
        
        result = dataframe.group_by(self.unique_index_columns(), maintain_order=True).agg(
            [
                # Use native List concatenation and flattening for all collections
                # Since these columns are already Lists, we need to flatten and dedupe
                pd.col('organizations').flatten().unique().alias('organizations'),
                pd.col('inventories').flatten().unique().alias('inventories'),
                pd.col('serials').flatten().unique().alias('serials'),
                pd.col('host_names_before_dedup').flatten().unique().alias('host_names_before_dedup'),
                
                # Use custom merging for JSON objects that need to be combined
                # These need special handling to merge the JSON objects properly
                pd.col('canonical_facts').alias('canonical_facts_list'),
                pd.col('facts').alias('facts_list'),
                
                # Simple aggregation
                pd.col('last_automation').max().alias('last_automation'),
            ]
        )
        
        print(f"===== REGROUP AFTER AGGREGATION =====")
        print(f"===== REGROUP OUTPUT SCHEMA: {result.schema} =====")
        print(f"===== REGROUP OUTPUT RECORD COUNT: {len(result)} =====")
        
        if result is not None and len(result) > 0:
            row = result.row(0, named=True)
            print(f"===== REGROUP OUTPUT SAMPLE: host={row.get('host_name')} =====")
            print(f"===== REGROUP OUTPUT: canonical_facts={row.get('canonical_facts')} (type: {type(row.get('canonical_facts'))}) =====")
            print(f"===== REGROUP OUTPUT: facts={row.get('facts')} (type: {type(row.get('facts'))}) =====")
        
        # Post-process to merge JSON objects for canonical_facts and facts
        if 'canonical_facts_list' in result.columns or 'facts_list' in result.columns:
            print("===== REGROUP POST-PROCESSING: Handling JSON merging =====")
            post_process_cols = []
            
            if 'canonical_facts_list' in result.columns:
                print("===== REGROUP: Processing canonical_facts_list =====")
                post_process_cols.append(
                    result['canonical_facts_list'].map_elements(
                        lambda x: merge_native_dicts(x.to_list() if hasattr(x, 'to_list') else x, 'canonical_facts'), 
                        return_dtype=pd.String
                    ).alias('canonical_facts')
                )
                
            if 'facts_list' in result.columns:
                print("===== REGROUP: Processing facts_list =====")
                post_process_cols.append(
                    result['facts_list'].map_elements(
                        lambda x: merge_native_dicts(x.to_list() if hasattr(x, 'to_list') else x, 'facts'), 
                        return_dtype=pd.String
                    ).alias('facts')
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

    @staticmethod
    def unique_index_columns():
        """Define columns that uniquely identify a record for grouping/deduplication."""
        return ['host_name', 'install_uuid']

    @staticmethod
    def data_columns():
        """Define data columns that need aggregation when grouping records."""
        return ['last_automation', 'organizations', 'inventories', 'canonical_facts', 'facts', 'serials', 'host_names_before_dedup']

    @staticmethod
    def initial_aggregations():
        """Define how to aggregate raw CSV data when grouping by unique_index_columns during initial processing.

        This is used in the group() method when processing CSV data from a single file/batch.
        For duplicate records with the same unique index, these aggregations combine the data.

        Returns:
            dict: Mapping of column_name -> aggregation_expression for Polars group_by().agg()
        """
        return {
            # Data columns aggregation rules for initial CSV processing
            'last_automation': 'max',  # Take latest automation time
            'organizations': 'collect_unique_as_json_set',  # Collect unique organization names into JSON set
            'inventories': 'collect_unique_as_json_set',  # Collect unique inventory names into JSON set
            'canonical_facts': 'combine_json_values',  # Combine JSON objects with arrays as values  
            'facts': 'combine_json_values',  # Combine JSON objects with arrays as values
            'serials': 'collect_unique_as_json_set',  # Collect unique serials into JSON set
            'host_names_before_dedup': 'first',  # Should be same for duplicates within same batch
        }


    @staticmethod
    def collector_schema() -> pa.Schema:
        """Define PyArrow schema for raw CSV collector data validation.
        
        This schema is used for validating main_host CSV data during initial
        collection and processing. It includes all columns that may appear in the
        raw CSV files for inventory scope tracking.
        
        Returns:
            PyArrow schema for collector data validation
        """
        return pa.schema([
            # Index columns (unique identifiers)
            pa.field("host_name", pa.string()),
            pa.field("install_uuid", pa.string()),
            
            # Raw CSV data columns (before aggregation)
            pa.field("last_automation", pa.string()),  # Keep as string for Polars compatibility
            pa.field("organization_name", pa.string()),
            pa.field("inventory_name", pa.string()),
            pa.field("ansible_host_variable", pa.string()),  # Host variable mapping for deduplication
            
            # Complex data columns (stored as JSON strings)
            pa.field("canonical_facts", pa.string()),  # JSON string for dynamic dictionary content
            pa.field("facts", pa.string()),  # JSON string for dynamic dictionary content
            pa.field("host_names_before_dedup", pa.string()),  # Store as string for dedup tracking
            
            # Calculated columns
            pa.field("organizations", pa.string()),  # JSON array string
            pa.field("inventories", pa.string()),  # JSON array string
            pa.field("serials", pa.string()),  # JSON array string
        ])

    @staticmethod
    def parquet_schema() -> pa.Schema:
        """Define PyArrow schema for aggregated rollup data validation.
        
        This schema is used for validating aggregated inventory scope data during
        rollup merging operations. It includes only the final aggregated columns
        after group_by operations.
        
        Returns:
            PyArrow schema for rollup data validation
        """
        return pa.schema([
            # Index columns (unique identifiers)
            pa.field("host_name", pa.string()),
            pa.field("install_uuid", pa.string()),
            
            # Aggregated data columns
            pa.field("last_automation", pa.timestamp('us')),  # Proper timestamp for datetime data
            
            # Complex aggregated data (JSON strings for merging compatibility)
            pa.field("organizations", pa.string()),  # JSON array string
            pa.field("inventories", pa.string()),  # JSON array string
            pa.field("canonical_facts", pa.string()),  # JSON object string
            pa.field("facts", pa.string()),  # JSON object string
            pa.field("serials", pa.string()),  # JSON array string
            pa.field("host_names_before_dedup", pa.string()),  # JSON array string
        ])

    def get_collector_default_values(self) -> Dict[str, Any]:
        """Get custom default values for collector schema columns.
        
        These defaults override the standard type-based defaults for domain-specific
        requirements for inventory scope processing.
        
        Returns:
            Dictionary mapping column names to custom default values
        """
        return {
            'host_name': '',
            'install_uuid': '',
            'last_automation': '',
            'organization_name': '',
            'inventory_name': '',
            'canonical_facts': '{}',  # Empty JSON object
            'facts': '{}',  # Empty JSON object
            'host_names_before_dedup': '',  # Empty string for single host tracking
            'organizations': '[]',  # Empty JSON array
            'inventories': '[]',  # Empty JSON array
            'serials': '[]',  # Empty JSON array
        }

    def get_rollup_default_values(self) -> Dict[str, Any]:
        """Get custom default values for rollup schema columns.
        
        Returns:
            Dictionary mapping column names to custom default values
        """
        return {
            'host_name': '',
            'install_uuid': '',
            'last_automation': '',
            'organizations': '[]',  # Empty JSON array
            'inventories': '[]',  # Empty JSON array
            'canonical_facts': '{}',  # Empty JSON object
            'facts': '{}',  # Empty JSON object
            'serials': '[]',  # Empty JSON array
            'host_names_before_dedup': '[]',  # Empty JSON array for dedup tracking
        }

    @staticmethod
    def operations():
        """Define how to merge rollup data when combining multiple rollup files.
        
        This is used by the summarize_merged_dataframes() method in the base class
        when resolving conflicts from join operations during rollup merging.
        
        Returns:
            dict: Mapping of column_name -> operation for resolving merge conflicts

        Valid operations:
            - 'min', 'max': Take minimum/maximum value
            - 'sum': Add values together
            - 'combine_set': Merge JSON set strings by union
            - 'combine_json_values': Merge JSON object strings by combining field values
        """
        return {
            # Index columns - should be identical but use min as safe fallback
            'host_name': 'min',
            'install_uuid': 'min',
            # Data columns
            'last_automation': 'max',
            'organizations': 'combine_set',
            'inventories': 'combine_set',
            'canonical_facts': 'combine_json_values',
            'facts': 'combine_json_values',
            'serials': 'combine_set',
            'host_names_before_dedup': 'combine_set',
        }

    def _combine_json_values_in_group(self, json_list, column_name):
        """Combine JSON values during initial group() aggregation.
        
        This handles the transformation from individual JSON objects to merged objects
        with arrays as values during the initial CSV processing aggregation.
        
        Args:
            json_list: Polars Series/List containing JSON strings from group aggregation
            column_name: Name of the column being processed (for debugging)
            
        Returns:
            JSON string with merged values as arrays
        """
        import json
        from metrics_utility.automation_controller_billing.dataframe_engine.base import combine_json_values
        
        print(f"!!!!! _combine_json_values_in_group called for {column_name} - this means combine_json_values aggregation is working !!!!!")
        print(f"DEBUG: Input type: {type(json_list)}")
        
        # Handle None or empty input
        if json_list is None:
            print(f"DEBUG: {column_name} json_list is None, returning empty dict")
            return "{}"
        
        # Convert Polars Series to Python list if needed
        if hasattr(json_list, 'to_list'):
            json_list = json_list.to_list()
            print(f"DEBUG: {column_name} converted to list: {json_list}")
        
        if not json_list:
            print(f"DEBUG: {column_name} json_list is empty, returning empty dict")
            return "{}"
            
        merged_dict = {}
        
        # Process each JSON string in the list
        for i, json_str in enumerate(json_list):
            print(f"DEBUG: {column_name} processing item {i}: {json_str} (type: {type(json_str)})")
            
            if json_str is None or json_str == '':
                continue
                
            try:
                parsed = json.loads(json_str)
                print(f"DEBUG: {column_name} parsed JSON {i}: {parsed}")
                
                if isinstance(parsed, dict):
                    # Filter out null values
                    clean_dict = {k: v for k, v in parsed.items() if v is not None and v != 'null' and v != 'NA'}
                    print(f"DEBUG: {column_name} clean dict {i}: {clean_dict}")
                    
                    if clean_dict:  # Only merge if there are valid values
                        merged_dict = combine_json_values(merged_dict, clean_dict)
                        print(f"DEBUG: {column_name} merged_dict after {i}: {merged_dict}")
                        
            except (json.JSONDecodeError, TypeError) as e:
                print(f"DEBUG: {column_name} JSON decode error for item {i}: {e}")
                continue
        
        # Convert sets to sorted lists before JSON serialization
        for key in merged_dict:
            if isinstance(merged_dict[key], set):
                merged_dict[key] = sorted(list(merged_dict[key]))
        
        result = json.dumps(merged_dict)
        print(f"DEBUG: {column_name} final result: {result}")
        return result

    def _merge_json_facts_list(self, json_list):
        """Merge a list of JSON objects into a single JSON object with arrays as values.
        
        This is used for combining canonical_facts and facts columns during aggregation.
        Each key in the resulting object will contain an array of all unique values
        from the input JSON objects.
        
        Args:
            json_list: Polars Series containing Lists of JSON strings from aggregation
            
        Returns:
            JSON string (not wrapped in a list)
        """
        import json
        from metrics_utility.automation_controller_billing.dataframe_engine.base import combine_json_values
        
        print(f"!!!!! _merge_json_facts_list called with: {json_list} (type: {type(json_list)}) !!!!!")
        
        # Handle None or empty input
        if json_list is None:
            print("!!!!! Returning empty dict for None input !!!!!")
            return "{}"
        
        # Convert Polars Series to Python list if needed
        if hasattr(json_list, 'to_list'):
            json_list = json_list.to_list()
            print(f"!!!!! Converted to list: {json_list} !!!!!")
            
        if not json_list:
            print("!!!!! Returning empty dict for empty list !!!!!")
            return "{}"
            
        merged_dict = {}
        
        # Process each item in the list (each item is a List containing JSON strings)
        for i, item in enumerate(json_list):
            print(f"!!!!! Processing item {i}: {item} (type: {type(item)}) !!!!!")
            
            if item is None:
                continue
                
            # Each item is a List of JSON strings
            if isinstance(item, list):
                print(f"!!!!! Item {i} is a list with {len(item)} elements !!!!!")
                for j, json_str in enumerate(item):
                    print(f"!!!!! Processing sub-item {i}.{j}: {json_str} !!!!!")
                    if json_str is None or json_str == '':
                        continue
                    try:
                        parsed = json.loads(json_str)
                        print(f"!!!!! Parsed {i}.{j}: {parsed} !!!!!")
                        if isinstance(parsed, dict):
                            # Filter out null values and 'NA' values when combining
                            clean_dict = {k: v for k, v in parsed.items() if v is not None and v != 'null' and v != 'NA'}
                            print(f"!!!!! Clean dict {i}.{j}: {clean_dict} !!!!!")
                            if clean_dict:  # Only merge if there are valid values
                                merged_dict = combine_json_values(merged_dict, clean_dict)
                                print(f"!!!!! Merged dict after {i}.{j}: {merged_dict} !!!!!")
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"!!!!! JSON decode error for {i}.{j}: {e} !!!!!")
                        continue
            elif isinstance(item, str):
                # Handle direct JSON string format
                print(f"!!!!! Item {i} is a string: {item} !!!!!")
                try:
                    parsed = json.loads(item)
                    if isinstance(parsed, dict):
                        clean_dict = {k: v for k, v in parsed.items() if v is not None and v != 'null'}
                        if clean_dict:
                            merged_dict = combine_json_values(merged_dict, clean_dict)
                except (json.JSONDecodeError, TypeError):
                    continue
        
        # Convert sets to sorted lists for consistent output
        result_dict = {}
        for key, values in merged_dict.items():
            if isinstance(values, set):
                result_dict[key] = sorted(list(values))
            else:
                result_dict[key] = values
        
        # Return as a JSON string (not wrapped in a list)
        return json.dumps(result_dict)

    @traced_method('inventory_scope.build_group')
    def build_group(self, batch_data):
        """Build Inventory Scope dataframe group from a single tarball's CSV data.

        Args:
            batch_data: Data from a single tarball extraction
                       Format: {'main_host': DataFrame, 'main_jobevent': DataFrame, ...}
        """
        # Get main_host data from this batch
        billing_data = batch_data.get('main_host')
        if billing_data is None or len(billing_data) == 0:
            return self.empty()

        # Process the single batch using existing logic
        processed_data = self._process_batch_inventory(billing_data, batch_data, trace.get_current_span(), None)
        return self.group(processed_data) if processed_data is not None else self.empty()
