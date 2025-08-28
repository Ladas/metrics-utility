import time
from typing import Any, Dict

import polars as pd
import pyarrow as pa

from opentelemetry import trace

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base
from metrics_utility.automation_controller_billing.helpers import parse_json
from metrics_utility.tracing import add_span_attributes, traced_method


def compute_serial(row):
    facts = parse_json(row['canonical_facts'])
    if facts.get('ansible_product_serial') is None or facts.get('ansible_machine_id') is None:
        return None
    return facts.get('ansible_product_serial', '') + '/' + facts.get('ansible_machine_id', '')


# dataframe for main_host
class DataframeInventoryScope(Base):
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


    @staticmethod
    def collector_dataframe_schema() -> Dict[str, str]:
        """Define Polars dataframe schema for processed CSV data (before grouping).
        
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
            'canonical_facts': 'String',  # JSON string
            'facts': 'String',  # JSON string
            'original_host_name': 'String',
        }

    @staticmethod
    def dataframe_schema() -> Dict[str, str]:
        """Define Polars dataframe schema for working dataframes (after grouping).
        
        Returns:
            Dictionary mapping column names to Polars dtypes as strings
        """
        return {
            # Index columns
            'host_name': 'String',
            'install_uuid': 'String',
            
            # Aggregated data columns
            'last_automation': 'Datetime',  # Max automation time as proper datetime
            
            # Complex aggregated data - Facts as JSON strings for list format storage
            'canonical_facts': 'String',  # JSON strings {"fact1": ["value1", "value2"]} list format
            'facts': 'String',  # JSON strings {"fact2": ["value2", "value3"]} list format
            
            # Collections as JSON strings for list merging operations
            'organizations': 'String',  # JSON array ["org1", "org2", "org3"]
            'inventories': 'String',  # JSON array ["inv1", "inv2", "inv3"]
            'serials': 'String',  # JSON array ["serial1", "serial2", "serial3"]
            'host_names_before_dedup': 'String',  # JSON array ["host1", "host2", "host3"]
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
            'canonical_facts': '{}',  # Empty JSON object
            'facts': '{}',  # Empty JSON object
            'install_uuid': '',
            'original_host_name': '',
            'serial': '',
            'host_names_before_dedup': '',  # Single host tracking initially
        }

    def get_rollup_default_values(self) -> Dict[str, Any]:
        """Get custom default values for rollup schema columns.
        
        Returns:
            Dictionary mapping column names to custom default values
        """
        return {
            'host_name': '',
            'install_uuid': '',
            'last_automation': None,  # Null datetime for aggregated data
            'canonical_facts': '{}',  # Empty JSON object
            'facts': '{}',  # Empty JSON object
            'organizations': '[]',  # Empty JSON array
            'inventories': '[]',  # Empty JSON array
            'serials': '[]',  # Empty JSON array
            'host_names_before_dedup': '[]',  # Empty JSON array for dedup tracking
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
            # Required identification columns that cannot be null
            'host_name': {'required': True, 'allow_null': False},
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
            'organizations': 'unique',  # Collect unique organization names
            'inventories': 'unique',  # Collect unique inventory names
            'serials': 'unique',  # Collect unique serial numbers
            'host_names_before_dedup': 'unique',  # Collect unique host names before deduplication
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

        # Handle empty DataFrame case
        if billing_data is None or len(billing_data) == 0:
            return self.empty()

        input_row_count = len(billing_data)

        # Step 1: Validate CSV data against collector schema
        billing_data = self.validate_collector_data(
            billing_data,
            strict_columns=['host_name'], 
            default_values=self.get_collector_default_values()
        )
        
        # Step 2: Add core metadata columns
        billing_data = billing_data.with_columns(pd.lit(batch_data['config']['install_uuid']).alias('install_uuid'))
        
        billing_data = self._apply_inventory_transformations(billing_data, current_span, date)
        
        # Step 4: Apply complete collector_dataframe schema (includes validation)
        billing_data = self.apply_complete_schema(billing_data, schema_type="collector_dataframe", operation_context="after_inventory_transformations")

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
            # Replace missing ansible_host_variable with host name and use it as host_name
            billing_data = billing_data.with_columns([
                billing_data['ansible_host_variable'].fill_null(billing_data['host_name']).alias('ansible_host_variable')
            ])
            billing_data = billing_data.with_columns([billing_data['ansible_host_variable'].alias('host_name')])

        # Serial computation - often computationally expensive
        serial_start = time.time()
        billing_data = billing_data.with_columns([
            billing_data['canonical_facts'].map_elements(lambda x: compute_serial({'canonical_facts': x}), return_dtype=pd.Utf8).alias('serial'),
            billing_data['host_name'].alias('host_names_before_dedup'),
            pd.lit('{}', dtype=pd.Utf8).alias('facts'),  # Initialize empty facts column for consistency
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

    # Do the aggregation
    @traced_method('inventory_scope.group')
    def group(self, dataframe):
        """Group and aggregate inventory scope dataframe with performance tracking."""
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

        # Use proper aggregation based on initial_aggregations() method to avoid data loss
        # Build aggregation expressions based on the defined aggregation rules
        agg_exprs = []
        initial_aggs = self.initial_aggregations()

        # Helper function to convert list to JSON set string
        import json

        def list_to_json_set(values_list):
            """Convert list to JSON string set format"""
            if values_list is None:
                return '[]'
            # Remove nulls and convert to unique list
            unique_values = [str(v) for v in values_list if v is not None and str(v) != 'null']
            return json.dumps(sorted(list(set(unique_values))))

        # Map column names from CSV to final column names and build aggregation expressions
        column_mapping = {'organization_name': 'organizations', 'inventory_name': 'inventories', 'serial': 'serials'}

        for final_col in self.data_columns():
            agg_rule = initial_aggs.get(final_col)

            if final_col in ['organizations', 'inventories', 'serials']:
                # These come from different CSV column names
                source_col = {v: k for k, v in column_mapping.items()}[final_col]
                if agg_rule == 'collect_unique_as_json_set':
                    agg_exprs.append(pd.col(source_col).filter(pd.col(source_col).is_not_null()).unique().alias(f'{final_col}_list'))
            elif agg_rule == 'max':
                agg_exprs.append(pd.col(final_col).max().alias(final_col))
            elif agg_rule == 'first_non_null':
                agg_exprs.append(pd.col(final_col).filter(pd.col(final_col).is_not_null()).first().alias(final_col))
            elif agg_rule == 'first':
                agg_exprs.append(pd.col(final_col).first().alias(final_col))

        group = dataframe.group_by(self.unique_index_columns(), maintain_order=True).agg(agg_exprs)

        # Convert list columns to JSON string format
        list_columns = ['organizations', 'inventories', 'serials']
        conversions = []
        for col in list_columns:
            list_col = f'{col}_list'
            if list_col in group.columns:
                conversions.append(group[list_col].map_elements(list_to_json_set, return_dtype=pd.Utf8).alias(col))

        if conversions:
            group = group.with_columns(conversions)
            # Drop temporary list columns
            drop_cols = [f'{col}_list' for col in list_columns if f'{col}_list' in group.columns]
            if drop_cols:
                group = group.drop(drop_cols)

        grouped_count = len(group) if group is not None else 0

        # Schema application will be handled by base class _group_with_schema
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

        result = dataframe.group_by(self.unique_index_columns(), maintain_order=True).agg(
            [
                pd.col('organizations').first().alias('organizations'),  # Simplified for polars compatibility
                pd.col('inventories').first().alias('inventories'),  # Simplified for polars compatibility
                pd.lit(None).alias('canonical_facts'),  # Simplified for polars compatibility
                pd.lit(None).alias('facts'),  # Simplified for polars compatibility
                pd.col('last_automation').max().alias('last_automation'),
                pd.col('serials').first().alias('serials'),  # Simplified for polars compatibility
                pd.col('host_names_before_dedup').first().alias('host_names_before_dedup'),  # Simplified
            ]
        )

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
            'canonical_facts': 'first_non_null',  # Take first non-null canonical facts JSON
            'facts': 'first_non_null',  # Take first non-null facts JSON
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
