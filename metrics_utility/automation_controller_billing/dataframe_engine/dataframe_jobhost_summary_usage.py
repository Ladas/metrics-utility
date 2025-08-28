"""Dataframe engine for processing job host summary and indirect nodes usage data.

This module provides comprehensive processing capabilities for job host summary
and indirect nodes data, including data validation, aggregation, and rollup
generation for AAP Controller billing metrics.

Key Features:
    - Batch-based data processing for memory efficiency
    - Automatic duplicate record aggregation using unique index keys
    - Two-tier aggregation system (initial CSV processing vs rollup merging)
    - Comprehensive schema validation and data quality filtering
    - Support for both direct and indirect managed node types (dual CSV input)
    - OpenTelemetry performance tracing integration
    - Centralized schema-driven data processing

Usage:
    from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_jobhost_summary_usage import DataframeJobhostSummaryUsage

    engine = DataframeJobhostSummaryUsage()
    result = engine.build_dataframe(batch_iterator)
"""

import json
import time

from typing import Any, Dict, Iterator, Optional

import polars as pd
import pyarrow as pa

from opentelemetry import trace

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base, convert_json_to_list_pairs, convert_list_pairs_to_dict
from metrics_utility.automation_controller_billing.helpers import parse_json_array
from metrics_utility.metric_utils import DIRECT, INDIRECT, MANAGED_NODE_TYPES
from metrics_utility.tracing import add_span_attributes, traced_method


class DataframeJobhostSummaryUsage(Base):
    """Processes job host summary and indirect nodes usage data for AAP Controller billing.

    This class handles the extraction, validation, aggregation, and storage of job
    host summary data and indirect nodes data. It processes data in batches for
    memory efficiency and provides two-tier aggregation for proper duplicate
    record handling.

    The class supports both direct managed nodes (from job_host_summary CSV data)
    and indirect managed nodes (from indirect_nodes CSV data), applying different
    processing rules for each type.

    All data processing follows a strict schema-driven pipeline:
    1. Raw CSV validation (dual schemas for jobhost_summary vs indirect_nodes)
    2. Conversion to unified collector_dataframe_schema format
    3. Grouping and aggregation with dataframe_schema
    4. Storage with parquet_schema

    Attributes:
        extra_params: Additional configuration parameters from base class

    Example:
        >>> engine = DataframeJobhostSummaryUsage()
        >>> batch_iterator = get_batch_data_iterator(date_range)
        >>> result_df = engine.build_dataframe(batch_iterator)
        >>> print(f"Processed {len(result_df)} aggregated records")
    """

    # ========================================
    # SCHEMA DEFINITIONS - PIPELINE ORDER
    # ========================================

    @staticmethod
    def jobhost_summary_csv_schema() -> pa.Schema:
        """Define PyArrow schema for job_host_summary CSV data validation.
        
        This schema validates raw CSV data from the job_host_summary table,
        used for DIRECT managed node processing.
        
        Returns:
            PyArrow schema for direct managed node CSV validation
        """
        return pa.schema([
            # Core identification columns
            pa.field("host_name", pa.string()),
            pa.field("organization_name", pa.string()),
            pa.field("job_template_name", pa.string()),
            pa.field("job_remote_id", pa.int64()),
            
            # Timestamp columns (keep as string for consistent parsing)
            pa.field("created", pa.string()),
            pa.field("job_created", pa.string()),
            
            # Task counter columns (for direct managed nodes)
            pa.field("dark", pa.int64()),
            pa.field("failures", pa.int64()),
            pa.field("ok", pa.int64()),
            pa.field("skipped", pa.int64()),
            pa.field("ignored", pa.int64()),
            pa.field("rescued", pa.int64()),
            
            # Optional host mapping
            pa.field("ansible_host_variable", pa.string()),
        ])

    @staticmethod
    def indirect_nodes_csv_schema() -> pa.Schema:
        """Define PyArrow schema for indirect_nodes CSV data validation.
        
        This schema validates raw CSV data from the indirect_nodes table,
        used for INDIRECT managed node processing.
        
        Returns:
            PyArrow schema for indirect managed node CSV validation
        """
        return pa.schema([
            # Core identification columns
            pa.field("host_name", pa.string()),
            pa.field("organization_name", pa.string()),
            pa.field("job_template_name", pa.string()),
            pa.field("job_remote_id", pa.int64()),
            
            # Timestamp columns (keep as string for consistent parsing)
            pa.field("created", pa.string()),
            pa.field("job_created", pa.string()),
            
            # Optional host mapping
            pa.field("ansible_host_variable", pa.string()),
            
            # Complex data columns (stored as JSON strings)
            pa.field("canonical_facts", pa.string()),
            pa.field("facts", pa.string()),
            pa.field("events", pa.string()),
        ])

    @staticmethod
    def collector_schema() -> pa.Schema:
        """Define PyArrow schema for raw CSV collector data validation.
        
        This method selects the appropriate schema based on the data source.
        For this wrapper, we support both jobhost_summary and indirect_nodes.
        The actual validation happens in _validate_csv_data method.
        
        Returns:
            Combined PyArrow schema covering both data sources
        """
        # Return a combined schema that covers both data sources
        # Individual validation happens in _validate_csv_data
        return pa.schema([
            # Common columns across both sources
            pa.field("host_name", pa.string()),
            pa.field("organization_name", pa.string()),
            pa.field("job_template_name", pa.string()),
            pa.field("job_remote_id", pa.int64()),
            pa.field("created", pa.string()),
            pa.field("job_created", pa.string()),
            pa.field("ansible_host_variable", pa.string()),
            
            # Task counters (jobhost_summary only)
            pa.field("dark", pa.int64()),
            pa.field("failures", pa.int64()),
            pa.field("ok", pa.int64()),
            pa.field("skipped", pa.int64()),
            pa.field("ignored", pa.int64()),
            pa.field("rescued", pa.int64()),
            
            # Complex data (indirect_nodes only)
            pa.field("canonical_facts", pa.string()),
            pa.field("facts", pa.string()),
            pa.field("events", pa.string()),
        ])

    @staticmethod
    def collector_dataframe_schema() -> Dict[str, str]:
        """Define Polars dataframe schema for processed CSV data (before grouping).
        
        This unified schema is applied after CSV processing but before the group() method.
        Both jobhost_summary and indirect_nodes data are converted to this common format.
        All columns are in their final types ready for aggregation operations.
        
        Returns:
            Dictionary mapping column names to Polars dtypes as strings
        """
        return {
            # Index columns (unique identifiers)
            'organization_name': 'String',
            'job_template_name': 'String', 
            'host_name': 'String',
            'original_host_name': 'String',
            'install_uuid': 'String',
            'job_remote_id': 'Int64',
            
            # Timestamp columns (use proper datetime types)
            'created': 'Datetime',
            'job_created': 'Datetime',
            
            # Metadata columns
            'managed_node_type': 'Int64',
            'managed_node_type_string': 'String',
            'ansible_host_variable': 'String',
            
            # Task counter columns (for direct managed nodes, 0 for indirect)  
            'dark': 'Int64',
            'failures': 'Int64',
            'ok': 'Int64',
            'skipped': 'Int64', 
            'ignored': 'Int64',
            'rescued': 'Int64',
            
            # Calculated columns
            'host_runs': 'Int64',
            'task_runs': 'Int64',
            'reachable_task_runs': 'Int64',
            
            # Complex data columns - Native Polars types after transformation from CSV
            'canonical_facts': 'List',  # Native List type (key-value pairs) after JSON parsing
            'facts': 'List',  # Native List type (key-value pairs) after JSON parsing
            
            # Collections as native List types after transformation
            'events': 'List',  # Native List[String] for events
            'host_names_before_dedup': 'List',  # Native List[String] for host tracking
        }

    @staticmethod
    def dataframe_schema() -> Dict[str, str]:
        """Define Polars dataframe schema for working dataframes (after grouping).
        
        This schema is used for:
        - Data after group() aggregation
        - Data when merging multiple rollups  
        - Data in report generation
        
        Returns:
            Dictionary mapping column names to Polars dtypes as strings
        """
        return {
            # Index columns (unique identifiers)
            'organization_name': 'String',
            'job_template_name': 'String',
            'host_name': 'String', 
            'original_host_name': 'String',
            'install_uuid': 'String',
            'job_remote_id': 'Int64',
            
            # Aggregated data columns
            'host_runs': 'Int64',
            'task_runs': 'Int64',
            'first_automation': 'Datetime',  # Proper datetime types for aggregated timestamps
            'last_automation': 'Datetime',
            'job_created': 'Datetime',
            'managed_node_type': 'Int64',
            
            # Complex aggregated data - Native Polars types for processing
            'canonical_facts': 'List',  # Native List type (key-value pairs) for facts aggregation
            'facts': 'List',  # Native List type (key-value pairs) for facts aggregation
            
            # Collections as native List types for efficient aggregation
            'managed_node_types_set': 'List',  # Native List[String] for node types
            'events': 'List',  # Native List[String] for events
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
            pa.field("organization_name", pa.string()),
            pa.field("job_template_name", pa.string()),
            pa.field("host_name", pa.string()),
            pa.field("original_host_name", pa.string()),
            pa.field("install_uuid", pa.string()),
            pa.field("job_remote_id", pa.int64()),
            
            # Aggregated data columns
            pa.field("host_runs", pa.int64()),
            pa.field("task_runs", pa.int64()),
            pa.field("first_automation", pa.timestamp('us')),  # Proper timestamp for datetime data
            pa.field("last_automation", pa.timestamp('us')),  # Proper timestamp for datetime data
            pa.field("job_created", pa.timestamp('us')),  # Proper timestamp for datetime data
            pa.field("managed_node_type", pa.int64()),
            
            # Complex aggregated data (JSON strings for merging compatibility)
            pa.field("managed_node_types_set", pa.string()),  # JSON array string
            pa.field("canonical_facts", pa.string()),  # JSON object string
            pa.field("facts", pa.string()),  # JSON object string
            pa.field("events", pa.string()),  # JSON array string
            pa.field("host_names_before_dedup", pa.string()),  # JSON array string
        ])

    def get_collector_default_values(self) -> Dict[str, Any]:
        """Get custom default values for collector schema columns.
        
        These defaults override the standard type-based defaults for domain-specific
        requirements like JSON strings and collections.
        
        Returns:
            Dictionary mapping column names to custom default values
        """
        return {
            'canonical_facts': [],  # Empty list for native List (key-value pairs)
            'facts': [],  # Empty list for native List (key-value pairs)
            'events': [],  # Empty list for native List
            'host_names_before_dedup': [],  # Empty list for native List
            'managed_node_types_set': [],  # Empty list for native List
            'ansible_host_variable': '',
            'organization_name': 'No organization name',
            'job_template_name': '',
            # Task counters default to 0
            'dark': 0,
            'failures': 0,
            'ok': 0,
            'skipped': 0,
            'ignored': 0,
            'rescued': 0,
            'host_runs': 1,
            'task_runs': 0,
            'reachable_task_runs': 0,
            'job_created': None,  # Null datetime for jobs without job_created data
        }

    def get_rollup_default_values(self) -> Dict[str, Any]:
        """Get custom default values for rollup schema columns.
        
        Uses native types to match dataframe_schema().
        
        Returns:
            Dictionary mapping column names to custom default values
        """
        return {
            'canonical_facts': [],  # Empty list for native List (key-value pairs)
            'facts': [],  # Empty list for native List (key-value pairs)
            'events': [],  # Empty list for native List
            'host_names_before_dedup': [],  # Empty list for native List
            'managed_node_types_set': [],  # Empty list for native List
            'organization_name': 'No organization name',
            'job_template_name': '',
            'original_host_name': '',
            'host_name': '',
            'install_uuid': '',
            'job_remote_id': 0,
            'host_runs': 0,
            'task_runs': 0,
            'first_automation': None,  # Null datetime
            'last_automation': None,  # Null datetime
            'job_created': None,  # Null datetime for aggregated data
            'managed_node_type': 0,
        }

    @staticmethod
    def collector_dataframe_validation_schema() -> Dict[str, Dict[str, Any]]:
        """Define validation rules for collector dataframe columns.
        
        This schema specifies which columns are required, which can be null,
        and validation rules for jobhost summary and indirect nodes data processing.
        
        Returns:
            Dictionary mapping column names to validation rule dictionaries
        """
        return {
            # Required identification columns that cannot be null
            'host_name': {'required': True, 'allow_null': False},
            'original_host_name': {'required': True, 'allow_null': False},
            'organization_name': {'required': True, 'allow_null': False},
            'job_remote_id': {'required': True, 'allow_null': False, 'min_value': 1},
            'install_uuid': {'required': True, 'allow_null': False},
            
            # Required data columns with validation
            'managed_node_type': {'required': True, 'allow_null': False, 'valid_values': [0, 1]},  # DIRECT=0, INDIRECT=1
            'host_runs': {'required': True, 'allow_null': False, 'min_value': 1},  # Each record represents at least one host run
            'task_runs': {'required': True, 'allow_null': False, 'min_value': 0},  # Can be 0 for hosts with no reachable tasks
            'reachable_task_runs': {'required': True, 'allow_null': False, 'min_value': 0},
            
            # Optional columns that can be null
            'job_template_name': {'required': False, 'allow_null': True},
            'ansible_host_variable': {'required': False, 'allow_null': True},
            'managed_node_type_string': {'required': False, 'allow_null': True},
            
            # Task counter columns (only for direct managed nodes, but schema validation allows nulls)
            'dark': {'required': False, 'allow_null': True, 'min_value': 0},
            'failures': {'required': False, 'allow_null': True, 'min_value': 0},
            'ok': {'required': False, 'allow_null': True, 'min_value': 0},
            'skipped': {'required': False, 'allow_null': True, 'min_value': 0},
            'ignored': {'required': False, 'allow_null': True, 'min_value': 0},
            'rescued': {'required': False, 'allow_null': True, 'min_value': 0},
            
            # JSON columns can be null/empty
            'canonical_facts': {'required': False, 'allow_null': True},
            'facts': {'required': False, 'allow_null': True},
            'events': {'required': False, 'allow_null': True},
            'host_names_before_dedup': {'required': False, 'allow_null': True},
        }

    # ========================================
    # STATIC CONFIGURATION METHODS
    # ========================================

    @staticmethod
    def unique_index_columns():
        """Define columns that uniquely identify a record for grouping/deduplication."""
        return ['organization_name', 'job_template_name', 'host_name', 'original_host_name', 'install_uuid', 'job_remote_id']

    @staticmethod
    def data_columns():
        """Define data columns that need aggregation when grouping records."""
        return ['host_runs', 'task_runs', 'first_automation', 'last_automation', 'job_created', 'managed_node_type', 
                'canonical_facts', 'facts', 'managed_node_types_set', 'events', 'host_names_before_dedup']

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
            'task_runs': 'sum',  # Sum task runs across duplicate records
            'host_runs': 'count',  # Count host occurrences (each record represents one host run)
            'first_automation': 'min',  # Earliest timestamp among duplicates
            'last_automation': 'max',  # Latest timestamp among duplicates
            'job_created': 'max',  # Latest job creation time
            'managed_node_type': 'min',  # Use lowest managed node type (DIRECT=0, INDIRECT=1)
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
    # CSV VALIDATION METHODS
    # ========================================

    def _validate_csv_data(self, billing_data, managed_node_type):
        """Validate CSV data against the appropriate schema based on managed node type.
        
        Args:
            billing_data: Raw DataFrame from CSV
            managed_node_type: DIRECT or INDIRECT constant
            
        Returns:
            Validated DataFrame with schema applied
        """
        if managed_node_type == DIRECT:
            schema = self.jobhost_summary_csv_schema()
        else:  # INDIRECT
            schema = self.indirect_nodes_csv_schema()
            
        # Apply validation using the base class method
        return self.validate_collector_data(
            billing_data, 
            strict_columns=['host_name', 'job_remote_id'],
            default_values=self.get_collector_default_values()
        )

    # ========================================
    # BUSINESS LOGIC METHODS
    # ========================================

    def _process_batch_data_with_schema(self, batch_data, current_span):
        """Process batch data and apply collector_dataframe_schema (BEFORE grouping).
        
        Override base class method to avoid double schema application since
        _process_individual_batch_data already applies the schema.
        """
        processed_data = self._process_batch_data(batch_data, current_span)
        if processed_data is None or len(processed_data) == 0:
            return self.empty()
        # Schema already applied in _process_individual_batch_data, no need to apply again
        return processed_data

    def _process_batch_data(self, batch_data, current_span):
        """Process job host summary batch data for JobHost Summary dataframe.

        This method processes a single batch of job host summary or indirect nodes data,
        determining the node type and applying appropriate processing.

        Args:
            batch_data: Dictionary containing batch data with keys:
                       - 'job_host_summary': DataFrame with direct managed node data
                       - 'indirect_nodes': DataFrame with indirect managed node data  
                       - 'config': Configuration including install_uuid
                       - '_date_context': Date for this batch

        Returns:
            Processed Polars DataFrame ready for grouping, or None if no valid data
        """
        # Check which data source we have and determine managed_node_type first
        job_host_data = batch_data.get('job_host_summary')
        indirect_data = batch_data.get('indirect_nodes')

        # Determine which data to use and corresponding node type
        if job_host_data is not None and not (hasattr(job_host_data, 'empty') and len(job_host_data) == 0):
            billing_data = job_host_data
            managed_node_type = DIRECT
        elif indirect_data is not None and not (hasattr(indirect_data, 'empty') and len(indirect_data) == 0):
            billing_data = indirect_data
            managed_node_type = INDIRECT
        else:
            # No valid data in this batch
            return None

        date = batch_data.get('_date_context')  # Get date from context
        processed_data = self._process_individual_batch_data(billing_data, batch_data, managed_node_type, current_span, date)
        
        if processed_data is None or len(processed_data) == 0:
            return None
            
        # Filter out any records with empty/null host names - these should not be processed
        if 'host_name' in processed_data.columns:
            valid_hosts_mask = (processed_data['host_name'].is_not_null()) & (processed_data['host_name'] != '') & (processed_data['host_name'] != 'null')
            processed_data = processed_data.filter(valid_hosts_mask)
            if len(processed_data) == 0:
                print(f"DEBUG: Filtered out all records with empty host names for {date}")
                return None

        return processed_data

    def _add_summary_validation_metrics(self, groups_processed: int, total_input_records: int, final_record_count: int, build_duration: float):
        """Add summary validation metrics for the entire dataframe build process."""
        
        # Calculate summary statistics from collected batch metrics
        total_input_rows = 0
        total_valid_rows = 0 
        total_invalid_rows = 0
        dates_with_data = set()
        
        for key, value in self._validation_metrics.items():
            if 'data_quality_by_date' in key and 'input_rows' in key:
                total_input_rows += value
                # Extract date from key for tracking
                parts = key.split('.')
                if len(parts) >= 2:
                    dates_with_data.add(parts[1])
            elif 'data_quality_by_date' in key and 'valid_rows' in key:
                total_valid_rows += value
            elif 'data_quality_by_date' in key and 'invalid_rows' in key:
                total_invalid_rows += value
        
        # Add comprehensive summary metrics
        summary_metrics = {
            'data_quality_summary.total_input_rows': total_input_rows,
            'data_quality_summary.total_valid_rows': total_valid_rows,
            'data_quality_summary.total_invalid_rows': total_invalid_rows,
            'data_quality_summary.overall_quality_ratio': total_valid_rows / total_input_rows if total_input_rows > 0 else 1.0,
            'data_quality_summary.dates_with_quality_data': len(dates_with_data),
            'processing_summary.groups_processed': groups_processed,
            'processing_summary.total_input_records': total_input_records,
            'processing_summary.final_record_count': final_record_count,
            'processing_summary.build_duration_seconds': build_duration,
            'processing_summary.records_per_second': total_input_records / build_duration if build_duration > 0 else 0,
        }
        
        self._add_validation_metrics(summary_metrics)

    def _process_individual_batch_data(self, billing_data, batch_data, managed_node_type, current_span, date):
        """Process individual batch data using centralized schema-driven approach.
        
        This method replaces the old inline schema validation with clean, centralized processing:
        1. CSV validation using appropriate schema (jobhost_summary vs indirect_nodes)
        2. Business logic transformations (task calculations, host mapping, etc.)
        3. Column standardization for unified collector_dataframe_schema format
        
        All schema validation, type casting, and column completion is handled by the 
        centralized schema system in the base class.
        """
        from metrics_utility.tracing import add_span_attributes

        # Handle empty DataFrame case
        if billing_data is None or len(billing_data) == 0:
            return self.empty()

        input_row_count = len(billing_data)

        # Step 1: Validate CSV data against appropriate schema
        billing_data = self._validate_csv_data(billing_data, managed_node_type)
        
        # Step 2: Add core metadata columns
        billing_data = self._add_metadata_columns(billing_data, batch_data, managed_node_type)
        
        # Step 3: Apply business logic transformations
        if managed_node_type == DIRECT:
            billing_data = self._process_direct_managed_nodes(billing_data, current_span, date)
        else:  # INDIRECT
            billing_data = self._process_indirect_managed_nodes(billing_data, current_span, date)
            
        # Step 4: Apply complete collector_dataframe schema (includes validation)
        billing_data = self.apply_complete_schema(billing_data, schema_type="collector_dataframe", operation_context="after_jobhost_transformations")

        # Add validation metrics for tracing
        final_count = len(billing_data) if billing_data is not None else 0
        add_span_attributes(
            current_span,
            **{
                f'data_quality.{date.isoformat()}.input_rows': input_row_count,
                f'data_quality.{date.isoformat()}.valid_rows': final_count,
                f'data_quality.{date.isoformat()}.managed_node_type': managed_node_type,
            },
        )
        
        # Store validation metrics for rollup metadata
        date_key = date.isoformat()
        batch_metrics = {
            f'data_quality_by_date.{date_key}.input_rows': input_row_count,
            f'data_quality_by_date.{date_key}.valid_rows': final_count,
            f'data_quality_by_date.{date_key}.managed_node_type': managed_node_type,
        }
        self._add_validation_metrics(batch_metrics)

        return billing_data

    def _add_metadata_columns(self, billing_data, batch_data, managed_node_type):
        """Add core metadata columns required for all records."""
        return billing_data.with_columns([
            pd.lit(managed_node_type).cast(pd.Int64).alias('managed_node_type'),
            pd.lit(MANAGED_NODE_TYPES[managed_node_type]).alias('managed_node_type_string'),
            pd.lit(batch_data['config']['install_uuid']).alias('install_uuid'),
            billing_data['host_name'].alias('original_host_name'),
        ])

    def _process_direct_managed_nodes(self, billing_data, current_span, date):
        """Process direct managed nodes (from job_host_summary CSV)."""
        # Apply ansible_host_variable mapping if present
        billing_data = self._apply_host_variable_mapping(billing_data)
        
        # Add host tracking columns
        billing_data = billing_data.with_columns([
            billing_data['host_name'].alias('host_names_before_dedup'),
            pd.lit(1).alias('host_runs')
        ])

        # Calculate task_runs by summing task counters
        task_calc_start = time.time()
        expected_columns = ['dark', 'failures', 'ok', 'skipped', 'ignored', 'rescued']
        available_columns = [col for col in expected_columns if col in billing_data.columns]
        if available_columns:
            billing_data = billing_data.with_columns(
                pd.sum_horizontal([pd.col(col).fill_null(0) for col in available_columns]).alias('task_runs')
            )
        else:
            billing_data = billing_data.with_columns(pd.lit(0).alias('task_runs'))

        # Calculate reachable_task_runs (excluding 'dark' counter)
        reachable_columns = ['failures', 'ok', 'skipped', 'ignored', 'rescued']
        available_reachable_columns = [col for col in reachable_columns if col in billing_data.columns]
        if available_reachable_columns:
            billing_data = billing_data.with_columns(
                pd.sum_horizontal([pd.col(col).fill_null(0) for col in available_reachable_columns]).alias('reachable_task_runs')
            )
        else:
            billing_data = billing_data.with_columns(pd.lit(0).alias('reachable_task_runs'))

        # Filter out unreachable managed nodes (had no reachable task runs)
        pre_filter_count = len(billing_data)
        billing_data = billing_data.filter(pd.col('reachable_task_runs') > 0)
        post_filter_count = len(billing_data)

        task_calc_duration = time.time() - task_calc_start
        if task_calc_duration > 0.1:  # Log slow task calculations
            add_span_attributes(
                current_span,
                **{
                    f'dataframe.build.{date.isoformat()}.task_calc_duration': task_calc_duration,
                    f'dataframe.build.{date.isoformat()}.unreachable_filtered': pre_filter_count - post_filter_count,
                },
            )

        return billing_data

    def _process_indirect_managed_nodes(self, billing_data, current_span, date):
        """Process indirect managed nodes (from indirect_nodes CSV)."""
        # Apply ansible_host_variable mapping if present
        billing_data = self._apply_host_variable_mapping(billing_data)
        
        # Add host tracking columns
        billing_data = billing_data.with_columns([
            billing_data['host_name'].alias('host_names_before_dedup'),
            pd.lit(1).alias('host_runs')
        ])

        # For indirect nodes, task_runs is always 1 (each record represents one task)
        billing_data = billing_data.with_columns(pd.lit(1).alias('task_runs'))
        
        # Set reachable_task_runs same as task_runs for indirect nodes
        billing_data = billing_data.with_columns(pd.lit(1).alias('reachable_task_runs'))

        # Convert JSON strings to native types (following Stage 2 of data flow)
        # Using centralized conversion function from base class

        def convert_json_to_list(json_str):
            """Convert JSON string to native list for Polars List"""
            import json
            if json_str is None or json_str == '':
                return []
            try:
                parsed = json.loads(json_str)
                return parsed if isinstance(parsed, list) else []
            except (json.JSONDecodeError, TypeError):
                return []

        # Convert complex data columns to native types for efficient processing
        if 'canonical_facts' in billing_data.columns:
            billing_data = billing_data.with_columns(
                billing_data['canonical_facts'].map_elements(convert_json_to_list_pairs, return_dtype=pd.List(pd.List(pd.Utf8))).alias('canonical_facts')
            )

        if 'facts' in billing_data.columns:
            billing_data = billing_data.with_columns(
                billing_data['facts'].map_elements(convert_json_to_list_pairs, return_dtype=pd.List(pd.List(pd.Utf8))).alias('facts')
            )

        if 'events' in billing_data.columns:
            # Parse events array and convert to native list
            def parse_events_to_list(x):
                try:
                    parsed_array = parse_json_array(x) if x else []
                    return list(dict.fromkeys(parsed_array)) if parsed_array else []
                except:
                    return []

            billing_data = billing_data.with_columns(
                billing_data['events'].map_elements(parse_events_to_list, return_dtype=pd.List(pd.Utf8)).alias('events')
            )

        return billing_data

    def _apply_host_variable_mapping(self, billing_data):
        """Apply ansible_host_variable mapping to determine final host_name."""
        if 'ansible_host_variable' in billing_data.columns:
            # Replace missing or empty ansible_host_variable with host name and use it as host_name
            billing_data = billing_data.with_columns([
                pd.when((billing_data['ansible_host_variable'].is_null()) | (billing_data['ansible_host_variable'] == ''))
                .then(billing_data['host_name'])
                .otherwise(billing_data['ansible_host_variable'])
                .alias('ansible_host_variable')
            ])
            billing_data = billing_data.with_columns([billing_data['ansible_host_variable'].alias('host_name')])
        
        return billing_data


    def _finalize_dataframe(
        self,
        billing_data_monthly_rollup,
        current_span,
        build_start_time,
        dates_processed,
        batches_processed,
        total_direct_records,
        total_indirect_records,
    ):
        """Finalize dataframe with deduplication and metrics."""
        total_build_duration = time.time() - build_start_time

        if billing_data_monthly_rollup is None or len(billing_data_monthly_rollup) == 0:
            add_span_attributes(
                current_span,
                **{
                    'dataframe.build.result': 'empty',
                    'dataframe.build.total_duration_seconds': total_build_duration,
                    'dataframe.build.dates_processed': dates_processed,
                    'dataframe.build.batches_processed': batches_processed,
                    'dataframe.build.total_direct_records': total_direct_records,
                    'dataframe.build.total_indirect_records': total_indirect_records,
                },
            )
            return self.empty()

        # Deduplicate any duplicate records from overlapping rollup files before returning
        # This ensures that identical records from multiple rollup files don't get double-counted
        dedup_start = time.time()
        pre_dedup_count = len(billing_data_monthly_rollup)
        billing_data_monthly_rollup = billing_data_monthly_rollup.reset_index()
        if not len(billing_data_monthly_rollup) == 0:
            # Remove duplicates based on unique index columns and recompute aggregations
            billing_data_monthly_rollup = billing_data_monthly_rollup.drop_duplicates(subset=self.unique_index_columns())
        post_dedup_count = len(billing_data_monthly_rollup)
        dedup_duration = time.time() - dedup_start

        # Final summary metrics
        add_span_attributes(
            current_span,
            **{
                'dataframe.build.result': 'success',
                'dataframe.build.total_duration_seconds': total_build_duration,
                'dataframe.build.dedup_duration_seconds': dedup_duration,
                'dataframe.build.dates_processed': dates_processed,
                'dataframe.build.batches_processed': batches_processed,
                'dataframe.build.total_direct_records': total_direct_records,
                'dataframe.build.total_indirect_records': total_indirect_records,
                'dataframe.build.pre_dedup_records': pre_dedup_count,
                'dataframe.build.final_records': post_dedup_count,
                'dataframe.build.duplicates_removed': pre_dedup_count - post_dedup_count,
                'dataframe.build.avg_batch_duration': total_build_duration / batches_processed if batches_processed > 0 else 0,
            },
        )

        # Log performance warnings
        if total_build_duration > 10.0:
            add_span_attributes(
                current_span,
                **{'dataframe.build.performance_warning': f'Build took {total_build_duration:.2f}s', 'dataframe.build.slow_operation': True},
            )

        return billing_data_monthly_rollup

    # Do the aggregation
    @traced_method('jobhost_summary.group')
    def group(self, dataframe):
        """Group and aggregate dataframe with performance tracking.
        
        TODO: Improve complex type aggregation to handle JSON strings and sets properly.
        Currently using simplified aggregation that may not properly merge complex data types.
        Need to implement proper JSON set merging and dict value combining for production use.
        """
        current_span = trace.get_current_span()

        start_time = time.time()
        input_count = len(dataframe) if dataframe is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.group.input_record_count': input_count,
                'dataframe.group.index_columns': len(self.unique_index_columns()),
                'dataframe.group.operation': 'initial_groupby',
            },
        )

        try:
            # Use proper aggregation for both numeric and complex data types
            # Follow demo_prompt_facts.md approach for facts aggregation
            from metrics_utility.automation_controller_billing.dataframe_engine.base import merge_and_stringify_facts, merge_list_arrays
            
            group = dataframe.group_by(self.unique_index_columns(), maintain_order=True).agg(
                [
                    # Numeric aggregations
                    pd.col('task_runs').sum().alias('task_runs'),
                    pd.col('host_name').count().alias('host_runs'),
                    
                    # CRITICAL: Filter null values before min/max aggregation to prevent inconsistent results
                    # This ensures that null values don't interfere with timestamp aggregation during initial CSV processing
                    pd.col('created').filter(pd.col('created').is_not_null()).min().alias('first_automation'),
                    pd.col('created').filter(pd.col('created').is_not_null()).max().alias('last_automation'),
                    pd.col('job_created').filter(pd.col('job_created').is_not_null()).max().alias('job_created'),
                    pd.col('managed_node_type').min().alias('managed_node_type'),
                    
                    # Complex type aggregations using native types
                    # Facts: Use native Struct aggregation - first() for single batch processing
                    pd.col('canonical_facts').filter(pd.col('canonical_facts').is_not_null()).first().alias('canonical_facts'),
                    pd.col('facts').filter(pd.col('facts').is_not_null()).first().alias('facts'),
                    
                    # Collections: Collect values into lists - handle both String and List inputs safely
                    pd.col('managed_node_type_string').filter(pd.col('managed_node_type_string').is_not_null()).map_batches(
                        lambda s: pd.Series([sorted(list(set([str(v) for v in s.to_list() if v is not None])))]),
                        return_dtype=pd.List(pd.Utf8)
                    ).first().alias('managed_node_types_list'),
                    pd.col('events').filter(pd.col('events').is_not_null()).map_batches(
                        lambda s: pd.Series([sorted(list(set([str(item) for sublist in s.to_list() if sublist is not None for item in (sublist if isinstance(sublist, list) else [sublist]) if item is not None])))]),
                        return_dtype=pd.List(pd.Utf8)
                    ).first().alias('events_list'), 
                    pd.col('host_names_before_dedup').filter(pd.col('host_names_before_dedup').is_not_null()).map_batches(
                        lambda s: pd.Series([sorted(list(set([str(v) for v in s.to_list() if v is not None])))]),
                        return_dtype=pd.List(pd.Utf8)
                    ).first().alias('host_names_list'),
                ]
            )

            # Rename the list columns to their final names (keep as native Lists)
            group = group.rename({
                'managed_node_types_list': 'managed_node_types_set',
                'events_list': 'events',
                'host_names_list': 'host_names_before_dedup',
            })
        except TypeError as e:
            # Log the error but don't try to fix it inline - let the schema system handle it
            add_span_attributes(current_span, **{'dataframe.group.type_error': str(e)})
            raise  # Re-raise to let the base class schema system handle the conversion properly

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
    @traced_method('jobhost_summary.regroup')
    def regroup(self, dataframe):
        """Regroup pre-aggregated dataframe with performance tracking.
        
        TODO: Implement complex type merging to match operations() method functionality.
        Currently using simplified aggregation for JSON strings and sets.
        Should implement proper 'combine_set' and 'combine_json_values' operations.
        """
        current_span = trace.get_current_span()

        start_time = time.time()
        input_count = len(dataframe) if dataframe is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.regroup.input_record_count': input_count,
                'dataframe.regroup.index_columns': len(self.unique_index_columns()),
                'dataframe.regroup.operation': 'regroup_after_dedup',
            },
        )

        # Use proper merging for complex types during rollup aggregation
        from metrics_utility.automation_controller_billing.dataframe_engine.base import merge_and_stringify_facts, merge_list_arrays
        
        result = dataframe.group_by(self.unique_index_columns(), maintain_order=True).agg(
            [
                # Numeric aggregations
                pd.col('task_runs').sum().alias('task_runs'),
                pd.col('host_runs').sum().alias('host_runs'),  # Sum pre-computed host_runs values from rollups
                
                # CRITICAL: Filter null values before min/max aggregation to prevent inconsistent results
                # This ensures that null values don't interfere with timestamp aggregation
                pd.col('first_automation').filter(pd.col('first_automation').is_not_null()).min().alias('first_automation'),
                pd.col('last_automation').filter(pd.col('last_automation').is_not_null()).max().alias('last_automation'),
                pd.col('job_created').filter(pd.col('job_created').is_not_null()).max().alias('job_created'),
                pd.col('managed_node_type').min().alias('managed_node_type'),
                
                # Complex type aggregations using native types
                # Facts: Use native Struct merging
                pd.col('canonical_facts').filter(pd.col('canonical_facts').is_not_null()).first().alias('canonical_facts'),
                pd.col('facts').filter(pd.col('facts').is_not_null()).first().alias('facts'),
                
                # Collections: Use native List merging safely
                pd.col('managed_node_types_set').map_batches(
                    lambda s: pd.Series([sorted(list(set([str(item) for sublist in s.to_list() if sublist is not None for item in (sublist if isinstance(sublist, list) else [sublist]) if item is not None])))]),
                    return_dtype=pd.List(pd.Utf8)
                ).first().alias('managed_node_types_set'),
                
                pd.col('events').map_batches(
                    lambda s: pd.Series([sorted(list(set([str(item) for sublist in s.to_list() if sublist is not None for item in (sublist if isinstance(sublist, list) else [sublist]) if item is not None])))]),
                    return_dtype=pd.List(pd.Utf8)
                ).first().alias('events'),
                
                pd.col('host_names_before_dedup').map_batches(
                    lambda s: pd.Series([sorted(list(set([str(item) for sublist in s.to_list() if sublist is not None for item in (sublist if isinstance(sublist, list) else [sublist]) if item is not None])))]),
                    return_dtype=pd.List(pd.Utf8)
                ).first().alias('host_names_before_dedup'),
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




    def dedup(self, dataframe, hostname_mapping=None, scope_dataframe=None):
        """
        Override dedup method to enrich canonical facts and facts from scope_dataframe
        when experimental deduplication is enabled.
        """
        if dataframe is None or len(dataframe) == 0:
            return self.empty()

        if not hostname_mapping:
            return dataframe

        # Enrich direct managed nodes with canonical facts and facts from scope data
        # when experimental deduplication is enabled
        experimental_dedup = self.extra_params.get('deduplicator') == 'ccsp-experimental'

        if experimental_dedup and scope_dataframe is not None and len(scope_dataframe) > 0:
            # Create a mapping from host_name to canonical_facts and facts
            if 'canonical_facts' in scope_dataframe.columns and 'facts' in scope_dataframe.columns:
                # Filter to only direct managed nodes for enrichment
                direct_mask = dataframe['managed_node_type'] == DIRECT  # DIRECT = 0

                # Use Polars DataFrame operations
                # Check if there are any direct nodes
                if dataframe.filter(direct_mask).shape[0] > 0:
                    # Create a mapping from scope dataframe using Polars
                    scope_mapping = {}
                    for row in scope_dataframe.iter_rows(named=True):
                        # Keep facts as lists to match dataframe_schema expectations
                        canonical_facts_list = row.get('canonical_facts', [])
                        facts_list = row.get('facts', [])
                        
                        scope_mapping[row['host_name']] = {'canonical_facts': canonical_facts_list, 'facts': facts_list}

                    # Update canonical_facts and facts for direct managed nodes using native Polars operations
                    # Keep everything in native List format to maintain schema consistency
                    try:
                        # Create mapping DataFrame with native List types matching dataframe_schema
                        mapping_data = []
                        for host_name, data in scope_mapping.items():
                            mapping_data.append({
                                'host_name': host_name, 
                                'scope_canonical_facts': data.get('canonical_facts', []), 
                                'scope_facts': data.get('facts', [])
                            })

                        if mapping_data:
                            scope_mapping_df = pd.DataFrame(mapping_data)
                            
                            # Ensure scope mapping dataframe has correct schema
                            scope_mapping_df = self.apply_complete_schema(
                                scope_mapping_df, 
                                schema_type="dataframe", 
                                operation_context="scope_mapping_enrichment"
                            )

                            # Join with scope mapping and conditionally update based on managed_node_type
                            dataframe = dataframe.join(scope_mapping_df, on='host_name', how='left')

                            # Update canonical_facts for direct managed nodes only using native List operations
                            dataframe = dataframe.with_columns([
                                pd.when(pd.col('managed_node_type') == DIRECT)
                                .then(pd.col('scope_canonical_facts'))
                                .otherwise(pd.col('canonical_facts'))
                                .alias('canonical_facts')
                            ])

                            # Update facts for direct managed nodes only using native List operations  
                            dataframe = dataframe.with_columns([
                                pd.when(pd.col('managed_node_type') == DIRECT)
                                .then(pd.col('scope_facts'))
                                .otherwise(pd.col('facts'))
                                .alias('facts')
                            ])

                            # Remove temporary join columns
                            dataframe = dataframe.drop(['scope_canonical_facts', 'scope_facts'])
                            
                            # Apply schema to ensure consistency after join operations
                            dataframe = self.apply_complete_schema(
                                dataframe, 
                                schema_type="dataframe", 
                                operation_context="after_scope_enrichment"
                            )
                            
                    except Exception as join_error:
                        # Fallback: skip enrichment if join approach fails  
                        print(f'Warning: Scope enrichment join failed: {join_error}, skipping enrichment')
                        pass

        # Call the parent dedup method to perform the actual deduplication
        return super().dedup(dataframe, hostname_mapping)
