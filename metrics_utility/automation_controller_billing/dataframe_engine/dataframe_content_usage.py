"""Dataframe engine for processing content usage data from main_jobevent CSV.

This module provides comprehensive processing capabilities for content usage
tracking including module names, collections, roles, and task execution data.

Key Features:
    - Module name and collection extraction from task actions
    - Role name processing with regex patterns
    - Task execution duration tracking
    - Schema-driven data validation and processing
    - OpenTelemetry performance tracing integration

Usage:
    from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_content_usage import DataframeContentUsage

    engine = DataframeContentUsage()
    result = engine.build_dataframe(batch_iterator)
"""

import re
import time
from typing import Any, Dict

import polars as pd
import pyarrow as pa

from opentelemetry import trace

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base
from metrics_utility.tracing import add_span_attributes, traced_method


class DataframeContentUsage(Base):
    """Processes content usage data from main_jobevent CSV for AAP Controller billing.

    This class handles the extraction, validation, aggregation, and storage of content
    usage data including module names, collections, roles, and task execution metrics.
    It processes data in batches for memory efficiency and provides comprehensive
    task execution tracking.

    All data processing follows a strict schema-driven pipeline:
    1. Raw CSV validation using collector_schema
    2. Business logic transformations (module/role extraction)
    3. Conversion to collector_dataframe_schema format
    4. Grouping and aggregation with dataframe_schema
    5. Storage with parquet_schema

    Attributes:
        extra_params: Additional configuration parameters from base class

    Example:
        >>> engine = DataframeContentUsage()
        >>> batch_iterator = get_batch_data_iterator(date_range)
        >>> result_df = engine.build_dataframe(batch_iterator)
        >>> print(f"Processed {len(result_df)} content usage records")
    """

    # ========================================
    # SCHEMA DEFINITIONS - PIPELINE ORDER
    # ========================================

    @staticmethod
    def collector_schema() -> pa.Schema:
        """Define PyArrow schema for raw CSV collector data validation.
        
        This schema validates raw CSV data from the main_jobevent table,
        used for content usage tracking.
        
        Returns:
            PyArrow schema for main_jobevent CSV data validation
        """
        return pa.schema([
            # Core identification columns
            pa.field("host_name", pa.string()),
            pa.field("job_remote_id", pa.int64()),
            
            # Task action data (for processing into module/collection names)
            pa.field("task_action", pa.string()),
            pa.field("resolved_action", pa.string()),
            pa.field("resolved_role", pa.string()), 
            pa.field("role", pa.string()),
            
            # Performance data
            pa.field("duration", pa.float64()),
        ])

    @staticmethod
    def collector_dataframe_schema() -> Dict[str, str]:
        """Define Polars dataframe schema for processed CSV data (before grouping).
        
        This schema is applied after CSV processing but before the group() method.
        All columns are in their final types ready for aggregation operations.
        
        Returns:
            Dictionary mapping column names to Polars dtypes as strings
        """
        return {
            # Index columns (unique identifiers)
            'host_name': 'String',
            'module_name': 'String',
            'collection_name': 'String', 
            'role_name': 'String',
            'install_uuid': 'String',
            'job_remote_id': 'Int64',
            
            # Data columns
            'task_runs': 'Int64',
            'duration': 'Float64',
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
            'host_name': 'String',
            'module_name': 'String',
            'collection_name': 'String',
            'role_name': 'String', 
            'install_uuid': 'String',
            'job_remote_id': 'Int64',
            
            # Aggregated data columns
            'task_runs': 'Int64',
            'duration': 'Float64',
        }

    @staticmethod
    def parquet_schema() -> pa.Schema:
        """Define PyArrow schema for aggregated rollup data validation.
        
        This schema is used for validating aggregated content usage data during
        rollup merging operations. It includes only the final aggregated columns
        after group_by operations.
        
        Returns:
            PyArrow schema for rollup data validation
        """
        return pa.schema([
            # Index columns (unique identifiers)
            pa.field("host_name", pa.string()),
            pa.field("module_name", pa.string()),
            pa.field("collection_name", pa.string()),
            pa.field("role_name", pa.string()),
            pa.field("install_uuid", pa.string()),
            pa.field("job_remote_id", pa.int64()),
            
            # Aggregated data columns
            pa.field("task_runs", pa.int64()),
            pa.field("duration", pa.float64()),
        ])

    def get_collector_default_values(self) -> Dict[str, Any]:
        """Get custom default values for collector schema columns.
        
        These defaults override the standard type-based defaults for domain-specific
        requirements for content usage processing.
        
        Returns:
            Dictionary mapping column names to custom default values
        """
        return {
            'host_name': '',
            'task_action': '',
            'resolved_action': '',
            'resolved_role': '',
            'role': '',
            'module_name': '',
            'collection_name': 'No collection used',
            'role_name': 'No role used',
            'install_uuid': '',
            'job_remote_id': 0,
            'duration': 0.0,
            'task_runs': 1,  # Each record represents one task
        }

    def get_rollup_default_values(self) -> Dict[str, Any]:
        """Get custom default values for rollup schema columns.
        
        Returns:
            Dictionary mapping column names to custom default values
        """
        return {
            'host_name': '',
            'module_name': '',
            'collection_name': 'No collection used',
            'role_name': 'No role used',
            'install_uuid': '',
            'job_remote_id': 0,
            'task_runs': 0,
            'duration': 0.0,
        }

    @staticmethod
    def collector_dataframe_validation_schema() -> Dict[str, Dict[str, Any]]:
        """Define validation rules for collector dataframe columns.
        
        This schema specifies which columns are required, which can be null,
        and validation rules extracted from the old inline validation logic.
        
        Returns:
            Dictionary mapping column names to validation rule dictionaries
        """
        return {
            # Required identification columns that cannot be null
            'host_name': {'required': True, 'allow_null': False},
            'module_name': {'required': True, 'allow_null': False},  # Renamed from task_action
            'job_remote_id': {'required': True, 'allow_null': False, 'min_value': 1},
            
            # Optional columns that can be null
            'collection_name': {'required': False, 'allow_null': True},
            'role_name': {'required': False, 'allow_null': True},
            'install_uuid': {'required': False, 'allow_null': True},
            
            # Numeric columns with range validation
            'duration': {'required': True, 'allow_null': True, 'min_value': 0.0},
            'task_runs': {'required': True, 'allow_null': False, 'min_value': 1},  # Each record represents at least one task
        }

    # ========================================
    # STATIC CONFIGURATION METHODS
    # ========================================

    @staticmethod
    def unique_index_columns():
        """Define columns that uniquely identify a record for grouping/deduplication."""
        return ['host_name', 'module_name', 'collection_name', 'role_name', 'install_uuid', 'job_remote_id']

    @staticmethod
    def data_columns():
        """Define data columns that need aggregation when grouping records."""
        return ['task_runs', 'duration']

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
            'task_runs': 'count',  # Count the task runs (each row represents one task)
            'duration': 'sum',  # Sum duration across duplicate records
        }

    @staticmethod
    def operations():
        """Define how to merge rollup data when combining multiple rollup files.
        
        This is used by the summarize_merged_dataframes() method in the base class
        when resolving conflicts from join operations during rollup merging.
        
        Returns:
            dict: Mapping of column_name -> operation for resolving merge conflicts
        """
        return {
            # Index columns - should be identical but use min as safe fallback
            'host_name': 'min',
            'module_name': 'min',
            'collection_name': 'min',
            'role_name': 'min',
            'install_uuid': 'min',
            'job_remote_id': 'min',
            # Data columns - sum the counts and durations
            'task_runs': 'sum',
            'duration': 'sum',
        }

    # ========================================
    # BUSINESS LOGIC METHODS
    # ========================================
    def _process_batch_data_with_schema(self, batch_data, current_span):
        """Process batch data and apply collector_dataframe_schema (BEFORE grouping).
        
        Override base class method to avoid double schema application since
        processing already applies the schema.
        """
        processed_data = self._process_batch_data(batch_data, current_span)
        if processed_data is None or len(processed_data) == 0:
            return self.empty()
        # Schema already applied in processing, no need to apply again
        return processed_data

    def _process_batch_data(self, batch_data, current_span):
        """Process individual batch data using centralized schema-driven approach.
        
        This method uses the new validation system:
        1. CSV validation using collector_schema
        2. Business logic transformations (module/role extraction) 
        3. Schema application with automatic validation via collector_dataframe_validation_schema
        
        All validation, type casting, and column completion is handled by the 
        centralized schema system in the base class.
        """
        # Get main_jobevent data from this batch
        events = batch_data.get('main_jobevent')
        if events is None or len(events) == 0:
            return self.empty()

        # Step 1: Validate CSV data against collector schema
        events = self.validate_collector_data(
            events,
            strict_columns=['host_name', 'job_remote_id'], 
            default_values=self.get_collector_default_values()
        )
        
        # Step 2: Add core metadata columns
        events = events.with_columns(pd.lit(batch_data['config']['install_uuid']).alias('install_uuid'))
        
        # Step 3: Apply business logic transformations
        events = self._process_content_transformations(events)
        
        # Step 4: Apply complete collector_dataframe schema (includes validation)
        events = self.apply_complete_schema(events, schema_type="collector_dataframe", operation_context="after_content_transformations")

        return events

    def _process_content_transformations(self, events):
        """Apply business logic transformations for content usage processing."""
        # If resolved_action and resolved_role are not there, fill them with task_action and role
        events = events.with_columns([
            events['resolved_action'].fill_null(events['task_action']).alias('task_action'),
            events['resolved_role'].fill_null(events['role']).alias('role'),
        ])

        # Extract role names and collection names using regex
        events = events.with_columns(
            events['role'].map_elements(lambda x: self.extract_role_name(x), return_dtype=pd.Utf8).alias('role')
        )

        # Rename columns to match reality - they are processed names, not raw columns anymore
        events = events.rename({'task_action': 'module_name', 'role': 'role_name'})

        # Extract collection names from module names
        events = events.with_columns(
            events['module_name'].map_elements(self.extract_collection_name, return_dtype=pd.Utf8).alias('collection_name')
        )

        # Filter out records with missing module names (required for valid content usage records)
        events = events.filter(events['module_name'].is_not_null())

        # Set human readable values for missing role and collection names
        events = events.with_columns([
            events['role_name'].fill_null('No role used').alias('role_name'),
            events['collection_name'].fill_null('No collection used').alias('collection_name'),
        ])

        # Add task_runs column (each record represents one task)
        events = events.with_columns(pd.lit(1).alias('task_runs'))

        return events


    # ========================================
    # AGGREGATION METHODS
    # ========================================

    @traced_method('content_usage.group')
    def group(self, dataframe):
        """Group and aggregate content usage dataframe with performance tracking."""
        current_span = trace.get_current_span()

        start_time = time.time()
        input_count = len(dataframe) if dataframe is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.group.input_record_count': input_count,
                'dataframe.group.index_columns': len(self.unique_index_columns()),
                'dataframe.group.operation': 'content_usage_groupby',
            },
        )

        # Use proper aggregation based on initial_aggregations() method
        result = dataframe.group_by(self.unique_index_columns(), maintain_order=True).agg([
            pd.col('module_name').count().alias('task_runs'),  # Count tasks (each row represents one task)
            pd.col('duration').sum().alias('duration'),  # Sum duration across duplicate records
        ])

        # Duration is null in older versions of Controller - handle with schema defaults
        result = result.with_columns(result['duration'].fill_null(0).alias('duration'))

        duration = time.time() - start_time
        output_count = len(result) if result is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.group.duration_seconds': duration,
                'dataframe.group.output_record_count': output_count,
                'dataframe.group.compression_ratio': (input_count - output_count) / input_count if input_count > 0 else 0,
                'dataframe.group.records_aggregated': input_count - output_count,
            },
        )

        if duration > 1.0:
            add_span_attributes(
                current_span, **{'dataframe.group.slow_operation': True, 'dataframe.group.performance_warning': f'Group took {duration:.2f}s'}
            )

        return result

    @traced_method('content_usage.regroup')
    def regroup(self, dataframe):
        """Regroup pre-aggregated content usage dataframe with performance tracking."""
        current_span = trace.get_current_span()

        start_time = time.time()
        input_count = len(dataframe) if dataframe is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.regroup.input_record_count': input_count,
                'dataframe.regroup.index_columns': len(self.unique_index_columns()),
                'dataframe.regroup.operation': 'content_usage_regroup_after_dedup',
            },
        )

        result = dataframe.group_by(self.unique_index_columns(), maintain_order=True).agg([
            pd.col('task_runs').sum().alias('task_runs'),
            pd.col('duration').sum().alias('duration'),
        ])

        duration = time.time() - start_time
        output_count = len(result) if result is not None else 0

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

    # ========================================
    # UTILITY METHODS
    # ========================================

    @staticmethod
    def collection_regexp():
        return r'^(\w+)\.(\w+)\.((\w+)(\.|$))+'

    @staticmethod
    def standalone_role_regexp():
        return r'^(\w+)\.(\w+)$'

    @staticmethod
    def extract_collection_name(x):
        if x is None:
            return None

        m = re.match(DataframeContentUsage.collection_regexp(), x)

        if m:
            return f'{m.groups()[0]}.{m.groups()[1]}'
        else:
            return None

    @staticmethod
    def extract_role_name(x):
        if x is None:
            return None

        collection_role = re.match(DataframeContentUsage.collection_regexp(), x)
        standalone_role = re.match(DataframeContentUsage.standalone_role_regexp(), x)

        if collection_role:
            return f'{collection_role.groups()[0]}.{collection_role.groups()[1]}.{collection_role.groups()[2]}'
        elif standalone_role:
            return f'{standalone_role.groups()[0]}.{standalone_role.groups()[1]}'
        else:
            return None

