import time
from typing import Any, Dict

import polars as pd
import pyarrow as pa

from opentelemetry import trace

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base
from metrics_utility.tracing import add_span_attributes, traced_method


# dataframe for data_collection_status
class DataframeCollectionStatus(Base):
    @traced_method('collection_status.build_dataframe')
    def build_dataframe(self, batch_data_iterator):
        """Build Collection Status dataframe by processing batch data iterator.

        Args:
            batch_data_iterator: Iterator yielding batch_data from CSV scanning
                               Each batch_data: {'data_collection_status': DataFrame, ...}

        Returns:
            Concatenated dataframe containing all processed batches
        """
        current_span = trace.get_current_span()
        build_start_time = time.time()
        total_records = 0
        groups_processed = 0

        add_span_attributes(current_span, **{'dataframe.build.dataframe_type': 'CollectionStatus', 'dataframe.build.mode': 'batch_iterator'})

        # Initialize accumulated dataframe (concat-based, no aggregation)
        accumulated_dataframe = None

        # Process each batch from the iterator
        for batch_data in batch_data_iterator:
            # Get data_collection_status data from this batch
            batch = batch_data.get('data_collection_status')
            if batch is None or len(batch) == 0:
                continue

            # Process this batch into a group (consistent with other dataframes)
            date = batch_data.get('_date_context')  # Get date from context
            group_dataframe = self._process_batch_data(batch, batch_data, date)
            if group_dataframe is None or len(group_dataframe) == 0:
                continue

            # Apply rollup schema validation after processing and grouping
            group_dataframe = self.validate_rollup_data(
                group_dataframe, strict_columns=['file_name', 'status'], default_values=self.get_rollup_default_values()
            )

            # Merge with accumulated dataframe (consistent with other dataframes)
            if accumulated_dataframe is None:
                accumulated_dataframe = group_dataframe
            else:
                accumulated_dataframe = self.merge(accumulated_dataframe, group_dataframe)

            total_records += len(group_dataframe)
            groups_processed += 1

        build_duration = time.time() - build_start_time
        final_count = len(accumulated_dataframe) if accumulated_dataframe is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.build.duration_seconds': build_duration,
                'dataframe.build.groups_processed': groups_processed,
                'dataframe.build.total_input_records': total_records,
                'dataframe.build.final_record_count': final_count,
            },
        )

        return accumulated_dataframe if accumulated_dataframe is not None else self.empty()

    def _process_batch_data(self, batch, batch_data, date):
        """Process individual batch data using centralized schema-driven approach.

        This method uses the new validation system:
        1. CSV validation using collector_schema
        2. Business logic transformations (minimal for collection status)
        3. Schema application with automatic validation via collector_dataframe_validation_schema

        All validation, type casting, and column completion is handled by the
        centralized schema system in the base class.
        """
        from metrics_utility.tracing import add_span_attributes
        from opentelemetry import trace

        current_span = trace.get_current_span()

        # Handle empty DataFrame case
        if batch is None or len(batch) == 0:
            return None

        input_row_count = len(batch)

        # Step 1: Validate CSV data against collector schema
        batch = self.validate_collector_data(batch, strict_columns=['file_name', 'status'], default_values=self.get_collector_default_values())

        # Step 2: Apply complete collector_dataframe schema (includes validation)
        batch = self.apply_complete_schema(batch, schema_type='collector_dataframe', operation_context='after_collection_status_transformations')

        # Add validation metrics for tracing
        final_count = len(batch) if batch is not None else 0
        add_span_attributes(
            current_span,
            **{
                f'data_quality.{date.isoformat()}.input_rows': input_row_count,
                f'data_quality.{date.isoformat()}.valid_rows': final_count,
            },
        )

        # Do the aggregation (consistent with other dataframes)
        batch_group = self.group(batch)
        return batch_group

    @traced_method('collection_status.group')
    def group(self, dataframe):
        """Group collection status dataframe by unique index columns with proper aggregation."""
        current_span = trace.get_current_span()
        
        if dataframe is None or len(dataframe) == 0:
            return self.empty()

        # Apply column mapping if needed (for source CSV with different column names)
        dataframe = self._apply_column_mapping(dataframe)

        # Use proper aggregation for collection status data
        from metrics_utility.automation_controller_billing.dataframe_engine.base import build_aggregation_expressions
        
        agg_exprs = build_aggregation_expressions(self.group_aggregations())
        
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

        # Schema application will be handled by base class _group_with_schema
        return group

    def _apply_column_mapping(self, dataframe):
        """Apply column mapping from source CSV format to expected format."""
        # Mapping from source CSV columns to expected DataframeCollectionStatus columns
        column_mapping = {
            'report_uuid': 'collection_start_timestamp',
            'created': 'since', 
            'modified': 'until',
            'collection_type': 'file_name',
            'collector_name': 'status',
            'status': 'elapsed',
        }
        
        # Check if dataframe needs mapping (has source column names)
        current_columns = set(dataframe.columns)
        source_columns = set(column_mapping.keys())
        
        # If dataframe has source column structure, apply mapping
        if source_columns.issubset(current_columns):
            # Rename columns according to mapping
            dataframe = dataframe.rename(column_mapping)
            
        return dataframe

    # Merge pre-aggregated
    @traced_method('collection_status.regroup')
    def regroup(self, dataframe):
        """Regroup collection status dataframe with performance tracking and proper aggregation."""
        current_span = trace.get_current_span()

        start_time = time.time()
        input_count = len(dataframe) if dataframe is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.regroup.input_record_count': input_count,
                'dataframe.regroup.operation': 'collection_status_proper_aggregation',
            },
        )

        # Use proper aggregation for regrouping collection status data
        from metrics_utility.automation_controller_billing.dataframe_engine.base import build_aggregation_expressions
        
        regroup_exprs = build_aggregation_expressions(self.regroup_aggregations())
        
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

        return result

    def merge(self, rollup, new_group):
        """Use standard merge behavior with proper aggregation."""
        # Use the base class merge method which includes proper aggregation via regroup()
        return super().merge(rollup, new_group)

    @staticmethod
    def unique_index_columns():
        return ['collection_start_timestamp', 'since', 'until', 'file_name', 'status']

    @staticmethod
    def data_columns():
        """Define data columns that need aggregation when grouping records."""
        return ['elapsed']

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
            'elapsed': 'sum',  # Sum elapsed time across duplicate records
        }

    @staticmethod
    def regroup_aggregations():
        """Define how to aggregate rollup data when combining multiple rollup files during regroup operations.

        This is used in the regroup() method when merging pre-aggregated data from different batches/files.
        
        Returns:
            dict: Mapping of column_name -> aggregation_name for centralized aggregation system
        """
        return {
            'elapsed': 'sum',  # Sum elapsed time across rollups
        }

    @staticmethod
    def collector_dataframe_validation_schema() -> Dict[str, Dict[str, Any]]:
        """Define validation rules for collector dataframe columns.

        This schema specifies which columns are required, which can be null,
        and validation rules for collection status data processing.

        Returns:
            Dictionary mapping column names to validation rule dictionaries
        """
        return {
            # Required identification columns that cannot be null
            'file_name': {'required': True, 'allow_null': False},
            'status': {'required': True, 'allow_null': False},
            # Optional columns that can be null
            'collection_start_timestamp': {'required': False, 'allow_null': True},
            'since': {'required': False, 'allow_null': True},
            'until': {'required': False, 'allow_null': True},
            # Numeric columns with validation
            'elapsed': {'required': False, 'allow_null': True, 'min_value': 0.0},
        }

    @staticmethod
    def collector_schema() -> pa.Schema:
        """Define PyArrow schema for raw CSV collector data validation.

        This schema is used for validating data_collection_status CSV data during
        initial collection and processing. It includes all columns that may appear
        in the raw CSV files for collection status tracking.

        Returns:
            PyArrow schema for collector data validation
        """
        return pa.schema(
            [
                # Index columns (unique identifiers)
                pa.field('collection_start_timestamp', pa.string()),  # Keep as string for Polars compatibility
                pa.field('since', pa.string()),  # Keep as string for Polars compatibility
                pa.field('until', pa.string()),  # Keep as string for Polars compatibility
                pa.field('file_name', pa.string()),
                pa.field('status', pa.string()),
                # Data columns
                pa.field('elapsed', pa.float64()),
            ]
        )

    @staticmethod
    def parquet_schema() -> pa.Schema:
        """Define PyArrow schema for aggregated rollup data validation.

        This schema is used for validating aggregated collection status data during
        rollup merging operations. It includes only the final aggregated columns
        after group_by operations.

        Returns:
            PyArrow schema for rollup data validation
        """
        return pa.schema(
            [
                # Index columns (unique identifiers)
                pa.field('collection_start_timestamp', pa.string()),  # Keep as string for Polars compatibility
                pa.field('since', pa.string()),  # Keep as string for Polars compatibility
                pa.field('until', pa.string()),  # Keep as string for Polars compatibility
                pa.field('file_name', pa.string()),
                pa.field('status', pa.string()),
                # Aggregated data columns
                pa.field('elapsed', pa.float64()),
            ]
        )

    def get_collector_default_values(self) -> Dict[str, Any]:
        """Get custom default values for collector schema columns.

        These defaults override the standard type-based defaults for domain-specific
        requirements for collection status processing.

        Returns:
            Dictionary mapping column names to custom default values
        """
        return {
            'collection_start_timestamp': '',
            'since': '',
            'until': '',
            'file_name': '',
            'status': '',
            'elapsed': 0.0,
        }

    def get_rollup_default_values(self) -> Dict[str, Any]:
        """Get custom default values for rollup schema columns.

        Returns:
            Dictionary mapping column names to custom default values
        """
        return {
            'collection_start_timestamp': '',
            'since': '',
            'until': '',
            'file_name': '',
            'status': '',
            'elapsed': 0.0,
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
            'elapsed': 'sum',  # Sum elapsed time when merging rollups
        }

    @staticmethod
    def collector_dataframe_schema() -> Dict[str, str]:
        """Define Polars dataframe schema for processed CSV data (before grouping).

        Returns:
            Dictionary mapping column names to Polars dtypes as strings
        """
        return {
            # Index/metadata columns
            'collection_start_timestamp': 'String',  # Keep as string for consistency
            'since': 'String',
            'until': 'String',
            'file_name': 'String',
            'status': 'String',
            # Data columns
            'elapsed': 'Float64',
        }

    @staticmethod
    def dataframe_schema() -> Dict[str, str]:
        """Define Polars dataframe schema for working dataframes (after grouping).

        Returns:
            Dictionary mapping column names to Polars dtypes as strings
        """
        return {
            # Index/metadata columns
            'collection_start_timestamp': 'String',  # Keep as string for consistency
            'since': 'String',
            'until': 'String',
            'file_name': 'String',
            'status': 'String',
            # Aggregated data columns
            'elapsed': 'Float64',
        }

    @traced_method('collection_status.build_group')
    def build_group(self, batch_data):
        """Build Collection Status dataframe group from a single tarball's CSV data.

        Args:
            batch_data: Data from a single tarball extraction
                       Format: {'data_collection_status': DataFrame, 'main_host': DataFrame, ...}
        """
        # Get data_collection_status data from this batch
        batch = batch_data.get('data_collection_status')
        if batch is None or len(batch) == 0:
            return self.empty()

        # Process the single batch using existing logic
        date = batch_data.get('_date_context')  # Get date from context
        processed_data = self._process_batch_data(batch, batch_data, date)
        return processed_data if processed_data is not None else self.empty()
