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

    def group(self, dataframe):
        """Group collection status dataframe by unique index columns."""
        if dataframe is None or len(dataframe) == 0:
            return self.empty()

        # Use proper aggregation based on initial_aggregations() method to avoid data loss
        # Build aggregation expressions based on the defined aggregation rules
        agg_exprs = []
        initial_aggs = self.initial_aggregations()

        for col in self.data_columns():
            agg_rule = initial_aggs.get(col)
            if agg_rule == 'sum':
                agg_exprs.append(pd.col(col).sum().alias(col))

        group = dataframe.group_by(self.unique_index_columns(), maintain_order=True).agg(agg_exprs)

        # Schema application will be handled by base class _group_with_schema
        return group

    # Merge pre-aggregated
    @traced_method('collection_status.regroup')
    def regroup(self, dataframe):
        """Regroup pre-aggregated collection status dataframe with performance tracking."""
        current_span = trace.get_current_span()

        start_time = time.time()
        input_count = len(dataframe) if dataframe is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.regroup.input_record_count': input_count,
                'dataframe.regroup.index_columns': len(self.unique_index_columns()),
                'dataframe.regroup.operation': 'collection_status_regroup_after_dedup',
            },
        )

        result = dataframe.group_by(self.unique_index_columns(), maintain_order=True).agg(
            [
                pd.col('elapsed').sum().alias('elapsed'),  # Sum elapsed time across dates
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
        return ['collection_start_timestamp', 'since', 'until', 'file_name', 'status']

    @staticmethod
    def data_columns():
        """Define data columns that need aggregation when grouping records."""
        return ['elapsed']

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
            'elapsed': 'sum',  # Sum elapsed time across duplicate records
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
