import time

import pandas as pd

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
            if batch is None or batch.empty:
                continue

            # Process this batch into a group (consistent with other dataframes)
            date = batch_data.get('_date_context')  # Get date from context
            group_dataframe = self._process_batch_data(batch, batch_data, date)
            if group_dataframe is None or group_dataframe.empty:
                continue

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
        """Process individual batch data with datetime parsing and grouping."""
        # DateTime parsing operations - often slow with large datasets
        batch['collection_start_timestamp'] = pd.to_datetime(batch['collection_start_timestamp'], format='ISO8601').dt.tz_localize(None)
        batch['since'] = pd.to_datetime(batch['since'], format='ISO8601').dt.tz_localize(None)
        batch['until'] = pd.to_datetime(batch['until'], format='ISO8601').dt.tz_localize(None)

        # Do the aggregation (consistent with other dataframes)
        batch_group = self.group(batch)
        return batch_group

    def group(self, dataframe):
        """Group collection status dataframe by unique index columns."""
        if dataframe is None or dataframe.empty:
            return self.empty()

        # For collection status, we group by unique_index_columns and sum elapsed time
        group = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            elapsed=('elapsed', 'sum')  # Sum elapsed time for identical entries
        )

        # Cast types to match the table
        result = self.cast_dataframe(group, self.cast_types())
        return result

    @staticmethod
    def unique_index_columns():
        return ['collection_start_timestamp', 'since', 'until', 'file_name', 'status']

    @staticmethod
    def data_columns():
        return ['elapsed']

    @staticmethod
    def cast_types():
        return {
            'elapsed': float,
        }

    @staticmethod
    def index_cast_types():
        """Return casting types for index columns (unique_index_columns)."""
        return {
            'collection_start_timestamp': 'datetime64[ns]',
            'since': 'datetime64[ns]',
            'until': 'datetime64[ns]',
            'file_name': str,
            'status': str,
        }

    @staticmethod
    def operations():
        return {
            'elapsed': 'sum',  # Sum elapsed time when merging rollups
        }

    @staticmethod
    def raw_data_cast_types():
        """Return casting types for raw data columns (before grouping)."""
        return {
            'elapsed': float,
            # Index columns - ensure datetime columns are consistently cast
            'collection_start_timestamp': 'datetime64[ns]',
            'since': 'datetime64[ns]',
            'until': 'datetime64[ns]',
            'file_name': str,
            'status': str,
        }

    @staticmethod
    def operations():
        return {
            'elapsed': 'max',  # Use max for elapsed times
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
        if batch is None or batch.empty:
            return self.empty()

        # Process the batch and return it (no aggregation)
        result = self._process_batch_data(batch, batch_data)
        return result.reset_index(drop=True) if result is not None else self.empty()
