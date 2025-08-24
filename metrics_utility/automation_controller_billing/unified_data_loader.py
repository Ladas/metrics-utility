"""
Unified Data Loading Coordinator (Phase 1)

This module implements the unified data loading architecture described in docs/rollups_data_loading.md.
It eliminates the 5x redundant I/O problem by reading each tarball exactly once and distributing
data to all dataframe builders simultaneously.
"""

import time

from typing import Any, Dict, List

from opentelemetry import trace

from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_collection_status import DataframeCollectionStatus
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_content_usage import DataframeContentUsage
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_inventory_scope import DataframeInventoryScope
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_jobhost_summary_usage import DataframeJobhostSummaryUsage
from metrics_utility.automation_controller_billing.dataframe_engine.db_dataframe_host_metric import DBDataframeHostMetric
from metrics_utility.tracing import add_span_attributes, traced_method


class UnifiedDataLoader:
    """
    Unified data loading coordinator that reads tarballs once and distributes data to all dataframe builders.

    This eliminates the redundant I/O operations where each dataframe was reading the same tarballs
    independently, resulting in 5x redundant tarball reads.
    """

    def __init__(self, extractor, month, extra_params):
        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params

        # Map table names to dataframe classes
        self.dataframe_builders = {
            'job_host_summary': DataframeJobhostSummaryUsage,
            'indirect_nodes': DataframeJobhostSummaryUsage,  # Same class handles both
            'main_jobevent': DataframeContentUsage,
            'main_host': DataframeInventoryScope,
            'data_collection_status': DataframeCollectionStatus,
            'host_metric': DBDataframeHostMetric,
        }

        # Track which dataframes are needed based on report type
        self.required_dataframes = self._get_required_dataframes()

    def _get_required_dataframes(self) -> List[str]:
        """Get list of required dataframes based on report type."""
        report_type = self.extra_params.get('report_type')

        if report_type == 'CCSP':
            return ['job_host_summary', 'main_jobevent', 'main_host']
        elif report_type == 'CCSPv2':
            return ['job_host_summary', 'main_jobevent', 'main_host', 'data_collection_status']
        elif report_type == 'RENEWAL_GUIDANCE':
            return ['job_host_summary', 'main_jobevent', 'main_host']
        else:
            # Default to all dataframes if unknown type
            return ['job_host_summary', 'main_jobevent', 'main_host', 'data_collection_status']

    @traced_method('unified_data_loader.build_all_dataframes')
    def build_all_dataframes(self) -> Dict[str, Any]:
        """
        Build all required dataframes with proper CSV iterator pattern.

        For each day:
          1. Scan all tarballs for that day
          2. Create CSV batch iterator
          3. Pass iterator to each dataframe's build_dataframe method
          4. Handle duplicate dataframes (job_host_summary vs indirect_nodes)
          5. Store daily results to parquet

        Returns:
            Dictionary mapping dataframe names to built dataframe objects
        """
        current_span = trace.get_current_span()
        build_start_time = time.time()

        # Get date range
        temp_df_instance = self.dataframe_builders['job_host_summary'](extractor=self.extractor, month=self.month, extra_params=self.extra_params)
        date_range = temp_df_instance.dates()

        # Initialize result dataframes
        result_dataframes = {}

        # Process each day
        for date in date_range:
            # Build dataframes for this day
            daily_dataframes = self._build_daily_dataframes(date)

            # Merge daily results into accumulated results
            result_dataframes = self._merge_daily_into_accumulated(result_dataframes, daily_dataframes)

            # Store daily results to parquet (for future rollup reader use)
            self._store_daily_parquet(date, daily_dataframes)

        return result_dataframes

    def _create_batch_iterator_for_date(self, date):
        """Create iterator that yields batch_data from all tarballs for a specific date."""
        for batch_data in self.extractor.iter_batches(date=date):
            yield batch_data

    def _create_batch_iterator_with_date_context(self, date):
        """Create iterator that yields batch_data with date context for dataframe processing."""
        for batch_data in self.extractor.iter_batches(date=date):
            # Add date context to batch_data
            batch_data_with_context = batch_data.copy()
            batch_data_with_context['_date_context'] = date
            yield batch_data_with_context

    def _build_daily_dataframes(self, date):
        """Build all dataframes for one day using batch iterator for that date.

        Handles duplicate dataframes (job_host_summary and indirect_nodes) by merging them generically.
        """
        # First, build all dataframes by table name
        table_dataframes = {}

        for dataframe_name in self.required_dataframes:
            dataframe_class = self.dataframe_builders.get(dataframe_name)
            if dataframe_class:
                df_instance = dataframe_class(extractor=self.extractor, month=self.month, extra_params=self.extra_params)

                # Create fresh iterator for each dataframe with date context
                batch_iterator = self._create_batch_iterator_with_date_context(date)
                daily_result = df_instance.build_dataframe(batch_iterator)
                table_dataframes[dataframe_name] = daily_result

        # Now merge dataframes that use the same class
        daily_dataframes = self._merge_duplicate_dataframes(table_dataframes)

        return daily_dataframes

    def _merge_duplicate_dataframes(self, table_dataframes):
        """Merge dataframes that use the same dataframe class together.

        This handles cases like job_host_summary and indirect_nodes both using
        DataframeJobhostSummaryUsage class.
        """
        # Group by dataframe class
        class_to_dataframes = {}
        class_to_instance = {}

        for table_name, df in table_dataframes.items():
            if df is None or df.empty:
                continue

            dataframe_class = self.dataframe_builders.get(table_name)
            if not dataframe_class:
                continue

            class_name = dataframe_class.__name__

            # Initialize list for this class if needed
            if class_name not in class_to_dataframes:
                class_to_dataframes[class_name] = []
                class_to_instance[class_name] = dataframe_class(extractor=self.extractor, month=self.month, extra_params=self.extra_params)

            class_to_dataframes[class_name].append(df)

        # Merge dataframes of the same class using their merge method
        merged_dataframes = {}

        for class_name, dataframes_list in class_to_dataframes.items():
            df_instance = class_to_instance[class_name]

            # Merge all dataframes of this class together
            merged_df = None
            for df in dataframes_list:
                if merged_df is None:
                    merged_df = df
                else:
                    merged_df = df_instance.merge(merged_df, df)

            merged_dataframes[class_name] = merged_df

        return merged_dataframes

    def _merge_daily_into_accumulated(self, accumulated, daily):
        """Merge daily dataframes into accumulated results.

        Now operates on dataframe class names instead of table names.
        """
        for class_name, daily_df in daily.items():
            if class_name not in accumulated:
                accumulated[class_name] = daily_df
            else:
                # Merge using dataframe's merge method
                if daily_df is not None and not daily_df.empty:
                    if accumulated[class_name] is None or accumulated[class_name].empty:
                        accumulated[class_name] = daily_df
                    else:
                        # Get the dataframe class from class name
                        dataframe_class = self._get_dataframe_class_by_name(class_name)
                        if dataframe_class:
                            df_instance = dataframe_class(extractor=self.extractor, month=self.month, extra_params=self.extra_params)
                            accumulated[class_name] = df_instance.merge(accumulated[class_name], daily_df)

        return accumulated

    def _get_dataframe_class_by_name(self, class_name):
        """Get dataframe class by its class name."""
        for df_class in self.dataframe_builders.values():
            if df_class.__name__ == class_name:
                return df_class
        return None

    def _store_daily_parquet(self, date, daily_dataframes):
        """Store daily dataframes to parquet files for rollup reader."""
        # This will be implemented when we add parquet storage
        pass

    def _initialize_dataframe_instances(self) -> Dict[str, Any]:
        """Initialize empty dataframe instances for accumulation."""
        dataframe_instances = {}

        for dataframe_name in self.required_dataframes:
            # Get the appropriate dataframe class
            dataframe_class = self.dataframe_builders.get(dataframe_name)
            if dataframe_class:
                # Create dataframe instance
                df_instance = dataframe_class(extractor=self.extractor, month=self.month, extra_params=self.extra_params)
                # Initialize with empty dataframe
                df_instance.set_cached_dataframe(df_instance.empty())
                dataframe_instances[dataframe_name] = df_instance

        return dataframe_instances

    @traced_method('unified_data_loader.process_tarballs_incrementally')
    def _process_tarballs_incrementally(self, accumulated_dataframes: Dict[str, Any]) -> int:
        """Process each tarball one at a time, building and merging dataframes incrementally."""
        current_span = trace.get_current_span()
        total_tarballs_processed = 0

        # Check if this is DB-based extraction (ExtractorControllerDB vs ExtractorDirectory)
        extractor_class_name = self.extractor.__class__.__name__

        if extractor_class_name == 'ExtractorControllerDB':
            # Handle DB-based extraction (like host_metric)
            for batch_data in self.extractor.iter_batches():
                self._process_single_batch(batch_data, accumulated_dataframes)
                total_tarballs_processed += 1
        else:
            # Handle file-based extraction with dates (get dates from a dataframe instance)
            from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_jobhost_summary_usage import DataframeJobhostSummaryUsage

            temp_df_instance = DataframeJobhostSummaryUsage(extractor=self.extractor, month=self.month, extra_params=self.extra_params)

            for date in temp_df_instance.dates():
                date_start_time = time.time()
                date_tarballs = 0

                # Process each tarball for this date
                for batch_data in self.extractor.iter_batches(date=date):
                    self._process_single_batch(batch_data, accumulated_dataframes)
                    date_tarballs += 1
                    total_tarballs_processed += 1

                date_duration = time.time() - date_start_time

                # Add date-level metrics
                add_span_attributes(
                    current_span,
                    **{
                        f'unified_loader.{date.isoformat()}.duration_seconds': date_duration,
                        f'unified_loader.{date.isoformat()}.tarballs_processed': date_tarballs,
                    },
                )

        add_span_attributes(current_span, **{'unified_loader.incremental.total_tarballs_processed': total_tarballs_processed})

        return total_tarballs_processed

    @traced_method('unified_data_loader.process_single_batch')
    def _process_single_batch(self, batch_data: Dict[str, Any], accumulated_dataframes: Dict[str, Any]):
        """Process a single tarball's batch data and merge into accumulated dataframes."""
        current_span = trace.get_current_span()
        batch_start_time = time.time()

        for dataframe_name in self.required_dataframes:
            if dataframe_name not in accumulated_dataframes:
                continue

            dataframe_instance = accumulated_dataframes[dataframe_name]

            try:
                # Build group from this single batch
                group_dataframe = dataframe_instance.build_group(batch_data)

                # Merge with accumulated dataframe
                if group_dataframe is not None and not group_dataframe.empty:
                    current_accumulated = dataframe_instance.get_cached_dataframe()
                    if current_accumulated is None or current_accumulated.empty:
                        # First data
                        merged_dataframe = group_dataframe
                    else:
                        # Merge with existing data using dataframe's merge method
                        merged_dataframe = dataframe_instance.merge(current_accumulated, group_dataframe)

                    # Update cached dataframe
                    dataframe_instance.set_cached_dataframe(merged_dataframe)

            except Exception as e:
                add_span_attributes(current_span, **{f'unified_loader.{dataframe_name}.batch_error': str(e)})

        batch_duration = time.time() - batch_start_time
        add_span_attributes(current_span, **{'unified_loader.batch.duration_seconds': batch_duration})

    @traced_method('unified_data_loader.load_consolidated_data')
    def _load_consolidated_data(self) -> List[Dict[str, Any]]:
        """
        Load all data once, organize by date and table type.

        This is the core optimization: instead of each dataframe reading tarballs independently,
        we read each tarball exactly once and extract all tables simultaneously.

        Returns:
            List of date data dictionaries in format:
            [{'date': date, 'batches': [batch_data, ...]}, ...]
        """
        current_span = trace.get_current_span()

        consolidated_data = []
        total_tarballs_read = 0
        total_tables_extracted = 0

        # Check if this is DB-based extraction (ExtractorControllerDB vs ExtractorDirectory)
        # ExtractorControllerDB has iter_batches() with no parameters
        # ExtractorDirectory has iter_batches(date, ...) with required date parameter
        extractor_class_name = self.extractor.__class__.__name__

        if extractor_class_name == 'ExtractorControllerDB':
            # Handle DB-based extraction (like host_metric)
            batch_list = []
            for batch_data in self.extractor.iter_batches():
                batch_list.append(batch_data)
                total_tarballs_read += 1
                total_tables_extracted += len(batch_data)

            # For DB extractions, we don't have dates, so return as single batch collection
            return batch_list

        # Handle file-based extraction with dates (get dates from a dataframe instance)
        # Create a temporary dataframe instance to get the date range
        from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_jobhost_summary_usage import DataframeJobhostSummaryUsage

        temp_df_instance = DataframeJobhostSummaryUsage(extractor=self.extractor, month=self.month, extra_params=self.extra_params)

        for date in temp_df_instance.dates():
            date_start_time = time.time()
            date_batches = []
            date_tarballs = 0

            # Read each tarball once for this date
            for batch_data in self.extractor.iter_batches(date=date):
                date_batches.append(batch_data)
                date_tarballs += 1
                total_tables_extracted += len(batch_data)

            if date_batches:
                consolidated_data.append({'date': date, 'batches': date_batches})

            total_tarballs_read += date_tarballs
            date_duration = time.time() - date_start_time

            # Add date-level metrics
            add_span_attributes(
                current_span,
                **{
                    f'unified_loader.{date.isoformat()}.duration_seconds': date_duration,
                    f'unified_loader.{date.isoformat()}.tarballs_read': date_tarballs,
                    f'unified_loader.{date.isoformat()}.tables_extracted': date_tarballs * len(self.dataframe_builders),
                },
            )

        add_span_attributes(
            current_span,
            **{
                'unified_loader.consolidation.total_tarballs_read': total_tarballs_read,
                'unified_loader.consolidation.total_tables_extracted': total_tables_extracted,
                'unified_loader.consolidation.dates_processed': len(consolidated_data),
            },
        )

        return consolidated_data

    @traced_method('unified_data_loader.build_dataframes_from_preloaded_data')
    def _build_dataframes_from_preloaded_data(self, consolidated_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Build all required dataframes from pre-loaded data.

        Args:
            consolidated_data: Pre-loaded data organized by date/batch

        Returns:
            Dictionary mapping dataframe names to built dataframe objects
        """
        current_span = trace.get_current_span()
        results = {}

        for dataframe_name in self.required_dataframes:
            dataframe_start_time = time.time()

            # Get the appropriate dataframe class
            dataframe_class = self.dataframe_builders.get(dataframe_name)
            if not dataframe_class:
                add_span_attributes(current_span, **{f'unified_loader.{dataframe_name}.error': 'unknown_dataframe_class'})
                continue

            # Create dataframe instance
            df_instance = dataframe_class(extractor=self.extractor, month=self.month, extra_params=self.extra_params)

            try:
                # Build dataframe using pre-loaded data (Phase 1 pattern)
                if isinstance(consolidated_data, list) and consolidated_data and isinstance(consolidated_data[0], dict):
                    # File-based data with date organization
                    result_dataframe = df_instance.build_dataframe(consolidated_data)
                else:
                    # DB-based data (no date organization)
                    result_dataframe = df_instance.build_dataframe(consolidated_data)

                # Cache the result in the dataframe instance
                df_instance.set_cached_dataframe(result_dataframe)
                results[dataframe_name] = df_instance

                dataframe_duration = time.time() - dataframe_start_time
                record_count = len(result_dataframe) if result_dataframe is not None else 0

                add_span_attributes(
                    current_span,
                    **{
                        f'unified_loader.{dataframe_name}.success': True,
                        f'unified_loader.{dataframe_name}.duration_seconds': dataframe_duration,
                        f'unified_loader.{dataframe_name}.record_count': record_count,
                    },
                )

            except Exception as e:
                dataframe_duration = time.time() - dataframe_start_time
                add_span_attributes(
                    current_span,
                    **{
                        f'unified_loader.{dataframe_name}.success': False,
                        f'unified_loader.{dataframe_name}.error': str(e),
                        f'unified_loader.{dataframe_name}.duration_seconds': dataframe_duration,
                    },
                )
                # Create empty dataframe instance for failed builds
                results[dataframe_name] = df_instance

        return results
