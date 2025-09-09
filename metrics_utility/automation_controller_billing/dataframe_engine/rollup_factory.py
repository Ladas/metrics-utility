import logging
import time

from opentelemetry import trace

from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_collection_status import DataframeCollectionStatus
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_content_usage import DataframeContentUsage
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_inventory_scope import DataframeInventoryScope
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_jobhost_summary_usage import DataframeJobhostSummaryUsage
from metrics_utility.automation_controller_billing.dataframe_engine.db_dataframe_host_metric import DBDataframeHostMetric
from metrics_utility.automation_controller_billing.rollups.reader import RollupReader
from metrics_utility.exceptions import NotSupportedFactory
from metrics_utility.tracing import SpanNames, add_span_attributes, traced_method


class RollupDataframeFactory:
    """
    Factory for creating dataframes from rollups, computing missing rollups automatically
    """

    def __init__(self, extractor, month, extra_params):
        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params
        self.logger = logging.getLogger(__name__)

    @traced_method(SpanNames.REPORT_DATAFRAME_FACTORY)
    def create(self):
        """
        Create dataframes using rollups, computing missing rollups automatically if needed

        Returns:
            Dictionary of dataframe objects ready for report generation
        """
        with trace.get_tracer(__name__).start_as_current_span('rollup.factory.initialization') as init_span:
            # Determine which dataframes are needed based on report type
            required_dataframes = self._get_required_dataframes()

            # Check if we have date range parameters (since/until)
            since_date, until_date = self._get_date_range()

            add_span_attributes(
                init_span,
                **{
                    'factory.report_type': self.extra_params.get('report_type'),
                    'factory.required_dataframes': ','.join(required_dataframes),
                    'factory.date_range': f'{since_date} to {until_date}' if since_date and until_date else 'month-based',
                },
            )

        # Always use rollups - compute them if they don't exist
        ship_path = self.extra_params.get('ship_path')
        if ship_path:
            with trace.get_tracer(__name__).start_as_current_span('rollup.availability.check') as check_span:
                rollup_reader = RollupReader(ship_path)

                # Check rollups with source data scanning for incremental updates
                rollups_available, missing_dates, stale_dates = rollup_reader.check_rollups_with_source_data(
                    since_date, until_date, required_dataframes, self.extractor
                )

                add_span_attributes(
                    check_span,
                    **{
                        'rollups.available': rollups_available,
                        'rollups.ship_path': ship_path,
                        'rollups.missing_count': len(missing_dates),
                        'rollups.stale_count': len(stale_dates),
                    },
                )

                # Combine missing and stale dates for computation
                dates_to_compute = sorted(set(missing_dates + stale_dates))

                if dates_to_compute:
                    if missing_dates:
                        self.logger.warning(
                            f'Missing rollups for {len(missing_dates)} dates: {missing_dates[:10]}{"..." if len(missing_dates) > 10 else ""}'
                        )
                    if stale_dates:
                        self.logger.info(
                            f'Stale rollups (newer source data) for {len(stale_dates)} dates: {stale_dates[:10]}{"..." if len(stale_dates) > 10 else ""}'
                        )

                    self.logger.warning(f'Attempting to compute {len(dates_to_compute)} rollup dates automatically...')

                    add_span_attributes(
                        check_span,
                        **{
                            'rollups.missing_dates': ','.join(str(d) for d in missing_dates[:10]),  # Limit to first 10
                            'rollups.stale_dates': ','.join(str(d) for d in stale_dates[:10]),  # Limit to first 10
                        },
                    )

                    try:
                        with trace.get_tracer(__name__).start_as_current_span('rollup.auto.computation') as compute_span:
                            add_span_attributes(
                                compute_span,
                                **{
                                    'rollups.auto_compute.total_dates': len(dates_to_compute),
                                    'rollups.auto_compute.missing_dates': len(missing_dates),
                                    'rollups.auto_compute.stale_dates': len(stale_dates),
                                    'rollups.auto_compute.required_dataframes': len(required_dataframes),
                                },
                            )
                            start_time = time.time()
                            self._compute_missing_rollups(dates_to_compute, required_dataframes)
                            computation_time = time.time() - start_time
                            add_span_attributes(
                                compute_span,
                                **{'rollups.auto_compute.duration_seconds': computation_time, 'rollups.auto_compute.success': True},
                            )
                    except Exception as e:
                        self.logger.warning(f'Failed to compute rollups: {e}')
                        self.logger.warning('Continuing with partial data - some dates may be missing from report')
                        # Store missing rollups info for data status reporting
                        self.extra_params['missing_rollups'] = dates_to_compute
                        add_span_attributes(check_span, **{'rollups.auto_compute.success': False, 'rollups.auto_compute.error': str(e)})

            self.logger.info('Using rollups for dataframe creation')
            return self._create_from_rollups(rollup_reader, since_date, until_date, required_dataframes)
        else:
            raise ValueError('ship_path is required for rollup-based report generation')

    def _get_required_dataframes(self):
        """Get list of required dataframes based on report type"""
        report_type = self.extra_params.get('report_type')

        if report_type == 'CCSP':
            return ['DataframeJobhostSummaryUsage', 'DataframeContentUsage', 'DataframeInventoryScope']
        elif report_type == 'CCSPv2':
            return ['DataframeJobhostSummaryUsage', 'DataframeContentUsage', 'DataframeInventoryScope', 'DataframeCollectionStatus']
        elif report_type == 'RENEWAL_GUIDANCE':
            return ['DataframeJobhostSummaryUsage', 'DataframeContentUsage', 'DataframeInventoryScope']
        else:
            raise NotSupportedFactory(f'Factory for {report_type} not supported')

    def _get_date_range(self):
        """Extract date range from extra_params or derive from month parameter"""
        since_datetime = self.extra_params.get('opt_since')
        until_datetime = self.extra_params.get('opt_until')

        if since_datetime and until_datetime:
            since_date = since_datetime.date() if hasattr(since_datetime, 'date') else since_datetime
            until_date = until_datetime.date() if hasattr(until_datetime, 'date') else until_datetime
            return since_date, until_date

        # Check if we have explicit date parameters
        since_date = self.extra_params.get('since_date')
        until_date = self.extra_params.get('until_date')

        if since_date and until_date:
            return since_date, until_date

        # If no explicit date range, derive from month parameter
        if self.month:
            from dateutil.relativedelta import relativedelta

            since_date = self.month.replace(day=1)
            until_date = since_date + relativedelta(months=1) - relativedelta(days=1)
            return since_date, until_date

        raise ValueError('No date range or month parameter provided - cannot determine date range for rollup processing')

    def _compute_missing_rollups(self, missing_dates, required_dataframes):
        """Compute rollups for missing dates using direct function calls instead of subprocess"""
        import os

        from metrics_utility.automation_controller_billing.extract.factory import Factory as ExtractorFactory
        from metrics_utility.automation_controller_billing.rollups.manager import BatchRollupTask, RollupManager

        try:
            # Get date range for rollup computation
            since_date = min(missing_dates)
            until_date = max(missing_dates)

            # Resolve ship_path to absolute path to avoid relative path issues in Docker
            ship_path = self.extra_params['ship_path']
            if not os.path.isabs(ship_path):
                # Convert relative path to absolute Docker path
                ship_path = os.path.abspath(ship_path)

            self.logger.info(f'Computing rollups for dates: {missing_dates} using ship_path: {ship_path}')

            # Create resolved extra_params for the direct function calls
            resolved_extra_params = self.extra_params.copy()
            resolved_extra_params['ship_path'] = ship_path

            # Initialize rollup manager and extractor directly with resolved extra_params
            rollup_manager = RollupManager(ship_path)
            extractor = ExtractorFactory(resolved_extra_params.get('ship_target', 'directory'), resolved_extra_params).create()

            # Create execution plan for missing dates
            smart_plan = rollup_manager.create_smart_dependency_plan(since_date, until_date, required_dataframes, force=False, extractor=extractor)

            if not smart_plan:
                self.logger.info('No rollups need computation.')
                return

            # Execute each date group using BatchRollupTask
            for date_group in smart_plan:
                target_date = date_group['date']
                dataframes = date_group['dataframes']

                # Only compute for dates that are actually missing
                if target_date in missing_dates:
                    self.logger.info(f'Computing rollups for {target_date}: {dataframes}')

                    # Create and execute batch task
                    batch_task = BatchRollupTask(target_date=target_date, dataframe_names=dataframes, max_priority=0)
                    self._compute_rollup_for_batch_task(rollup_manager, batch_task, extractor, resolved_extra_params)

            self.logger.info(f'Successfully computed rollups for dates: {missing_dates}')

        except Exception as e:
            self.logger.error(f'Failed to compute rollups: {e}')
            raise e

    def _compute_rollup_for_batch_task(self, rollup_manager, batch_task, extractor, resolved_extra_params):
        """Compute rollups for a batch task (single date, multiple dataframes)"""
        import time

        from opentelemetry import trace

        from metrics_utility.tracing import SpanNames, add_span_attributes

        target_date = batch_task.target_date
        dataframe_names = batch_task.dataframe_names

        with trace.get_tracer(__name__).start_as_current_span(SpanNames.ROLLUP_TASK_EXECUTION) as task_span:
            add_span_attributes(
                task_span,
                **{
                    'rollup.task.date': target_date.isoformat(),
                    'rollup.task.dataframe_count': len(dataframe_names),
                    'rollup.task.dataframes': ','.join(dataframe_names),
                },
            )

            # Use unified data loading for efficient single-pass processing
            start_time = time.time()
            dataframes = self._create_dataframes_for_date(target_date, dataframe_names, extractor, resolved_extra_params)
            processing_time = time.time() - start_time

            # Store rollups for each dataframe using correct RollupManager method names
            for dataframe_name, dataframe in dataframes.items():
                if dataframe is not None and not dataframe.empty:
                    records_processed = len(dataframe)
                    rollup_manager.save_rollup_data(target_date, dataframe_name, dataframe, records_processed, processing_time)
                    self.logger.info(f'✓ Stored rollup for {dataframe_name} on {target_date} with {records_processed} records')
                else:
                    # Store metadata for no-data case
                    rollup_manager.save_no_data_metadata(target_date, dataframe_name)
                    self.logger.info(f'✓ Stored no-data rollup for {dataframe_name} on {target_date}')

    def _create_dataframes_for_date(self, target_date, dataframe_names, extractor, resolved_extra_params):
        """Create dataframes for a specific date using unified data loading"""
        # Build batch data iterator for this specific date
        batch_data_iterator = extractor.iter_batches(target_date)

        # Create dataframe instances using resolved_extra_params
        dataframe_instances = {}
        for dataframe_name in dataframe_names:
            # Get dataframe class by name (dataframe_names contains class names like 'DataframeJobhostSummaryUsage')
            DataframeClass = self._get_dataframe_class_by_name_unified(dataframe_name)
            if DataframeClass:
                dataframe_instances[dataframe_name] = DataframeClass(extractor, self.month, resolved_extra_params)
                self.logger.info(f'Created dataframe instance for {dataframe_name}: {DataframeClass.__name__}')
            else:
                self.logger.warning(f'No class mapping found for dataframe: {dataframe_name}')

        # Process each batch and accumulate data
        result_dataframes = {name: None for name in dataframe_names}
        batch_count = 0

        for batch_data in batch_data_iterator:
            batch_count += 1
            self.logger.info(f'Processing batch {batch_count} for {target_date}: keys={list(batch_data.keys())}')
            batch_data['_date_context'] = target_date  # Add date context

            # Process this batch for each required dataframe
            for dataframe_name, dataframe_instance in dataframe_instances.items():
                # Create iterator that yields just this batch
                single_batch_iterator = [batch_data]

                # Build dataframe for this batch
                batch_result = dataframe_instance.build_dataframe(iter(single_batch_iterator))

                if batch_result is not None and not batch_result.empty:
                    self.logger.info(f'Got {len(batch_result)} records for {dataframe_name} from batch {batch_count}')

                # Merge with accumulated results
                if result_dataframes[dataframe_name] is None:
                    result_dataframes[dataframe_name] = batch_result
                elif batch_result is not None and not batch_result.empty:
                    result_dataframes[dataframe_name] = dataframe_instance.merge(result_dataframes[dataframe_name], batch_result)

        self.logger.info(f'Processed {batch_count} batches for {target_date}')

        # build_dataframe() already groups the data, so result_dataframes contains grouped dataframes
        return result_dataframes

    def _get_rollup_to_standard_mapping(self):
        """Map rollup dataframe names to standard dataframe names expected by dedup/report"""
        return {
            'DataframeJobhostSummaryUsage': 'job_host_summary',
            'DataframeContentUsage': 'main_jobevent',
            'DataframeInventoryScope': 'main_host',
            'DataframeCollectionStatus': 'data_collection_status',
        }

    def _create_from_rollups(self, rollup_reader, since_date, until_date, required_dataframes):
        """Create dataframes by loading and merging rollups"""
        with trace.get_tracer(__name__).start_as_current_span('rollup.dataframes.loading') as loading_span:
            add_span_attributes(
                loading_span,
                **{
                    'rollup.loading.since_date': since_date.isoformat(),
                    'rollup.loading.until_date': until_date.isoformat(),
                    'rollup.loading.dataframes_count': len(required_dataframes),
                    'rollup.loading.dataframes': ','.join(required_dataframes),
                },
            )

            try:
                start_time = time.time()
                merged_dataframes = rollup_reader.load_rollup_dataframes(since_date, until_date, required_dataframes)
                loading_time = time.time() - start_time

                # Calculate total records loaded
                total_records = sum(len(df) if df is not None and hasattr(df, '__len__') else 0 for df in merged_dataframes.values())

                add_span_attributes(
                    loading_span,
                    **{
                        'rollup.loading.success': True,
                        'rollup.loading.duration_seconds': loading_time,
                        'rollup.loading.total_records': total_records,
                        'rollup.loading.loaded_dataframes': len([df for df in merged_dataframes.values() if df is not None]),
                    },
                )

            except ValueError as e:
                # Handle case where rollups are completely missing
                self.logger.error(f'Could not load rollups: {e}')
                self.logger.error('No rollups available - falling back to standard processing')
                merged_dataframes = {df_name: None for df_name in required_dataframes}

                add_span_attributes(
                    loading_span,
                    **{'rollup.loading.success': False, 'rollup.loading.error': str(e), 'rollup.loading.fallback': 'standard_processing'},
                )

        # Map rollup names to standard names expected by dedup/report
        rollup_to_standard = self._get_rollup_to_standard_mapping()

        # Convert to the expected format (actual pandas DataFrames for reports)
        result = {}
        for rollup_name in required_dataframes:
            standard_name = rollup_to_standard.get(rollup_name, rollup_name)
            if rollup_name in merged_dataframes:
                # Use merged rollup data directly
                dataframe_data = merged_dataframes[rollup_name]

                # Return actual pandas DataFrame for reports
                if dataframe_data is not None:
                    result[standard_name] = dataframe_data
                else:
                    # Create empty dataframe using class structure for consistency
                    dataframe_class = self._get_dataframe_class_by_name_unified(rollup_name)
                    if dataframe_class:
                        instance = dataframe_class(extractor=self.extractor, month=self.month, extra_params=self.extra_params)
                        result[standard_name] = instance.empty()
                    else:
                        result[standard_name] = None
            else:
                # Create empty dataframe using class structure
                dataframe_class = self._get_dataframe_class_by_name_unified(rollup_name)
                if dataframe_class:
                    instance = dataframe_class(extractor=self.extractor, month=self.month, extra_params=self.extra_params)
                    result[standard_name] = instance.empty()
                else:
                    result[standard_name] = None

        return result

    def _create_dataframe_object(self, rollup_name, dataframe_data):
        """Create the proper dataframe object instance with pre-loaded data"""
        dataframe_class = self._get_dataframe_class_by_name_unified(rollup_name)
        if not dataframe_class:
            raise ValueError(f'Unknown dataframe class for rollup: {rollup_name}')

        # Create instance with extractor, month, and extra_params
        instance = dataframe_class(extractor=self.extractor, month=self.month, extra_params=self.extra_params)

        # Set the preloaded data using the proper method
        if dataframe_data is not None:
            instance.set_cached_dataframe(dataframe_data)
        else:
            # For None data, set empty dataframe using the class's empty() method
            instance.set_cached_dataframe(instance.empty())

        return instance

    def _create_from_standard_processing(self):
        """Create dataframes using unified data loading (Phase 1 implementation)"""
        return self._create_with_unified_data_loading()

    @traced_method('rollup_factory.unified_data_loading')
    def _create_with_unified_data_loading(self):
        """
        Create dataframes using unified data loading architecture.

        This eliminates the 5x redundant I/O problem by reading each tarball exactly once
        and distributing data to all dataframe builders simultaneously.

        Returns:
            Dictionary mapping standard dataframe names to built dataframe objects
        """
        current_span = trace.get_current_span()
        build_start_time = time.time()

        # Get table name to dataframe class mapping
        table_to_class = self._get_table_to_class_mapping()

        # Get required table names (different from rollup names)
        required_tables = self._get_required_table_names()

        add_span_attributes(
            current_span,
            **{
                'unified_loader.required_tables': ','.join(required_tables),
                'unified_loader.total_classes': len(set(table_to_class.values())),
                'unified_loader.mode': 'daily_processing',
            },
        )

        # Get date range
        temp_df_instance = DataframeJobhostSummaryUsage(extractor=self.extractor, month=self.month, extra_params=self.extra_params)
        date_range = temp_df_instance.dates()

        # Initialize result dataframes (by class name)
        result_dataframes = {}

        # Process each day
        for date in date_range:
            # Build dataframes for this day
            daily_dataframes = self._build_daily_dataframes_unified(date, table_to_class, required_tables)

            # Merge daily results into accumulated results
            result_dataframes = self._merge_daily_into_accumulated_unified(result_dataframes, daily_dataframes)

        # Convert class-based results to standard names expected by reports
        standard_results = self._convert_to_standard_names(result_dataframes)

        build_duration = time.time() - build_start_time
        add_span_attributes(
            current_span,
            **{
                'unified_loader.total_duration_seconds': build_duration,
                'unified_loader.dates_processed': len(date_range),
                'unified_loader.final_dataframes': len(standard_results),
            },
        )

        return standard_results

    def _get_table_to_class_mapping(self):
        """Map table names to dataframe classes"""
        return {
            'job_host_summary': DataframeJobhostSummaryUsage,
            'indirect_nodes': DataframeJobhostSummaryUsage,  # Same class handles both
            'main_jobevent': DataframeContentUsage,
            'main_host': DataframeInventoryScope,
            'data_collection_status': DataframeCollectionStatus,
            'host_metric': DBDataframeHostMetric,
        }

    def _get_required_table_names(self):
        """Get list of required table names based on report type"""
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

    def _create_batch_iterator_with_date_context(self, date):
        """Create iterator that yields batch_data with date context for dataframe processing."""
        for batch_data in self.extractor.iter_batches(date=date):
            # Add date context to batch_data
            batch_data_with_context = batch_data.copy()
            batch_data_with_context['_date_context'] = date
            yield batch_data_with_context

    def _build_daily_dataframes_unified(self, date, table_to_class, required_tables):
        """Build all dataframes for one day using batch iterator for that date.

        Handles duplicate dataframes (job_host_summary and indirect_nodes) by processing them together.
        """
        # Group tables by the dataframe class that handles them
        class_to_tables = {}
        for table_name in required_tables:
            dataframe_class = table_to_class.get(table_name)
            if dataframe_class:
                class_name = dataframe_class.__name__
                if class_name not in class_to_tables:
                    class_to_tables[class_name] = []
                class_to_tables[class_name].append(table_name)

        # Build dataframes by class (not by table)
        daily_dataframes = {}

        for class_name, tables in class_to_tables.items():
            dataframe_class = self._get_dataframe_class_by_name_unified(class_name)
            if dataframe_class:
                df_instance = dataframe_class(extractor=self.extractor, month=self.month, extra_params=self.extra_params)

                # Create fresh iterator for this dataframe class with date context
                # The dataframe class will handle multiple tables (like job_host_summary and indirect_nodes)
                batch_iterator = self._create_batch_iterator_with_date_context(date)
                daily_result = df_instance.build_dataframe(batch_iterator)
                daily_dataframes[class_name] = daily_result

        return daily_dataframes

    def _merge_duplicate_dataframes_unified(self, table_dataframes, table_to_class):
        """Merge dataframes that use the same dataframe class together.

        This handles cases like job_host_summary and indirect_nodes both using
        DataframeJobhostSummaryUsage class.
        """
        # Group by dataframe class
        class_to_dataframes = {}
        class_to_instance = {}

        for table_name, df in table_dataframes.items():
            if df is None or (hasattr(df, 'empty') and df.empty):
                continue

            dataframe_class = table_to_class.get(table_name)
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

    def _merge_daily_into_accumulated_unified(self, accumulated, daily):
        """Merge daily dataframes into accumulated results.

        Now operates on dataframe class names instead of table names.
        """
        for class_name, daily_df in daily.items():
            if class_name not in accumulated:
                accumulated[class_name] = daily_df
            else:
                # Merge using dataframe's merge method
                if daily_df is not None and not daily_df.empty:
                    if accumulated[class_name] is None or (hasattr(accumulated[class_name], 'empty') and accumulated[class_name].empty):
                        accumulated[class_name] = daily_df
                    else:
                        # Get the dataframe class from class name
                        dataframe_class = self._get_dataframe_class_by_name_unified(class_name)
                        if dataframe_class:
                            df_instance = dataframe_class(extractor=self.extractor, month=self.month, extra_params=self.extra_params)
                            accumulated[class_name] = df_instance.merge(accumulated[class_name], daily_df)

        return accumulated

    def _get_dataframe_class_by_name_unified(self, class_name):
        """Get dataframe class by its class name."""
        class_mapping = {
            'DataframeJobhostSummaryUsage': DataframeJobhostSummaryUsage,
            'DataframeContentUsage': DataframeContentUsage,
            'DataframeInventoryScope': DataframeInventoryScope,
            'DataframeCollectionStatus': DataframeCollectionStatus,
            'DBDataframeHostMetric': DBDataframeHostMetric,
        }
        return class_mapping.get(class_name)

    def _convert_to_standard_names(self, class_dataframes):
        """Convert class-based dataframes to standard names expected by reports."""
        # Map class names to standard names
        class_to_standard = {
            'DataframeJobhostSummaryUsage': 'job_host_summary',
            'DataframeContentUsage': 'main_jobevent',
            'DataframeInventoryScope': 'main_host',
            'DataframeCollectionStatus': 'data_collection_status',
            'DBDataframeHostMetric': 'host_metric',
        }

        result = {}
        for class_name, df in class_dataframes.items():
            standard_name = class_to_standard.get(class_name, class_name.lower())

            # Create dataframe object with cached data
            if df is not None:
                dataframe_class = self._get_dataframe_class_by_name_unified(class_name)
                if dataframe_class:
                    df_instance = dataframe_class(extractor=self.extractor, month=self.month, extra_params=self.extra_params)
                    df_instance.set_cached_dataframe(df)
                    result[standard_name] = df_instance
                else:
                    result[standard_name] = df
            else:
                result[standard_name] = None

        return result
