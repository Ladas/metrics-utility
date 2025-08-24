import time

import pandas as pd

from opentelemetry import trace

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base, merge_setdicts, merge_sets
from metrics_utility.automation_controller_billing.helpers import merge_arrays, merge_json_sets, parse_json_array
from metrics_utility.metric_utils import DIRECT, INDIRECT, MANAGED_NODE_TYPES
from metrics_utility.tracing import add_span_attributes, traced_method


# dataframe for job_host_summary / indirect_nodes
class DataframeJobhostSummaryUsage(Base):
    @traced_method('jobhost_summary.build_dataframe')
    def build_dataframe(self, batch_data_iterator):
        """Build JobHost Summary dataframe by processing batch data iterator and merging groups.

        Args:
            batch_data_iterator: Iterator yielding batch_data from CSV scanning
                               Each batch_data: {'job_host_summary': DataFrame, 'main_host': DataFrame, ...}

        Returns:
            Merged dataframe containing all processed groups
        """
        current_span = trace.get_current_span()
        build_start_time = time.time()
        total_records = 0
        groups_processed = 0

        add_span_attributes(current_span, **{'dataframe.build.dataframe_type': 'JobhostSummaryUsage', 'dataframe.build.mode': 'batch_iterator'})

        # Initialize accumulated dataframe
        accumulated_dataframe = None

        # Process each batch from the iterator
        for batch_data in batch_data_iterator:
            # Check which data source we have and determine managed_node_type first
            job_host_data = batch_data.get('job_host_summary')
            indirect_data = batch_data.get('indirect_nodes')
            
            # Determine which data to use and corresponding node type
            if job_host_data is not None and not (hasattr(job_host_data, 'empty') and job_host_data.empty):
                billing_data = job_host_data
                managed_node_type = DIRECT
            elif indirect_data is not None and not (hasattr(indirect_data, 'empty') and indirect_data.empty):
                billing_data = indirect_data
                managed_node_type = INDIRECT
            else:
                # No valid data in this batch
                continue
            date = batch_data.get('_date_context')  # Get date from context
            batch_dataframe = self._process_batch_data(billing_data, batch_data, managed_node_type, current_span, date)
            if batch_dataframe is None or batch_dataframe.empty:
                continue

            # Group this batch before merging with accumulated results
            group_dataframe = self.group(batch_dataframe)
            if group_dataframe is None or group_dataframe.empty:
                continue

            # Merge grouped dataframe with accumulated grouped dataframes
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

        # accumulated_dataframe already contains grouped data from individual batch processing
        return accumulated_dataframe if accumulated_dataframe is not None else self.empty()

    def _process_batch_data(self, billing_data, batch_data, managed_node_type, current_span, date):
        """Process individual batch data with shared logic between preloaded and extractor modes."""
        # Handle empty DataFrame case
        if billing_data is None or (hasattr(billing_data, 'empty') and billing_data.empty):
            return self.empty()
        
        billing_data['managed_node_type'] = managed_node_type
        billing_data['managed_node_type_string'] = MANAGED_NODE_TYPES[managed_node_type]

        billing_data['organization_name'] = billing_data.organization_name.fillna('No organization name')
        billing_data['install_uuid'] = batch_data['config']['install_uuid']

        # Store the original host name for mapping purposes
        billing_data['original_host_name'] = billing_data['host_name']

        if 'ansible_host_variable' in billing_data.columns:
            # Replace missing ansible_host_variable with host name
            billing_data['ansible_host_variable'] = billing_data.ansible_host_variable.fillna(billing_data['host_name'])
            # And use the new ansible_host_variable instead of host_name, since
            # what is in ansible_host_variable should be the actual host we count
            billing_data['host_name'] = billing_data['ansible_host_variable']

        # Store ansible_host || hostname for tracking deduplication impact
        # Always populate host_names_before_dedup with the actual host name for consistent tracking
        billing_data['host_names_before_dedup'] = billing_data['host_name']
        
        # Initialize host_runs to 1 for each record (will be summed during grouping)
        billing_data['host_runs'] = 1

        # Summarize all task counts into 1 col
        def sum_columns(row):
            expected_columns = ['dark', 'failures', 'ok', 'skipped', 'ignored', 'rescued']
            return sum([row.get(i, 0) for i in expected_columns if i in row.index])

        # Summarize all reachable task counts into 1 col
        def sum_reachable_columns(row):
            expected_columns = ['failures', 'ok', 'skipped', 'ignored', 'rescued']
            return sum([row.get(i, 0) for i in expected_columns if i in row.index])

        if managed_node_type == DIRECT:
            task_calc_start = time.time()
            billing_data['task_runs'] = billing_data.apply(sum_columns, axis=1)

            # Filter out managed nodes that were unreachable (represented as the dark counter).
            # We want to count hosts that had at least one task running.
            billing_data['reachable_task_runs'] = billing_data.apply(sum_reachable_columns, axis=1)
            pre_filter_count = len(billing_data)
            billing_data = billing_data[billing_data['reachable_task_runs'] > 0].copy()
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

            # Initialize with empty dicts - will be populated during deduplication if experimental dedup is enabled
            billing_data['facts'] = {}
            billing_data['canonical_facts'] = {}
            billing_data['events'] = None
        elif managed_node_type == INDIRECT:
            # For indirect nodes, task_runs is always 1 (each record represents one task)
            billing_data['task_runs'] = 1
            
            # For indirect nodes, preserve existing canonical_facts and facts from CSV data
            # Only initialize with empty dicts if they don't exist or are null
            if 'canonical_facts' not in billing_data.columns:
                billing_data['canonical_facts'] = {}
            else:
                # Fill null values with empty dicts, but preserve existing data
                billing_data['canonical_facts'] = billing_data['canonical_facts'].fillna({}).apply(lambda x: x if x else {})
            
            if 'facts' not in billing_data.columns:
                billing_data['facts'] = {}
            else:
                # Fill null values with empty dicts, but preserve existing data
                billing_data['facts'] = billing_data['facts'].fillna({}).apply(lambda x: x if x else {})
            
            # Load the events array safely if it exists
            if 'events' in billing_data.columns:
                parse_start = time.time()
                billing_data['events'] = billing_data['events'].apply(parse_json_array)
                parse_duration = time.time() - parse_start

                if parse_duration > 0.1:  # Log slow JSON parsing
                    add_span_attributes(current_span, **{f'dataframe.build.{date.isoformat()}.json_parse_duration': parse_duration})
            else:
                billing_data['events'] = None

        # Date parsing operations (manual conversion)
        datetime_start = time.time()
        # Convert to datetime, ensuring all values are properly typed (float NaNs become pd.NaT)
        # First, ensure any float NaN values are converted to string 'NaN' for proper handling
        # TODO: Add tracking/logging for failed datetime casting to identify data quality issues
        billing_data['created'] = billing_data['created'].astype(str)
        billing_data['created'] = pd.to_datetime(billing_data['created'], format='ISO8601', errors='coerce').dt.tz_localize(None)

        if 'job_created' in billing_data:
            # Ensure job_created column exists and handle NaN values properly  
            billing_data['job_created'] = billing_data['job_created'].astype(str)
            billing_data['job_created'] = pd.to_datetime(billing_data['job_created'], format='ISO8601', errors='coerce').dt.tz_localize(None)
        else:
            billing_data['job_created'] = pd.NaT
        datetime_duration = time.time() - datetime_start

        # Apply raw data casting, skipping manually converted datetime columns
        manually_converted_columns = {'created', 'job_created'}
        billing_data = self._apply_raw_data_casting(billing_data, manually_converted_columns)

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

        if billing_data_monthly_rollup is None or billing_data_monthly_rollup.empty:
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
        if not billing_data_monthly_rollup.empty:
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
        """Group and aggregate dataframe with performance tracking."""
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
            group = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
                task_runs=('task_runs', 'sum'),
                host_runs=('host_name', 'count'),
                first_automation=('created', 'min'),
                last_automation=('created', 'max'),
                job_created=('job_created', 'max'),
                managed_node_type=('managed_node_type', 'min'),
                managed_node_types_set=('managed_node_type_string', set),
                # TODO: optimize the aggregation to keep less rows around
                # job_ids=('inventory_name', set),
                events=('events', merge_arrays),
                canonical_facts=('canonical_facts', merge_json_sets),
                facts=('facts', merge_json_sets),
                host_names_before_dedup=('host_names_before_dedup', set),
            )
        except TypeError as e:
            if 'not supported between instances' in str(e) and ('float' in str(e) and 'Timestamp' in str(e)):
                # Handle mixed float/Timestamp data by ensuring proper datetime conversion
                add_span_attributes(current_span, **{'dataframe.group.datetime_conversion_error': str(e)})
                
                # Ensure datetime columns are properly converted before aggregation
                for col in ['created', 'job_created']:
                    if col in dataframe.columns:
                        dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce')
                
                # Retry aggregation after datetime conversion
                group = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
                    task_runs=('task_runs', 'sum'),
                    host_runs=('host_name', 'count'),
                    first_automation=('created', 'min'),
                    last_automation=('created', 'max'),
                    job_created=('job_created', 'max'),
                    managed_node_type=('managed_node_type', 'min'),
                    managed_node_types_set=('managed_node_type_string', set),
                    events=('events', merge_arrays),
                    canonical_facts=('canonical_facts', merge_json_sets),
                    facts=('facts', merge_json_sets),
                    host_names_before_dedup=('host_names_before_dedup', set),
                )
            else:
                raise  # Re-raise if it's a different TypeError

        grouped_count = len(group) if group is not None else 0
        result = self.cast_dataframe(group, self.cast_types())

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
        """Regroup pre-aggregated dataframe with performance tracking."""
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

        result = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            task_runs=('task_runs', 'sum'),
            host_runs=('host_runs', 'sum'),  # Sum pre-computed host_runs values from rollups
            first_automation=('first_automation', 'min'),
            last_automation=('last_automation', 'max'),
            job_created=('job_created', 'max'),
            managed_node_type=('managed_node_type', 'min'),
            managed_node_types_set=('managed_node_types_set', merge_sets),
            events=('events', merge_arrays),
            canonical_facts=('canonical_facts', merge_setdicts),
            facts=('facts', merge_setdicts),
            host_names_before_dedup=('host_names_before_dedup', merge_sets),
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

        if duration > 0.5:
            add_span_attributes(current_span, **{'dataframe.regroup.slow_operation': True})

        return result

    @staticmethod
    def unique_index_columns():
        return ['organization_name', 'job_template_name', 'host_name', 'original_host_name', 'install_uuid', 'job_remote_id']

    @staticmethod
    def data_columns():
        return [
            'host_runs',
            'task_runs',
            'first_automation',
            'last_automation',
            'job_created',
            'managed_node_type',
            'managed_node_types_set',
            'canonical_facts',
            'facts',
            'events',
            'host_names_before_dedup',
        ]

    @staticmethod
    def cast_types():
        """Return casting types for grouped/aggregated data columns."""
        return {
            'task_runs': int,
            'host_runs': int,
            'managed_node_type': int,
            'first_automation': 'datetime64[ns]',
            'last_automation': 'datetime64[ns]',
            'job_created': 'datetime64[ns]',
        }

    @staticmethod
    def raw_data_cast_types():
        """Return casting types for raw data columns (before grouping)."""
        return {
            'task_runs': int,
            'host_runs': int,
            'managed_node_type': int,
            'job_created': 'datetime64[ns]',
            # Note: first_automation and last_automation don't exist in raw data
            # They are created during grouping from the 'created' column
        }

    @staticmethod
    def index_cast_types():
        """Return casting types for index columns (unique_index_columns)."""
        return {
            'organization_name': str,
            'job_template_name': str,
            'host_name': str,
            'original_host_name': str,
            'install_uuid': str,
            'job_remote_id': int,
        }

    @staticmethod
    def operations():
        return {
            'task_runs': 'sum',  # Sum task runs when merging rollups
            'host_runs': 'sum',  # Sum host runs when merging rollups (this is correct for rollup merging)
            'first_automation': 'min',
            'last_automation': 'max',
            'job_created': 'max',
            'managed_node_type': 'min',
            'managed_node_types_set': 'combine_set',
            'events': 'combine_set',
            'canonical_facts': 'combine_json_values',
            'facts': 'combine_json_values',
            'host_names_before_dedup': 'combine_set',
        }

    def dedup(self, dataframe, hostname_mapping=None, scope_dataframe=None):
        """
        Override dedup method to enrich canonical facts and facts from scope_dataframe
        when experimental deduplication is enabled.
        """
        if dataframe is None or dataframe.empty:
            return self.empty()

        if not hostname_mapping:
            return dataframe

        # Enrich direct managed nodes with canonical facts and facts from scope data
        # when experimental deduplication is enabled
        experimental_dedup = self.extra_params.get('deduplicator') == 'ccsp-experimental'

        if experimental_dedup and scope_dataframe is not None and not scope_dataframe.empty:
            # Create a mapping from host_name to canonical_facts and facts
            if 'canonical_facts' in scope_dataframe.columns and 'facts' in scope_dataframe.columns:
                # Filter to only direct managed nodes for enrichment
                direct_mask = dataframe['managed_node_type'] == DIRECT  # DIRECT = 0

                if direct_mask.any():
                    host_facts_mapping = scope_dataframe.set_index('host_name')[['canonical_facts', 'facts']].to_dict('index')

                    # Enrich canonical_facts and facts for direct managed nodes
                    dataframe.loc[direct_mask, 'canonical_facts'] = dataframe.loc[direct_mask, 'host_name'].map(
                        lambda x: host_facts_mapping.get(x, {}).get('canonical_facts', {})
                    )
                    dataframe.loc[direct_mask, 'facts'] = dataframe.loc[direct_mask, 'host_name'].map(
                        lambda x: host_facts_mapping.get(x, {}).get('facts', {})
                    )

        # Call the parent dedup method to perform the actual deduplication
        return super().dedup(dataframe, hostname_mapping)
