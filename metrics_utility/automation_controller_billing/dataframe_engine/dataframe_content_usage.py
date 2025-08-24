import re
import time

from opentelemetry import trace

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base
from metrics_utility.tracing import add_span_attributes, traced_method


# dataframe for main_jobevent
class DataframeContentUsage(Base):
    @traced_method('content_usage.build_dataframe')
    def build_dataframe(self, batch_data_iterator):
        """Build Content Usage dataframe by processing batch data iterator and merging groups.

        Args:
            batch_data_iterator: Iterator yielding batch_data from CSV scanning
                               Each batch_data: {'main_jobevent': DataFrame, 'main_host': DataFrame, ...}

        Returns:
            Merged dataframe containing all processed groups
        """
        current_span = trace.get_current_span()
        build_start_time = time.time()
        total_records = 0
        groups_processed = 0

        add_span_attributes(current_span, **{'dataframe.build.dataframe_type': 'ContentUsage', 'dataframe.build.mode': 'batch_iterator'})

        # Initialize accumulated dataframe
        accumulated_dataframe = None

        # Process each batch from the iterator
        for batch_data in batch_data_iterator:
            # Get main_jobevent data from this batch
            events = batch_data.get('main_jobevent')
            if events is None or events.empty:
                continue

            # Process this batch into a group (need to provide all required parameters)
            date = batch_data.get('_date_context')  # Get date from context
            group_dataframe = self._process_batch_data(events, batch_data, current_span, date)
            if group_dataframe is None or group_dataframe.empty:
                continue

            # Merge with accumulated dataframe
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

    def _process_batch_data(self, events, batch_data, current_span, date):
        """Process individual batch data with shared logic between preloaded and extractor modes."""
        # Process the batch data using existing logic
        processed_events = self._process_batch_events(events, batch_data, current_span, date)
        if processed_events is None or processed_events.empty:
            return self.empty()

        # Do the aggregation
        events_group = self.group(processed_events)
        return events_group

    def _process_batch_events(self, events, batch_data, current_span, date):
        """Process individual batch events with shared logic."""
        # Filter non relevant rows
        filter_start = time.time()
        events = events[events['task_action'].notnull()]
        events = events[events['host_name'].notnull()]
        filter_duration = time.time() - filter_start

        # If the dataframe is empty, skip additional processing
        if events.empty:
            return None

        events['install_uuid'] = batch_data['config']['install_uuid']

        # String processing operations - often slow with large datasets
        string_ops_start = time.time()
        # If resolved_action resolved role are not there, fill them with task action
        # and role
        events['task_action'] = events.resolved_action.fillna(events.task_action).astype(str)
        events['role'] = events.resolved_role.fillna(events.role).astype(str)
        string_ops_duration = time.time() - string_ops_start

        # Regex operations - computationally expensive
        regex_start = time.time()
        # Only get valid role names into role name
        events['role'] = events['role'].apply(lambda x: self.extract_role_name(x))

        # Rename columns to match the reality, they are just names, not normalized cols anymore
        events.rename(columns={'task_action': 'module_name', 'role': 'role_name'}, inplace=True)

        events['collection_name'] = events['module_name'].apply(self.extract_collection_name)
        regex_duration = time.time() - regex_start

        # Final cleanup operations
        cleanup_start = time.time()
        # Final cleanup if some module names didn't connect, otherwise this will fail
        # to insert with not null constraint on module_name
        events = events[events['module_name'].notnull()]

        # Set a human readable values for missing role and collection name
        events['role_name'] = events['role_name'].fillna('No role used').astype(str)
        events['collection_name'] = events['collection_name'].fillna('No collection used').astype(str)
        cleanup_duration = time.time() - cleanup_start

        return events

    # Do the aggregation
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

        group = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            task_runs=('module_name', 'count'),
            duration=('duration', 'sum'),
        )

        # Duration is null in older versions of Controller
        group['duration'] = group.duration.fillna(0)

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

        result = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            task_runs=('task_runs', 'sum'),
            duration=('duration', 'sum'),
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

    @staticmethod
    def unique_index_columns():
        return ['host_name', 'module_name', 'collection_name', 'role_name', 'install_uuid', 'job_remote_id']

    @staticmethod
    def data_columns():
        return ['task_runs', 'duration']

    @staticmethod
    def cast_types():
        return {'duration': 'float64', 'task_runs': 'int64'}

    @staticmethod
    def index_cast_types():
        """Return casting types for index columns (unique_index_columns)."""
        return {
            'host_name': str,
            'module_name': str,
            'collection_name': str,
            'role_name': str,
            'install_uuid': str,
            'job_remote_id': int,
        }

    @staticmethod
    def raw_data_cast_types():
        """Return casting types for raw data columns (before grouping)."""
        return {
            'duration': 'float64',
            'task_runs': 'int64',
            # Index columns
            'host_name': str,
            'module_name': str,
            'collection_name': str,
            'role_name': str,
            'install_uuid': str,
            'job_remote_id': int,
        }

    @staticmethod
    def operations():
        return {}
