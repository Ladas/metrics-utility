import time

import pandas as pd

from opentelemetry import trace

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base, merge_setdicts, merge_sets
from metrics_utility.automation_controller_billing.helpers import merge_json_sets, parse_json
from metrics_utility.tracing import add_span_attributes, traced_method


def compute_serial(row):
    facts = parse_json(row['canonical_facts'])
    if pd.isnull(facts.get('ansible_product_serial')) or pd.isnull(facts.get('ansible_machine_id')):
        return None
    return facts.get('ansible_product_serial', '') + '/' + facts.get('ansible_machine_id', '')


# dataframe for main_host
class DataframeInventoryScope(Base):
    @traced_method('inventory_scope.build_dataframe')
    def build_dataframe(self, batch_data_iterator):
        """Build Inventory Scope dataframe by processing batch data iterator and merging groups.

        Args:
            batch_data_iterator: Iterator yielding batch_data from CSV scanning
                               Each batch_data: {'main_host': DataFrame, 'main_jobevent': DataFrame, ...}

        Returns:
            Merged dataframe containing all processed groups
        """
        current_span = trace.get_current_span()
        build_start_time = time.time()
        total_records = 0
        groups_processed = 0

        add_span_attributes(current_span, **{'dataframe.build.dataframe_type': 'InventoryScope', 'dataframe.build.mode': 'batch_iterator'})

        # Initialize accumulated dataframe
        accumulated_dataframe = None

        # Process each batch from the iterator
        for batch_data in batch_data_iterator:
            # Get main_host data from this batch
            billing_data = batch_data.get('main_host')
            if billing_data is None or billing_data.empty:
                continue

            # Process this batch into a group (need to provide all required parameters)
            date = batch_data.get('_date_context')  # Get date from context
            group_dataframe = self._process_batch_data(billing_data, batch_data, current_span, date)
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

    def _process_batch_data(self, billing_data, batch_data, current_span, date):
        """Process individual batch data with shared logic between preloaded and extractor modes."""
        # Process the batch data using existing logic
        processed_data = self._process_batch_inventory(billing_data, batch_data, current_span, date)
        if processed_data is None or processed_data.empty:
            return self.empty()

        # Do the aggregation
        billing_data_group = self.group(processed_data)
        return billing_data_group

    def _process_batch_inventory(self, billing_data, batch_data, current_span, date):
        """Process individual batch inventory data with shared logic."""
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

        # Handle None values in last_automation before datetime conversion
        datetime_start = time.time()
        billing_data['last_automation'] = pd.to_datetime(billing_data['last_automation'], format='ISO8601', errors='coerce').dt.tz_localize(None)
        datetime_duration = time.time() - datetime_start

        # Serial computation - often computationally expensive
        serial_start = time.time()
        billing_data['serial'] = billing_data.apply(compute_serial, axis=1)
        billing_data['host_names_before_dedup'] = billing_data['host_name']
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

        group = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            organizations=('organization_name', set),
            inventories=('inventory_name', set),
            canonical_facts=('canonical_facts', merge_json_sets),
            facts=('facts', merge_json_sets),
            last_automation=('last_automation', 'max'),
            serials=('serial', set),
            host_names_before_dedup=('host_names_before_dedup', set),
        )

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

        result = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            organizations=('organizations', merge_sets),
            inventories=('inventories', merge_sets),
            canonical_facts=('canonical_facts', merge_setdicts),
            facts=('facts', merge_setdicts),
            last_automation=('last_automation', 'max'),
            serials=('serials', merge_sets),
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
        return ['host_name', 'install_uuid']

    @staticmethod
    def data_columns():
        return ['last_automation', 'organizations', 'inventories', 'canonical_facts', 'facts', 'serials', 'host_names_before_dedup']

    @staticmethod
    def cast_types():
        return {'last_automation': 'datetime64[ns]'}

    @staticmethod
    def index_cast_types():
        """Return casting types for index columns (unique_index_columns)."""
        return {
            'host_name': str,
            'install_uuid': str,
        }

    @staticmethod
    def operations():
        return {
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
        if billing_data is None or billing_data.empty:
            return self.empty()

        # Process the single batch using existing logic
        processed_data = self._process_batch_inventory(billing_data, batch_data, trace.get_current_span(), None)
        return self.group(processed_data) if processed_data is not None else self.empty()
