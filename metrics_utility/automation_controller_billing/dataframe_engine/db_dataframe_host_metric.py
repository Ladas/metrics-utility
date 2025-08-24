import time

import pandas as pd

from opentelemetry import trace

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base
from metrics_utility.tracing import add_span_attributes, traced_method


# dataframe for host_metric
class DBDataframeHostMetric(Base):
    @traced_method('host_metric.build_dataframe')
    def build_dataframe(self, preloaded_data):
        """Build Host Metric dataframe with Phase 1 unified data loading.

        Args:
            preloaded_data: Pre-loaded data from unified coordinator, organized by batch
                          Format: [batch_data, batch_data, ...] (no date organization for DB queries)
        """
        current_span = trace.get_current_span()
        build_start_time = time.time()
        total_records_processed = 0
        batches_processed = 0
        concat_operations = 0
        datetime_operations = 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.build.cache_hit': False,
                'dataframe.build.dataframe_type': 'DBHostMetric',
                'dataframe.build.aggregation_type': 'none_concat_only',
                'dataframe.build.extraction_method': 'direct_db_query',
                'dataframe.build.mode': 'preloaded_data',
                'dataframe.build.phase': 'unified_loading_phase1',
            },
        )

        host_metric_concat = None

        for batch_data in preloaded_data:
            batch_start_time = time.time()

            # If the dataframe is empty, skip additional processing
            host_metric = batch_data.get('host_metric')
            if host_metric is None or host_metric.empty:
                continue

            batch_count = len(host_metric)
            total_records_processed += batch_count

            # Process the batch data using existing logic
            processed_data = self._process_batch_host_metric(host_metric, current_span)
            if processed_data is None:
                continue

            # Concat operation tracking
            concat_start = time.time()
            if host_metric_concat is None:
                host_metric_concat = processed_data
            else:
                host_metric_concat = pd.concat([host_metric_concat, processed_data], ignore_index=True)
                concat_operations += 1
            concat_duration = time.time() - concat_start
            datetime_operations += 3  # Three datetime columns processed

            batch_duration = time.time() - batch_start_time
            batches_processed += 1

            # Log slow operations
            if batch_duration > 0.5 or concat_duration > 0.1:
                add_span_attributes(
                    current_span,
                    **{
                        f'dataframe.build.batch_{batches_processed}.slow_batch': True,
                        f'dataframe.build.batch_{batches_processed}.duration': batch_duration,
                        f'dataframe.build.batch_{batches_processed}.concat_duration': concat_duration,
                        f'dataframe.build.batch_{batches_processed}.record_count': batch_count,
                    },
                )

        total_build_duration = time.time() - build_start_time

        if host_metric_concat is None:
            add_span_attributes(
                current_span,
                **{
                    'dataframe.build.result': 'empty',
                    'dataframe.build.total_duration_seconds': total_build_duration,
                    'dataframe.build.batches_processed': batches_processed,
                    'dataframe.build.total_records_processed': total_records_processed,
                    'dataframe.build.concat_operations': concat_operations,
                    'dataframe.build.datetime_operations': datetime_operations,
                },
            )
            return None

        # Final reset_index operation
        reset_start = time.time()
        result = host_metric_concat.reset_index()
        reset_duration = time.time() - reset_start
        final_count = len(result) if result is not None else 0

        # Final summary metrics
        add_span_attributes(
            current_span,
            **{
                'dataframe.build.result': 'success',
                'dataframe.build.total_duration_seconds': total_build_duration,
                'dataframe.build.reset_index_duration_seconds': reset_duration,
                'dataframe.build.batches_processed': batches_processed,
                'dataframe.build.total_records_processed': total_records_processed,
                'dataframe.build.concat_operations': concat_operations,
                'dataframe.build.datetime_operations': datetime_operations,
                'dataframe.build.final_records': final_count,
                'dataframe.build.avg_batch_size': total_records_processed / batches_processed if batches_processed > 0 else 0,
            },
        )

        # Log performance warnings
        if total_build_duration > 5.0:
            add_span_attributes(
                current_span,
                **{'dataframe.build.performance_warning': f'Build took {total_build_duration:.2f}s', 'dataframe.build.slow_operation': True},
            )

        # Log high datetime operation count
        if datetime_operations > 300:  # 100 batches * 3 columns
            add_span_attributes(
                current_span,
                **{
                    'dataframe.build.high_datetime_ops': True,
                    'dataframe.build.datetime_performance_warning': f'{datetime_operations} datetime operations',
                },
            )

        return result

    def _process_batch_host_metric(self, host_metric, current_span):
        """Process individual batch host metric data with shared logic."""
        # Spreadsheet doesn't support timezones - DateTime parsing operations
        datetime_start = time.time()
        host_metric['first_automation'] = pd.to_datetime(host_metric['first_automation'], format='ISO8601').dt.tz_localize(None)
        host_metric['last_automation'] = pd.to_datetime(host_metric['last_automation'], format='ISO8601').dt.tz_localize(None)
        host_metric['last_deleted'] = pd.to_datetime(host_metric['last_deleted'], format='ISO8601').dt.tz_localize(None)
        datetime_duration = time.time() - datetime_start

        return host_metric
