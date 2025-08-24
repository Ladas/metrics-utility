import datetime
import logging
import os
import time

from argparse import RawDescriptionHelpFormatter
from datetime import timedelta, timezone

from django.core.management.base import BaseCommand
from opentelemetry import trace

from metrics_utility.automation_controller_billing.extract.factory import Factory as ExtractorFactory
from metrics_utility.automation_controller_billing.rollups.manager import BatchRollupTask, RollupManager
from metrics_utility.exceptions import BadRequiredEnvVar, BadShipTarget, MissingRequiredEnvVar
from metrics_utility.management.validation import (
    date_format_text,
    handle_directory_ship_target,
    handle_env_validation,
    handle_not_crc,
    handle_not_s3,
    handle_s3_ship_target,
    parse_number_of_days,
)
from metrics_utility.tracing import SpanAttributes, SpanNames, add_span_attributes, traced_method


class Command(BaseCommand):
    """
    Compute Rollups - Pre-compute daily rollups of metrics data for faster report generation
    """

    help = 'Compute daily rollups of metrics data'
    help_texts = {
        'since': (f'Start date for rollup computation, including. {date_format_text.format(name="since")}'),
        'until': (f'End date for rollup computation, including. {date_format_text.format(name="until")}'),
        'force': ('With this option, existing rollups will be recomputed even if they already exist.'),
        'verbose': ('Starts to print debug information to terminal.'),
        'list': ('List all available rollups and their status.'),
        'clean': ('Clean invalid or partial rollups for the specified date range.'),
        'parallel': ('Show parallel execution plan without actually executing.'),
    }

    def create_parser(self, prog_name, subcommand, **kwargs):
        return super().create_parser(
            prog_name,
            subcommand,
            formatter_class=RawDescriptionHelpFormatter,
            epilog='\n'.join(
                [
                    'DESCRIPTION',
                    '    This command pre-computes daily rollups of metrics data to enable faster report generation.',
                    '    Rollups are stored as versioned parquet files in SHIP_PATH/rollups/daily/YYYY/MM/DD/DataframeName/timestamp_v1.0/',
                    '    Each version includes metadata.json tracking computation status and a data.parquet file.',
                    '    The system processes rollups by date groups with optimized dependency resolution.',
                    'ENVIRONMENT',
                    "    METRICS_UTILITY_REPORT_TYPE (required, case sensitive): one of 'CCSPv2', 'CCSP', 'RENEWAL_GUIDANCE'",
                    '        determines which dataframes are computed in rollups',
                    '',
                    "    METRICS_UTILITY_SHIP_TARGET (required): one of 'directory', 's3', 'controller_db'",
                    '        input mechanism for raw data',
                    '',
                    '    METRICS_UTILITY_SHIP_PATH (required): a path',
                    '        local or s3 directory path, input tarballs in path/data/, output rollups in path/rollups/daily/',
                    '',
                    "    METRICS_UTILITY_DEDUPLICATOR (optional): one of 'ccsp', 'renewal', 'ccsp-experimental'",
                    "        choice of deduplication algorithm, defaults to 'ccsp' or 'renewal' based on the chosen report type",
                    '',
                    'EXAMPLES',
                    '    # Compute rollups for a specific date range',
                    '    python manage.py compute_rollups --since=2024-01-01 --until=2024-01-31',
                    '',
                    '    # List all available rollups',
                    '    python manage.py compute_rollups --list',
                    '',
                    '    # Force recomputation of existing rollups',
                    '    python manage.py compute_rollups --since=2024-01-01 --until=2024-01-07 --force',
                    '',
                    '    # Clean invalid rollups for a date range',
                    '    python manage.py compute_rollups --since=2024-01-01 --until=2024-01-07 --clean',
                ]
            ),
            **kwargs,
        )

    def add_arguments(self, parser):
        parser.add_argument('--since', dest='since', action='store', help=self.help_texts.get('since'))
        parser.add_argument('--until', dest='until', action='store', help=self.help_texts.get('until'))
        parser.add_argument('--force', dest='force', action='store_true', help=self.help_texts.get('force'))
        parser.add_argument('--verbose', dest='verbose', action='store_true', help=self.help_texts.get('verbose'))
        parser.add_argument('--list', dest='list', action='store_true', help=self.help_texts.get('list'))
        parser.add_argument('--clean', dest='clean', action='store_true', help=self.help_texts.get('clean'))
        parser.add_argument('--parallel', dest='parallel', action='store_true', help=self.help_texts.get('parallel'))

    def init_logging(self):
        self.logger = logging.getLogger('awx.main.analytics')
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)  # Enable debug level for logger
        self.logger.propagate = False

    @traced_method(SpanNames.ROLLUP_COMPUTATION)
    def handle(self, *args, **options):
        self.init_logging()
        handle_env_validation('build')

        ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET', None)
        extra_params = self._handle_extra_params(ship_target)

        # Initialize rollup manager
        rollup_manager = RollupManager(extra_params['ship_path'])

        # Handle list option
        if options.get('list'):
            self._handle_list_rollups(rollup_manager)
            return

        # Validate date parameters
        opt_since, opt_until = self._validate_date_params(options)

        # Handle clean option
        if options.get('clean'):
            self._handle_clean_rollups(rollup_manager, opt_since, opt_until)
            return

        # Get required dataframes based on report type
        required_dataframes = self._get_required_dataframes(extra_params['report_type'])

        # Handle parallel plan option
        if options.get('parallel'):
            self._handle_parallel_plan(
                rollup_manager, opt_since, opt_until, required_dataframes, options.get('force', False), ship_target, extra_params
            )
            return

        # Main computation logic using smart dependency resolution
        opt_force = options.get('force', False)

        self.logger.info(f'Computing rollups for date range: {opt_since} to {opt_until}')
        self.logger.info('Using smart dependency resolution with source data scanning and incremental updates')

        # Setup common parameters and create extractor first
        extra_params['deduplicator'] = os.getenv('METRICS_UTILITY_DEDUPLICATOR', None) or None
        extractor = ExtractorFactory(ship_target, extra_params).create()

        smart_plan = rollup_manager.create_smart_dependency_plan(opt_since, opt_until, required_dataframes, force=opt_force, extractor=extractor)

        if not smart_plan:
            self.logger.info('No rollups need computation. All dates and dataframes are already processed.')
            return

        self.logger.info(f'Created execution plan with {len(smart_plan)} date groups')
        self._display_smart_execution_plan(smart_plan)

        self._execute_smart_plan(rollup_manager, smart_plan, extractor, extra_params)

    def _validate_date_params(self, options):
        """Validate and parse date parameters"""
        since_str = options.get('since')
        until_str = options.get('until')

        if not since_str:
            raise ValueError('--since parameter is required for rollup computation')

        try:
            opt_since = datetime.datetime.strptime(since_str, '%Y-%m-%d').date()
        except ValueError:
            raise ValueError(f'Invalid since date format: {since_str}. Expected YYYY-MM-DD')

        if until_str:
            try:
                opt_until = datetime.datetime.strptime(until_str, '%Y-%m-%d').date()
            except ValueError:
                raise ValueError(f'Invalid until date format: {until_str}. Expected YYYY-MM-DD')
        else:
            opt_until = opt_since

        if opt_since > opt_until:
            raise ValueError('Since date cannot be after until date')

        return opt_since, opt_until

    def _handle_list_rollups(self, rollup_manager):
        """Handle the --list option to display available rollups"""
        self.logger.info('Scanning available rollups...')
        rollups = rollup_manager.list_available_rollups()

        if not rollups:
            self.logger.info('No rollups found.')
            return

        self.logger.info(f'Found {len(rollups)} rollup dates:')
        self.logger.info('Date       | Status')
        self.logger.info('-' * 25)

        for rollup_date, status in rollups:
            self.logger.info(f'{rollup_date} | {status.value}')

    def _handle_clean_rollups(self, rollup_manager, since_date, until_date):
        """Handle the --clean option to remove invalid rollups"""
        self.logger.info(f'Cleaning rollups for date range: {since_date} to {until_date}')

        current_date = since_date
        cleaned_count = 0

        while current_date <= until_date:
            rollup_manager.clean_invalid_rollups(current_date)
            cleaned_count += 1
            current_date += timedelta(days=1)

        self.logger.info(f'Cleaned {cleaned_count} rollup directories.')

    def _execute_smart_plan(self, rollup_manager, smart_plan, extractor, extra_params):
        """Execute rollup tasks using smart dependency resolution"""
        total_date_groups = len(smart_plan)
        total_dataframes = sum(len(date_group['dataframes']) for date_group in smart_plan)
        successful_date_groups = 0
        successful_dataframes = 0
        failed_dataframes = 0

        # Add execution metrics to span
        current_span = trace.get_current_span()
        add_span_attributes(
            current_span,
            **{
                'rollup.execution.total_date_groups': total_date_groups,
                'rollup.execution.total_dataframes': total_dataframes,
                'rollup.execution.mode': 'smart_dependency',
            },
        )

        for idx, date_group in enumerate(smart_plan, 1):
            target_date = date_group['date']
            dataframes = date_group['dataframes']

            self.logger.info(f'Executing date group {idx}/{total_date_groups}: {len(dataframes)} dataframes for {target_date}')
            self.logger.info(f'  Dataframes: {", ".join(dataframes)}')

            try:
                # Use the existing batch computation logic for each date
                from metrics_utility.automation_controller_billing.rollups.manager import BatchRollupTask

                batch_task = BatchRollupTask(target_date=target_date, dataframe_names=dataframes, max_priority=0)

                batch_results = self._compute_rollup_for_batch_task(rollup_manager, batch_task, extractor, extra_params)
                successful_date_groups += 1

                # Count individual dataframe successes/failures
                for dataframe_name, success in batch_results.items():
                    if success:
                        successful_dataframes += 1
                        self.logger.info(f'  ✓ Successfully computed {dataframe_name} for {target_date}')
                    else:
                        failed_dataframes += 1
                        self.logger.error(f'  ✗ Failed to compute {dataframe_name} for {target_date}')

                # Update span with progress
                add_span_attributes(
                    current_span,
                    **{
                        'rollup.execution.successful_date_groups': successful_date_groups,
                        'rollup.execution.successful_dataframes': successful_dataframes,
                    },
                )
            except Exception as e:
                self.logger.error(f'✗ Failed date group {idx}: {target_date}: {e}')
                failed_dataframes += len(dataframes)

                # Save error metadata for all dataframes in failed date group
                for dataframe_name in dataframes:
                    rollup_manager.save_error_metadata(target_date, dataframe_name, f'Date group failed: {str(e)}')

                # Update span with failure info
                add_span_attributes(current_span, **{'rollup.execution.failed_dataframes': failed_dataframes})
                continue

        # Add final execution metrics
        add_span_attributes(
            current_span,
            **{
                'rollup.execution.final.successful_date_groups': successful_date_groups,
                'rollup.execution.final.successful_dataframes': successful_dataframes,
                'rollup.execution.final.failed_dataframes': failed_dataframes,
                'rollup.execution.success_rate': successful_dataframes / total_dataframes if total_dataframes > 0 else 0,
            },
        )

        self.logger.info(
            f'Rollup computation completed. {successful_dataframes} successful, {failed_dataframes} failed out of {total_dataframes} dataframes in {successful_date_groups}/{total_date_groups} date groups.'
        )

    def _display_smart_execution_plan(self, smart_plan):
        """Display the execution plan for rollup tasks"""
        self.logger.info('\nExecution Plan:')
        self.logger.info('=' * 50)

        for idx, date_group in enumerate(smart_plan, 1):
            self.logger.info(f'Date Group {idx}: {date_group["date"]}')
            self.logger.info(f'  Dataframes ({len(date_group["dataframes"])}): {", ".join(date_group["dataframes"])}')
            self.logger.info('')

    def _compute_rollup_for_batch_task(self, rollup_manager, batch_task: BatchRollupTask, extractor, extra_params):
        """
        Compute rollups for a batch task (single date, multiple dataframes)
        This eliminates duplicate tarball extraction, CSV processing, and dataframe factory calls
        """
        target_date = batch_task.target_date
        dataframe_names = batch_task.dataframe_names

        with trace.get_tracer(__name__).start_as_current_span(SpanNames.ROLLUP_TASK_EXECUTION) as task_span:
            # Add batch context to span
            add_span_attributes(
                task_span,
                **{
                    SpanAttributes.ROLLUP_DATE: target_date.isoformat(),
                    'rollup.batch.dataframe_count': len(dataframe_names),
                    'rollup.batch.dataframes': ','.join(dataframe_names),
                    'rollup.optimization': 'same_date_batching',
                },
            )

            start_time = time.time()

            # Clean any existing invalid rollups for this date (all dataframes)
            for dataframe_name in dataframe_names:
                rollup_manager.clean_invalid_rollups(target_date, dataframe_name)

            # Setup date-specific parameters - SHARED ACROSS ALL DATAFRAMES
            date_params = extra_params.copy()
            date_params['since_date'] = target_date
            date_params['until_date'] = target_date
            date_params['opt_since'] = datetime.datetime.combine(target_date, datetime.time.min, timezone.utc)
            date_params['opt_until'] = datetime.datetime.combine(target_date, datetime.time.max, timezone.utc)
            date_params['ephemeral_days'] = parse_number_of_days(None)

            # Dummy month parameter (not used for daily rollups)
            month = target_date.replace(day=1)

            # Check source data availability first
            source_data_available = False
            if hasattr(extractor, 'scan_tarballs_for_date'):
                source_tarballs = extractor.scan_tarballs_for_date(target_date)
                source_data_available = len(source_tarballs) > 0
                self.logger.debug(f'Source data scan for {target_date}: {len(source_tarballs)} tarballs found')
            else:
                # Fallback - assume source data is available if we can't scan
                source_data_available = True
                self.logger.debug(f'Extractor does not support scanning - assuming source data available for {target_date}')

            # *** OPTIMIZATION: Create all dataframes ONCE per date ***
            dataframes = {}
            if source_data_available:
                with trace.get_tracer(__name__).start_as_current_span(SpanNames.ROLLUP_DATAFRAME_PROCESSING) as dataframe_span:
                    add_span_attributes(
                        dataframe_span, **{SpanAttributes.ROLLUP_DATE: target_date.isoformat(), 'dataframe.factory.type': 'DataframeFactory'}
                    )

                    # Use unified data loading directly (avoid auto-rollup computation that RollupDataframeFactory.create does)
                    from metrics_utility.automation_controller_billing.dataframe_engine.rollup_factory import RollupDataframeFactory

                    factory = RollupDataframeFactory(extractor=extractor, month=month, extra_params=date_params)
                    dataframes = factory._create_with_unified_data_loading()
            else:
                self.logger.info(f'No source data available for {target_date} - will create no_source_data status for all dataframes')

            # Map standard factory output names to proper class names
            name_mapping = {
                'job_host_summary': 'DataframeJobhostSummaryUsage',
                'main_jobevent': 'DataframeContentUsage',
                'main_host': 'DataframeInventoryScope',
                'data_collection_status': 'DataframeCollectionStatus',
                'host_metric': 'DataframeHostMetric',
            }

            # Process each dataframe from the shared factory output
            batch_results = {}

            for dataframe_name in dataframe_names:
                try:
                    with trace.get_tracer(__name__).start_as_current_span(SpanNames.DATA_TRANSFORMATION) as transform_span:
                        add_span_attributes(
                            transform_span,
                            **{SpanAttributes.ROLLUP_DATE: target_date.isoformat(), SpanAttributes.ROLLUP_DATAFRAME_NAME: dataframe_name},
                        )

                        # Check if source data is available for this date
                        if not source_data_available:
                            # Save no_source_data status - no tarballs found for this date
                            version = rollup_manager.save_no_source_data_metadata(target_date, dataframe_name)
                            self.logger.debug(f'Saved no_source_data rollup {dataframe_name} for {target_date} as version {version}')
                            batch_results[dataframe_name] = True
                            continue

                        # Find the specific dataframe we need to save
                        # Reverse lookup: from class name to factory output name
                        class_to_factory = {v: k for k, v in name_mapping.items()}
                        factory_name = class_to_factory.get(dataframe_name)

                        if factory_name is None:
                            raise ValueError(f'No factory mapping found for dataframe class {dataframe_name}')

                        if factory_name not in dataframes:
                            raise ValueError(f'Factory output {factory_name} for {dataframe_name} not found in dataframes: {list(dataframes.keys())}')

                        dataframe_obj = dataframes[factory_name]

                        if dataframe_obj is None:
                            # Save no_data status - no dataframe object available but tarballs existed
                            version = rollup_manager.save_no_data_metadata(target_date, dataframe_name)
                            self.logger.debug(f'Saved no_data rollup {dataframe_name} for {target_date} as version {version}')
                            batch_results[dataframe_name] = True
                            continue

                        # Get the actual dataframe data - RollupDataframeFactory already built the dataframes
                        if hasattr(dataframe_obj, 'get_cached_dataframe'):
                            dataframe = dataframe_obj.get_cached_dataframe()
                        else:
                            dataframe = dataframe_obj

                        if dataframe is None or (hasattr(dataframe, 'empty') and dataframe.empty):
                            # Save no_data status - dataframe exists but is empty/None, tarballs existed
                            version = rollup_manager.save_no_data_metadata(target_date, dataframe_name)
                            self.logger.debug(f'Saved no_data rollup {dataframe_name} for {target_date} as version {version}')
                            batch_results[dataframe_name] = True
                            continue

                    # Save the rollup data using the rollup manager
                    processing_time = time.time() - start_time
                    version = rollup_manager.save_rollup_data(target_date, dataframe_name, dataframe, len(dataframe), processing_time)

                    self.logger.debug(f'Saved rollup {dataframe_name} for {target_date} as version {version} with {len(dataframe)} records')
                    batch_results[dataframe_name] = True

                except Exception as e:
                    self.logger.error(f'Failed to process {dataframe_name} for {target_date}: {e}')
                    rollup_manager.save_error_metadata(target_date, dataframe_name, str(e))
                    batch_results[dataframe_name] = False

            # Add final metrics to task span
            successful_count = sum(1 for success in batch_results.values() if success)
            total_processing_time = time.time() - start_time

            add_span_attributes(
                task_span,
                **{
                    'rollup.batch.successful_dataframes': successful_count,
                    'rollup.batch.total_dataframes': len(dataframe_names),
                    'rollup.batch.processing_time': total_processing_time,
                    'rollup.batch.success_rate': successful_count / len(dataframe_names) if dataframe_names else 0,
                    'rollup.status': 'complete',
                },
            )

            return batch_results

    def _handle_ship_target(self, ship_target):
        """Handle ship target configuration"""
        if ship_target in ['controller_db', 'directory']:
            handle_not_crc()
            handle_not_s3()
            return handle_directory_ship_target()
        elif ship_target == 's3':
            handle_not_crc()
            return handle_s3_ship_target()
        else:
            allowed = ', '.join(['controller_db', 'directory', 's3'])
            raise BadShipTarget(f'Unexpected value for METRICS_UTILITY_SHIP_TARGET env var ({ship_target}), allowed values: {allowed}')

    def _handle_extra_params(self, ship_target=None):
        """Handle extra parameters setup"""
        base = self._handle_ship_target(ship_target)

        report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE', None)
        price_per_node = float(os.getenv('METRICS_UTILITY_PRICE_PER_NODE', 0))

        if not report_type:
            raise MissingRequiredEnvVar('Missing required env variable METRICS_UTILITY_REPORT_TYPE.')

        if report_type not in ['CCSP', 'CCSPv2', 'RENEWAL_GUIDANCE']:
            raise BadRequiredEnvVar(
                "Bad value for required env variable METRICS_UTILITY_REPORT_TYPE, allowed values are: ['CCSP', 'CCSPv2', 'RENEWAL_GUIDANCE']"
            )

        base.update(
            {
                'report_type': report_type,
                'price_per_node': price_per_node,
                'report_organization_filter': os.getenv('METRICS_UTILITY_ORGANIZATION_FILTER', None),
                'optional_sheets': os.getenv(
                    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS',
                    'ccsp_summary,managed_nodes,usage_by_organizations,usage_by_collections,usage_by_roles,usage_by_modules',
                )
                .rstrip(',')
                .split(','),
            }
        )

        return base

    def _get_required_dataframes(self, report_type):
        """Get list of required dataframes based on report type"""
        if report_type == 'CCSP':
            return ['DataframeJobhostSummaryUsage', 'DataframeContentUsage', 'DataframeInventoryScope']
        elif report_type == 'CCSPv2':
            return ['DataframeJobhostSummaryUsage', 'DataframeContentUsage', 'DataframeInventoryScope', 'DataframeCollectionStatus']
        elif report_type == 'RENEWAL_GUIDANCE':
            return ['DataframeHostMetric']
        else:
            raise ValueError(f'Unknown report type: {report_type}')

    def _handle_parallel_plan(self, rollup_manager, since_date, until_date, required_dataframes, force, ship_target, extra_params):
        """Handle the --parallel option to show execution plan"""
        self.logger.info('Creating execution plan...')

        # Create extractor for source data scanning in parallel plan
        extra_params['deduplicator'] = os.getenv('METRICS_UTILITY_DEDUPLICATOR', None) or None
        extractor = ExtractorFactory(ship_target, extra_params).create()

        smart_plan = rollup_manager.create_smart_dependency_plan(since_date, until_date, required_dataframes, force=force, extractor=extractor)

        if not smart_plan:
            self.logger.info('No rollups need computation. All dates and dataframes are already processed.')
            return

        self._display_smart_execution_plan(smart_plan)
        total_dataframes = sum(len(date_group['dataframes']) for date_group in smart_plan)
        self.logger.info(f'Total dataframes: {total_dataframes} in {len(smart_plan)} date groups')
