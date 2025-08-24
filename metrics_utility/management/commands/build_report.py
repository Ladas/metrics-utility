import datetime
import os

from argparse import RawDescriptionHelpFormatter

from django.core.management.base import BaseCommand
from opentelemetry import trace

from metrics_utility.automation_controller_billing.dataframe_engine.rollup_factory import RollupDataframeFactory
from metrics_utility.automation_controller_billing.dedup.factory import Factory as DedupFactory
from metrics_utility.automation_controller_billing.extract.factory import Factory as ExtractorFactory
from metrics_utility.automation_controller_billing.report.factory import Factory as ReportFactory
from metrics_utility.automation_controller_billing.report_saver.factory import Factory as ReportSaverFactory
from metrics_utility.exceptions import BadRequiredEnvVar, BadShipTarget, MissingRequiredEnvVar
from metrics_utility.logger import debug, logger
from metrics_utility.management.validation import (
    date_format_text,
    handle_directory_ship_target,
    handle_env_validation,
    handle_month,
    handle_not_crc,
    handle_not_s3,
    handle_s3_ship_target,
    parse_number_of_days,
    validate_build_params,
)
from metrics_utility.tracing import SpanAttributes, SpanNames, add_span_attributes, get_tracer, traced_method


def get_report_path(ship_path, date):
    year = date.strftime('%Y')
    month = date.strftime('%m')

    return f'{ship_path}/reports/{year}/{month}'


def get_organization_filter():
    # handle None or empty string
    if not os.getenv('METRICS_UTILITY_ORGANIZATION_FILTER'):
        return None
    return os.getenv('METRICS_UTILITY_ORGANIZATION_FILTER').rstrip(';')


class Command(BaseCommand):
    """
    Build Report
    """

    help = 'Build Report'
    help_texts = {
        'since': (f'Start date for collection, including. {date_format_text.format(name="since")}'),
        'until': (f'End date for collection, including. {date_format_text.format(name="until")}'),
        'month': (
            'Month the report will be generated for, with format YYYY-MM. '
            "If this parameter is not provided, the previous month's report will be generated if it does not already exist."
        ),
        'ephemeral': (
            'Duration in months or days to determine if host is ephemeral. '
            'Months are considered as 30 days in duration. '
            'Example: --ephemeral=3months, or --ephemeral=3days'
        ),
        'force': ('With this option, the existing reports will be overwritten if running this command again.'),
        'verbose': ('Print debug information to console.'),
    }

    def create_parser(self, prog_name, subcommand, **kwargs):
        return super().create_parser(
            prog_name,
            subcommand,
            # ensure newlines are preserved in descriptions and epilog
            formatter_class=RawDescriptionHelpFormatter,
            epilog='\n'.join(
                [
                    'ENVIRONMENT',
                    '',
                    '  Core Configuration:',
                    "    METRICS_UTILITY_REPORT_TYPE (required): one of 'CCSPv2', 'CCSP', 'RENEWAL_GUIDANCE' - determines which kind of report we're generating",  # noqa: E501
                    "    METRICS_UTILITY_SHIP_TARGET (required): one of 'directory', 's3', 'controller_db' - input/output mechanism",
                    '    METRICS_UTILITY_SHIP_PATH (required): local or s3 directory path, input tarballs in path/data/, output xlsx in path/reports/',  # noqa: E501
                    '',
                    '  Optional Configuration:',
                    "    METRICS_UTILITY_DEDUPLICATOR (optional): one of 'ccsp', 'renewal', 'ccsp-experimental' - choice of deduplication algorithm",  # noqa: E501
                    '    METRICS_UTILITY_ORGANIZATION_FILTER (optional): CCSPv2 only, semicolon-separated list of org names to filter by',  # noqa: E501
                    '    METRICS_UTILITY_PRICE_PER_NODE (optional): price per node multiplier for cost calculations',
                    '    METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS (optional): enables optional report sheets, comma-separated list',  # noqa: E501
                    '    REPORT_RENEWAL_GUIDANCE_DEDUP_ITERATIONS (optional): max dedup iterations for renewal guidance (default: 3)',  # noqa: E501
                    '',
                    '  Report Customization (Optional):',
                    '    METRICS_UTILITY_REPORT_SKU (optional): SKU identifier for the report',
                    '    METRICS_UTILITY_REPORT_SKU_DESCRIPTION (optional): SKU description for the report',
                    '    METRICS_UTILITY_REPORT_H1_HEADING (optional): main heading for the report',
                    '    METRICS_UTILITY_REPORT_COMPANY_NAME (optional): company name for the report',
                    '    METRICS_UTILITY_REPORT_EMAIL (optional): contact email for the report',
                    '    METRICS_UTILITY_REPORT_RHN_LOGIN (optional): Red Hat Network login for the report',
                    '    METRICS_UTILITY_REPORT_PO_NUMBER (optional): purchase order number for the report',
                    '    METRICS_UTILITY_REPORT_COMPANY_BUSINESS_LEADER (optional): business leader name for the report',  # noqa: E501
                    '    METRICS_UTILITY_REPORT_COMPANY_PROCUREMENT_LEADER (optional): procurement leader name for the report',  # noqa: E501
                    '    METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME (optional): end user company name for the report',  # noqa: E501
                    '    METRICS_UTILITY_REPORT_END_USER_CITY (optional): end user company city for the report',
                    '    METRICS_UTILITY_REPORT_END_USER_STATE (optional): end user company state for the report',
                    '    METRICS_UTILITY_REPORT_END_USER_COUNTRY (optional): end user company country for the report',
                    '',
                    '  S3 Configuration:',
                    '    METRICS_UTILITY_BUCKET_NAME (optional): S3 bucket name',
                    '    METRICS_UTILITY_BUCKET_ENDPOINT (optional): S3 endpoint URL',
                    '    METRICS_UTILITY_BUCKET_ACCESS_KEY (optional): S3 access key',
                    '    METRICS_UTILITY_BUCKET_SECRET_KEY (optional): S3 secret key',
                    '    METRICS_UTILITY_BUCKET_REGION (optional): S3 region',
                    '',
                ]
            ),
            **kwargs,
        )

    def add_arguments(self, parser):
        parser.add_argument('--month', dest='month', action='store', help=self.help_texts.get('month'))
        parser.add_argument('--since', dest='since', action='store', help=self.help_texts.get('since'))
        parser.add_argument('--until', dest='until', action='store', help=self.help_texts.get('until'))
        parser.add_argument('--ephemeral', dest='ephemeral', action='store', help=self.help_texts.get('ephemeral'))
        parser.add_argument('--force', dest='force', action='store_true', help=self.help_texts.get('force'))
        parser.add_argument('--verbose', dest='verbose', action='store_true', help=self.help_texts.get('verbose'))

    @traced_method(SpanNames.REPORT_BUILD)
    def handle(self, *args, **options):
        if options.get('verbose'):
            debug()

        handle_env_validation('build')

        opt_since, opt_until = validate_build_params(options, self.help_texts)

        # Add span attributes for report parameters
        tracer = get_tracer()
        current_span = trace.get_current_span()
        add_span_attributes(
            current_span,
            **{
                SpanAttributes.REPORT_SINCE_DATE: opt_since.isoformat() if opt_since else None,
                SpanAttributes.REPORT_UNTIL_DATE: opt_until.isoformat() if opt_until else None,
            },
        )

        opt_month, month, next_month = handle_month(options.get('month') or None)
        opt_ephemeral = parse_number_of_days(options.get('ephemeral'))
        opt_force = options.get('force')

        ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET')

        # FIXME: separate params per factory
        extra_params = self._handle_extra_params(ship_target)
        extra_params['opt_since'] = opt_since
        extra_params['opt_until'] = opt_until
        extra_params['ephemeral_days'] = opt_ephemeral
        extra_params['month_since'] = month
        extra_params['month_until'] = next_month
        extra_params['deduplicator'] = os.getenv('METRICS_UTILITY_DEDUPLICATOR') or None

        # Add report configuration to span
        add_span_attributes(
            current_span,
            **{
                SpanAttributes.REPORT_TYPE: extra_params.get('report_type', 'unknown'),
                'ship.target': ship_target,
                'ship.path': extra_params.get('ship_path', 'unknown'),
                'report.force': opt_force,
            },
        )

        # Determine destination path for generated report and skip processing if it exists
        report_type = extra_params['report_type']
        ship_path = extra_params['ship_path']
        if opt_since is not None:
            since_date = opt_since.date()
            until_date = opt_until.date() if opt_until else datetime.date.today()

            extra_params['since_date'] = since_date
            extra_params['until_date'] = until_date

            extra_params['report_period'] = f'{since_date}, {until_date}'
            extra_params['report_spreadsheet_destination_path'] = os.path.join(
                get_report_path(ship_path, until_date),
                f'{report_type}-{since_date}--{until_date}.xlsx',
            )
        else:
            extra_params['report_period'] = opt_month
            extra_params['report_spreadsheet_destination_path'] = os.path.join(
                get_report_path(ship_path, month),
                f'{report_type}-{opt_month}.xlsx',
            )

        report_saver_engine = ReportSaverFactory(ship_target, extra_params=extra_params).create()

        if report_saver_engine.report_exist() and not opt_force:
            # If the monthly report already exists, skip the generation
            logger.info(
                'Skipping report generation, report: '
                f'{report_saver_engine.report_spreadsheet_destination_path} already exists. '
                'Use --force option to override the report.'
            )
            return

        with tracer.start_as_current_span(SpanNames.DATA_EXTRACTION) as extraction_span:
            add_span_attributes(extraction_span, **{'extraction.ship_target': ship_target})
            extractor = ExtractorFactory(ship_target, extra_params).create()

        # Use rollup dataframe factory with unified data loading (Phase 1)
        with tracer.start_as_current_span(SpanNames.REPORT_DATAFRAME_FACTORY) as dataframe_span:
            add_span_attributes(
                dataframe_span,
                **{
                    SpanAttributes.REPORT_TYPE: extra_params.get('report_type', 'unknown'),
                    'dataframe.loading.method': 'rollup_factory_unified_data_loader',
                    'dataframe.optimization': 'single_tarball_read',
                },
            )
            dataframe_factory = RollupDataframeFactory(extractor=extractor, month=month, extra_params=extra_params)
            dataframes = dataframe_factory.create()

        with tracer.start_as_current_span(SpanNames.REPORT_DEDUPLICATION) as dedup_span:
            add_span_attributes(dedup_span, **{'deduplication.algorithm': extra_params.get('deduplicator', 'default')})
            
            # Create dataframe instances for deduplication (needed to call dedup methods)
            dataframe_instances = {}
            dataframes_by_class_name = {}
            
            # Map standard names to class names for deduplication
            class_name_mapping = {
                'job_host_summary': 'DataframeJobhostSummaryUsage',
                'main_jobevent': 'DataframeContentUsage', 
                'main_host': 'DataframeInventoryScope',
                'data_collection_status': 'DataframeCollectionStatus'
            }
            
            for df_name, dataframe_data in dataframes.items():
                if dataframe_data is not None:
                    class_name = class_name_mapping.get(df_name, df_name)
                    
                    # Create dataframe instance
                    dataframe_class = self._get_dataframe_class_for_dedup(class_name)
                    if dataframe_class:
                        dataframe_instances[class_name] = dataframe_class(extractor=extractor, month=month, extra_params=extra_params)
                        dataframes_by_class_name[class_name] = dataframe_data
            
            # Create dedup factory and pass DataFrames keyed by class names
            dedup = DedupFactory(dataframes=dataframes_by_class_name, extra_params=extra_params).create()
            
            # Set up the deduplicator with both the actual DataFrames and the instances
            dedup.dataframes = dataframes_by_class_name
            dedup.dataframe_instances = dataframe_instances
            
            deduplicated_dataframes = dedup.run()
            
            # Map deduplicated results back to standard names for reports
            reverse_mapping = {v: k for k, v in class_name_mapping.items()}
            dataframes = {}
            for class_name, deduped_df in deduplicated_dataframes.items():
                standard_name = reverse_mapping.get(class_name, class_name)
                dataframes[standard_name] = deduped_df

        # Check if we have any data, but allow partial reports
        non_empty_dataframes = [df for df in dataframes.values() if df is not None and not df.empty]
        if not non_empty_dataframes:
            if opt_since is not None:
                logger.warning(f'No billing data found for input date range {since_date}--{until_date}')
                logger.warning('All dataframes are empty - this may indicate missing data or configuration issues')
            else:
                logger.warning(f'No billing data found for month {opt_month}')
                logger.warning('All dataframes are empty - this may indicate missing data or configuration issues')
        else:
            logger.info(f'Found data in {len(non_empty_dataframes)} out of {len(dataframes)} dataframes')
            if len(non_empty_dataframes) < len(dataframes):
                missing_dataframes = [name for name, df in dataframes.items() if df is None or (hasattr(df, 'empty') and df.empty)]
                logger.info(f'Some dataframes have no data: {missing_dataframes}')
                logger.info('Generating report with available data')

        with tracer.start_as_current_span(SpanNames.REPORT_SHEET_GENERATION) as sheet_span:
            add_span_attributes(sheet_span, **{'report.dataframe_count': len([df for df in dataframes.values() if df is not None and not df.empty])})
            report_engine = ReportFactory(dataframes=dataframes, extra_params=extra_params).create()
            report_spreadsheet = report_engine.build_spreadsheet()

        # Save the report to the configured destination
        with tracer.start_as_current_span(SpanNames.REPORT_XLSX_SAVE) as save_span:
            add_span_attributes(save_span, **{SpanAttributes.REPORT_OUTPUT_PATH: report_saver_engine.report_spreadsheet_destination_path})
            report_saver_engine.save(report_spreadsheet)

        # Add final report metrics to main span
        add_span_attributes(current_span, **{SpanAttributes.REPORT_OUTPUT_PATH: report_saver_engine.report_spreadsheet_destination_path})

        logger.info(f'Report generated into {ship_target}: {report_saver_engine.report_spreadsheet_destination_path}')

    def _handle_ship_target(self, ship_target):
        if ship_target in ['controller_db', 'directory']:
            # controller_db is just directory but with different extractor
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
        base = self._handle_ship_target(ship_target)

        report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE')
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
                'optional_sheets': os.getenv(
                    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS',
                    'ccsp_summary,managed_nodes,usage_by_organizations,usage_by_collections,usage_by_roles,usage_by_modules',
                )
                .rstrip(',')
                .split(','),
                # XLSX specific params
                'report_sku': os.getenv('METRICS_UTILITY_REPORT_SKU', ''),
                'report_sku_description': os.getenv('METRICS_UTILITY_REPORT_SKU_DESCRIPTION', ''),
                'report_h1_heading': os.getenv('METRICS_UTILITY_REPORT_H1_HEADING', ''),
                'report_company_name': os.getenv('METRICS_UTILITY_REPORT_COMPANY_NAME', ''),
                'report_email': os.getenv('METRICS_UTILITY_REPORT_EMAIL', ''),
                'report_rhn_login': os.getenv('METRICS_UTILITY_REPORT_RHN_LOGIN', ''),
                'report_po_number': os.getenv('METRICS_UTILITY_REPORT_PO_NUMBER', ''),
                'report_company_business_leader': os.getenv('METRICS_UTILITY_REPORT_COMPANY_BUSINESS_LEADER', ''),
                'report_company_procurement_leader': os.getenv('METRICS_UTILITY_REPORT_COMPANY_PROCUREMENT_LEADER', ''),
                'report_end_user_company_name': os.getenv('METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME', ''),
                'report_end_user_company_city': os.getenv('METRICS_UTILITY_REPORT_END_USER_CITY', ''),
                'report_end_user_company_state': os.getenv('METRICS_UTILITY_REPORT_END_USER_STATE', ''),
                'report_end_user_company_country': os.getenv('METRICS_UTILITY_REPORT_END_USER_COUNTRY', ''),
                # Renewal guidance specific params
                'report_renewal_guidance_dedup_iterations': os.getenv('REPORT_RENEWAL_GUIDANCE_DEDUP_ITERATIONS', '3'),
                'report_organization_filter': get_organization_filter(),
                # optional bits
                'optional_sheets': os.getenv(
                    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS',
                    'ccsp_summary,managed_nodes,usage_by_organizations,usage_by_collections,usage_by_roles,usage_by_modules',
                )
                .rstrip(',')
                .split(','),
            }
        )
        return base

    def _get_dataframe_class_for_dedup(self, class_name):
        """Get the dataframe class for deduplication instance creation"""
        try:
            if class_name == 'DataframeJobhostSummaryUsage':
                from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_jobhost_summary_usage import DataframeJobhostSummaryUsage
                return DataframeJobhostSummaryUsage
            elif class_name == 'DataframeContentUsage':
                from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_content_usage import DataframeContentUsage
                return DataframeContentUsage
            elif class_name == 'DataframeInventoryScope':
                from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_inventory_scope import DataframeInventoryScope
                return DataframeInventoryScope
            elif class_name == 'DataframeCollectionStatus':
                from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_collection_status import DataframeCollectionStatus
                return DataframeCollectionStatus
            else:
                return None
        except ImportError:
            return None
