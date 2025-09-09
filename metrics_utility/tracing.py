"""
OpenTelemetry tracing configuration for metrics utility.
"""

import logging
import os

from functools import wraps
from typing import Optional

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.django import DjangoInstrumentor
from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


logger = logging.getLogger(__name__)

# Global tracer instance
tracer: Optional[trace.Tracer] = None


def setup_tracing():
    """
    Initialize OpenTelemetry tracing for the metrics utility.
    """
    global tracer

    # Check if tracing should be enabled
    if not os.getenv('OTEL_TRACES_ENABLED', 'false').lower() == 'true':
        logger.info('OpenTelemetry tracing is disabled')
        tracer = trace.NoOpTracer()
        return

    # Configure resource attributes with configurable service naming
    # Default service name follows the pattern: metrics-service-{instance}
    default_service_name = 'metrics-service'
    service_instance = os.getenv('METRICS_UTILITY_INSTANCE_ID', os.getenv('HOSTNAME', 'default'))
    if service_instance and service_instance != 'default':
        default_service_name = f'metrics-service-{service_instance}'

    resource = Resource.create(
        {
            'service.name': os.getenv('OTEL_SERVICE_NAME', default_service_name),
            'service.version': os.getenv('OTEL_SERVICE_VERSION', '0.1.0'),
            'service.instance.id': os.getenv('OTEL_SERVICE_INSTANCE_ID', service_instance),
            'deployment.environment': os.getenv('OTEL_ENVIRONMENT', 'production'),
            # Additional attributes for multi-instance deployments
            'metrics.utility.controller.id': os.getenv('METRICS_UTILITY_CONTROLLER_ID', ''),
            'metrics.utility.cluster.name': os.getenv('METRICS_UTILITY_CLUSTER_NAME', ''),
            'metrics.utility.region': os.getenv('METRICS_UTILITY_REGION', ''),
        }
    )

    # Set up tracer provider
    trace.set_tracer_provider(TracerProvider(resource=resource))

    # Configure OTLP exporter
    otlp_endpoint = os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://localhost:4317')
    otlp_headers = os.getenv('OTEL_EXPORTER_OTLP_HEADERS', '')

    exporter = OTLPSpanExporter(
        endpoint=otlp_endpoint, headers=dict(h.split('=') for h in otlp_headers.split(',') if '=' in h) if otlp_headers else None, insecure=True
    )

    # Add span processor
    span_processor = BatchSpanProcessor(exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)

    # Initialize automatic instrumentations
    try:
        DjangoInstrumentor().instrument()
        RequestsInstrumentor().instrument()
        PsycopgInstrumentor().instrument()
        logger.info('Initialized OpenTelemetry automatic instrumentations')
    except Exception as e:
        logger.warning(f'Failed to initialize some automatic instrumentations: {e}')

    # Get tracer instance
    tracer = trace.get_tracer(__name__)

    logger.info(f'OpenTelemetry tracing initialized with endpoint: {otlp_endpoint}')


def get_tracer() -> trace.Tracer:
    """Get the global tracer instance."""
    global tracer
    if tracer is None:
        setup_tracing()
    return tracer


def traced_method(operation_name: Optional[str] = None, **span_attributes):
    """
    Decorator to automatically trace method calls.

    Args:
        operation_name: Name for the span (defaults to method name)
        **span_attributes: Additional attributes to add to the span
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_tracer = get_tracer()
            span_name = operation_name or f'{func.__module__}.{func.__qualname__}'

            with current_tracer.start_as_current_span(span_name) as span:
                # Add default attributes
                span.set_attribute('function.name', func.__name__)
                span.set_attribute('function.module', func.__module__)

                # Add custom attributes
                for key, value in span_attributes.items():
                    span.set_attribute(key, str(value))

                # Add method arguments as attributes (be careful with sensitive data)
                if args and hasattr(args[0], '__class__'):
                    span.set_attribute('class.name', args[0].__class__.__name__)

                try:
                    result = func(*args, **kwargs)
                    span.set_status(trace.Status(trace.StatusCode.OK))
                    return result
                except Exception as e:
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise

        return wrapper

    return decorator


def add_span_attributes(span: trace.Span, **attributes):
    """Helper to add multiple attributes to a span."""
    for key, value in attributes.items():
        if value is not None:
            span.set_attribute(key, str(value))


def create_child_span(name: str, **attributes) -> trace.Span:
    """Create a child span with the given name and attributes."""
    current_tracer = get_tracer()
    span = current_tracer.start_span(name)
    add_span_attributes(span, **attributes)
    return span


# Span names constants for consistency
class SpanNames:
    """Constants for span names used throughout the application."""

    # Rollup computation spans
    ROLLUP_COMPUTATION = 'rollup.computation'
    ROLLUP_DAG_CREATION = 'rollup.dag.creation'
    ROLLUP_TASK_EXECUTION = 'rollup.task.execution'
    ROLLUP_DATAFRAME_PROCESSING = 'rollup.dataframe.processing'
    ROLLUP_PARQUET_SAVE = 'rollup.parquet.save'
    ROLLUP_MANIFEST_SAVE = 'rollup.manifest.save'

    # Report building spans
    REPORT_BUILD = 'report.build'
    REPORT_ROLLUP_LOADING = 'report.rollup.loading'
    REPORT_DATAFRAME_FACTORY = 'report.dataframe.factory'
    REPORT_DEDUPLICATION = 'report.deduplication'
    REPORT_SHEET_GENERATION = 'report.sheet.generation'
    REPORT_XLSX_SAVE = 'report.xlsx.save'

    # Rollup factory spans
    ROLLUP_FACTORY_INITIALIZATION = 'rollup.factory.initialization'
    ROLLUP_AVAILABILITY_CHECK = 'rollup.availability.check'
    ROLLUP_AUTO_COMPUTATION = 'rollup.auto.computation'
    ROLLUP_DATAFRAMES_LOADING = 'rollup.dataframes.loading'
    ROLLUP_FACTORY_STANDARD_FALLBACK = 'rollup.factory.standard_fallback'

    # Data processing spans
    DATA_EXTRACTION = 'data.extraction'
    DATA_DEDUPLICATION = 'data.deduplication'
    DATA_TRANSFORMATION = 'data.transformation'

    # Dataframe engine spans
    DATAFRAME_GROUP = 'dataframe.group'
    DATAFRAME_MERGE = 'dataframe.merge'
    DATAFRAME_REGROUP = 'dataframe.regroup'
    DATAFRAME_SCHEMA_APPLICATION = 'dataframe.schema.application'
    DATAFRAME_AGGREGATION_BUILD = 'dataframe.aggregation.build'
    DATAFRAME_CONCAT = 'dataframe.concat'
    DATAFRAME_ALIGNMENT = 'dataframe.alignment'
    DATAFRAME_COLUMN_MAPPING = 'dataframe.column.mapping'

    # Tarball processing spans
    TARBALL_EXTRACTION = 'tarball.extraction'
    TARBALL_CONFIG_LOADING = 'tarball.config.loading'
    CSV_PROCESSING = 'csv.processing'
    CSV_DATA_LOADING = 'csv.data.loading'

    # Storage spans
    STORAGE_READ = 'storage.read'
    STORAGE_WRITE = 'storage.write'
    STORAGE_S3_OPERATION = 'storage.s3.operation'

    # Database spans
    DB_QUERY = 'db.query'
    DB_CONNECTION = 'db.connection'


# Span attributes constants
class SpanAttributes:
    """Constants for span attributes used throughout the application."""

    # Rollup attributes
    ROLLUP_DATE = 'rollup.date'
    ROLLUP_SINCE_DATE = 'rollup.since.date'
    ROLLUP_UNTIL_DATE = 'rollup.until.date'
    ROLLUP_DATAFRAME_NAME = 'rollup.dataframe.name'
    ROLLUP_VERSION = 'rollup.version'
    ROLLUP_RECORDS_PROCESSED = 'rollup.records.processed'
    ROLLUP_PROCESSING_TIME = 'rollup.processing.time'
    ROLLUP_TASK_PRIORITY = 'rollup.task.priority'
    ROLLUP_DEPENDENCIES = 'rollup.task.dependencies'

    # Report attributes
    REPORT_TYPE = 'report.type'
    REPORT_DATE_RANGE = 'report.date.range'
    REPORT_SINCE_DATE = 'report.since.date'
    REPORT_UNTIL_DATE = 'report.until.date'
    REPORT_OUTPUT_PATH = 'report.output.path'
    REPORT_SHEET_COUNT = 'report.sheet.count'

    # Data attributes
    DATA_SOURCE = 'data.source'
    DATA_SIZE_BYTES = 'data.size.bytes'
    DATA_RECORD_COUNT = 'data.record.count'
    DATA_FORMAT = 'data.format'

    # Performance attributes
    PROCESSING_TIME_MS = 'processing.time.ms'
    MEMORY_USAGE_MB = 'memory.usage.mb'
    CPU_USAGE_PERCENT = 'cpu.usage.percent'

    # Dataframe engine attributes
    DATAFRAME_INPUT_RECORDS = 'dataframe.input.records'
    DATAFRAME_OUTPUT_RECORDS = 'dataframe.output.records'
    DATAFRAME_COMPRESSION_RATIO = 'dataframe.compression.ratio'
    DATAFRAME_AGGREGATION_COLUMNS = 'dataframe.aggregation.columns'
    DATAFRAME_INDEX_COLUMNS = 'dataframe.index.columns'
    DATAFRAME_CONCAT_METHOD = 'dataframe.concat.method'
    DATAFRAME_SCHEMA_ERRORS = 'dataframe.schema.errors'
    DATAFRAME_TYPE_CONVERSIONS = 'dataframe.type.conversions'
