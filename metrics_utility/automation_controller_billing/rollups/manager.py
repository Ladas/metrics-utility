import json
import logging
import os
import re

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from opentelemetry import trace

from metrics_utility.tracing import SpanAttributes, SpanNames, add_span_attributes, traced_method


class ComputationStatus(Enum):
    """Enumeration for rollup computation status"""

    COMPLETE = 'complete'
    PARTIAL = 'partial'
    MISSING = 'missing'
    ERROR = 'error'
    NO_SOURCE_DATA = 'no_source_data'


@dataclass
class RollupManifest:
    """Data class representing a rollup manifest for a single dataframe"""

    computation_timestamp: datetime
    processing_status: ComputationStatus
    dataframe_name: str
    since_date: date
    until_date: date
    records_processed: int
    processing_time_seconds: float
    version: str
    latest_version: str
    error_message: Optional[str] = None

    def to_dict(self) -> Dict:
        """Convert manifest to dictionary for JSON serialization"""
        return {
            'computation_timestamp': self.computation_timestamp.isoformat(),
            'processing_status': self.processing_status.value,
            'dataframe_name': self.dataframe_name,
            'since_date': self.since_date.isoformat(),
            'until_date': self.until_date.isoformat(),
            'records_processed': self.records_processed,
            'processing_time_seconds': self.processing_time_seconds,
            'version': self.version,
            'latest_version': self.latest_version,
            'error_message': self.error_message,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'RollupManifest':
        """Create manifest from dictionary"""
        # Handle backward compatibility for old manifest format
        if 'dataframes' in data:
            # Old format - convert to new format
            dataframe_name = data.get('dataframes', ['Unknown'])[0]
        else:
            dataframe_name = data['dataframe_name']

        # Handle version fields
        version = data.get('version', '1.0')
        latest_version = data.get('latest_version', version)

        return cls(
            computation_timestamp=datetime.fromisoformat(data['computation_timestamp']),
            processing_status=ComputationStatus(data['processing_status']),
            dataframe_name=dataframe_name,
            since_date=date.fromisoformat(data['since_date']),
            until_date=date.fromisoformat(data['until_date']),
            records_processed=data['records_processed'],
            processing_time_seconds=data['processing_time_seconds'],
            version=version,
            latest_version=latest_version,
            error_message=data.get('error_message'),
        )


@dataclass
class RollupTask:
    """Represents a single rollup computation task"""

    target_date: date
    dataframe_name: str
    dependencies: List[str]  # List of dataframe names this depends on
    priority: int = 0  # Lower number = higher priority


@dataclass
class BatchRollupTask:
    """Represents a batch of rollup computation tasks for the same date - Phase 1 optimization"""

    target_date: date
    dataframe_names: List[str]  # All dataframes to compute for this date
    max_priority: int = 0  # Highest priority among constituent tasks


class RollupManager:
    """Manager class for handling versioned rollup computation, scanning, and manifest management"""

    def __init__(self, ship_path: str):
        self.ship_path = ship_path
        self.rollups_path = os.path.join(ship_path, 'rollups', 'daily')
        self.logger = logging.getLogger(__name__)

        # Define dataframe dependencies for DAG computation
        self.dataframe_dependencies = {
            'DataframeJobhostSummaryUsage': [],  # No dependencies
            'DataframeContentUsage': [],  # No dependencies
            'DataframeInventoryScope': [],  # No dependencies
            'DataframeCollectionStatus': ['DataframeJobhostSummaryUsage'],  # Depends on job host summary
        }

    def get_rollup_directory(self, target_date: date) -> str:
        """Get the directory path for a specific date's rollups"""
        year = target_date.strftime('%Y')
        month = target_date.strftime('%m')
        day = target_date.strftime('%d')
        return os.path.join(self.rollups_path, year, month, day)

    def get_dataframe_directory(self, target_date: date, dataframe_name: str) -> str:
        """Get the directory path for a specific dataframe's rollups"""
        return os.path.join(self.get_rollup_directory(target_date), dataframe_name)

    def get_version_directory(self, target_date: date, dataframe_name: str, version: str) -> str:
        """Get the directory path for a specific version of a dataframe rollup"""
        return os.path.join(self.get_dataframe_directory(target_date, dataframe_name), version)

    def generate_version_string(self) -> str:
        """Generate a new version string based on current timestamp with microseconds"""
        now = datetime.now()
        return f'{now.strftime("%Y%m%d_%H%M%S_%f")}_v1.0'

    def get_metadata_path(self, target_date: date, dataframe_name: str, version: str) -> str:
        """Get the metadata file path for a specific version"""
        return os.path.join(self.get_version_directory(target_date, dataframe_name, version), 'metadata.json')

    def ensure_dataframe_directory(self, target_date: date, dataframe_name: str) -> str:
        """Ensure the dataframe directory exists for the given date and dataframe"""
        directory = self.get_dataframe_directory(target_date, dataframe_name)
        Path(directory).mkdir(parents=True, exist_ok=True)
        return directory

    def ensure_version_directory(self, target_date: date, dataframe_name: str, version: str) -> str:
        """Ensure the version directory exists for the given date, dataframe, and version"""
        directory = self.get_version_directory(target_date, dataframe_name, version)
        Path(directory).mkdir(parents=True, exist_ok=True)
        return directory

    def load_metadata(self, target_date: date, dataframe_name: str, version: str) -> Optional[Dict]:
        """Load metadata for a specific version"""
        metadata_path = self.get_metadata_path(target_date, dataframe_name, version)
        if not os.path.exists(metadata_path):
            return None

        try:
            with open(metadata_path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            self.logger.warning(f'Failed to load metadata for {target_date}/{dataframe_name}/{version}: {e}')
            return None

    def has_valid_rollup(self, target_date: date, dataframe_name: str) -> bool:
        """Check if a valid rollup exists for the given date and dataframe"""
        latest_version = self.get_latest_version(target_date, dataframe_name)
        return latest_version is not None

    def scan_rollups_directory(self, since_date: date, until_date: date, dataframe_names: List[str]) -> Dict[Tuple[date, str], ComputationStatus]:
        """
        Scan the rollups directory and determine computation status for each date/dataframe combination

        Returns:
            Dictionary mapping (date, dataframe_name) tuples to their computation status
        """
        status_map = {}
        current_date = since_date

        while current_date <= until_date:
            for dataframe_name in dataframe_names:
                key = (current_date, dataframe_name)

                # Get all available versions for this dataframe/date
                all_versions = self.get_available_versions(current_date, dataframe_name)

                if not all_versions:
                    # No versions found at all
                    status_map[key] = ComputationStatus.MISSING
                    continue

                # Check for status versions first (no_source_data, no_data, error)
                status_versions = [v for v in all_versions if '__status__' in v]

                if status_versions:
                    # Use the latest status version
                    latest_status_version = status_versions[0]  # Already sorted latest first

                    if '__status__no_source_data' in latest_status_version:
                        # Mark as complete - this date was processed but had no source data
                        status_map[key] = ComputationStatus.COMPLETE
                    elif '__status__no_data' in latest_status_version:
                        # Mark as complete - this date was processed but had no data (tarballs existed but empty)
                        status_map[key] = ComputationStatus.COMPLETE
                    elif '__status__error' in latest_status_version:
                        status_map[key] = ComputationStatus.ERROR
                    else:
                        status_map[key] = ComputationStatus.PARTIAL
                    continue

                # No status versions, check for regular data versions
                latest_version = self.get_latest_version(current_date, dataframe_name)

                if latest_version is None:
                    # No valid data versions found
                    status_map[key] = ComputationStatus.MISSING
                else:
                    # Check if the latest version has data.parquet
                    latest_version_dir = self.get_version_directory(current_date, dataframe_name, latest_version)
                    data_file = os.path.join(latest_version_dir, 'data.parquet')

                    if not os.path.exists(data_file):
                        self.logger.warning(f'Missing data file for {current_date}/{dataframe_name}: {data_file}')
                        status_map[key] = ComputationStatus.PARTIAL
                    else:
                        # Check metadata for status if available
                        metadata = self.load_metadata(current_date, dataframe_name, latest_version)
                        if metadata:
                            processing_status = metadata.get('processing_status', 'complete')
                            if processing_status == 'error':
                                status_map[key] = ComputationStatus.ERROR
                            elif processing_status == 'partial':
                                status_map[key] = ComputationStatus.PARTIAL
                            else:
                                status_map[key] = ComputationStatus.COMPLETE
                        else:
                            # No metadata but data file exists - assume complete
                            status_map[key] = ComputationStatus.COMPLETE

            current_date += timedelta(days=1)

        return status_map

    def get_available_versions(self, target_date: date, dataframe_name: str) -> List[str]:
        """Get all available versions for a specific dataframe, sorted by timestamp (latest first)"""
        dataframe_dir = self.get_dataframe_directory(target_date, dataframe_name)
        if not os.path.exists(dataframe_dir):
            return []

        versions = []
        for item in os.listdir(dataframe_dir):
            item_path = os.path.join(dataframe_dir, item)
            # Look for timestamp_v1.0 format directories (including status versions with suffix)
            if os.path.isdir(item_path) and (
                re.match(r'^\d{8}_\d{6}_\d{6}_v\d+\.\d+$', item)
                or re.match(r'^\d{8}_\d{6}_\d{6}_v\d+\.\d+__status__(error|no_data|no_source_data)$', item)
            ):
                versions.append(item)

        # Sort versions by timestamp prefix (latest first)
        # Extract timestamp from regular and status versions for sorting
        def extract_timestamp(version):
            if '__status__' in version:
                # Extract timestamp from YYYYMMDD_HHMMSS_FFFFFF_v1.0__status__*
                return version[:22]  # Get timestamp portion before __status__*
            else:
                # Extract timestamp from YYYYMMDD_HHMMSS_FFFFFF_v1.0
                return version[:22]

        return sorted(versions, key=extract_timestamp, reverse=True)

    def get_latest_version(self, target_date: date, dataframe_name: str) -> Optional[str]:
        """Get the latest version for a specific dataframe that has data.parquet (excludes error versions)"""
        versions = self.get_available_versions(target_date, dataframe_name)

        # Find first valid version that has data.parquet file
        for version in versions:
            # Skip status versions (error, no_data) when looking for valid data
            if '__status__' in version:
                continue

            version_dir = self.get_version_directory(target_date, dataframe_name, version)
            data_file = os.path.join(version_dir, 'data.parquet')
            if os.path.exists(data_file):
                return version

        return None

    @traced_method(SpanNames.ROLLUP_DAG_CREATION)
    def create_rollup_dag(self, since_date: date, until_date: date, required_dataframes: List[str], force: bool = False) -> List[RollupTask]:
        """
        Create a DAG of rollup computation tasks based on dependencies

        Returns:
            List of RollupTask objects ordered by dependencies and priority
        """
        current_span = trace.get_current_span()

        add_span_attributes(
            current_span,
            **{
                SpanAttributes.ROLLUP_SINCE_DATE: since_date.isoformat(),
                SpanAttributes.ROLLUP_UNTIL_DATE: until_date.isoformat(),
                'rollup.dataframes.required': ','.join(required_dataframes),
                'rollup.force': force,
            },
        )
        tasks = []

        # Get computation status for all date/dataframe combinations
        status_map = self.scan_rollups_directory(since_date, until_date, required_dataframes)

        # Create tasks for missing/incomplete rollups
        current_date = since_date
        while current_date <= until_date:
            for dataframe_name in required_dataframes:
                key = (current_date, dataframe_name)
                status = status_map.get(key, ComputationStatus.MISSING)

                # Add task if force is True or if rollup is missing/incomplete
                if force or status in [ComputationStatus.MISSING, ComputationStatus.PARTIAL, ComputationStatus.ERROR]:
                    dependencies = self.dataframe_dependencies.get(dataframe_name, [])
                    # Set priority based on dependencies (fewer dependencies = higher priority)
                    priority = len(dependencies)

                    tasks.append(RollupTask(target_date=current_date, dataframe_name=dataframe_name, dependencies=dependencies, priority=priority))

            current_date += timedelta(days=1)

        # Sort tasks by date first, then by priority (dependencies)
        sorted_tasks = sorted(tasks, key=lambda t: (t.target_date, t.priority))

        # Add DAG metrics to span
        add_span_attributes(
            current_span,
            **{
                'rollup.dag.task_count': len(sorted_tasks),
                'rollup.dag.date_count': len(set(t.target_date for t in sorted_tasks)),
                'rollup.dag.max_priority': max((t.priority for t in sorted_tasks), default=0),
            },
        )

        return sorted_tasks

    @traced_method(SpanNames.ROLLUP_DAG_CREATION)
    def create_batched_rollup_dag(
        self, since_date: date, until_date: date, required_dataframes: List[str], force: bool = False
    ) -> List[BatchRollupTask]:
        """
        Create a batched DAG of rollup computation tasks - Phase 1 optimization
        Groups tasks by date to eliminate duplicate processing

        Returns:
            List of BatchRollupTask objects ordered by date
        """
        current_span = trace.get_current_span()

        add_span_attributes(
            current_span,
            **{
                SpanAttributes.ROLLUP_SINCE_DATE: since_date.isoformat(),
                SpanAttributes.ROLLUP_UNTIL_DATE: until_date.isoformat(),
                'rollup.dataframes.required': ','.join(required_dataframes),
                'rollup.force': force,
                'rollup.optimization': 'batched_same_date',
            },
        )

        batch_tasks = []

        # Get computation status for all date/dataframe combinations
        status_map = self.scan_rollups_directory(since_date, until_date, required_dataframes)

        # Group by date and collect dataframes that need computation
        current_date = since_date
        while current_date <= until_date:
            dataframes_needed = []
            max_priority = 0

            for dataframe_name in required_dataframes:
                key = (current_date, dataframe_name)
                status = status_map.get(key, ComputationStatus.MISSING)

                # Add dataframe if force is True or if rollup is missing/incomplete
                if force or status in [ComputationStatus.MISSING, ComputationStatus.PARTIAL, ComputationStatus.ERROR]:
                    dataframes_needed.append(dataframe_name)
                    # Calculate priority based on dependencies
                    dependencies = self.dataframe_dependencies.get(dataframe_name, [])
                    priority = len(dependencies)
                    max_priority = max(max_priority, priority)

            # Create batch task if any dataframes need computation for this date
            if dataframes_needed:
                batch_tasks.append(BatchRollupTask(target_date=current_date, dataframe_names=dataframes_needed, max_priority=max_priority))

            current_date += timedelta(days=1)

        # Sort batch tasks by date (natural order for daily processing)
        sorted_batch_tasks = sorted(batch_tasks, key=lambda t: t.target_date)

        # Add DAG metrics to span
        total_dataframes = sum(len(task.dataframe_names) for task in sorted_batch_tasks)
        add_span_attributes(
            current_span,
            **{
                'rollup.dag.batch_count': len(sorted_batch_tasks),
                'rollup.dag.total_dataframes': total_dataframes,
                'rollup.dag.date_count': len(sorted_batch_tasks),
                'rollup.dag.max_priority': max((t.max_priority for t in sorted_batch_tasks), default=0),
                'rollup.optimization.efficiency_gain': f'{(total_dataframes - len(sorted_batch_tasks)) / max(total_dataframes, 1):.2%}'
                if total_dataframes > 0
                else '0%',
            },
        )

        return sorted_batch_tasks

    @traced_method(SpanNames.ROLLUP_DAG_CREATION)
    def create_smart_dependency_plan(
        self, since_date: date, until_date: date, required_dataframes: List[str], force: bool = False, extractor=None
    ) -> List[Dict]:
        """
        Create a smart dependency execution plan with source data scanning
        Eliminates DAG overhead by processing all dataframes for each date together
        Includes incremental update detection based on source data timestamps

        Args:
            since_date: Start date for planning
            until_date: End date for planning
            required_dataframes: List of dataframe names to process
            force: If True, recompute all rollups regardless of status
            extractor: Extractor instance for source data scanning

        Returns:
            List of date group dictionaries with format: [{'date': date, 'dataframes': [str], 'stale_info': {}}]
        """
        current_span = trace.get_current_span()

        add_span_attributes(
            current_span,
            **{
                SpanAttributes.ROLLUP_SINCE_DATE: since_date.isoformat(),
                SpanAttributes.ROLLUP_UNTIL_DATE: until_date.isoformat(),
                'rollup.dataframes.required': ','.join(required_dataframes),
                'rollup.force': force,
                'rollup.optimization': 'smart_dependency_resolution_with_source_scanning',
            },
        )

        smart_plan = []

        # Get computation status for all date/dataframe combinations
        status_map = self.scan_rollups_directory(since_date, until_date, required_dataframes)

        # Scan source data timestamps if extractor is provided
        source_data_map = {}
        stale_rollups = {}
        if extractor is not None:
            self.logger.info('Scanning source data timestamps for incremental updates...')
            source_data_map = self.scan_source_data_timestamps(since_date, until_date, extractor)
            stale_rollups = self.identify_stale_rollups(since_date, until_date, required_dataframes, source_data_map)

            add_span_attributes(
                current_span,
                **{
                    'rollup.source_scan.total_dates_with_data': len([d for d, tarballs in source_data_map.items() if tarballs]),
                    'rollup.source_scan.stale_rollups': len(stale_rollups),
                },
            )

        # Create date groups - Phase 2 eliminates intra-date dependencies
        current_date = since_date
        while current_date <= until_date:
            dataframes_needed = []
            stale_info = {}

            for dataframe_name in required_dataframes:
                key = (current_date, dataframe_name)
                status = status_map.get(key, ComputationStatus.MISSING)

                # Check if this rollup is stale (has newer source data)
                is_stale = key in stale_rollups

                # Add dataframe if:
                # 1. Force is True, OR
                # 2. Rollup is missing/incomplete, OR
                # 3. Rollup has newer source data (stale)
                if force or status in [ComputationStatus.MISSING, ComputationStatus.PARTIAL, ComputationStatus.ERROR] or is_stale:
                    dataframes_needed.append(dataframe_name)

                    if is_stale:
                        stale_info[dataframe_name] = {'reason': 'newer_source_data', 'new_tarballs': stale_rollups[key]}
                    elif status != ComputationStatus.MISSING:
                        stale_info[dataframe_name] = {'reason': status.value, 'new_tarballs': []}

            # Create date group if any dataframes need computation for this date
            if dataframes_needed:
                date_group = {'date': current_date, 'dataframes': dataframes_needed}

                # Add stale info if we have it
                if stale_info:
                    date_group['stale_info'] = stale_info

                # Add source data info for this date
                source_tarballs = source_data_map.get(current_date, [])
                date_group['source_tarballs'] = len(source_tarballs)

                smart_plan.append(date_group)

            current_date += timedelta(days=1)

        # Add smart plan metrics to span
        total_dataframes = sum(len(date_group['dataframes']) for date_group in smart_plan)
        stale_dataframes = sum(len(date_group.get('stale_info', {})) for date_group in smart_plan)

        add_span_attributes(
            current_span,
            **{
                'rollup.smart_plan.date_groups': len(smart_plan),
                'rollup.smart_plan.total_dataframes': total_dataframes,
                'rollup.smart_plan.stale_dataframes': stale_dataframes,
                'rollup.smart_plan.parallelization_potential': len(smart_plan),  # Each date can run in parallel
                'rollup.optimization.dependency_elimination': True,
                'rollup.optimization.incremental_updates': len(stale_rollups) > 0,
                'rollup.optimization.source_data_integration': extractor is not None,
            },
        )

        return smart_plan

    def get_tasks_to_compute(self, since_date: date, until_date: date, required_dataframes: List[str], force: bool = False) -> List[RollupTask]:
        """
        Get list of rollup tasks that need to be computed in dependency order

        Args:
            since_date: Start date
            until_date: End date
            required_dataframes: List of dataframe names to process
            force: If True, recompute all tasks regardless of status

        Returns:
            List of RollupTask objects in computation order
        """
        return self.create_rollup_dag(since_date, until_date, required_dataframes, force)

    def list_available_rollups(self) -> List[Tuple[date, str, ComputationStatus, str]]:
        """
        List all available rollups with their status

        Returns:
            List of tuples (date, dataframe_name, status, latest_version) for all found rollups
        """
        rollups = []

        if not os.path.exists(self.rollups_path):
            return rollups

        # Walk through the directory structure
        for year_dir in os.listdir(self.rollups_path):
            year_path = os.path.join(self.rollups_path, year_dir)
            if not os.path.isdir(year_path) or not year_dir.isdigit():
                continue

            for month_dir in os.listdir(year_path):
                month_path = os.path.join(year_path, month_dir)
                if not os.path.isdir(month_path) or not month_dir.isdigit():
                    continue

                for day_dir in os.listdir(month_path):
                    day_path = os.path.join(month_path, day_dir)
                    if not os.path.isdir(day_path) or not day_dir.isdigit():
                        continue

                    try:
                        target_date = date(int(year_dir), int(month_dir), int(day_dir))

                        # Check each dataframe directory
                        for dataframe_dir in os.listdir(day_path):
                            dataframe_path = os.path.join(day_path, dataframe_dir)
                            if not os.path.isdir(dataframe_path):
                                continue

                            latest_version = self.get_latest_version(target_date, dataframe_dir)
                            if latest_version:
                                # Check status from metadata if available
                                metadata = self.load_metadata(target_date, dataframe_dir, latest_version)
                                if metadata:
                                    processing_status = metadata.get('processing_status', 'complete')
                                    if processing_status == 'error':
                                        status = ComputationStatus.ERROR
                                    elif processing_status == 'partial':
                                        status = ComputationStatus.PARTIAL
                                    else:
                                        status = ComputationStatus.COMPLETE
                                else:
                                    # No metadata but version exists - assume complete
                                    status = ComputationStatus.COMPLETE

                                rollups.append((target_date, dataframe_dir, status, latest_version))
                            else:
                                # No valid versions found
                                rollups.append((target_date, dataframe_dir, ComputationStatus.MISSING, 'none'))

                    except ValueError:
                        # Invalid date format
                        continue

        return sorted(rollups)

    def clean_invalid_rollups(self, target_date: date, dataframe_name: Optional[str] = None) -> None:
        """Clean up invalid or partial rollups for a specific date and optionally specific dataframe"""
        if dataframe_name:
            # Clean specific dataframe
            dataframe_dir = self.get_dataframe_directory(target_date, dataframe_name)
            if os.path.exists(dataframe_dir):
                import shutil

                shutil.rmtree(dataframe_dir)
        else:
            # Clean all dataframes for the date
            date_dir = self.get_rollup_directory(target_date)
            if os.path.exists(date_dir):
                import shutil

                shutil.rmtree(date_dir)

    @traced_method(SpanNames.ROLLUP_PARQUET_SAVE)
    def save_rollup_data(self, target_date: date, dataframe_name: str, dataframe_obj, records_processed: int, processing_time: float) -> str:
        """Save rollup data and create manifest for a specific dataframe"""
        current_span = trace.get_current_span()

        add_span_attributes(
            current_span,
            **{
                SpanAttributes.ROLLUP_DATE: target_date.isoformat(),
                SpanAttributes.ROLLUP_DATAFRAME_NAME: dataframe_name,
                SpanAttributes.ROLLUP_RECORDS_PROCESSED: records_processed,
                SpanAttributes.ROLLUP_PROCESSING_TIME: processing_time,
            },
        )
        # Generate new version
        version = self.generate_version_string()

        # Ensure version directory exists
        version_dir = self.ensure_version_directory(target_date, dataframe_name, version)

        # Save the parquet data
        parquet_path = os.path.join(version_dir, 'data.parquet')

        # Convert dataframe to pandas DataFrame for parquet storage
        if hasattr(dataframe_obj, 'build_dataframe'):
            df = dataframe_obj.build_dataframe()
        else:
            # Assume it's already a DataFrame
            df = dataframe_obj

        # Check if dataframe is None or empty - treat as no data available
        if df is None:
            self.logger.warning(f'Dataframe for {dataframe_name} is None - no data available')
            return self.save_no_data_metadata(target_date, dataframe_name)

        if len(df) == 0:
            self.logger.warning(f'Dataframe for {dataframe_name} is empty - no data available')
            return self.save_no_data_metadata(target_date, dataframe_name)

        # Polars-compatible parquet storage
        import json
        df_for_parquet = df.clone()
        
        # NOTE: Polars DataFrames don't have pandas-style indexes, so no need to reset_index
        # This simplifies the code significantly compared to pandas
        
        # For now, save the Polars DataFrame directly to parquet
        # Polars handles complex data types better than pandas for parquet storage
        try:
            df_for_parquet.write_parquet(parquet_path)
        except Exception as e:
            # If direct parquet write fails, try to convert problematic types to strings
            self.logger.warning(f'Direct parquet write failed for {dataframe_name}, attempting type conversion: {e}')
            
            # Convert any remaining complex types to JSON strings for parquet compatibility
            import polars as pl
            
            for col in df_for_parquet.columns:
                # Check if column contains complex types that parquet can't handle
                dtype_str = str(df_for_parquet[col].dtype)
                if 'List' in dtype_str or 'Struct' in dtype_str or 'Object' in dtype_str:
                    # Convert complex types to JSON strings with proper return type
                    df_for_parquet = df_for_parquet.with_columns(
                        df_for_parquet[col].map_elements(
                            lambda x: json.dumps(x, default=str) if x is not None else None,
                            return_dtype=pl.Utf8
                        ).alias(col)
                    )
            
            # Try parquet write again with converted types
            df_for_parquet.write_parquet(parquet_path)

        # Save a simple metadata file in the version directory for debugging/info
        metadata = {
            'computation_timestamp': datetime.now().isoformat(),
            'processing_status': 'complete',
            'dataframe_name': dataframe_name,
            'since_date': target_date.isoformat(),
            'until_date': target_date.isoformat(),
            'records_processed': records_processed,
            'processing_time_seconds': processing_time,
            'version': version,
        }

        metadata_path = os.path.join(version_dir, 'metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        # Add final span attributes
        add_span_attributes(
            current_span,
            **{
                SpanAttributes.ROLLUP_VERSION: version,
                'rollup.parquet.size_bytes': os.path.getsize(parquet_path) if os.path.exists(parquet_path) else 0,
            },
        )

        self.logger.info(f'Saved rollup {dataframe_name} for {target_date} as version {version}')
        return version

    def save_error_metadata(self, target_date: date, dataframe_name: str, error_message: str) -> str:
        """Save error metadata for a failed rollup computation"""
        # Generate error version with status suffix instead of prefix
        base_version = self.generate_version_string()
        version = f'{base_version}__status__error'

        # Ensure version directory exists
        version_dir = self.ensure_version_directory(target_date, dataframe_name, version)

        # Save error metadata
        metadata = {
            'computation_timestamp': datetime.now().isoformat(),
            'processing_status': 'error',
            'dataframe_name': dataframe_name,
            'since_date': target_date.isoformat(),
            'until_date': target_date.isoformat(),
            'records_processed': 0,
            'processing_time_seconds': 0.0,
            'version': version,
            'error_message': error_message,
        }

        metadata_path = os.path.join(version_dir, 'metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        self.logger.warning(f'Saved error metadata for {dataframe_name} on {target_date} as version {version}')
        return version

    def save_no_data_metadata(self, target_date: date, dataframe_name: str) -> str:
        """Save no_data metadata for a rollup with no data"""
        # Generate no_data version with status suffix
        base_version = self.generate_version_string()
        version = f'{base_version}__status__no_data'

        # Ensure version directory exists
        version_dir = self.ensure_version_directory(target_date, dataframe_name, version)

        # Save no_data metadata
        metadata = {
            'computation_timestamp': datetime.now().isoformat(),
            'processing_status': 'no_data',
            'dataframe_name': dataframe_name,
            'since_date': target_date.isoformat(),
            'until_date': target_date.isoformat(),
            'records_processed': 0,
            'processing_time_seconds': 0.0,
            'version': version,
            'error_message': None,
        }

        metadata_path = os.path.join(version_dir, 'metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        self.logger.info(f'Saved no_data metadata for {dataframe_name} on {target_date} as version {version}')
        return version

    def save_no_source_data_metadata(self, target_date: date, dataframe_name: str) -> str:
        """Save no_source_data metadata when no tarballs found for this date"""
        # Generate no_source_data version with status suffix
        base_version = self.generate_version_string()
        version = f'{base_version}__status__no_source_data'

        # Ensure version directory exists
        version_dir = self.ensure_version_directory(target_date, dataframe_name, version)

        # Save no_source_data metadata
        metadata = {
            'computation_timestamp': datetime.now().isoformat(),
            'processing_status': 'no_source_data',
            'dataframe_name': dataframe_name,
            'since_date': target_date.isoformat(),
            'until_date': target_date.isoformat(),
            'records_processed': 0,
            'processing_time_seconds': 0.0,
            'version': version,
            'error_message': None,
        }

        metadata_path = os.path.join(version_dir, 'metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        self.logger.info(f'Saved no_source_data metadata for {dataframe_name} on {target_date} as version {version}')
        return version

    @traced_method(SpanNames.ROLLUP_DAG_CREATION)
    def scan_source_data_timestamps(self, since_date: date, until_date: date, extractor) -> Dict[date, List[Tuple[str, datetime]]]:
        """
        Scan raw data directory for tarballs and return their modification timestamps

        Args:
            since_date: Start date for scanning
            until_date: End date for scanning
            extractor: Extractor instance to use for scanning

        Returns:
            Dictionary mapping date to list of (tarball_path, modification_time) tuples
        """
        current_span = trace.get_current_span()
        add_span_attributes(
            current_span,
            **{
                'source_scan.since_date': since_date.isoformat(),
                'source_scan.until_date': until_date.isoformat(),
                'source_scan.extractor_type': type(extractor).__name__,
            },
        )

        source_data_map = {}
        current_date = since_date

        while current_date <= until_date:
            # Use extractor to scan for tarballs on this date
            if hasattr(extractor, 'scan_tarballs_for_date'):
                tarballs = extractor.scan_tarballs_for_date(current_date)
                source_data_map[current_date] = tarballs
            else:
                # Fallback for extractors that don't support scanning
                source_data_map[current_date] = []

            current_date += timedelta(days=1)

        total_tarballs = sum(len(tarballs) for tarballs in source_data_map.values())
        add_span_attributes(
            current_span,
            **{
                'source_scan.total_dates': len(source_data_map),
                'source_scan.total_tarballs': total_tarballs,
            },
        )

        return source_data_map

    @traced_method(SpanNames.ROLLUP_DAG_CREATION)
    def identify_stale_rollups(
        self, since_date: date, until_date: date, dataframe_names: List[str], source_data_map: Dict[date, List[Tuple[str, datetime]]]
    ) -> Dict[Tuple[date, str], List[str]]:
        """
        Compare rollup timestamps with source data timestamps to identify stale rollups

        Args:
            since_date: Start date for comparison
            until_date: End date for comparison
            dataframe_names: List of dataframe names to check
            source_data_map: Dictionary mapping date to list of (tarball_path, modification_time) tuples

        Returns:
            Dictionary mapping (date, dataframe_name) tuples to list of new tarball paths
        """
        current_span = trace.get_current_span()
        add_span_attributes(
            current_span,
            **{
                'stale_scan.since_date': since_date.isoformat(),
                'stale_scan.until_date': until_date.isoformat(),
                'stale_scan.dataframes': ','.join(dataframe_names),
            },
        )

        stale_rollups = {}
        current_date = since_date

        while current_date <= until_date:
            # Get source data for this date
            source_tarballs = source_data_map.get(current_date, [])

            for dataframe_name in dataframe_names:
                key = (current_date, dataframe_name)

                # Get the latest rollup version for this dataframe/date
                latest_version = self.get_latest_version(current_date, dataframe_name)

                if latest_version is None:
                    # No rollup exists - will be handled by missing rollup logic
                    continue

                # Load rollup metadata to get computation timestamp
                metadata = self.load_metadata(current_date, dataframe_name, latest_version)
                if not metadata:
                    # Can't load metadata - treat as stale to be safe
                    if source_tarballs:
                        stale_rollups[key] = [tarball_path for tarball_path, _ in source_tarballs]
                    continue

                rollup_timestamp_str = metadata.get('computation_timestamp')
                if not rollup_timestamp_str:
                    # No timestamp in metadata - treat as stale
                    if source_tarballs:
                        stale_rollups[key] = [tarball_path for tarball_path, _ in source_tarballs]
                    continue

                try:
                    rollup_timestamp = datetime.fromisoformat(rollup_timestamp_str.replace('Z', '+00:00'))
                    # Convert to naive datetime for comparison
                    if rollup_timestamp.tzinfo:
                        rollup_timestamp = rollup_timestamp.replace(tzinfo=None)
                except (ValueError, AttributeError):
                    # Invalid timestamp format - treat as stale
                    if source_tarballs:
                        stale_rollups[key] = [tarball_path for tarball_path, _ in source_tarballs]
                    continue

                # Check if any source tarballs are newer than the rollup
                new_tarballs = []
                for tarball_path, tarball_timestamp in source_tarballs:
                    if tarball_timestamp > rollup_timestamp:
                        new_tarballs.append(tarball_path)

                if new_tarballs:
                    stale_rollups[key] = new_tarballs

            current_date += timedelta(days=1)

        add_span_attributes(
            current_span,
            **{
                'stale_scan.stale_rollups_count': len(stale_rollups),
                'stale_scan.total_new_tarballs': sum(len(tarballs) for tarballs in stale_rollups.values()),
            },
        )

        return stale_rollups
