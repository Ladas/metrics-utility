import json
import logging
import os

from datetime import date, timedelta
from typing import Dict, List

import pandas as pd

from opentelemetry import trace

from metrics_utility.tracing import add_span_attributes, traced_method

from .manager import ComputationStatus, RollupManager


class RollupReader:
    """Reader class for loading and merging rollup data from parquet files"""

    def __init__(self, ship_path: str):
        self.rollup_manager = RollupManager(ship_path)
        self.logger = logging.getLogger(__name__)

    def _normalize_dataframe_names(self, dataframe_names: List[str]) -> List[str]:
        """
        Normalize dataframe names to class names used in rollup storage
        Handles both factory names (job_host_summary) and class names (DataframeJobhostSummaryUsage)
        """
        factory_to_class_mapping = {
            'job_host_summary': 'DataframeJobhostSummaryUsage',
            'main_jobevent': 'DataframeContentUsage',
            'main_host': 'DataframeInventoryScope',
            'data_collection_status': 'DataframeCollectionStatus',
            'host_metric': 'DataframeHostMetric',
        }

        normalized_names = []
        for name in dataframe_names:
            # If it's a factory name, convert to class name
            if name in factory_to_class_mapping:
                normalized_names.append(factory_to_class_mapping[name])
            else:
                # Assume it's already a class name
                normalized_names.append(name)

        return normalized_names

    def can_use_rollups(self, since_date: date, until_date: date, required_dataframes: List[str]) -> bool:
        """
        Check if rollups can be used for the given date range and required dataframes

        Args:
            since_date: Start date for the report
            until_date: End date for the report
            required_dataframes: List of dataframe names required for the report (supports both factory and class names)

        Returns:
            True if we can process rollups (has some valid data or all processed as no_data)
        """
        # Normalize dataframe names to class names for rollup lookup
        normalized_dataframes = self._normalize_dataframe_names(required_dataframes)
        status_map = self.rollup_manager.scan_rollups_directory(since_date, until_date, normalized_dataframes)

        # Count valid vs problematic rollups across all date/dataframe combinations
        total_combinations = 0
        missing_combinations = 0
        error_combinations = 0

        current_date = since_date
        while current_date <= until_date:
            for dataframe_name in normalized_dataframes:
                total_combinations += 1
                key = (current_date, dataframe_name)
                status = status_map.get(key, ComputationStatus.MISSING)

                if status == ComputationStatus.MISSING:
                    missing_combinations += 1
                elif status == ComputationStatus.ERROR:
                    error_combinations += 1
                # COMPLETE status (includes __status__no_data) is fine
                # PARTIAL status is also acceptable - we'll load what we can

            current_date += timedelta(days=1)

        # Always try to build reports with available data
        # Only refuse if ALL combinations are missing (no rollups computed at all)
        if missing_combinations == total_combinations:
            self.logger.debug(f'Cannot use rollups: All {total_combinations} combinations are missing')
            return False

        # Warn about data quality issues but still proceed
        if missing_combinations > 0:
            self.logger.warning(f'Some rollups missing: {missing_combinations}/{total_combinations} combinations')

        if error_combinations > 0:
            self.logger.warning(f'Some rollups have errors: {error_combinations}/{total_combinations} combinations')

        available_combinations = total_combinations - missing_combinations - error_combinations
        self.logger.info(f'Using rollups: {available_combinations}/{total_combinations} combinations available')

        # Always return True unless everything is missing
        return True

    @traced_method('rollup.reader.load_dataframes')
    def load_rollup_dataframes(self, since_date: date, until_date: date, required_dataframes: List[str]) -> Dict[str, pd.DataFrame]:
        """
        Load and merge rollup dataframes for the specified date range using latest versions

        Args:
            since_date: Start date for the report
            until_date: End date for the report
            required_dataframes: List of dataframe names to load (supports both factory and class names)

        Returns:
            Dictionary mapping original dataframe names to merged pandas DataFrames (None if no data available)
        """
        if not self.can_use_rollups(since_date, until_date, required_dataframes):
            raise ValueError('Cannot use rollups for the specified date range and dataframes')

        # Normalize dataframe names to class names for rollup loading
        normalized_dataframes = self._normalize_dataframe_names(required_dataframes)

        # Create mapping from original names to normalized names for result mapping
        original_to_normalized = {}
        for i, original_name in enumerate(required_dataframes):
            original_to_normalized[original_name] = normalized_dataframes[i]

        self.logger.info(f'Loading versioned rollups for date range {since_date} to {until_date}')

        # Add span attributes
        current_span = trace.get_current_span()
        add_span_attributes(
            current_span,
            **{
                'rollup.since_date': since_date.isoformat(),
                'rollup.until_date': until_date.isoformat(),
                'rollup.dataframes.required': ','.join(required_dataframes),
                'rollup.dataframes.normalized': ','.join(normalized_dataframes),
            },
        )

        # Initialize result dictionary and tracking for data collection status
        merged_dataframes = {}
        no_source_data_dates = {}  # Track dates with no source data for each dataframe
        no_data_dates = {}  # Track dates with no data for each dataframe
        error_dates = {}  # Track dates with errors for each dataframe
        for df_name in normalized_dataframes:
            merged_dataframes[df_name] = None
            no_source_data_dates[df_name] = []
            no_data_dates[df_name] = []
            error_dates[df_name] = []

        # Iterate through each date and load/merge dataframes
        current_date = since_date
        while current_date <= until_date:
            for df_name in normalized_dataframes:
                # Check for status versions first (no_source_data, no_data, error)
                all_versions = self.rollup_manager.get_available_versions(current_date, df_name)

                # Check for no_source_data status versions
                no_source_data_versions = [v for v in all_versions if '__status__no_source_data' in v]
                if no_source_data_versions:
                    # This date was processed but had no source data (no tarballs) - track for data collection status
                    no_source_data_dates[df_name].append(current_date)
                    self.logger.debug(f'Date {current_date} has no source data for {df_name} (status version: {no_source_data_versions[0]})')
                    continue

                # Check for no_data status versions
                no_data_versions = [v for v in all_versions if '__status__no_data' in v]
                if no_data_versions:
                    # This date was processed but had no data (tarballs existed but empty) - track for data collection status
                    no_data_dates[df_name].append(current_date)
                    self.logger.debug(f'Date {current_date} has no data for {df_name} (status version: {no_data_versions[0]})')
                    continue

                # Check for error status versions
                error_versions = [v for v in all_versions if '__status__error' in v]
                if error_versions:
                    # This date had processing errors - track and skip
                    error_dates[df_name].append(current_date)
                    self.logger.warning(f'Date {current_date} has error for {df_name} (status version: {error_versions[0]})')
                    continue

                # Get the latest data version for this dataframe
                latest_version = self.rollup_manager.get_latest_version(current_date, df_name)

                if latest_version:
                    version_dir = self.rollup_manager.get_version_directory(current_date, df_name, latest_version)
                    parquet_path = os.path.join(version_dir, 'data.parquet')

                    if os.path.exists(parquet_path):
                        try:
                            # Load parquet data directly - minimal processing to avoid corruption
                            df = pd.read_parquet(parquet_path)

                            # Only do minimal transformations for specific dataframes
                            if df_name == 'DataframeJobhostSummaryUsage':
                                # Convert lists back to sets for known set columns (critical for proper aggregation)
                                df = self._convert_lists_to_sets(df, df_name)
                                # Apply JSON normalization for canonical_facts and facts
                                df = self._normalize_dataframe_types(df, df_name)
                            elif df_name == 'DataframeInventoryScope':
                                # Convert lists back to sets for known set columns (critical for proper aggregation)
                                df = self._convert_lists_to_sets(df, df_name)
                                # Apply JSON normalization for canonical_facts and facts
                                df = self._normalize_dataframe_types(df, df_name)
                            # For DataframeContentUsage and DataframeCollectionStatus, use raw parquet data

                            if merged_dataframes[df_name] is None:
                                merged_dataframes[df_name] = df
                            else:
                                # Use dataframe class merge operations for proper rollup aggregation
                                merged_dataframes[df_name] = self._merge_using_dataframe_class(merged_dataframes[df_name], df, df_name)

                            self.logger.debug(f'Loaded {df_name} v{latest_version} for {current_date}: {len(df)} records')

                        except Exception as e:
                            self.logger.error(f'Failed to load {parquet_path}: {e}')
                            error_dates[df_name].append(current_date)
                            # Continue processing other dates even if this one fails
                    else:
                        self.logger.debug(f'No data file found for {df_name} v{latest_version} on {current_date}')
                else:
                    self.logger.debug(f'No versions found for {df_name} on {current_date}')

            current_date += timedelta(days=1)

        # Log summary of loaded data and add metrics to span
        total_records = 0
        loaded_dataframes = 0
        total_no_source_data_dates = 0
        total_no_data_dates = 0
        total_error_dates = 0

        for df_name, df in merged_dataframes.items():
            if df is not None and not df.empty:
                record_count = len(df)
                total_records += record_count
                loaded_dataframes += 1
                self.logger.info(f'Merged {df_name}: {record_count} total records')
            else:
                # Leave as None - don't create empty dataframes
                self.logger.info(f'No data found for {df_name}, dataframe will be None')

            # Log no-source-data dates
            if no_source_data_dates[df_name]:
                total_no_source_data_dates += len(no_source_data_dates[df_name])
                self.logger.info(
                    f'Dataframe {df_name} had no source data for {len(no_source_data_dates[df_name])} dates: {no_source_data_dates[df_name]}'
                )

            # Log no-data dates
            if no_data_dates[df_name]:
                total_no_data_dates += len(no_data_dates[df_name])
                self.logger.info(f'Dataframe {df_name} had no data for {len(no_data_dates[df_name])} dates: {no_data_dates[df_name]}')

            # Log error dates
            if error_dates[df_name]:
                total_error_dates += len(error_dates[df_name])
                self.logger.warning(f'Dataframe {df_name} had errors for {len(error_dates[df_name])} dates: {error_dates[df_name]}')

        # Add final metrics to span
        add_span_attributes(
            current_span,
            **{
                'rollup.total_records_loaded': total_records,
                'rollup.dataframes_loaded': loaded_dataframes,
                'rollup.no_source_data_dates_total': total_no_source_data_dates,
                'rollup.no_data_dates_total': total_no_data_dates,
                'rollup.error_dates_total': total_error_dates,
                'rollup.date_range_days': (until_date - since_date).days + 1,
            },
        )

        # Check if dataframes need index reset for dedup/report compatibility
        # Only reset index if there's actually a MultiIndex - avoid spurious 'index' column
        # NO regrouping here - it over-aggregates and loses unique records
        result_dataframes_normalized = {}
        for df_name, dataframe in merged_dataframes.items():
            if dataframe is not None and not dataframe.empty:
                # Only reset index if we have a MultiIndex (named levels)
                if dataframe.index.names != [None]:  # Has named index levels (MultiIndex)
                    dataframe = dataframe.reset_index()
                    self.logger.debug(f'Loaded {df_name}: {len(dataframe)} records (index reset for MultiIndex compatibility)')
                else:
                    self.logger.debug(f'Loaded {df_name}: {len(dataframe)} records (ready for dedup/report processing)')
                result_dataframes_normalized[df_name] = dataframe
            else:
                result_dataframes_normalized[df_name] = dataframe

        # Map back to original dataframe names for backward compatibility
        result_dataframes = {}
        normalized_to_original = {v: k for k, v in original_to_normalized.items()}

        for normalized_name, dataframe in result_dataframes_normalized.items():
            original_name = normalized_to_original.get(normalized_name, normalized_name)
            result_dataframes[original_name] = dataframe

        return result_dataframes

    def _convert_lists_to_sets(self, df: pd.DataFrame, dataframe_name: str) -> pd.DataFrame:
        """Convert lists back to sets for known set columns after loading from parquet.

        When storing to parquet, sets are converted to sorted lists. This method converts them back.
        """
        # Define which columns should be sets for each dataframe type
        set_columns_map = {
            'DataframeJobhostSummaryUsage': ['managed_node_types_set', 'events', 'host_names_before_dedup'],
            'DataframeInventoryScope': ['organizations', 'inventories', 'serials', 'host_names_before_dedup'],
            'DataframeContentUsage': ['playbooks', 'organizations'],
            'DataframeCollectionStatus': [],  # No set columns in collection status
        }

        set_columns = set_columns_map.get(dataframe_name, [])

        for col in set_columns:
            if col in df.columns:
                # Convert lists/arrays to sets, handling None values and numpy arrays properly
                def convert_to_set(x):
                    if x is None:
                        return set()
                    elif isinstance(x, (list, tuple)):
                        return set(x)
                    elif hasattr(x, 'tolist'):  # numpy array
                        return set(x.tolist())
                    elif hasattr(x, '__iter__') and not isinstance(x, (str, bytes)):
                        return set(x)
                    else:
                        return x

                df[col] = df[col].apply(convert_to_set)

        return df

    def _normalize_dataframe_types(self, df: pd.DataFrame, dataframe_name: str) -> pd.DataFrame:
        """Normalize dataframe types after loading from parquet (convert JSON back to dicts/sets and handle complex types)"""
        df_normalized = df.copy()

        import numpy as np

        # Define which columns should have JSON parsing for each dataframe type
        json_columns_map = {
            'DataframeJobhostSummaryUsage': ['canonical_facts', 'facts'],
            'DataframeInventoryScope': ['canonical_facts', 'facts'],
            'DataframeContentUsage': [],
            'DataframeCollectionStatus': [],  # No JSON columns in collection status
        }

        json_columns = json_columns_map.get(dataframe_name, [])

        # Handle JSON fields that were serialized for parquet storage - only for specified columns
        for col in json_columns:
            if col in df_normalized.columns and df_normalized[col].dtype == 'object':
                # Check if this might be a JSON column
                sample_val = df_normalized[col].dropna().iloc[0] if not df_normalized[col].dropna().empty else None

                if isinstance(sample_val, str) and sample_val.strip().startswith(('{', '[')):
                    try:
                        # Try to parse as JSON
                        df_normalized[col] = df_normalized[col].apply(lambda x: json.loads(x) if x is not None and x != '' else None)

                        # Special handling for canonical_facts and facts columns
                        if col in ['canonical_facts', 'facts']:
                            df_normalized[col] = df_normalized[col].apply(
                                lambda x: self._normalize_canonical_facts_dict(x) if x is not None else None
                            )

                    except (json.JSONDecodeError, TypeError):
                        # If JSON parsing fails, keep original values
                        pass

        # Handle numpy arrays and complex iterables for ALL columns (but safely)
        for col in df_normalized.columns:
            if df_normalized[col].dtype == 'object':
                sample_val = df_normalized[col].dropna().iloc[0] if not df_normalized[col].dropna().empty else None

                if isinstance(sample_val, np.ndarray):
                    # Convert numpy arrays to lists for Excel compatibility
                    df_normalized[col] = df_normalized[col].apply(lambda x: x.tolist() if x is not None and hasattr(x, 'tolist') else x)
                elif hasattr(sample_val, '__iter__') and not isinstance(sample_val, (str, bytes, dict)):
                    # Convert other complex iterables to simple types (but not strings or dicts)
                    df_normalized[col] = df_normalized[col].apply(
                        lambda x: list(x) if x is not None and hasattr(x, '__iter__') and not isinstance(x, (str, bytes, dict)) else x
                    )

        return df_normalized

    def _normalize_canonical_facts_dict(self, facts_dict):
        """
        Normalize canonical_facts or facts dictionary to convert string set representations back to sets

        Example:
        Input:  {'ansible_machine_id': "{'e56eb592febecd4e03860514ce5a9f55'}"}
        Output: {'ansible_machine_id': {'e56eb592febecd4e03860514ce5a9f55'}}
        """
        if not isinstance(facts_dict, dict):
            return facts_dict

        normalized = {}
        for key, value in facts_dict.items():
            if isinstance(value, str) and value.startswith('{') and value.endswith('}'):
                try:
                    # Try to parse as a set representation and keep as set
                    import ast

                    parsed_set = ast.literal_eval(value)
                    if isinstance(parsed_set, set):
                        normalized[key] = parsed_set
                    else:
                        normalized[key] = value
                except (ValueError, SyntaxError):
                    # If parsing fails, keep original value
                    normalized[key] = value
            else:
                normalized[key] = value

        return normalized

    def _get_dataframe_class(self, dataframe_name: str):
        """Get the dataframe class for a given dataframe name"""
        try:
            if dataframe_name == 'DataframeJobhostSummaryUsage':
                from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_jobhost_summary_usage import (
                    DataframeJobhostSummaryUsage,
                )

                return DataframeJobhostSummaryUsage
            elif dataframe_name == 'DataframeContentUsage':
                from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_content_usage import DataframeContentUsage

                return DataframeContentUsage
            elif dataframe_name == 'DataframeInventoryScope':
                from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_inventory_scope import DataframeInventoryScope

                return DataframeInventoryScope
            elif dataframe_name == 'DataframeCollectionStatus':
                from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_collection_status import DataframeCollectionStatus

                return DataframeCollectionStatus
            elif dataframe_name == 'DataframeHostMetric':
                from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_host_metric import DataframeHostMetric

                return DataframeHostMetric
            else:
                return None
        except ImportError:
            return None

    def get_rollup_date_range(self, since_date: date, until_date: date, required_dataframes: List[str]) -> tuple:
        """
        Get the actual date range that has rollups available for all required dataframes

        Args:
            since_date: Requested start date
            until_date: Requested end date
            required_dataframes: List of dataframe names required

        Returns:
            Tuple of (actual_since_date, actual_until_date) based on available rollups
        """
        status_map = self.rollup_manager.scan_rollups_directory(since_date, until_date, required_dataframes)

        # Find dates where all required dataframes are complete
        complete_dates = set()
        current_date = since_date

        while current_date <= until_date:
            all_complete = True
            for df_name in required_dataframes:
                key = (current_date, df_name)
                if status_map.get(key, ComputationStatus.MISSING) != ComputationStatus.COMPLETE:
                    all_complete = False
                    break

            if all_complete:
                complete_dates.add(current_date)

            current_date += timedelta(days=1)

        if not complete_dates:
            return None, None

        return min(complete_dates), max(complete_dates)

    def get_missing_rollups(self, since_date: date, until_date: date, required_dataframes: List[str] = None) -> List[date]:
        """
        Get list of dates that are missing rollups in the specified range

        Args:
            since_date: Start date
            until_date: End date
            required_dataframes: List of dataframe names to check (if None, uses default set)

        Returns:
            List of dates that need rollup computation
        """
        if required_dataframes is None:
            required_dataframes = ['DataframeJobhostSummaryUsage', 'DataframeContentUsage', 'DataframeInventoryScope']

        tasks = self.rollup_manager.get_tasks_to_compute(since_date, until_date, required_dataframes, force=False)

        # Extract unique dates from tasks
        missing_dates = sorted(set(task.target_date for task in tasks))
        return missing_dates

    def check_rollups_with_source_data(self, since_date: date, until_date: date, required_dataframes: List[str], extractor) -> tuple:
        """
        Check rollup availability considering source data timestamps for incremental updates

        Args:
            since_date: Start date to check
            until_date: End date to check
            required_dataframes: List of dataframe names to check
            extractor: Extractor instance for source data scanning

        Returns:
            Tuple of (rollups_available: bool, missing_dates: List[date], stale_dates: List[date])
        """
        # Normalize dataframe names to class names for rollup lookup
        normalized_dataframes = self._normalize_dataframe_names(required_dataframes)

        # Check basic rollup availability first
        basic_availability = self.can_use_rollups(since_date, until_date, required_dataframes)

        # Get missing rollups using existing logic
        missing_dates = self.get_missing_rollups(since_date, until_date, required_dataframes)

        # Check for stale rollups if extractor supports scanning
        stale_dates = []
        if extractor and hasattr(extractor, 'scan_tarballs_for_date'):
            # Use rollup manager's stale detection
            source_data_map = self.rollup_manager.scan_source_data_timestamps(since_date, until_date, extractor)
            stale_rollups = self.rollup_manager.identify_stale_rollups(since_date, until_date, normalized_dataframes, source_data_map)

            # Extract unique dates that have stale rollups
            stale_dates = sorted(set(date_key[0] for date_key in stale_rollups.keys()))

            self.logger.info(f'Found {len(stale_dates)} dates with stale rollups requiring recomputation')
            if stale_dates:
                self.logger.debug(f'Stale rollup dates: {stale_dates}')

        # Rollups are available if we have basic availability AND no stale rollups need recomputation
        # But we consider them "available for use" if we can load something, even if we need to recompute some
        rollups_available = basic_availability

        return rollups_available, missing_dates, stale_dates

    def find_data_gaps(self, since_date: date, until_date: date, dataframe_name: str) -> List[tuple]:
        """
        Find gaps in data availability for a specific dataframe in the given date range.

        Returns:
            List of tuples (gap_start_date, gap_end_date) representing continuous gaps
        """
        from datetime import timedelta

        # Normalize dataframe name
        normalized_names = self._normalize_dataframe_names([dataframe_name])
        if not normalized_names:
            return []

        dataframe_name = normalized_names[0]

        # Scan all dates in the range to find gaps
        current_date = since_date
        gaps = []
        gap_start = None

        while current_date <= until_date:
            # Check if we have data for this date
            versions = self.rollup_manager.get_available_versions(current_date, dataframe_name)

            # Filter out status versions to see if we have actual data
            data_versions = [
                v for v in versions if not any(status in v for status in ['__status__error', '__status__no_data', '__status__no_source_data'])
            ]

            has_data = len(data_versions) > 0

            if not has_data:
                # No data for this date
                if gap_start is None:
                    gap_start = current_date
            else:
                # We have data for this date
                if gap_start is not None:
                    # End of a gap
                    gap_end = current_date
                    gaps.append((gap_start, gap_end))
                    gap_start = None

            current_date += timedelta(days=1)

        # Handle gap that extends to the end of the range
        if gap_start is not None:
            gap_end = until_date + timedelta(days=1)  # Extend gap beyond the requested range
            gaps.append((gap_start, gap_end))

        return gaps

    def _merge_using_dataframe_class(self, existing_df, new_df, dataframe_name):
        """
        Merge two dataframes using dataframe class operations and load_from_parquet for schema consistency.

        Args:
            existing_df: Existing accumulated dataframe (can be None)
            new_df: New dataframe to merge
            dataframe_name: Name of the dataframe class to use for operations

        Returns:
            Merged dataframe using dataframe class merge method
        """
        # Get the dataframe class and create instance for operations
        dataframe_class = self._get_dataframe_class(dataframe_name)
        if not dataframe_class:
            raise ValueError(f'Unknown dataframe class for {dataframe_name} - cannot perform merge operation')

        # Create dataframe instance for operations with minimal required context
        # Note: extractor=None is acceptable for merge operations as they only need the class methods
        df_instance = dataframe_class(extractor=None, month=None, extra_params={})

        # If only one dataframe, return as-is (load_from_parquet already handled schema)
        if existing_df is None:
            return new_df

        # Use dataframe class merge method to properly combine rollups from different days
        # This preserves aggregated data structures and handles complex merging logic
        return df_instance.merge(existing_df, new_df)
