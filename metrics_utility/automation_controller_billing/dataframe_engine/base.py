import datetime
import time

from functools import reduce

import pandas as pd

from dateutil.relativedelta import relativedelta
from opentelemetry import trace

from metrics_utility.tracing import add_span_attributes, traced_method


def granularity_cast(date, granularity):
    if granularity == 'monthly':
        return date.replace(day=1)
    elif granularity == 'yearly':
        return date.replace(month=1, day=1)
    else:
        return date


def list_dates(start_date, end_date, granularity):
    # Given start date and end date, return list of dates in the given granularity
    # e.g. for daily it is a list of days withing the interval, for monthly it is a
    # list of months withing the interval, etc.
    start_date = granularity_cast(start_date, granularity)
    end_date = granularity_cast(end_date, granularity)

    dates_arr = []
    while start_date < end_date:
        dates_arr.append(start_date)

        if granularity == 'monthly':
            start_date += relativedelta(months=+1)
        elif granularity == 'yearly':
            start_date += relativedelta(years=+1)
        else:
            start_date += datetime.timedelta(days=1)

    dates_arr.append(end_date)

    return dates_arr


# For JSON/dict columns: update one dict with the other (later values overwrite earlier ones)
def combine_json(json1, json2):
    merged = {}
    if isinstance(json1, dict):
        merged.update(json1)
    if isinstance(json2, dict):
        merged.update(json2)
    return merged


# For set columns: take the union of the two sets
def combine_set(set1, set2):
    """
    Combine two collections (set or list) into a single set of unique items.
    If an input is a list, it is first converted to a set.
    If an input is not a list or a set, it is treated as empty.
    """
    # Convert to set if input is a list; otherwise, if not a set, default to an empty set.
    if isinstance(set1, list):
        set1 = set(set1)
    elif not isinstance(set1, set):
        set1 = set()

    if isinstance(set2, list):
        set2 = set(set2)
    elif not isinstance(set2, set):
        set2 = set()

    # Return the union of both sets.
    return set1.union(set2)


def merge_sets(x):
    return set().union(*x)


def merge_setdicts(x):
    return reduce(combine_json_values, x, {})


# Helper function to combine two JSON values.
# For each key, it builds a set of non-null, non-empty values from both inputs.
def combine_json_values(val1, val2):
    merged = {}
    for d in [val1, val2]:
        if isinstance(d, dict):
            for key, value in d.items():
                if value is not None and value != '':
                    if isinstance(value, set):
                        merged.setdefault(key, set()).update(value)
                    else:
                        merged.setdefault(key, set()).add(value)

    return merged


class Base:
    def __init__(self, extractor, month, extra_params):
        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params
        self._cached_dataframe = None

    def build_dataframe(self, batch_data_iterator):
        """Build dataframe from batch data iterator. Subclasses must override this method."""
        raise NotImplementedError('Subclasses must implement build_dataframe(batch_data_iterator)')

    def set_cached_dataframe(self, dataframe):
        """Set a pre-computed dataframe to use instead of building from scratch."""
        self._cached_dataframe = dataframe

    def get_cached_dataframe(self):
        """Get the cached dataframe, if any."""
        return self._cached_dataframe

    def has_cached_dataframe(self):
        """Check if a cached dataframe is available."""
        return self._cached_dataframe is not None

    def dates(self):
        if self.extra_params.get('since_date') is not None:
            beginning_of_the_month = self.extra_params.get('since_date')
            end_of_the_month = self.extra_params.get('until_date')
        else:
            beginning_of_the_month = self.month.replace(day=1)
            end_of_the_month = beginning_of_the_month + relativedelta(months=1) - relativedelta(days=1)

        dates_list = list_dates(start_date=beginning_of_the_month, end_date=end_of_the_month, granularity='daily')
        return dates_list

    @traced_method('dataframe.cast')
    def cast_dataframe(self, df, types):
        """Cast dataframe columns and index to specified types with tracing."""
        current_span = trace.get_current_span()

        start_time = time.time()
        record_count = len(df) if df is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.cast.input_record_count': record_count,
                'dataframe.cast.type_count': len(types),
                'dataframe.cast.has_composite_index': len(self.unique_index_columns()) > 1,
            },
        )

        levels = []
        if len(self.unique_index_columns()) == 1:
            # Special behavior if the index is not composite, but only 1 column
            # Casting index field to object
            df.index = df.index.astype(object)
        else:
            # Composite index branch
            # Check if we actually have a MultiIndex, if not, skip index casting
            if hasattr(df.index, 'levels'):
                # Casting index field to object
                for index, _level in enumerate(df.index.levels):
                    casted_level = df.index.levels[index].astype(object)
                    levels.append(casted_level)

                df.index = df.index.set_levels(levels)
            else:
                # DataFrame has RangeIndex instead of MultiIndex, skip index casting
                # This can happen when dataframe hasn't been properly grouped yet
                pass

        # Handle NA/NaN values before casting to avoid "Cannot convert non-finite values (NA or inf) to integer" error
        result = df.copy()
        for col, col_type in types.items():
            if col in result.columns:
                if col_type is int or col_type == 'int' or str(col_type).startswith('int'):
                    # For integer columns, fill NaN with 0 before casting
                    result[col] = result[col].fillna(0).astype(col_type)
                elif col_type is float or col_type == 'float' or str(col_type).startswith('float'):
                    # For float columns, NaN values are fine
                    result[col] = result[col].astype(col_type)
                elif str(col_type) == 'datetime64[ns]':
                    # For datetime columns, use pd.to_datetime which handles NaN properly
                    result[col] = pd.to_datetime(result[col])
                else:
                    # For other types (str, object, etc.), use standard astype
                    result[col] = result[col].astype(col_type)

        cast_duration = time.time() - start_time
        add_span_attributes(
            current_span,
            **{'dataframe.cast.duration_seconds': cast_duration, 'dataframe.cast.output_record_count': len(result) if result is not None else 0},
        )

        return result

    @traced_method('dataframe.summarize_merged')
    def summarize_merged_dataframes(self, df, columns, operations={}):
        """Summarize merged dataframes with tracing for performance monitoring."""
        current_span = trace.get_current_span()

        start_time = time.time()
        record_count = len(df) if df is not None else 0

        # Find all columns that have _x/_y suffixes from merge, not just data_columns
        all_suffix_columns = set()
        for col_name in df.columns:
            if col_name.endswith('_x'):
                base_col = col_name[:-2]
                if f'{base_col}_y' in df.columns:
                    all_suffix_columns.add(base_col)
            elif col_name.endswith('_y'):
                base_col = col_name[:-2]
                if f'{base_col}_x' in df.columns:
                    all_suffix_columns.add(base_col)

        # Process both data_columns and any other columns with suffixes
        columns_to_process = set(columns) | all_suffix_columns

        add_span_attributes(
            current_span,
            **{
                'dataframe.summarize.input_record_count': record_count,
                'dataframe.summarize.column_count': len(columns),
                'dataframe.summarize.suffix_columns_found': len(all_suffix_columns),
                'dataframe.summarize.total_columns_to_process': len(columns_to_process),
                'dataframe.summarize.operations': ','.join(f'{k}:{v}' for k, v in operations.items()),
            },
        )

        for col in columns_to_process:
            col_start_time = time.time()

            # Check if merge suffix columns exist (they're only created when there are actual conflicts)
            col_x = f'{col}_x'
            col_y = f'{col}_y'

            if col_x not in df.columns or col_y not in df.columns:
                # No merge conflicts for this column, skip summarization
                continue

            if operations.get(col) == 'min':
                # Handle NaN values properly for min operation, with special handling for mixed types
                try:
                    df[col] = df[[col_x, col_y]].min(axis=1, skipna=True)
                except TypeError as e:
                    if 'not supported between instances' in str(e):
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.warning(f'Mixed type comparison detected for column {col} during min operation: {e}. Using safe comparison fallback.')
                        
                        # Handle mixed type comparison by using apply with proper NaN handling
                        def safe_min(row):
                            val_x = row[col_x]
                            val_y = row[col_y]
                            
                            # If both are NaN, return NaN
                            if pd.isna(val_x) and pd.isna(val_y):
                                return pd.NaT if 'datetime' in str(type(val_x)) or 'datetime' in str(type(val_y)) else None
                            # If one is NaN, return the other
                            elif pd.isna(val_x):
                                return val_y
                            elif pd.isna(val_y):
                                return val_x
                            # Both are valid, compare them
                            else:
                                return min(val_x, val_y)
                        
                        df[col] = df.apply(safe_min, axis=1)
                    else:
                        raise  # Re-raise if it's a different TypeError
            elif operations.get(col) == 'max':
                # Handle NaN values properly for max operation, with special handling for mixed types
                col_x_data = df[col_x]
                col_y_data = df[col_y]

                # Check if we have mixed types (strings and NaN) that cause comparison issues
                try:
                    df[col] = df[[col_x, col_y]].max(axis=1, skipna=True)
                except TypeError as e:
                    if 'not supported between instances' in str(e):
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.warning(f'Mixed type comparison detected for column {col} during max operation: {e}. Using safe comparison fallback.')
                        
                        # Handle mixed type comparison by using apply with proper NaN handling
                        def safe_max(row):
                            val_x = row[col_x]
                            val_y = row[col_y]

                            # If both are NaN, return NaN
                            if pd.isna(val_x) and pd.isna(val_y):
                                return pd.NaT if 'datetime' in str(type(val_x)) or 'datetime' in str(type(val_y)) else None
                            # If one is NaN, return the other
                            elif pd.isna(val_x):
                                return val_y
                            elif pd.isna(val_y):
                                return val_x
                            # Both are valid, compare them
                            else:
                                return max(val_x, val_y)

                        df[col] = df.apply(safe_max, axis=1)
                    else:
                        raise  # Re-raise if it's a different TypeError
            elif operations.get(col) == 'combine_set':
                df[col] = df.apply(lambda row: combine_set(row.get(col_x), row.get(col_y)), axis=1)
            elif operations.get(col) == 'combine_json':
                df[col] = df.apply(lambda row: combine_json(row.get(col_x), row.get(col_y)), axis=1)
            elif operations.get(col) == 'combine_json_values':
                df[col] = df.apply(lambda row: combine_json_values(row.get(col_x), row.get(col_y)), axis=1)
            else:
                # For columns not in operations (usually raw data columns), choose appropriate default
                try:
                    # Try sum for numeric columns
                    df[col] = df[[col_x, col_y]].sum(axis=1, skipna=True)
                except (TypeError, ValueError):
                    # For non-numeric columns, take the first non-null value
                    df[col] = df[col_x].combine_first(df[col_y])

            del df[col_x]
            del df[col_y]

            col_duration = time.time() - col_start_time
            if col_duration > 0.1:  # Log slow column operations
                add_span_attributes(
                    current_span,
                    **{
                        f'dataframe.summarize.{col}.duration_seconds': col_duration,
                        f'dataframe.summarize.{col}.operation': operations.get(col, 'sum'),
                    },
                )

        total_duration = time.time() - start_time
        add_span_attributes(
            current_span,
            **{
                'dataframe.summarize.total_duration_seconds': total_duration,
                'dataframe.summarize.output_record_count': len(df) if df is not None else 0,
            },
        )

        return df

    def empty(self):
        return pd.DataFrame(columns=self.unique_index_columns() + self.data_columns())

    # Multipart collection, merge the dataframes and sum counts
    @traced_method('dataframe.merge')
    def merge(self, rollup, new_group):
        """Merge two dataframes with comprehensive performance tracking."""
        current_span = trace.get_current_span()

        start_time = time.time()
        rollup_count = len(rollup) if rollup is not None else 0
        new_group_count = len(new_group) if new_group is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.merge.rollup_record_count': rollup_count,
                'dataframe.merge.new_group_record_count': new_group_count,
                'dataframe.merge.index_columns': ','.join(self.unique_index_columns()),
                'dataframe.merge.data_columns': ','.join(self.data_columns()),
            },
        )

        if rollup is None:
            add_span_attributes(
                current_span, **{'dataframe.merge.operation': 'return_new_group', 'dataframe.merge.duration_seconds': time.time() - start_time}
            )
            return new_group

        # Pandas merge operation - often the slowest part
        merge_start = time.time()
        rollup = pd.merge(rollup.loc[:,], new_group.loc[:,], on=self.unique_index_columns(), how='outer')
        merge_duration = time.time() - merge_start

        add_span_attributes(
            current_span,
            **{
                'dataframe.merge.pandas_merge_duration_seconds': merge_duration,
                'dataframe.merge.after_merge_record_count': len(rollup) if rollup is not None else 0,
            },
        )

        # Summarize merged data
        rollup = self.summarize_merged_dataframes(rollup, self.data_columns(), operations=self.operations())

        # Cast types for grouped/aggregated data (rollups)
        rollup = self._apply_consistent_casting(rollup)

        total_duration = time.time() - start_time
        final_count = len(rollup) if rollup is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.merge.total_duration_seconds': total_duration,
                'dataframe.merge.final_record_count': final_count,
                'dataframe.merge.record_growth': final_count - rollup_count if rollup_count > 0 else final_count,
            },
        )

        # Log slow merges
        if total_duration > 1.0:
            add_span_attributes(
                current_span, **{'dataframe.merge.slow_operation': True, 'dataframe.merge.performance_warning': f'Merge took {total_duration:.2f}s'}
            )

        return rollup

    @traced_method('dataframe.dedup')
    def dedup(self, dataframe, hostname_mapping=None):
        """Deduplicate dataframe with hostname mapping and performance tracking."""
        current_span = trace.get_current_span()

        start_time = time.time()
        input_count = len(dataframe) if dataframe is not None else 0
        mapping_count = len(hostname_mapping) if hostname_mapping else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.dedup.input_record_count': input_count,
                'dataframe.dedup.hostname_mapping_count': mapping_count,
                'dataframe.dedup.has_mapping': bool(hostname_mapping),
            },
        )

        if dataframe is None or dataframe.empty:
            add_span_attributes(
                current_span, **{'dataframe.dedup.operation': 'return_empty', 'dataframe.dedup.duration_seconds': time.time() - start_time}
            )
            return self.empty()

        if not hostname_mapping:
            add_span_attributes(
                current_span, **{'dataframe.dedup.operation': 'no_mapping_passthrough', 'dataframe.dedup.duration_seconds': time.time() - start_time}
            )
            return dataframe

        # map hostnames to canonical value
        copy_start = time.time()
        df = dataframe.copy()
        copy_duration = time.time() - copy_start

        # Apply hostname mapping
        map_start = time.time()
        df['host_name'] = df['host_name'].map(hostname_mapping).fillna(df['host_name'])
        map_duration = time.time() - map_start

        # Only regroup if hostname mapping actually created duplicates
        regroup_start = time.time()
        unique_index_cols = self.unique_index_columns()
        if len(unique_index_cols) > 0:
            # Check for actual duplicates based on unique index columns
            has_duplicates = df.duplicated(subset=unique_index_cols, keep=False).any()

            if has_duplicates:
                df_grouped = self.regroup(df)
                regroup_duration = time.time() - regroup_start
                add_span_attributes(current_span, **{'dataframe.dedup.regrouping_applied': True, 'dataframe.dedup.found_duplicates': True})
            else:
                df_grouped = df
                regroup_duration = time.time() - regroup_start
                add_span_attributes(current_span, **{'dataframe.dedup.regrouping_applied': False, 'dataframe.dedup.found_duplicates': False})
        else:
            # No index columns to check, skip regrouping
            df_grouped = df
            regroup_duration = time.time() - regroup_start
            add_span_attributes(current_span, **{'dataframe.dedup.regrouping_applied': False, 'dataframe.dedup.no_index_columns': True})

        # cast types to match the table
        cast_start = time.time()
        df_grouped = self.cast_dataframe(df_grouped, self.cast_types())
        result = df_grouped.reset_index()
        cast_duration = time.time() - cast_start

        total_duration = time.time() - start_time
        output_count = len(result) if result is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.dedup.copy_duration_seconds': copy_duration,
                'dataframe.dedup.map_duration_seconds': map_duration,
                'dataframe.dedup.regroup_duration_seconds': regroup_duration,
                'dataframe.dedup.cast_duration_seconds': cast_duration,
                'dataframe.dedup.total_duration_seconds': total_duration,
                'dataframe.dedup.output_record_count': output_count,
                'dataframe.dedup.deduplication_ratio': (input_count - output_count) / input_count if input_count > 0 else 0,
                'dataframe.dedup.records_deduplicated': input_count - output_count,
            },
        )

        # Log slow or highly effective deduplication
        if total_duration > 0.5:
            add_span_attributes(current_span, **{'dataframe.dedup.slow_operation': True})

        dedup_ratio = (input_count - output_count) / input_count if input_count > 0 else 0
        if dedup_ratio > 0.1:  # More than 10% deduplication
            add_span_attributes(
                current_span, **{'dataframe.dedup.high_deduplication': True, 'dataframe.dedup.deduplication_percentage': f'{dedup_ratio:.1%}'}
            )

        return result


    @staticmethod
    def unique_index_columns():
        pass

    @staticmethod
    def data_columns():
        pass

    @staticmethod
    def cast_types():
        pass

    @staticmethod
    def operations():
        pass

    @staticmethod
    def index_cast_types():
        """Return casting types for index columns (unique_index_columns)."""
        # Default implementation - subclasses can override
        return {}

    def load_from_parquet(self, parquet_path):
        """Load dataframe from parquet file with consistent schema (for rollup reader).

        Args:
            parquet_path: Path to parquet file containing pre-computed day groups

        Returns:
            DataFrame loaded from parquet with consistent columns, types, and indexing
        """
        import pandas as pd

        try:
            df = pd.read_parquet(parquet_path)
            if df.empty:
                return self.empty()
        except FileNotFoundError:
            return self.empty()

        # Ensure all required columns exist with proper defaults
        df = self._ensure_complete_schema(df)

        # Apply casting for both index and data columns
        df = self._apply_consistent_casting(df)

        # Set proper index
        df = self._apply_consistent_indexing(df)

        return df

    def _ensure_complete_schema(self, df):
        """Ensure dataframe has all required columns with proper defaults."""
        # Get all required columns from class methods
        index_columns = self.__class__.unique_index_columns() if hasattr(self.__class__, 'unique_index_columns') else []
        data_columns = self.__class__.data_columns() if hasattr(self.__class__, 'data_columns') else []
        all_columns = index_columns + data_columns

        for col in all_columns:
            if col not in df.columns:
                # Add missing column with appropriate default value
                default_value = self._get_column_default_value(col)
                # Use pandas method to assign default value that handles length properly
                if len(df) > 0:
                    # For non-empty DataFrame, fill with the default value
                    if isinstance(default_value, (set, dict, list)):
                        # For collection types, assign the same collection to each row
                        df[col] = [default_value.copy() if hasattr(default_value, 'copy') else default_value for _ in range(len(df))]
                    else:
                        # For scalar types, assign the same value to each row
                        df[col] = default_value
                else:
                    # For empty DataFrame, just add the column
                    df[col] = default_value

        return df

    def _get_column_default_value(self, column_name):
        """Get appropriate default value for a missing column."""
        # Get type info from cast_types and index_cast_types
        cast_types = self.cast_types() or {}
        index_cast_types = self.index_cast_types() or {}

        column_type = cast_types.get(column_name) or index_cast_types.get(column_name)

        # Return appropriate default based on type
        if column_type == int or column_type == 'int64':
            return 0
        elif column_type == float or column_type == 'float64':
            return 0.0
        elif column_type == str:
            return ''
        elif column_type == 'datetime64[ns]':
            return pd.NaT
        elif column_name in [
            'managed_node_types_set',
            'events',
            'canonical_facts',
            'facts',
            'host_names_before_dedup',
            'organizations',
            'inventories',
            'serials',
        ]:
            # Set/dict columns get empty collections
            if column_name in ['canonical_facts', 'facts']:
                return {}
            else:
                return set()
        else:
            # Fall back to existing method for detailed type handling
            return self.get_default_value_for_column(column_name)

    def _apply_consistent_casting(self, df):
        """Apply consistent type casting for grouped/aggregated data (rollups)."""
        # Cast data columns using grouped data cast types
        if hasattr(self.__class__, 'cast_types'):
            data_cast_types = self.__class__.cast_types() or {}
            available_data_cast_types = {k: v for k, v in data_cast_types.items() if k in df.columns}
            if available_data_cast_types:
                df = self.cast_dataframe(df, available_data_cast_types)

        # Cast index columns if index_cast_types is available - but only columns that exist
        if hasattr(self.__class__, 'index_cast_types'):
            index_types = self.__class__.index_cast_types() or {}
            for col, col_type in index_types.items():
                if col in df.columns:
                    if col_type == 'datetime64[ns]':
                        df[col] = pd.to_datetime(df[col])
                    elif col_type == int or col_type == 'int' or str(col_type).startswith('int'):
                        # For integer columns, fill NaN with 0 before casting
                        df[col] = df[col].fillna(0).astype(col_type)
                    else:
                        df[col] = df[col].astype(col_type)

        return df

    def _apply_raw_data_casting(self, df, manually_converted_columns=None):
        """Apply type casting for raw CSV data, skipping manually converted columns."""
        if manually_converted_columns is None:
            manually_converted_columns = set()

        # Use raw data cast types if available, otherwise fall back to regular cast types
        if hasattr(self.__class__, 'raw_data_cast_types'):
            raw_cast_types = self.__class__.raw_data_cast_types() or {}
        else:
            raw_cast_types = self.__class__.cast_types() or {}

        # Filter out manually converted columns and columns that don't exist
        available_cast_types = {
            k: v for k, v in raw_cast_types.items()
            if k in df.columns and k not in manually_converted_columns
        }

        if available_cast_types:
            df = self.cast_dataframe(df, available_cast_types)

        # Cast index columns if index_cast_types is available - but only columns that exist
        if hasattr(self.__class__, 'index_cast_types'):
            index_types = self.__class__.index_cast_types() or {}
            for col, col_type in index_types.items():
                if col in df.columns and col not in manually_converted_columns:
                    if col_type == 'datetime64[ns]':
                        df[col] = pd.to_datetime(df[col])
                    elif col_type == int or col_type == 'int' or str(col_type).startswith('int'):
                        # For integer columns, fill NaN with 0 before casting
                        df[col] = df[col].fillna(0).astype(col_type)
                    else:
                        df[col] = df[col].astype(col_type)

        return df

    def _apply_consistent_indexing(self, df):
        """Apply consistent indexing using unique_index_columns."""
        # Reset index to ensure we have all index columns as regular columns
        if df.index.name is not None or len(df.index.names) > 1:
            df = df.reset_index()

        # Set index using unique_index_columns from class method
        if hasattr(self.__class__, 'unique_index_columns'):
            index_cols = self.__class__.unique_index_columns()
            available_index_cols = [col for col in index_cols if col in df.columns]

            if available_index_cols:
                df = df.set_index(available_index_cols)

        return df

    def get_default_value_for_column(self, column_name):
        """Get appropriate default value for a column based on casting type information."""
        # First check if it's defined in cast_types (data columns)
        cast_types = self.cast_types() or {}
        index_cast_types = self.index_cast_types() or {}

        # Combine both casting dictionaries
        all_cast_types = {**cast_types, **index_cast_types}

        if column_name in all_cast_types:
            expected_type = all_cast_types[column_name]

            # Handle different type specifications
            if expected_type == int or expected_type == 'int64':
                return 0
            elif expected_type == float or expected_type == 'float64':
                return 0.0
            elif expected_type == str or expected_type == 'object':
                return ''
            elif expected_type == 'datetime64[ns]' or 'datetime' in str(expected_type):
                import pandas as pd

                return pd.NaT
            elif expected_type == bool:
                return False
            else:
                raise ValueError(f'Unknown type specification "{expected_type}" for column "{column_name}" in {self.__class__.__name__}')

        # No fallback patterns - everything must be explicitly defined
        raise ValueError(
            f'Column "{column_name}" not found in cast_types() or index_cast_types() for {self.__class__.__name__}. '
            f'All columns must be explicitly defined in dataframe class methods.'
        )
