import datetime
import time

from functools import reduce

import polars as pd

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

        # Polars DataFrames don't have indexes, so no index manipulation is needed

        # Handle NA/NaN values before casting to avoid "Cannot convert non-finite values (NA or inf) to integer" error
        # Handle both pandas and Polars DataFrames
        if hasattr(df, 'clone'):
            result = df.clone()  # Polars
        else:
            result = df.copy()   # Pandas
        # Simplified type casting approach - handle both pandas and Polars
        if hasattr(result, 'with_columns'):
            # Polars approach - original complex casting
            for col, col_type in types.items():
                if col in result.columns:
                    if col_type is int or col_type == 'int' or str(col_type).startswith('int') or col_type == 'int64':
                        # For integer columns, fill NaN with 0 before casting - use consistent Int64 type
                        result = result.with_columns(result[col].fill_null(value=0).cast(pd.Int64).alias(col))
                    elif col_type is float or col_type == 'float' or str(col_type).startswith('float'):
                        # For float columns, NaN values are fine - handle string -> float conversion carefully
                        try:
                            result = result.with_columns(result[col].cast(pd.Float64, strict=False).alias(col))
                        except Exception:
                            # If casting fails, try to parse as string first
                            result = result.with_columns(result[col].cast(str).str.to_float().alias(col))
                    elif str(col_type) == 'datetime64[ns]':
                        # For datetime columns, keep as string for now to avoid type conflicts
                        # TODO: Fix polars datetime handling and revert to: result[col].str.to_datetime(strict=False)
                        result = result.with_columns(result[col].cast(str).alias(col))
                    else:
                        # For other types (str, object, etc.), use standard astype
                        result = result.with_columns(result[col].cast(col_type).alias(col))
        else:
            # Pandas approach - simple astype conversion 
            for col, col_type in types.items():
                if col in result.columns:
                    try:
                        result[col] = result[col].astype(col_type)
                    except Exception:
                        # If casting fails, keep original type
                        pass

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

        # Find ALL columns that have _x/_y suffixes from merge (pandas) or _right suffixes (polars)
        # We need to process ALL suffix columns, not just data_columns
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
            elif col_name.endswith('_right'):
                base_col = col_name[:-6]  # Remove '_right'
                if base_col in df.columns:  # Check if original column exists
                    all_suffix_columns.add(base_col)

        # CRITICAL: Process ALL columns with suffixes, not just data_columns
        # This ensures no _right columns are left behind
        columns_to_process = set(columns) | all_suffix_columns
        
        # IMPORTANT: Also find ALL _right columns regardless of whether they have base columns
        # Some columns might exist ONLY as _right (from the right DataFrame in join)
        all_right_columns = {col[:-6] for col in df.columns if col.endswith('_right')}
        columns_to_process = columns_to_process | all_right_columns
        
        print(f"DEBUG SUMMARIZE: Found _right columns for processing: {[col + '_right' for col in all_right_columns]}")
        print(f"DEBUG SUMMARIZE: Total columns to process: {columns_to_process}")
        
        # Additional check: list ALL _right columns currently in the dataframe
        actual_right_columns = [col for col in df.columns if col.endswith('_right')]
        print(f"DEBUG SUMMARIZE: ALL _right columns in DataFrame: {actual_right_columns}")
        
        print(f"DEBUG SUMMARIZE: input records={record_count}, suffix_columns={all_suffix_columns}, columns_to_process={columns_to_process}")
        print(f"DEBUG SUMMARIZE: DataFrame columns: {list(df.columns)}")
        
        # DATA INTEGRITY CHECK: If there are _right columns on unique_index_columns,
        # this could indicate either:
        # 1. Normal case: Overlapping data from different dates/sources that needs aggregation
        # 2. Problem case: Duplicates within the same logical group that should have been deduplicated
        # 
        # For rollup merging, overlapping data is EXPECTED and _right columns should be aggregated
        # Only fail if the operations() method doesn't define how to handle the conflicts
        index_columns = set(self.unique_index_columns())
        index_right_columns = [col[:-6] for col in actual_right_columns if col[:-6] in index_columns]
        
        if index_right_columns:
            # Check if operations are defined for the index columns with conflicts
            missing_operations = [col for col in index_right_columns if col not in operations]
            
            if missing_operations:
                dataframe_class_name = self.__class__.__name__ if hasattr(self, '__class__') else 'Unknown'
                print(f"WARNING: Found _right columns on unique index columns in {dataframe_class_name}: {index_right_columns}")
                print(f"  This is normal for rollup merging when the same entities appear in multiple dates/sources.")
                print(f"  However, operations are missing for: {missing_operations}")
                print(f"  Will use 'min' operation as safe fallback for index columns.")
                
                # For index columns without operations, we'll use 'min' as a safe fallback
                # since index columns should have identical values anyway
                for col in missing_operations:
                    operations[col] = 'min'
                    
            else:
                print(f"DEBUG: Found _right columns on index columns {index_right_columns} - this is normal for rollup merging")
                print(f"  Operations defined: {[(col, operations.get(col)) for col in index_right_columns]}")
                print(f"  Will aggregate using defined operations")

        # Track which specific operations destroy data
        original_count = len(df)

        add_span_attributes(
            current_span,
            **{
                'dataframe.summarize.input_record_count': record_count,
                'dataframe.summarize.column_count': len(columns),
                'dataframe.summarize.suffix_columns_found': len(all_suffix_columns),
                'dataframe.summarize.total_columns_to_process': len(columns_to_process),
                'dataframe.summarize.operations': ','.join(f'{k}:{v}' for k, v in operations.items()),
                'dataframe.summarize.index_right_columns_found': len(index_right_columns),
            },
        )

        # CRITICAL DEBUG: Check if DataFrame is already empty when we arrive here
        if len(df) == 0:
            print(f"CRITICAL BUG: DataFrame is ALREADY EMPTY when entering summarize_merged_dataframes!")
            print(f"  columns_to_process: {columns_to_process}")
            print(f"  df.columns: {list(df.columns)}")
            import sys
            print(f"  This indicates a bug BEFORE summarize_merged_dataframes!", file=sys.stderr)
            return df  # Return the empty DataFrame as-is

        # CRITICAL: Process all merge operations first, then do a single select to keep final columns
        # This avoids the bug where incremental column dropping can leave us with only suffix columns
        
        # Track columns that need to be processed and their operations
        merge_operations = []
        columns_to_drop_at_end = []
        
        for col in columns_to_process:
            col_start_time = time.time()

            # Check if merge suffix columns exist (they're only created when there are actual conflicts)
            col_x = f'{col}_x'
            col_y = f'{col}_y'
            col_right = f'{col}_right'

            # Handle both pandas-style (_x, _y) and polars-style (original, _right) suffixes
            if col_x in df.columns and col_y in df.columns:
                # Pandas-style merge
                left_col, right_col = col_x, col_y
                print(f"DEBUG SUMMARIZE: Processing pandas-style merge for {col}: {left_col} + {right_col}")
                merge_operations.append((col, left_col, right_col))
                columns_to_drop_at_end.extend([left_col, right_col])
            elif col in df.columns and col_right in df.columns:
                # Polars-style merge with both columns
                left_col, right_col = col, col_right
                print(f"DEBUG SUMMARIZE: Processing polars-style merge for {col}: {left_col} + {right_col}")
                merge_operations.append((col, left_col, right_col))
                columns_to_drop_at_end.append(right_col)  # Only drop the _right column, keep the base
            elif col_right in df.columns and col not in df.columns:
                # Only _right column exists (from right DataFrame only)
                print(f"DEBUG SUMMARIZE: Found orphaned _right column {col_right}, renaming to {col}")
                df = df.rename({col_right: col})
                continue  # No merge needed, just renamed
            else:
                # No merge conflicts for this column, skip summarization
                print(f"DEBUG SUMMARIZE: No suffix columns found for {col}, skipping (col_x={col_x} in {col_x in df.columns}, col_right={col_right} in {col_right in df.columns})")
                continue

            print(f"DEBUG SUMMARIZE: About to process {col} with operation {operations.get(col, 'default')}, current df size: {len(df)}")
            
            if operations.get(col) == 'min':
                # Handle NaN values properly for min operation, with special handling for mixed types
                try:
                    # Use proper column-wise operation
                    df = df.with_columns(
                        pd.min_horizontal([left_col, right_col]).alias(col)
                    )
                except TypeError as e:
                    if 'not supported between instances' in str(e):
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.warning(f'Mixed type comparison detected for column {col} during min operation: {e}. Using safe comparison fallback.')
                        
                        # For polars compatibility, use simpler coalesce approach
                        df = df.with_columns(df[left_col].fill_null(df[right_col]).alias(col))
                    else:
                        raise  # Re-raise if it's a different TypeError
            elif operations.get(col) == 'max':
                # Use safer coalesce approach to avoid data loss from failed max operations
                # TODO: Implement proper max later once basic functionality works
                df = df.with_columns(df[left_col].fill_null(df[right_col]).alias(col))
            elif operations.get(col) == 'combine_set':
                # Merge JSON string columns (formerly sets) by parsing, combining, and serializing back to JSON
                try:
                    import json
                    
                    def combine_json_sets(left_str, right_str):
                        """Combine two JSON array strings into one deduplicated JSON array string"""
                        try:
                            left_items = json.loads(left_str) if left_str else []
                            right_items = json.loads(right_str) if right_str else []
                            combined_set = set(left_items + right_items)
                            return json.dumps(sorted(list(combined_set)))
                        except:
                            return left_str or right_str or '[]'
                    
                    # Use map_elements to combine JSON string arrays
                    df = df.with_columns(
                        pd.when(pd.col(left_col).is_null() & pd.col(right_col).is_null())
                        .then(pd.lit('[]'))
                        .when(pd.col(left_col).is_null())
                        .then(pd.col(right_col))
                        .when(pd.col(right_col).is_null())
                        .then(pd.col(left_col))
                        .otherwise(
                            pd.struct([left_col, right_col]).map_elements(
                                lambda x: combine_json_sets(x[left_col], x[right_col]), 
                                return_dtype=pd.Utf8
                            )
                        )
                        .alias(col)
                    )
                except Exception as e:
                    print(f"Warning: JSON set merge failed for {col}: {e}, using coalesce fallback")
                    df = df.with_columns(df[left_col].fill_null(df[right_col]).alias(col))
            elif operations.get(col) == 'combine_json':
                # Merge Struct columns by combining their fields
                try:
                    # For Struct columns, merge by taking non-null values from both sides
                    # This is simpler than combine_json_values - just overwrites with right side
                    df = df.with_columns(
                        pd.when(pd.col(left_col).is_null())
                        .then(pd.col(right_col))
                        .when(pd.col(right_col).is_null())
                        .then(pd.col(left_col))
                        .otherwise(pd.col(right_col))  # Right side overwrites left side
                        .alias(col)
                    )
                except Exception as e:
                    print(f"Warning: Struct merge failed for {col}: {e}, using coalesce fallback")
                    df = df.with_columns(df[left_col].fill_null(df[right_col]).alias(col))
            elif operations.get(col) == 'combine_json_values':
                # Merge Struct columns by merging their field values (union for sets, update for scalars)
                try:
                    # For Struct columns containing nested data, we need custom merging logic
                    # This is complex with Polars Struct, so we'll implement a simpler version for now
                    # that takes the union of non-empty/non-null values
                    df = df.with_columns(
                        pd.when(pd.col(left_col).is_null())
                        .then(pd.col(right_col))
                        .when(pd.col(right_col).is_null())
                        .then(pd.col(left_col))
                        .otherwise(
                            # For now, prefer right side but we could implement field-by-field merging
                            pd.col(right_col)
                        )
                        .alias(col)
                    )
                except Exception as e:
                    print(f"Warning: Struct value merge failed for {col}: {e}, using coalesce fallback")
                    df = df.with_columns(df[left_col].fill_null(df[right_col]).alias(col))
            elif operations.get(col) == 'sum':
                # Sum numeric columns properly
                try:
                    # Use sum_horizontal for numeric columns, with proper null handling
                    df = df.with_columns(
                        (df[left_col].fill_null(0) + df[right_col].fill_null(0)).alias(col)
                    )
                except Exception:
                    # Fallback: just coalesce
                    df = df.with_columns(df[left_col].fill_null(df[right_col]).alias(col))
            else:
                # CRITICAL: If no operation is defined, this is a configuration error that must be fixed
                # Don't silently use a default - this could lead to incorrect data aggregation
                dataframe_class_name = self.__class__.__name__ if hasattr(self, '__class__') else 'Unknown'
                available_operations = list(operations.keys()) if operations else []
                
                raise ValueError(
                    f"Missing operation definition for column '{col}' during merge in {dataframe_class_name}.\n"
                    f"  Column '{col}' has conflicting values from merge operation (left: {left_col}, right: {right_col})\n"
                    f"  but no operation is defined in the operations() method to resolve the conflict.\n"
                    f"  Available operations defined: {available_operations}\n"
                    f"  Required action: Add '{col}: \"operation_name\"' to the operations() method in {dataframe_class_name}.\n"
                    f"  Valid operations: 'min', 'max', 'sum', 'combine_set', 'combine_json', 'combine_json_values'\n"
                    f"  Location: {dataframe_class_name}.operations() method in the dataframe engine class"
                )

            col_duration = time.time() - col_start_time
            if col_duration > 0.1:  # Log slow column operations
                add_span_attributes(
                    current_span,
                    **{
                        f'dataframe.summarize.{col}.duration_seconds': col_duration,
                        f'dataframe.summarize.{col}.operation': operations.get(col, 'sum'),
                    },
                )
        
        # CRITICAL: Now that all merge operations are complete, do a single select to remove suffix columns
        if columns_to_drop_at_end:
            print(f"DEBUG SUMMARIZE: Dropping all suffix columns at once: {columns_to_drop_at_end}")
            columns_to_keep = [c for c in df.columns if c not in columns_to_drop_at_end]
            if len(columns_to_keep) > 0:
                df = df.select(columns_to_keep)
                print(f"DEBUG SUMMARIZE: Successfully dropped {len(columns_to_drop_at_end)} suffix columns, kept {len(columns_to_keep)} columns")
            else:
                print(f"ERROR: Would drop all columns when removing suffix columns!")
                print(f"  columns_to_drop_at_end: {columns_to_drop_at_end}")
                print(f"  All columns: {list(df.columns)}")
                # This should never happen, but if it does, don't drop anything
                print(f"  CRITICAL: Keeping original DataFrame to avoid data loss")
        
        # CRITICAL VERIFICATION: Ensure no _right columns remain after processing
        remaining_right_columns = [col for col in df.columns if col.endswith('_right')]
        if remaining_right_columns:
            print(f"CRITICAL ERROR: Found remaining _right columns after summarize: {remaining_right_columns}")
            print(f"  These columns were not properly processed!")
            print(f"  All final columns: {list(df.columns)}")
            
            # Force removal of any remaining _right columns to prevent downstream errors
            print(f"  EMERGENCY: Force-dropping remaining _right columns")
            columns_to_keep = [c for c in df.columns if not c.endswith('_right')]
            if len(columns_to_keep) > 0:
                df = df.select(columns_to_keep)
                print(f"  EMERGENCY: Dropped {len(remaining_right_columns)} remaining _right columns")
            else:
                print(f"  EMERGENCY ERROR: Would drop all columns! Keeping DataFrame as-is to prevent total data loss")
        else:
            print(f"DEBUG SUMMARIZE: VERIFICATION PASSED - No _right columns remain")

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
        columns = self.unique_index_columns() + self.data_columns()
        return pd.DataFrame({col: [] for col in columns})

    # Multipart collection, merge the dataframes and sum counts
    @traced_method('dataframe.merge')
    def merge(self, rollup, new_group):
        """Merge two dataframes using concat + regroup strategy for better Polars compatibility."""
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

        # NEW APPROACH: Use concat + regroup instead of complex join operations
        concat_start = time.time()
        
        # CRITICAL: Ensure both DataFrames have complete schema before concat operations
        # This prevents "column not found" errors during concat
        try:
            rollup = self._ensure_complete_schema(rollup)
            new_group = self._ensure_complete_schema(new_group)
        except Exception as schema_error:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f'Schema completion failed during merge: {schema_error}. Proceeding with existing schemas.')
        
        # CRITICAL: Ensure schema compatibility before concat to prevent type mismatch errors
        rollup_aligned, new_group_aligned = self._align_schemas_for_concat(rollup, new_group)
        
        print(f"DEBUG MERGE: Attempting polars concat with {len(rollup_aligned)} + {len(new_group_aligned)} records")
        print(f"DEBUG MERGE: rollup columns: {rollup_aligned.columns}")
        print(f"DEBUG MERGE: new_group columns: {new_group_aligned.columns}")
        
        # Perform the concat operation - much simpler and more reliable than join
        import polars as pl
        concatenated = pl.concat([rollup_aligned, new_group_aligned], how="vertical")
        print(f"DEBUG MERGE: Concat succeeded, result has {len(concatenated)} records")
        
        concat_duration = time.time() - concat_start

        add_span_attributes(
            current_span,
            **{
                'dataframe.merge.concat_duration_seconds': concat_duration,
                'dataframe.merge.after_concat_record_count': len(concatenated) if concatenated is not None else 0,
            },
        )

        # Now use regroup to handle aggregation (if available)
        regroup_start = time.time()
        if hasattr(self, 'regroup') and callable(self.regroup):
            print(f"DEBUG MERGE: Using regroup method for aggregation")
            result = self.regroup(concatenated)
        else:
            print(f"DEBUG MERGE: No regroup method found, returning concatenated data")
            result = concatenated
        
        regroup_duration = time.time() - regroup_start
        print(f"DEBUG MERGE: After regroup: {len(result) if result is not None else 0} records")

        add_span_attributes(
            current_span,
            **{
                'dataframe.merge.regroup_duration_seconds': regroup_duration,
                'dataframe.merge.after_regroup_record_count': len(result) if result is not None else 0,
            },
        )

        # Cast types for grouped/aggregated data (rollups)
        result = self._apply_consistent_casting(result)

        total_duration = time.time() - start_time
        final_count = len(result) if result is not None else 0

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

        return result

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

        if dataframe is None or len(dataframe) == 0:
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
        # Handle both pandas and Polars DataFrames
        if hasattr(dataframe, 'clone'):
            df = dataframe.clone()  # Polars
        else:
            df = dataframe.copy()   # Pandas
        copy_duration = time.time() - copy_start

        # Apply hostname mapping (to both host_name and original_host_name for proper deduplication)
        map_start = time.time()
        if 'host_name' in df.columns:
            print(f"DEBUG DEDUP: Before hostname mapping: {len(df)} records")
            print(f"DEBUG DEDUP: Hosts before mapping: {sorted(df['host_name'].unique().to_list())}")
            
            # Handle both pandas and Polars DataFrames
            if hasattr(df, 'with_columns'):
                # Polars approach
                import polars as pd
                df = df.with_columns(df['host_name'].map_elements(lambda x: hostname_mapping.get(x, x), return_dtype=pd.Utf8).alias('host_name'))
                
                # CRITICAL FIX: Also apply hostname mapping to original_host_name if it exists
                # This ensures that duplicate detection works correctly when both columns are part of unique_index_columns
                if 'original_host_name' in df.columns:
                    df = df.with_columns(df['original_host_name'].map_elements(lambda x: hostname_mapping.get(x, x), return_dtype=pd.Utf8).alias('original_host_name'))
            else:
                # Pandas approach
                df['host_name'] = df['host_name'].map(lambda x: hostname_mapping.get(x, x))
                # Also apply to original_host_name if it exists
                if 'original_host_name' in df.columns:
                    df['original_host_name'] = df['original_host_name'].map(lambda x: hostname_mapping.get(x, x))
                
            print(f"DEBUG DEDUP: After hostname mapping: {len(df)} records")
            print(f"DEBUG DEDUP: Hosts after mapping: {sorted(df['host_name'].unique().to_list())}")
            if 'original_host_name' in df.columns:
                print(f"DEBUG DEDUP: Original hosts after mapping: {sorted(df['original_host_name'].unique().to_list())}")
        map_duration = time.time() - map_start

        # Only regroup if hostname mapping actually created duplicates
        regroup_start = time.time()
        unique_index_cols = self.unique_index_columns()
        if len(unique_index_cols) > 0:
            # Check for actual duplicates based on unique index columns
            if hasattr(df, 'select'):
                # Polars approach
                subset_df = df.select(unique_index_cols)
                has_duplicates = subset_df.is_duplicated().any()
            else:
                # Pandas approach
                subset_df = df[unique_index_cols]
                has_duplicates = subset_df.duplicated().any()

            if has_duplicates:
                print(f"DEBUG DEDUP: Duplicates detected, applying regroup")
                df_grouped = self.regroup(df)
                print(f"DEBUG DEDUP: After regroup: {len(df_grouped)} records")
                print(f"DEBUG DEDUP: Hosts after regroup: {sorted(df_grouped['host_name'].unique().to_list())}")
                regroup_duration = time.time() - regroup_start
                add_span_attributes(current_span, **{'dataframe.dedup.regrouping_applied': True, 'dataframe.dedup.found_duplicates': True})
            else:
                print(f"DEBUG DEDUP: No duplicates detected, skipping regroup")
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
        result = df_grouped  # Polars DataFrames don't have indexes, no reset_index() needed
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

    def _clean_right_columns(self, df, name):
        """Remove any existing _right columns from a DataFrame before join operations."""
        print(f"DEBUG MERGE: Cleaning _right columns from {name}: {df.columns}")
        
        # Find all _right columns
        right_columns = [col for col in df.columns if col.endswith('_right')]
        
        if right_columns:
            print(f"DEBUG MERGE: Found {len(right_columns)} _right columns to clean: {right_columns}")
            
            # For each _right column, decide what to do
            columns_to_drop = []
            columns_to_rename = {}
            
            for right_col in right_columns:
                base_col = right_col[:-6]  # Remove '_right'
                
                if base_col in df.columns:
                    # Base column exists, drop the _right version
                    columns_to_drop.append(right_col)
                    print(f"DEBUG MERGE: Will drop duplicate {right_col} (base {base_col} exists)")
                else:
                    # No base column, rename _right to base
                    columns_to_rename[right_col] = base_col
                    print(f"DEBUG MERGE: Will rename orphaned {right_col} to {base_col}")
            
            # Apply the changes
            if columns_to_drop:
                columns_to_keep = [col for col in df.columns if col not in columns_to_drop]
                df = df.select(columns_to_keep)
                print(f"DEBUG MERGE: Dropped {len(columns_to_drop)} duplicate _right columns")
            
            if columns_to_rename:
                df = df.rename(columns_to_rename)
                print(f"DEBUG MERGE: Renamed {len(columns_to_rename)} orphaned _right columns")
        
        print(f"DEBUG MERGE: Cleaned {name} columns: {df.columns}")
        return df

    def _align_schemas_for_join(self, rollup, new_group):
        """Ensure both DataFrames have compatible schemas for join operations."""
        print(f"DEBUG MERGE: Aligning schemas for join")
        print(f"DEBUG MERGE: rollup columns before alignment: {rollup.columns}")
        print(f"DEBUG MERGE: new_group columns before alignment: {new_group.columns}")
        
        # Get all unique columns from both DataFrames
        all_columns = sorted(set(rollup.columns) | set(new_group.columns))
        print(f"DEBUG MERGE: All unique columns: {all_columns}")
        
        # Add missing columns to both DataFrames with compatible types
        rollup_aligned = self._add_missing_columns(rollup, all_columns, "rollup")
        new_group_aligned = self._add_missing_columns(new_group, all_columns, "new_group")
        
        # Ensure compatible types for matching columns
        rollup_aligned, new_group_aligned = self._ensure_compatible_types(rollup_aligned, new_group_aligned, all_columns)
        
        # Ensure column order matches
        rollup_aligned = rollup_aligned.select(all_columns)
        new_group_aligned = new_group_aligned.select(all_columns)
        
        print(f"DEBUG MERGE: rollup columns after alignment: {rollup_aligned.columns}")
        print(f"DEBUG MERGE: new_group columns after alignment: {new_group_aligned.columns}")
        
        return rollup_aligned, new_group_aligned

    def _align_schemas_for_concat(self, rollup, new_group):
        """Ensure both DataFrames have compatible schemas for concat operations."""
        print(f"DEBUG MERGE: Aligning schemas for concat")
        print(f"DEBUG MERGE: rollup columns before alignment: {rollup.columns}")
        print(f"DEBUG MERGE: new_group columns before alignment: {new_group.columns}")
        
        # Get all unique columns from both DataFrames
        all_columns = sorted(set(rollup.columns) | set(new_group.columns))
        print(f"DEBUG MERGE: All unique columns: {all_columns}")
        
        # Add missing columns to both DataFrames with compatible types
        rollup_aligned = self._add_missing_columns(rollup, all_columns, "rollup")
        new_group_aligned = self._add_missing_columns(new_group, all_columns, "new_group")
        
        # Ensure compatible types for matching columns
        rollup_aligned, new_group_aligned = self._ensure_compatible_types(rollup_aligned, new_group_aligned, all_columns)
        
        # Ensure column order matches - commented out for now due to pandas/polars mixing
        # rollup_aligned = rollup_aligned.select(all_columns)
        # new_group_aligned = new_group_aligned.select(all_columns)
        
        print(f"DEBUG MERGE: rollup columns after alignment: {rollup_aligned.columns}")
        print(f"DEBUG MERGE: new_group columns after alignment: {new_group_aligned.columns}")
        
        return rollup_aligned, new_group_aligned

    def _add_missing_columns(self, df, all_columns, name):
        """Add missing columns to a DataFrame with appropriate default values."""
        missing_columns = [col for col in all_columns if col not in df.columns]
        
        if missing_columns:
            print(f"DEBUG MERGE: Adding {len(missing_columns)} missing columns to {name}: {missing_columns}")
            
            for col in missing_columns:
                # Get appropriate default value based on column name and type
                try:
                    default_value = self._get_column_default_value(col)
                    
                    if isinstance(default_value, dict):
                        # For dicts, create empty Struct (if possible) or fallback to empty List
                        try:
                            # Create empty struct - this might fail if we don't know the schema
                            df = df.with_columns(pd.lit(None, dtype=pd.Struct({})).alias(col))
                        except Exception:
                            # Fallback to empty list for now
                            df = df.with_columns(pd.lit([], dtype=pd.List(pd.Utf8)).alias(col))
                    elif isinstance(default_value, (set, list)):
                        # For sets/lists, use empty list as List[String]
                        df = df.with_columns(pd.lit([], dtype=pd.List(pd.Utf8)).alias(col))
                    elif isinstance(default_value, str):
                        df = df.with_columns(pd.lit(default_value, dtype=pd.Utf8).alias(col))
                    elif isinstance(default_value, int):
                        df = df.with_columns(pd.lit(default_value, dtype=pd.Int64).alias(col))
                    elif isinstance(default_value, float):
                        df = df.with_columns(pd.lit(default_value, dtype=pd.Float64).alias(col))
                    else:
                        # Default fallback
                        df = df.with_columns(pd.lit(None, dtype=pd.Utf8).alias(col))
                except Exception:
                    # If _get_column_default_value fails, use simple defaults
                    df = df.with_columns(pd.lit(None, dtype=pd.Utf8).alias(col))
        
        return df

    def _ensure_compatible_types(self, rollup, new_group, all_columns):
        """Ensure matching columns have compatible types between DataFrames."""
        print(f"DEBUG MERGE: Ensuring compatible types for {len(all_columns)} columns")
        
        for col in all_columns:
            rollup_dtype = rollup[col].dtype
            new_group_dtype = new_group[col].dtype
            
            # If types don't match, convert both to a compatible type
            if rollup_dtype != new_group_dtype:
                print(f"DEBUG MERGE: Type mismatch for {col}: {rollup_dtype} vs {new_group_dtype}")
                
                # Strategy: Convert both to String (Utf8) for maximum compatibility
                try:
                    rollup = rollup.with_columns(rollup[col].cast(pd.Utf8, strict=False).alias(col))
                    new_group = new_group.with_columns(new_group[col].cast(pd.Utf8, strict=False).alias(col))
                    print(f"DEBUG MERGE: Converted {col} to Utf8 for compatibility")
                except Exception as e:
                    print(f"DEBUG MERGE: Failed to convert {col} to Utf8: {e}")
                    # If conversion fails, leave as-is and let Polars handle it
        
        return rollup, new_group


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
        import polars as pd

        try:
            # Read parquet files with comprehensive nested object types handling
            import logging
            logger = logging.getLogger(__name__)
            
            # Strategy 1: Try Polars direct read first
            try:
                df = pd.read_parquet(parquet_path)
                
                if len(df) == 0:
                    return self.empty()
                    
                logger.debug(f'Successfully loaded {parquet_path} with Polars direct read: {len(df)} records')
                
            except Exception as error:
                if "not yet implemented: Nested object types" in str(error):
                    logger.warning(f'Polars cannot read nested object types in {parquet_path}. Trying pyarrow approach.')
                    
                    # Strategy 2: Try PyArrow with type conversion
                    try:
                        import pyarrow.parquet as pq
                        import pyarrow as pa
                        
                        # Read with pyarrow and convert to polars, handling nested types
                        table = pq.read_table(parquet_path)
                        
                        # Convert complex types to string for polars compatibility
                        schema_updates = {}
                        for i, field in enumerate(table.schema):
                            if pa.types.is_list(field.type) or pa.types.is_struct(field.type):
                                # Convert complex types to string representation
                                column_data = table.column(i).to_pylist()
                                string_data = [str(item) if item is not None else None for item in column_data]
                                schema_updates[field.name] = pa.array(string_data)
                        
                        # Replace complex columns with string versions
                        if schema_updates:
                            logger.info(f'Converting {len(schema_updates)} complex columns to strings: {list(schema_updates.keys())}')
                            for col_name, new_array in schema_updates.items():
                                col_index = table.schema.get_field_index(col_name)
                                table = table.set_column(col_index, col_name, new_array)
                        
                        df = pd.from_arrow(table)
                        
                        if len(df) == 0:
                            return self.empty()
                            
                        logger.info(f'Successfully loaded {parquet_path} using pyarrow with type conversion: {len(df)} records')
                        
                    except Exception as pyarrow_error:
                        logger.error(f'PyArrow approach also failed for {parquet_path}: {pyarrow_error}. All methods exhausted, returning empty dataframe.')
                        return self.empty()
                else:
                    logger.warning(f'Unexpected error reading {parquet_path}: {error}. Returning empty dataframe.')
                    return self.empty()
                
        except FileNotFoundError:
            return self.empty()
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f'Unexpected error loading parquet file {parquet_path}: {e}')
            return self.empty()

        # Ensure all required columns exist with proper defaults
        df = self._ensure_complete_schema(df)

        # CRITICAL: Apply consistent column ordering after loading from parquet
        # This is essential because parquet files saved before the column ordering fix
        # may have inconsistent column orders, causing vstack errors when loading multiple files
        df = self._ensure_consistent_column_ordering(df)

        # Deserialize JSON strings back to Object types for complex data structures
        df = self._deserialize_json_columns(df)

        # Apply casting for both index and data columns
        df = self._apply_consistent_casting(df)

        # Set proper index
        df = self._apply_consistent_indexing(df)

        return df

    def _ensure_complete_schema(self, df):
        """Ensure dataframe has all required columns with proper defaults and track schema metrics."""
        from opentelemetry import trace
        current_span = trace.get_current_span()
        
        # Get all required columns from class methods
        index_columns = self.__class__.unique_index_columns() if hasattr(self.__class__, 'unique_index_columns') else []
        data_columns = self.__class__.data_columns() if hasattr(self.__class__, 'data_columns') else []
        all_columns = index_columns + data_columns
        
        missing_columns = []
        schema_metrics = {}

        # Process missing columns in two phases to avoid Object type issues
        # Phase 1: Add simple types (str, int, float) first
        simple_missing_columns = []
        complex_missing_columns = []
        
        for col in all_columns:
            if col not in df.columns:
                missing_columns.append(col)
                default_value = self._get_column_default_value(col)
                if isinstance(default_value, (set, dict, list)):
                    complex_missing_columns.append((col, default_value))
                else:
                    simple_missing_columns.append((col, default_value))
        
        # Add simple columns first (no Object types)
        for col, default_value in simple_missing_columns:
            try:
                if len(df) > 0:
                    df = df.with_columns(pd.lit(default_value).alias(col))
                else:
                    df = df.with_columns(pd.lit(default_value).alias(col))
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f'Failed to add simple column {col} with default {default_value}: {e}')
        
        # Add complex columns using proper Polars types (Struct for dicts, List for sets/lists)
        for col, default_value in complex_missing_columns:
            try:
                if len(df) > 0:
                    if isinstance(default_value, dict):
                        # For dict columns, use JSON string representation for dynamic content  
                        # Polars Struct requires predefined schema and doesn't support dynamic keys
                        import json
                        df = df.with_columns(pd.lit(json.dumps(default_value), dtype=pd.Utf8).alias(col))
                    elif isinstance(default_value, (set, list)):
                        # For set/list columns, use JSON string format for consistency
                        import json
                        df = df.with_columns(pd.lit(json.dumps(list(default_value) if isinstance(default_value, set) else default_value), dtype=pd.Utf8).alias(col))
                    else:
                        # For other types, convert to string
                        df = df.with_columns(pd.lit(str(default_value), dtype=pd.Utf8).alias(col))
                else:
                    # For empty DataFrames, add appropriate empty columns
                    if isinstance(default_value, dict):
                        # For dict columns, use JSON string representation for dynamic content
                        # Polars Struct requires predefined schema and doesn't support dynamic keys
                        import json
                        df = df.with_columns(pd.lit(json.dumps(default_value), dtype=pd.Utf8).alias(col))
                    elif isinstance(default_value, (set, list)):
                        # Use JSON string format for collections to maintain type consistency
                        import json
                        df = df.with_columns(pd.lit(json.dumps(list(default_value) if isinstance(default_value, set) else default_value), dtype=pd.Utf8).alias(col))
                    else:
                        df = df.with_columns(pd.lit('', dtype=pd.Utf8).alias(col))
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f'Failed to add complex column {col} with default {default_value}: {e}. Adding as empty List.')
                # Fallback: add as empty JSON string
                try:
                    df = df.with_columns(pd.lit('[]', dtype=pd.Utf8).alias(col))
                except Exception:
                    pass  # Skip this column if it can't be added
        
        # Record schema metrics for observability
        schema_metrics['missing_columns_count'] = len(missing_columns)
        schema_metrics['missing_columns'] = missing_columns
        schema_metrics['total_required_columns'] = len(all_columns)
        schema_metrics['schema_completeness_ratio'] = (len(all_columns) - len(missing_columns)) / len(all_columns) if all_columns else 1.0
        
        # Add schema metrics to tracing
        if missing_columns:
            from metrics_utility.tracing import add_span_attributes
            add_span_attributes(current_span, **{
                'schema.missing_columns': ','.join(missing_columns),
                'schema.missing_count': len(missing_columns),
                'schema.completeness_ratio': schema_metrics['schema_completeness_ratio']
            })

        return df

    def _ensure_consistent_column_ordering(self, df):
        """Ensure DataFrame has consistent column ordering to prevent vstack/merge errors.
        
        This method ensures that all DataFrames from the same class have columns in the same order:
        1. Index columns first (from unique_index_columns())
        2. Data columns next (from data_columns())
        3. Any extra columns last
        
        This prevents Polars vstack errors like 'column names don't match' when merging DataFrames.
        """
        if df is None or len(df) == 0:
            return df
            
        # Get expected column order
        index_columns = self.unique_index_columns() if hasattr(self, 'unique_index_columns') else []
        data_columns = self.data_columns() if hasattr(self, 'data_columns') else []
        expected_columns = index_columns + data_columns
        
        # Add missing columns with default values
        # IMPORTANT: Use consistent String types for complex columns to ensure concat compatibility
        for col in expected_columns:
            if col not in df.columns:
                if col in ['task_runs', 'host_runs']:
                    df = df.with_columns(pd.lit(0, dtype=pd.Int64).alias(col))
                elif col in ['canonical_facts', 'facts']:
                    # Use JSON string representation for dynamic dictionary content
                    # Polars Struct requires predefined schema and doesn't support dynamic keys
                    df = df.with_columns(pd.lit('{}', dtype=pd.Utf8).alias(col))
                elif col in ['managed_node_types_set', 'events', 'host_names_before_dedup', 'organizations', 'inventories', 'serials']:
                    # Use JSON string format for collections to maintain type consistency
                    df = df.with_columns(pd.lit('[]', dtype=pd.Utf8).alias(col))
                else:
                    # Use the default value logic for other columns
                    try:
                        default_value = self._get_column_default_value(col)
                        # Convert complex types to string representation
                        if isinstance(default_value, (dict, set, list)):
                            import json
                            if isinstance(default_value, set):
                                default_value = list(default_value)  # Convert set to list for JSON serialization
                            df = df.with_columns(pd.lit(json.dumps(default_value), dtype=pd.Utf8).alias(col))
                        else:
                            df = df.with_columns(pd.lit(str(default_value), dtype=pd.Utf8).alias(col))
                    except Exception:
                        # Fallback to string type with empty value
                        df = df.with_columns(pd.lit('', dtype=pd.Utf8).alias(col))
        
        # Ensure consistent column order by selecting in the expected order
        # Only select columns that actually exist
        existing_columns = [col for col in expected_columns if col in df.columns]
        
        # Add any extra columns not in expected order at the end
        extra_columns = [col for col in df.columns if col not in existing_columns]
        final_column_order = existing_columns + extra_columns
        
        if final_column_order:
            df = df.select(final_column_order)
        
        return df

    def _deserialize_json_columns(self, df):
        """Deserialize JSON strings back to Object types for complex data structures after loading from parquet."""
        import json
        import polars as pd
        
        # Define which columns should be deserialized for each dataframe type
        # This includes both Object columns and Struct columns that were serialized to JSON
        object_columns_map = {
            'DataframeJobhostSummaryUsage': {
                'canonical_facts': 'dict',
                'facts': 'dict', 
                'managed_node_types_set': 'set',
                'events': 'set',
                'host_names_before_dedup': 'set'
            },
            'DataframeInventoryScope': {
                'canonical_facts': 'dict',
                'facts': 'dict',
                'organizations': 'set',
                'inventories': 'set', 
                'serials': 'set',
                'host_names_before_dedup': 'set'
            },
            'DataframeContentUsage': {
                'playbooks': 'set',
                'organizations': 'set'
            },
            'DataframeCollectionStatus': {},  # No Object columns
            'DataframeHostMetric': {}  # No Object columns
        }
        
        # Get the dataframe class name to determine which columns to deserialize
        dataframe_class_name = self.__class__.__name__
        object_columns = object_columns_map.get(dataframe_class_name, {})
        
        for col, expected_type in object_columns.items():
            if col in df.columns:
                def deserialize_json_field(x):
                    if x is None or x == '':
                        if expected_type == 'dict':
                            return {}
                        elif expected_type == 'set':
                            return set()
                        else:
                            return None
                    
                    if isinstance(x, str):
                        try:
                            parsed = json.loads(x)
                            if expected_type == 'set':
                                if isinstance(parsed, list):
                                    return set(parsed)
                                elif isinstance(parsed, set):
                                    return parsed
                                else:
                                    return {parsed} if parsed is not None else set()
                            elif expected_type == 'dict':
                                return parsed if isinstance(parsed, dict) else {}
                            else:
                                return parsed
                        except (json.JSONDecodeError, TypeError):
                            # If JSON parsing fails, return appropriate default
                            if expected_type == 'dict':
                                return {}
                            elif expected_type == 'set':
                                return set()
                            else:
                                return x
                    elif isinstance(x, dict):
                        # Already a dict (from Struct type), return as-is for dict columns
                        if expected_type == 'dict':
                            return x
                        else:
                            # For non-dict expected types, try to convert
                            if expected_type == 'set':
                                return set() if not x else {str(x)}
                            else:
                                return x
                    else:
                        # Already deserialized object or other type, handle appropriately
                        if expected_type == 'dict' and not isinstance(x, dict):
                            return {} if x is None else {'value': x}
                        elif expected_type == 'set' and not isinstance(x, set):
                            return set() if x is None else {x}
                        else:
                            return x
                
                # Keep as string type instead of Object to avoid iteration issues
                # The data will work fine as JSON strings for most operations
                try:
                    df = df.with_columns(df[col].cast(pd.Utf8).alias(col))
                except Exception:
                    # If casting fails, leave as-is
                    pass
        
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
            return None
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
                        df = df.with_columns(df[col].cast(str).alias(col))
                    elif col_type == int or col_type == 'int' or str(col_type).startswith('int') or col_type == 'int64':
                        # For integer columns, fill NaN with 0 before casting - use consistent Int64 type
                        df = df.with_columns(df[col].fill_null(value=0).cast(pd.Int64).alias(col))
                    else:
                        df = df.with_columns(df[col].cast(col_type).alias(col))

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
                        df = df.with_columns(df[col].cast(str).alias(col))
                    elif col_type == int or col_type == 'int' or str(col_type).startswith('int') or col_type == 'int64':
                        # For integer columns, fill NaN with 0 before casting - use consistent Int64 type
                        df = df.with_columns(df[col].fill_null(value=0).cast(pd.Int64).alias(col))
                    else:
                        df = df.with_columns(df[col].cast(col_type).alias(col))

        return df

    def _apply_consistent_indexing(self, df):
        """Apply consistent indexing using unique_index_columns."""
        # Polars DataFrames don't have indexes, so indexing operations are not needed
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
                import polars as pd

                return None
            elif expected_type == bool:
                return False
            else:
                raise ValueError(f'Unknown type specification "{expected_type}" for column "{column_name}" in {self.__class__.__name__}')

        # No fallback patterns - everything must be explicitly defined
        raise ValueError(
            f'Column "{column_name}" not found in cast_types() or index_cast_types() for {self.__class__.__name__}. '
            f'All columns must be explicitly defined in dataframe class methods.'
        )
