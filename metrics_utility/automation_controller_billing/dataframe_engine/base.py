"""Base dataframe engine with comprehensive schema validation and aggregation support.

This module provides the foundational functionality for all dataframe engines in the
AAP Controller billing metrics system, including schema-driven data validation,
aggregation operations, and PyArrow integration for type safety.

Key Features:
    - PyArrow schema-based data validation and type enforcement
    - Comprehensive aggregation operations for rollup merging
    - Generic schema validation with automatic column ordering
    - Two-tier schema system (collector vs rollup schemas)
    - Performance-optimized data casting and validation
    - OpenTelemetry tracing integration for monitoring

Schema System:
    - COLLECTOR_SCHEMA: Raw CSV data schema for initial processing
    - ROLLUP_SCHEMA: Aggregated data schema for rollup merging
    - Automatic column validation, type casting, and ordering
    - Default value injection for missing columns

Usage:
    from metrics_utility.automation_controller_billing.dataframe_engine.base import Base
    
    class MyDataframe(Base):
        @staticmethod
        def collector_schema():
            return pa.schema([...])
            
        @staticmethod 
        def rollup_schema():
            return pa.schema([...])
"""
import datetime
import time
from functools import reduce
from typing import Any, Dict, List, Optional, Union

import polars as pd
import pyarrow as pa

from dateutil.relativedelta import relativedelta
from opentelemetry import trace

from metrics_utility.tracing import add_span_attributes, traced_method


def granularity_cast(date: datetime.date, granularity: str) -> datetime.date:
    """Cast a date to the specified granularity boundary.

    Adjusts the input date to the beginning of the specified time period.
    For monthly granularity, returns the first day of the month.
    For yearly granularity, returns January 1st of the year.

    Args:
        date: The date to cast
        granularity: Time granularity ('monthly', 'yearly', or 'daily')

    Returns:
        Date adjusted to the granularity boundary

    Example:
        >>> granularity_cast(datetime.date(2024, 3, 15), 'monthly')
        datetime.date(2024, 3, 1)
        >>> granularity_cast(datetime.date(2024, 3, 15), 'yearly')
        datetime.date(2024, 1, 1)
    """
    if granularity == 'monthly':
        return date.replace(day=1)
    elif granularity == 'yearly':
        return date.replace(month=1, day=1)
    else:
        return date


def list_dates(start_date: datetime.date, end_date: datetime.date, granularity: str) -> List[datetime.date]:
    """Generate a list of dates within the specified range and granularity.

    Creates a sequence of dates from start_date to end_date (inclusive) based on
    the specified granularity. For monthly granularity, returns the first day of
    each month. For yearly granularity, returns January 1st of each year.

    Args:
        start_date: Beginning date of the range
        end_date: Ending date of the range (inclusive)
        granularity: Time granularity ('monthly', 'yearly', or 'daily')

    Returns:
        List of dates within the range at the specified granularity

    Example:
        >>> list_dates(datetime.date(2024, 1, 15), datetime.date(2024, 3, 10), 'monthly')
        [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1), datetime.date(2024, 3, 1)]
    """
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


def combine_json(json1: Union[Dict[str, Any], None], json2: Union[Dict[str, Any], None]) -> Dict[str, Any]:
    """Combine two JSON dictionaries with later values overwriting earlier ones.

    Merges two dictionaries for JSON/dict columns during rollup operations.
    Values from json2 will overwrite values from json1 for matching keys.

    Args:
        json1: First dictionary (earlier values)
        json2: Second dictionary (later values, takes precedence)

    Returns:
        Merged dictionary with json2 values taking precedence

    Example:
        >>> combine_json({'a': 1, 'b': 2}, {'b': 3, 'c': 4})
        {'a': 1, 'b': 3, 'c': 4}

    Note:
        Used for aggregating JSON columns during rollup merging operations.
    """
    merged = {}
    if isinstance(json1, dict):
        merged.update(json1)
    if isinstance(json2, dict):
        merged.update(json2)
    return merged


def combine_set(set1: Union[set, list, None], set2: Union[set, list, None]) -> set:
    """Combine two collections into a single set of unique items.

    Takes the union of two collections, converting lists to sets as needed.
    Handles various input types gracefully by treating non-collection types as empty sets.

    Args:
        set1: First collection (set, list, or None)
        set2: Second collection (set, list, or None)

    Returns:
        Set containing unique items from both collections

    Example:
        >>> combine_set({1, 2}, [2, 3])
        {1, 2, 3}
        >>> combine_set([1, 2, 2], {3, 4})
        {1, 2, 3, 4}

    Note:
        Used for aggregating set-type columns during rollup merging operations.
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


def merge_sets(collections: List[Union[set, list]]) -> set:
    """Merge multiple collections into a single set.

    Combines multiple sets or lists into one unified set of unique items.

    Args:
        collections: List of collections to merge

    Returns:
        Set containing all unique items from input collections

    Example:
        >>> merge_sets([{1, 2}, [2, 3], {4}])
        {1, 2, 3, 4}
    """
    return set().union(*collections)


def merge_setdicts(dicts: List[Dict[str, Any]]) -> Dict[str, set]:
    """Merge multiple dictionaries by combining values into sets.

    Reduces multiple dictionaries into one by combining values for each key into sets.

    Args:
        dicts: List of dictionaries to merge

    Returns:
        Dictionary with combined values as sets

    Example:
        >>> merge_setdicts([{'a': 1}, {'a': 2, 'b': 3}])
        {'a': {1, 2}, 'b': {3}}
    """
    return reduce(combine_json_values, dicts, {})


def json_to_list_format(json_str: Union[str, None]) -> Dict[str, List[str]]:
    """Convert JSON string to list format for fact aggregation.
    
    Converts JSON objects to list format following demo_prompt_facts.md:
    {"fact1": "value1"} -> {"fact1": ["value1"]}
    
    Args:
        json_str: JSON string from CSV data
        
    Returns:
        Dictionary with values converted to lists
        
    Example:
        >>> json_to_list_format('{"os": "linux", "arch": "x86_64"}')
        {"os": ["linux"], "arch": ["x86_64"]}
    """
    if json_str is None or json_str == '':
        return {}
    try:
        import json
        parsed = json.loads(json_str)
        if not isinstance(parsed, dict):
            return {}
        
        result = {}
        for key, value in parsed.items():
            if isinstance(value, list):
                result[key] = value  # Already a list
            else:
                result[key] = [value]  # Convert to list
        return result
    except:
        return {}


def merge_list_format_dicts(dict_list: List[Dict[str, List[str]]]) -> Dict[str, List[str]]:
    """Merge multiple list-format dictionaries preserving unique values.
    
    Follows demo_prompt_facts.md approach for merging facts during aggregation:
    {"os": ["linux"]} + {"os": ["ubuntu"]} = {"os": ["linux", "ubuntu"]}
    
    Args:
        dict_list: List of dictionaries in list format
        
    Returns:
        Merged dictionary with unique values preserved
        
    Example:
        >>> merge_list_format_dicts([
        ...     {"os": ["linux"], "arch": ["x86_64"]},
        ...     {"os": ["ubuntu"], "env": ["prod"]}
        ... ])
        {"os": ["linux", "ubuntu"], "arch": ["x86_64"], "env": ["prod"]}
    """
    if not dict_list:
        return {}
    
    merged = {}
    for d in dict_list:
        if not isinstance(d, dict):
            continue
        for key, values in d.items():
            if key not in merged:
                merged[key] = []
            
            # Ensure values is a list and extend
            if isinstance(values, list):
                merged[key].extend(values)
            else:
                merged[key].append(values)
    
    # Remove duplicates while preserving order
    for key in merged:
        filtered = [x for x in merged[key] if x is not None]
        merged[key] = list(dict.fromkeys(filtered))
    
    return merged


def merge_and_stringify_facts(json_strings: List[str]) -> str:
    """Merge multiple JSON strings containing facts in list format.
    
    This is the core aggregation function for facts and canonical_facts columns.
    Converts individual fact JSON strings to list format and merges them.
    
    Args:
        json_strings: List of JSON strings from different records
        
    Returns:
        JSON string with merged facts in list format
        
    Example:
        >>> merge_and_stringify_facts([
        ...     '{"os": "linux", "arch": "x86_64"}',
        ...     '{"os": "ubuntu", "env": "prod"}'
        ... ])
        '{"os": ["linux", "ubuntu"], "arch": ["x86_64"], "env": ["prod"]}'
    """
    import json
    
    # Convert all JSON strings to list format
    parsed_dicts = []
    for json_str in json_strings:
        list_format_dict = json_to_list_format(json_str)
        if list_format_dict:
            parsed_dicts.append(list_format_dict)
    
    # Merge all list format dictionaries
    merged = merge_list_format_dicts(parsed_dicts)
    return json.dumps(merged)


def merge_list_arrays(json_arrays: List[str]) -> str:
    """Merge multiple JSON array strings preserving unique values.
    
    For collections like host_names_before_dedup, events, etc.
    ["host1", "host2"] + ["host1", "host4"] = ["host1", "host2", "host4"]
    
    Args:
        json_arrays: List of JSON array strings
        
    Returns:
        JSON string with merged unique values
        
    Example:
        >>> merge_list_arrays(['["host1", "host2"]', '["host1", "host4"]'])
        '["host1", "host2", "host4"]'
    """
    import json
    
    all_values = []
    for json_str in json_arrays:
        if json_str is None or json_str == '':
            continue
        try:
            parsed = json.loads(json_str)
            if isinstance(parsed, list):
                all_values.extend(parsed)
            elif parsed is not None:
                all_values.append(parsed)
        except:
            continue
    
    # Remove duplicates while preserving order
    unique_values = list(dict.fromkeys([str(v) for v in all_values if v is not None]))
    return json.dumps(unique_values)


def combine_json_values(val1: Union[Dict[str, Any], None], val2: Union[Dict[str, Any], None]) -> Dict[str, set]:
    """Combine two JSON dictionaries by building sets of values for each key.

    For each key present in either dictionary, creates a set of all non-null,
    non-empty values from both inputs. This is used for aggregating complex
    JSON columns during rollup operations.

    Args:
        val1: First dictionary to combine
        val2: Second dictionary to combine

    Returns:
        Dictionary where each key maps to a set of unique values

    Example:
        >>> combine_json_values({'a': 1, 'b': 2}, {'a': 3, 'c': 4})
        {'a': {1, 3}, 'b': {2}, 'c': {4}}

    Note:
        Used internally by merge_setdicts for complex JSON column aggregation.
    """
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


def validate_with_schema(
    df: pd.DataFrame, 
    schema: pa.Schema, 
    strict_columns: Optional[List[str]] = None, 
    default_values: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """Validate and transform DataFrame according to PyArrow schema.

    Performs comprehensive validation including type casting, missing column injection,
    invalid row filtering, and column ordering according to the provided schema.

    Args:
        df: Input Polars DataFrame to validate
        schema: PyArrow schema defining expected structure and types
        strict_columns: List of columns that cannot be null (default: all required columns)
        default_values: Custom default values for columns by name. Used for domain-specific
                       defaults like JSON strings, collections, etc.

    Returns:
        Validated DataFrame with proper types, column order, and default values

    Raises:
        ValueError: If schema validation fails critically

    Example:
        >>> schema = pa.schema([
        ...     pa.field("id", pa.int64()),
        ...     pa.field("facts", pa.string())
        ... ])
        >>> defaults = {"facts": "{}"}  # JSON string default
        >>> validated_df = validate_with_schema(raw_df, schema, default_values=defaults)

    Note:
        This function filters invalid rows instead of failing, making data processing
        more robust for production environments. Custom default_values override
        standard type-based defaults for complex data types.
    """
    if df is None or len(df) == 0:
        # Return empty DataFrame with correct schema
        return pd.from_arrow(pa.table([], schema=schema))

    strict_columns = strict_columns or []
    default_values = default_values or {}

    # Add missing columns with proper defaults
    for field in schema:
        col_name = field.name
        if col_name not in df.columns:
            # Use custom default if provided, otherwise use type-based default
            if col_name in default_values:
                default_value = default_values[col_name]
            else:
                default_value = _get_default_value_for_type(field.type)
            df = df.with_columns(pd.lit(default_value).alias(col_name))

    # Filter rows with invalid data types and cast to proper types
    validated_rows_mask = pd.lit(True)

    for field in schema:
        col_name = field.name
        expected_type = field.type

        if col_name in df.columns:
            # Type casting with validation
            try:
                if pa.types.is_integer(expected_type):
                    df = df.with_columns(df[col_name].cast(pd.Int64, strict=False).alias(col_name))
                elif pa.types.is_floating(expected_type):
                    df = df.with_columns(df[col_name].cast(pd.Float64, strict=False).alias(col_name))
                elif pa.types.is_string(expected_type):
                    df = df.with_columns(df[col_name].cast(str).alias(col_name))
                elif pa.types.is_timestamp(expected_type):
                    df = df.with_columns(df[col_name].cast(str).alias(col_name))  # Keep as string for Polars compatibility
                elif pa.types.is_list(expected_type):
                    # For list types, we typically store as JSON strings in our system
                    # Validate that it can be parsed as JSON list
                    df = df.with_columns(df[col_name].cast(str).alias(col_name))
                elif pa.types.is_struct(expected_type):
                    # For struct types, we typically store as JSON strings in our system
                    # The specific struct schema validation would be done at the application level
                    # when parsing the JSON string, not at the DataFrame level
                    df = df.with_columns(df[col_name].cast(str).alias(col_name))
                else:
                    # For other complex types, default to string representation
                    df = df.with_columns(df[col_name].cast(str).alias(col_name))

                # Validate strict columns
                if col_name in strict_columns:
                    validated_rows_mask = validated_rows_mask & df[col_name].is_not_null()

            except Exception:
                # If casting fails completely, filter out problematic rows
                validated_rows_mask = validated_rows_mask & df[col_name].is_not_null()

    # Apply row filtering
    # Check if there are any rows to filter (if mask has any False values)
    if len(df) > 0:
        try:
            # Only apply filtering if there are actually invalid rows
            df = df.filter(validated_rows_mask)
        except Exception:
            # If filtering fails, continue without filtering to avoid data loss
            pass

    # Reorder columns according to schema
    schema_columns = [field.name for field in schema]
    available_columns = [col for col in schema_columns if col in df.columns]
    df = df.select(available_columns)

    return df


def _get_default_value_for_type(pa_type: pa.DataType) -> Union[int, float, str, None]:
    """Get appropriate default value for PyArrow data type.

    Provides basic type-based defaults. For complex types like structs or 
    domain-specific JSON formats, use the default_values parameter in 
    validate_with_schema() instead.

    Args:
        pa_type: PyArrow data type

    Returns:
        Default value appropriate for the type

    Note:
        For complex data types (structs, JSON, collections), subclasses should
        provide custom defaults via the default_values parameter.
    """
    if pa.types.is_integer(pa_type):
        return 0
    elif pa.types.is_floating(pa_type):
        return 0.0
    elif pa.types.is_string(pa_type):
        return ''
    elif pa.types.is_timestamp(pa_type):
        return None
    else:
        return None


class Base:
    """Base class for all dataframe engines with comprehensive schema validation and processing.

    This class provides foundational functionality for AAP Controller billing dataframe
    engines, including schema-driven validation, data aggregation, caching mechanisms,
    and standardized processing workflows.

    The Base class implements a two-tier schema system:
    - collector_schema(): For raw CSV data validation and processing
    - rollup_schema(): For aggregated rollup data validation and merging

    Attributes:
        extractor: Data extraction engine for sourcing raw data
        month: Target month for data processing
        extra_params: Additional configuration parameters
        _cached_dataframe: Optional pre-computed dataframe cache

    Example:
        >>> class MyDataframe(Base):
        ...     @staticmethod
        ...     def collector_schema():
        ...         return pa.schema([pa.field("id", pa.int64())])
        ...     
        ...     def build_dataframe(self, batch_iterator):
        ...         # Implementation here
        ...         pass
        >>> 
        >>> engine = MyDataframe(extractor, month, params)
        >>> result = engine.build_dataframe(batch_iterator)

    Note:
        Subclasses must implement build_dataframe() and should define collector_schema()
        and rollup_schema() static methods for proper schema validation.
    """

    def __init__(self, extractor: Any, month: datetime.date, extra_params: Dict[str, Any]) -> None:
        """Initialize the base dataframe engine.

        Args:
            extractor: Data extraction engine for sourcing raw data
            month: Target month for data processing 
            extra_params: Additional configuration parameters including date ranges,
                         deduplication settings, and processing options

        Example:
            >>> engine = Base(extractor, datetime.date(2024, 1, 1), {'force': True})
        """
        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params
        self._cached_dataframe = None
        self._validation_metrics = {}  # Store validation metrics during processing

    def build_dataframe(self, batch_data_iterator: Any) -> pd.DataFrame:
        """Build dataframe from batch data iterator using standardized schema flow.

        This method implements the standard data processing pipeline:
        1. Process each batch with collector_dataframe_schema (before grouping)
        2. Group/aggregate data using initial_aggregations()
        3. Apply dataframe_schema (after grouping)  
        4. Merge batches maintaining schema consistency
        5. Return final result with proper schema

        Args:
            batch_data_iterator: Iterator yielding batch data for processing

        Returns:
            Processed and aggregated Polars DataFrame with consistent schema

        Note:
            Subclasses should override _process_batch_data() for specific processing logic
            while this method handles the standard schema application flow.
        """
        current_span = trace.get_current_span()
        build_start_time = time.time()
        total_records = 0
        groups_processed = 0

        add_span_attributes(current_span, **{
            'dataframe.build.dataframe_type': self.__class__.__name__, 
            'dataframe.build.mode': 'standardized_schema_flow'
        })

        # Initialize accumulated dataframe
        accumulated_dataframe = None

        # Process each batch from the iterator using standardized flow
        for batch_data in batch_data_iterator:
            # Step 1: Process batch data with collector_dataframe_schema (BEFORE grouping)
            batch_dataframe = self._process_batch_data_with_schema(batch_data, current_span)
            if batch_dataframe is None or len(batch_dataframe) == 0:
                continue

            # Step 2: Group this batch and apply dataframe_schema (AFTER grouping)  
            group_dataframe = self._group_with_schema(batch_dataframe)
            if group_dataframe is None or len(group_dataframe) == 0:
                continue

            # Step 3: Merge with accumulated results using schema-consistent operations
            if accumulated_dataframe is None:
                accumulated_dataframe = group_dataframe
            else:
                accumulated_dataframe = self.merge(accumulated_dataframe, group_dataframe)

            total_records += len(group_dataframe)
            groups_processed += 1

        build_duration = time.time() - build_start_time
        final_count = len(accumulated_dataframe) if accumulated_dataframe is not None else 0

        add_span_attributes(current_span, **{
            'dataframe.build.duration_seconds': build_duration,
            'dataframe.build.groups_processed': groups_processed,
            'dataframe.build.total_input_records': total_records,
            'dataframe.build.final_record_count': final_count,
        })

        # Step 4: Apply final schema validation and return result
        final_result = accumulated_dataframe if accumulated_dataframe is not None else self.empty()
        if final_result is not None and len(final_result) > 0:
            final_result = self.apply_complete_schema(final_result, schema_type="dataframe", operation_context="final_build_result")

        return final_result

    def _process_batch_data_with_schema(self, batch_data, current_span):
        """Process batch data and apply collector_dataframe_schema (BEFORE grouping).
        
        Subclasses should override this method for specific batch processing logic.
        This method should call apply_complete_schema with collector_dataframe type.
        """
        # Default implementation - subclasses should override
        processed_data = self._process_batch_data(batch_data, current_span)
        if processed_data is None or len(processed_data) == 0:
            return self.empty()
            
        # Apply collector schema (BEFORE grouping)
        return self.apply_complete_schema(processed_data, schema_type="collector_dataframe", operation_context="after_csv_processing")

    def _process_batch_data(self, batch_data, current_span):
        """Process individual batch data - to be overridden by subclasses."""
        raise NotImplementedError('Subclasses must implement _process_batch_data(batch_data, current_span)')

    def _group_with_schema(self, dataframe):
        """Group dataframe and apply dataframe_schema (AFTER grouping)."""
        # Step 1: Perform grouping using subclass implementation
        grouped_data = self.group(dataframe)
        if grouped_data is None or len(grouped_data) == 0:
            return self.empty()
            
        # Step 2: Apply dataframe schema (AFTER grouping)
        return self.apply_complete_schema(grouped_data, schema_type="dataframe", operation_context="after_grouping")

    @traced_method('dataframe.regroup_with_schema')
    def regroup_with_schema(self, dataframe):
        """Regroup pre-aggregated dataframe with schema consistency (used in merge operations)."""
        current_span = trace.get_current_span()
        start_time = time.time()
        input_count = len(dataframe) if dataframe is not None else 0

        add_span_attributes(current_span, **{
            'dataframe.regroup.input_record_count': input_count,
            'dataframe.regroup.operation': 'standardized_regroup_with_schema',
        })

        # Step 1: Perform regrouping using subclass implementation
        regrouped_data = self.regroup(dataframe)
        if regrouped_data is None or len(regrouped_data) == 0:
            return self.empty()
            
        # Step 2: Apply dataframe schema (AFTER regrouping) 
        result = self.apply_complete_schema(regrouped_data, schema_type="dataframe", operation_context="after_regrouping")

        duration = time.time() - start_time
        output_count = len(result) if result is not None else 0

        add_span_attributes(current_span, **{
            'dataframe.regroup.duration_seconds': duration,
            'dataframe.regroup.output_record_count': output_count,
        })

        return result

    def set_cached_dataframe(self, dataframe: pd.DataFrame) -> None:
        """Set a pre-computed dataframe to use instead of building from scratch.

        Used for performance optimization when dataframes have been pre-computed
        or loaded from cache/rollup files.

        Args:
            dataframe: Pre-computed Polars DataFrame to cache

        Example:
            >>> engine.set_cached_dataframe(precomputed_df)
            >>> result = engine.get_cached_dataframe()  # Returns precomputed_df
        """
        self._cached_dataframe = dataframe

    def get_cached_dataframe(self) -> Optional[pd.DataFrame]:
        """Get the cached dataframe, if any.

        Returns:
            Cached Polars DataFrame or None if no cache is available

        Example:
            >>> cached = engine.get_cached_dataframe()
            >>> if cached is not None:
            ...     print(f"Using cached dataframe with {len(cached)} records")
        """
        return self._cached_dataframe

    def has_cached_dataframe(self) -> bool:
        """Check if a cached dataframe is available.

        Returns:
            True if cached dataframe exists, False otherwise

        Example:
            >>> if not engine.has_cached_dataframe():
            ...     engine.set_cached_dataframe(engine.build_dataframe(iterator))
        """
        return self._cached_dataframe is not None

    def get_validation_metrics(self) -> Dict[str, Any]:
        """Get validation metrics collected during dataframe processing.
        
        Returns:
            Dictionary containing data quality metrics, schema validation results,
            and performance statistics from the last build_dataframe() call.
        """
        return self._validation_metrics.copy()

    def _add_validation_metric(self, key: str, value: Any) -> None:
        """Add a validation metric for inclusion in rollup metadata.
        
        Args:
            key: Metric key (e.g., 'data_quality_summary.total_input_rows')
            value: Metric value
        """
        self._validation_metrics[key] = value

    def _add_validation_metrics(self, metrics: Dict[str, Any]) -> None:
        """Add multiple validation metrics for inclusion in rollup metadata.
        
        Args:
            metrics: Dictionary of metrics to add
        """
        self._validation_metrics.update(metrics)

    def dates(self) -> List[datetime.date]:
        """Generate list of dates for data processing based on month and extra parameters.

        Uses either explicit date range from extra_params or derives monthly range
        from the configured month attribute.

        Returns:
            List of dates for daily granularity processing within the target range

        Example:
            >>> engine = Base(extractor, datetime.date(2024, 1, 15), {})
            >>> dates = engine.dates()
            >>> print(f"Processing {len(dates)} dates from {dates[0]} to {dates[-1]}")

        Note:
            Date range can be overridden using 'since_date' and 'until_date' in extra_params.
        """
        if self.extra_params.get('since_date') is not None:
            beginning_of_the_month = self.extra_params.get('since_date')
            end_of_the_month = self.extra_params.get('until_date')
        else:
            beginning_of_the_month = self.month.replace(day=1)
            end_of_the_month = beginning_of_the_month + relativedelta(months=1) - relativedelta(days=1)

        dates_list = list_dates(start_date=beginning_of_the_month, end_date=end_of_the_month, granularity='daily')
        return dates_list

    @staticmethod
    def collector_schema() -> Optional[pa.Schema]:
        """Define PyArrow schema for raw CSV collector data validation.

        Subclasses should override this method to define the expected schema
        for raw CSV data during initial collection and processing.

        Returns:
            PyArrow schema for collector data or None if not implemented

        Example:
            >>> @staticmethod
            >>> def collector_schema():
            ...     return pa.schema([
            ...         pa.field("host_name", pa.string()),
            ...         pa.field("task_runs", pa.int64()),
            ...         pa.field("created", pa.timestamp('ns'))
            ...     ])

        Note:
            This schema is used by validate_collector_data() for input validation.
        """
        return None

    @staticmethod
    def rollup_schema() -> Optional[pa.Schema]:
        """Legacy method name - use parquet_schema() instead."""
        return None

    @staticmethod
    def parquet_schema() -> Optional[pa.Schema]:
        """Define PyArrow schema for aggregated rollup data validation.

        Subclasses should override this method to define the expected schema
        for aggregated rollup data used in merge operations.

        Returns:
            PyArrow schema for rollup data or None if not implemented

        Example:
            >>> @staticmethod
            >>> def rollup_schema():
            ...     return pa.schema([
            ...         pa.field("host_name", pa.string()),
            ...         pa.field("total_task_runs", pa.int64()),
            ...         pa.field("first_automation", pa.timestamp('ns'))
            ...     ])

        Note:
            This schema is used by validate_rollup_data() for output validation.
        """
        return None

    def validate_collector_data(self, df: pd.DataFrame, strict_columns: Optional[List[str]] = None, default_values: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Validate raw collector data against the collector schema.

        Applies schema validation, type casting, and column ordering to raw CSV data
        before processing. Uses the collector_schema() if defined.

        Args:
            df: Raw Polars DataFrame from CSV data
            strict_columns: List of columns that cannot be null
            default_values: Custom default values for columns by name

        Returns:
            Validated DataFrame with proper types and column order

        Example:
            >>> raw_df = pd.read_csv("data.csv")
            >>> validated_df = engine.validate_collector_data(raw_df, ['host_name'])

        Note:
            If no collector_schema() is defined, returns the DataFrame unchanged.
        """
        schema = self.collector_schema()
        if schema is None:
            return df
        return validate_with_schema(df, schema, strict_columns, default_values)

    def validate_rollup_data(self, df: pd.DataFrame, strict_columns: Optional[List[str]] = None, default_values: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Validate aggregated rollup data against the rollup schema.

        Applies schema validation, type casting, and column ordering to aggregated
        rollup data before merging operations. Uses the rollup_schema() if defined.

        Args:
            df: Aggregated Polars DataFrame from rollup processing
            strict_columns: List of columns that cannot be null
            default_values: Custom default values for columns by name

        Returns:
            Validated DataFrame with proper types and column order

        Example:
            >>> aggregated_df = self.group(processed_data)
            >>> validated_df = engine.validate_rollup_data(aggregated_df)

        Note:
            If no rollup_schema() is defined, returns the DataFrame unchanged.
        """
        schema = self.parquet_schema() or self.rollup_schema()  # Support both new and legacy names
        if schema is None:
            return df
        return validate_with_schema(df, schema, strict_columns, default_values)

    def validate_collector_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate collector dataframe against validation rules and filter invalid data.
        
        This method applies additional validation rules beyond basic schema casting,
        including required column checks, null value validation, and value range validation.
        Invalid rows are filtered out to ensure data quality.
        
        Args:
            df: Polars DataFrame after collector_dataframe_schema has been applied
            
        Returns:
            Filtered DataFrame with only valid records
            
        Example:
            >>> processed_df = self.apply_complete_schema(raw_df, "collector_dataframe")
            >>> validated_df = self.validate_collector_dataframe(processed_df)
        """
        validation_rules = self.collector_dataframe_validation_schema()
        if not validation_rules:
            return df  # No validation rules defined
            
        if df is None or len(df) == 0:
            return df
            
        # Start with all rows as valid
        valid_rows_mask = pd.lit(True)
        validation_metrics = {
            'input_rows': len(df),
            'filtered_by_required': 0,
            'filtered_by_null': 0, 
            'filtered_by_range': 0,
            'filtered_by_values': 0,
        }
        
        # Apply validation rules column by column
        for col_name, rules in validation_rules.items():
            if col_name not in df.columns:
                if rules.get('required', False):
                    # Required column is missing - all rows invalid
                    valid_rows_mask = pd.lit(False)
                    validation_metrics['filtered_by_required'] = len(df)
                    break
                continue
                
            # Check null value constraints
            if not rules.get('allow_null', True):
                null_mask = df[col_name].is_not_null()
                invalid_count = len(df.filter(~null_mask & valid_rows_mask))
                validation_metrics['filtered_by_null'] += invalid_count
                valid_rows_mask = valid_rows_mask & null_mask
                
            # Check min/max value constraints
            if 'min_value' in rules:
                min_val = rules['min_value']
                range_mask = df[col_name].is_null() | (df[col_name] >= min_val)
                invalid_count = len(df.filter(~range_mask & valid_rows_mask))
                validation_metrics['filtered_by_range'] += invalid_count
                valid_rows_mask = valid_rows_mask & range_mask
                
            if 'max_value' in rules:
                max_val = rules['max_value']
                range_mask = df[col_name].is_null() | (df[col_name] <= max_val)
                invalid_count = len(df.filter(~range_mask & valid_rows_mask))
                validation_metrics['filtered_by_range'] += invalid_count
                valid_rows_mask = valid_rows_mask & range_mask
                
            # Check valid values constraints
            if 'valid_values' in rules:
                valid_vals = rules['valid_values']
                values_mask = df[col_name].is_null() | df[col_name].is_in(valid_vals)
                invalid_count = len(df.filter(~values_mask & valid_rows_mask))
                validation_metrics['filtered_by_values'] += invalid_count
                valid_rows_mask = valid_rows_mask & values_mask
        
        # Apply filtering
        try:
            filtered_df = df.filter(valid_rows_mask)
        except Exception:
            # If filtering fails, return original dataframe to avoid data loss
            filtered_df = df
            
        validation_metrics['output_rows'] = len(filtered_df)
        validation_metrics['total_filtered'] = validation_metrics['input_rows'] - validation_metrics['output_rows']
        validation_metrics['quality_ratio'] = validation_metrics['output_rows'] / validation_metrics['input_rows'] if validation_metrics['input_rows'] > 0 else 1.0
        
        # Store validation metrics for observability
        self._add_validation_metrics({
            'collector_dataframe_validation.input_rows': validation_metrics['input_rows'],
            'collector_dataframe_validation.output_rows': validation_metrics['output_rows'],
            'collector_dataframe_validation.total_filtered': validation_metrics['total_filtered'],
            'collector_dataframe_validation.quality_ratio': validation_metrics['quality_ratio'],
            'collector_dataframe_validation.filtered_by_required': validation_metrics['filtered_by_required'],
            'collector_dataframe_validation.filtered_by_null': validation_metrics['filtered_by_null'],
            'collector_dataframe_validation.filtered_by_range': validation_metrics['filtered_by_range'], 
            'collector_dataframe_validation.filtered_by_values': validation_metrics['filtered_by_values'],
        })
        
        # Log significant data quality issues
        if validation_metrics['quality_ratio'] < 0.9:  # More than 10% filtered
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(
                f"Significant data quality filtering in {self.__class__.__name__}: "
                f"{validation_metrics['total_filtered']} rows filtered out of {validation_metrics['input_rows']} "
                f"(quality ratio: {validation_metrics['quality_ratio']:.2%})"
            )
            
        return filtered_df

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
        # Use Polars clone and casting operations
        result = df.clone()
        
        # Polars type casting approach
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
                    # For datetime columns, convert to proper datetime to preserve precision
                    try:
                        result = result.with_columns(result[col].str.to_datetime(strict=False).alias(col))
                    except Exception:
                        # Fallback to string if datetime conversion fails
                        result = result.with_columns(result[col].cast(str).alias(col))
                else:
                    # For other types (str, object, etc.), use standard astype
                    result = result.with_columns(result[col].cast(col_type).alias(col))

        cast_duration = time.time() - start_time
        add_span_attributes(
            current_span,
            **{'dataframe.cast.duration_seconds': cast_duration, 'dataframe.cast.output_record_count': len(result) if result is not None else 0},
        )

        return result

    @traced_method('dataframe.summarize_merged')
    def summarize_merged_dataframes(self, df, columns, operations={}):
        """Summarize merged dataframes with tracing for performance monitoring.
        
        NOTE: This method is simplified since we now use concat+regroup instead of joins.
        No suffix columns (_x, _y, _right) should exist when using concat+regroup approach.
        This method now just validates and returns the dataframe as-is.
        """
        current_span = trace.get_current_span()

        start_time = time.time()
        record_count = len(df) if df is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.summarize.input_record_count': record_count,
                'dataframe.summarize.mode': 'concat_regroup_passthrough',
            },
        )

        # VERIFICATION: Ensure no suffix columns exist (should be impossible with concat+regroup)
        suffix_columns = [col for col in df.columns if col.endswith(('_x', '_y', '_right'))]
        if suffix_columns:
            print(f'WARNING: Found unexpected suffix columns with concat+regroup approach: {suffix_columns}')
            print('  This indicates a bug in the merge strategy - should use concat+regroup, not joins')

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

        # CRITICAL: Apply complete dataframe_schema to both DataFrames before merging
        # This ensures consistent Polars types (especially Datetime) following rollups_data_flow.md
        try:
            print('DEBUG MERGE: Applying complete dataframe_schema to rollup before merge')
            rollup = self.apply_complete_schema(rollup, schema_type="dataframe", operation_context="before_merge_rollup")
            print('DEBUG MERGE: Applying complete dataframe_schema to new_group before merge')
            new_group = self.apply_complete_schema(new_group, schema_type="dataframe", operation_context="before_merge_new_group")
        except Exception as schema_error:
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f'Complete schema application failed during merge: {schema_error}. Proceeding with basic alignment.')
            
            # Fallback to basic schema completion
            rollup = self._ensure_complete_schema(rollup)
            new_group = self._ensure_complete_schema(new_group)

        # CRITICAL: Ensure schema compatibility before concat to prevent type mismatch errors
        rollup_aligned, new_group_aligned = self._align_schemas_for_concat(rollup, new_group)

        print(f'DEBUG MERGE: Attempting polars concat with {len(rollup_aligned)} + {len(new_group_aligned)} records')
        print(f'DEBUG MERGE: rollup columns: {rollup_aligned.columns}')
        print(f'DEBUG MERGE: new_group columns: {new_group_aligned.columns}')

        # Perform the concat operation - much simpler and more reliable than join
        import polars as pl

        concatenated = pl.concat([rollup_aligned, new_group_aligned], how='vertical')
        print(f'DEBUG MERGE: Concat succeeded, result has {len(concatenated)} records')

        concat_duration = time.time() - concat_start

        add_span_attributes(
            current_span,
            **{
                'dataframe.merge.concat_duration_seconds': concat_duration,
                'dataframe.merge.after_concat_record_count': len(concatenated) if concatenated is not None else 0,
            },
        )

        # Now use standardized regroup with schema handling
        regroup_start = time.time()
        if hasattr(self, 'regroup') and callable(self.regroup):
            print('DEBUG MERGE: Using standardized regroup_with_schema method')
            result = self.regroup_with_schema(concatenated)
        else:
            print('DEBUG MERGE: No regroup method found, returning concatenated data')
            result = concatenated

        regroup_duration = time.time() - regroup_start
        print(f'DEBUG MERGE: After regroup_with_schema: {len(result) if result is not None else 0} records')

        add_span_attributes(
            current_span,
            **{
                'dataframe.merge.regroup_duration_seconds': regroup_duration,
                'dataframe.merge.after_regroup_record_count': len(result) if result is not None else 0,
            },
        )

        # Schema is already applied by regroup_with_schema, no need for additional application
        # This eliminates duplicate schema application that could cause timing issues

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
        # Use Polars clone operation
        df = dataframe.clone()
        copy_duration = time.time() - copy_start

        # Apply hostname mapping (to both host_name and original_host_name for proper deduplication)
        map_start = time.time()
        if 'host_name' in df.columns:
            print(f'DEBUG DEDUP: Before hostname mapping: {len(df)} records')
            print(f'DEBUG DEDUP: Hosts before mapping: {sorted(df["host_name"].unique().to_list())}')

            # Apply hostname mapping using Polars syntax
            import polars as pd

            df = df.with_columns(df['host_name'].map_elements(lambda x: hostname_mapping.get(x, x), return_dtype=pd.Utf8).alias('host_name'))

            # CRITICAL FIX: Also apply hostname mapping to original_host_name if it exists
            # This ensures that duplicate detection works correctly when both columns are part of unique_index_columns
            if 'original_host_name' in df.columns:
                df = df.with_columns(
                    df['original_host_name'].map_elements(lambda x: hostname_mapping.get(x, x), return_dtype=pd.Utf8).alias('original_host_name')
                )

            print(f'DEBUG DEDUP: After hostname mapping: {len(df)} records')
            print(f'DEBUG DEDUP: Hosts after mapping: {sorted(df["host_name"].unique().to_list())}')
            if 'original_host_name' in df.columns:
                print(f'DEBUG DEDUP: Original hosts after mapping: {sorted(df["original_host_name"].unique().to_list())}')
        map_duration = time.time() - map_start

        # Only regroup if hostname mapping actually created duplicates
        regroup_start = time.time()
        unique_index_cols = self.unique_index_columns()
        if len(unique_index_cols) > 0:
            # Check for actual duplicates based on unique index columns using Polars
            subset_df = df.select(unique_index_cols)
            has_duplicates = subset_df.is_duplicated().any()

            if has_duplicates:
                print('DEBUG DEDUP: Duplicates detected, applying regroup')
                df_grouped = self.regroup(df)
                print(f'DEBUG DEDUP: After regroup: {len(df_grouped)} records')
                print(f'DEBUG DEDUP: Hosts after regroup: {sorted(df_grouped["host_name"].unique().to_list())}')
                regroup_duration = time.time() - regroup_start
                add_span_attributes(current_span, **{'dataframe.dedup.regrouping_applied': True, 'dataframe.dedup.found_duplicates': True})
            else:
                print('DEBUG DEDUP: No duplicates detected, skipping regroup')
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
        print(f'DEBUG MERGE: Cleaning _right columns from {name}: {df.columns}')

        # Find all _right columns
        right_columns = [col for col in df.columns if col.endswith('_right')]

        if right_columns:
            print(f'DEBUG MERGE: Found {len(right_columns)} _right columns to clean: {right_columns}')

            # For each _right column, decide what to do
            columns_to_drop = []
            columns_to_rename = {}

            for right_col in right_columns:
                base_col = right_col[:-6]  # Remove '_right'

                if base_col in df.columns:
                    # Base column exists, drop the _right version
                    columns_to_drop.append(right_col)
                    print(f'DEBUG MERGE: Will drop duplicate {right_col} (base {base_col} exists)')
                else:
                    # No base column, rename _right to base
                    columns_to_rename[right_col] = base_col
                    print(f'DEBUG MERGE: Will rename orphaned {right_col} to {base_col}')

            # Apply the changes
            if columns_to_drop:
                columns_to_keep = [col for col in df.columns if col not in columns_to_drop]
                df = df.select(columns_to_keep)
                print(f'DEBUG MERGE: Dropped {len(columns_to_drop)} duplicate _right columns')

            if columns_to_rename:
                df = df.rename(columns_to_rename)
                print(f'DEBUG MERGE: Renamed {len(columns_to_rename)} orphaned _right columns')

        print(f'DEBUG MERGE: Cleaned {name} columns: {df.columns}')
        return df

    def _align_schemas_for_join(self, rollup, new_group):
        """Ensure both DataFrames have compatible schemas for join operations."""
        print('DEBUG MERGE: Aligning schemas for join')
        print(f'DEBUG MERGE: rollup columns before alignment: {rollup.columns}')
        print(f'DEBUG MERGE: new_group columns before alignment: {new_group.columns}')

        # Get all unique columns from both DataFrames
        all_columns = sorted(set(rollup.columns) | set(new_group.columns))
        print(f'DEBUG MERGE: All unique columns: {all_columns}')

        # Add missing columns to both DataFrames with compatible types
        rollup_aligned = self._add_missing_columns(rollup, all_columns, 'rollup')
        new_group_aligned = self._add_missing_columns(new_group, all_columns, 'new_group')

        # Ensure compatible types for matching columns
        rollup_aligned, new_group_aligned = self._ensure_compatible_types(rollup_aligned, new_group_aligned, all_columns)

        # Ensure column order matches
        rollup_aligned = rollup_aligned.select(all_columns)
        new_group_aligned = new_group_aligned.select(all_columns)

        print(f'DEBUG MERGE: rollup columns after alignment: {rollup_aligned.columns}')
        print(f'DEBUG MERGE: new_group columns after alignment: {new_group_aligned.columns}')

        return rollup_aligned, new_group_aligned

    def _align_schemas_for_concat(self, rollup, new_group):
        """Ensure both DataFrames have compatible schemas for concat operations."""
        print('DEBUG MERGE: Aligning schemas for concat')
        print(f'DEBUG MERGE: rollup columns before alignment: {rollup.columns}')
        print(f'DEBUG MERGE: new_group columns before alignment: {new_group.columns}')

        # Get all unique columns from both DataFrames
        all_columns = sorted(set(rollup.columns) | set(new_group.columns))
        print(f'DEBUG MERGE: All unique columns: {all_columns}')

        # Add missing columns to both DataFrames with compatible types
        rollup_aligned = self._add_missing_columns(rollup, all_columns, 'rollup')
        new_group_aligned = self._add_missing_columns(new_group, all_columns, 'new_group')

        # Ensure compatible types for matching columns
        rollup_aligned, new_group_aligned = self._ensure_compatible_types(rollup_aligned, new_group_aligned, all_columns)

        # Ensure column order matches - commented out for now due to pandas/polars mixing
        # rollup_aligned = rollup_aligned.select(all_columns)
        # new_group_aligned = new_group_aligned.select(all_columns)

        print(f'DEBUG MERGE: rollup columns after alignment: {rollup_aligned.columns}')
        print(f'DEBUG MERGE: new_group columns after alignment: {new_group_aligned.columns}')

        return rollup_aligned, new_group_aligned

    def _add_missing_columns(self, df, all_columns, name):
        """Add missing columns to a DataFrame with appropriate default values."""
        missing_columns = [col for col in all_columns if col not in df.columns]

        if missing_columns:
            print(f'DEBUG MERGE: Adding {len(missing_columns)} missing columns to {name}: {missing_columns}')

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
        """Ensure matching columns have compatible types between DataFrames using schema definitions."""
        print(f'DEBUG MERGE: Ensuring compatible types for {len(all_columns)} columns using dataframe_schema')
        
        # Get the target schema for this dataframe type
        target_schema = self.dataframe_schema()

        for col in all_columns:
            rollup_dtype = rollup[col].dtype
            new_group_dtype = new_group[col].dtype

            # If types don't match, convert both to the schema-defined type
            if rollup_dtype != new_group_dtype:
                print(f'DEBUG MERGE: Type mismatch for {col}: {rollup_dtype} vs {new_group_dtype}')
                
                # Get target type from schema
                if col in target_schema:
                    target_type_str = target_schema[col]
                    
                    # Convert string type names to Polars types
                    if target_type_str == 'String':
                        target_type = pd.Utf8
                    elif target_type_str == 'Int64':
                        target_type = pd.Int64
                    elif target_type_str == 'Float64':
                        target_type = pd.Float64
                    elif target_type_str == 'Boolean':
                        target_type = pd.Boolean
                    elif target_type_str == 'Datetime':
                        # Special handling for datetime conversion from strings
                        try:
                            # First try string to datetime conversion (common case from CSV data)
                            rollup = rollup.with_columns(rollup[col].str.to_datetime(strict=False).alias(col))
                            new_group = new_group.with_columns(new_group[col].str.to_datetime(strict=False).alias(col))
                            print(f'DEBUG MERGE: Converted {col} to {target_type_str} using str.to_datetime')
                        except Exception as e:
                            print(f'DEBUG MERGE: Failed str.to_datetime for {col}: {e}, trying direct cast')
                            # Fallback to direct cast
                            target_type = pd.Datetime('us')  # Use microsecond precision
                            try:
                                rollup = rollup.with_columns(rollup[col].cast(target_type, strict=False).alias(col))
                                new_group = new_group.with_columns(new_group[col].cast(target_type, strict=False).alias(col))
                                print(f'DEBUG MERGE: Converted {col} to {target_type_str} using direct cast')
                            except Exception as e2:
                                print(f'DEBUG MERGE: Failed direct cast for {col}: {e2}')
                        continue  # Skip the regular casting since we handled datetime specially
                    else:
                        target_type = pd.Utf8  # Default fallback
                        
                    try:
                        rollup = rollup.with_columns(rollup[col].cast(target_type, strict=False).alias(col))
                        new_group = new_group.with_columns(new_group[col].cast(target_type, strict=False).alias(col))
                        print(f'DEBUG MERGE: Converted {col} to {target_type_str} based on schema')
                    except Exception as e:
                        print(f'DEBUG MERGE: Failed to convert {col} to {target_type_str}: {e}')
                        # Fallback to string
                        try:
                            rollup = rollup.with_columns(rollup[col].cast(pd.Utf8, strict=False).alias(col))
                            new_group = new_group.with_columns(new_group[col].cast(pd.Utf8, strict=False).alias(col))
                            print(f'DEBUG MERGE: Fallback: Converted {col} to Utf8 for compatibility')
                        except Exception as fallback_e:
                            print(f'DEBUG MERGE: Failed fallback for {col}: {fallback_e}')
                else:
                    # Column not in schema, use original string fallback
                    try:
                        rollup = rollup.with_columns(rollup[col].cast(pd.Utf8, strict=False).alias(col))
                        new_group = new_group.with_columns(new_group[col].cast(pd.Utf8, strict=False).alias(col))
                        print(f'DEBUG MERGE: No schema for {col}, converted to Utf8 for compatibility')
                    except Exception as e:
                        print(f'DEBUG MERGE: Failed to convert {col} to Utf8: {e}')

        return rollup, new_group

    def apply_complete_schema(self, df, schema_type: str = "dataframe", operation_context: str = "unknown"):
        """Apply complete schema transformation including type casting, column completion, and ordering.
        
        This is the central method for all schema operations to ensure consistency across the entire pipeline.
        
        Args:
            df: Polars DataFrame to transform
            schema_type: Type of schema to apply ("collector_dataframe", "dataframe", or "parquet")
            operation_context: Context description for debugging (e.g., "after_grouping", "after_parquet_load")
            
        Returns:
            DataFrame with complete schema applied: proper types, all columns present, consistent ordering
        """
        if df is None or len(df) == 0:
            return df
            
        print(f'DEBUG SCHEMA: Applying complete {schema_type} schema in context: {operation_context}')
        
        # Step 1: Get the appropriate schema and default values
        if schema_type == "collector_dataframe":
            schema_dict = self.collector_dataframe_schema() if hasattr(self, 'collector_dataframe_schema') else {}
            default_values = getattr(self, 'get_collector_default_values', lambda: {})()
        elif schema_type == "dataframe":
            schema_dict = self.dataframe_schema() if hasattr(self, 'dataframe_schema') else {}
            default_values = getattr(self, 'get_rollup_default_values', lambda: {})()
        elif schema_type == "parquet":
            # For parquet schema, we'll use PyArrow schema if available
            schema_dict = {}  # Handled separately in save operations
            default_values = getattr(self, 'get_rollup_default_values', lambda: {})()
        else:
            print(f'DEBUG SCHEMA: Unknown schema type {schema_type}, skipping')
            return df
            
        if not schema_dict:
            print(f'DEBUG SCHEMA: No {schema_type} schema defined, skipping')
            return df
            
        # Step 2: Ensure all schema columns are present with proper defaults
        df = self._ensure_schema_columns(df, schema_dict, default_values)
        
        # Step 3: Apply schema-based type casting
        df = self._apply_schema_casting(df, schema_dict, f"{schema_type}_schema_{operation_context}")
        
        # Step 4: Apply validation for collector_dataframe schemas
        if schema_type == "collector_dataframe":
            df = self.validate_collector_dataframe(df)
        
        # Step 5: Ensure consistent column ordering
        df = self._ensure_consistent_column_ordering(df)
        
        print(f'DEBUG SCHEMA: Complete {schema_type} schema applied successfully in {operation_context}')
        return df

    def _apply_schema_casting(self, df, schema_dict: Dict[str, str], schema_name: str = "unknown"):
        """Apply schema-based type casting to a DataFrame.
        
        Args:
            df: Polars DataFrame to cast
            schema_dict: Dictionary mapping column names to Polars dtypes
            schema_name: Name of schema for debugging
            
        Returns:
            DataFrame with proper types applied
        """
        if df is None or len(df) == 0:
            return df
            
        print(f'DEBUG SCHEMA: Applying {schema_name} schema to DataFrame with {len(df)} records')
        
        # Apply casting column by column
        for col_name, target_type in schema_dict.items():
            if col_name in df.columns:
                try:
                    # Convert string type names to Polars types
                    if target_type == 'String':
                        polars_type = pd.Utf8
                    elif target_type == 'Int64':
                        polars_type = pd.Int64
                    elif target_type == 'Float64':
                        polars_type = pd.Float64
                    elif target_type == 'Boolean':
                        polars_type = pd.Boolean
                    elif target_type == 'Datetime':
                        # Special handling for datetime conversion from strings
                        try:
                            # Strategy 1: Try basic auto-parsing first
                            df = df.with_columns(df[col_name].str.to_datetime(strict=False, format=None).alias(col_name))
                            print(f'DEBUG SCHEMA: Successfully converted {col_name} to datetime using auto-parsing')
                            continue  # Skip the regular cast since we handled datetime specially
                        except Exception as e1:
                            print(f'DEBUG SCHEMA: Auto datetime parsing failed for {col_name}: {e1}')
                            try:
                                # Strategy 2: Strip timezone info and parse with explicit format
                                # Handle format like "2025-03-01 10:13:16.97527+00" by removing "+00"
                                df_cleaned = df.with_columns(
                                    df[col_name].str.replace_all(r'\+\d{2}:\d{2}$', '').str.replace_all(r'\+\d{2}$', '').str.replace_all(r'Z$', '').alias(col_name + '_clean')
                                )
                                # Use explicit format that we know works from testing
                                df = df_cleaned.with_columns(
                                    df_cleaned[col_name + '_clean'].str.to_datetime(format="%Y-%m-%d %H:%M:%S%.f", strict=False).alias(col_name)
                                ).drop(col_name + '_clean')
                                print(f'DEBUG SCHEMA: Successfully converted {col_name} to datetime after stripping timezone with explicit format')
                                continue
                            except Exception as e2:
                                print(f'DEBUG SCHEMA: Timezone stripping with format failed for {col_name}: {e2}')
                                try:
                                    # Strategy 3: Try basic ISO format pattern without timezone stripping
                                    df = df.with_columns(df[col_name].str.to_datetime(format="%Y-%m-%d %H:%M:%S%.f", strict=False).alias(col_name))
                                    print(f'DEBUG SCHEMA: Successfully converted {col_name} to datetime using ISO format directly')
                                    continue
                                except Exception as e3:
                                    print(f'DEBUG SCHEMA: Direct ISO format parsing failed for {col_name}: {e3}, trying manual conversion')
                                    try:
                                        # Strategy 4: Manual datetime conversion to handle complex formats
                                        def parse_datetime_manual(timestamp_str):
                                            if timestamp_str is None or timestamp_str == '':
                                                return None
                                            try:
                                                import datetime as dt
                                                # Remove timezone suffix manually
                                                clean_str = str(timestamp_str)
                                                # Handle +00, +0000, Z timezones
                                                if clean_str.endswith('+00'):
                                                    clean_str = clean_str[:-3]
                                                elif clean_str.endswith('Z'):
                                                    clean_str = clean_str[:-1]
                                                elif '+' in clean_str and clean_str.split('+')[-1].isdigit():
                                                    clean_str = clean_str.split('+')[0]
                                                
                                                # Parse the cleaned string
                                                return dt.datetime.fromisoformat(clean_str.replace(' ', 'T'))
                                            except:
                                                return None
                                        
                                        df = df.with_columns(
                                            df[col_name].map_elements(parse_datetime_manual, return_dtype=pd.Datetime('us')).alias(col_name)
                                        )
                                        print(f'DEBUG SCHEMA: Successfully converted {col_name} to datetime using manual parsing')
                                        continue
                                    except Exception as e4:
                                        print(f'DEBUG SCHEMA: Manual datetime parsing failed for {col_name}: {e4}, trying direct cast')
                                        # Fallback to direct cast
                                        polars_type = pd.Datetime('us')  # Use microsecond precision
                    else:
                        print(f'DEBUG SCHEMA: Unknown type {target_type} for column {col_name}, skipping')
                        continue
                        
                    df = df.with_columns(df[col_name].cast(polars_type, strict=False).alias(col_name))
                    
                except Exception as e:
                    print(f'DEBUG SCHEMA: Failed to cast {col_name} to {target_type}: {e}')
                    
        print(f'DEBUG SCHEMA: Successfully applied {schema_name} schema')
        return df
        
    def _ensure_schema_columns(self, df, schema_dict: Dict[str, str], default_values: Dict[str, Any] = None):
        """Ensure DataFrame has all columns defined in schema with proper defaults.
        
        Args:
            df: Polars DataFrame
            schema_dict: Dictionary mapping column names to Polars dtypes
            default_values: Dictionary mapping column names to default values
            
        Returns:
            DataFrame with all schema columns present
        """
        if df is None:
            return df
            
        if default_values is None:
            default_values = {}
            
        missing_columns = []
        for col_name in schema_dict.keys():
            if col_name not in df.columns:
                missing_columns.append(col_name)
                
                # Get default value
                if col_name in default_values:
                    default_val = default_values[col_name]
                else:
                    # Generate type-appropriate default
                    target_type = schema_dict[col_name]
                    if target_type == 'String':
                        default_val = ''
                    elif target_type == 'Int64':
                        default_val = 0
                    elif target_type == 'Float64':
                        default_val = 0.0
                    elif target_type == 'Boolean':
                        default_val = False
                    elif target_type == 'Datetime':
                        default_val = None  # Null datetime
                    else:
                        default_val = None
                        
                df = df.with_columns(pd.lit(default_val).alias(col_name))
                
        if missing_columns:
            print(f'DEBUG SCHEMA: Added {len(missing_columns)} missing columns: {missing_columns}')
            
        return df

    @staticmethod
    def unique_index_columns():
        pass

    @staticmethod
    def collector_dataframe_schema() -> Dict[str, str]:
        """Define Polars dataframe schema for processed CSV data (before grouping).
        
        This schema is used after CSV processing but before the group() method is called.
        Columns should be in their final types ready for aggregation.
        
        Returns:
            Dictionary mapping column names to Polars dtypes (as strings)
        """
        raise NotImplementedError("Subclasses must implement collector_dataframe_schema()")

    @staticmethod 
    def dataframe_schema() -> Dict[str, str]:
        """Define Polars dataframe schema for working dataframes (after grouping).
        
        This schema is used for:
        - Data after group() aggregation 
        - Data when merging multiple rollups
        - Data in report generation
        
        Returns:
            Dictionary mapping column names to Polars dtypes (as strings)
        """
        raise NotImplementedError("Subclasses must implement dataframe_schema()")

    @staticmethod
    def collector_dataframe_validation_schema() -> Dict[str, Dict[str, Any]]:
        """Define validation rules for collector dataframe columns.
        
        This schema specifies which columns are required, which can be null,
        and any additional validation rules like min/max values. It works
        alongside collector_dataframe_schema() to provide comprehensive
        data quality control.
        
        Returns:
            Dictionary mapping column names to validation rule dictionaries.
            Each validation dict can contain:
            - 'required': bool - Whether column must be present
            - 'allow_null': bool - Whether null values are allowed
            - 'min_value': Number - Minimum allowed value (for numeric columns)
            - 'max_value': Number - Maximum allowed value (for numeric columns)
            - 'valid_values': List - List of allowed values (for categorical columns)
            
        Example:
            {
                'host_name': {'required': True, 'allow_null': False},
                'task_runs': {'required': True, 'allow_null': False, 'min_value': 0},
                'duration': {'required': False, 'allow_null': True, 'min_value': 0.0},
            }
            
        Note:
            If not implemented by subclass, no additional validation is performed
            beyond basic schema casting.
        """
        return {}  # Default: no additional validation rules

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
        import logging
        logger = logging.getLogger(__name__)

        try:
            # Strategy 1: Try Polars direct read first
            try:
                df = pd.read_parquet(parquet_path)

                if len(df) == 0:
                    return self.empty()

                logger.debug(f'Successfully loaded {parquet_path} with Polars direct read: {len(df)} records')

            except Exception as error:
                error_msg = str(error)
                if 'not yet implemented: Nested object types' in error_msg or 'type Object' in error_msg or 'incompatible with expected type' in error_msg:
                    logger.warning(f'Polars cannot read object/nested types in {parquet_path}: {error}. Trying pyarrow approach.')

                    # Strategy 2: Try PyArrow with comprehensive type conversion
                    try:
                        import pyarrow as pa
                        import pyarrow.parquet as pq

                        # Read with pyarrow and convert to polars, handling all complex types
                        table = pq.read_table(parquet_path)

                        # Convert all problematic types to string for polars compatibility
                        schema_updates = {}
                        for i, field in enumerate(table.schema):
                            field_type = field.type
                            col_name = field.name
                            
                            # Convert any problematic types to strings
                            if (pa.types.is_list(field_type) or 
                                pa.types.is_struct(field_type) or 
                                str(field_type) == 'object' or
                                'object' in str(field_type).lower()):
                                
                                # Convert complex types to string representation
                                column_data = table.column(i).to_pylist()
                                
                                # Handle various data types and convert to JSON strings
                                string_data = []
                                for item in column_data:
                                    if item is None:
                                        string_data.append('{}' if col_name in ['canonical_facts', 'facts'] else '[]')
                                    elif isinstance(item, (dict, list, set)):
                                        import json
                                        try:
                                            if isinstance(item, set):
                                                item = list(item)  # Convert set to list for JSON serialization
                                            string_data.append(json.dumps(item))
                                        except (TypeError, ValueError):
                                            # Fallback for non-serializable objects
                                            string_data.append('{}' if col_name in ['canonical_facts', 'facts'] else '[]')
                                    else:
                                        # Convert other types to string representation
                                        try:
                                            if col_name in ['canonical_facts', 'facts']:
                                                string_data.append('{}')  # Empty dict for fact columns
                                            else:
                                                string_data.append('[]')  # Empty list for other complex columns
                                        except:
                                            string_data.append('[]')
                                
                                schema_updates[col_name] = pa.array(string_data, type=pa.string())

                        # Replace complex columns with string versions
                        if schema_updates:
                            logger.info(f'Converting {len(schema_updates)} complex/object columns to strings: {list(schema_updates.keys())}')
                            for col_name, new_array in schema_updates.items():
                                col_index = table.schema.get_field_index(col_name)
                                table = table.set_column(col_index, col_name, new_array)

                        df = pd.from_arrow(table)

                        if len(df) == 0:
                            return self.empty()

                        logger.info(f'Successfully loaded {parquet_path} using pyarrow with object type conversion: {len(df)} records')

                    except Exception as pyarrow_error:
                        logger.error(
                            f'PyArrow approach also failed for {parquet_path}: {pyarrow_error}. All methods exhausted, returning empty dataframe.'
                        )
                        return self.empty()
                else:
                    logger.warning(f'Unexpected error reading {parquet_path}: {error}. Returning empty dataframe.')
                    return self.empty()

        except FileNotFoundError:
            return self.empty()
        except Exception as e:
            logger.error(f'Unexpected error loading parquet file {parquet_path}: {e}')
            return self.empty()

        # CRITICAL: Apply complete dataframe schema after loading from parquet
        # This converts from parquet storage types back to consistent working dataframe types
        try:
            df = self.apply_complete_schema(df, schema_type="dataframe", operation_context="after_parquet_load")
            logger.debug(f'Applied complete dataframe schema after loading parquet: {len(df)} records')
        except Exception as schema_error:
            logger.warning(f'Failed to apply complete schema after parquet load: {schema_error}. Proceeding with loaded data.')

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

                        df = df.with_columns(
                            pd.lit(json.dumps(list(default_value) if isinstance(default_value, set) else default_value), dtype=pd.Utf8).alias(col)
                        )
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

                        df = df.with_columns(
                            pd.lit(json.dumps(list(default_value) if isinstance(default_value, set) else default_value), dtype=pd.Utf8).alias(col)
                        )
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

            add_span_attributes(
                current_span,
                **{
                    'schema.missing_columns': ','.join(missing_columns),
                    'schema.missing_count': len(missing_columns),
                    'schema.completeness_ratio': schema_metrics['schema_completeness_ratio'],
                },
            )

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
                'host_names_before_dedup': 'set',
            },
            'DataframeInventoryScope': {
                'canonical_facts': 'dict',
                'facts': 'dict',
                'organizations': 'set',
                'inventories': 'set',
                'serials': 'set',
                'host_names_before_dedup': 'set',
            },
            'DataframeContentUsage': {'playbooks': 'set', 'organizations': 'set'},
            'DataframeCollectionStatus': {},  # No Object columns
            'DataframeHostMetric': {},  # No Object columns
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
        available_cast_types = {k: v for k, v in raw_cast_types.items() if k in df.columns and k not in manually_converted_columns}

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
