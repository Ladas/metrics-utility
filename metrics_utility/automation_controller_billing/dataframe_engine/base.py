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
import logging
import time
from functools import reduce
from typing import Any, Dict, List, Optional, Union

import polars as pd
import pyarrow as pa

from dateutil.relativedelta import relativedelta
from opentelemetry import trace

from metrics_utility.tracing import add_span_attributes, traced_method

logger = logging.getLogger(__name__)


# ========================================
# CENTRALIZED AGGREGATION EXPRESSIONS
# ========================================
# Define all aggregation patterns in one place for consistency across dataframe engines

def get_aggregation_expressions() -> Dict[str, Any]:
    """Define all available aggregation expressions for Polars group_by().agg() operations.
    
    This centralized system ensures consistent aggregation behavior across all dataframe engines.
    Each dataframe engine can reference these by name in their initial_aggregations() method.
    
    Returns:
        Dictionary mapping aggregation names to Polars expressions
    """
    return {
        # ========================================
        # NUMERIC AGGREGATIONS
        # ========================================
        'sum': lambda col: pd.col(col).sum().alias(col),
        'count': lambda col: pd.col(col).count().alias(col),
        'max': lambda col: pd.col(col).max().alias(col),
        'min': lambda col: pd.col(col).min().alias(col),
        'first': lambda col: pd.col(col).first().alias(col),
        'last': lambda col: pd.col(col).last().alias(col),
        
        # ========================================
        # NULL-AWARE AGGREGATIONS
        # ========================================
        'first_non_null': lambda col: pd.col(col).filter(pd.col(col).is_not_null()).first().alias(col),
        'last_non_null': lambda col: pd.col(col).filter(pd.col(col).is_not_null()).last().alias(col),
        'max_non_null': lambda col: pd.col(col).filter(pd.col(col).is_not_null()).max().alias(col),
        'min_non_null': lambda col: pd.col(col).filter(pd.col(col).is_not_null()).min().alias(col),
        
        # ========================================
        # LIST/COLLECTION AGGREGATIONS
        # ========================================
        'unique': lambda col: pd.col(col).filter((pd.col(col).is_not_null()) & (pd.col(col) != '')).unique().alias(col),
        'unique_non_empty': lambda col: pd.col(col).filter((pd.col(col).is_not_null()) & (pd.col(col) != '')).unique().alias(col),
        'collect_list': lambda col: pd.col(col).filter(pd.col(col).is_not_null()).alias(col),
        'flatten_unique': lambda col: pd.col(col).flatten().unique().alias(col),
        'collect_list_safe': lambda col: pd.col(col).filter(pd.col(col).is_not_null()).alias(col),
        
        # ========================================
        # JSON AGGREGATIONS
        # ========================================
        'combine_json_values': lambda col: pd.col(col).filter(pd.col(col).is_not_null()).alias(f'{col}_list'),
        'collect_unique_as_json_set': lambda col: pd.col(col).filter((pd.col(col).is_not_null()) & (pd.col(col) != '')).unique().alias(col),
        
        # ========================================
        # SPECIAL AGGREGATIONS FOR HOST TRACKING
        # ========================================
        'original_host_names': lambda col: pd.col('original_host_name').unique().alias(col),
        'host_names_from_original': lambda col: pd.col('original_host_name').unique().alias(col),
        
        # ========================================
        # COMPLEX AGGREGATIONS
        # ========================================
        'merge_lists_unique': lambda col: (
            pd.col(col)
            .map_batches(
                lambda s: pd.Series([
                    sorted(list(set([
                        str(item)
                        for sublist in s.to_list()
                        if sublist is not None
                        for item in (sublist if isinstance(sublist, list) else [sublist])
                        if item is not None
                    ])))
                ]),
                return_dtype=pd.List(pd.Utf8),
            )
            .first()
            .alias(col)
        ),
        
        # ========================================
        # ADVANCED LIST OPERATIONS
        # ========================================
        'flatten_unique': lambda col: pd.col(col).flatten().unique().alias(col),
        
        # ========================================
        # SPECIALIZED AGGREGATIONS FOR FACTS AND COMPLEX DATA
        # ========================================
        'merge_json_facts': lambda col: (
            pd.col(col)
            .filter(pd.col(col).is_not_null())
            .map_batches(lambda s: pd.Series([merge_and_stringify_facts(s.to_list())]), return_dtype=pd.Utf8)
            .first()
            .alias(col)
        ),
    }


def build_aggregation_expressions(column_aggregations) -> List[Any]:
    """Build Polars aggregation expressions from aggregation configuration.
    
    Args:
        column_aggregations: Either:
            - Dict[str, str]: Simple mapping {column: aggregation} (legacy format)
            - Dict[str, Tuple[str, str]]: Aliased mapping {alias: (source_column, aggregation)}
        
    Returns:
        List of Polars expressions for use in group_by().agg()
        
    Examples:
        >>> # Legacy format (deprecated)
        >>> aggs = build_aggregation_expressions({
        ...     'task_runs': 'sum',
        ...     'host_runs': 'count'
        ... })
        
        >>> # New format with aliasing
        >>> aggs = build_aggregation_expressions({
        ...     'first_automation': ('created', 'min_non_null'),
        ...     'last_automation': ('created', 'max_non_null'),
        ...     'task_runs': ('task_runs', 'sum')
        ... })
        >>> dataframe.group_by(index_cols).agg(aggs)
    """
    available_aggs = get_aggregation_expressions()
    expressions = []
    
    for key, value in column_aggregations.items():
        if isinstance(value, tuple) and len(value) == 2:
            # New format: {alias: (source_column, aggregation)}
            source_column, agg_name = value
            alias = key
            
            if agg_name not in available_aggs:
                raise ValueError(f"Unknown aggregation '{agg_name}' for column '{source_column}' -> '{alias}'. Available: {list(available_aggs.keys())}")
            
            # Create expression with custom alias
            # For aliasing mode, we need to create expressions without the automatic alias
            agg_func = available_aggs[agg_name]
            if agg_name == 'min_non_null':
                expr = pd.col(source_column).filter(pd.col(source_column).is_not_null()).min().alias(alias)
            elif agg_name == 'max_non_null':
                expr = pd.col(source_column).filter(pd.col(source_column).is_not_null()).max().alias(alias)
            else:
                # For other aggregations, use the function but override the alias
                expr = agg_func(source_column)
                # Extract the expression without the alias and apply our alias
                # This is a bit hacky, but necessary for the current Polars API
                expr = expr.alias(alias)
            expressions.append(expr)
            
        elif isinstance(value, str):
            # Legacy format: {column: aggregation} (column name is both source and alias)
            column = key
            agg_name = value
            
            if agg_name not in available_aggs:
                raise ValueError(f"Unknown aggregation '{agg_name}' for column '{column}'. Available: {list(available_aggs.keys())}")
            
            expressions.append(available_aggs[agg_name](column))
        else:
            raise ValueError(f"Invalid aggregation format for '{key}': {value}. Expected string or (source_column, aggregation) tuple.")
    
    return expressions


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

    # Remove duplicates and sort for consistent ordering
    # Also remove keys with empty lists (fields that had no actual values)
    final_merged = {}
    for key in merged:
        filtered = [x for x in merged[key] if x is not None]
        unique_values = list(dict.fromkeys(filtered))
        if unique_values:  # Only keep keys that have actual values
            final_merged[key] = sorted(unique_values)

    return final_merged


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

    # Remove duplicates and sort for consistent ordering
    unique_values = list(dict.fromkeys([str(v) for v in all_values if v is not None]))
    return json.dumps(sorted(unique_values))


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


# ========================================
# STAGE 3: INITIAL AGGREGATION (GROUP METHOD) - EXTRACTED AGGREGATION FUNCTIONS
# ========================================
# These functions implement Stage 3 from docs/rollups_data_flow.md:
# "Group-by aggregation within single CSV batch using native Polars types"


def merge_json_lists_to_dict(json_list: List[str], column_name: str = 'unknown') -> str:
    """Stage 3: Convert list of JSON strings to merged dictionary with arrays as values.

    This implements the core aggregation logic for Stage 3 (Initial Aggregation/Group Method)
    where individual JSON objects from CSV records are combined into a single JSON object
    with arrays as values.

    **Stage 3 Data Flow Example:**
    Input Data (from CSV records):
        canonical_facts column contains:
        - Record 1: '{"os": "linux", "arch": "x86_64"}'
        - Record 2: '{"os": "ubuntu", "env": "prod"}'

    **Stage 3 Aggregation Result:**
        '{"os": ["linux", "ubuntu"], "arch": ["x86_64"], "env": ["prod"]}'

    Args:
        json_list: List of JSON strings from group aggregation (e.g., ['{"os": "linux"}', '{"os": "ubuntu"}'])
        column_name: Name of the column being processed (for debugging)

    Returns:
        JSON string with merged values as arrays: '{"os": ["linux", "ubuntu"]}'

    Note:
        This is used in dataframe_engine group() methods for 'combine_json_values' aggregations.
        Filters out null values and 'NA' values when combining to ensure clean data.
    """
    import json

    print(f'!!!!! merge_json_lists_to_dict called for {column_name} with {len(json_list) if json_list else 0} items !!!!!')

    if json_list is None or not json_list:
        return '{}'

    merged_dict = {}

    # Process each JSON string in the list
    for i, json_str in enumerate(json_list):
        if json_str is None or json_str == '':
            continue

        try:
            parsed = json.loads(json_str)
            if isinstance(parsed, dict):
                # Filter out null values and 'NA' values when combining
                clean_dict = {k: v for k, v in parsed.items() if v is not None and v != 'null' and v != 'NA'}

                if clean_dict:  # Only merge if there are valid values
                    merged_dict = combine_json_values(merged_dict, clean_dict)

        except (json.JSONDecodeError, TypeError) as e:
            print(f'DEBUG: {column_name} JSON decode error for item {i}: {e}')
            continue

    # Convert sets to sorted lists before JSON serialization
    for key in merged_dict:
        if isinstance(merged_dict[key], set):
            merged_dict[key] = sorted(list(merged_dict[key]))

    result = json.dumps(merged_dict)
    print(f'DEBUG: {column_name} Stage 3 result: {result}')
    return result


# ========================================
# STAGE 4: ROLLUP AGGREGATION (REGROUP METHOD) - EXTRACTED AGGREGATION FUNCTIONS
# ========================================
# These functions implement Stage 4 from docs/rollups_data_flow.md:
# "Cross-file rollup merging with native type operations for combining pre-aggregated data"


def merge_native_dicts(series: List[str], column_name: str = 'unknown') -> str:
    """Stage 4: Merge multiple dict JSON strings that already contain arrays as values.

    This implements Stage 4 (Rollup Aggregation/Regroup Method) where pre-aggregated
    JSON objects from different batches/files are merged. The input objects already
    contain arrays as values from Stage 3 processing.

    **Stage 4 Data Flow Example:**
    Input Data (from multiple Stage 3 results):
        canonical_facts column contains:
        - Batch 1: '{"os": ["linux", "ubuntu"], "arch": ["x86_64"]}'
        - Batch 2: '{"os": ["centos"], "env": ["prod", "dev"]}'

    **Stage 4 Aggregation Result:**
        '{"os": ["centos", "linux", "ubuntu"], "arch": ["x86_64"], "env": ["dev", "prod"]}'

    Args:
        series: List of JSON strings where each already contains arrays as values
        column_name: Name of the column being processed (for debugging)

    Returns:
        JSON string with merged and deduplicated arrays, sorted for consistency

    Note:
        This is used in dataframe_engine regroup() methods for combining rollups.
        Handles both list and non-list values, converting single values to arrays.
    """
    import json

    print(f'!!!!! merge_native_dicts called for {column_name} with {len(series) if series else 0} items !!!!!')

    if series is None or not series:
        return '{}'

    merged_dict = {}

    for i, json_str in enumerate(series):
        if json_str is None or json_str == '':
            continue

        try:
            parsed = json.loads(json_str)
            if isinstance(parsed, dict):
                for key, values in parsed.items():
                    if key not in merged_dict:
                        merged_dict[key] = []

                    # Handle both list and non-list values
                    if isinstance(values, list):
                        merged_dict[key].extend(values)
                    else:
                        merged_dict[key].append(values)

        except (json.JSONDecodeError, TypeError) as e:
            print(f'DEBUG: {column_name} JSON decode error for item {i}: {e}')
            continue

    # Remove duplicates and sort for consistency
    for key in merged_dict:
        filtered = [str(x) for x in merged_dict[key] if x is not None and x != 'null' and x != 'NA']
        merged_dict[key] = sorted(list(set(filtered)))

    result = json.dumps(merged_dict)
    print(f'DEBUG: {column_name} Stage 4 result: {result}')
    return result


def merge_native_lists(series: List[List[str]], column_name: str = 'unknown') -> List[str]:
    """Stage 4: Merge multiple List columns into single List with unique values.

    This implements Stage 4 (Rollup Aggregation/Regroup Method) for native List type
    columns like organizations, inventories, serials. Combines lists from multiple
    batches and removes duplicates.

    **Stage 4 Data Flow Example:**
    Input Data (from multiple batches):
        organizations column contains:
        - Batch 1: ["Default", "Test Org 1"]
        - Batch 2: ["Test Org 1", "Test Org 2"]

    **Stage 4 Aggregation Result:**
        ["Default", "Test Org 1", "Test Org 2"]

    Args:
        series: List of List values from different batches
        column_name: Name of the column being processed (for debugging)

    Returns:
        Merged list with unique values, sorted for consistency

    Note:
        This is used in dataframe_engine regroup() methods for List type columns.
        Handles both list and single value inputs gracefully.
    """
    print(f'!!!!! merge_native_lists called for {column_name} with {len(series) if series else 0} items !!!!!')

    if series is None or not series:
        return []

    all_values = []
    for lst in series:
        if lst is not None and isinstance(lst, list):
            all_values.extend(lst)
        elif lst is not None:
            all_values.append(lst)

    # Remove duplicates while preserving order, filter out nulls
    unique_values = list(set([str(v) for v in all_values if v is not None and v != 'null' and v != 'NA']))
    result = sorted(unique_values)

    print(f'DEBUG: {column_name} Stage 4 list result: {result}')
    return result


def validate_with_schema(
    df: pd.DataFrame, schema: pa.Schema, strict_columns: Optional[List[str]] = None, default_values: Optional[Dict[str, Any]] = None
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
                    df = df.with_columns(
                        df[col_name]
                        .cast(
                            pd.Int64,
                        )
                        .alias(col_name)
                    )
                elif pa.types.is_floating(expected_type):
                    df = df.with_columns(
                        df[col_name]
                        .cast(
                            pd.Float64,
                        )
                        .alias(col_name)
                    )
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
        except Exception as e:
            # If filtering fails, continue without filtering to avoid data loss
            logger.warning(f'Row filtering failed during schema validation: {e}. Continuing without filtering to avoid data loss.')

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


def convert_json_to_list_pairs(json_str):
    """Convert JSON string to list of key-value pairs for native List type.

    This is the standard conversion function for Stage 2 (CSV to Native Type Conversion).
    Transforms JSON dictionary strings into List format compatible with native Polars types.

    CRITICAL: All values are normalized to arrays for consistent merging operations.
    CRITICAL: Never returns [null] - always returns proper empty list [] for invalid input.

    Args:
        json_str: JSON string to convert

    Returns:
        List of [key, array_value] pairs for dictionary data, empty list for invalid input

    Example:
        >>> convert_json_to_list_pairs('{"fact1": "value1", "fact2": ["v1", "v2"]}')
        [["fact1", ["value1"]], ["fact2", ["v1", "v2"]]]
    """
    import json

    if json_str is None or json_str == '' or json_str == 'null':
        return []  # CRITICAL: Always return empty list, never [null]
    try:
        parsed = json.loads(json_str)
        if isinstance(parsed, dict):
            # Convert dict to list of [key, value] pairs with consistent array format
            result = []
            for key, value in parsed.items():
                # Skip null keys or values to prevent [null] creation
                if key is None or value is None:
                    continue
                # Ensure all values are arrays for consistent merging
                if isinstance(value, list):
                    # Filter out null elements from arrays
                    clean_value = [v for v in value if v is not None]
                    result.append([key, clean_value])
                else:
                    result.append([key, [value]])  # Wrap single values in arrays
            return result
        else:
            return []  # CRITICAL: Always return empty list, never [null]
    except (json.JSONDecodeError, TypeError):
        return []  # CRITICAL: Always return empty list, never [null]


def convert_list_pairs_to_json_string(list_pairs):
    """Convert List pairs format to JSON string format.

    Args:
        list_pairs: List in format [['key1', ['value1', 'value2']], ['key2', ['value3']]]

    Returns:
        JSON string representation of the data as a dictionary
    """
    if not list_pairs or not isinstance(list_pairs, list):
        return '{}'

    try:
        # Convert to dictionary first
        result_dict = convert_list_pairs_to_dict(list_pairs)
        # Convert to JSON string
        import json

        return json.dumps(result_dict)
    except Exception:
        return '{}'


def convert_list_pairs_to_dict(list_pairs):
    """Convert list of key-value pairs back to dictionary.

    This is the reverse operation for when dictionary access is needed.
    Handles the consistent array format where all values are arrays.

    CRITICAL: Never produces dictionaries with null values that could create [null] lists.

    Args:
        list_pairs: List of [key, array_value] pairs

    Returns:
        Dictionary reconstructed from pairs with arrays preserved

    Example:
        >>> convert_list_pairs_to_dict([["fact1", ["value1"]], ["fact2", ["v1", "v2"]]])
        {"fact1": ["value1"], "fact2": ["v1", "v2"]}
    """
    if not isinstance(list_pairs, list) or list_pairs is None:
        return {}
    result = {}
    for pair in list_pairs:
        if isinstance(pair, list) and len(pair) == 2:
            key, value = pair[0], pair[1]
            # Skip null keys to prevent issues
            if key is None:
                continue
            # Ensure value is never null - use empty list as fallback
            if value is None:
                value = []
            result[key] = value
    return result


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

        add_span_attributes(
            current_span, **{'dataframe.build.dataframe_type': self.__class__.__name__, 'dataframe.build.mode': 'standardized_schema_flow'}
        )

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

        add_span_attributes(
            current_span,
            **{
                'dataframe.build.duration_seconds': build_duration,
                'dataframe.build.groups_processed': groups_processed,
                'dataframe.build.total_input_records': total_records,
                'dataframe.build.final_record_count': final_count,
            },
        )

        # Step 4: Apply final schema validation and return result
        final_result = accumulated_dataframe if accumulated_dataframe is not None else self.empty()
        if final_result is not None and len(final_result) > 0:
            final_result = self.apply_dataframe_schema_complete(final_result)

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
        return self.apply_complete_schema(processed_data, schema_type='collector_dataframe', operation_context='after_csv_processing')

    def _process_batch_data(self, batch_data, current_span):
        """Process individual batch data - to be overridden by subclasses."""
        raise NotImplementedError('Subclasses must implement _process_batch_data(batch_data, current_span)')

    def _group_with_schema(self, dataframe):
        """Group dataframe and apply dataframe_schema (AFTER grouping)."""
        # Step 1: Perform grouping using subclass implementation
        grouped_data = self.group(dataframe)
        if grouped_data is None or len(grouped_data) == 0:
            return self.empty()

        # Step 2: Apply schema transformations for type changes during aggregation
        # TODO: Re-enable after fixing core serialization issue
        # grouped_data = self._apply_aggregation_schema_transformations(grouped_data)

        # Step 3: Apply dataframe schema (AFTER grouping)
        return self.apply_dataframe_schema_complete(grouped_data)

    @traced_method('dataframe.regroup_with_schema')
    def regroup_with_schema(self, dataframe):
        """Regroup pre-aggregated dataframe with schema consistency (used in merge operations)."""
        current_span = trace.get_current_span()
        start_time = time.time()
        input_count = len(dataframe) if dataframe is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.regroup.input_record_count': input_count,
                'dataframe.regroup.operation': 'standardized_regroup_with_schema',
            },
        )

        # Step 1: Perform regrouping using subclass implementation
        regrouped_data = self.regroup(dataframe)
        if regrouped_data is None or len(regrouped_data) == 0:
            return self.empty()

        # Step 2: Apply dataframe schema (AFTER regrouping)
        result = self.apply_dataframe_schema_complete(regrouped_data)

        duration = time.time() - start_time
        output_count = len(result) if result is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.regroup.duration_seconds': duration,
                'dataframe.regroup.output_record_count': output_count,
            },
        )

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

    def validate_collector_data(
        self, df: pd.DataFrame, strict_columns: Optional[List[str]] = None, default_values: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
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

    def validate_rollup_data(
        self, df: pd.DataFrame, strict_columns: Optional[List[str]] = None, default_values: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
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
            'filtered_by_empty': 0,
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

            # Check empty string constraints for string columns
            if not rules.get('allow_empty', True):
                # Only apply empty check for string columns
                if df[col_name].dtype == pd.String:
                    empty_mask = (df[col_name].is_not_null()) & (df[col_name] != '') & (df[col_name] != 'null')
                    invalid_count = len(df.filter(~empty_mask & valid_rows_mask))
                    validation_metrics['filtered_by_empty'] += invalid_count
                    valid_rows_mask = valid_rows_mask & empty_mask

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
        validation_metrics['quality_ratio'] = (
            validation_metrics['output_rows'] / validation_metrics['input_rows'] if validation_metrics['input_rows'] > 0 else 1.0
        )

        # Store validation metrics for observability
        self._add_validation_metrics(
            {
                'collector_dataframe_validation.input_rows': validation_metrics['input_rows'],
                'collector_dataframe_validation.output_rows': validation_metrics['output_rows'],
                'collector_dataframe_validation.total_filtered': validation_metrics['total_filtered'],
                'collector_dataframe_validation.quality_ratio': validation_metrics['quality_ratio'],
                'collector_dataframe_validation.filtered_by_required': validation_metrics['filtered_by_required'],
                'collector_dataframe_validation.filtered_by_null': validation_metrics['filtered_by_null'],
                'collector_dataframe_validation.filtered_by_range': validation_metrics['filtered_by_range'],
                'collector_dataframe_validation.filtered_by_values': validation_metrics['filtered_by_values'],
            }
        )

        # Log significant data quality issues
        if validation_metrics['quality_ratio'] < 0.9:  # More than 10% filtered
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(
                f'Significant data quality filtering in {self.__class__.__name__}: '
                f'{validation_metrics["total_filtered"]} rows filtered out of {validation_metrics["input_rows"]} '
                f'(quality ratio: {validation_metrics["quality_ratio"]:.2%})'
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
                'dataframe.cast.type_count': len(types) if types is not None else 0,
                'dataframe.cast.has_composite_index': len(self.unique_index_columns()) > 1,
            },
        )

        # Polars DataFrames don't have indexes, so no index manipulation is needed

        # Handle NA/NaN values before casting to avoid "Cannot convert non-finite values (NA or inf) to integer" error
        # Use Polars clone and casting operations
        result = df.clone()

        # Handle None types gracefully
        if types is None:
            types = {}

        # Polars type casting approach
        for col, col_type in types.items():
            if col in result.columns:
                if col_type is int or col_type == 'int' or str(col_type).startswith('int') or col_type == 'int64':
                    # For integer columns, fill NaN with 0 before casting - use consistent Int64 type
                    result = result.with_columns(result[col].fill_null(value=0).cast(pd.Int64).alias(col))
                elif col_type is float or col_type == 'float' or str(col_type).startswith('float'):
                    # For float columns, use strict casting to prevent invalid conversions
                    try:
                        # First check if we have string values that need parsing
                        if result[col].dtype in [pd.Utf8, pd.String]:
                            # Parse string values to float properly
                            result = result.with_columns(result[col].str.to_float().alias(col))
                        else:
                            # Direct cast for numeric types - use strict=True to prevent List(Null) issues
                            result = result.with_columns(result[col].cast(pd.Float64, strict=True).alias(col))
                    except Exception:
                        # If all casting fails, try to parse as string first then convert
                        try:
                            result = result.with_columns(result[col].cast(str).str.to_float().alias(col))
                        except Exception as e:
                            # Last resort: keep as original type
                            logger.warning(f'Failed to cast column {col} to float type: {e}. Keeping original type.')
                elif str(col_type) == 'datetime64[ns]':
                    # For datetime columns, convert to proper datetime to preserve precision
                    try:
                        result = result.with_columns(result[col].str.to_datetime().alias(col))
                    except Exception as e:
                        # Fallback to string if datetime conversion fails
                        logger.warning(f'Failed to convert column {col} to datetime: {e}. Converting to string instead.')
                        result = result.with_columns(result[col].cast(str).alias(col))
                else:
                    # For other types (str, object, etc.), use standard astype with special handling for List to String conversion
                    try:
                        # Special handling for canonical_facts and facts: convert List pairs to JSON strings
                        if col in ['canonical_facts', 'facts'] and col_type in [str, 'str', 'String', pd.Utf8]:
                            # Check if this is a List column that needs conversion to String
                            if result[col].dtype.base_type() == pd.List:
                                print(f'Converting {col} from List format to JSON string format')
                                # Convert List pairs to JSON strings using existing conversion function
                                result = result.with_columns(
                                    result[col]
                                    .map_elements(lambda x: convert_list_pairs_to_json_string(x) if x is not None else '{}', return_dtype=pd.Utf8)
                                    .alias(col)
                                )
                            else:
                                # Standard string casting
                                result = result.with_columns(result[col].cast(col_type).alias(col))
                        else:
                            # Standard casting for other columns
                            result = result.with_columns(result[col].cast(col_type).alias(col))
                    except Exception as e:
                        print(f'Failed to cast {col} to {col_type}: {e}. Continuing with original type.')
                        # Continue with original type if casting fails

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
        """Create an empty DataFrame with proper schema types.

        This ensures that empty DataFrames have the same schema as DataFrames with data,
        preventing type mismatches during merge operations.
        """
        columns = self.unique_index_columns() + self.data_columns()
        df = pd.DataFrame({col: [] for col in columns})

        # Apply the complete dataframe schema to ensure consistent types
        # This prevents List vs String type mismatches during merges
        return self.apply_dataframe_schema_complete(df)

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

        if rollup is None or len(rollup) == 0:
            add_span_attributes(
                current_span, **{'dataframe.merge.operation': 'return_new_group', 'dataframe.merge.duration_seconds': time.time() - start_time}
            )
            return new_group

        if new_group is None or len(new_group) == 0:
            add_span_attributes(
                current_span, **{'dataframe.merge.operation': 'return_rollup', 'dataframe.merge.duration_seconds': time.time() - start_time}
            )
            return rollup

        # NEW APPROACH: Use concat + regroup instead of complex join operations
        concat_start = time.time()

        # CRITICAL: Apply complete dataframe_schema to both DataFrames before merging
        # This ensures consistent Polars types (especially Datetime) following rollups_data_flow.md
        try:
            rollup = self.apply_dataframe_schema_complete(rollup)
            new_group = self.apply_dataframe_schema_complete(new_group)
        except Exception as schema_error:
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f'Complete schema application failed during merge: {schema_error}. Proceeding with basic alignment.')

            # Fallback to basic schema completion
            rollup = self._ensure_complete_schema(rollup)
            new_group = self._ensure_complete_schema(new_group)

        # CRITICAL: Ensure schema compatibility before concat to prevent type mismatch errors
        rollup_aligned, new_group_aligned = self._align_schemas_for_concat(rollup, new_group)
        
        # DEBUG: Check for duplicate columns before concat
        rollup_cols = rollup_aligned.columns
        new_group_cols = new_group_aligned.columns
        rollup_duplicates = [col for col in set(rollup_cols) if rollup_cols.count(col) > 1]
        new_group_duplicates = [col for col in set(new_group_cols) if new_group_cols.count(col) > 1]
        
        if rollup_duplicates or new_group_duplicates:
            print(f"ROLLUP DEBUG: Found duplicate columns before concat!")
            print(f"  Rollup duplicates: {rollup_duplicates}")
            print(f"  New group duplicates: {new_group_duplicates}")
            print(f"  Rollup columns: {rollup_cols}")
            print(f"  New group columns: {new_group_cols}")

        # Perform the concat operation - much simpler and more reliable than join
        import polars as pl

        concatenated = pl.concat([rollup_aligned, new_group_aligned], how='vertical')

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
            result = self.regroup_with_schema(concatenated)
        else:
            result = concatenated

        regroup_duration = time.time() - regroup_start

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
    def dedup(self, dataframe, hostname_mapping=None, scope_dataframe=None):
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
            # Apply hostname mapping using Polars syntax
            import polars as pd

            df = df.with_columns(df['host_name'].map_elements(lambda x: hostname_mapping.get(x, x), return_dtype=pd.Utf8).alias('host_name'))

            # CRITICAL: Do NOT apply hostname mapping to original_host_name
            # The original_host_name should preserve the actual original hostname value
            # Only host_name gets mapped to the canonical value for deduplication
        map_duration = time.time() - map_start

        # Only regroup if hostname mapping actually created duplicates
        regroup_start = time.time()
        unique_index_cols = self.unique_index_columns()
        if len(unique_index_cols) > 0:
            # Check for actual duplicates based on unique index columns using Polars
            subset_df = df.select(unique_index_cols)
            has_duplicates = subset_df.is_duplicated().any()

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
        # Find all _right columns
        right_columns = [col for col in df.columns if col.endswith('_right')]

        if right_columns:
            # For each _right column, decide what to do
            columns_to_drop = []
            columns_to_rename = {}

            for right_col in right_columns:
                base_col = right_col[:-6]  # Remove '_right'

                if base_col in df.columns:
                    # Base column exists, drop the _right version
                    columns_to_drop.append(right_col)
                else:
                    # No base column, rename _right to base
                    columns_to_rename[right_col] = base_col

            # Apply the changes
            if columns_to_drop:
                columns_to_keep = [col for col in df.columns if col not in columns_to_drop]
                df = df.select(columns_to_keep)

            if columns_to_rename:
                df = df.rename(columns_to_rename)

        return df

    def _align_schemas_for_join(self, rollup, new_group):
        """Ensure both DataFrames have compatible schemas for join operations."""
        # Get all unique columns from both DataFrames
        all_columns = sorted(set(rollup.columns) | set(new_group.columns))

        # Add missing columns to both DataFrames with compatible types
        rollup_aligned = self._add_missing_columns(rollup, all_columns, 'rollup')
        new_group_aligned = self._add_missing_columns(new_group, all_columns, 'new_group')

        # Ensure compatible types for matching columns
        rollup_aligned, new_group_aligned = self._ensure_compatible_types(rollup_aligned, new_group_aligned, all_columns)

        # Ensure column order matches
        rollup_aligned = rollup_aligned.select(all_columns)
        new_group_aligned = new_group_aligned.select(all_columns)

        return rollup_aligned, new_group_aligned

    def _align_schemas_for_concat(self, rollup, new_group):
        """Ensure both DataFrames have compatible schemas for concat operations."""
        # Get all unique columns from both DataFrames
        all_columns = sorted(set(rollup.columns) | set(new_group.columns))

        # Add missing columns to both DataFrames with compatible types
        rollup_aligned = self._add_missing_columns(rollup, all_columns, 'rollup')
        new_group_aligned = self._add_missing_columns(new_group, all_columns, 'new_group')

        # Ensure compatible types for matching columns
        rollup_aligned, new_group_aligned = self._ensure_compatible_types(rollup_aligned, new_group_aligned, all_columns)

        # Ensure column order matches to prevent "more than one occurrence" errors during concat
        # This is critical for Polars concat operations to work correctly
        rollup_aligned = rollup_aligned.select(all_columns)
        new_group_aligned = new_group_aligned.select(all_columns)

        return rollup_aligned, new_group_aligned

    def _add_missing_columns(self, df, all_columns, name):
        """Add missing columns to a DataFrame with appropriate default values."""
        missing_columns = [col for col in all_columns if col not in df.columns]

        if missing_columns:
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
        # Get the target schema for this dataframe type
        target_schema = self.dataframe_schema()

        for col in all_columns:
            rollup_dtype = rollup[col].dtype
            new_group_dtype = new_group[col].dtype

            # If types don't match, convert both to the schema-defined type
            if rollup_dtype != new_group_dtype:
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
                            rollup = rollup.with_columns(rollup[col].str.to_datetime().alias(col))
                            new_group = new_group.with_columns(new_group[col].str.to_datetime().alias(col))
                        except Exception as e:
                            # Fallback to direct cast
                            target_type = pd.Datetime('us')  # Use microsecond precision
                            try:
                                rollup = rollup.with_columns(
                                    rollup[col]
                                    .cast(
                                        target_type,
                                    )
                                    .alias(col)
                                )
                                new_group = new_group.with_columns(
                                    new_group[col]
                                    .cast(
                                        target_type,
                                    )
                                    .alias(col)
                                )
                            except Exception as e2:
                                pass  # Continue with processing other columns
                        continue  # Skip the regular casting since we handled datetime specially
                    else:
                        target_type = pd.Utf8  # Default fallback

                    try:
                        rollup = rollup.with_columns(
                            rollup[col]
                            .cast(
                                target_type,
                            )
                            .alias(col)
                        )
                        new_group = new_group.with_columns(
                            new_group[col]
                            .cast(
                                target_type,
                            )
                            .alias(col)
                        )
                    except Exception as e:
                        # Fallback to string
                        try:
                            rollup = rollup.with_columns(
                                rollup[col]
                                .cast(
                                    pd.Utf8,
                                )
                                .alias(col)
                            )
                            new_group = new_group.with_columns(
                                new_group[col]
                                .cast(
                                    pd.Utf8,
                                )
                                .alias(col)
                            )
                        except Exception as fallback_e:
                            pass  # Continue with processing other columns
                else:
                    # Column not in schema, use original string fallback
                    try:
                        rollup = rollup.with_columns(
                            rollup[col]
                            .cast(
                                pd.Utf8,
                            )
                            .alias(col)
                        )
                        new_group = new_group.with_columns(
                            new_group[col]
                            .cast(
                                pd.Utf8,
                            )
                            .alias(col)
                        )
                    except Exception as e:
                        pass  # Continue with processing other columns

        return rollup, new_group

    def apply_complete_schema(self, df, schema_type: str = 'dataframe', operation_context: str = 'unknown'):
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

        # Step 1: Get the appropriate schema and default values
        if schema_type == 'collector_dataframe':
            schema_dict = self.collector_dataframe_schema() if hasattr(self, 'collector_dataframe_schema') else {}
            default_values = getattr(self, 'get_collector_default_values', lambda: {})()
        elif schema_type == 'dataframe':
            schema_dict = self.dataframe_schema() if hasattr(self, 'dataframe_schema') else {}
            default_values = getattr(self, 'get_rollup_default_values', lambda: {})()
        elif schema_type == 'parquet':
            # For parquet schema, we'll use PyArrow schema if available
            schema_dict = {}  # Handled separately in save operations
            default_values = getattr(self, 'get_rollup_default_values', lambda: {})()
        else:
            # Unknown schema type
            return df

        if not schema_dict:
            # No schema defined
            return df

        # Step 2: Ensure all schema columns are present with proper defaults
        df = self._ensure_schema_columns(df, schema_dict, default_values)

        # Step 2.5: For collector_dataframe schema, convert JSON strings to native types BEFORE casting
        if schema_type == 'collector_dataframe':
            df = self._convert_json_strings_to_native_types(df, schema_dict)

        # Step 3: Apply schema-based type casting
        df = self._apply_schema_casting(df, schema_dict, f'{schema_type}_schema_{operation_context}')

        # Step 4: Apply validation for collector_dataframe schemas
        if schema_type == 'collector_dataframe':
            df = self.validate_collector_dataframe(df)

        # Step 5: Ensure consistent column ordering
        df = self._ensure_consistent_column_ordering(df)

        return df

    def _apply_schema_casting(self, df, schema_dict: Dict[str, str], schema_name: str = 'unknown'):
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
                    elif target_type == 'List':
                        # Native List type for collections (organizations, inventories, etc.)
                        # CRITICAL: Ensure List columns never contain [null] - convert to empty lists []
                        polars_type = pd.List(pd.Utf8)
                        # Clean up [null] values to [] before casting
                        try:
                            # Use proper Polars expression instead of map_elements to avoid Series/scalar confusion
                            # First, convert any [null] lists to empty lists using native Polars operations

                            # Strategy 1: Use list.eval to handle [null] -> [] conversion
                            # This properly handles the nested list structure without map_elements confusion
                            # Safe list cleaning that handles all edge cases
                            df = df.with_columns(
                                [
                                    pd.when(df[col_name].is_null())
                                    .then(pd.lit([], dtype=pd.List(pd.Utf8)))
                                    .when(df[col_name].list.len() == 0)
                                    .then(pd.lit([], dtype=pd.List(pd.Utf8)))
                                    .when(df[col_name].list.len() >= 1)
                                    .then(
                                        # Try to access first element safely
                                        pd.when(df[col_name].list.first().is_null()).then(pd.lit([], dtype=pd.List(pd.Utf8))).otherwise(df[col_name])
                                    )
                                    .otherwise(df[col_name])
                                    .alias(col_name)
                                ]
                            )

                            continue  # Skip the regular cast since we handled List type specially
                        except Exception as list_clean_error:
                            # If cleaning fails, try regular cast - the list should already be in proper format
                            import logging

                            logger = logging.getLogger(__name__)
                            logger.warning(f'List null cleaning failed for {col_name}: {list_clean_error}. Proceeding with regular cast.')
                    elif target_type == 'Struct':
                        # DEPRECATED: Struct types are no longer used - all JSON data is converted to List format
                        continue
                    elif target_type == 'Datetime':
                        # Special handling for datetime conversion from strings
                        try:
                            # Strategy 1: Try basic auto-parsing first
                            df_temp = df.with_columns(df[col_name].str.to_datetime(format=None).alias(col_name + '_temp'))
                            # Check if any values became null - this indicates partial failure
                            null_count = df_temp[col_name + '_temp'].null_count()
                            original_null_count = df[col_name].null_count()

                            if null_count == original_null_count:
                                # All conversions succeeded, no new nulls created
                                df = df_temp.drop(col_name).rename({col_name + '_temp': col_name})
                                continue  # Skip the regular cast since we handled datetime specially
                            else:
                                # Some conversions failed, try strategy 2
                                raise ValueError(f'Partial conversion failure: {null_count - original_null_count} new nulls created')
                        except Exception as e1:
                            try:
                                # Strategy 2: Strip timezone info and parse with explicit format
                                # Handle format like "2025-03-01 10:13:16.97527+00" by removing "+00"
                                df_cleaned = df.with_columns(
                                    df[col_name]
                                    .str.replace_all(r'\+\d{2}:\d{2}$', '')
                                    .str.replace_all(r'\+\d{2}$', '')
                                    .str.replace_all(r'Z$', '')
                                    .alias(col_name + '_clean')
                                )
                                # Use explicit format that we know works from testing
                                df = df_cleaned.with_columns(
                                    df_cleaned[col_name + '_clean']
                                    .str.to_datetime(
                                        format='%Y-%m-%d %H:%M:%S%.f',
                                    )
                                    .alias(col_name)
                                ).drop(col_name + '_clean')
                                continue
                            except Exception as e2:
                                try:
                                    # Strategy 3: Try basic ISO format pattern without timezone stripping
                                    df = df.with_columns(
                                        df[col_name]
                                        .str.to_datetime(
                                            format='%Y-%m-%d %H:%M:%S%.f',
                                        )
                                        .alias(col_name)
                                    )
                                    continue
                                except Exception as e3:
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
                                        continue
                                    except Exception as e4:
                                        # Fallback to direct cast
                                        polars_type = pd.Datetime('us')  # Use microsecond precision
                    else:
                        continue

                    # Use strict casting to prevent data quality issues like List(Null) to String conversion
                    # Only use  for datetime conversions that legitimately need it
                    if target_type == 'Datetime':
                        df = df.with_columns(
                            df[col_name]
                            .cast(
                                polars_type,
                            )
                            .alias(col_name)
                        )
                    else:
                        # Use strict=True for all other types to prevent invalid conversions
                        df = df.with_columns(df[col_name].cast(polars_type, strict=True).alias(col_name))

                except Exception as e:
                    # Failed to cast column - continue with original type
                    import logging

                    logger = logging.getLogger(__name__)
                    logger.warning(f'Failed to cast {col_name} to {target_type}: {e}. Continuing with original type.')

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
                    elif target_type == 'List':
                        default_val = []  # Empty list for collections
                    elif target_type == 'Struct':
                        default_val = {}  # Empty dict for facts
                    elif target_type == 'Datetime':
                        default_val = None  # Null datetime
                    else:
                        default_val = None

                # For List types, ensure we create proper empty lists, not [null]
                if target_type == 'List' and default_val == []:
                    # Create a column with proper empty List type
                    df = df.with_columns(pd.lit(None).cast(pd.List(pd.Utf8)).fill_null([]).alias(col_name))
                else:
                    df = df.with_columns(pd.lit(default_val).alias(col_name))

        if missing_columns:
            pass  # Schema columns added successfully

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
        raise NotImplementedError('Subclasses must implement collector_dataframe_schema()')

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
        raise NotImplementedError('Subclasses must implement dataframe_schema()')

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

    def serialize_dataframe_to_parquet_schema(self, df):
        """Convert dataframe from native dataframe_schema types to parquet-compatible types.

        This automatically handles the conversion from native Polars types (List, Struct, etc.)
        to parquet-compatible string representations based on PARQUET SCHEMA definitions.

        CRITICAL: Only converts columns that are actually JSON strings in parquet_schema.
        Columns that are native types in both dataframe_schema and parquet_schema are left unchanged.

        Args:
            df: DataFrame using dataframe_schema native types

        Returns:
            DataFrame with parquet-compatible types (complex types as JSON strings only where needed)
        """
        if df is None or len(df) == 0:
            return df

        dataframe_schema = self.dataframe_schema() if hasattr(self, 'dataframe_schema') else {}
        parquet_schema = self.parquet_schema() if hasattr(self, 'parquet_schema') else None

        if not dataframe_schema:
            return df

        # Create a copy for parquet serialization
        df_parquet = df.clone()

        # Only serialize columns that need conversion based on parquet_schema
        if parquet_schema:
            # Build a map of column names to parquet field types
            parquet_field_types = {}
            for field in parquet_schema:
                field_name = field.name
                if field.type == pa.string():
                    parquet_field_types[field_name] = 'string'
                elif field.type == pa.int64():
                    parquet_field_types[field_name] = 'int64'
                elif field.type == pa.float64():
                    parquet_field_types[field_name] = 'float64'
                elif pa.types.is_timestamp(field.type):
                    parquet_field_types[field_name] = 'timestamp'
                else:
                    parquet_field_types[field_name] = 'string'  # Default to string for unknown types

            # Convert each column only if dataframe_schema says it's complex but parquet_schema says it's a string
            for col_name, dataframe_type in dataframe_schema.items():
                if col_name in df_parquet.columns and col_name in parquet_field_types:
                    parquet_type = parquet_field_types[col_name]

                    # Only serialize if: dataframe_schema has List/Struct but parquet_schema has string
                    if dataframe_type == 'List' and parquet_type == 'string':
                        # Convert native List to JSON string
                        def serialize_list(x):
                            if x is None:
                                return '[]'
                            elif isinstance(x, list):
                                # Clean out any null values and serialize
                                clean_list = [item for item in x if item is not None]
                                import json

                                return json.dumps(clean_list)
                            elif isinstance(x, str):
                                # Already serialized, verify it's valid JSON
                                try:
                                    import json

                                    parsed = json.loads(x)
                                    return json.dumps(parsed if isinstance(parsed, list) else [])
                                except:
                                    return '[]'
                            else:
                                import json

                                return json.dumps([])

                        df_parquet = df_parquet.with_columns(df_parquet[col_name].map_elements(serialize_list, return_dtype=pd.Utf8).alias(col_name))
                    elif dataframe_type == 'Struct' and parquet_type == 'string':
                        # Convert native Struct/dict to JSON string
                        def serialize_struct(x):
                            if x is None:
                                return '{}'
                            elif isinstance(x, dict):
                                import json

                                return json.dumps(x)
                            elif isinstance(x, str):
                                # Already serialized, verify it's valid JSON
                                try:
                                    import json

                                    parsed = json.loads(x)
                                    return json.dumps(parsed if isinstance(parsed, dict) else {})
                                except:
                                    return '{}'
                            else:
                                return '{}'

                        df_parquet = df_parquet.with_columns(
                            df_parquet[col_name].map_elements(serialize_struct, return_dtype=pd.Utf8).alias(col_name)
                        )
                    # For other type combinations, leave unchanged (they're already compatible)
        else:
            # Fallback to old behavior if no parquet_schema defined
            for col_name, schema_type in dataframe_schema.items():
                if col_name in df_parquet.columns:
                    if schema_type == 'List':
                        # Convert native List to JSON string
                        def serialize_list(x):
                            if x is None:
                                return '[]'
                            elif isinstance(x, list):
                                # Clean out any null values and serialize
                                clean_list = [item for item in x if item is not None]
                                import json

                                return json.dumps(clean_list)
                            elif isinstance(x, str):
                                # Already serialized, verify it's valid JSON
                                try:
                                    import json

                                    parsed = json.loads(x)
                                    return json.dumps(parsed if isinstance(parsed, list) else [])
                                except:
                                    return '[]'
                            else:
                                import json

                                return json.dumps([])

                        df_parquet = df_parquet.with_columns(df_parquet[col_name].map_elements(serialize_list, return_dtype=pd.Utf8).alias(col_name))
                    elif schema_type == 'Struct':
                        # Convert native Struct/dict to JSON string
                        def serialize_struct(x):
                            if x is None:
                                return '{}'
                            elif isinstance(x, dict):
                                import json

                                return json.dumps(x)
                            elif isinstance(x, str):
                                # Already serialized, verify it's valid JSON
                                try:
                                    import json

                                    parsed = json.loads(x)
                                    return json.dumps(parsed if isinstance(parsed, dict) else {})
                                except:
                                    return '{}'
                            else:
                                return '{}'

                        df_parquet = df_parquet.with_columns(
                            df_parquet[col_name].map_elements(serialize_struct, return_dtype=pd.Utf8).alias(col_name)
                        )

        return df_parquet

    def apply_dataframe_schema_complete(self, df):
        """Apply complete dataframe_schema with all transformations.

        This is the CENTRAL method for all schema operations. It reuses existing
        schema helper methods to avoid code duplication:
        1. First deserializes parquet strings back to native types
        2. Then applies existing schema completion and ordering methods

        Args:
            df: DataFrame to apply complete schema to

        Returns:
            DataFrame with complete native dataframe_schema applied
        """
        if df is None or len(df) == 0:
            return df

        # Step 1: Deserialize any JSON strings back to native types for List/Struct columns
        df = self._deserialize_json_strings_to_native_types(df)

        # Step 2: Apply existing schema completion (adds missing columns with defaults)
        df = self._ensure_complete_schema(df)

        # Step 3: Apply consistent type casting based on dataframe_schema
        df = self._apply_schema_casting(df, self.dataframe_schema(), 'complete_dataframe_schema')

        # Step 4: Ensure consistent column ordering
        df = self._ensure_consistent_column_ordering(df)

        return df

    def _convert_json_strings_to_native_types(self, df, schema_dict: Dict[str, str]):
        """Convert JSON strings to native types based on collector_dataframe_schema.

        This handles Stage 2 conversion (JSON strings from CSV → native Polars types).
        Should only be called during collector_dataframe schema application.

        Args:
            df: DataFrame with JSON string columns from CSV processing
            schema_dict: collector_dataframe_schema() mapping column names to target types

        Returns:
            DataFrame with JSON strings converted to native List types where specified
        """
        if df is None or len(df) == 0:
            return df

        # Only convert columns that are currently strings but should be List in the schema
        for col_name, target_type in schema_dict.items():
            if col_name in df.columns and target_type == 'List':
                current_dtype = str(df[col_name].dtype)

                # Only convert if it's currently a string (JSON) but should be native List
                if current_dtype in ['Utf8', 'String']:

                    def convert_json_to_list(x):
                        """Convert JSON string to native List format."""
                        if x is None or x == '' or x == 'null':
                            return []
                        return convert_json_to_list_pairs(x)

                    try:
                        df = df.with_columns(df[col_name].map_elements(convert_json_to_list, return_dtype=pd.List(pd.List(pd.Utf8))).alias(col_name))
                    except Exception as e:
                        import logging

                        logger = logging.getLogger(__name__)
                        logger.warning(f'Failed to convert {col_name} from JSON string to List: {e}. Keeping as string.')

        return df

    def _deserialize_json_strings_to_native_types(self, df):
        """Convert JSON strings back to native Polars types based on dataframe_schema.

        This handles the reverse of serialization - converting stored JSON strings
        back to native List and Struct types for processing.
        """
        if df is None or len(df) == 0:
            return df

        dataframe_schema = self.dataframe_schema() if hasattr(self, 'dataframe_schema') else {}
        if not dataframe_schema:
            return df

        # Only convert columns that should be native types but are currently strings
        for col_name, schema_type in dataframe_schema.items():
            if col_name in df.columns and schema_type in ['List', 'Struct']:
                current_dtype = str(df[col_name].dtype)

                # Only convert if it's currently a string (from parquet) but should be native type
                if current_dtype in ['Utf8', 'String'] and schema_type == 'List':

                    def deserialize_list(x):
                        if x is None or x == '':
                            return []
                        try:
                            import json

                            parsed = json.loads(x) if isinstance(x, str) else x
                            return parsed if isinstance(parsed, list) else []
                        except:
                            return []

                    df = df.with_columns(
                        df[col_name]
                        .map_elements(
                            deserialize_list,
                            return_dtype=pd.List(pd.Utf8),
                        )
                        .alias(col_name)
                    )
                elif current_dtype in ['Utf8', 'String'] and schema_type == 'Struct':
                    # For Struct types, keep as JSON string for now since Polars Struct requires fixed schema
                    pass

        return df

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
            # Read parquet file directly - all complex types are stored as JSON strings
            df = pd.read_parquet(parquet_path)

            if len(df) == 0:
                return self.empty()

            logger.debug(f'Successfully loaded {parquet_path}: {len(df)} records')

            # CRITICAL: Apply complete dataframe schema (handles all transformations)
            df = self.apply_dataframe_schema_complete(df)
            logger.debug(f'Applied complete dataframe schema: {len(df)} records')

        except FileNotFoundError:
            return self.empty()
        except Exception as e:
            logger.error(f'Error loading parquet file {parquet_path}: {e}')
            return self.empty()

        return df

    def _apply_aggregation_schema_transformations(self, df):
        """Apply automatic schema transformations during aggregation based on schema differences.

        This method automatically converts between collector_dataframe_schema and dataframe_schema
        types when they differ, handling common patterns like String -> List conversions.
        """
        if df is None or len(df) == 0:
            return df

        # Get both schemas to compare
        collector_schema = self.collector_dataframe_schema() if hasattr(self, 'collector_dataframe_schema') else {}
        dataframe_schema = self.dataframe_schema() if hasattr(self, 'dataframe_schema') else {}

        if not collector_schema or not dataframe_schema:
            return df

        # Find columns that need type transformation
        transformations = []
        for col_name in df.columns:
            if col_name in collector_schema and col_name in dataframe_schema:
                from_type = collector_schema[col_name]
                to_type = dataframe_schema[col_name]

                # Handle String -> List transformation (common during aggregation)
                if from_type == 'String' and to_type == 'List':
                    transformations.append((col_name, 'string_to_list'))

        # Apply transformations
        for col_name, transform_type in transformations:
            if transform_type == 'string_to_list':
                # Convert String values to single-item Lists
                # This handles the common case where CSV string values become List collections
                df = df.with_columns(
                    [
                        pd.when(df[col_name].is_not_null() & (df[col_name] != ''))
                        .then(df[col_name].str.split(','))  # Handle comma-separated values
                        .otherwise(pd.lit([], dtype=pd.List(pd.Utf8)))
                        .alias(col_name)
                    ]
                )

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
                except Exception as e:
                    logger.warning(f'Failed to add fallback empty list column {col}: {e}. Skipping this column.')
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

        # Add missing columns with default values using native types from dataframe_schema
        for col in expected_columns:
            if col not in df.columns:
                # Get default value from subclass if available
                try:
                    if hasattr(self, 'get_rollup_default_values'):
                        default_values = self.get_rollup_default_values()
                        if col in default_values:
                            default_value = default_values[col]
                            df = df.with_columns(pd.lit(default_value).alias(col))
                            continue
                except Exception:
                    pass

                # NO FALLBACK DEFAULTS - All dataframe wrappers must define complete schemas
                # This prevents String vs native type mismatches during merging
                raise ValueError(
                    f"Column '{col}' missing from dataframe and not defined in get_rollup_default_values() "
                    f'for {self.__class__.__name__}. All dataframe wrappers must define complete schemas '
                    f'with consistent native types (List, Struct, etc.) to prevent merge type conflicts.'
                )

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
