######################################
# Code for building the spreadsheet
######################################
import json

import polars as pd

from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows

from metrics_utility.metric_utils import INDIRECT


class Base:
    BLACK_COLOR_HEX = '00000000'
    WHITE_COLOR_HEX = '00FFFFFF'
    BLUE_COLOR_HEX = '000000FF'
    RED_COLOR_HEX = 'FF0000'
    LIGHT_BLUE_COLOR_HEX = 'd4eaf3'
    GREEN_COLOR_HEX = '92d050'
    YELLOW_COLOR_HEX = 'ffcc17'

    FONT = 'Arial'
    PRICE_FORMAT = '$#,##0.00'
    HOST_NAME = 'Host name'
    JOB_RUNS = 'Job runs'
    NUM_OF_TASKS_OR_RUNS = 'Number of task\nruns'
    HOST_RUNS_UNIQUE = 'Unique managed nodes\nautomated'
    HOST_RUNS = 'Non-unique managed\nnodes automated'
    DURATION = 'Duration of task\nruns [seconds]'

    # Deduplication column labels
    DEDUP_COLUMN_LABELS = {
        'host_names_before_dedup': 'Host names\nbefore\ndeduplication',
        'host_names_before_dedup_count': 'Host names\nbefore\ndeduplication\ncount',
    }

    def optional_report_sheets(self):
        return self.extra_params.get('optional_sheets')

    def has_dedup_enabled(self):
        """Check if experimental deduplication is enabled."""
        return self.extra_params.get('deduplicator') == 'ccsp-experimental'

    def calculate_dedup_count(self, series):
        """Calculate count for dedup column more efficiently."""
        # Handle both pandas and Polars Series
        if hasattr(series, 'map_elements'):  # Polars Series
            import polars as pd
            import json

            # Check if it's a List type column - use direct list.len() for efficiency
            if hasattr(series, 'list') and hasattr(series.list, 'len'):
                try:
                    return series.list.len().cast(pd.Int64)
                except:
                    pass  # Fall back to map_elements if list.len() fails

            def count_items(x):
                """Count items in collection, handling JSON strings, sets, and lists."""
                # When map_elements is used on List columns, x is passed as a Series
                if hasattr(x, 'to_list'):
                    x_list = x.to_list()
                    return len(x_list)
                elif isinstance(x, str):
                    try:
                        parsed = json.loads(x)
                        return len(parsed) if isinstance(parsed, (list, set)) else 1
                    except (json.JSONDecodeError, TypeError):
                        return 1
                elif isinstance(x, (set, list)):
                    return len(x)
                else:
                    return 1

            return series.map_elements(count_items, return_dtype=pd.Int64)
        else:  # pandas Series fallback
            return series.map(lambda x: len(x) if isinstance(x, (set, list)) else 1)

    def add_dedup_count_column(self, dataframe, base_col_name, count_col_name):
        """Add deduplication count column if base column exists."""
        if base_col_name in dataframe.columns:
            dataframe = dataframe.with_columns(self.calculate_dedup_count(dataframe[base_col_name]).alias(count_col_name))
        return dataframe

    def handle_dedup_columns_for_scope(self, dataframe, columns, convert_cols):
        """Handle deduplication columns for inventory scope if experimental dedup is enabled."""
        if self.has_dedup_enabled() and 'host_names_before_dedup' in dataframe.columns:
            convert_cols.append('host_names_before_dedup')
            dataframe = self.add_dedup_count_column(dataframe, 'host_names_before_dedup', 'host_names_before_dedup_count')
            columns += ['host_names_before_dedup', 'host_names_before_dedup_count']
        return dataframe, columns, convert_cols


    def handle_dedup_columns_for_usage(self, dataframe, columns, convert_cols):
        """Handle deduplication columns for usage tables if experimental dedup is enabled."""
        if self.has_dedup_enabled():
            dataframe = self.add_dedup_count_column(dataframe, 'host_names_before_dedup', 'host_names_before_dedup_count')
            columns += ['host_names_before_dedup', 'host_names_before_dedup_count']
            if 'host_names_before_dedup' in dataframe.columns:
                convert_cols.append('host_names_before_dedup')
        return dataframe, columns, convert_cols

    def add_dedup_labels_if_needed(self, labels, column_names):
        """Add deduplication labels for specified columns if experimental dedup is enabled."""
        if self.has_dedup_enabled():
            labels.update({k: v for k, v in self.DEDUP_COLUMN_LABELS.items() if k in column_names})
        return labels

    def convert_cell(self, cell):
        # DEBUG: Check for web01 data corruption - handle Series safely
        has_web01 = False
        try:
            has_web01 = 'web01' in str(cell)
        except:
            pass
            
        if has_web01:
            pass # Debug disabled
            # print(f'!!!!! CONVERT_CELL DEBUG: Processing cell with web01 data !!!!!')
            # print(f'  Cell type: {type(cell)}')
            # print(f'  Cell value: {cell}')
        
        # If the cell is a Polars Series (from List column), convert to Python list first
        if hasattr(cell, 'to_list'):  # Polars Series object
            cell = cell.to_list()
            if has_web01:
                pass # print(f'  After to_list(): {cell}')

        # Handle canonical_facts and facts - they should already be JSON strings at this point

        # If the cell is a dictionary, convert each set value to a sorted list, then dump as a JSON string.
        if isinstance(cell, dict):
            if has_web01:
                pass # print(f'  Processing as dict: {cell}')
            # Convert sets to lists and filter out keys with empty arrays
            new_cell = {}
            for k, v in cell.items():
                if isinstance(v, set):
                    processed_value = sorted(list(v))
                else:
                    processed_value = v
                
                # Only include keys that have non-empty values
                # This filters out keys like 'ansible_machine_id': [] or 'ansible_product_serial': []
                if processed_value not in [[], None, ""]:
                    new_cell[k] = processed_value
            result = json.dumps(new_cell)
            if has_web01:
                pass # print(f'  Dict result: {result}')
            return result
        # If the cell itself is a set, convert it to a sorted list and then to a JSON string.
        elif isinstance(cell, set):
            if has_web01:
                print(f'  Processing as set: {cell}')
            result = json.dumps(sorted(list(cell)))
            if has_web01:
                print(f'  Set result: {result}')
            return result
        # If the cell is a list, convert any set elements inside to sorted lists and dump as a JSON string.
        elif isinstance(cell, list):
            if has_web01:
                print(f'  Processing as list: {cell}')
            new_cell = [sorted(list(item)) if isinstance(item, set) else item for item in cell]
            # Sort the list itself if it contains strings
            if new_cell and all(isinstance(item, str) for item in new_cell):
                new_cell = sorted(new_cell)
            result = json.dumps(new_cell)
            if has_web01:
                print(f'  List result: {result}')
            return result
        # If the cell is a string that looks like JSON, try to parse and filter it
        elif isinstance(cell, str) and cell.strip().startswith('{') and cell.strip().endswith('}'):
            if has_web01:
                print(f'  Processing as JSON string: {cell}')
            try:
                parsed_dict = json.loads(cell)
                if isinstance(parsed_dict, dict):
                    # Apply the same filtering logic as dictionary processing
                    filtered_dict = {}
                    for k, v in parsed_dict.items():
                        if isinstance(v, set):
                            processed_value = sorted(list(v))
                        else:
                            processed_value = v
                        
                        # Only include keys that have non-empty values
                        if processed_value not in [[], None, ""]:
                            filtered_dict[k] = processed_value
                    
                    result = json.dumps(filtered_dict)
                    if has_web01:
                        print(f'  JSON string result: {result}')
                    return result
                else:
                    # Not a dictionary, return unchanged
                    return cell
            except (json.JSONDecodeError, TypeError, ValueError):
                # Not valid JSON, return unchanged
                if has_web01:
                    print(f'  Invalid JSON, returning unchanged: {cell}')
                return cell
        # Otherwise, return the cell unchanged.
        if has_web01:
            print(f'  Returning unchanged: {cell}')
        return cell

    def rename_dataframe(self, dataframe, columns_mapping):
        """Rename DataFrame columns for Polars."""
        # Polars rename expects a dict mapping old_name -> new_name
        return dataframe.rename(columns_mapping)

    def to_pandas_for_excel(self, dataframe):
        """Convert Polars DataFrame to pandas for use with openpyxl."""
        # Convert Polars to pandas for Excel export
        pandas_df = dataframe.to_pandas()

        # Strip timezone information from datetime columns for Excel compatibility
        for col in pandas_df.columns:
            if pandas_df[col].dtype.name.startswith('datetime'):
                if hasattr(pandas_df[col].dtype, 'tz') and pandas_df[col].dtype.tz is not None:
                    # Convert timezone-aware datetime to naive datetime
                    pandas_df[col] = pandas_df[col].dt.tz_convert(None)
                # Also handle individual datetime objects that might have tzinfo
                elif pandas_df[col].dtype == 'object':

                    def strip_timezone(x):
                        if hasattr(x, 'tzinfo') and x.tzinfo is not None:
                            return x.replace(tzinfo=None)
                        return x

                    pandas_df[col] = pandas_df[col].apply(strip_timezone)

        return pandas_df

    def reset_index_if_needed(self, dataframe):
        """Polars DataFrames don't have row indices, so this is a no-op."""
        # Polars DataFrame - no operation needed
        return dataframe

    def reindex_columns(self, dataframe, columns):
        """Reorder columns for Polars."""
        return dataframe.select(columns)

    def add_sheet(self, title, sheet_index, widths=None):
        self.wb.create_sheet(title=title)
        ws = self.wb.worksheets[sheet_index]
        if widths:
            self.set_widths(ws, widths)
        return ws

    def set_widths(self, ws, widths):
        for key, value in widths.items():
            ws.column_dimensions[get_column_letter(key)].width = value

    def _fix_event_host_names(self, mapping_dataframe, destination_dataframe):
        if destination_dataframe is None:
            return None

        # Check for empty dataframes
        if mapping_dataframe is None or len(mapping_dataframe) == 0:
            return destination_dataframe

        if len(destination_dataframe) == 0:
            return destination_dataframe

        # Use efficient Polars operations instead of slow map_rows
        # Create composite ID for mapping dataframe using Polars string operations
        mapping_dataframe = mapping_dataframe.with_columns(
            [
                (pd.col('original_host_name').cast(str) + '__' + pd.col('install_uuid').cast(str) + '__' + pd.col('job_remote_id').cast(str)).alias(
                    'host_composite_id'
                )
            ]
        )

        # Convert to dictionary for mapping using Polars
        mapping_rows = mapping_dataframe.select(['host_composite_id', 'host_name']).to_dicts()
        mapping_dict = {row['host_composite_id']: str(row['host_name']) for row in mapping_rows}

        # Create composite ID for destination dataframe using Polars operations
        destination_dataframe = destination_dataframe.with_columns(
            [
                (pd.col('host_name').cast(str) + '__' + pd.col('install_uuid').cast(str) + '__' + pd.col('job_remote_id').cast(str)).alias(
                    'host_composite_id_temp'
                )
            ]
        )
        # Apply mapping using efficient join operation instead of map_rows
        mapping_df = pd.DataFrame([{'host_composite_id_temp': k, 'mapped_host_name': v} for k, v in mapping_dict.items()])

        # Join to get mapped host names
        destination_dataframe = destination_dataframe.join(mapping_df, on='host_composite_id_temp', how='left')

        # Use mapped name if available, otherwise keep original
        destination_dataframe = destination_dataframe.with_columns(
            [pd.when(pd.col('mapped_host_name').is_not_null()).then(pd.col('mapped_host_name')).otherwise(pd.col('host_name')).alias('host_name')]
        )

        # Clean up temporary columns and create final composite ID
        destination_dataframe = destination_dataframe.drop(['host_composite_id_temp', 'mapped_host_name'])
        destination_dataframe = destination_dataframe.with_columns(
            [
                (pd.col('host_name').cast(str) + '__' + pd.col('install_uuid').cast(str) + '__' + pd.col('job_remote_id').cast(str)).alias(
                    'host_composite_id'
                )
            ]
        )

        return destination_dataframe

    def _build_data_section_scope(self, current_row, ws, dataframe, mode=None):
        header_font = Font(name=self.FONT, size=10, color=self.BLACK_COLOR_HEX, bold=True)
        value_font = Font(name=self.FONT, size=10, color=self.BLACK_COLOR_HEX)

        # Clone the Polars DataFrame
        ccsp_report_dataframe = dataframe.clone()

        # Convert arrays and dict fields into string, so they can be rendered into xlsx
        convert_cols = ['organizations', 'inventories', 'canonical_facts', 'facts']
        columns = [
            'host_name',
            'last_automation',
            'organizations',
            'inventories',
            'canonical_facts',
            'facts',
        ]

        # Handle deduplication columns if enabled
        ccsp_report_dataframe, columns, convert_cols = self.handle_dedup_columns_for_scope(ccsp_report_dataframe, columns, convert_cols)

        for col in convert_cols:
            if col in ccsp_report_dataframe.columns:
                ccsp_report_dataframe = ccsp_report_dataframe.with_columns(
                    ccsp_report_dataframe[col].map_elements(self.convert_cell, return_dtype=pd.String).alias(col)
                )

        # We're not showing cluster/install_uuid until we support multi-cluster view officially
        if 'install_uuid' in ccsp_report_dataframe.columns:
            ccsp_report_dataframe = ccsp_report_dataframe.drop('install_uuid')

        labels = {
            'host_name': self.HOST_NAME,
            'last_automation': 'Last\nAutomation',
            'organizations': 'Organizations',
            'inventories': 'Inventories',
            'canonical_facts': 'Canonical Facts',
            'facts': 'Facts',
        }

        # Add dedup labels if needed
        self.add_dedup_labels_if_needed(labels, ['host_names_before_dedup', 'host_names_before_dedup_count'])

        # Filter columns that exist
        columns = [col for col in columns if col in ccsp_report_dataframe.columns]
        ccsp_report_dataframe = ccsp_report_dataframe[columns]

        labels = {k: v for k, v in labels.items() if k in columns}
        ccsp_report_dataframe = self.rename_dataframe(ccsp_report_dataframe, labels)

        # Sort by host name to ensure consistent ordering for tests
        if self.HOST_NAME in ccsp_report_dataframe.columns:
            ccsp_report_dataframe = ccsp_report_dataframe.sort(self.HOST_NAME)

        row_counter = 0
        rows = dataframe_to_rows(self.to_pandas_for_excel(ccsp_report_dataframe), index=False)
        for r_idx, row in enumerate(rows, current_row):
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx)
                cell.value = value

                if row_counter == 0:
                    # set header style
                    cell.font = header_font
                    rd = ws.row_dimensions[r_idx]
                    rd.height = 25
                else:
                    # set value style
                    cell.font = value_font

            row_counter += 1

        return current_row + row_counter

    def _build_data_section_infrastructure_summary(self, current_row, ws, dataframe):
        header_font = Font(name=self.FONT, size=10, color=self.BLACK_COLOR_HEX, bold=True)
        value_font = Font(name=self.FONT, size=10, color=self.BLACK_COLOR_HEX)

        # Handle invalid dataframes gracefully (but allow empty dataframes with proper columns to proceed)
        if dataframe is None or 'managed_node_type' not in dataframe.columns:
            # If no valid dataframe structure, show empty message
            cell = ws.cell(row=current_row, column=1)
            cell.value = 'No infrastructure data available'
            cell.font = value_font
            return current_row + 1

        # Extract infrastructure facts from indirect nodes
        # Filter for indirect nodes
        indirect_filter = dataframe['managed_node_type'] == INDIRECT
        filtered_df = dataframe.filter(indirect_filter)

        # Clone the Polars DataFrame
        indirect_nodes = filtered_df.clone()

        if len(indirect_nodes) == 0:
            # If no indirect nodes, show empty message
            cell = ws.cell(row=current_row, column=1)
            cell.value = 'No indirect nodes found'
            cell.font = value_font
            return current_row + 1

        # Parse facts to extract infra_type, infra_bucket, and device_type
        def extract_infra_info(facts):
            def extract_value(value):
                if isinstance(value, set):
                    return list(value)[0] if value else 'Unknown'
                elif isinstance(value, list):
                    return value[0] if value else 'Unknown'
                elif isinstance(value, str):
                    return value
                else:
                    return 'Unknown'

            if isinstance(facts, dict):
                return {
                    'infra_type': extract_value(facts.get('infra_type', 'Unknown')),
                    'infra_bucket': extract_value(facts.get('infra_bucket', 'Unknown')),
                    'device_type': extract_value(facts.get('device_type', 'Unknown')),
                }
            elif isinstance(facts, str):
                import json

                try:
                    facts_dict = json.loads(facts)
                    return {
                        'infra_type': extract_value(facts_dict.get('infra_type', 'Unknown')),
                        'infra_bucket': extract_value(facts_dict.get('infra_bucket', 'Unknown')),
                        'device_type': extract_value(facts_dict.get('device_type', 'Unknown')),
                    }
                except Exception:
                    pass
            return {'infra_type': 'Unknown', 'infra_bucket': 'Unknown', 'device_type': 'Unknown'}

        # Extract infrastructure information
        infra_info = indirect_nodes['facts'].map_elements(extract_infra_info)
        indirect_nodes = indirect_nodes.with_columns(infra_info.map_elements(lambda x: x['infra_type']).alias('infra_type'))
        indirect_nodes = indirect_nodes.with_columns(infra_info.map_elements(lambda x: x['infra_bucket']).alias('infra_bucket'))
        indirect_nodes = indirect_nodes.with_columns(infra_info.map_elements(lambda x: x['device_type']).alias('device_type'))

        # Import centralized aggregation functions
        from metrics_utility.automation_controller_billing.dataframe_engine.base import get_aggregation_expressions
        agg_functions = get_aggregation_expressions()

        # Group by infra_type, infra_bucket, and device_type
        summary_df = indirect_nodes.group_by(['infra_type', 'infra_bucket', 'device_type']).agg([
            pd.col('host_name').n_unique().alias('indirect_hosts_unique'),
            pd.col('host_name').count().alias('indirect_hosts_total'),
        ])
        summary_df = self.reset_index_if_needed(summary_df)

        # Sort by infrastructure type, then bucket, then device type
        summary_df = summary_df.sort(['infra_type', 'infra_bucket', 'device_type'])

        # Add all column headers
        headers = ['Infrastructure', 'Device Category', 'Device Type', 'Unique Nodes', 'Total Nodes']
        for c_idx, header in enumerate(headers, 1):
            cell = ws.cell(row=current_row, column=c_idx)
            cell.value = header
            cell.font = header_font
            cell.alignment = Alignment(horizontal='left')

        ws.row_dimensions[current_row].height = 25
        current_row += 1

        # Create hierarchical display
        prev_infra_type = None
        prev_infra_bucket = None

        # Use Polars DataFrame iteration
        iterator = summary_df.iter_rows(named=True)

        for row in iterator:
            infra_type = row['infra_type']
            infra_bucket = row['infra_bucket']
            device_type = row['device_type']

            # Write infrastructure type header when it changes
            if infra_type != prev_infra_type:
                # Merge cells across three columns for infrastructure type
                ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=3)
                cell = ws.cell(row=current_row, column=1)
                cell.value = infra_type
                cell.font = header_font
                cell.alignment = Alignment(horizontal='left')
                ws.row_dimensions[current_row].height = 25
                current_row += 1
                prev_infra_type = infra_type
                prev_infra_bucket = None  # Reset bucket tracking

            # Write infrastructure bucket header when it changes
            if infra_bucket != prev_infra_bucket:
                cell = ws.cell(row=current_row, column=2)
                cell.value = infra_bucket
                cell.font = header_font
                cell.alignment = Alignment(horizontal='left')
                ws.row_dimensions[current_row].height = 25
                current_row += 1
                prev_infra_bucket = infra_bucket

            # Write device type row
            cell = ws.cell(row=current_row, column=3)
            cell.value = device_type
            cell.font = value_font

            cell = ws.cell(row=current_row, column=4)
            cell.value = row['indirect_hosts_unique']
            cell.font = value_font

            cell = ws.cell(row=current_row, column=5)
            cell.value = row['indirect_hosts_total']
            cell.font = value_font

            current_row += 1

        return current_row

    def _build_data_section_usage_by_node(self, current_row, ws, dataframe, mode=None, managed_node_type=None):
        header_font = Font(name=self.FONT, size=10, color=self.BLACK_COLOR_HEX, bold=True)
        value_font = Font(name=self.FONT, size=10, color=self.BLACK_COLOR_HEX)

        # Handle empty dataframes gracefully
        if dataframe is None or len(dataframe) == 0 or 'host_name' not in dataframe.columns:
            ccsp_report_dataframe = pd.DataFrame({
                'host_name': [], 'organizations': [], 'host_runs': [], 'task_runs': [],
                'first_automation': [], 'last_automation': [], 'canonical_facts': [],
                'facts': [], 'events': [], 'managed_node_types_set': [], 'host_names_before_dedup': []
            })
        else:
            # Import centralized aggregation functions
            from metrics_utility.automation_controller_billing.dataframe_engine.base import get_aggregation_expressions
            agg_functions = get_aggregation_expressions()

            # Build aggregation expressions using centralized functions
            agg_exprs = [
                pd.col('organization_name').n_unique().alias('organizations'),
                pd.col('host_name').count().alias('host_runs'),
                agg_functions['sum']('task_runs'),
                agg_functions['min_non_null']('first_automation'),
                agg_functions['max_non_null']('last_automation'),
            ]

            # Add complex field aggregations if columns exist
            # Note: For report generation, we need List types for collections that will be converted to JSON strings
            if 'managed_node_types_set' in dataframe.columns:
                agg_exprs.append(agg_functions['flatten_unique']('managed_node_types_set'))
            if 'events' in dataframe.columns:
                agg_exprs.append(agg_functions['flatten_unique']('events'))
            if 'canonical_facts' in dataframe.columns:
                agg_exprs.append(agg_functions['merge_json_facts']('canonical_facts'))
            if 'facts' in dataframe.columns:
                agg_exprs.append(agg_functions['merge_json_facts']('facts'))
            if self.has_dedup_enabled() and 'host_names_before_dedup' in dataframe.columns:
                agg_exprs.append(agg_functions['flatten_unique']('host_names_before_dedup'))

            ccsp_report_dataframe = dataframe.group_by('host_name').agg(agg_exprs)

        # Convert arrays and dict fields into string, so they can be rendered into xlsx
        convert_cols = ['managed_node_types_set', 'events', 'canonical_facts', 'facts']
        if self.has_dedup_enabled():
            convert_cols.append('host_names_before_dedup')

        # Reset index only for pandas DataFrames (Polars doesn't have row indices)
        ccsp_report_dataframe = self.reset_index_if_needed(ccsp_report_dataframe)
        columns = [
            'host_name',
            'organizations',
            'host_runs',
            'task_runs',
            'first_automation',
            'last_automation',
        ]

        # Add facts and canonical facts for managed nodes (direct) when experimental dedup is enabled
        # or always for indirect nodes
        if managed_node_type == 'indirect' or (managed_node_type == 'direct' and self.has_dedup_enabled()):
            columns += ['canonical_facts', 'facts']

        if managed_node_type == 'indirect':
            columns += ['managed_node_types_set', 'events']

        # Handle deduplication columns if enabled
        ccsp_report_dataframe, columns, convert_cols = self.handle_dedup_columns_for_usage(ccsp_report_dataframe, columns, convert_cols)

        for col in convert_cols:
            if col in ccsp_report_dataframe.columns:
                ccsp_report_dataframe = ccsp_report_dataframe.with_columns(
                    ccsp_report_dataframe[col].map_elements(self.convert_cell, return_dtype=pd.String).alias(col)
                )

        if mode == 'by_organization':
            # Filter some columns out based on mode
            columns = [col for col in columns if col not in ['organizations']]
        ccsp_report_dataframe = self.reindex_columns(ccsp_report_dataframe, columns)

        labels = {
            'host_name': self.HOST_NAME,
            'organizations': 'Automated by\norganizations',
            'host_runs': self.JOB_RUNS,  # Job runs is the same as host_runs, Non-unique managed nodes automated
            'task_runs': self.NUM_OF_TASKS_OR_RUNS,
            'first_automation': 'First\nautomation',
            'last_automation': 'Last\nautomation',
            'canonical_facts': 'Canonical\nFacts',
            'facts': 'Facts',
        }
        if managed_node_type == 'indirect':
            labels.update(
                {
                    'managed_node_types_set': 'Manage\nNode\nTypes',
                    'events': 'Events',
                }
            )

        # Add deduplication labels if needed
        self.add_dedup_labels_if_needed(labels, ['host_names_before_dedup', 'host_names_before_dedup_count'])

        labels = {k: v for k, v in labels.items() if k in columns}
        ccsp_report_dataframe = self.rename_dataframe(ccsp_report_dataframe, labels)

        # Sort by host name to ensure consistent ordering for tests
        if self.HOST_NAME in ccsp_report_dataframe.columns:
            ccsp_report_dataframe = ccsp_report_dataframe.sort(self.HOST_NAME)

        row_counter = 0
        rows = dataframe_to_rows(self.to_pandas_for_excel(ccsp_report_dataframe), index=False)
        for r_idx, row in enumerate(rows, current_row):
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx)
                cell.value = value

                if row_counter == 0:
                    # set header style
                    cell.font = header_font
                    rd = ws.row_dimensions[r_idx]
                    rd.height = 25
                else:
                    # set value style
                    cell.font = value_font

            row_counter += 1

        return current_row + row_counter

    def _build_data_section_usage_by_collections(self, current_row, ws, dataframe):
        header_font = Font(name=self.FONT, size=10, color=self.BLACK_COLOR_HEX, bold=True)
        value_font = Font(name=self.FONT, size=10, color=self.BLACK_COLOR_HEX)

        # Handle empty dataframes gracefully
        if dataframe is None or len(dataframe) == 0 or 'collection_name' not in dataframe.columns:
            ccsp_report_dataframe = pd.DataFrame({'collection_name': [], 'host_runs_unique': [], 'host_runs': [], 'task_runs': [], 'duration': []})
        else:
            # Import centralized aggregation functions
            from metrics_utility.automation_controller_billing.dataframe_engine.base import get_aggregation_expressions
            agg_functions = get_aggregation_expressions()

            ccsp_report_dataframe = dataframe.group_by(['collection_name']).agg([
                pd.col('host_name').n_unique().alias('host_runs_unique'),
                pd.col('host_composite_id').n_unique().alias('host_runs'),
                agg_functions['sum']('task_runs'),
                agg_functions['sum']('duration'),
            ])
            # Reset index only for grouped data (collection_name becomes a regular column)
            ccsp_report_dataframe = self.reset_index_if_needed(ccsp_report_dataframe)

            # Sort by collection_name to ensure consistent ordering
            ccsp_report_dataframe = ccsp_report_dataframe.sort('collection_name')

        # Rename the columns based on the template

        rename_columns = {
            'collection_name': 'Collection name',
            'host_runs_unique': self.HOST_RUNS_UNIQUE,
            'host_runs': self.HOST_RUNS,
            'task_runs': self.NUM_OF_TASKS_OR_RUNS,
            'duration': self.DURATION,
        }

        ccsp_report_dataframe = self.rename_dataframe(ccsp_report_dataframe, rename_columns)

        row_counter = 0
        rows = dataframe_to_rows(self.to_pandas_for_excel(ccsp_report_dataframe), index=False)
        for r_idx, row in enumerate(rows, current_row):
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx)
                cell.value = value

                if row_counter == 0:
                    # set header style
                    cell.font = header_font
                    rd = ws.row_dimensions[r_idx]
                    rd.height = 25
                else:
                    # set value style
                    cell.font = value_font

            row_counter += 1

        return current_row + row_counter

    def _build_data_section_usage_by_roles(self, current_row, ws, dataframe):
        header_font = Font(name=self.FONT, size=10, color=self.BLACK_COLOR_HEX, bold=True)
        value_font = Font(name=self.FONT, size=10, color=self.BLACK_COLOR_HEX)

        # Handle empty dataframes gracefully
        if dataframe is None or len(dataframe) == 0 or 'role_name' not in dataframe.columns:
            ccsp_report_dataframe = pd.DataFrame({'role_name': [], 'host_runs_unique': [], 'host_runs': [], 'task_runs': [], 'duration': []})
        else:
            # Import centralized aggregation functions
            from metrics_utility.automation_controller_billing.dataframe_engine.base import get_aggregation_expressions
            agg_functions = get_aggregation_expressions()

            ccsp_report_dataframe = dataframe.group_by(['role_name']).agg([
                pd.col('host_name').n_unique().alias('host_runs_unique'),
                pd.col('host_composite_id').n_unique().alias('host_runs'),
                agg_functions['sum']('task_runs'),
                agg_functions['sum']('duration'),
            ])
            # Reset index only for grouped data (role_name becomes a regular column)
            ccsp_report_dataframe = self.reset_index_if_needed(ccsp_report_dataframe)

            # Sort by role_name to ensure consistent ordering
            ccsp_report_dataframe = ccsp_report_dataframe.sort('role_name')

        # Rename the columns based on the template

        rename_columns = {
            'role_name': 'Role name',
            'host_runs_unique': self.HOST_RUNS_UNIQUE,
            'host_runs': self.HOST_RUNS,
            'task_runs': self.NUM_OF_TASKS_OR_RUNS,
            'duration': self.DURATION,
        }

        ccsp_report_dataframe = self.rename_dataframe(ccsp_report_dataframe, rename_columns)

        row_counter = 0
        rows = dataframe_to_rows(self.to_pandas_for_excel(ccsp_report_dataframe), index=False)
        for r_idx, row in enumerate(rows, current_row):
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx)
                cell.value = value
                # cell.border = dotted_border

                if row_counter == 0:
                    # set header style
                    cell.font = header_font
                    rd = ws.row_dimensions[r_idx]
                    rd.height = 25
                else:
                    # set value style
                    cell.font = value_font

            row_counter += 1

        return current_row + row_counter

    def _build_data_section_usage_by_modules(self, current_row, ws, dataframe):
        header_font = Font(name=self.FONT, size=10, color=self.BLACK_COLOR_HEX, bold=True)
        value_font = Font(name=self.FONT, size=10, color=self.BLACK_COLOR_HEX)

        # Handle empty dataframes gracefully
        if dataframe is None or len(dataframe) == 0 or 'module_name' not in dataframe.columns:
            ccsp_report_dataframe = pd.DataFrame({'module_name': [], 'host_runs_unique': [], 'host_runs': [], 'task_runs': [], 'duration': []})
        else:
            # Import centralized aggregation functions  
            from metrics_utility.automation_controller_billing.dataframe_engine.base import get_aggregation_expressions
            agg_functions = get_aggregation_expressions()

            ccsp_report_dataframe = dataframe.group_by(['module_name']).agg([
                pd.col('host_name').n_unique().alias('host_runs_unique'),
                pd.col('host_composite_id').n_unique().alias('host_runs'),
                agg_functions['sum']('task_runs'),
                agg_functions['sum']('duration'),
            ])
            # Reset index only for grouped data (module_name becomes a regular column)
            ccsp_report_dataframe = self.reset_index_if_needed(ccsp_report_dataframe)

            # Sort by module_name to ensure consistent ordering
            ccsp_report_dataframe = ccsp_report_dataframe.sort('module_name')

        # Rename the columns based on the template

        rename_columns = {
            'module_name': 'Module name',
            'host_runs_unique': self.HOST_RUNS_UNIQUE,
            'host_runs': self.HOST_RUNS,
            'task_runs': self.NUM_OF_TASKS_OR_RUNS,
            'duration': self.DURATION,
        }

        ccsp_report_dataframe = self.rename_dataframe(ccsp_report_dataframe, rename_columns)

        row_counter = 0
        rows = dataframe_to_rows(self.to_pandas_for_excel(ccsp_report_dataframe), index=False)
        for r_idx, row in enumerate(rows, current_row):
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx)
                cell.value = value

                if row_counter == 0:
                    # set header style
                    cell.font = header_font
                    rd = ws.row_dimensions[r_idx]
                    rd.height = 25
                else:
                    # set value style
                    cell.font = value_font

            row_counter += 1

        return current_row + row_counter
